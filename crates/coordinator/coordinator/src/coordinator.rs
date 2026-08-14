//! Primary coordinator type and shared mutable state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use compose_mailbox::traits::MailboxQueue;
use compose_peer::traits::PeerCoordinator;
use compose_primitives::{ChainId, InstanceId, PeriodId, SequenceNumber, SuperblockNumber};
use compose_simulation::traits::Simulator;
use prost::Message;
use reqwest::Client;
use tokio::sync::{oneshot, Notify, RwLock};
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use compose_metrics::SidecarMetrics;
use compose_primitives_traits::{
    CoordinatorError, MailboxSender, PublisherClient, PutInboxBuilder, XtBuilderClient,
};
use compose_proto::{wire_message::Payload, MailboxMessage};

use crate::model::chain_overlay::ChainOverlay;
use crate::model::pending_xt::PendingXt;
use crate::model::xt_status::{determine_xt_status, XtStatusResponse};
use crate::nonce_manager::DeferredNonceManager;
use crate::pipeline::delivery::build_sender_nonce_cache;
use crate::pipeline::submission::{build_xt_request, xt_request_fingerprint};

type PendingSubmissionResult = Result<InstanceId, String>;
type PendingSubmissionSender = oneshot::Sender<PendingSubmissionResult>;

/// Inbound verification hook configuration.
#[derive(Debug, Clone, Default)]
pub struct VerificationConfig {
    pub enabled: bool,
    pub url: String,
    pub timeout_ms: u64,
}

// Transaction chunks
#[derive(Debug, Clone, Default)]
pub struct TransactionChunk {
    pub instance_id: String,
    pub stage: u64,
    pub transactions: Vec<Vec<u8>>,
}

/// Shared coordinator state protected by a `RwLock`.
#[derive(Debug)]
pub(crate) struct CoordinatorState {
    pub pending: HashMap<InstanceId, PendingXt>,
    pub current_period_id: PeriodId,
    pub current_superblock_num: SuperblockNumber,
    pub period_initialized: bool,
    pub last_sequence_num: SequenceNumber,
    pub last_known_blocks: HashMap<ChainId, u64>,
    /// Monotonic counter for locally-originated XTs in standalone mode.
    pub origin_seq: SequenceNumber,
    /// Per-chain overlay of post-simulation state diffs. Lets XT-B see the
    /// state produced by XT-A within the current coordinator window.
    pub chain_overlay: HashMap<ChainId, ChainOverlay>,
    /// Notified whenever a mailbox message arrives, waking waiting simulations.
    pub mailbox_notify: Arc<Notify>,
    /// Maps XT fingerprints to instance IDs for standalone-mode deduplication.
    pub submitted_fingerprints: HashMap<String, InstanceId>,
    /// Oneshot channels waiting for the publisher to assign an instance ID
    /// after an `XtRequest` is submitted. Keyed by fingerprint.
    pub pending_submissions: HashMap<String, Vec<PendingSubmissionSender>>,
    /// Index from raw `instance_id` bytes → XT id for mailbox routing (O(1)).
    pub mailbox_index: HashMap<Vec<u8>, InstanceId>,
    /// Mailbox messages that arrived before the XT was registered (race buffer).
    ///
    /// When sidecar-a's simulation completes very fast, it may send outbound
    /// mailbox messages to sidecar-b before forwarding the XT.  Messages that
    /// arrive while the XT is unknown are stored here keyed by raw `instance_id`
    /// bytes and drained into `PendingXt::pending_mailbox` the moment the XT
    /// is registered.  Entries are cleared on rollback when the period resets.
    pub mailbox_buffer: HashMap<Vec<u8>, Vec<MailboxMessage>>,
}

impl CoordinatorState {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            current_period_id: PeriodId(0),
            current_superblock_num: SuperblockNumber(0),
            period_initialized: false,
            last_sequence_num: SequenceNumber(0),
            last_known_blocks: HashMap::new(),
            origin_seq: SequenceNumber(0),
            chain_overlay: HashMap::new(),
            mailbox_notify: Arc::new(Notify::new()),
            submitted_fingerprints: HashMap::new(),
            pending_submissions: HashMap::new(),
            mailbox_index: HashMap::new(),
            mailbox_buffer: HashMap::new(),
        }
    }

    /// Buffer a mailbox message for an XT that has not yet been registered.
    ///
    /// Called when a CIRC message arrives before the forwarded XT, which can
    /// happen when sidecar-a's simulation completes in <1 ms and the outbound
    /// message reaches sidecar-b before the XT forward does.
    pub(crate) fn buffer_orphan_mailbox(&mut self, msg: MailboxMessage) {
        self.mailbox_buffer
            .entry(msg.instance_id.clone())
            .or_default()
            .push(msg);
    }

    /// Drain any buffered mailbox messages for the given raw `instance_id` key
    /// and return them so the caller can attach them to the newly registered XT.
    pub(crate) fn drain_mailbox_buffer(&mut self, raw_id: &[u8]) -> Vec<MailboxMessage> {
        self.mailbox_buffer.remove(raw_id).unwrap_or_default()
    }
}

/// The default coordinator implementation.
///
/// This struct is cheaply cloneable (all shared state is behind `Arc`).
#[derive(Clone)]
pub struct DefaultCoordinator {
    pub(crate) chain_id: ChainId,
    pub(crate) state: Arc<RwLock<CoordinatorState>>,
    pub(crate) nonce_manager: Arc<DeferredNonceManager>,
    pub(crate) simulator: Option<Arc<dyn Simulator>>,
    pub(crate) publisher: Option<Arc<dyn PublisherClient>>,
    pub(crate) mailbox_sender: Option<Arc<dyn MailboxSender>>,
    pub(crate) mailbox_queue: Option<Arc<dyn MailboxQueue>>,
    pub(crate) peer_coordinator: Option<Arc<dyn PeerCoordinator>>,
    pub(crate) put_inbox_builder: Option<Arc<dyn PutInboxBuilder>>,
    pub(crate) xt_builder_client: Option<Arc<dyn XtBuilderClient>>,
    pub(crate) circ_timeout_ms: u64,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) metrics: Option<Arc<SidecarMetrics>>,
    pub(crate) verification: VerificationConfig,
    pub(crate) verification_client: Option<Client>,
}

impl std::fmt::Debug for DefaultCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultCoordinator")
            .field("chain_id", &self.chain_id)
            .field("circ_timeout_ms", &self.circ_timeout_ms)
            .finish()
    }
}

impl DefaultCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        simulator: Option<Arc<dyn Simulator>>,
        publisher: Option<Arc<dyn PublisherClient>>,
        mailbox_sender: Option<Arc<dyn MailboxSender>>,
        mailbox_queue: Option<Arc<dyn MailboxQueue>>,
        peer_coordinator: Option<Arc<dyn PeerCoordinator>>,
        circ_timeout_ms: u64,
        verification: VerificationConfig,
    ) -> Self {
        Self {
            chain_id,
            state: Arc::new(RwLock::new(CoordinatorState::new())),
            nonce_manager: Arc::new(DeferredNonceManager::new()),
            simulator,
            publisher,
            mailbox_sender,
            mailbox_queue,
            peer_coordinator,
            put_inbox_builder: None,
            xt_builder_client: None,
            circ_timeout_ms,
            task_tracker: TaskTracker::new(),
            metrics: None,
            verification_client: Self::build_verification_client(&verification),
            verification,
        }
    }

    /// Attach a metrics instance to this coordinator.
    pub fn set_metrics(&mut self, metrics: Arc<SidecarMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Attach a putInbox signer used for local dependency fulfillment.
    pub fn set_put_inbox_builder(&mut self, builder: Arc<dyn PutInboxBuilder>) {
        self.put_inbox_builder = Some(builder);
    }

    /// Attach a builder-control client for XT reservation lifecycle events.
    pub fn set_xt_builder_client(&mut self, client: Arc<dyn XtBuilderClient>) {
        self.xt_builder_client = Some(client);
    }

    fn build_verification_client(verification: &VerificationConfig) -> Option<Client> {
        if !verification.enabled {
            return None;
        }

        Some(
            Client::builder()
                .timeout(Duration::from_millis(verification.timeout_ms))
                .build()
                .expect("verification client configuration should be valid"),
        )
    }

    /// Start the coordinator's background tasks (cleanup loop, etc.).
    pub async fn start(&self) -> Result<(), CoordinatorError> {
        info!(chain_id = %self.chain_id, "Starting coordinator");

        let coord = self.clone();
        self.task_tracker.spawn(async move {
            coord.cleanup_loop().await;
        });

        Ok(())
    }

    /// Gracefully shut down, waiting for all spawned tasks to complete.
    pub async fn stop(&self) -> Result<(), CoordinatorError> {
        info!("Stopping coordinator");
        self.task_tracker.close();
        self.task_tracker.wait().await;
        Ok(())
    }

    /// Remove decided XTs older than `max_age`.
    pub async fn cleanup(&self, max_age: Duration) {
        let mut state = self.state.write().await;
        let now = std::time::Instant::now();
        let mut new_mailbox_index = HashMap::with_capacity(state.pending.len());
        state.pending.retain(|_id, xt| {
            let age_ref = xt.confirmed_at.or(xt.decided_at);
            let keep = if let Some(t) = age_ref {
                now.duration_since(t) <= max_age
            } else {
                true
            };
            if keep {
                new_mailbox_index.insert(xt.instance_id.clone(), xt.id.clone());
            }
            keep
        });
        state.mailbox_index = new_mailbox_index;
        let stale_fps: Vec<String> = state
            .submitted_fingerprints
            .iter()
            .filter(|(_, id)| !state.pending.contains_key(id.as_str()))
            .map(|(fp, _)| fp.clone())
            .collect();
        for fp in stale_fps {
            state.submitted_fingerprints.remove(&fp);
        }
        // Drop submission channels where the caller already timed out.
        state.pending_submissions.retain(|_, waiters| {
            waiters.retain(|tx| !tx.is_closed());
            !waiters.is_empty()
        });
        // Remove orphan mailbox_buffer entries whose XTs will never arrive.
        let orphan_keys: Vec<Vec<u8>> = state
            .mailbox_buffer
            .keys()
            .filter(|key| !state.mailbox_index.contains_key(key.as_slice()))
            .cloned()
            .collect();
        for key in orphan_keys {
            state.mailbox_buffer.remove(&key);
        }
        if let Some(m) = &self.metrics {
            m.mailbox_buffer_size.set(state.mailbox_buffer.len() as i64);
        }
    }

    async fn cleanup_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.cleanup(Duration::from_secs(300)).await;
        }
    }

    pub(crate) async fn resolve_pending_submission(
        &self,
        fingerprint: &str,
        result: PendingSubmissionResult,
    ) {
        let waiters = self
            .state
            .write()
            .await
            .pending_submissions
            .remove(fingerprint);
        if let Some(waiters) = waiters {
            Self::notify_pending_submission_waiters(waiters, result);
        }
    }

    pub(crate) fn notify_pending_submission_waiters(
        waiters: Vec<PendingSubmissionSender>,
        result: PendingSubmissionResult,
    ) {
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// Query the status of a cross-chain transaction by instance ID.
    pub async fn get_xt_status(
        &self,
        instance_id: &str,
    ) -> Result<XtStatusResponse, CoordinatorError> {
        let state = self.state.read().await;
        let xt = state
            .pending
            .get(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        Ok(XtStatusResponse {
            instance_id: instance_id.to_string(),
            status: determine_xt_status(xt),
            decision: xt.decision,
        })
    }

    /// Whether the publisher connection is currently active.
    pub(crate) async fn is_publisher_connected(&self) -> bool {
        self.publisher
            .as_ref()
            .map(|p| p.is_connected())
            .unwrap_or(false)
    }

    /// In standalone mode, compute whether the instance can be decided from the
    /// currently known local and peer votes.
    ///
    /// Rules are aligned with SCP/2PC docs:
    /// - any `false` vote decides `false` immediately;
    /// - `true` is decided only when all expected votes are collected.
    pub(crate) fn maybe_make_standalone_decision(
        &self,
        xt: &mut PendingXt,
    ) -> Option<(bool, usize, usize)> {
        if xt.decision.is_some() {
            return None;
        }

        let expected = xt.raw_txs.len();
        let mut collected = 0usize;
        let mut has_abort_vote = false;

        if let Some(local) = xt.local_vote {
            collected += 1;
            if !local {
                has_abort_vote = true;
            }
        }

        for (cid, &vote) in &xt.peer_votes {
            if *cid == self.chain_id {
                continue;
            }
            collected += 1;
            if !vote {
                has_abort_vote = true;
            }
        }

        if has_abort_vote {
            xt.record_decision(false);
            return Some((false, collected, expected));
        }

        if expected > 0 && collected >= expected {
            xt.record_decision(true);
            return Some((true, collected, expected));
        }

        None
    }

    /// Submit a cross-chain transaction.
    ///
    /// In publisher-connected mode, the XT is encoded as an `XtRequest` protobuf
    /// and sent to the publisher, which assigns the instance ID. In standalone
    /// mode, a local ID is generated and the XT is forwarded to peer sidecars.
    pub async fn submit_xt(
        &self,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
    ) -> Result<String, CoordinatorError> {
        if txs.is_empty() {
            return Err(CoordinatorError::NoTransactions);
        }
        if txs.len() < 2 {
            return Err(CoordinatorError::Other(
                "cross-chain transaction must span at least 2 chains".to_string(),
            ));
        }

        if self.is_publisher_connected().await {
            self.submit_xt_publisher(txs).await
        } else {
            self.submit_xt_standalone(txs).await
        }
    }

    async fn submit_xt_publisher(
        &self,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
    ) -> Result<String, CoordinatorError> {
        let publisher = self
            .publisher
            .as_ref()
            .ok_or(CoordinatorError::PublisherNotConnected)?;

        let xt_request = build_xt_request(&txs);
        let fingerprint = xt_request_fingerprint(&xt_request);

        let (tx, rx) = oneshot::channel();
        let should_send = {
            let mut state = self.state.write().await;
            let waiters = state
                .pending_submissions
                .entry(fingerprint.clone())
                .or_default();
            let should_send = waiters.is_empty();
            waiters.push(tx);
            should_send
        };

        if should_send {
            let wire = compose_proto::WireMessage {
                sender_id: String::new(),
                payload: Some(Payload::XtRequest(xt_request)),
            };
            let data = wire.encode_to_vec();

            if let Err(e) = publisher.send_raw(&data).await {
                let message = format!("failed to send XT to publisher: {e}");
                self.resolve_pending_submission(&fingerprint, Err(message.clone()))
                    .await;
                return Err(CoordinatorError::Other(message));
            }
        }

        // Wait for the publisher to respond with StartInstance, which carries
        // the canonical instance_id.
        let instance_id = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| {
                CoordinatorError::Other(
                    "timed out waiting for publisher to assign instance_id".to_string(),
                )
            })?
            .map_err(|_| {
                CoordinatorError::Other(
                    "publisher submission resolution dropped unexpectedly".to_string(),
                )
            })?
            .map_err(CoordinatorError::Other)?;

        info!(instance_id = %instance_id, "Submitted XT to publisher");
        Ok(instance_id.to_string())
    }

    async fn submit_xt_standalone(
        &self,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
    ) -> Result<String, CoordinatorError> {
        const MAX_PENDING_XTS: usize = 100;

        // Compute fingerprint before acquiring the lock to detect duplicates.
        let xt_request = build_xt_request(&txs);
        let fingerprint = xt_request_fingerprint(&xt_request);

        let (instance_id, txs_for_forward, local_submission) = {
            let mut state = self.state.write().await;

            // Return the existing instance ID for duplicate submissions, as long
            // as the original XT is still pending. Once cleaned up, re-submission
            // is allowed (the fingerprint entry is pruned by cleanup).
            if let Some(existing_id) = state.submitted_fingerprints.get(&fingerprint) {
                if state.pending.contains_key(existing_id.as_str()) {
                    let id = existing_id.clone();
                    info!(instance_id = %id, "Duplicate XT submission, returning existing ID");
                    return Ok(id.to_string());
                }
                // Original was cleaned up; remove the stale fingerprint entry.
                state.submitted_fingerprints.remove(&fingerprint);
            }

            let undecided_count = state
                .pending
                .values()
                .filter(|xt| xt.decision.is_none())
                .count();
            if undecided_count >= MAX_PENDING_XTS {
                return Err(CoordinatorError::TooManyPendingInstances(MAX_PENDING_XTS));
            }

            state.origin_seq = SequenceNumber(state.origin_seq.0 + 1);
            let seq = state.origin_seq;
            let id = InstanceId::standalone(self.chain_id, seq.0);

            // Clone only for forwarding when a peer coordinator is configured;
            // `txs` itself is moved into the XT to avoid an unconditional clone.
            let txs_for_forward = self.peer_coordinator.as_ref().map(|_| txs.clone());

            let mut xt = PendingXt::new(id.to_string(), id.as_bytes().to_vec());
            xt.origin_chain = Some(self.chain_id);
            xt.origin_seq = seq;
            xt.sender_nonces = build_sender_nonce_cache(&txs);
            xt.raw_txs = txs;
            // Pre-lock so only one local simulation task claims this XT.
            xt.locked_chains.insert(self.chain_id);

            state
                .mailbox_index
                .insert(id.as_bytes().to_vec(), id.clone());
            state.pending.insert(id.clone(), xt);
            state.submitted_fingerprints.insert(fingerprint, id.clone());
            let local_submission = state
                .pending
                .get(&id)
                .and_then(|xt| self.local_builder_submission(xt));
            (id, txs_for_forward, local_submission)
        }; // write lock released here

        if let Some(submission) = local_submission {
            if let Err(err) = self.submit_xt_to_builder(submission).await {
                self.remove_pending_xt(&instance_id).await;
                return Err(err);
            }
        }

        if let Some(m) = &self.metrics {
            m.xt_received_total.inc();
            m.xt_pending_count.inc();
        }
        info!(instance_id = %instance_id, "Submitted XT locally (standalone mode)");

        // Start simulation immediately after local registration.
        {
            let coordinator = self.clone();
            let id = instance_id.clone();
            self.task_tracker.spawn(async move {
                coordinator.process_xt(&id).await;
            });
        }

        if let Some(peer_coordinator) = &self.peer_coordinator {
            let id = instance_id.clone();
            let chain_id = self.chain_id;
            let origin_seq = {
                let state = self.state.read().await;
                state.origin_seq
            };
            let pc = peer_coordinator.clone();
            // txs_for_forward is Some(_) whenever peer_coordinator is Some.
            let txs = txs_for_forward.expect("cloned above when peer_coordinator is set");
            self.task_tracker.spawn(async move {
                if let Err(e) = pc.forward_xt(&id, &txs, chain_id, origin_seq).await {
                    error!(instance_id = %id, error = %e, "Failed to forward XT to peers");
                }
            });
        } else {
            warn!(
                instance_id = %instance_id,
                "No peer coordinator configured, XT will only be processed locally"
            );
        }

        Ok(instance_id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::sleep;

    #[derive(Debug, Default)]
    struct MockPublisher {
        send_raw_calls: AtomicUsize,
    }

    #[async_trait]
    impl PublisherClient for MockPublisher {
        async fn connect(&self) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn connect_with_retry(&self) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn disconnect(&self) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn send_vote(
            &self,
            _instance_id: &[u8],
            _vote: bool,
        ) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn send_confirmed(&self, _instance_id: &[u8], _chain_id: u64 ) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn send_raw(&self, _data: &[u8]) -> Result<(), CoordinatorError> {
            self.send_raw_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn is_connected(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn cleanup_removes_old_decided_xts() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.decision = Some(true);
            xt.decided_at = Some(
                std::time::Instant::now()
                    .checked_sub(Duration::from_secs(400))
                    .unwrap(),
            );
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.cleanup(Duration::from_secs(300)).await;

        let state = coordinator.state.read().await;
        assert!(state.pending.is_empty());
    }

    #[tokio::test]
    async fn cleanup_removes_old_confirmed_xts() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-2".to_string(), b"xt-77777-2".to_vec());
            xt.decision = Some(true);
            xt.decided_at = Some(std::time::Instant::now());
            // confirmed_at is the age reference when set; simulate old confirmation.
            xt.confirmed_at = Some(
                std::time::Instant::now()
                    .checked_sub(Duration::from_secs(400))
                    .unwrap(),
            );
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.cleanup(Duration::from_secs(300)).await;

        let state = coordinator.state.read().await;
        assert!(state.pending.is_empty());
    }

    #[tokio::test]
    async fn cleanup_retains_recently_confirmed_xts() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-3".to_string(), b"xt-77777-3".to_vec());
            xt.decision = Some(true);
            xt.decided_at = Some(
                std::time::Instant::now()
                    .checked_sub(Duration::from_secs(400))
                    .unwrap(),
            );
            // decided_at is old but confirmed_at is recent — should be retained.
            xt.confirmed_at = Some(std::time::Instant::now());
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.cleanup(Duration::from_secs(300)).await;

        let state = coordinator.state.read().await;
        assert_eq!(state.pending.len(), 1);
    }

    #[tokio::test]
    async fn submit_xt_publisher_joins_duplicate_waiters() {
        let publisher = Arc::new(MockPublisher::default());
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            Some(publisher.clone()),
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        let mut txs = HashMap::new();
        txs.insert(ChainId(77777), vec![vec![0x01, 0x02, 0x03]]);
        txs.insert(ChainId(88888), vec![vec![0x04, 0x05, 0x06]]);
        let fingerprint = xt_request_fingerprint(&build_xt_request(&txs));

        let coordinator_a = coordinator.clone();
        let txs_a = txs.clone();
        let first = tokio::spawn(async move { coordinator_a.submit_xt(txs_a).await });

        let coordinator_b = coordinator.clone();
        let txs_b = txs.clone();
        let second = tokio::spawn(async move { coordinator_b.submit_xt(txs_b).await });

        // Wait until both tasks are parked in pending_submissions AND the
        // publisher call has been made. The two conditions can briefly diverge:
        // the winning task releases the state lock (incrementing waiter_count)
        // before it calls send_raw(), so checking both together is required to
        // avoid a race.
        for _ in 0..100 {
            let waiter_count = coordinator
                .state
                .read()
                .await
                .pending_submissions
                .get(&fingerprint)
                .map(Vec::len)
                .unwrap_or_default();
            let send_calls = publisher.send_raw_calls.load(Ordering::SeqCst);
            if waiter_count == 2 && send_calls == 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(publisher.send_raw_calls.load(Ordering::SeqCst), 1);

        coordinator
            .resolve_pending_submission(&fingerprint, Ok(InstanceId::from("xt-77777-1")))
            .await;

        assert_eq!(first.await.unwrap().unwrap(), "xt-77777-1");
        assert_eq!(second.await.unwrap().unwrap(), "xt-77777-1");
    }

    #[tokio::test]
    async fn rollback_resolves_pending_submission_waiters() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );
        let (tx, rx) = oneshot::channel();

        {
            let mut state = coordinator.state.write().await;
            state
                .pending_submissions
                .insert("fp-1".to_string(), vec![tx]);
        }

        coordinator
            .handle_rollback(PeriodId(1), 5, b"hash")
            .await
            .unwrap();

        let result = rx.await.unwrap();
        assert_eq!(
            result,
            Err("publisher submission aborted by rollback".to_string())
        );
    }
}
