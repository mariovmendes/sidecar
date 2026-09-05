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
    CoordinatorError, L2BridgeTxBuilder, MailboxSender, PublisherClient, PutInboxBuilder,
    XtBuilderClient,
};
use compose_proto::{wire_message::Payload, MailboxMessage};

use crate::model::chain_overlay::ChainOverlay;
use crate::model::pending_xt::PendingXt;
use crate::model::xt_status::{determine_xt_status, XtStatusResponse};
use crate::nonce_manager::DeferredNonceManager;
use crate::pipeline::delivery::{build_sender_nonce_cache, describe_txs};
use crate::pipeline::submission::{build_xt_request, xt_request_fingerprint};
use compose_primitives::xtflow;

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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChunkStage {
    #[default]
    Registered,
    WaitingForMessages,
    WaitingForProcessing,
    WaitingForDecided,
    Confirmed,
    Aborted
}

#[derive(Debug, Clone, Default)]
pub struct TransactionChunk {
    pub instance_id: String,
    pub confirmed_stage: Option<ChunkStage>,
    pub stage: ChunkStage,
    pub organised_transactions: HashMap<String, Vec<u8>>,
    pub is_sender: Option<bool>,
    /// How many times finalization (`sendConfirm`/`recvConfirm`) or
    /// compensation (`sendAbort`/`recvAbort`) has been attempted and failed.
    ///
    /// A failed compensation leaves the user's tokens escrowed on the sender
    /// chain with nothing on the way to refund them, so the attempt must be
    /// repeated rather than dropped. The watchdog re-dispatches chunks whose
    /// `confirmed_stage` never caught up with `stage`, up to
    /// [`MAX_FINALIZE_ATTEMPTS`].
    pub finalize_attempts: u32,
}

/// Cap on automatic finalize/compensate retries before the instance is
/// escalated. Reaching it means tokens may be stranded, so it is logged as
/// `finalize_abandoned` rather than passing silently.
pub const MAX_FINALIZE_ATTEMPTS: u32 = 10;

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
    pub inflight_chunks: HashMap<String, TransactionChunk>,
    pub mailbox_messages: HashMap<String, Vec<MailboxMessage>>,
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
            inflight_chunks: HashMap::new(),
            mailbox_messages: HashMap::new(),
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
    pub(crate) l2_bridge_builder: Option<Arc<dyn L2BridgeTxBuilder>>,
    pub(crate) xt_builder_client: Option<Arc<dyn XtBuilderClient>>,
    pub(crate) circ_timeout_ms: u64,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) metrics: Option<Arc<SidecarMetrics>>,
    pub(crate) verification: VerificationConfig,
    pub(crate) verification_client: Option<Client>,
    pub(crate) chunk_sender: Option<tokio::sync::mpsc::Sender<String>>,
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
            l2_bridge_builder: None,
            xt_builder_client: None,
            circ_timeout_ms,
            task_tracker: TaskTracker::new(),
            metrics: None,
            verification_client: Self::build_verification_client(&verification),
            verification,
            chunk_sender: None,
        }
    }

    /// Attach a metrics instance to this coordinator.
    pub fn set_metrics(&mut self, metrics: Arc<SidecarMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Attach the sender half of the main.rs chunk-processing signal channel.
    /// Only an `instance_id` is ever sent through it — the chunk processor
    /// always re-fetches the current `TransactionChunk` from `state` at
    /// dispatch time via `get_inflight_chunk`, so there's a single source of
    /// truth and no risk of dispatching on a stale snapshot.
    pub fn set_chunk_sender(&mut self, sender: tokio::sync::mpsc::Sender<String>) {
        self.chunk_sender = Some(sender);
    }

    /// Attach a putInbox signer used for local dependency fulfillment.
    pub fn set_put_inbox_builder(&mut self, builder: Arc<dyn PutInboxBuilder>) {
        self.put_inbox_builder = Some(builder);
    }

    /// Attach a builder-control client for XT reservation lifecycle events.
    pub fn set_xt_builder_client(&mut self, client: Arc<dyn XtBuilderClient>) {
        self.xt_builder_client = Some(client);
    }

    /// Attach a signer for `ComposeL2ToL2Bridge` finalize/compensate
    /// transactions (`sendConfirm`, `sendAbort*`, `recvConfirm*`, `recvAbort*`).
    pub fn set_l2_bridge_builder(&mut self, builder: Arc<dyn L2BridgeTxBuilder>) {
        self.l2_bridge_builder = Some(builder);
    }

    /// Current inflight chunk for `instance_id`, if any — a fresh clone, read
    /// and released immediately. This is the single source of truth the
    /// chunk processor dispatches on; callers must not hold a chunk fetched
    /// this way across a call into `register_xt`/`process_xt`/`confirm_xt`/
    /// `abort_xt`, which themselves lock `state` internally.
    pub async fn get_inflight_chunk(&self, instance_id: &str) -> Option<TransactionChunk> {
        self.state
            .read()
            .await
            .inflight_chunks
            .get(instance_id)
            .cloned()
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
    ///
    /// Requires the chunk sender to be attached first: the tasks below are
    /// spawned from clones of `self`, and `chunk_sender` is a plain field, so
    /// one attached afterwards would be invisible to them and the watchdog
    /// could never re-dispatch a failed finalization.
    pub async fn start(&self) -> Result<(), CoordinatorError> {
        if self.chunk_sender.is_none() {
            return Err(CoordinatorError::ChunkSenderNotSet(
                "set_chunk_sender must be called before start()".to_string(),
            ));
        }

        info!(chain_id = %self.chain_id, "Starting coordinator");

        let coord = self.clone();
        self.task_tracker.spawn(async move {
            coord.cleanup_loop().await;
        });

        let coord = self.clone();
        self.task_tracker.spawn(async move {
            coord.watchdog_loop().await;
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

    /// Periodic liveness dump. The sidecar has no timer of its own: the
    /// consensus round is bounded by the publisher's SCP timeout
    /// (`CONSENSUS_TIMEOUT`), which broadcasts `Decided(false)` and lands here
    /// as an ordinary decision. Everything after that decision — the abort
    /// compensation, the builder's inclusion callback — is unbounded, so an XT
    /// that loses a mailbox message or a builder callback sits in
    /// `inflight_chunks` indefinitely, and 100 such undecided XTs make
    /// `MAX_PENDING_XTS` reject every new submission.
    ///
    /// This loop makes both visible: one `state_dump` line per tick plus one
    /// `stuck` line per XT undecided or unconfirmed for longer than
    /// `STUCK_AFTER`. An XT still stuck well past the publisher's timeout
    /// means the `Decided` never arrived or never reached its chunk.
    async fn watchdog_loop(&self) {
        // Just over the publisher's default 20s CONSENSUS_TIMEOUT, so a
        // normally-timing-out round doesn't show up as stuck.
        const STUCK_AFTER: Duration = Duration::from_secs(25);
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            self.retry_unfinished_finalizations().await;
            self.stuck_scan(STUCK_AFTER).await;
        }
    }

    /// Re-dispatch chunks whose finalize/compensate step never completed.
    ///
    /// `confirm_xt`/`abort_xt` only record `confirmed_stage` once their
    /// on-chain step has actually been submitted, so a chunk sitting at
    /// `Confirmed`/`Aborted` with a lagging `confirmed_stage` is one whose
    /// `sendConfirm`/`sendAbort` failed. Nothing else would ever wake it: the
    /// publisher broadcasts each decision once. For an abort that means the
    /// user's escrowed tokens are waiting on a refund that will never be
    /// retried, which is exactly how the last stress run stranded 72 accounts.
    async fn retry_unfinished_finalizations(&self) {
        let pending: Vec<(String, ChunkStage, u32)> = {
            let state = self.state.read().await;
            state
                .inflight_chunks
                .values()
                .filter(|chunk| {
                    matches!(chunk.stage, ChunkStage::Confirmed | ChunkStage::Aborted)
                        && chunk.confirmed_stage != Some(chunk.stage)
                })
                .map(|chunk| {
                    (
                        chunk.instance_id.clone(),
                        chunk.stage,
                        chunk.finalize_attempts,
                    )
                })
                .collect()
        };

        for (instance_id, stage, attempts) in pending {
            if attempts >= MAX_FINALIZE_ATTEMPTS {
                xtflow!(
                    "finalize_abandoned",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    stage = format!("{stage:?}"),
                    attempts = attempts,
                );
                error!(
                    instance_id,
                    ?stage,
                    attempts,
                    "Giving up on finalization; escrowed funds may need manual compensation"
                );
                continue;
            }

            xtflow!(
                "finalize_retry",
                instance_id = instance_id,
                chain = self.chain_id,
                stage = format!("{stage:?}"),
                attempts = attempts,
            );

            let Some(sender) = self.chunk_sender.as_ref() else {
                warn!(instance_id, "No chunk sender configured, cannot retry finalization");
                continue;
            };
            if let Err(e) = sender.send(instance_id.clone()).await {
                warn!(instance_id, error = %e, "Failed to enqueue finalization retry");
            }
        }
    }

    /// Record that a finalize/compensate attempt failed, so the watchdog can
    /// bound how often it is retried.
    pub(crate) async fn record_finalize_failure(&self, instance_id: &str) -> u32 {
        let mut state = self.state.write().await;
        match state.inflight_chunks.get_mut(instance_id) {
            Some(chunk) => {
                chunk.finalize_attempts = chunk.finalize_attempts.saturating_add(1);
                chunk.finalize_attempts
            }
            None => 0,
        }
    }

    async fn stuck_scan(&self, stuck_after: Duration) {
        let state = self.state.read().await;
        let mut undecided = 0usize;
        let mut unconfirmed = 0usize;

        for (id, xt) in &state.pending {
            let decided = xt.decision.is_some();
            if !decided {
                undecided += 1;
            }
            if decided && xt.confirmed_at.is_none() {
                unconfirmed += 1;
            }

            // Terminal and cheap to skip: decided + builder-confirmed.
            if decided && xt.confirmed_at.is_some() {
                continue;
            }
            let age = xt.created_at.elapsed();
            if age < stuck_after {
                continue;
            }

            let chunk = state.inflight_chunks.get(id.as_str());
            xtflow!(
                "stuck",
                instance_id = id,
                chain = self.chain_id,
                age_ms = age.as_millis(),
                chunk_stage = chunk
                    .map(|c| format!("{:?}", c.stage))
                    .unwrap_or_else(|| "no_chunk".to_string()),
                confirmed_stage = chunk
                    .map(|c| format!("{:?}", c.confirmed_stage))
                    .unwrap_or_else(|| "-".to_string()),
                is_sender = chunk
                    .map(|c| format!("{:?}", c.is_sender))
                    .unwrap_or_else(|| "-".to_string()),
                decision = format!("{:?}", xt.decision),
                local_vote = format!("{:?}", xt.local_vote),
                peer_votes = xt.peer_votes.len(),
                expected_votes = xt.raw_txs.len(),
                mailbox_msgs = state
                    .mailbox_messages
                    .get(id.as_str())
                    .map(Vec::len)
                    .unwrap_or(0),
                confirmed = xt.confirmed_at.is_some(),
            );
        }

        let mut stages: HashMap<String, usize> = HashMap::new();
        for chunk in state.inflight_chunks.values() {
            *stages.entry(format!("{:?}", chunk.stage)).or_default() += 1;
        }
        let mut stages: Vec<String> = stages
            .into_iter()
            .map(|(stage, count)| format!("{stage}:{count}"))
            .collect();
        stages.sort();

        xtflow!(
            "state_dump",
            chain = self.chain_id,
            pending = state.pending.len(),
            undecided = undecided,
            max_pending = 100,
            decided_unconfirmed = unconfirmed,
            inflight_chunks = state.inflight_chunks.len(),
            chunk_stages = if stages.is_empty() {
                "-".to_string()
            } else {
                stages.join(",")
            },
            mailbox_instances = state.mailbox_messages.len(),
            mailbox_buffer = state.mailbox_buffer.len(),
            period = state.current_period_id.0,
            last_seq = state.last_sequence_num.0,
            recycled_nonces = self.nonce_manager.freed_count().await,
        );
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
        xtflow!(
            "submit_received",
            chain = self.chain_id,
            chains = txs.len(),
            txs = describe_txs(&txs),
        );
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
                xtflow!(
                    "publisher_submit_failed",
                    chain = self.chain_id,
                    fingerprint = fingerprint,
                    error = e,
                );
                self.resolve_pending_submission(&fingerprint, Err(message.clone()))
                    .await;
                return Err(CoordinatorError::Other(message));
            }
            xtflow!(
                "publisher_submit",
                chain = self.chain_id,
                fingerprint = fingerprint,
                txs = describe_txs(&txs),
            );
        } else {
            xtflow!(
                "publisher_submit_joined",
                chain = self.chain_id,
                fingerprint = fingerprint,
            );
        }

        // Wait for the publisher to respond with StartInstance, which carries
        // the canonical instance_id. This 10s cap is the *only* timeout on the
        // submission path: once an instance_id is assigned nothing else in the
        // pipeline is time-bounded (see `stuck_scan` in the watchdog loop).
        let waited = std::time::Instant::now();
        let assigned = tokio::time::timeout(Duration::from_secs(10), rx).await;
        let instance_id = match assigned {
            Err(_) => {
                xtflow!(
                    "publisher_assign_timeout",
                    chain = self.chain_id,
                    fingerprint = fingerprint,
                    waited_ms = waited.elapsed().as_millis(),
                    timeout_ms = 10_000,
                );
                return Err(CoordinatorError::Other(
                    "timed out waiting for publisher to assign instance_id".to_string(),
                ));
            }
            Ok(Err(_)) => {
                xtflow!(
                    "publisher_assign_dropped",
                    chain = self.chain_id,
                    fingerprint = fingerprint,
                    waited_ms = waited.elapsed().as_millis(),
                );
                return Err(CoordinatorError::Other(
                    "publisher submission resolution dropped unexpectedly".to_string(),
                ));
            }
            Ok(Ok(Err(e))) => {
                xtflow!(
                    "publisher_assign_rejected",
                    chain = self.chain_id,
                    fingerprint = fingerprint,
                    waited_ms = waited.elapsed().as_millis(),
                    error = e,
                );
                return Err(CoordinatorError::Other(e));
            }
            Ok(Ok(Ok(id))) => id,
        };

        xtflow!(
            "publisher_assigned",
            instance_id = instance_id,
            chain = self.chain_id,
            fingerprint = fingerprint,
            waited_ms = waited.elapsed().as_millis(),
        );
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
        xtflow!(
            "registered_standalone",
            instance_id = instance_id,
            chain = self.chain_id,
        );
        info!(instance_id = %instance_id, "Submitted XT locally (standalone mode)");

        // Start simulation immediately after local registration.
        {
            let coordinator = self.clone();
            let id = instance_id.clone();
            self.task_tracker.spawn(async move {
                let mut chunk = TransactionChunk {
                    instance_id: id.to_string(),
                    ..Default::default()
                };
                coordinator.register_xt(&mut chunk).await;
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

    fn chunk_at(instance_id: &str, stage: ChunkStage, confirmed: Option<ChunkStage>) -> TransactionChunk {
        TransactionChunk {
            instance_id: instance_id.to_string(),
            stage,
            confirmed_stage: confirmed,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn start_requires_the_chunk_sender_and_background_clones_keep_it() {
        // `start()` spawns its loops from clones of `self`. Attaching the
        // chunk sender afterwards leaves those clones holding `None`, which is
        // how the finalization retry silently did nothing for a whole stress
        // run — it logged every retry and enqueued none.
        let mut coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        assert!(
            coordinator.start().await.is_err(),
            "start() must refuse to spawn background tasks without a chunk sender"
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        coordinator.set_chunk_sender(tx);
        coordinator.start().await.expect("start after wiring");

        {
            let mut state = coordinator.state.write().await;
            state.inflight_chunks.insert(
                "xt-clone".to_string(),
                chunk_at("xt-clone", ChunkStage::Aborted, Some(ChunkStage::WaitingForMessages)),
            );
        }

        // A clone taken the way `start()` takes one must still be able to enqueue.
        coordinator
            .clone()
            .retry_unfinished_finalizations()
            .await;
        assert_eq!(rx.recv().await.unwrap(), "xt-clone");
        // Not calling stop(): the cleanup and watchdog loops never return, so
        // TaskTracker::wait would block forever. The runtime drops them.
    }

    #[tokio::test]
    async fn unfinished_compensation_is_retried_and_eventually_abandoned() {
        // A failed sendAbort leaves the chunk at Aborted with confirmed_stage
        // behind. Nothing else ever wakes it — the publisher broadcasts each
        // decision once — so the watchdog has to, or the user's escrowed
        // tokens are never refunded.
        let mut coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        coordinator.set_chunk_sender(tx);

        {
            let mut state = coordinator.state.write().await;
            state.inflight_chunks.insert(
                "xt-retry".to_string(),
                chunk_at("xt-retry", ChunkStage::Aborted, Some(ChunkStage::WaitingForMessages)),
            );
            // A chunk that did finish must not be retried.
            state.inflight_chunks.insert(
                "xt-done".to_string(),
                chunk_at("xt-done", ChunkStage::Confirmed, Some(ChunkStage::Confirmed)),
            );
        }

        coordinator.retry_unfinished_finalizations().await;
        assert_eq!(rx.recv().await.unwrap(), "xt-retry");
        assert!(rx.try_recv().is_err(), "finished chunks must not be retried");

        // Each failed attempt is counted, and retries stop at the cap.
        for expected in 1..=MAX_FINALIZE_ATTEMPTS {
            assert_eq!(
                coordinator.record_finalize_failure("xt-retry").await,
                expected
            );
        }

        coordinator.retry_unfinished_finalizations().await;
        assert!(
            rx.try_recv().is_err(),
            "must stop retrying once the attempt cap is reached"
        );
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
