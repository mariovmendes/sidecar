//! Simulation pipeline and vote emission flow.

use std::time::{Duration, Instant as StdInstant};

use compose_mailbox::matching::{
    contains_message, dependency_keys_equal, matches_dependency, DependencyKey, MailboxMessageKey,
};
use compose_mailbox::overrides::merge_overrides;
use compose_mailbox::wire;
use compose_primitives::{ChainId, CrossRollupDependency, CrossRollupMessage, StateOverride};
use compose_proto::MailboxMessage;
use serde::Serialize;
use tokio::time::{sleep_until, Instant};
use tracing::{debug, error, info, warn};

use crate::coordinator::DefaultCoordinator;
use crate::model::chain_overlay::ChainOverlay;

#[derive(Debug, Serialize)]
struct VerificationPayload<'a> {
    instance_id: &'a str,
    dest_chain_id: u64,
    origin_chain_id: Option<u64>,
    txs: Vec<String>,
}

impl DefaultCoordinator {
    /// Run the simulation pipeline for the local chain's portion of an XT.
    ///
    /// Simulates transactions sequentially, discovers mailbox dependencies,
    /// waits for CIRC messages, and sends a vote.
    pub async fn process_xt(&self, instance_id: &str) {
        info!(instance_id, chain_id = %self.chain_id, "Processing XT");

        // Capture everything we need from state in a single read lock. Start
        // from the accumulated local overlay so later XTs see the post-state
        // of previously committed local XTs in this period.
        let (tx_bytes_list, mut current_overrides) = {
            let state = self.state.read().await;
            match state.pending.get(instance_id) {
                Some(xt) => match xt.raw_txs.get(&self.chain_id) {
                    Some(txs) if !txs.is_empty() => {
                        debug!(
                            instance_id,
                            chain_id = %self.chain_id,
                            "Simulation state check"
                        );

                        let mut overrides = StateOverride::default();
                        if let Some(chain_overlay) = state.chain_overlay.get(&self.chain_id) {
                            merge_overrides(&mut overrides, &chain_overlay.overlay);
                        }

                        (txs.clone(), overrides)
                    }
                    _ => {
                        warn!(instance_id, "No local transactions, rejecting");
                        drop(state);
                        let _ = self.send_vote(instance_id, false).await;
                        return;
                    }
                },
                None => {
                    warn!(
                        instance_id,
                        "XT disappeared during simulation (likely rollback)"
                    );
                    return;
                }
            }
        };

        if let Err(err) = self.verify_xt(instance_id, &tx_bytes_list).await {
            warn!(instance_id, error = %err, "Verification hook rejected XT");
            let _ = self.send_vote(instance_id, false).await;
            return;
        }


        let simulator = match &self.simulator {
            Some(s) => s.clone(),
            None => {
                warn!("No simulator configured, voting yes without simulation");
                let _ = self.send_vote(instance_id, true).await;
                return;
            }
        };

        // Initialize fulfilled deps once. They are refreshed from state only
        // after a dep-wait cycle, not on every simulation attempt.
        let mut fulfilled_deps = {
            let state = self.state.read().await;
            match state.pending.get(instance_id) {
                Some(xt) => xt.fulfilled_deps.clone(),
                None => {
                    warn!(
                        instance_id,
                        "XT disappeared during simulation setup (likely rollback)"
                    );
                    return;
                }
            }
        };

        // Simulate each transaction sequentially.
        for (tx_index, tx_bytes) in tx_bytes_list.iter().enumerate() {
            // Retry simulation until success or CIRC timeout. The
            // wait_for_dependencies deadline is the only bound on the loop.
            loop {
                let sim_start = StdInstant::now();
                let sim_result = simulator
                    .simulate_with_mailbox(
                        self.chain_id,
                        tx_bytes,
                        &current_overrides,
                        &fulfilled_deps,
                    )
                    .await;
                if let Some(m) = &self.metrics {
                    m.simulation_duration_seconds
                        .observe(sim_start.elapsed().as_secs_f64());
                }
                match sim_result {
                    Ok(result) => {
                        current_overrides = self
                            .record_simulation_state(instance_id, &result, &current_overrides)
                            .await;

                        if !result.success && result.dependencies.is_empty() {
                            warn!(
                                instance_id,
                                tx_index,
                                error = ?result.error,
                                "Simulation returned failure with no dependencies"
                            );
                            let _ = self.send_vote(instance_id, false).await;
                            return;
                        }

                        if !result.success
                            && result.dependencies.iter().all(|dep| {
                                fulfilled_deps
                                    .iter()
                                    .any(|fulfilled| dependency_keys_equal(fulfilled, dep))
                            })
                        {
                            warn!(
                                instance_id,
                                tx_index,
                                error = ?result.error,
                                dep_count = result.dependencies.len(),
                                "Simulation failed after all mailbox dependencies were already fulfilled"
                            );
                            let _ = self.send_vote(instance_id, false).await;
                            return;
                        }

                        if result.success {
                            if let Err(e) = self
                                .dispatch_outbound_mailbox(instance_id, &result.outbound_messages)
                                .await
                            {
                                error!(instance_id, error = %e, "Failed to dispatch mailbox messages");
                                let _ = self.send_vote(instance_id, false).await;
                                return;
                            }
                            break;
                        }

                        // A simulation can both produce outbound messages and have unresolved
                        // dependencies (e.g. a contract that writes then reads the mailbox).
                        // Dispatch those messages now so peer sidecars can fulfill their own
                        // dependencies while we wait on ours.
                        if !result.outbound_messages.is_empty() {
                            if let Err(e) = self
                                .dispatch_outbound_mailbox(instance_id, &result.outbound_messages)
                                .await
                            {
                                error!(instance_id, error = %e, "Failed to dispatch mailbox messages");
                                let _ = self.send_vote(instance_id, false).await;
                                return;
                            }
                        }

                        info!(
                            instance_id,
                            tx_index,
                            dep_count = result.dependencies.len(),
                            "Simulation waiting for mailbox dependencies"
                        );

                        // TODO: Here because it is sequential, it will wait for the message to be received. Instead, I want to be able to pass to the processing
                        // to the next transaction and only come back to this if an answer is received.
                        if !self
                            .wait_for_dependencies(instance_id, &result.dependencies)
                            .await
                        {
                            warn!(
                                instance_id,
                                tx_index, "Timed out waiting for mailbox dependencies"
                            );
                            let _ = self.send_vote(instance_id, false).await;
                            return;
                        }

                        // Deps were fulfilled: refresh local view from state once per
                        // dep-wait cycle instead of once per simulation attempt.
                        {
                            let state = self.state.read().await;
                            match state.pending.get(instance_id) {
                                Some(xt) => {
                                    fulfilled_deps = xt.fulfilled_deps.clone();
                                }
                                None => {
                                    warn!(
                                        instance_id,
                                        "XT disappeared during dep-wait (likely rollback)"
                                    );
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(instance_id, error = %e, "Simulation failed");
                        if let Some(m) = &self.metrics {
                            m.simulation_error_total.inc();
                        }
                        let _ = self.send_vote(instance_id, false).await;
                        return;
                    }
                }
            }
        }

        // Verification hook already run before simulation.
        let _ = self.send_vote(instance_id, true).await;
    }

    async fn verify_xt(&self, instance_id: &str, txs: &[Vec<u8>]) -> Result<(), String> {
        if !self.verification.enabled {
            return Ok(());
        }

        let origin_chain = {
            let state = self.state.read().await;
            state
                .pending
                .get(instance_id)
                .and_then(|xt| xt.origin_chain)
        };

        let payload = VerificationPayload {
            instance_id,
            dest_chain_id: self.chain_id.0,
            origin_chain_id: origin_chain.map(|cid| cid.0),
            txs: txs.iter().map(hex::encode).collect(),
        };

        let client = self
            .verification_client
            .as_ref()
            .ok_or_else(|| "verification client not configured".to_string())?;

        let response = client
            .post(&self.verification.url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("verification request failed: {e}"))?;

        if !response.status().is_success() {
            return Err(format!(
                "verification rejected with status {}",
                response.status()
            ));
        }

        debug!(
            instance_id,
            url = %self.verification.url,
            timeout_ms = self.verification.timeout_ms,
            ?payload,
            "Verification hook approved XT"
        );

        Ok(())
    }

    /// Record simulation results into XT state, update the chain overlay with
    /// the post-simulation overrides so subsequent XTs see the committed state,
    /// and return the overrides for the next simulation step.
    async fn record_simulation_state(
        &self,
        instance_id: &str,
        result: &compose_primitives::SimulationResult,
        base_overrides: &StateOverride,
    ) -> StateOverride {
        let mut state = self.state.write().await;
        let Some(xt) = state.pending.get_mut(instance_id) else {
            return base_overrides.clone();
        };

        let mut merged_overrides = base_overrides.clone();
        if result.success {
            if let Some(ref result_overrides) = result.state_overrides {
                merge_overrides(&mut merged_overrides, result_overrides);
            }
        }

        for dep in &result.dependencies {
            let key = DependencyKey::from(dep);
            if xt.dep_keys.insert(key) {
                xt.dependencies.push(dep.clone());
            }
        }

        for msg in &result.outbound_messages {
            if !contains_message(&xt.outbound_messages, msg) {
                xt.outbound_messages.push(msg.clone());
            }
        }

        // Update the chain overlay so the next XT simulated on this chain sees
        // the accumulated post-simulation state.
        if result.success {
            let overlay = state
                .chain_overlay
                .entry(self.chain_id)
                .or_insert_with(ChainOverlay::new);
            merge_overrides(&mut overlay.overlay, &merged_overrides);
        }

        merged_overrides
    }

    /// Wait until at least one dependency is fulfilled or the CIRC timeout
    /// expires. Uses `Notify` to wake immediately when a mailbox message
    /// arrives, replacing the previous 50 ms busy-poll loop.
    async fn wait_for_dependencies(
        &self,
        instance_id: &str,
        deps: &[CrossRollupDependency],
    ) -> bool {
        let deadline = Instant::now() + Duration::from_millis(self.circ_timeout_ms);
        let wait_started = StdInstant::now();
        loop {
            let fulfilled = self
                .fulfill_dependencies_from_mailbox(instance_id, deps)
                .await;
            if fulfilled > 0 {
                if let Some(m) = &self.metrics {
                    m.mailbox_wait_duration_seconds
                        .observe(wait_started.elapsed().as_secs_f64());
                }
                return true;
            }
            if Instant::now() >= deadline {
                if let Some(m) = &self.metrics {
                    m.mailbox_wait_duration_seconds
                        .observe(wait_started.elapsed().as_secs_f64());
                    m.mailbox_wait_timeout_total.inc();
                }
                return false;
            }
            // Clone the Arc before dropping the lock so we can call .notified()
            // outside the critical section. This avoids missing a notification
            // that arrives between the dependency check above and the select below.
            let notify = {
                let state = self.state.read().await;
                state.mailbox_notify.clone()
            };
            tokio::select! {
                _ = notify.notified() => {}
                _ = sleep_until(deadline) => {
                    if let Some(m) = &self.metrics {
                        m.mailbox_wait_duration_seconds
                            .observe(wait_started.elapsed().as_secs_f64());
                        m.mailbox_wait_timeout_total.inc();
                    }
                    return false;
                }
            }
        }
    }

    async fn fulfill_dependencies_from_mailbox(
        &self,
        instance_id: &str,
        deps: &[CrossRollupDependency],
    ) -> usize {
        let mut added = 0usize;

        let mut state = self.state.write().await;
        let Some(xt) = state.pending.get_mut(instance_id) else {
            return 0;
        };

        for dep in deps {
            let key = DependencyKey::from(dep);
            if xt.fulfilled_dep_keys.contains(&key) {
                continue;
            }

            if let Some(idx) = xt
                .pending_mailbox
                .iter()
                .position(|msg| matches_dependency(msg, dep))
            {
                let mailbox_msg = xt.pending_mailbox.remove(idx);
                let mut fulfilled = dep.clone();
                fulfilled.data = Some(mailbox_msg.payload.clone());
                xt.fulfilled_dep_keys.insert(key);
                xt.fulfilled_deps.push(fulfilled);
                added += 1;
            }
        }

        if added > 0 {
            info!(
                instance_id,
                fulfilled = added,
                total_fulfilled = xt.fulfilled_deps.len(),
                "Fulfilled dependencies from mailbox messages"
            );
        }

        added
    }

    async fn dispatch_outbound_mailbox(
        &self,
        instance_id: &str,
        outbound_messages: &[CrossRollupMessage],
    ) -> Result<(), compose_primitives_traits::CoordinatorError> {
        if outbound_messages.is_empty() {
            return Ok(());
        }

        let Some(sender) = self.mailbox_sender.as_ref().cloned() else {
            warn!(
                instance_id,
                "Mailbox sender not configured, skipping outbound mailbox delivery"
            );
            return Ok(());
        };

        let mut to_send = Vec::<MailboxMessage>::new();
        {
            let mut state = self.state.write().await;
            let Some(xt) = state.pending.get_mut(instance_id) else {
                return Ok(());
            };

            for msg in outbound_messages {
                let mailbox_msg = MailboxMessage {
                    instance_id: xt.instance_id.clone(),
                    source_chain: msg.source_chain_id.0,
                    destination_chain: msg.dest_chain_id.0,
                    sender: msg.sender.as_slice().to_vec(),
                    receiver: msg.receiver.as_slice().to_vec(),
                    label: msg.label.clone(),
                    payload: msg.data.clone(),
                    session_id: wire::encode_session_id(msg.session_id),
                };

                let key = MailboxMessageKey::from(&mailbox_msg);
                if xt.sent_mailbox_keys.insert(key) {
                    xt.sent_mailbox.push(mailbox_msg.clone());
                    to_send.push(mailbox_msg);
                }
            }
        }

        let sent_count = to_send.len();
        for msg in to_send {
            sender.send(ChainId(msg.destination_chain), &msg).await?;
        }
        if sent_count > 0 {
            if let Some(m) = &self.metrics {
                m.circ_messages_sent_total.inc_by(sent_count as u64);
            }
        }

        Ok(())
    }

    /// Send a vote for the given instance.
    pub(crate) async fn send_vote(
        &self,
        instance_id: &str,
        vote: bool,
    ) -> Result<(), compose_primitives_traits::CoordinatorError> {
        let standalone_mode = !self.is_publisher_connected().await;
        let mut decision_made: Option<(bool, usize, usize)> = None;
        let mut builder_command = None;

        let instance_bytes = {
            let mut state = self.state.write().await;
            let Some(xt) = state.pending.get_mut(instance_id) else {
                return Ok(());
            };

            // First local vote wins for the instance.
            if xt.local_vote.is_some() {
                debug!(
                    instance_id,
                    existing_vote = ?xt.local_vote,
                    duplicate_vote = vote,
                    "Local vote already recorded, ignoring duplicate"
                );
                return Ok(());
            }

            xt.simulated_at = Some(std::time::Instant::now());
            let simulation_duration = xt.simulated_at.unwrap().duration_since(xt.created_at).as_secs_f64();
            info!(instance_id,simulation_duration_ms = (simulation_duration * 1000.0) as u64,"Finished simulating cTx:");
            xt.vote_sent = true;
            xt.local_vote = Some(vote);
            xt.locked_chains.insert(self.chain_id);
            if standalone_mode {
                decision_made = self.maybe_make_standalone_decision(xt);
                if let Some((decision, _, _)) = decision_made {
                    builder_command = self.local_builder_command(xt, decision);
                }
            }
            xt.instance_id.clone()
        };

        if let Some((decision, collected, expected)) = decision_made {
            info!(
                instance_id,
                decision,
                votes = collected,
                expected_votes = expected,
                "Made local decision (standalone mode)"
            );
            if let Some(m) = &self.metrics {
                m.xt_decision_latency_seconds.observe(
                    self.state
                        .read()
                        .await
                        .pending
                        .get(instance_id)
                        .map(|xt| xt.created_at.elapsed().as_secs_f64())
                        .unwrap_or(0.0),
                );
                m.xt_pending_count.dec();
            }
        }

        if let Some(command) = builder_command {
            self.apply_builder_command(command).await?;
        }

        if !standalone_mode {
            if let Some(publisher) = &self.publisher {
                if let Err(e) = publisher.send_vote(&instance_bytes, vote).await {
                    error!(instance_id, error = %e, "Failed to send vote to publisher");
                    if let Some(m) = &self.metrics {
                        m.vote_send_failed_total.inc();
                    }
                } else {
                    info!(instance_id, vote, "Vote sent to publisher");
                }
            }
        } else {
            info!(
                instance_id,
                vote,
                chain_id = %self.chain_id,
                "Local vote recorded (standalone mode)"
            );

            // Forward vote to peers.
            if let Some(peer_coord) = &self.peer_coordinator {
                let chain_id = self.chain_id;
                let id = instance_id.to_string();
                let pc = peer_coord.clone();
                let metrics = self.metrics.clone();
                self.task_tracker.spawn(async move {
                    if let Err(e) = pc.send_vote_to_peers(&id, chain_id, vote).await {
                        error!(instance_id = %id, error = %e, "Failed to send vote to peers");
                        if let Some(m) = metrics {
                            m.vote_send_failed_total.inc();
                        }
                    }
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use alloy::primitives::U256;
    use alloy_rpc_types_eth::state::AccountOverride;
    use async_trait::async_trait;
    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use compose_mailbox::wire;
    use compose_primitives::ChainId;
    use compose_primitives::StateOverride;
    use compose_primitives::{CrossRollupDependency, SimulationResult};
    use compose_simulation::error::SimulationError;
    use compose_simulation::traits::Simulator;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle};

    use crate::coordinator::{DefaultCoordinator, VerificationConfig};
    use crate::model::chain_overlay::ChainOverlay;
    use crate::model::pending_xt::PendingXt;

    #[derive(Clone)]
    struct VerificationServerState {
        hits: Arc<AtomicUsize>,
        status: StatusCode,
        body: &'static str,
    }

    struct TestVerificationServer {
        hits: Arc<AtomicUsize>,
        task: JoinHandle<()>,
        url: String,
    }

    impl TestVerificationServer {
        async fn spawn(status: StatusCode, body: &'static str) -> Self {
            let hits = Arc::new(AtomicUsize::new(0));
            let app = Router::new()
                .route("/verify", post(test_verification_handler))
                .with_state(VerificationServerState {
                    hits: hits.clone(),
                    status,
                    body,
                });

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });

            Self {
                hits,
                task,
                url: format!("http://{addr}/verify"),
            }
        }

        fn assert_hits(&self, expected: usize) {
            assert_eq!(self.hits.load(Ordering::SeqCst), expected);
        }
    }

    impl Drop for TestVerificationServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn test_verification_handler(
        State(state): State<VerificationServerState>,
    ) -> (StatusCode, &'static str) {
        state.hits.fetch_add(1, Ordering::SeqCst);
        (state.status, state.body)
    }

    #[tokio::test]
    async fn send_vote_does_not_overwrite_existing_local_vote() {
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
            let xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-1", true).await.unwrap();
        coordinator.send_vote("xt-77777-1", false).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn send_vote_decides_when_peer_vote_already_present() {
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
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), true);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-2", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-2").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(true));
    }

    #[tokio::test]
    async fn send_vote_applies_existing_abort_peer_vote() {
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
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), false);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-3", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-3").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(false));
    }

    /// Simple stub simulator for testing: always returns success or always fails.
    struct StubSimulator {
        succeed: bool,
    }

    #[async_trait]
    impl Simulator for StubSimulator {
        async fn simulate(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            _state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            if self.succeed {
                Ok(SimulationResult {
                    success: true,
                    error: None,
                    state_overrides: None,
                    dependencies: Vec::new(),
                    outbound_messages: Vec::new(),
                })
            } else {
                Err(SimulationError::Failed("stub failure".to_string()))
            }
        }

        async fn simulate_with_mailbox(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
            _fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate(chain_id, tx, state_overrides).await
        }
    }

    struct RetryRecordingSimulator {
        calls: AtomicUsize,
        seen_overrides: Mutex<Vec<StateOverride>>,
        dependency: CrossRollupDependency,
        failed_override_addr: Address,
    }

    #[async_trait]
    impl Simulator for RetryRecordingSimulator {
        async fn simulate(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate_with_mailbox(chain_id, tx, state_overrides, &[])
                .await
        }

        async fn simulate_with_mailbox(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            state_overrides: &StateOverride,
            _fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            self.seen_overrides
                .lock()
                .unwrap()
                .push(state_overrides.clone());

            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let mut failed_overrides = StateOverride::default();
                failed_overrides.insert(
                    self.failed_override_addr,
                    AccountOverride {
                        nonce: Some(9),
                        ..Default::default()
                    },
                );
                Ok(SimulationResult {
                    success: false,
                    error: Some("missing mailbox".to_string()),
                    state_overrides: Some(failed_overrides),
                    dependencies: vec![self.dependency.clone()],
                    outbound_messages: Vec::new(),
                })
            } else {
                Ok(SimulationResult {
                    success: true,
                    error: None,
                    state_overrides: None,
                    dependencies: Vec::new(),
                    outbound_messages: Vec::new(),
                })
            }
        }
    }

    struct FailingAfterFulfilledDependencySimulator {
        calls: AtomicUsize,
        dependency: CrossRollupDependency,
    }

    #[async_trait]
    impl Simulator for FailingAfterFulfilledDependencySimulator {
        async fn simulate(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate_with_mailbox(chain_id, tx, state_overrides, &[])
                .await
        }

        async fn simulate_with_mailbox(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            _state_overrides: &StateOverride,
            fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                assert!(fulfilled_deps.is_empty());
                Ok(SimulationResult {
                    success: false,
                    error: Some("missing mailbox".to_string()),
                    state_overrides: None,
                    dependencies: vec![self.dependency.clone()],
                    outbound_messages: Vec::new(),
                })
            } else {
                assert_eq!(fulfilled_deps.len(), 1);
                Ok(SimulationResult {
                    success: false,
                    error: Some("out of gas".to_string()),
                    state_overrides: None,
                    dependencies: vec![self.dependency.clone()],
                    outbound_messages: Vec::new(),
                })
            }
        }
    }

    #[tokio::test]
    async fn process_xt_votes_true_on_success_with_no_deps() {
        let simulator = Arc::new(StubSimulator { succeed: true });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-10".to_string(), b"xt-77777-10".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-10").await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-10").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn process_xt_verification_allows_commit_on_success_response() {
        let server = TestVerificationServer::spawn(StatusCode::OK, "I confirm").await;

        let simulator = Arc::new(StubSimulator { succeed: true });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig {
                enabled: true,
                url: server.url.clone(),
                timeout_ms: 2_000,
            },
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-verify".to_string(), b"xt-77777-verify".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-verify").await;

        server.assert_hits(1);
        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-verify").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn process_xt_verification_abort_on_reject_response() {
        let server = TestVerificationServer::spawn(StatusCode::FORBIDDEN, "reject").await;

        let simulator = Arc::new(StubSimulator { succeed: true });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig {
                enabled: true,
                url: server.url.clone(),
                timeout_ms: 2_000,
            },
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-reject".to_string(), b"xt-77777-reject".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-reject").await;

        server.assert_hits(1);
        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-reject").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }

    #[tokio::test]
    async fn process_xt_votes_false_on_simulation_error() {
        let simulator = Arc::new(StubSimulator { succeed: false });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-11".to_string(), b"xt-77777-11".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xde, 0xad]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-11").await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-11").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }

    #[tokio::test]
    async fn process_xt_votes_false_when_no_local_txs() {
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
            // XT has transactions only for a different chain.
            let mut xt = PendingXt::new("xt-77777-12".to_string(), b"xt-77777-12".to_vec());
            xt.raw_txs.insert(ChainId(88888), vec![vec![0xff]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-12").await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-12").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }

    #[tokio::test]
    async fn process_xt_does_not_carry_failed_dependency_overrides_into_retry() {
        let base_addr = Address::repeat_byte(0x11);
        let failed_addr = Address::repeat_byte(0x22);
        let dependency = CrossRollupDependency {
            source_chain_id: ChainId(88888),
            dest_chain_id: ChainId(77777),
            sender: Address::repeat_byte(0x33),
            receiver: Address::repeat_byte(0x44),
            label: b"SEND".to_vec(),
            data: None,
            session_id: U256::ZERO,
        };

        let simulator = Arc::new(RetryRecordingSimulator {
            calls: AtomicUsize::new(0),
            seen_overrides: Mutex::new(Vec::new()),
            dependency: dependency.clone(),
            failed_override_addr: failed_addr,
        });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator.clone()),
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

        let mut base_overrides = StateOverride::default();
        base_overrides.insert(
            base_addr,
            AccountOverride {
                nonce: Some(7),
                ..Default::default()
            },
        );

        {
            let mut state = coordinator.state.write().await;
            state.chain_overlay.insert(
                ChainId(77777),
                ChainOverlay {
                    overlay: base_overrides.clone(),
                },
            );

            let mut xt = PendingXt::new("xt-77777-13".to_string(), b"xt-77777-13".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            xt.pending_mailbox.push(compose_proto::MailboxMessage {
                source_chain: 88888,
                destination_chain: 77777,
                sender: Address::repeat_byte(0x33).as_slice().to_vec(),
                receiver: Address::repeat_byte(0x44).as_slice().to_vec(),
                label: "SEND".to_string(),
                payload: vec![1, 2, 3],
                session_id: wire::encode_session_id(U256::ZERO),
                ..Default::default()
            });
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.process_xt("xt-77777-13").await;

        let seen = simulator.seen_overrides.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[0].contains_key(&base_addr));
        assert!(!seen[0].contains_key(&failed_addr));
        assert!(seen[1].contains_key(&base_addr));
        assert!(
            !seen[1].contains_key(&failed_addr),
            "retry should start from base overrides, not failed-trace post-state"
        );
    }

    #[tokio::test]
    async fn process_xt_votes_false_immediately_when_failed_retry_only_has_fulfilled_deps() {
        let dependency = CrossRollupDependency {
            source_chain_id: ChainId(88888),
            dest_chain_id: ChainId(77777),
            sender: Address::repeat_byte(0x33),
            receiver: Address::repeat_byte(0x44),
            label: b"SEND".to_vec(),
            data: None,
            session_id: U256::ZERO,
        };

        let simulator = Arc::new(FailingAfterFulfilledDependencySimulator {
            calls: AtomicUsize::new(0),
            dependency: dependency.clone(),
        });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator.clone()),
            None,
            None,
            None,
            None,
            10_000,
            VerificationConfig::default(),
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-14".to_string(), b"xt-77777-14".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            xt.pending_mailbox.push(compose_proto::MailboxMessage {
                source_chain: 88888,
                destination_chain: 77777,
                sender: Address::repeat_byte(0x33).as_slice().to_vec(),
                receiver: Address::repeat_byte(0x44).as_slice().to_vec(),
                label: "SEND".to_string(),
                payload: vec![1, 2, 3],
                session_id: wire::encode_session_id(U256::ZERO),
                ..Default::default()
            });
            state.pending.insert(xt.id.clone(), xt);
        }

        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            coordinator.process_xt("xt-77777-14"),
        )
        .await
        .expect("process_xt should not wait for already-fulfilled deps");

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-14").unwrap();
        assert_eq!(xt.local_vote, Some(false));
        assert_eq!(xt.fulfilled_deps.len(), 1);
        assert_eq!(simulator.calls.load(Ordering::SeqCst), 2);
    }
}
