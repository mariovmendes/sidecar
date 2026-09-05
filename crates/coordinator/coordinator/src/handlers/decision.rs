//! Decision message handling and XT finalization updates.

use tracing::{info, warn};

use crate::coordinator::DefaultCoordinator;
use compose_primitives::xtflow;
use compose_primitives_traits::CoordinatorError;
use crate::coordinator::ChunkStage::{Aborted, Confirmed};

impl DefaultCoordinator {
    /// Record a commit/abort decision for an instance.
    pub async fn on_decision(
        &self,
        instance_id: &str,
        decision: bool,
    ) -> Result<(), CoordinatorError> {
        {
            let mut state = self.state.write().await;

            let Some(xt) = state.pending.get_mut(instance_id) else {
                xtflow!(
                    "decision_unknown_instance",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    decision = decision,
                );
                return Err(CoordinatorError::InstanceNotFound(instance_id.to_string()));
            };

            if xt.decision.is_some() {
                xtflow!(
                    "decision_duplicate",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    decision = decision,
                );
                info!(instance_id, "Decision already recorded, ignoring duplicate");
                return Ok(());
            }

            // Record the decision unconditionally and immediately, even if
            // register_xt/process_xt hasn't reached a checkpoint for this
            // instance yet (i.e. no inflight chunk exists here). This is now
            // the durable source of truth: register_xt and process_xt each
            // check `xt.decision` at their own checkpoints (right after
            // tagging is_sender/organised_transactions, and right after
            // reaching WaitingForDecided) and reconcile against it there.
            //
            // Previously, an early decision arrival stamped a blank
            // tombstone chunk (confirmed_stage: None, is_sender: None,
            // organised_transactions: {}) directly into `inflight_chunks`.
            // That either blocked register_xt from ever running (the
            // dispatcher only calls it when no chunk exists yet) or got
            // silently clobbered by register_xt's own insert, losing the
            // abort decision entirely — so abort_xt, if it ever ran, always
            // saw confirmed_stage: None and skipped compensation.
            let latency = xt.created_at.elapsed();
            xt.record_decision(decision);

            xtflow!(
                "decision",
                instance_id = instance_id,
                chain = self.chain_id,
                decision = decision,
                latency_ms = latency.as_millis(),
                local_vote = format!("{:?}", xt.local_vote),
            );
            info!(instance_id, decision, "Decision received");

            if let Some(m) = &self.metrics {
                if decision {
                    m.xt_decided_commit_total.inc();
                } else {
                    m.xt_decided_abort_total.inc();
                }
                m.xt_decision_latency_seconds.observe(latency.as_secs_f64());
                m.xt_pending_count.dec();
            }

            // Only advance+dispatch an existing chunk here. If none exists
            // yet, there's nothing to stamp — register_xt will notice
            // `xt.decision` is already set once it reaches its checkpoint.
            let Some(chunk) = state.inflight_chunks.get_mut(instance_id) else {
                // register_xt/process_xt will reconcile against xt.decision
                // when they reach their own checkpoint.
                xtflow!(
                    "decision_no_chunk_yet",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    decision = decision,
                );
                return Ok(());
            };
            chunk.stage = if decision { Confirmed } else { Aborted };
        };

        let Some(sender) = self.chunk_sender.as_ref() else {
            xtflow!("signal_failed", instance_id = instance_id, chain = self.chain_id, from = "decision", error = "no_chunk_sender");
            warn!(instance_id, "No chunk sender configured, dropping");
            return Err(CoordinatorError::ChunkSenderNotSet(instance_id.to_string()));
        };

        if let Err(e) = sender.send(instance_id.to_string()).await {
            xtflow!("signal_failed", instance_id = instance_id, chain = self.chain_id, from = "decision", error = e);
            warn!(instance_id, error = %e, "Failed to enqueue chunk for processing");
            return Err(CoordinatorError::QueueError(instance_id.to_string()));
        }
        xtflow!("signal_enqueued", instance_id = instance_id, chain = self.chain_id, from = "decision");

        /*if let Some(command) = builder_command {
            self.apply_builder_command(command).await?;
        }*/

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::ChainId;

    use crate::coordinator::{ChunkStage, DefaultCoordinator, TransactionChunk, VerificationConfig};
    use crate::model::pending_xt::PendingXt;

    fn waiting_for_decided_chunk(instance_id: &str) -> TransactionChunk {
        TransactionChunk {
            instance_id: instance_id.to_string(),
            stage: ChunkStage::WaitingForDecided,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn on_decision_ignores_duplicate() {
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        coordinator.set_chunk_sender(tx);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            state.pending.insert(xt.id.clone(), xt);
            state
                .inflight_chunks
                .insert("xt-77777-1".to_string(), waiting_for_decided_chunk("xt-77777-1"));
        }

        // First decision should succeed.
        coordinator
            .on_decision("xt-77777-1", true)
            .await
            .expect("first decision should succeed");

        // Second (duplicate) decision should also return Ok without error.
        coordinator
            .on_decision("xt-77777-1", true)
            .await
            .expect("duplicate decision should be silently ignored");

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.decision, Some(true));

        // Exactly one signal (from the first decision) should have been enqueued.
        let sent_id = rx.recv().await.expect("signal should have been enqueued");
        assert_eq!(sent_id, "xt-77777-1");
        let chunk = coordinator
            .get_inflight_chunk("xt-77777-1")
            .await
            .expect("chunk should exist");
        assert_eq!(chunk.stage, ChunkStage::Confirmed);
        assert!(rx.try_recv().is_err(), "duplicate should not enqueue again");
    }

    #[tokio::test]
    async fn on_decision_records_decision_early_when_chunk_not_ready_yet() {
        // Reproduces the case where Decided arrives before register_xt has
        // had a chance to insert an inflight chunk (e.g. it's still tagging
        // is_sender/organised_transactions). The decision must be recorded
        // durably right away — not dropped, and not stamped onto a blank
        // tombstone chunk that would block or get clobbered by register_xt's
        // own insert — so register_xt/process_xt can reconcile against it
        // once they reach their own checkpoint.
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        coordinator.set_chunk_sender(tx);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            state.pending.insert(xt.id.clone(), xt);
            // No matching inflight chunk yet.
        }

        let result = coordinator.on_decision("xt-77777-1", false).await;
        assert!(result.is_ok(), "an early decision should be recorded, not rejected");

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.decision, Some(false));
        assert!(
            !state.inflight_chunks.contains_key("xt-77777-1"),
            "no tombstone chunk should be created for a not-yet-registered instance"
        );
        drop(state);

        // Nothing to dispatch yet since there's no chunk to process.
        assert!(rx.try_recv().is_err(), "should not enqueue a signal with no chunk to process");
    }

    #[tokio::test]
    async fn on_decision_abort_sets_chunk_stage() {
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        coordinator.set_chunk_sender(tx);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            state.pending.insert(xt.id.clone(), xt);
            state
                .inflight_chunks
                .insert("xt-77777-1".to_string(), waiting_for_decided_chunk("xt-77777-1"));
        }

        coordinator
            .on_decision("xt-77777-1", false)
            .await
            .expect("abort decision should succeed");

        let sent_id = rx.recv().await.expect("signal should have been enqueued");
        assert_eq!(sent_id, "xt-77777-1");
        let chunk = coordinator
            .get_inflight_chunk("xt-77777-1")
            .await
            .expect("chunk should exist");
        assert_eq!(chunk.stage, ChunkStage::Aborted);
    }
}
