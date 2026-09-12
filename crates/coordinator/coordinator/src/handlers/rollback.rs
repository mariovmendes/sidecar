//! Rollback handling for aborting undecided instances.

use compose_primitives::{InstanceId, PeriodId, SuperblockNumber};
use tracing::warn;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Abort every instance that is not already aborted and reset period state.
    pub async fn handle_rollback(
        &self,
        period_id: PeriodId,
        last_finalized_superblock_num: u64,
        _last_finalized_superblock_hash: &[u8],
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let aborted_ids: Vec<InstanceId> = state
            .pending
            .values()
            .filter(|xt| xt.decision != Some(false))
            .map(|xt| xt.id.clone())
            .collect();

        state.period_initialized = false;
        state.current_period_id = period_id;
        state.current_superblock_num = SuperblockNumber(last_finalized_superblock_num + 1);
        state.last_sequence_num = Default::default();
        state.last_known_blocks.clear();
        state.chain_overlay.clear();
        state.mailbox_buffer.clear();
        let pending_submissions = std::mem::take(&mut state.pending_submissions);

        warn!(
            period_id = period_id.0,
            last_finalized_superblock = last_finalized_superblock_num,
            aborted_instances = aborted_ids.len(),
            "Rollback received, pending instances aborted"
        );

        drop(state);

        // Same as `handle_start_period`: the abort has to reach the chunk
        // processor, which is what runs `abort_xt` and compensates on chain.
        // Recording the decision here without signalling strands any chunk
        // past `WaitingForMessages` at `WaitingForDecided` forever.
        for id in &aborted_ids {
            if let Err(e) = self.on_decision(id.as_str(), false).await {
                warn!(instance_id = %id, error = %e, "Failed to abort instance on rollback");
            }
        }

        // A rollback rewinds the chain itself, so every locally reserved nonce
        // is void. Drop the counter entirely (including recycled nonces) and
        // let the next reservation re-read the builder's expected next nonce.
        self.nonce_manager.reset().await;

        for waiters in pending_submissions.into_values() {
            Self::notify_pending_submission_waiters(
                waiters,
                Err("publisher submission aborted by rollback".to_string()),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::{ChainId, PeriodId, SuperblockNumber};

    use crate::coordinator::ChunkStage::{Aborted, WaitingForDecided, WaitingForMessages};
    use crate::coordinator::{DefaultCoordinator, TransactionChunk};
    use crate::model::pending_xt::PendingXt;

    /// A chunk already past `WaitingForMessages` has its putInbox/receive (or
    /// bridge) tx at the builder, so the rollback owes it a compensation. That
    /// only happens if the id reaches the chunk processor. The finalize
    /// watchdog never looks at `WaitingForDecided`.
    #[tokio::test]
    async fn handle_rollback_signals_the_chunk_processor_to_compensate() {
        let mut coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        coordinator.set_chunk_sender(tx);

        {
            let mut state = coordinator.state.write().await;
            let xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            state.pending.insert(xt.id.clone(), xt);
            state.inflight_chunks.insert(
                "xt-77777-1".to_string(),
                TransactionChunk {
                    instance_id: "xt-77777-1".to_string(),
                    stage: WaitingForDecided,
                    confirmed_stage: Some(WaitingForMessages),
                    is_sender: Some(false),
                    ..Default::default()
                },
            );
        }

        coordinator
            .handle_rollback(PeriodId(8), 50, b"hash")
            .await
            .unwrap();

        assert_eq!(rx.recv().await.unwrap(), "xt-77777-1");
        let state = coordinator.state.read().await;
        assert_eq!(state.inflight_chunks["xt-77777-1"].stage, Aborted);
    }

    #[tokio::test]
    async fn handle_rollback_updates_period_and_superblock() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);

        // Set initial state.
        {
            let mut state = coordinator.state.write().await;
            state.current_period_id = PeriodId(10);
            state.current_superblock_num = SuperblockNumber(100);
            state.period_initialized = true;
        }

        coordinator
            .handle_rollback(PeriodId(8), 50, b"hash")
            .await
            .unwrap();

        let state = coordinator.state.read().await;
        assert_eq!(state.current_period_id, PeriodId(8));
        assert_eq!(state.current_superblock_num, SuperblockNumber(51));
        assert!(!state.period_initialized);
    }
}
