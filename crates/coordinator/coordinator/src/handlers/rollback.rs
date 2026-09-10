//! Rollback handling for aborting undecided instances.

use compose_primitives::{PeriodId, SuperblockNumber};
use tracing::warn;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Abort all pending local builder reservations and reset period state.
    pub async fn handle_rollback(
        &self,
        period_id: PeriodId,
        last_finalized_superblock_num: u64,
        _last_finalized_superblock_hash: &[u8],
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let mut aborted = 0;
        let mut builder_abort_ids = Vec::new();
        for xt in state.pending.values_mut() {
            if let Some(crate::handlers::builder_control::XtBuilderCommand::Abort { instance_id }) =
                self.local_builder_command(xt, false)
            {
                builder_abort_ids.push(instance_id);
            }
            if xt.decision != Some(false) {
                xt.record_decision(false);
                aborted += 1;
            }
        }

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
            aborted_instances = aborted,
            "Rollback received, pending instances aborted"
        );

        drop(state);

        for instance_id in &builder_abort_ids {
            self.apply_builder_command(super::builder_control::XtBuilderCommand::Abort {
                instance_id: instance_id.clone(),
            })
            .await?;
        }

        // A rollback rewinds the chain itself and aborts the pending instances
        // at the builder just above, so every locally reserved nonce is void.
        // Drop the counter entirely (including recycled nonces) and let the
        // next reservation re-read the builder's expected next nonce.
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

    use crate::coordinator::{DefaultCoordinator};

    #[tokio::test]
    async fn handle_rollback_updates_period_and_superblock() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

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
