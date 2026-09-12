//! Start-period handling and period state transitions.

use compose_primitives::{InstanceId, PeriodId, SuperblockNumber};
use std::time::Duration;
use tracing::{error, info};

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

const STALE_AFTER: Duration = Duration::from_secs(20);

impl DefaultCoordinator {
    /// Handle a new period from the publisher. Aborts any stale undecided
    /// instances from prior periods and sends abort votes to the publisher
    /// so it can complete the 2PC for those instances.
    pub async fn handle_start_period(
        &self,
        period_id: PeriodId,
        superblock_num: SuperblockNumber,
    ) -> Result<(), CoordinatorError> {
        let (aborted_instance_ids, stale_ids): (Vec<Vec<u8>>, Vec<InstanceId>) = {
            let mut state = self.state.write().await;

            let mut aborted_ids = Vec::new();
            let mut stale_ids = Vec::new();
            for xt in state.pending.values() {
                if xt.decision.is_some() || xt.period_id.0 == 0 || xt.period_id >= period_id {
                    continue;
                }
                if xt.created_at.elapsed() < STALE_AFTER {
                    continue;
                }
                aborted_ids.push(xt.instance_id.clone());
                stale_ids.push(xt.id.clone());
            }

            state.current_period_id = period_id;
            state.current_superblock_num = superblock_num;
            state.period_initialized = true;
            state.last_sequence_num = Default::default();
            state.last_known_blocks.clear();
            state.chain_overlay.clear();

            info!(
                period_id = period_id.0,
                superblock_num = superblock_num.0,
                aborted_stale = aborted_ids.len(),
                "Started new period"
            );

            (aborted_ids, stale_ids)
        }; // write lock released before async operations

        // Abort through the normal decision path rather than dropping the
        // instance at the builder directly: `on_decision` records the abort,
        // moves any existing chunk to `Aborted` and hands the id to the chunk
        // processor, which is what actually runs `abort_xt` and puts the
        // compensation on chain. Marking the decision here without signalling
        // would strand a chunk past `WaitingForMessages` at `WaitingForDecided`
        // forever. As such, the finalize watchdog only looks at terminal stages.
        //
        // Logged and skipped rather than propagated: one stale instance that
        // cannot be aborted must not stop the period from starting.
        for id in &stale_ids {
            if let Err(e) = self.on_decision(id.as_str(), false).await {
                error!(instance_id = %id, error = %e, "Failed to abort stale XT on period change");
            }
        }

        if let Err(e) = self.resync_put_inbox_nonce_monotonic().await {
            error!(error = %e, "Failed to resync putInbox nonce on period change");
            self.nonce_manager.reset().await;
        }

        // Notify the publisher of the abort for each stale XT so it can
        // complete the 2PC round and unblock the next period's instances.
        if !aborted_instance_ids.is_empty() {
            if let Some(publisher) = &self.publisher {
                if publisher.is_connected() {
                    for instance_id in &aborted_instance_ids {
                        if let Err(e) = publisher.send_vote(instance_id, false).await {
                            error!(error = %e, "Failed to send abort vote for stale XT");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
