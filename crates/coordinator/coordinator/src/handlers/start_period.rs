//! Start-period handling and period state transitions.

use compose_primitives::{PeriodId, SuperblockNumber};
use tracing::{error, info};

use crate::coordinator::DefaultCoordinator;
use compose_primitives::xtflow;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Handle a new period from the publisher. Aborts any stale undecided
    /// instances from prior periods and sends abort votes to the publisher
    /// so it can complete the 2PC for those instances.
    pub async fn handle_start_period(
        &self,
        period_id: PeriodId,
        superblock_num: SuperblockNumber,
    ) -> Result<(), CoordinatorError> {
        let (aborted_instance_ids, builder_abort_ids): (Vec<Vec<u8>>, Vec<String>) = {
            let mut state = self.state.write().await;

            let mut aborted_ids = Vec::new();
            let mut builder_abort_ids = Vec::new();
            for xt in state.pending.values_mut() {
                if xt.decision.is_some() || xt.period_id.0 == 0 || xt.period_id >= period_id {
                    continue;
                }
                aborted_ids.push(xt.instance_id.clone());
                if let Some(super::builder_control::XtBuilderCommand::Abort { instance_id }) =
                    self.local_builder_command(xt, false)
                {
                    builder_abort_ids.push(instance_id);
                }
                xt.record_decision(false);
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

            (aborted_ids, builder_abort_ids)
        }; // write lock released before async operations

        for instance_id in &builder_abort_ids {
            self.apply_builder_command(super::builder_control::XtBuilderCommand::Abort {
                instance_id: instance_id.clone(),
            })
            .await?;
        }

        if builder_abort_ids.is_empty() {
            if let Err(e) = self.resync_put_inbox_nonce_monotonic().await {
                error!(error = %e, "Failed to resync putInbox nonce on period change");
                self.nonce_manager.reset().await;
            }
        } else {
            // `ethera_abortXt` drops the instance *and its reservations*, so
            // the builder just handed back coordinator nonces that this
            // sidecar already considers spent — including, in the worst case,
            // one whose transaction it had already accepted. The monotonic
            // resync cannot follow that downwards, and the recycling only
            // covers nonces the builder explicitly rejected, so the two views
            // would stay one apart forever and every later submission would be
            // refused for a nonce gap. Drop the counter instead and let the
            // next reservation re-read the builder's own expectation.
            //
            // ponytail: a reservation that is built but not yet submitted when
            // this runs is invisible to the builder's pending count, so its
            // nonce can be handed out twice; the duplicate is refused, recycled
            // and retried. Track in-flight reservations if that shows up.
            xtflow!(
                "nonce_reset_after_abort",
                chain = self.chain_id,
                aborted_instances = builder_abort_ids.len(),
            );
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
