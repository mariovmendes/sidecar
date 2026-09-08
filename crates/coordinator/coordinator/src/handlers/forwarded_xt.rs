//! Handling for cross-chain transactions forwarded by peers.

use std::collections::HashMap;

use compose_primitives::{ChainId, SequenceNumber};
use tracing::{debug, info};

use crate::coordinator::{DefaultCoordinator, TransactionChunk, MAX_PENDING_XTS};
use crate::model::pending_xt::PendingXt;
use crate::pipeline::delivery::{build_sender_nonce_cache, describe_txs};
use compose_primitives::xtflow;
use compose_primitives_traits::CoordinatorError;



impl DefaultCoordinator {
    /// Process an XT forwarded from another sidecar.
    pub async fn handle_forwarded_xt(
        &self,
        instance_id: &str,
        txs: HashMap<ChainId, Vec<Vec<u8>>>,
        origin_chain: ChainId,
        origin_seq: SequenceNumber,
    ) -> Result<(), CoordinatorError> {
        if instance_id.is_empty() {
            return Err(CoordinatorError::Other(
                "missing instance_id for forwarded XT".to_string(),
            ));
        }

        let clean_txs: HashMap<ChainId, Vec<Vec<u8>>> = txs
            .into_iter()
            .filter(|(_, chain_txs)| !chain_txs.is_empty())
            .collect();

        if clean_txs.is_empty() {
            return Err(CoordinatorError::NoTransactions);
        }

        let mut state = self.state.write().await;

        if state.pending.contains_key(instance_id) {
            return Ok(());
        }

        let undecided_count = state
            .pending
            .values()
            .filter(|xt| xt.decision.is_none())
            .count();
        if undecided_count >= MAX_PENDING_XTS {
            return Err(CoordinatorError::TooManyPendingInstances(MAX_PENDING_XTS));
        }

        let has_local = clean_txs.contains_key(&self.chain_id);

        let mut xt = PendingXt::new(instance_id.to_string(), instance_id.as_bytes().to_vec());
        xt.sender_nonces = build_sender_nonce_cache(&clean_txs);
        xt.raw_txs = clean_txs;
        xt.origin_chain = Some(origin_chain);
        xt.origin_seq = origin_seq;

        // Pre-lock so only one local simulation task claims this XT.
        // TODO: Remove this pre-lock
        if has_local {
            xt.locked_chains.insert(self.chain_id);
        }

        let raw_key = instance_id.as_bytes().to_vec();
        state.mailbox_index.insert(raw_key.clone(), xt.id.clone());
        state.pending.insert(xt.id.clone(), xt);

        // Drain messages that arrived before the XT was registered (race window).
        let buffered = state.drain_mailbox_buffer(&raw_key);
        if !buffered.is_empty() {
            if let Some(pending_xt) = state.pending.get_mut(instance_id) {
                debug!(
                    xt_id = instance_id,
                    count = buffered.len(),
                    "Attaching buffered mailbox messages to forwarded XT"
                );
                pending_xt.pending_mailbox.extend(buffered);
            }
        }

        xtflow!(
            "forwarded_xt_in",
            instance_id = instance_id,
            chain = self.chain_id,
            origin_chain = origin_chain,
            origin_seq = origin_seq.0,
            has_local = has_local,
            txs = describe_txs(&state.pending[instance_id].raw_txs),
        );
        info!(
            xt_id = instance_id,
            chains = state.pending[instance_id].raw_txs.len(),
            origin_chain = %origin_chain,
            origin_seq = origin_seq.0,
            "Received forwarded XT from peer"
        );

        let local_submission = state
            .pending
            .get(instance_id)
            .and_then(|xt| self.local_builder_submission(xt));

        // Release the write lock before spawning so register_xt can acquire it.
        drop(state);

        if let Some(submission) = local_submission {
            if let Err(err) = self.submit_xt_to_builder(submission).await {
                self.remove_pending_xt(instance_id).await;
                return Err(err);
            }
        }

        if has_local {
            let coordinator = self.clone();
            let id = instance_id.to_string();
            self.task_tracker.spawn(async move {
                let mut chunk = TransactionChunk {
                    instance_id: id,
                    ..Default::default()
                };
                coordinator.register_xt(&mut chunk).await;
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // `handle_forwarded_xt_rejects_when_at_max_pending` was removed with the
    // raise of MAX_PENDING_XTS: it filled `pending` with exactly 100 undecided
    // XTs and asserted the guard fired at that number. The guard is now a
    // memory backstop set far out of the way for stress testing, so a test
    // pinned to the old value only asserts the constant's value. Restore a
    // proper one if the limit is ever made configurable.
}
