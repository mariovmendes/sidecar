//! Start-instance handling and sequencing validation.

use compose_primitives::{ChainId, InstanceId, PeriodId, SequenceNumber};
use compose_proto::StartInstance;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, warn};

use crate::coordinator::DefaultCoordinator;
use crate::model::pending_xt::PendingXt;
use crate::pipeline::delivery::{build_sender_nonce_cache, describe_txs};
use crate::pipeline::submission::xt_request_fingerprint;
use compose_primitives::xtflow;
use compose_primitives_traits::CoordinatorError;

/// Maximum number of pending XTs before new submissions are rejected.
const MAX_PENDING_XTS: usize = 100;

impl DefaultCoordinator {
    /// Process a new instance from the publisher. Validates the period and
    /// sequence, decodes transactions, and registers the XT.
    pub async fn handle_start_instance(
        &self,
        msg: &StartInstance,
        sender: &Sender<String>,
    ) -> Result<(), CoordinatorError> {
        let instance_id = InstanceId::from_publisher_bytes(&msg.instance_id);
        let xt_request = msg
            .xt_request
            .as_ref()
            .ok_or_else(|| CoordinatorError::Other("missing xt_request".to_string()))?;

        // Check if local chain participates.
        let mut includes_local = false;
        for req in &xt_request.transaction_requests {
            let chain_id = ChainId(req.chain_id);
            if chain_id == self.chain_id && !req.transaction.is_empty() {
                includes_local = true;
                break;
            }
        }

        // Decode transactions per chain.
        let mut raw_txs: HashMap<ChainId, Vec<Vec<u8>>> = HashMap::new();
        for req in &xt_request.transaction_requests {
            let chain_id = ChainId(req.chain_id);
            for tx_bytes in &req.transaction {
                raw_txs.entry(chain_id).or_default().push(tx_bytes.clone());
            }
        }

        let sender_nonces = build_sender_nonce_cache(&raw_txs);

        let fingerprint = xt_request_fingerprint(xt_request);
        let mut state = self.state.write().await;

        if state.pending.contains_key(&instance_id) {
            return Err(CoordinatorError::InstanceAlreadyPending(
                instance_id.to_string(),
            ));
        }

        let undecided_count = state
            .pending
            .values()
            .filter(|xt| xt.decision.is_none())
            .count();
        if undecided_count >= MAX_PENDING_XTS {
            let error = CoordinatorError::TooManyPendingInstances(MAX_PENDING_XTS).to_string();
            xtflow!(
                "backpressure",
                instance_id = instance_id,
                chain = self.chain_id,
                undecided = undecided_count,
                max_pending = MAX_PENDING_XTS,
            );
            drop(state);
            self.resolve_pending_submission(&fingerprint, Err(error))
                .await;
            if let Some(m) = &self.metrics {
                m.xt_received_total.inc();
            }
            return Err(CoordinatorError::TooManyPendingInstances(MAX_PENDING_XTS));
        }

        if !state.period_initialized {
            drop(state);
            self.resolve_pending_submission(
                &fingerprint,
                Err(CoordinatorError::PeriodNotInitialized.to_string()),
            )
            .await;
            warn!(instance_id = %instance_id, "Period not initialized, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        let msg_period = PeriodId(msg.period_id);
        let current_period = state.current_period_id;
        if msg_period < current_period {
            drop(state);
            self.resolve_pending_submission(
                &fingerprint,
                Err(format!(
                    "publisher start-instance rejected: stale period {} < current {}",
                    msg_period.0, current_period.0
                )),
            )
            .await;
            warn!(instance_id = %instance_id, msg_period = msg_period.0, current_period = current_period.0, "Stale period, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }
        if msg_period > current_period {
            drop(state);
            self.resolve_pending_submission(
                &fingerprint,
                Err(format!(
                    "publisher start-instance rejected: future period {} > current {}",
                    msg_period.0, current_period.0
                )),
            )
            .await;
            warn!(instance_id = %instance_id, msg_period = msg_period.0, current_period = current_period.0, "Future period (last block still building), rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        let msg_seq = SequenceNumber(msg.sequence_number);
        if msg_seq <= state.last_sequence_num {
            drop(state);
            self.resolve_pending_submission(
                &fingerprint,
                Err(CoordinatorError::StaleSequence.to_string()),
            )
            .await;
            warn!(instance_id = %instance_id, "Stale sequence, rejecting");
            self.reject_start_instance(&instance_id, msg).await;
            return Ok(());
        }

        state.last_sequence_num = msg_seq;

        let mut xt = PendingXt::new(instance_id.to_string(), msg.instance_id.clone());
        xt.period_id = msg_period;
        xt.sequence_num = msg_seq;
        xt.raw_txs = raw_txs;
        xt.sender_nonces = sender_nonces;

        // TODO: Remove this as there is no more chain locking.
        // Pre-lock so only one local simulation task claims this XT.
        if includes_local {
            xt.locked_chains.insert(self.chain_id);
        }

        state
            .mailbox_index
            .insert(msg.instance_id.clone(), instance_id.clone());
        state.pending.insert(instance_id.clone(), xt);

        // Drain messages that arrived before the XT was registered (race window).
        let buffered = state.drain_mailbox_buffer(&msg.instance_id);
        if !buffered.is_empty() {
            if let Some(pending_xt) = state.pending.get_mut(&instance_id) {
                debug!(
                    instance_id = %instance_id,
                    count = buffered.len(),
                    "Attaching buffered mailbox messages to new instance"
                );
                pending_xt.pending_mailbox.extend(buffered);
            }
        }

        xtflow!(
            "start_instance",
            instance_id = instance_id,
            chain = self.chain_id,
            period = msg.period_id,
            seq = msg.sequence_number,
            chains = state.pending[&instance_id].raw_txs.len(),
            includes_local = includes_local,
            txs = describe_txs(&state.pending[&instance_id].raw_txs),
        );
        info!(
            instance_id = %instance_id,
            period_id = msg.period_id,
            sequence = msg.sequence_number,
            chains = state.pending[&instance_id].raw_txs.len(),
            "New instance started"
        );

        if let Some(m) = &self.metrics {
            m.xt_received_total.inc();
            m.xt_pending_count.inc();
        }

        // Release the write lock before spawning so register_xt can acquire it.
        drop(state);

        /*
        let local_submission = state
            .pending
            .get(&instance_id)
            .and_then(|xt| self.local_builder_submission(xt));

        if let Some(submission) = local_submission {
            if let Err(err) = self.submit_xt_to_builder(submission).await {
                self.remove_pending_xt(&instance_id).await;
                self.resolve_pending_submission(&fingerprint, Err(err.to_string()))
                    .await;
                self.reject_start_instance(&instance_id, msg).await;
                return Err(err);
            }
        }*/

        self.resolve_pending_submission(&fingerprint, Ok(instance_id.clone()))
            .await;

        if includes_local {
            let id = instance_id.to_string();
            if let Err(e) = sender.send(id.clone()).await {
                xtflow!("signal_failed", instance_id = id, chain = self.chain_id, error = e);
                error!(error = %e, instance_id = %id, "Failed to enqueue new instance, skipping XT processing");
            } else {
                xtflow!("signal_enqueued", instance_id = id, chain = self.chain_id, from = "start_instance");
                info!(instance_id = %id, "New instance signalled to chunk processor");
            }
        } else {
            xtflow!("not_local", instance_id = instance_id, chain = self.chain_id);
        }

        Ok(())
    }

    async fn reject_start_instance(&self, instance_id: &str, msg: &StartInstance) {
        xtflow!(
            "start_instance_rejected",
            instance_id = instance_id,
            chain = self.chain_id,
            period = msg.period_id,
            seq = msg.sequence_number,
        );
        warn!(
            instance_id,
            period_id = msg.period_id,
            sequence = msg.sequence_number,
            "Rejecting StartInstance"
        );
        if let Some(m) = &self.metrics {
            m.xt_rejected_total.inc();
        }

        // Send abort vote directly to the publisher, bypassing send_vote()
        // which requires the XT to exist in pending state. In standalone mode
        // this is a no-op — there's no XT to track and no publisher to notify.
        if let Some(publisher) = &self.publisher {
            if publisher.is_connected() {
                if let Err(e) = publisher.send_vote(&msg.instance_id, false).await {
                    error!(instance_id, error = %e, "Failed to send reject vote to publisher");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::{ChainId, PeriodId};
    use compose_proto::{StartInstance, TransactionRequest, XtRequest};
    use tokio::sync::mpsc;

    use crate::coordinator::{DefaultCoordinator, VerificationConfig};

    fn start_instance(sequence_number: u64) -> StartInstance {
        StartInstance {
            instance_id: format!("xt-{sequence_number}").into_bytes(),
            period_id: 1,
            sequence_number,
            xt_request: Some(XtRequest {
                transaction_requests: vec![TransactionRequest {
                    chain_id: 77777,
                    transaction: vec![vec![sequence_number as u8]],
                }],
            }),
        }
    }

    #[tokio::test]
    async fn handle_start_instance_allows_multiple_local_xts() {
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
            state.period_initialized = true;
            state.current_period_id = PeriodId(1);
        }

        let (tx, _rx) = mpsc::channel::<String>(300);

        coordinator
            .handle_start_instance(&start_instance(1), &tx)
            .await
            .unwrap();
        coordinator
            .handle_start_instance(&start_instance(2), &tx)
            .await
            .unwrap();

        let state = coordinator.state.read().await;
        assert_eq!(state.pending.len(), 2);
        assert_eq!(state.last_sequence_num.0, 2);
    }
}
