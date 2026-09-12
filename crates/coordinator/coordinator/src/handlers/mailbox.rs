//! Inbound mailbox message handling and state updates.

use compose_mailbox::wire;
use compose_primitives::CrossRollupDependency;
use compose_proto::MailboxMessage;
use tracing::{debug, warn};

use crate::coordinator::ChunkStage::{WaitingForMessages, WaitingForProcessing};
use crate::coordinator::DefaultCoordinator;
use compose_primitives::xtflow;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Handle an ACK `CrossRollupDependency` reported by a peer sidecar right
    /// after it submitted the matching `receiveTokens`/`receiveETH`
    /// transaction (`POST /mailbox/ack`).
    ///
    /// Does not submit anything to the builder inline from the HTTP request
    /// path: records the dependency into `mailbox_messages` the same way an
    /// inbound `/mailbox` message is recorded, then enqueues a
    /// `TransactionChunk` at `WaitingForProcessing` so the chunk processor
    /// picks it up and builds/submits the matching `putInbox` from there,
    /// like every other chunk.
    pub async fn handle_ack_dependency(
        &self,
        instance_id: String,
        dependency: CrossRollupDependency,
    ) -> Result<(), CoordinatorError> {
        xtflow!(
            "ack_in",
            instance_id = instance_id,
            chain = self.chain_id,
            source_chain = dependency.source_chain_id,
            session = dependency.session_id,
            label = String::from_utf8_lossy(&dependency.label),
        );

        let mailbox_msg = MailboxMessage {
            instance_id: instance_id.clone().into_bytes(),
            source_chain: dependency.source_chain_id.0,
            destination_chain: dependency.dest_chain_id.0,
            sender: dependency.sender.as_slice().to_vec(),
            receiver: dependency.receiver.as_slice().to_vec(),
            label: String::from_utf8_lossy(&dependency.label).to_string(),
            payload: dependency.data.unwrap_or_default(),
            session_id: wire::encode_session_id(dependency.session_id),
        };

        let advanced = {
            let mut state = self.state.write().await;
            state
                .mailbox_messages
                .entry(instance_id.clone())
                .or_default()
                .push(mailbox_msg);

            state
                .inflight_chunks
                .get_mut(instance_id.as_str())
                .is_some_and(|chunk| {
                    if chunk.stage == WaitingForMessages {
                        chunk.stage = WaitingForProcessing;
                        true
                    } else {
                        false
                    }
                })
        };

        match (advanced, &self.chunk_sender) {
            (true, Some(sender)) => {
                if let Err(e) = sender.send(instance_id.clone()).await {
                    xtflow!("signal_failed", instance_id = instance_id, chain = self.chain_id, from = "ack_in", error = e);
                    warn!(instance_id, error = %e, "Failed to enqueue ack chunk for processing");
                } else {
                    xtflow!("signal_enqueued", instance_id = instance_id, chain = self.chain_id, from = "ack_in");
                }
            }
            (true, None) => {
                xtflow!("signal_failed", instance_id = instance_id, chain = self.chain_id, from = "ack_in", error = "no_chunk_sender");
                warn!(
                    instance_id,
                    "No chunk sender configured, ack recorded but not scheduled for processing"
                );
            }
            (false, _) => {
                // The ACK is recorded but nothing is waiting on it: either the
                // chunk has not reached WaitingForMessages yet (it will find
                // the message itself) or it already moved past it — in the
                // latter case nothing will ever re-dispatch this instance.
                xtflow!(
                    "ack_in_not_advanced",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    chunk_stage = self
                        .state
                        .read()
                        .await
                        .inflight_chunks
                        .get(instance_id.as_str())
                        .map(|c| format!("{:?}", c.stage))
                        .unwrap_or_else(|| "no_chunk".to_string()),
                );
                warn!(instance_id, "No inflight chunk found for ack, dropping");
            }
        }

        Ok(())
    }

    /// Handle an incoming CIRC message from a peer sidecar.
    pub async fn handle_mailbox_message(
        &self,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError> {
        let instance_id = hex::encode(&msg.instance_id);

        xtflow!(
            "mailbox_in",
            instance_id = instance_id,
            chain = self.chain_id,
            source_chain = msg.source_chain,
            dest_chain = msg.destination_chain,
            label = msg.label,
            payload_bytes = msg.payload.len(),
        );

        debug!(
            instance_id,
            source_chain = msg.source_chain,
            dest_chain = msg.destination_chain,
            label = %msg.label,
            "Received mailbox message from peer"
        );

        if let Some(queue) = &self.mailbox_queue {
            queue
                .record(msg)
                .await
                .map_err(|e| CoordinatorError::Mailbox(e.to_string()))?;
        }

        if let Some(m) = &self.metrics {
            m.circ_messages_received_total.inc();
        }

        // Record the message and, if the matching chunk is already sitting in
        // WaitingForMessages (registered via register_xt, waiting on exactly
        // this dependency), advance it and re-enqueue it without holding the
        // state lock across the channel send below.
        let advanced = {
            let mut state = self.state.write().await;
            state
                .mailbox_messages
                .entry(instance_id.clone())
                .or_default()
                .push(msg.clone());

            state
                .inflight_chunks
                .get_mut(instance_id.as_str())
                .is_some_and(|chunk| {
                    if chunk.stage == WaitingForMessages {
                        chunk.stage = WaitingForProcessing;
                        true
                    } else {
                        false
                    }
                })
        };

        match (advanced, &self.chunk_sender) {
            (true, Some(sender)) => {
                if let Err(e) = sender.send(instance_id.clone()).await {
                    xtflow!("signal_failed", instance_id = instance_id, chain = self.chain_id, from = "mailbox_in", error = e);
                    warn!(instance_id, error = %e, "Failed to enqueue chunk for processing");
                } else {
                    xtflow!("signal_enqueued", instance_id = instance_id, chain = self.chain_id, from = "mailbox_in");
                }
            }
            (true, None) => {
                xtflow!("signal_failed", instance_id = instance_id, chain = self.chain_id, from = "mailbox_in", error = "no_chunk_sender");
                warn!(
                    instance_id,
                    "No chunk sender configured, message recorded but not scheduled for processing"
                );
            }
            (false, _) => {
                xtflow!(
                    "mailbox_in_not_advanced",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    label = msg.label,
                    chunk_stage = self
                        .state
                        .read()
                        .await
                        .inflight_chunks
                        .get(instance_id.as_str())
                        .map(|c| format!("{:?}", c.stage))
                        .unwrap_or_else(|| "no_chunk".to_string()),
                );
                debug!(
                    instance_id,
                    "No inflight chunk waiting on this instance yet"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use alloy_rpc_types_eth::state::StateOverride;
    use async_trait::async_trait;
    use compose_primitives::{ChainId, CrossRollupDependency, SimulationResult};
    use compose_proto::MailboxMessage;
    use compose_simulation::error::SimulationError;
    use compose_simulation::traits::Simulator;
    use crate::coordinator::{DefaultCoordinator, TransactionChunk};
    use crate::model::pending_xt::PendingXt;

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

    #[tokio::test]
    async fn mailbox_message_stored_when_xt_not_yet_registered() {
        // Reproduce the race where the mailbox message arrives before the
        // forwarded XT is registered in mailbox_index.
        let coordinator = DefaultCoordinator::new(
            ChainId(88888),
            None,
            None,
            None,
            None,
            None,
            1_000,
        );

        let instance_id = "xt-77777-2".to_string();

        // Send the mailbox message BEFORE the XT is registered.
        let msg = MailboxMessage {
            instance_id: instance_id.as_bytes().to_vec(),
            source_chain: 77777,
            destination_chain: 88888,
            label: "SEND".to_string(),
            ..Default::default()
        };
        coordinator.handle_mailbox_message(&msg).await.unwrap();

        // Verify the message has been saved to mailbox_messages
        {
            let state = coordinator.state.read().await;
            let wrapped_mailbox = state.mailbox_messages.get(&hex::encode(instance_id.as_bytes()));
            assert_eq!(wrapped_mailbox.is_some(),true);
            let unwrapped_mailbox = wrapped_mailbox.unwrap();
            assert_eq!(unwrapped_mailbox.len(), 1);
            assert_eq!(unwrapped_mailbox.get(0).unwrap().label, "SEND");
        }
    }

    #[tokio::test]
    async fn mailbox_message_attaches_to_pending_xt_by_raw_instance_id() {
        let simulator = Arc::new(StubSimulator { succeed: true });
        let coordinator = DefaultCoordinator::new(
            ChainId(88888),
            Some(simulator),
            None,
            None,
            None,
            None,
            1_000,
        );

        let instance_id = "xt-77777-1".to_string();
        let mut xt = PendingXt::new(instance_id.clone(), instance_id.as_bytes().to_vec());

        {
            let mut state = coordinator.state.write().await;
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xde, 0xad]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .register_xt(&mut TransactionChunk {
                instance_id: "xt-77777-11".to_string(),
                ..Default::default()
            })
            .await;

        let msg = MailboxMessage {
            instance_id: instance_id.as_bytes().to_vec(),
            source_chain: 77777,
            destination_chain: 88888,
            label: "SEND".to_string(),
            ..Default::default()
        };

        coordinator.handle_mailbox_message(&msg).await.unwrap();

        let state = coordinator.state.read().await;
        let wrapped_mailbox = state.mailbox_messages.get(&hex::encode(instance_id.as_bytes()));
        assert_eq!(wrapped_mailbox.is_some(),true);
        let unwrapped_mailbox = wrapped_mailbox.unwrap();
        assert_eq!(unwrapped_mailbox.len(), 1);
        assert_eq!(unwrapped_mailbox.get(0).unwrap().label, "SEND");
    }
}
