//! Inbound mailbox message handling and state updates.

use compose_mailbox::wire;
use compose_primitives::CrossRollupDependency;
use compose_proto::MailboxMessage;
use tracing::{debug, warn};

use crate::coordinator::ChunkStage::{WaitingForMessages, WaitingForProcessing};
use crate::coordinator::DefaultCoordinator;
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
                    warn!(instance_id, error = %e, "Failed to enqueue ack chunk for processing");
                }
            }
            (true, None) => {
                warn!(
                    instance_id,
                    "No chunk sender configured, ack recorded but not scheduled for processing"
                );
            }
            (false, _) => {
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
                    warn!(instance_id, error = %e, "Failed to enqueue chunk for processing");
                }
            }
            (true, None) => {
                warn!(
                    instance_id,
                    "No chunk sender configured, message recorded but not scheduled for processing"
                );
            }
            (false, _) => {
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
    use compose_primitives::ChainId;
    use compose_proto::MailboxMessage;

    use crate::coordinator::{DefaultCoordinator, VerificationConfig};
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn mailbox_message_buffered_when_xt_not_yet_registered() {
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
            VerificationConfig::default(),
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

        // Verify the message is in the buffer, not lost.
        {
            let state = coordinator.state.read().await;
            let buffered = state
                .mailbox_buffer
                .get(instance_id.as_bytes())
                .expect("message should be buffered");
            assert_eq!(buffered.len(), 1);
            assert_eq!(buffered[0].label, "SEND");
        }

        // Now register the XT via handle_forwarded_xt.
        use std::collections::HashMap;
        coordinator
            .handle_forwarded_xt(
                &instance_id,
                HashMap::from([(ChainId(88888), vec![vec![1]])]),
                ChainId(77777),
                compose_primitives::SequenceNumber(1),
            )
            .await
            .unwrap();

        // Buffer should be drained and message attached to the XT.
        let state = coordinator.state.read().await;
        assert!(
            !state.mailbox_buffer.contains_key(instance_id.as_bytes()),
            "buffer should be empty after XT registration"
        );
        let xt = state.pending.get(instance_id.as_str()).unwrap();
        assert_eq!(xt.pending_mailbox.len(), 1);
        assert_eq!(xt.pending_mailbox[0].label, "SEND");
    }

    #[tokio::test]
    async fn mailbox_message_attaches_to_pending_xt_by_raw_instance_id() {
        let coordinator = DefaultCoordinator::new(
            ChainId(88888),
            None,
            None,
            None,
            None,
            None,
            1_000,
            VerificationConfig::default(),
        );

        let instance_id = "xt-77777-1".to_string();
        let xt = PendingXt::new(instance_id.clone(), instance_id.as_bytes().to_vec());

        {
            let mut state = coordinator.state.write().await;
            state
                .mailbox_index
                .insert(instance_id.as_bytes().to_vec(), xt.id.clone());
            state.pending.insert(xt.id.clone(), xt);
        }

        let msg = MailboxMessage {
            instance_id: instance_id.as_bytes().to_vec(),
            source_chain: 77777,
            destination_chain: 88888,
            label: "SEND".to_string(),
            ..Default::default()
        };

        coordinator.handle_mailbox_message(&msg).await.unwrap();

        let state = coordinator.state.read().await;
        let updated = state.pending.get(instance_id.as_str()).unwrap();

        assert_eq!(updated.pending_mailbox.len(), 1);
        assert_eq!(updated.pending_mailbox[0].label, "SEND");
    }
}
