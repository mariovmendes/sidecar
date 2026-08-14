//! Publisher message dispatch for inbound control-plane events.

use std::sync::Arc;

use bytes::Bytes;
use compose_coordinator::coordinator::{DefaultCoordinator, TransactionChunk};
use compose_primitives::{PeriodId, SuperblockNumber};
use compose_proto::wire_message::Payload;
use prost::Message;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, warn};

/// Dispatch an inbound protobuf message from the publisher connection.
pub async fn handle_publisher_message(coordinator: Arc<DefaultCoordinator>, data: Bytes, sender: Sender<TransactionChunk>) {
    let msg = match compose_proto::WireMessage::decode(data) {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "Failed to decode publisher message");
            return;
        }
    };

    match msg.payload {
        Some(Payload::Decided(decided)) => {
            let instance_id = hex::encode(&decided.instance_id);
            if let Err(e) = coordinator
                .on_decision(&instance_id, decided.decision)
                .await
            {
                error!(error = %e, instance_id, "Failed to handle decision");
            }
        }
        Some(Payload::StartInstance(start_instance)) => {
            if let Err(e) = coordinator.handle_start_instance(&start_instance, &sender).await {
                error!(error = %e, "Failed to handle StartInstance");
            }
            drop(sender);
        }
        Some(Payload::StartPeriod(start_period)) => {
            if let Err(e) = coordinator
                .handle_start_period(
                    PeriodId(start_period.period_id),
                    SuperblockNumber(start_period.superblock_number),
                )
                .await
            {
                error!(error = %e, "Failed to handle StartPeriod");
            }
        }
        Some(Payload::MailboxMessage(mailbox_msg)) => {
            if let Err(e) = coordinator.handle_mailbox_message(&mailbox_msg).await {
                error!(error = %e, "Failed to handle MailboxMessage");
            }
        }
        Some(Payload::Rollback(rollback)) => {
            if let Err(e) = coordinator
                .handle_rollback(
                    PeriodId(rollback.period_id),
                    rollback.last_finalized_superblock_number,
                    &rollback.last_finalized_superblock_hash,
                )
                .await
            {
                error!(error = %e, "Failed to handle Rollback");
            }
        }
        Some(Payload::Vote(_)) => {
            debug!("Received vote from publisher (ignored by sidecar)");
        }
        Some(Payload::Confirmed(_)) =>{
            debug!("Confirmed the inclusion of a cross-chain transaction");
        }
        other => {
            warn!(?other, "Unhandled publisher message type");
        }
    }
}
