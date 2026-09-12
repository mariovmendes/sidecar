//! Mailbox sender trait for CIRC message delivery.

use async_trait::async_trait;
use compose_primitives::{ChainId, CrossRollupDependency};
use compose_proto::MailboxMessage;

use crate::error::CoordinatorError;

/// Sender for CIRC mailbox messages to peer sidecars.
#[async_trait]
pub trait MailboxSender: Send + Sync + 'static {
    async fn send(
        &self,
        dest_chain_id: ChainId,
        msg: &MailboxMessage,
    ) -> Result<(), CoordinatorError>;

    /// Report a dependency (a decoded `writeMessage`) to the peer sidecar on
    /// `dest_chain_id` so it can build and submit the matching `putInbox`
    /// transaction on its own chain ahead of the recipient's receive call.
    async fn send_ack(
        &self,
        dest_chain_id: ChainId,
        instance_id: &str,
        dependency: &CrossRollupDependency,
    ) -> Result<(), CoordinatorError>;
}
