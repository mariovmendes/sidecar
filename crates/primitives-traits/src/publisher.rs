//! Publisher client trait for SP communication.

use async_trait::async_trait;

use crate::error::CoordinatorError;

/// Client for communicating with the shared publisher (SP).
#[async_trait]
pub trait PublisherClient: Send + Sync + 'static {
    /// Establish a connection to the publisher.
    async fn connect(&self) -> Result<(), CoordinatorError>;

    /// Connect with automatic retry logic.
    async fn connect_with_retry(&self) -> Result<(), CoordinatorError>;

    /// Disconnect from the publisher.
    async fn disconnect(&self) -> Result<(), CoordinatorError>;

    /// Send a vote for a cross-chain transaction instance.
    async fn send_vote(&self, instance_id: &[u8], vote: bool) -> Result<(), CoordinatorError>;

    /// Send confirmation of cross-chain transaction inclusion.
    async fn send_confirmed(&self, instance_id: &[u8], chain_id: u64) -> Result<(), CoordinatorError>;

    /// Send raw protobuf-encoded data to the publisher.
    async fn send_raw(&self, data: &[u8]) -> Result<(), CoordinatorError>;

    /// Whether the publisher connection is active.
    fn is_connected(&self) -> bool;
}
