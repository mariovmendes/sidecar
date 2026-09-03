//! Trait for controlling XT reservations inside the local builder.

use async_trait::async_trait;

use crate::error::CoordinatorError;

/// Client for synchronizing XT lifecycle events into the local builder.
#[async_trait]
pub trait XtBuilderClient: Send + Sync + 'static {
    /// Reserve the local XT transactions in the builder as locked.
    async fn submit_locked_xt(
        &self,
        instance_id: &str,
        period_id: u64,
        sequence_number: u64,
        transactions: Vec<Vec<u8>>,
    ) -> Result<(), CoordinatorError>;

    /// Submits transaction to be included without any prior reservation needed.
    async fn submit_tx(&self, tx: &[u8]) -> Result<(), CoordinatorError>;

    /// Followup on an instance by submitting the putInbox transaction.
    async fn submit_followup_xt(
        &self,
        instance_id: &str,
        put_inbox_transactions: Vec<Vec<u8>>,
    ) -> Result<(), CoordinatorError>;

    /// Release a previously reserved XT so the builder may execute it.
    ///
    /// `put_inbox_transactions` must already be signed and ordered before the
    /// XT's user transactions.
    async fn release_xt(
        &self,
        instance_id: &str,
        put_inbox_transactions: Vec<Vec<u8>>,
    ) -> Result<(), CoordinatorError>;

    /// Abort a previously reserved XT and free its nonce reservations.
    async fn abort_xt(&self, instance_id: &str) -> Result<(), CoordinatorError>;
}
