//! Coordinator error definitions.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("coordinator already running")]
    AlreadyRunning,

    #[error("chunk sender not set")]
    ChunkSenderNotSet(String),

    #[error("enqueue to chunk sender error")]
    QueueError(String),

    #[error("function called at wrong stage of chunk")]
    WrongChunkStage(String),

    #[error("coordinator not running")]
    NotRunning,

    #[error("instance not found: {0}")]
    InstanceNotFound(String),

    #[error("instance already pending: {0}")]
    InstanceAlreadyPending(String),

    #[error("period not initialized")]
    PeriodNotInitialized,

    #[error("period mismatch")]
    PeriodMismatch,

    #[error("stale sequence number")]
    StaleSequence,

    #[error("no transactions provided")]
    NoTransactions,

    #[error("publisher not connected")]
    PublisherNotConnected,

    #[error("transaction decode error: {0}")]
    TransactionDecode(String),

    #[error("simulation error: {0}")]
    Simulation(String),

    #[error("mailbox error: {0}")]
    Mailbox(String),

    #[error("nonce error: {0}")]
    Nonce(String),

    #[error("put inbox builder not configured")]
    PutInboxNotConfigured,

    /// Transport-level failure talking to the builder: timeout, connection
    /// error, malformed response. The transaction may or may not have been
    /// accepted, so its nonce must be treated as consumed.
    #[error("builder control error: {0}")]
    BuilderControl(String),

    /// The builder answered with a JSON-RPC error, so the transaction was
    /// definitely *not* accepted into its pool and the nonce it reserved can
    /// safely be recycled.
    #[error("builder rejected {method}: {message}")]
    BuilderRejected { method: String, message: String },

    #[error("timeout waiting for CIRC from chain {0}")]
    CircTimeout(u64),

    #[error("too many pending instances (limit: {0})")]
    TooManyPendingInstances(usize),

    #[error("{0}")]
    Other(String),
}

impl CoordinatorError {
    /// Whether the builder definitively refused the transaction, so the nonce
    /// it reserved was never used and can be handed out again.
    pub fn is_builder_rejection(&self) -> bool {
        matches!(self, Self::BuilderRejected { .. })
    }
}
