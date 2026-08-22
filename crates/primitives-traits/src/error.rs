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

    #[error("builder control error: {0}")]
    BuilderControl(String),

    #[error("timeout waiting for CIRC from chain {0}")]
    CircTimeout(u64),

    #[error("too many pending instances (limit: {0})")]
    TooManyPendingInstances(usize),

    #[error("{0}")]
    Other(String),
}
