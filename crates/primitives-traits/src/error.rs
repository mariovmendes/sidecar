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

    /// Whether the builder refused because it no longer knows the instance.
    ///
    /// `ethera_abortXt` removes an instance together with every transaction of
    /// it that had not executed yet, so an "unknown instance" answer means
    /// there is nothing left on that chain to finalize or compensate — and
    /// nothing a retry could change. Matched on the message because the RPC
    /// reports it as a generic `InvalidParams` (-32602).
    pub fn is_unknown_instance(&self) -> bool {
        match self {
            Self::BuilderRejected { message, .. } => message.contains("unknown instance"),
            _ => false,
        }
    }

    /// The nonce the builder said it expected, when it refused a transaction
    /// for a nonce gap.
    ///
    /// `validate_nonce_sequence` answers with the exact value it wants
    /// ("nonce gap for 0x..: expected 5146, got 5155"), which is the only
    /// authoritative view of the builder's pool. Recycling the refused nonce
    /// instead just re-offers the same wrong value on the next attempt, so a
    /// desync — a mass `ethera_abortXt` retracting reservations, say — becomes
    /// a livelock that never resolves. Snapping to this value fixes it on the
    /// first retry, whatever caused it.
    pub fn expected_nonce(&self) -> Option<u64> {
        let Self::BuilderRejected { message, .. } = self else {
            return None;
        };
        let rest = message.split("expected ").nth(1)?;
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::CoordinatorError;

    #[test]
    fn reads_the_expected_nonce_out_of_a_gap_rejection() {
        let gap = CoordinatorError::BuilderRejected {
            method: "ethera_releaseXt".to_string(),
            message: "code -32602: nonce gap for 0xabc: expected 5146, got 5155".to_string(),
        };
        assert_eq!(gap.expected_nonce(), Some(5146));

        // Anything that is not a nonce gap must not produce a target.
        let unknown = CoordinatorError::BuilderRejected {
            method: "ethera_releaseXt".to_string(),
            message: "code -32602: unknown instance abc".to_string(),
        };
        assert_eq!(unknown.expected_nonce(), None);
        assert_eq!(
            CoordinatorError::BuilderControl("connection reset".to_string()).expected_nonce(),
            None
        );
    }

    #[test]
    fn classifies_builder_answers() {
        // Exactly what op-rbuilder returns when a period tick already dropped
        // the instance: compensation is pointless, not failed.
        let dropped = CoordinatorError::BuilderRejected {
            method: "ethera_releaseXt".to_string(),
            message: "code -32602: unknown instance afe806373dc7cc30".to_string(),
        };
        assert!(dropped.is_builder_rejection());
        assert!(dropped.is_unknown_instance());

        // A nonce gap is a real rejection: the nonce is recyclable, but the
        // instance still exists and the attempt must be retried.
        let gap = CoordinatorError::BuilderRejected {
            method: "ethera_releaseXt".to_string(),
            message: "code -32602: nonce gap for 0xabc: expected 5, got 6".to_string(),
        };
        assert!(gap.is_builder_rejection());
        assert!(!gap.is_unknown_instance());

        // A transport failure says nothing about whether it landed.
        let transport = CoordinatorError::BuilderControl("connection reset".to_string());
        assert!(!transport.is_builder_rejection());
        assert!(!transport.is_unknown_instance());
    }
}
