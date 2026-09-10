//! In-memory representation of a pending cross-chain transaction.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use alloy::primitives::Address;
use compose_mailbox::matching::{DependencyKey, MailboxMessageKey};
use compose_primitives::{
    ChainId, CrossRollupDependency, CrossRollupMessage, InstanceId, PeriodId, SequenceNumber,
};
use compose_proto::MailboxMessage;

/// A cross-chain transaction in flight, tracking its full lifecycle from
/// submission through simulation, voting, and decision.
#[derive(Debug, Clone)]
pub struct PendingXt {
    /// Instance identifier (cheap-clone `Arc<str>`).
    pub id: InstanceId,
    /// Raw instance ID bytes.
    pub instance_id: Vec<u8>,
    /// Period during which this XT was created.
    pub period_id: PeriodId,
    /// Sequence number within the period.
    pub sequence_num: SequenceNumber,

    /// Raw RLP-encoded transactions per chain.
    pub raw_txs: HashMap<ChainId, Vec<Vec<u8>>>,

    /// When the XT was created.
    pub created_at: Instant,
    /// When simulation completed.
    pub simulated_at: Option<Instant>,
    /// When a decision was recorded.
    pub decided_at: Option<Instant>,
    /// When the builder confirmed this XT was included in a block.
    pub confirmed_at: Option<Instant>,

    /// Final commit/abort decision.
    pub decision: Option<bool>,
    /// Whether the local vote was sent.
    pub vote_sent: bool,
    /// The local chain's vote.
    pub local_vote: Option<bool>,
    /// Votes received from peer sidecars.
    pub peer_votes: HashMap<ChainId, bool>,

    /// Origin chain for standalone-mode XTs.
    pub origin_chain: Option<ChainId>,
    /// Origin sequence number.
    pub origin_seq: SequenceNumber,

    /// Inbound CIRC messages waiting to be matched.
    pub pending_mailbox: Vec<MailboxMessage>,
    /// Already-sent outbound CIRC messages.
    pub sent_mailbox: Vec<MailboxMessage>,
    /// Dedup key set for `sent_mailbox`.
    pub sent_mailbox_keys: HashSet<MailboxMessageKey>,
    /// Cross-rollup dependencies discovered during simulation.
    pub dependencies: Vec<CrossRollupDependency>,
    /// Dedup set for `dependencies`, keyed by the mailbox key fields.
    pub dep_keys: HashSet<DependencyKey>,
    /// Dependencies that have been fulfilled.
    pub fulfilled_deps: Vec<CrossRollupDependency>,
    /// Dedup set for `fulfilled_deps`, keyed by the mailbox key fields.
    pub fulfilled_dep_keys: HashSet<DependencyKey>,
    /// Outbound messages from simulation.
    pub outbound_messages: Vec<CrossRollupMessage>,
    /// Cached sender address and nonce per chain, derived from the first raw tx.
    ///
    /// Populated at registration to avoid repeated ECDSA recovery on every
    /// builder lifecycle step. If recovery fails, the cache entry is omitted.
    pub sender_nonces: HashMap<ChainId, (Address, u64)>,
}

impl PendingXt {
    pub fn new(id: String, instance_id: Vec<u8>) -> Self {
        Self {
            id: id.into(),
            instance_id,
            period_id: PeriodId(0),
            sequence_num: SequenceNumber(0),
            raw_txs: HashMap::new(),
            created_at: Instant::now(),
            simulated_at: None,
            decided_at: None,
            confirmed_at: None,
            decision: None,
            vote_sent: false,
            local_vote: None,
            peer_votes: HashMap::new(),
            origin_chain: None,
            origin_seq: SequenceNumber(0),
            pending_mailbox: Vec::new(),
            sent_mailbox: Vec::new(),
            sent_mailbox_keys: HashSet::new(),
            dependencies: Vec::new(),
            dep_keys: HashSet::new(),
            fulfilled_deps: Vec::new(),
            fulfilled_dep_keys: HashSet::new(),
            outbound_messages: Vec::new(),
            sender_nonces: HashMap::new(),
        }
    }

    /// Whether this XT has received a commit/abort decision.
    pub fn is_decided(&self) -> bool {
        self.decision.is_some()
    }

    /// Whether this XT was committed.
    pub fn is_committed(&self) -> bool {
        self.decision == Some(true)
    }

    /// Record a commit/abort decision and free simulation memory that is no
    /// longer needed post-decision.
    pub fn record_decision(&mut self, decision: bool) {
        self.decision = Some(decision);
        self.decided_at = Some(Instant::now());
    }
}
