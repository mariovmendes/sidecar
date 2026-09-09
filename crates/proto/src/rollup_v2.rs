/// Hand-written protobuf wire message types aligned with
/// `specs/compose/proto/protocol_messages.proto`.
use prost::Message;

// Connection-level authentication handshake messages.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct HandshakeRequest {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub public_key: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub signature: Vec<u8>,
    #[prost(string, tag = "4")]
    pub client_id: String,
    #[prost(bytes = "vec", tag = "5")]
    pub nonce: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct HandshakeResponse {
    #[prost(bool, tag = "1")]
    pub accepted: bool,
    #[prost(string, tag = "2")]
    pub error: String,
    #[prost(string, tag = "3")]
    pub session_id: String,
}

// Connection health check messages.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Ping {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Pong {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

// SCP core payloads.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct TransactionRequest {
    #[prost(uint64, tag = "1")]
    pub chain_id: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub transaction: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct XtRequest {
    #[prost(message, repeated, tag = "1")]
    pub transaction_requests: Vec<TransactionRequest>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StartInstance {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub period_id: u64,
    #[prost(uint64, tag = "3")]
    pub sequence_number: u64,
    #[prost(message, optional, tag = "4")]
    pub xt_request: Option<XtRequest>,
}

impl StartInstance {
    /// Return the hex-encoded instance ID.
    pub fn instance_id_hex(&self) -> String {
        hex::encode(&self.instance_id)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Vote {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub chain_id: u64,
    #[prost(bool, tag = "3")]
    pub vote: bool,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Decided {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Confirmed {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub chain_id: u64,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct MailboxMessage {
    /// 32-byte big-endian session ID matching the contract's uint256 sessionId.
    #[prost(bytes = "vec", tag = "1")]
    pub session_id: Vec<u8>,
    /// Instance ID assigned by the SP; used for routing, not part of the on-chain key.
    #[prost(bytes = "vec", tag = "2")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub source_chain: u64,
    #[prost(uint64, tag = "4")]
    pub destination_chain: u64,
    /// msg.sender of the writeMessage call (bridge contract on source chain).
    #[prost(bytes = "vec", tag = "5")]
    pub sender: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    pub receiver: Vec<u8>,
    #[prost(string, tag = "7")]
    pub label: String,
    /// ABI-encoded message payload (the bytes field of the on-chain Message struct).
    #[prost(bytes = "vec", tag = "8")]
    pub payload: Vec<u8>,
}

// SBCP payloads.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct StartPeriod {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub superblock_number: u64,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Rollback {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub last_finalized_superblock_number: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub last_finalized_superblock_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Proof {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub superblock_number: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub proof_data: Vec<u8>,
}

// CDCP payloads.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeDecided {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct WsDecided {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

// Top-level wrapper.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct WireMessage {
    #[prost(string, tag = "1")]
    pub sender_id: String,
    #[prost(
        oneof = "wire_message::Payload",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
    )]
    pub payload: Option<wire_message::Payload>,
}

pub mod wire_message {
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Payload {
        #[prost(message, tag = "2")]
        HandshakeRequest(super::HandshakeRequest),
        #[prost(message, tag = "3")]
        HandshakeResponse(super::HandshakeResponse),
        #[prost(message, tag = "4")]
        Ping(super::Ping),
        #[prost(message, tag = "5")]
        Pong(super::Pong),

        #[prost(message, tag = "6")]
        XtRequest(super::XtRequest),
        #[prost(message, tag = "7")]
        StartInstance(super::StartInstance),
        #[prost(message, tag = "8")]
        Vote(super::Vote),
        #[prost(message, tag = "9")]
        Decided(super::Decided),
        #[prost(message, tag = "10")]
        MailboxMessage(super::MailboxMessage),

        #[prost(message, tag = "11")]
        StartPeriod(super::StartPeriod),
        #[prost(message, tag = "12")]
        Rollback(super::Rollback),
        #[prost(message, tag = "13")]
        Proof(super::Proof),

        #[prost(message, tag = "14")]
        NativeDecided(super::NativeDecided),
        #[prost(message, tag = "15")]
        WsDecided(super::WsDecided),
        #[prost(message, tag = "16")]
        Confirmed(super::Confirmed),
    }
}
