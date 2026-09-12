use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use compose_primitives::ChainId;
use compose_primitives_traits::{CoordinatorError, PublisherClient};
use compose_proto::{Confirmed, Vote, wire_message::Payload};
use compose_transport::traits::Transport;
use prost::Message;

/// Publisher connection implementing the `PublisherClient` trait.
///
/// Transport-agnostic: works with any `Transport` implementation (QUIC, TCP, etc.).
pub struct PublisherConnection {
    transport: Arc<dyn Transport>,
    chain_id: ChainId,
}

impl PublisherConnection {
    pub fn new(transport: Arc<dyn Transport>, chain_id: ChainId) -> Self {
        Self {
            transport,
            chain_id,
        }
    }

    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.transport
    }
}

impl std::fmt::Debug for PublisherConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublisherConnection")
            .field("connected", &self.transport.is_connected())
            .field("chain_id", &self.chain_id)
            .finish()
    }
}

#[async_trait]
impl PublisherClient for PublisherConnection {
    async fn connect(&self) -> Result<(), CoordinatorError> {
        self.transport
            .connect()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn connect_with_retry(&self) -> Result<(), CoordinatorError> {
        self.transport
            .connect_with_retry()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn disconnect(&self) -> Result<(), CoordinatorError> {
        self.transport
            .close()
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn send_vote(&self, instance_id: &[u8], vote: bool) -> Result<(), CoordinatorError> {
        let msg = compose_proto::WireMessage {
            sender_id: String::new(),
            payload: Some(Payload::Vote(Vote {
                instance_id: instance_id.to_vec(),
                chain_id: self.chain_id.0,
                vote,
            })),
        };

        self.transport
            .send(Bytes::from(msg.encode_to_vec()))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn send_confirmed(&self, instance_id: &[u8], chain_id: u64) -> Result<(), CoordinatorError> {
        let msg = compose_proto::WireMessage {
            sender_id: String::new(),
            payload: Some(Payload::Confirmed(Confirmed {
                instance_id: instance_id.to_vec(),
                chain_id,
            })),
        };

        self.transport
            .send(Bytes::from(msg.encode_to_vec()))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    async fn send_raw(&self, data: &[u8]) -> Result<(), CoordinatorError> {
        self.transport
            .send(Bytes::copy_from_slice(data))
            .await
            .map_err(|e| CoordinatorError::Other(e.to_string()))
    }

    fn is_connected(&self) -> bool {
        self.transport.is_connected()
    }
}
