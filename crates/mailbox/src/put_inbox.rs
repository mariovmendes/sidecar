//! Signed `putInbox` transaction builder.

use alloy::eips::{BlockId, Encodable2718};
use alloy::network::TransactionBuilder;
use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use compose_primitives::{ChainId, CrossRollupDependency};
use compose_primitives_traits::{CoordinatorError, PutInboxBuilder};
use reqwest::Url;

use crate::abi;

const PUT_INBOX_GAS_LIMIT: u64 = 2_000_000;

/// Builds signed `putInbox` transactions for local dependency fulfillment.
#[derive(Clone)]
pub struct PutInboxTxBuilder {
    chain_id: ChainId,
    rpc_url: String,
    provider: DynProvider,
    mailbox_address: Address,
    signer: PrivateKeySigner,
    signer_address: Address,
}

impl std::fmt::Debug for PutInboxTxBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutInboxTxBuilder")
            .field("chain_id", &self.chain_id)
            .field("mailbox_address", &self.mailbox_address)
            .field("signer_address", &self.signer_address)
            .finish()
    }
}

impl PutInboxTxBuilder {
    pub fn new(
        chain_id: ChainId,
        rpc_url: String,
        mailbox_address: String,
        coordinator_key: String,
    ) -> Result<Self, CoordinatorError> {
        let mailbox_address: Address = mailbox_address
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid mailbox address: {e}")))?;

        let key = if coordinator_key.starts_with("0x") {
            coordinator_key
        } else {
            format!("0x{coordinator_key}")
        };
        let signer: PrivateKeySigner = key
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid coordinator key: {e}")))?;
        let signer_address = signer.address();
        let rpc_url: Url = rpc_url
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid builder rpc url: {e}")))?;
        let provider = ProviderBuilder::new()
            .connect_http(rpc_url.clone())
            .erased();

        Ok(Self {
            chain_id,
            rpc_url: rpc_url.to_string(),
            provider,
            mailbox_address,
            signer,
            signer_address,
        })
    }
}

#[async_trait]
impl PutInboxBuilder for PutInboxTxBuilder {
    fn signer_address(&self) -> Address {
        self.signer_address
    }

    async fn canonical_nonce_at(&self) -> Result<u64, CoordinatorError> {
        self.provider
            .get_transaction_count(self.signer_address)
            .block_id(BlockId::latest())
            .await
            .map_err(|e| CoordinatorError::Nonce(format!("get canonical nonce: {e}")))
    }

    async fn build_put_inbox_tx_with_nonce(
        &self,
        dep: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let data = dep.data.as_deref().unwrap_or_default();
        let calldata = abi::encode_put_inbox(
            dep.source_chain_id.0,
            dep.sender,
            dep.receiver,
            dep.session_id,
            &dep.label,
            data,
        )
        .map_err(|e| CoordinatorError::Mailbox(format!("encode putInbox calldata: {e}")))?;

        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_remove_inbox_tx_with_nonce(
        &self,
        dep: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let data = dep.data.as_deref().unwrap_or_default();
        let calldata = abi::encode_remove_inbox(
            dep.source_chain_id.0,
            dep.sender,
            dep.receiver,
            dep.session_id,
            &dep.label,
            data,
        )
        .map_err(|e| CoordinatorError::Mailbox(format!("encode removeInbox calldata: {e}")))?;

        self.build_signed_tx(calldata, nonce).await
    }
}

impl PutInboxTxBuilder {
    async fn build_signed_tx(&self, calldata: Vec<u8>, nonce: u64) -> Result<Vec<u8>, CoordinatorError> {
        let tx = TransactionRequest::default()
            .with_from(self.signer_address)
            .with_to(self.mailbox_address)
            .with_chain_id(self.chain_id.0)
            .with_nonce(nonce)
            .gas_limit(PUT_INBOX_GAS_LIMIT)
            .with_input(calldata);

        let rpc_url: Url = self
            .rpc_url
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid builder rpc url: {e}")))?;
        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect_http(rpc_url);
        let signed = provider
            .fill(tx)
            .await
            .map_err(|e| CoordinatorError::Other(format!("fill mailbox tx: {e}")))?
            .try_into_envelope()
            .map_err(|e| {
                CoordinatorError::Other(format!(
                    "fill mailbox tx returned unsigned transaction: {:?}",
                    e.into_inner()
                ))
            })?;

        Ok(signed.encoded_2718())
    }
}
