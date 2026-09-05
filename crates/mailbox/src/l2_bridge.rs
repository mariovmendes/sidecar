//! Signed `ComposeL2ToL2Bridge` finalize/compensate transaction builder.

use alloy::eips::{BlockId, Encodable2718};
use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolCall;
use async_trait::async_trait;
use compose_primitives::CrossRollupDependency;
use compose_primitives_traits::{
    CoordinatorError, L2BridgeTxBuilder, SendAbortEthParams, SendAbortTokenParams,
};
use reqwest::Url;

use crate::contract::{
    recvAbortETHCall, recvAbortTokenCall, recvConfirmETHCall, recvConfirmTokenCall,
    sendAbortETHCall, sendAbortTokenCall, sendConfirmCall, MessageHeader,
};

const L2_BRIDGE_GAS_LIMIT: u64 = 2_000_000;

fn header_from_dependency(dep: &CrossRollupDependency) -> MessageHeader {
    MessageHeader {
        chainSrc: U256::from(dep.source_chain_id.0),
        chainDest: U256::from(dep.dest_chain_id.0),
        sender: dep.sender,
        receiver: dep.receiver,
        sessionId: dep.session_id,
        label: String::from_utf8_lossy(&dep.label).to_string(),
    }
}

/// Builds signed `ComposeL2ToL2Bridge` finalize/compensate transactions,
/// using the same coordinator signer as `PutInboxTxBuilder`.
#[derive(Clone)]
pub struct L2BridgeContractTxBuilder {
    chain_id: u64,
    rpc_url: String,
    provider: DynProvider,
    bridge_address: Address,
    signer: PrivateKeySigner,
    signer_address: Address,
}

impl std::fmt::Debug for L2BridgeContractTxBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L2BridgeContractTxBuilder")
            .field("chain_id", &self.chain_id)
            .field("bridge_address", &self.bridge_address)
            .field("signer_address", &self.signer_address)
            .finish()
    }
}

impl L2BridgeContractTxBuilder {
    pub fn new(
        chain_id: u64,
        rpc_url: String,
        bridge_address: String,
        coordinator_key: String,
    ) -> Result<Self, CoordinatorError> {
        let bridge_address: Address = bridge_address
            .parse()
            .map_err(|e| CoordinatorError::Other(format!("invalid bridge address: {e}")))?;

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
            bridge_address,
            signer,
            signer_address,
        })
    }

    async fn build_signed_tx(&self, calldata: Vec<u8>, nonce: u64) -> Result<Vec<u8>, CoordinatorError> {
        let tx = TransactionRequest::default()
            .with_from(self.signer_address)
            .with_to(self.bridge_address)
            .with_chain_id(self.chain_id)
            .with_nonce(nonce)
            .gas_limit(L2_BRIDGE_GAS_LIMIT)
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
            .map_err(|e| CoordinatorError::Other(format!("fill l2 bridge tx: {e}")))?
            .try_into_envelope()
            .map_err(|e| {
                CoordinatorError::Other(format!(
                    "fill l2 bridge tx returned unsigned transaction: {:?}",
                    e.into_inner()
                ))
            })?;

        Ok(signed.encoded_2718())
    }
}

#[async_trait]
impl L2BridgeTxBuilder for L2BridgeContractTxBuilder {
    fn signer_address(&self) -> Address {
        self.signer_address
    }

    fn contract_address(&self) -> Address {
        self.bridge_address
    }

    /// The nonce the *builder* will accept next, not the one the chain is at.
    ///
    /// op-rbuilder's `eth_getTransactionCount(addr, "pending")` returns its
    /// `true_next_nonce` — on-chain, plus its mempool, plus the XT pool's
    /// reservations — which is exactly the value `validate_nonce_sequence`
    /// compares a submission against. Asking for `latest` instead desyncs the
    /// two views: when `ethera_abortXt` retracts an instance the builder gives
    /// its reserved coordinator nonces back, and a sidecar reconciling against
    /// the chain can neither see that nor move down to it, so every later
    /// submission is refused for a nonce gap.
    async fn canonical_nonce_at(&self) -> Result<u64, CoordinatorError> {
        self.provider
            .get_transaction_count(self.signer_address)
            .block_id(BlockId::pending())
            .await
            .map_err(|e| CoordinatorError::Nonce(format!("get canonical nonce: {e}")))
    }

    async fn build_send_confirm_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = sendConfirmCall {
            sendHeader: header_from_dependency(header),
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_send_abort_token_tx(
        &self,
        params: &SendAbortTokenParams,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = sendAbortTokenCall {
            chainDest: U256::from(params.chain_dest.0),
            token: params.token,
            sender: params.sender,
            receiver: params.receiver,
            amount: params.amount,
            sessionId: params.session_id,
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_send_abort_eth_tx(
        &self,
        params: &SendAbortEthParams,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = sendAbortETHCall {
            chainDest: U256::from(params.chain_dest.0),
            sender: params.sender,
            receiver: params.receiver,
            amount: params.amount,
            sessionId: params.session_id,
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_recv_confirm_token_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = recvConfirmTokenCall {
            msgHeader: header_from_dependency(header),
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_recv_abort_token_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = recvAbortTokenCall {
            msgHeader: header_from_dependency(header),
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_recv_confirm_eth_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = recvConfirmETHCall {
            msgHeader: header_from_dependency(header),
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }

    async fn build_recv_abort_eth_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError> {
        let calldata = recvAbortETHCall {
            msgHeader: header_from_dependency(header),
        }
        .abi_encode();
        self.build_signed_tx(calldata, nonce).await
    }
}