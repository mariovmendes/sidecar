//! Trait for constructing signed `ComposeL2ToL2Bridge` finalize/compensate
//! transactions (`sendConfirm`, `sendAbortToken`, `sendAbortETH`,
//! `recvConfirmToken`, `recvAbortToken`, `recvConfirmETH`, `recvAbortETH`).

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use compose_primitives::{ChainId, CrossRollupDependency};

use crate::error::CoordinatorError;

/// Parameters for `sendAbortToken`, which refunds the original sender the
/// locked/burned ERC20 or CET amount. Distinct from `CrossRollupDependency`
/// because the contract call needs `token`/`amount`, which aren't part of a
/// message header.
#[derive(Debug, Clone)]
pub struct SendAbortTokenParams {
    pub chain_dest: ChainId,
    pub token: Address,
    pub sender: Address,
    pub receiver: Address,
    pub amount: U256,
    pub session_id: U256,
}

/// Parameters for `sendAbortETH`, which refunds the original sender the
/// locked ETH amount.
#[derive(Debug, Clone)]
pub struct SendAbortEthParams {
    pub chain_dest: ChainId,
    pub sender: Address,
    pub receiver: Address,
    pub amount: U256,
    pub session_id: U256,
}

/// Builder for signed `ComposeL2ToL2Bridge` transactions.
#[async_trait]
pub trait L2BridgeTxBuilder: Send + Sync + 'static {
    /// Address of the coordinator signer used for these calls (the same
    /// signer used for `putInbox`).
    fn signer_address(&self) -> Address;

    /// Address of the `ComposeL2ToL2Bridge` contract itself. Needed on the
    /// sender side to reconstruct the SEND header's `sender` field, which is
    /// always the bridge contract's own address (deployed at the same
    /// address on every chain via CREATE2), not the end user.
    fn contract_address(&self) -> Address;

    /// Return the coordinator signer's canonical on-chain nonce.
    async fn canonical_nonce_at(&self) -> Result<u64, CoordinatorError>;

    /// Build a signed `sendConfirm(sendHeader)` transaction. `header` must be
    /// the exact header used when the original SEND message was written.
    async fn build_send_confirm_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;

    /// Build a signed `sendAbortToken(...)` transaction.
    async fn build_send_abort_token_tx(
        &self,
        params: &SendAbortTokenParams,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;

    /// Build a signed `sendAbortETH(...)` transaction.
    async fn build_send_abort_eth_tx(
        &self,
        params: &SendAbortEthParams,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;

    /// Build a signed `recvConfirmToken(msgHeader)` transaction. `header` is
    /// the header of the original inbound SEND message.
    async fn build_recv_confirm_token_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;

    /// Build a signed `recvAbortToken(msgHeader)` transaction.
    async fn build_recv_abort_token_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;

    /// Build a signed `recvConfirmETH(msgHeader)` transaction.
    async fn build_recv_confirm_eth_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;

    /// Build a signed `recvAbortETH(msgHeader)` transaction.
    async fn build_recv_abort_eth_tx(
        &self,
        header: &CrossRollupDependency,
        nonce: u64,
    ) -> Result<Vec<u8>, CoordinatorError>;
}