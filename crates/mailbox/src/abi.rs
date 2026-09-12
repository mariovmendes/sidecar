//! Mailbox ABI encoding utilities.

use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::contract::{putInboxCall, removeInboxCall};
use crate::error::MailboxError;

/// Encode a `putInbox` call for the `UniversalBridgeMailbox` contract.
pub fn encode_put_inbox(
    source_chain_id: u64,
    sender: Address,
    receiver: Address,
    session_id: U256,
    label: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, MailboxError> {
    let call = putInboxCall {
        chainMessageSender: U256::from(source_chain_id),
        sender,
        receiver,
        sessionId: session_id,
        label: String::from_utf8_lossy(label).to_string(),
        data: data.to_vec().into(),
    };
    Ok(call.abi_encode())
}

/// Encode a `removeInbox` call for the `UniversalBridgeMailbox` contract.
/// `data` must exactly match the payload originally passed to `putInbox` for
/// this key — the contract reverts with `MessageNotFound` on a mismatch.
pub fn encode_remove_inbox(
    source_chain_id: u64,
    sender: Address,
    receiver: Address,
    session_id: U256,
    label: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, MailboxError> {
    let call = removeInboxCall {
        chainMessageSender: U256::from(source_chain_id),
        sender,
        receiver,
        sessionId: session_id,
        label: String::from_utf8_lossy(label).to_string(),
        data: data.to_vec().into(),
    };
    Ok(call.abi_encode())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_put_inbox_round_trips_fields() {
        let session_id = U256::from(42u64);
        let data = encode_put_inbox(
            901,
            Address::ZERO,
            Address::ZERO,
            session_id,
            b"SEND_TOKENS",
            b"hello",
        )
        .unwrap();

        let decoded = putInboxCall::abi_decode(&data).unwrap();
        assert_eq!(decoded.chainMessageSender, U256::from(901u64));
        assert_eq!(decoded.sessionId, session_id);
        assert_eq!(decoded.label, "SEND_TOKENS");
        assert_eq!(decoded.data.as_ref(), b"hello");
    }
}
