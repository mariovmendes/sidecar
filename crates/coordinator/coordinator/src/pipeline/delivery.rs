//! Helpers for deriving sender/nonce metadata from XT transactions.

use std::collections::HashMap;

use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::primitives::Address;
use compose_primitives::{ChainId, CrossRollupDependency};

/// Decode the sender address and nonce from a raw RLP-encoded signed transaction.
pub fn decode_sender_nonce(raw_tx: &[u8]) -> Option<(Address, u64)> {
    let signed: TxEnvelope = alloy::rlp::Decodable::decode(&mut &raw_tx[..]).ok()?;
    let from = signed.recover_signer().ok()?;
    Some((from, signed.nonce()))
}

/// Build a sender/nonce cache from each chain's first raw transaction.
///
/// Performs ECDSA recovery once at XT registration so later lifecycle steps
/// do not need to repeat recovery for the first local transaction on a chain.
pub fn build_sender_nonce_cache(
    txs: &HashMap<ChainId, Vec<Vec<u8>>>,
) -> HashMap<ChainId, (Address, u64)> {
    txs.iter()
        .filter_map(|(&chain_id, chain_txs)| {
            chain_txs
                .first()
                .and_then(|tx| decode_sender_nonce(tx))
                .map(|sn| (chain_id, sn))
        })
        .collect()
}

/// Compact one-line description of a raw signed transaction for `XTFLOW`
/// logging: `<chain>/<sender>/<nonce>/<selector>/<hash>`. Never fails — an
/// undecodable transaction is reported as `<chain>/undecodable`.
pub fn describe_tx(chain_id: ChainId, raw_tx: &[u8]) -> String {
    let Ok(signed) = <TxEnvelope as alloy::rlp::Decodable>::decode(&mut &raw_tx[..]) else {
        return format!("{chain_id}/undecodable");
    };
    let sender = signed
        .recover_signer()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unrecoverable".to_string());
    let input = signed.input();
    let selector = if input.len() >= 4 {
        format!("0x{}", hex::encode(&input[..4]))
    } else {
        "0x".to_string()
    };
    format!(
        "{chain_id}/{sender}/{}/{selector}/{}",
        signed.nonce(),
        signed.tx_hash()
    )
}

/// Same as [`describe_tx`] for a list of raw transactions on one chain.
pub fn describe_local_txs(chain_id: ChainId, txs: &[Vec<u8>]) -> String {
    txs.iter()
        .map(|raw| describe_tx(chain_id, raw))
        .collect::<Vec<_>>()
        .join(",")
}

/// Same as [`describe_tx`] for every transaction of an XT, joined by `,`.
/// Chains are emitted in ascending order so two sidecars log the same string
/// for the same XT.
pub fn describe_txs(txs: &HashMap<ChainId, Vec<Vec<u8>>>) -> String {
    let mut chains: Vec<&ChainId> = txs.keys().collect();
    chains.sort();
    chains
        .into_iter()
        .flat_map(|chain_id| {
            txs[chain_id]
                .iter()
                .map(move |raw| describe_tx(*chain_id, raw))
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Filter dependencies to only those targeting the given chain.
pub fn deps_for_chain(
    deps: &[CrossRollupDependency],
    chain_id: ChainId,
) -> Vec<CrossRollupDependency> {
    deps.iter()
        .filter(|dep| dep.dest_chain_id == chain_id)
        .cloned()
        .collect()
}
