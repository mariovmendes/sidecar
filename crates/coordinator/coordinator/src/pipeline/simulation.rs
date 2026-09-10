//! Simulation pipeline and vote emission flow.

use crate::coordinator::ChunkStage::{
    Aborted, Confirmed, Registered, WaitingForDecided, WaitingForMessages,
};
use crate::coordinator::{ChunkStage, DefaultCoordinator, TransactionChunk};
use crate::pipeline::delivery::{decode_sender_nonce, describe_local_txs};
use compose_primitives::xtflow;
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::{SolCall, SolValue};
use compose_mailbox::contract::{
    approveCall, bridgeCETToCall, bridgeERC20ToCall, bridgeEthToCall, receiveETHCall,
    receiveTokensCall,
};
use compose_mailbox::matching::matches_dependency;
use compose_mailbox::overrides::merge_overrides;
use compose_mailbox::wire;
use compose_primitives::{ChainId, CrossRollupDependency, StateOverride};
use compose_primitives_traits::{SendAbortEthParams, SendAbortTokenParams};
use compose_proto::MailboxMessage;
use compose_simulation::error::SimulationError;
use tracing::{debug, error, info, warn};

/// Result of trying to put an XT's compensation on chain.
enum Compensation {
    /// The compensating transaction was handed to the builder.
    Submitted,
    /// There is nothing to compensate — nothing of this instance ever reached
    /// the builder, so no funds are escrowed.
    NotApplicable,
    /// The attempt failed and must be retried: funds may be escrowed with no
    /// refund in flight.
    Failed,
}

/// Which receive-leg call a `"receive"` chunk entry decodes to.
enum ReceiveCallKind {
    Tokens,
    Eth,
}

/// Fields recovered from a signed `bridgeERC20To`/`bridgeCETTo`/`bridgeEthTo`
/// transaction, sufficient to build the matching `sendConfirm`/`sendAbort*`
/// call without re-simulating or re-tracing it.
enum BridgeCallArgs {
    Token {
        chain_dest: ChainId,
        token: Address,
        amount: U256,
        receiver: Address,
        session_id: U256,
    },
    Eth {
        chain_dest: ChainId,
        amount: U256,
        receiver: Address,
        session_id: U256,
    },
}

impl BridgeCallArgs {
    fn chain_dest(&self) -> ChainId {
        match self {
            Self::Token { chain_dest, .. } | Self::Eth { chain_dest, .. } => *chain_dest,
        }
    }

    fn receiver(&self) -> Address {
        match self {
            Self::Token { receiver, .. } | Self::Eth { receiver, .. } => *receiver,
        }
    }

    fn session_id(&self) -> U256 {
        match self {
            Self::Token { session_id, .. } | Self::Eth { session_id, .. } => *session_id,
        }
    }

    fn label(&self) -> &'static [u8] {
        match self {
            Self::Token { .. } => b"SEND_TOKENS",
            Self::Eth { .. } => b"SEND_ETH",
        }
    }
}

impl DefaultCoordinator {
    /// Run the simulation pipeline for the local chain's portion of an XT.
    ///
    pub async fn register_xt(&self, transaction_chunk: &mut TransactionChunk) {
        xtflow!(
            "register_begin",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
        );
        info!(transaction_chunk.instance_id, chain_id = %self.chain_id, "Processing XT");

        // Capture everything we need from state in a single read lock. Start
        // from the accumulated local overlay so later XTs see the post-state
        // of previously committed local XTs in this period.
        let tx_bytes_list = {
            let state = self.state.read().await;
            match state.pending.get(transaction_chunk.instance_id.as_str()) {
                Some(xt) => match xt.raw_txs.get(&self.chain_id) {
                    Some(txs) if !txs.is_empty() => {
                        debug!(
                            transaction_chunk.instance_id,
                            chain_id = %self.chain_id,
                            "Simulation state check"
                        );

                        txs.clone()
                    }
                    _ => {
                        warn!(
                            transaction_chunk.instance_id,
                            "No local transactions, rejecting"
                        );
                        drop(state);
                        let _ = self
                            .send_vote(transaction_chunk.instance_id.as_str(), false)
                            .await;
                        return;
                    }
                },
                None => {
                    warn!(
                        transaction_chunk.instance_id,
                        "XT disappeared during simulation (likely rollback)"
                    );
                    return;
                }
            }
        };

        let simulator = match &self.simulator {
            Some(s) => s.clone(),
            None => {
                warn!("No simulator configured, voting yes without simulation");
                let _ = self
                    .send_vote(transaction_chunk.instance_id.as_str(), true)
                    .await;
                return;
            }
        };
        let mut needs_approve = false;
        let mut include_transactions = false;

        // Re-validate under the write lock: the XT may have been rolled back
        // between the read lock above and here.
        let received_message_xt = {
            let state = self.state.write().await;
            if !state
                .pending
                .contains_key(transaction_chunk.instance_id.as_str())
            {
                warn!(
                    transaction_chunk.instance_id,
                    "XT disappeared during simulation (likely rollback)"
                );
                drop(state);
                return;
            }

            state
                .mailbox_messages
                .contains_key(&transaction_chunk.instance_id)
        };

        // Process each transaction.
        for tx_bytes in tx_bytes_list.iter() {
            let input = match Self::get_input(tx_bytes) {
                Ok(input) => input,
                Err(e) => {
                    warn!(transaction_chunk.instance_id, error = %e, "Failed to decode transaction, voting false");
                    let _ = self
                        .send_vote(transaction_chunk.instance_id.as_str(), false)
                        .await;
                    return;
                }
            };
            if input.len() >= 4 {
                let selector = &input[..4];
                if selector == bridgeERC20ToCall::SELECTOR
                    || selector == bridgeCETToCall::SELECTOR
                    || selector == bridgeEthToCall::SELECTOR
                {
                    if selector == bridgeERC20ToCall::SELECTOR {
                        needs_approve = true;
                    }
                    include_transactions = true;
                    transaction_chunk
                        .organised_transactions
                        .insert(String::from("bridge"), tx_bytes.clone());
                    transaction_chunk.is_sender = Some(true);
                } else if selector == approveCall::SELECTOR {
                    transaction_chunk
                        .organised_transactions
                        .insert(String::from("approve"), tx_bytes.clone());
                } else if selector == receiveTokensCall::SELECTOR
                    || selector == receiveETHCall::SELECTOR
                {
                    transaction_chunk.is_sender = Some(false);
                    transaction_chunk
                        .organised_transactions
                        .insert(String::from("receive"), tx_bytes.clone());
                } else {
                    warn!("Unrecognised transaction type, voting false");
                    let _ = self
                        .send_vote(transaction_chunk.instance_id.as_str(), false)
                        .await;
                    return;
                }
            } else {
                warn!("Unrecognised transaction, voting false");
                let _ = self
                    .send_vote(transaction_chunk.instance_id.as_str(), false)
                    .await;
                return;
            }
        }

        transaction_chunk.confirmed_stage = Some(Registered);
        transaction_chunk.stage = WaitingForMessages;
        {
            let mut state = self.state.write().await;
            state.inflight_chunks.insert(
                transaction_chunk.instance_id.clone(),
                transaction_chunk.clone(),
            );
        }
        let mut roles: Vec<&String> = transaction_chunk.organised_transactions.keys().collect();
        roles.sort();
        xtflow!(
            "stage",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            from = "Registered",
            to = "WaitingForMessages",
            is_sender = format!("{:?}", transaction_chunk.is_sender),
            roles = roles
                .iter()
                .map(|r| r.as_str())
                .collect::<Vec<_>>()
                .join("+"),
            mailbox_already_present = received_message_xt,
        );

        // A Decided(false) may have raced ahead of register_xt and already
        // been recorded on `xt.decision` by on_decision, before any chunk
        // existed for it to advance. Nothing has been submitted to the
        // builder yet at this point (that happens below), so there's
        // nothing to compensate. We just need to mark this instance done instead of
        // falling through and submitting approve/bridge for an XT that's
        // already known to be aborted.
        let already_decided = {
            let state = self.state.read().await;
            state
                .pending
                .get(transaction_chunk.instance_id.as_str())
                .and_then(|xt| xt.decision)
        };
        if already_decided == Some(false) {
            xtflow!(
                "register_pre_aborted",
                instance_id = transaction_chunk.instance_id,
                chain = self.chain_id,
            );
            info!(
                transaction_chunk.instance_id,
                "Decision already recorded as abort before submission, skipping"
            );
            transaction_chunk.stage = Aborted;
            transaction_chunk.confirmed_stage = Some(Aborted);
            let mut state = self.state.write().await;
            state.inflight_chunks.insert(
                transaction_chunk.instance_id.clone(),
                transaction_chunk.clone(),
            );
            return;
        }

        if received_message_xt {
            let _ = self.process_xt(transaction_chunk).await;
            return;
        }

        if include_transactions {
            if let Some(builder) = &self.xt_builder_client {
                let approve_tx_bytes = transaction_chunk
                    .organised_transactions
                    .get("approve")
                    .cloned();
                let mut txs_to_submit: Vec<Vec<u8>> = Vec::new();
                if needs_approve {
                    if let Some(approve_tx_bytes) = &approve_tx_bytes {
                        txs_to_submit.push(approve_tx_bytes.clone());
                    }
                }

                let (period_id, sequence_number) = {
                    let state = self.state.read().await;
                    state
                        .pending
                        .get(transaction_chunk.instance_id.as_str())
                        .map(|xt| {
                            let seq = if xt.sequence_num.0 != 0 {
                                xt.sequence_num.0
                            } else {
                                xt.origin_seq.0
                            };
                            (xt.period_id.0, seq)
                        })
                        .unwrap_or((0, 0))
                };

                let bridge_tx_bytes = transaction_chunk
                    .organised_transactions
                    .get("bridge")
                    .cloned();
                if let Some(bridge_tx_bytes) = &bridge_tx_bytes {
                    txs_to_submit.push(bridge_tx_bytes.clone());
                    let submitted = describe_local_txs(self.chain_id, &txs_to_submit);
                    xtflow!(
                        "builder_submit",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        call = "ethera_submitXt",
                        period = period_id,
                        seq = sequence_number,
                        tx_count = txs_to_submit.len(),
                        txs = submitted,
                    );
                    match builder
                        .submit_locked_xt(
                            transaction_chunk.instance_id.as_str(),
                            period_id,
                            sequence_number,
                            txs_to_submit,
                        )
                        .await
                    {
                        Ok(()) => {
                            xtflow!(
                                "builder_submit_ok",
                                instance_id = transaction_chunk.instance_id,
                                chain = self.chain_id,
                                call = "ethera_submitXt",
                            );
                            self.notify_outbound_dependency(
                                &simulator,
                                transaction_chunk.instance_id.as_str(),
                                approve_tx_bytes.as_deref(),
                                &bridge_tx_bytes,
                            )
                            .await;
                        }
                        Err(e) => {
                            xtflow!(
                                "builder_submit_err",
                                instance_id = transaction_chunk.instance_id,
                                chain = self.chain_id,
                                call = "ethera_submitXt",
                                error = e,
                            );
                            warn!(
                                transaction_chunk.instance_id,
                                error = %e,
                                "Failed to submit locked XT to builder, voting false"
                            );
                            let _ = self
                                .send_vote(transaction_chunk.instance_id.as_str(), false)
                                .await;
                            let mut state = self.state.write().await;
                            state
                                .inflight_chunks
                                .remove(transaction_chunk.instance_id.as_str());
                        }
                    }
                }
            }
        }
    }

    /// Simulate `approve` (if present) followed by the `bridge...To` call to
    /// recover the `writeMessage` this XT will emit, and deliver it to the
    /// destination chain's sidecar mailbox so it can be matched against the
    /// recipient's receive call once that XT is processed there.
    async fn notify_outbound_dependency(
        &self,
        simulator: &std::sync::Arc<dyn compose_simulation::traits::Simulator>,
        instance_id: &str,
        approve_tx_bytes: Option<&[u8]>,
        bridge_tx_bytes: &[u8],
    ) {
        let Some(mailbox_sender) = &self.mailbox_sender else {
            xtflow!(
                "mailbox_out_skip",
                instance_id = instance_id,
                chain = self.chain_id,
                reason = "no_mailbox_sender",
            );
            return;
        };

        let mut overrides = StateOverride::default();
        if let Some(approve_tx_bytes) = approve_tx_bytes {
            match simulator
                .simulate(self.chain_id, approve_tx_bytes, &StateOverride::default())
                .await
            {
                Ok(result) if result.success => {
                    if let Some(approve_overrides) = result.state_overrides {
                        merge_overrides(&mut overrides, &approve_overrides);
                    }
                }
                Ok(result) => {
                    xtflow!(
                        "mailbox_out_skip",
                        instance_id = instance_id,
                        chain = self.chain_id,
                        reason = "approve_simulation_failed",
                        error = format!("{:?}", result.error),
                    );
                    warn!(instance_id, error = ?result.error, "Approve simulation failed, skipping outbound ack");
                    return;
                }
                Err(e) => {
                    xtflow!(
                        "mailbox_out_skip",
                        instance_id = instance_id,
                        chain = self.chain_id,
                        reason = "approve_simulation_error",
                        error = e,
                    );
                    warn!(instance_id, error = %e, "Failed to simulate approve, skipping outbound ack");
                    return;
                }
            }
        }

        let bridge_result = match simulator
            .simulate(self.chain_id, bridge_tx_bytes, &overrides)
            .await
        {
            Ok(result) if result.success => result,
            Ok(result) => {
                xtflow!(
                    "mailbox_out_skip",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    reason = "bridge_simulation_failed",
                    error = format!("{:?}", result.error),
                );
                warn!(instance_id, error = ?result.error, "Bridge simulation failed, skipping outbound ack");
                return;
            }
            Err(e) => {
                xtflow!(
                    "mailbox_out_skip",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    reason = "bridge_simulation_error",
                    error = e,
                );
                warn!(instance_id, error = %e, "Failed to simulate bridge tx, skipping outbound ack");
                return;
            }
        };

        if bridge_result.outbound_messages.is_empty() {
            xtflow!(
                "mailbox_out_skip",
                instance_id = instance_id,
                chain = self.chain_id,
                reason = "no_outbound_messages",
            );
            return;
        }

        let raw_instance_id = {
            let state = self.state.read().await;
            let Some(xt) = state.pending.get(instance_id) else {
                xtflow!(
                    "mailbox_out_skip",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    reason = "xt_gone",
                );
                return;
            };
            xt.instance_id.clone()
        };

        for msg in &bridge_result.outbound_messages {
            let mailbox_msg = MailboxMessage {
                instance_id: raw_instance_id.clone(),
                source_chain: msg.source_chain_id.0,
                destination_chain: msg.dest_chain_id.0,
                sender: msg.sender.as_slice().to_vec(),
                receiver: msg.receiver.as_slice().to_vec(),
                label: msg.label.clone(),
                payload: msg.data.clone(),
                session_id: wire::encode_session_id(msg.session_id),
            };

            xtflow!(
                "mailbox_out",
                instance_id = instance_id,
                chain = self.chain_id,
                dest_chain = msg.dest_chain_id,
                label = msg.label,
                session = msg.session_id,
                path = "POST /mailbox",
            );
            if let Err(e) = mailbox_sender.send(msg.dest_chain_id, &mailbox_msg).await {
                xtflow!(
                    "mailbox_out_err",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    dest_chain = msg.dest_chain_id,
                    label = msg.label,
                    error = e,
                );
                warn!(
                    instance_id,
                    dest_chain = %msg.dest_chain_id,
                    error = %e,
                    "Failed to send outbound mailbox message to peer"
                );
            } else {
                xtflow!(
                    "mailbox_out_ok",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    dest_chain = msg.dest_chain_id,
                    label = msg.label,
                );
            }
        }
    }

    /// After successfully submitting `putInbox`+`receive...` for an inbound
    /// SEND message, derive the ACK that `receiveTokens`/`receiveETH` writes
    /// internally (`mailbox.writeMessage(ackHeader, ...)`) and deliver it to
    /// the origin chain's sidecar mailbox, so it can eventually `putInbox` the
    /// ACK on its own chain. The ACK's fields are fully determined by the SEND
    /// dependency plus the already-known SEND payload, so no simulation is
    /// needed here.
    async fn notify_receive_ack(
        &self,
        instance_id: &str,
        dependency: &CrossRollupDependency,
        receive_tx_bytes: &[u8],
    ) {
        let Some(mailbox_sender) = &self.mailbox_sender else {
            xtflow!(
                "ack_out_skip",
                instance_id = instance_id,
                chain = self.chain_id,
                reason = "no_mailbox_sender",
            );
            return;
        };
        let Some(send_payload) = dependency.data.as_ref() else {
            xtflow!(
                "ack_out_skip",
                instance_id = instance_id,
                chain = self.chain_id,
                reason = "dependency_without_payload",
            );
            return;
        };

        let input = match Self::get_input(receive_tx_bytes) {
            Ok(input) => input,
            Err(e) => {
                warn!(instance_id, error = %e, "Failed to decode receive tx, skipping ack");
                return;
            }
        };
        if input.len() < 4 {
            return;
        }

        let ack_payload = if input[..4] == receiveTokensCall::SELECTOR {
            match compose_mailbox::contract::SendTokensPayloadHead::abi_decode(send_payload) {
                Ok(head) => compose_mailbox::contract::AckPayload {
                    remoteAsset: head.remoteAsset,
                    amount: head.amount,
                },
                Err(e) => {
                    warn!(instance_id, error = %e, "Failed to decode SEND_TOKENS payload, skipping ack");
                    return;
                }
            }
        } else if input[..4] == receiveETHCall::SELECTOR {
            match compose_mailbox::contract::SendEthPayload::abi_decode(send_payload) {
                Ok(send) => compose_mailbox::contract::AckPayload {
                    remoteAsset: alloy::primitives::Address::ZERO,
                    amount: send.amount,
                },
                Err(e) => {
                    warn!(instance_id, error = %e, "Failed to decode SEND_ETH payload, skipping ack");
                    return;
                }
            }
        } else {
            return;
        };

        let ack_dependency = CrossRollupDependency {
            source_chain_id: self.chain_id,
            dest_chain_id: dependency.source_chain_id,
            sender: dependency.receiver,
            receiver: dependency.sender,
            label: b"ACK".to_vec(),
            data: Some(ack_payload.abi_encode()),
            session_id: dependency.session_id,
        };

        xtflow!(
            "ack_out",
            instance_id = instance_id,
            chain = self.chain_id,
            dest_chain = dependency.source_chain_id,
            session = dependency.session_id,
            path = "POST /mailbox/ack",
        );
        if let Err(e) = mailbox_sender
            .send_ack(dependency.source_chain_id, instance_id, &ack_dependency)
            .await
        {
            xtflow!(
                "ack_out_err",
                instance_id = instance_id,
                chain = self.chain_id,
                dest_chain = dependency.source_chain_id,
                error = e,
            );
            warn!(
                instance_id,
                dest_chain = %dependency.source_chain_id,
                error = %e,
                "Failed to send ACK to peer"
            );
        } else {
            xtflow!(
                "ack_out_ok",
                instance_id = instance_id,
                chain = self.chain_id,
                dest_chain = dependency.source_chain_id,
            );
        }
    }

    fn get_input(tx_bytes: &[u8]) -> Result<Bytes, SimulationError> {
        let signed: TxEnvelope = alloy::rlp::Decodable::decode(&mut &tx_bytes[..])
            .map_err(|e| SimulationError::Other(format!("failed to decode tx: {e}")))?;

        Ok(signed.input().clone())
    }

    /// Decodes `tx_bytes` as a signed `receiveTokens`/`receiveETH` call and derives the
    /// `CrossRollupDependency` key (without `data`) that the matching mailbox message must
    /// satisfy, mirroring the on-chain `MessageHeader` this receive call was built from.
    fn decode_receive_header(tx_bytes: &[u8]) -> Result<CrossRollupDependency, SimulationError> {
        let input = Self::get_input(tx_bytes)?;
        if input.len() < 4 {
            return Err(SimulationError::Other(
                "receive tx input too short".to_string(),
            ));
        }

        let header = if input[..4] == receiveTokensCall::SELECTOR {
            receiveTokensCall::abi_decode(&input)
                .map_err(|e| {
                    SimulationError::Other(format!("failed to decode receiveTokens call: {e}"))
                })?
                .msgHeader
        } else if input[..4] == receiveETHCall::SELECTOR {
            receiveETHCall::abi_decode(&input)
                .map_err(|e| {
                    SimulationError::Other(format!("failed to decode receiveETH call: {e}"))
                })?
                .msgHeader
        } else {
            return Err(SimulationError::Other(
                "receive tx is not a receiveTokens/receiveETH call".to_string(),
            ));
        };

        let source_chain_id = u64::try_from(header.chainSrc)
            .map_err(|_| SimulationError::Other("chainSrc does not fit in u64".to_string()))?;
        let dest_chain_id = u64::try_from(header.chainDest)
            .map_err(|_| SimulationError::Other("chainDest does not fit in u64".to_string()))?;

        Ok(CrossRollupDependency {
            source_chain_id: ChainId(source_chain_id),
            dest_chain_id: ChainId(dest_chain_id),
            sender: header.sender,
            receiver: header.receiver,
            label: header.label.into_bytes(),
            data: None,
            session_id: header.sessionId,
        })
    }

    pub async fn process_xt(&self, transaction_chunk: &mut TransactionChunk) {
        xtflow!(
            "process_begin",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            is_sender = format!("{:?}", transaction_chunk.is_sender),
            confirmed_stage = format!("{:?}", transaction_chunk.confirmed_stage),
        );
        if let Some(builder) = &self.xt_builder_client {
            if let Some(val) = transaction_chunk.is_sender {
                if val {
                    // Either a locally-decoded writeMessage tx (legacy path,
                    // currently unused) or an ACK CrossRollupDependency
                    // recorded via handle_ack_dependency (POST /mailbox/ack).
                    let ack_send_bytes = transaction_chunk
                        .organised_transactions
                        .get("ackSend")
                        .cloned();

                    let put_inbox_result = if let Some(ack_send_bytes) = ack_send_bytes {
                        Some(
                            self.submit_put_inbox_for_tx(
                                transaction_chunk.instance_id.as_str(),
                                &ack_send_bytes,
                            )
                            .await,
                        )
                    } else {
                        let ack_dependency = self
                            .lookup_mailbox_dependency(
                                transaction_chunk.instance_id.as_str(),
                                "ACK",
                            )
                            .await;

                        match ack_dependency {
                            Some(dep) => {
                                xtflow!(
                                    "put_inbox_ack",
                                    instance_id = transaction_chunk.instance_id,
                                    chain = self.chain_id,
                                    session = dep.session_id,
                                    label = String::from_utf8_lossy(&dep.label),
                                );
                                Some(
                                    self.handle_dependency(
                                        transaction_chunk.instance_id.as_str(),
                                        dep,
                                    )
                                    .await,
                                )
                            }
                            None => {
                                xtflow!(
                                    "process_reject",
                                    instance_id = transaction_chunk.instance_id,
                                    chain = self.chain_id,
                                    reason = "no_ack_mailbox_message",
                                );
                                warn!("No ackSend tx or mailbox ACK found for instance, rejecting");
                                let _ = self
                                    .send_vote(transaction_chunk.instance_id.as_str(), false)
                                    .await;
                                return;
                            }
                        }
                    };

                    if let Some(Err(e)) = put_inbox_result {
                        xtflow!(
                            "put_inbox_err",
                            instance_id = transaction_chunk.instance_id,
                            chain = self.chain_id,
                            side = "sender",
                            error = e,
                        );
                        warn!("Failed to submit putInbox tx: {e}");
                        let _ = self
                            .send_vote(transaction_chunk.instance_id.as_str(), false)
                            .await;
                        return;
                    }
                    xtflow!(
                        "put_inbox_ok",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        side = "sender",
                        call = "ethera_submitFollowup",
                    );

                    let _ = self
                        .send_vote(transaction_chunk.instance_id.as_str(), true)
                        .await;
                    transaction_chunk.confirmed_stage = Some(WaitingForMessages);
                    transaction_chunk.stage = WaitingForDecided;
                    xtflow!(
                        "stage",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        from = "WaitingForMessages",
                        to = "WaitingForDecided",
                        side = "sender",
                    );
                    self.state.write().await.inflight_chunks.insert(
                        transaction_chunk.instance_id.clone(),
                        transaction_chunk.clone(),
                    );
                    self.dispatch_if_already_decided(transaction_chunk).await;
                    return;
                } else {
                    if transaction_chunk.organised_transactions.is_empty() {
                        warn!("No transactions to process, rejecting");
                        let _ = self
                            .send_vote(transaction_chunk.instance_id.as_str(), false)
                            .await;
                        return;
                    }

                    let Some(receive_tx_bytes) = transaction_chunk
                        .organised_transactions
                        .get("receive")
                        .cloned()
                    else {
                        warn!("Transaction of chunk not recognised, rejecting");
                        let _ = self
                            .send_vote(transaction_chunk.instance_id.as_str(), false)
                            .await;
                        return;
                    };

                    let expected_dependency = match Self::decode_receive_header(&receive_tx_bytes) {
                        Ok(dep) => dep,
                        Err(e) => {
                            warn!("Failed to decode receive tx header: {e}");
                            let _ = self
                                .send_vote(transaction_chunk.instance_id.as_str(), false)
                                .await;
                            return;
                        }
                    };

                    let mailbox_msg = {
                        let state = self.state.read().await;
                        state
                            .mailbox_messages
                            .get(transaction_chunk.instance_id.as_str())
                            .and_then(|msgs| {
                                msgs.iter()
                                    .find(|msg| matches_dependency(msg, &expected_dependency))
                            })
                            .cloned()
                    };

                    let Some(mailbox_msg) = mailbox_msg else {
                        xtflow!(
                            "process_reject",
                            instance_id = transaction_chunk.instance_id,
                            chain = self.chain_id,
                            reason = "no_matching_mailbox_message",
                            want_session = expected_dependency.session_id,
                            want_label = String::from_utf8_lossy(&expected_dependency.label),
                            recorded = self
                                .state
                                .read()
                                .await
                                .mailbox_messages
                                .get(transaction_chunk.instance_id.as_str())
                                .map(Vec::len)
                                .unwrap_or(0),
                        );
                        warn!("No matching mailbox message recorded for instance, rejecting");
                        let _ = self
                            .send_vote(transaction_chunk.instance_id.as_str(), false)
                            .await;
                        return;
                    };

                    let dependency = CrossRollupDependency {
                        data: Some(mailbox_msg.payload.clone()),
                        ..expected_dependency
                    };

                    let mut txs_to_submit: Vec<Vec<u8>> = Vec::new();

                    // putInbox must execute before the receive tx: readMessage()
                    // reverts unless putInbox already ran in this block. Submitting
                    // them separately via submit_tx only orders their arrival at the
                    // pool, not their execution order, so we bundle them into a single
                    // atomically-ordered submission instead.
                    let (put_inbox_tx, put_inbox_nonce) = match self
                        .build_put_inbox_transaction_with_nonce(&dependency)
                        .await
                    {
                        Ok(built) => built,
                        Err(e) => {
                            xtflow!(
                                "put_inbox_build_err",
                                instance_id = transaction_chunk.instance_id,
                                chain = self.chain_id,
                                side = "receiver",
                                error = e,
                            );
                            if let Err(resync_err) = self.resync_put_inbox_nonce_monotonic().await {
                                warn!(error = %resync_err, "Failed to resync putInbox nonce after build error");
                            }
                            warn!("Failed to build putInbox tx: {e}");
                            let _ = self
                                .send_vote(transaction_chunk.instance_id.as_str(), false)
                                .await;
                            return;
                        }
                    };

                    txs_to_submit.push(put_inbox_tx.clone());
                    txs_to_submit.push(receive_tx_bytes.clone());

                    let (period_id, sequence_number) = {
                        let state = self.state.read().await;
                        state
                            .pending
                            .get(transaction_chunk.instance_id.as_str())
                            .map(|xt| {
                                let seq = if xt.sequence_num.0 != 0 {
                                    xt.sequence_num.0
                                } else {
                                    xt.origin_seq.0
                                };
                                (xt.period_id.0, seq)
                            })
                            .unwrap_or((0, 0))
                    };

                    xtflow!(
                        "builder_submit",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        call = "ethera_submitXt",
                        side = "receiver",
                        period = period_id,
                        seq = sequence_number,
                        tx_count = txs_to_submit.len(),
                        txs = describe_local_txs(self.chain_id, &txs_to_submit),
                    );
                    if let Err(e) = builder
                        .submit_locked_xt(
                            transaction_chunk.instance_id.as_str(),
                            period_id,
                            sequence_number,
                            txs_to_submit,
                        )
                        .await
                    {
                        xtflow!(
                            "builder_submit_err",
                            instance_id = transaction_chunk.instance_id,
                            chain = self.chain_id,
                            call = "ethera_submitXt",
                            side = "receiver",
                            error = e,
                        );
                        // The bundle carries the coordinator's putInbox: either
                        // snap to the nonce the builder asked for, or hand this
                        // one back, so the chain's coordinator sequence does not
                        // stall behind it.
                        self.reconcile_nonce_after_rejection(
                            &e,
                            put_inbox_nonce,
                            1,
                            "receive_bundle_rejected",
                        )
                        .await;
                        warn!("Failed to submit putInbox+receive bundle: {e}");
                        let _ = self
                            .send_vote(transaction_chunk.instance_id.as_str(), false)
                            .await;
                        return;
                    }
                    xtflow!(
                        "builder_submit_ok",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        call = "ethera_submitXt",
                        side = "receiver",
                    );
                    self.notify_receive_ack(
                        transaction_chunk.instance_id.as_str(),
                        &dependency,
                        &receive_tx_bytes,
                    )
                    .await;

                    let _ = self
                        .send_vote(transaction_chunk.instance_id.as_str(), true)
                        .await;
                    transaction_chunk.confirmed_stage = Some(WaitingForMessages);
                    transaction_chunk.stage = WaitingForDecided;
                    xtflow!(
                        "stage",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        from = "WaitingForMessages",
                        to = "WaitingForDecided",
                        side = "receiver",
                    );
                    self.state.write().await.inflight_chunks.insert(
                        transaction_chunk.instance_id.clone(),
                        transaction_chunk.clone(),
                    );
                    self.dispatch_if_already_decided(transaction_chunk).await;
                    return;
                }
            } else {
                xtflow!(
                    "process_reject",
                    instance_id = transaction_chunk.instance_id,
                    chain = self.chain_id,
                    reason = "chunk_missing_is_sender",
                );
                warn!("Transaction chunk not registered properly, rejecting");
                let _ = self
                    .send_vote(transaction_chunk.instance_id.as_str(), false)
                    .await;
                return;
            }
        } else {
            warn!("No transaction builder configured, rejecting");
            let _ = self
                .send_vote(transaction_chunk.instance_id.as_str(), false)
                .await;
        }
    }

    /// Called right after a chunk reaches `WaitingForDecided`. A decision may
    /// have already raced ahead and been recorded on `xt.decision` by
    /// on_decision while this instance's own processing was still in
    /// flight — since the publisher only broadcasts `Decided` once, nothing
    /// would ever come along afterwards to dispatch confirm_xt/abort_xt for
    /// it, leaving the chunk stuck at `WaitingForDecided` forever. Check for
    /// that here and, if the decision is already known, finalize/compensate
    /// immediately using the now-accurate `confirmed_stage` instead of
    /// waiting on a signal that will never arrive.
    async fn dispatch_if_already_decided(&self, transaction_chunk: &mut TransactionChunk) {
        let already_decided = {
            let state = self.state.read().await;
            state
                .pending
                .get(transaction_chunk.instance_id.as_str())
                .and_then(|xt| xt.decision)
        };
        let Some(decision) = already_decided else {
            return;
        };

        xtflow!(
            "decision_raced_ahead",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            decision = decision,
        );
        transaction_chunk.stage = if decision { Confirmed } else { Aborted };
        {
            let mut state = self.state.write().await;
            state.inflight_chunks.insert(
                transaction_chunk.instance_id.clone(),
                transaction_chunk.clone(),
            );
        }

        if decision {
            self.confirm_xt(transaction_chunk).await;
        } else {
            self.abort_xt(transaction_chunk).await;
        }
    }

    /// Finalize a decided-commit XT: `sendConfirm` on the sender side,
    /// `recvConfirmToken`/`recvConfirmETH` on the receiver side.
    pub async fn confirm_xt(&self, transaction_chunk: &mut TransactionChunk) {
        xtflow!(
            "confirm_begin",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            is_sender = format!("{:?}", transaction_chunk.is_sender),
            confirmed_stage = format!("{:?}", transaction_chunk.confirmed_stage),
        );
        match transaction_chunk.is_sender {
            Some(true) => {
                let Some(bridge_tx_bytes) = transaction_chunk.organised_transactions.get("bridge")
                else {
                    warn!(
                        transaction_chunk.instance_id,
                        "No bridge tx recorded for sender confirm, dropping"
                    );
                    return;
                };
                let args = match Self::decode_bridge_call_args(bridge_tx_bytes) {
                    Ok(args) => args,
                    Err(e) => {
                        warn!(transaction_chunk.instance_id, error = %e, "Failed to decode bridge tx for confirm, dropping");
                        return;
                    }
                };
                let Some(l2_builder) = &self.l2_bridge_builder else {
                    warn!(
                        transaction_chunk.instance_id,
                        "No l2 bridge builder configured, dropping confirm"
                    );
                    return;
                };
                let header = CrossRollupDependency {
                    source_chain_id: self.chain_id,
                    dest_chain_id: args.chain_dest(),
                    sender: l2_builder.contract_address(),
                    receiver: args.receiver(),
                    label: args.label().to_vec(),
                    data: None,
                    session_id: args.session_id(),
                };

                if let Err(e) = self
                    .submit_send_confirm(transaction_chunk.instance_id.as_str(), &header)
                    .await
                {
                    xtflow!(
                        "confirm_err",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        side = "sender",
                        call = "sendConfirm",
                        error = e,
                    );
                    warn!(transaction_chunk.instance_id, error = %e, "Failed to submit sendConfirm");
                    self.note_finalize_failure(transaction_chunk, "sendConfirm").await;
                    return;
                }

                xtflow!(
                    "confirm_ok",
                    instance_id = transaction_chunk.instance_id,
                    chain = self.chain_id,
                    side = "sender",
                    call = "sendConfirm",
                    session = header.session_id,
                );
                self.mark_confirmed_stage(transaction_chunk.instance_id.as_str(), Confirmed)
                    .await;
            }
            Some(false) => {
                let Some(receive_tx_bytes) =
                    transaction_chunk.organised_transactions.get("receive")
                else {
                    warn!(
                        transaction_chunk.instance_id,
                        "No receive tx recorded for receiver confirm, dropping"
                    );
                    return;
                };
                let header = match Self::decode_receive_header(receive_tx_bytes) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(transaction_chunk.instance_id, error = %e, "Failed to decode receive tx for confirm, dropping");
                        return;
                    }
                };

                let instance_id = transaction_chunk.instance_id.as_str();
                let result = match Self::receive_call_kind(receive_tx_bytes) {
                    Some(ReceiveCallKind::Tokens) => {
                        self.submit_recv_confirm_token(instance_id, &header).await
                    }
                    Some(ReceiveCallKind::Eth) => {
                        self.submit_recv_confirm_eth(instance_id, &header).await
                    }
                    None => {
                        warn!(
                            transaction_chunk.instance_id,
                            "Receive tx selector not recognised for confirm, dropping"
                        );
                        return;
                    }
                };

                if let Err(e) = result {
                    xtflow!(
                        "confirm_err",
                        instance_id = transaction_chunk.instance_id,
                        chain = self.chain_id,
                        side = "receiver",
                        call = "recvConfirm",
                        error = e,
                    );
                    warn!(transaction_chunk.instance_id, error = %e, "Failed to submit recvConfirm");
                    self.note_finalize_failure(transaction_chunk, "recvConfirm").await;
                    return;
                }

                xtflow!(
                    "confirm_ok",
                    instance_id = transaction_chunk.instance_id,
                    chain = self.chain_id,
                    side = "receiver",
                    call = "recvConfirm",
                    session = header.session_id,
                );
                self.mark_confirmed_stage(transaction_chunk.instance_id.as_str(), Confirmed)
                    .await;
            }
            None => {
                warn!(
                    transaction_chunk.instance_id,
                    "Chunk missing is_sender at confirm, dropping"
                );
            }
        }
    }

    /// Compensate a decided-abort XT: `sendAbortToken`/`sendAbortETH` on the
    /// sender side, `recvAbortToken`/`recvAbortETH` on the receiver side.
    pub async fn abort_xt(&self, transaction_chunk: &mut TransactionChunk) {
        xtflow!(
            "abort_begin",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            is_sender = format!("{:?}", transaction_chunk.is_sender),
            confirmed_stage = format!("{:?}", transaction_chunk.confirmed_stage),
        );
        match transaction_chunk.is_sender {
            Some(true) => {
                let outcome = match transaction_chunk.confirmed_stage {
                    Some(Registered) => self.send_abort_submit(transaction_chunk, None).await,
                    Some(WaitingForMessages) => {
                        let ack_dependency = self
                            .lookup_mailbox_dependency(
                                transaction_chunk.instance_id.as_str(),
                                "ACK",
                            )
                            .await;
                        if ack_dependency.is_none() {
                            warn!(
                                transaction_chunk.instance_id,
                                "No ACK mailbox message found to removeInbox on abort"
                            );
                        }
                        self.send_abort_submit(transaction_chunk, ack_dependency.as_ref())
                            .await
                    }
                    // Nothing of this instance reached the builder, so there is
                    // no escrow to refund.
                    _ => Compensation::NotApplicable,
                };

                // Only mark the chunk done once the compensation is actually on
                // its way. A failure here means the user's tokens are escrowed
                // with no refund in flight, so leave `confirmed_stage` behind
                // and let the watchdog retry.
                if let Compensation::Failed = outcome {
                    self.note_finalize_failure(transaction_chunk, "sendAbort").await;
                    return;
                }

                // The chunk only reaches Aborted from WaitingForDecided, which
                // (is_sender == true) is only reached after process_xt
                // successfully putInbox'd the ACK — so it's always there to
                // remove. Undoes that putInbox, mirroring `unwrite` compensating
                // `writeMessage`.
                self.mark_confirmed_stage(transaction_chunk.instance_id.as_str(), Aborted)
                    .await;
            }
            Some(false) => {
                match transaction_chunk.confirmed_stage {
                    Some(stage) => {
                        match stage {
                            WaitingForMessages => {
                                let Some(receive_tx_bytes) =
                                    transaction_chunk.organised_transactions.get("receive")
                                else {
                                    warn!(
                                        transaction_chunk.instance_id,
                                        "No receive tx recorded for receiver abort, dropping"
                                    );
                                    return;
                                };
                                let header = match Self::decode_receive_header(receive_tx_bytes) {
                                    Ok(h) => h,
                                    Err(e) => {
                                        warn!(transaction_chunk.instance_id, error = %e, "Failed to decode receive tx for abort, dropping");
                                        return;
                                    }
                                };

                                // Reaching Aborted here (is_sender == false) only happens
                                // after process_xt already putInbox'd the original SEND
                                // message, so it's always present to remove. removeInbox
                                // requires the exact original payload, so re-match the
                                // recorded mailbox message the same way process_xt did to
                                // build the putInbox in the first place. The recvAbort and
                                // removeInbox transactions are released to the builder
                                // together.
                                let mailbox_msg = {
                                    let state = self.state.read().await;
                                    state
                                        .mailbox_messages
                                        .get(transaction_chunk.instance_id.as_str())
                                        .and_then(|msgs| {
                                            msgs.iter().find(|msg| matches_dependency(msg, &header))
                                        })
                                        .cloned()
                                };

                                let send_dependency = match mailbox_msg {
                                    Some(mailbox_msg) => Some(CrossRollupDependency {
                                        data: Some(mailbox_msg.payload),
                                        ..header.clone()
                                    }),
                                    None => {
                                        warn!(transaction_chunk.instance_id, "No matching mailbox message found to removeInbox on abort");
                                        None
                                    }
                                };

                                let instance_id = transaction_chunk.instance_id.as_str();
                                let result = match Self::receive_call_kind(receive_tx_bytes) {
                                    Some(ReceiveCallKind::Tokens) => {
                                        self.submit_recv_abort_token(
                                            instance_id,
                                            &header,
                                            send_dependency.as_ref(),
                                        )
                                        .await
                                    }
                                    Some(ReceiveCallKind::Eth) => {
                                        self.submit_recv_abort_eth(
                                            instance_id,
                                            &header,
                                            send_dependency.as_ref(),
                                        )
                                        .await
                                    }
                                    None => {
                                        warn!(transaction_chunk.instance_id, "Receive tx selector not recognised for abort, dropping");
                                        return;
                                    }
                                };

                                if let Err(e) = result {
                                    if e.is_unknown_instance() {
                                        // Already dropped at the builder along
                                        // with its un-executed transactions —
                                        // nothing to compensate, and no retry
                                        // can change that.
                                        xtflow!(
                                            "abort_not_applicable",
                                            instance_id = transaction_chunk.instance_id,
                                            chain = self.chain_id,
                                            side = "receiver",
                                            call = "recvAbort",
                                            reason = "instance already dropped at builder",
                                        );
                                        self.mark_confirmed_stage(
                                            transaction_chunk.instance_id.as_str(),
                                            Aborted,
                                        )
                                        .await;
                                        return;
                                    }
                                    xtflow!(
                                        "abort_err",
                                        instance_id = transaction_chunk.instance_id,
                                        chain = self.chain_id,
                                        side = "receiver",
                                        call = "recvAbort",
                                        error = e,
                                    );
                                    warn!(transaction_chunk.instance_id, error = %e, "Failed to submit recvAbort");
                                    self.note_finalize_failure(transaction_chunk, "recvAbort")
                                        .await;
                                    return;
                                } else {
                                    xtflow!(
                                        "abort_ok",
                                        instance_id = transaction_chunk.instance_id,
                                        chain = self.chain_id,
                                        side = "receiver",
                                        call = "recvAbort",
                                        remove_inbox = send_dependency.is_some(),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                    None => {}
                }

                self.mark_confirmed_stage(transaction_chunk.instance_id.as_str(), Aborted)
                    .await;
            }
            None => {
                warn!(
                    transaction_chunk.instance_id,
                    "Chunk missing is_sender at abort, dropping"
                );
            }
        }
    }

    async fn send_abort_submit(
        &self,
        transaction_chunk: &mut TransactionChunk,
        remove_inbox_dependency: Option<&CrossRollupDependency>,
    ) -> Compensation {
        let Some(bridge_tx_bytes) = transaction_chunk.organised_transactions.get("bridge") else {
            warn!(
                transaction_chunk.instance_id,
                "No bridge tx recorded for sender abort, dropping"
            );
            return Compensation::NotApplicable;
        };
        let Some((sender, _nonce)) = decode_sender_nonce(bridge_tx_bytes) else {
            warn!(
                transaction_chunk.instance_id,
                "Failed to recover bridge tx signer for abort, dropping"
            );
            return Compensation::NotApplicable;
        };
        let args = match Self::decode_bridge_call_args(bridge_tx_bytes) {
            Ok(args) => args,
            Err(e) => {
                warn!(transaction_chunk.instance_id, error = %e, "Failed to decode bridge tx for abort, dropping");
                return Compensation::NotApplicable;
            }
        };

        let instance_id = transaction_chunk.instance_id.as_str();
        let result = match args {
            BridgeCallArgs::Token {
                chain_dest,
                token,
                amount,
                receiver,
                session_id,
            } => {
                let params = SendAbortTokenParams {
                    chain_dest,
                    token,
                    sender,
                    receiver,
                    amount,
                    session_id,
                };
                self.submit_send_abort_token(instance_id, &params, remove_inbox_dependency)
                    .await
            }
            BridgeCallArgs::Eth {
                chain_dest,
                amount,
                receiver,
                session_id,
            } => {
                let params = SendAbortEthParams {
                    chain_dest,
                    sender,
                    receiver,
                    amount,
                    session_id,
                };
                self.submit_send_abort_eth(instance_id, &params, remove_inbox_dependency)
                    .await
            }
        };

        if let Err(e) = result {
            if e.is_unknown_instance() {
                // The instance was already dropped at the builder (a period
                // tick aborts it there and locally at the same time), taking
                // its un-executed transactions with it. There is no escrow
                // left to refund, so this is done, not failed — retrying would
                // only repeat the same rejection and raise a false alarm.
                xtflow!(
                    "abort_not_applicable",
                    instance_id = transaction_chunk.instance_id,
                    chain = self.chain_id,
                    side = "sender",
                    call = "sendAbort",
                    reason = "instance already dropped at builder",
                );
                return Compensation::NotApplicable;
            }
            xtflow!(
                "abort_err",
                instance_id = transaction_chunk.instance_id,
                chain = self.chain_id,
                side = "sender",
                call = "sendAbort",
                error = e,
            );
            warn!(transaction_chunk.instance_id, error = %e, "Failed to submit sendAbort");
            return Compensation::Failed;
        }

        xtflow!(
            "abort_ok",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            side = "sender",
            call = "sendAbort",
            remove_inbox = remove_inbox_dependency.is_some(),
        );
        Compensation::Submitted
    }

    /// Count a failed finalize/compensate attempt and leave the chunk short of
    /// `confirmed_stage`, so `retry_unfinished_finalizations` picks it up on
    /// the next watchdog tick.
    async fn note_finalize_failure(&self, transaction_chunk: &TransactionChunk, call: &str) {
        let attempts = self
            .record_finalize_failure(transaction_chunk.instance_id.as_str())
            .await;
        xtflow!(
            "finalize_failed",
            instance_id = transaction_chunk.instance_id,
            chain = self.chain_id,
            call = call,
            attempts = attempts,
            max_attempts = crate::coordinator::MAX_FINALIZE_ATTEMPTS,
        );
    }

    fn receive_call_kind(tx_bytes: &[u8]) -> Option<ReceiveCallKind> {
        let input = Self::get_input(tx_bytes).ok()?;
        if input.len() < 4 {
            return None;
        }
        if input[..4] == receiveTokensCall::SELECTOR {
            Some(ReceiveCallKind::Tokens)
        } else if input[..4] == receiveETHCall::SELECTOR {
            Some(ReceiveCallKind::Eth)
        } else {
            None
        }
    }

    /// Record that `instance_id`'s inflight chunk finished processing at
    /// `stage` (`confirm_xt`/`abort_xt` only perform side effects; they don't
    /// advance `stage` itself the way `register_xt`/`process_xt` do, since
    /// `on_decision` already set it to the target `Confirmed`/`Aborted`
    /// value). Takes the write lock only for this single field update.
    async fn mark_confirmed_stage(&self, instance_id: &str, stage: ChunkStage) {
        let marked = self
            .state
            .write()
            .await
            .inflight_chunks
            .get_mut(instance_id)
            .map(|chunk| {
                chunk.confirmed_stage = Some(stage);
            })
            .is_some();
        xtflow!(
            "terminal",
            instance_id = instance_id,
            chain = self.chain_id,
            confirmed_stage = format!("{stage:?}"),
            chunk_present = marked,
        );
    }

    /// Look up a recorded mailbox message for `instance_id` with the given
    /// `label` (e.g. `"ACK"`) and convert it into a `CrossRollupDependency`.
    async fn lookup_mailbox_dependency(
        &self,
        instance_id: &str,
        label: &str,
    ) -> Option<CrossRollupDependency> {
        let state = self.state.read().await;
        let msg = state
            .mailbox_messages
            .get(instance_id)
            .and_then(|msgs| msgs.iter().find(|m| m.label == label))?;

        Some(CrossRollupDependency {
            source_chain_id: ChainId(msg.source_chain),
            dest_chain_id: ChainId(msg.destination_chain),
            sender: alloy::primitives::Address::from_slice(&msg.sender),
            receiver: alloy::primitives::Address::from_slice(&msg.receiver),
            label: msg.label.clone().into_bytes(),
            data: Some(msg.payload.clone()),
            session_id: wire::decode_session_id(&msg.session_id)?,
        })
    }

    /// Decodes `tx_bytes` as a signed `bridgeERC20To`/`bridgeCETTo`/`bridgeEthTo`
    /// call and recovers the fields needed to build `sendConfirm`/`sendAbort*`
    /// later, without needing to re-simulate or re-trace the transaction.
    fn decode_bridge_call_args(tx_bytes: &[u8]) -> Result<BridgeCallArgs, SimulationError> {
        let signed: TxEnvelope = alloy::rlp::Decodable::decode(&mut &tx_bytes[..])
            .map_err(|e| SimulationError::Other(format!("failed to decode tx: {e}")))?;
        let input = signed.input();
        if input.len() < 4 {
            return Err(SimulationError::Other(
                "bridge tx input too short".to_string(),
            ));
        }

        if input[..4] == bridgeERC20ToCall::SELECTOR {
            let call = bridgeERC20ToCall::abi_decode(input).map_err(|e| {
                SimulationError::Other(format!("failed to decode bridgeERC20To call: {e}"))
            })?;
            Ok(BridgeCallArgs::Token {
                chain_dest: ChainId(u64::try_from(call.chainDest).map_err(|_| {
                    SimulationError::Other("chainDest does not fit in u64".to_string())
                })?),
                token: call.tokenSrc,
                amount: call.amount,
                receiver: call.receiver,
                session_id: call.sessionId,
            })
        } else if input[..4] == bridgeCETToCall::SELECTOR {
            let call = bridgeCETToCall::abi_decode(input).map_err(|e| {
                SimulationError::Other(format!("failed to decode bridgeCETTo call: {e}"))
            })?;
            Ok(BridgeCallArgs::Token {
                chain_dest: ChainId(u64::try_from(call.chainDest).map_err(|_| {
                    SimulationError::Other("chainDest does not fit in u64".to_string())
                })?),
                token: call.cetTokenSrc,
                amount: call.amount,
                receiver: call.receiver,
                session_id: call.sessionId,
            })
        } else if input[..4] == bridgeEthToCall::SELECTOR {
            let call = bridgeEthToCall::abi_decode(input).map_err(|e| {
                SimulationError::Other(format!("failed to decode bridgeEthTo call: {e}"))
            })?;
            Ok(BridgeCallArgs::Eth {
                chain_dest: ChainId(u64::try_from(call.chainDest).map_err(|_| {
                    SimulationError::Other("chainDest does not fit in u64".to_string())
                })?),
                amount: signed.value(),
                receiver: call.receiver,
                session_id: call.sessionId,
            })
        } else {
            Err(SimulationError::Other(
                "tx is not a bridge...To call".to_string(),
            ))
        }
    }

    /// Send a vote for the given instance.
    pub(crate) async fn send_vote(
        &self,
        instance_id: &str,
        vote: bool,
    ) -> Result<(), compose_primitives_traits::CoordinatorError> {
        let standalone_mode = !self.is_publisher_connected().await;
        let mut decision_made: Option<(bool, usize, usize)> = None;
        let mut builder_command = None;

        let instance_bytes = {
            let mut state = self.state.write().await;
            let Some(xt) = state.pending.get_mut(instance_id) else {
                xtflow!(
                    "vote_skipped",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    vote = vote,
                    reason = "xt_not_pending",
                );
                return Ok(());
            };

            // First local vote wins for the instance.
            if xt.local_vote.is_some() {
                xtflow!(
                    "vote_skipped",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    vote = vote,
                    reason = "duplicate_local_vote",
                    existing = format!("{:?}", xt.local_vote),
                );
                debug!(
                    instance_id,
                    existing_vote = ?xt.local_vote,
                    duplicate_vote = vote,
                    "Local vote already recorded, ignoring duplicate"
                );
                return Ok(());
            }

            xt.simulated_at = Some(std::time::Instant::now());
            let simulation_duration = xt
                .simulated_at
                .unwrap()
                .duration_since(xt.created_at)
                .as_secs_f64();
            info!(
                instance_id,
                simulation_duration_ms = (simulation_duration * 1000.0) as u64,
                "Finished simulating cTx:"
            );
            xt.vote_sent = true;
            xt.local_vote = Some(vote);
            if standalone_mode {
                decision_made = self.maybe_make_standalone_decision(xt);
                if let Some((decision, _, _)) = decision_made {
                    builder_command = self.local_builder_command(xt, decision);
                }
            }
            xt.instance_id.clone()
        };

        if let Some((decision, collected, expected)) = decision_made {
            info!(
                instance_id,
                decision,
                votes = collected,
                expected_votes = expected,
                "Made local decision (standalone mode)"
            );
            if let Some(m) = &self.metrics {
                m.xt_decision_latency_seconds.observe(
                    self.state
                        .read()
                        .await
                        .pending
                        .get(instance_id)
                        .map(|xt| xt.created_at.elapsed().as_secs_f64())
                        .unwrap_or(0.0),
                );
                m.xt_pending_count.dec();
            }
        }

        if let Some(command) = builder_command {
            self.apply_builder_command(command).await?;
        }

        if !standalone_mode {
            if let Some(publisher) = &self.publisher {
                if let Err(e) = publisher.send_vote(&instance_bytes, vote).await {
                    xtflow!(
                        "vote_err",
                        instance_id = instance_id,
                        chain = self.chain_id,
                        vote = vote,
                        to = "publisher",
                        error = e,
                    );
                    error!(instance_id, error = %e, "Failed to send vote to publisher");
                    if let Some(m) = &self.metrics {
                        m.vote_send_failed_total.inc();
                    }
                } else {
                    xtflow!(
                        "vote",
                        instance_id = instance_id,
                        chain = self.chain_id,
                        vote = vote,
                        to = "publisher",
                    );
                    info!(instance_id, vote, "Vote sent to publisher");
                }
            } else {
                xtflow!(
                    "vote_skipped",
                    instance_id = instance_id,
                    chain = self.chain_id,
                    vote = vote,
                    reason = "no_publisher_client",
                );
            }
        } else {
            xtflow!(
                "vote",
                instance_id = instance_id,
                chain = self.chain_id,
                vote = vote,
                to = "peers",
            );
            info!(
                instance_id,
                vote,
                chain_id = %self.chain_id,
                "Local vote recorded (standalone mode)"
            );

            // Forward vote to peers.
            if let Some(peer_coord) = &self.peer_coordinator {
                let chain_id = self.chain_id;
                let id = instance_id.to_string();
                let pc = peer_coord.clone();
                let metrics = self.metrics.clone();
                self.task_tracker.spawn(async move {
                    if let Err(e) = pc.send_vote_to_peers(&id, chain_id, vote).await {
                        error!(instance_id = %id, error = %e, "Failed to send vote to peers");
                        if let Some(m) = metrics {
                            m.vote_send_failed_total.inc();
                        }
                    }
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use alloy::primitives::U256;
    use alloy_rpc_types_eth::state::AccountOverride;
    use async_trait::async_trait;
    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use compose_mailbox::wire;
    use compose_primitives::ChainId;
    use compose_primitives::StateOverride;
    use compose_primitives::{CrossRollupDependency, SimulationResult};
    use compose_simulation::error::SimulationError;
    use compose_simulation::traits::Simulator;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle};

    use crate::coordinator::{DefaultCoordinator, TransactionChunk};
    use crate::model::chain_overlay::ChainOverlay;
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn send_vote_does_not_overwrite_existing_local_vote() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-1", true).await.unwrap();
        coordinator.send_vote("xt-77777-1", false).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn send_vote_decides_when_peer_vote_already_present() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-2".to_string(), b"xt-77777-2".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), true);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-2", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-2").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(true));
    }

    #[tokio::test]
    async fn send_vote_applies_existing_abort_peer_vote() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-3".to_string(), b"xt-77777-3".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.peer_votes.insert(ChainId(88888), false);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator.send_vote("xt-77777-3", true).await.unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-3").unwrap();
        assert_eq!(xt.local_vote, Some(true));
        assert_eq!(xt.decision, Some(false));
    }

    /// Simple stub simulator for testing: always returns success or always fails.
    struct StubSimulator {
        succeed: bool,
    }

    #[async_trait]
    impl Simulator for StubSimulator {
        async fn simulate(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            _state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            if self.succeed {
                Ok(SimulationResult {
                    success: true,
                    error: None,
                    state_overrides: None,
                    dependencies: Vec::new(),
                    outbound_messages: Vec::new(),
                })
            } else {
                Err(SimulationError::Failed("stub failure".to_string()))
            }
        }

        async fn simulate_with_mailbox(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
            _fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate(chain_id, tx, state_overrides).await
        }
    }

    struct RetryRecordingSimulator {
        calls: AtomicUsize,
        seen_overrides: Mutex<Vec<StateOverride>>,
        dependency: CrossRollupDependency,
        failed_override_addr: Address,
    }

    #[async_trait]
    impl Simulator for RetryRecordingSimulator {
        async fn simulate(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate_with_mailbox(chain_id, tx, state_overrides, &[])
                .await
        }

        async fn simulate_with_mailbox(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            state_overrides: &StateOverride,
            _fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            self.seen_overrides
                .lock()
                .unwrap()
                .push(state_overrides.clone());

            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let mut failed_overrides = StateOverride::default();
                failed_overrides.insert(
                    self.failed_override_addr,
                    AccountOverride {
                        nonce: Some(9),
                        ..Default::default()
                    },
                );
                Ok(SimulationResult {
                    success: false,
                    error: Some("missing mailbox".to_string()),
                    state_overrides: Some(failed_overrides),
                    dependencies: vec![self.dependency.clone()],
                    outbound_messages: Vec::new(),
                })
            } else {
                Ok(SimulationResult {
                    success: true,
                    error: None,
                    state_overrides: None,
                    dependencies: Vec::new(),
                    outbound_messages: Vec::new(),
                })
            }
        }
    }

    struct FailingAfterFulfilledDependencySimulator {
        calls: AtomicUsize,
        dependency: CrossRollupDependency,
    }

    #[async_trait]
    impl Simulator for FailingAfterFulfilledDependencySimulator {
        async fn simulate(
            &self,
            chain_id: ChainId,
            tx: &[u8],
            state_overrides: &StateOverride,
        ) -> Result<SimulationResult, SimulationError> {
            self.simulate_with_mailbox(chain_id, tx, state_overrides, &[])
                .await
        }

        async fn simulate_with_mailbox(
            &self,
            _chain_id: ChainId,
            _tx: &[u8],
            _state_overrides: &StateOverride,
            fulfilled_deps: &[CrossRollupDependency],
        ) -> Result<SimulationResult, SimulationError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                assert!(fulfilled_deps.is_empty());
                Ok(SimulationResult {
                    success: false,
                    error: Some("missing mailbox".to_string()),
                    state_overrides: None,
                    dependencies: vec![self.dependency.clone()],
                    outbound_messages: Vec::new(),
                })
            } else {
                assert_eq!(fulfilled_deps.len(), 1);
                Ok(SimulationResult {
                    success: false,
                    error: Some("out of gas".to_string()),
                    state_overrides: None,
                    dependencies: vec![self.dependency.clone()],
                    outbound_messages: Vec::new(),
                })
            }
        }
    }

    #[tokio::test]
    async fn process_xt_votes_true_on_success_with_no_deps() {
        let simulator = Arc::new(StubSimulator { succeed: true });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-10".to_string(), b"xt-77777-10".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .register_xt(&mut TransactionChunk {
                instance_id: "xt-77777-10".to_string(),
                ..Default::default()
            })
            .await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-10").unwrap();
        assert_eq!(xt.local_vote, Some(true));
    }

    #[tokio::test]
    async fn process_xt_votes_false_on_simulation_error() {
        let simulator = Arc::new(StubSimulator { succeed: false });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator),
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-11".to_string(), b"xt-77777-11".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xde, 0xad]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .register_xt(&mut TransactionChunk {
                instance_id: "xt-77777-11".to_string(),
                ..Default::default()
            })
            .await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-11").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }

    #[tokio::test]
    async fn process_xt_votes_false_when_no_local_txs() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
        );

        {
            let mut state = coordinator.state.write().await;
            // XT has transactions only for a different chain.
            let mut xt = PendingXt::new("xt-77777-12".to_string(), b"xt-77777-12".to_vec());
            xt.raw_txs.insert(ChainId(88888), vec![vec![0xff]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .register_xt(&mut TransactionChunk {
                instance_id: "xt-77777-12".to_string(),
                ..Default::default()
            })
            .await;

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-12").unwrap();
        assert_eq!(xt.local_vote, Some(false));
    }

    #[tokio::test]
    async fn process_xt_does_not_carry_failed_dependency_overrides_into_retry() {
        let base_addr = Address::repeat_byte(0x11);
        let failed_addr = Address::repeat_byte(0x22);
        let dependency = CrossRollupDependency {
            source_chain_id: ChainId(88888),
            dest_chain_id: ChainId(77777),
            sender: Address::repeat_byte(0x33),
            receiver: Address::repeat_byte(0x44),
            label: b"SEND".to_vec(),
            data: None,
            session_id: U256::ZERO,
        };

        let simulator = Arc::new(RetryRecordingSimulator {
            calls: AtomicUsize::new(0),
            seen_overrides: Mutex::new(Vec::new()),
            dependency: dependency.clone(),
            failed_override_addr: failed_addr,
        });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator.clone()),
            None,
            None,
            None,
            None,
            1000,
        );

        let mut base_overrides = StateOverride::default();
        base_overrides.insert(
            base_addr,
            AccountOverride {
                nonce: Some(7),
                ..Default::default()
            },
        );

        {
            let mut state = coordinator.state.write().await;
            state.chain_overlay.insert(
                ChainId(77777),
                ChainOverlay {
                    overlay: base_overrides.clone(),
                },
            );

            let mut xt = PendingXt::new("xt-77777-13".to_string(), b"xt-77777-13".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            xt.pending_mailbox.push(compose_proto::MailboxMessage {
                source_chain: 88888,
                destination_chain: 77777,
                sender: Address::repeat_byte(0x33).as_slice().to_vec(),
                receiver: Address::repeat_byte(0x44).as_slice().to_vec(),
                label: "SEND".to_string(),
                payload: vec![1, 2, 3],
                session_id: wire::encode_session_id(U256::ZERO),
                ..Default::default()
            });
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .register_xt(&mut TransactionChunk {
                instance_id: "xt-77777-13".to_string(),
                ..Default::default()
            })
            .await;

        let seen = simulator.seen_overrides.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[0].contains_key(&base_addr));
        assert!(!seen[0].contains_key(&failed_addr));
        assert!(seen[1].contains_key(&base_addr));
        assert!(
            !seen[1].contains_key(&failed_addr),
            "retry should start from base overrides, not failed-trace post-state"
        );
    }

    #[tokio::test]
    async fn process_xt_votes_false_immediately_when_failed_retry_only_has_fulfilled_deps() {
        let dependency = CrossRollupDependency {
            source_chain_id: ChainId(88888),
            dest_chain_id: ChainId(77777),
            sender: Address::repeat_byte(0x33),
            receiver: Address::repeat_byte(0x44),
            label: b"SEND".to_vec(),
            data: None,
            session_id: U256::ZERO,
        };

        let simulator = Arc::new(FailingAfterFulfilledDependencySimulator {
            calls: AtomicUsize::new(0),
            dependency: dependency.clone(),
        });
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            Some(simulator.clone()),
            None,
            None,
            None,
            None,
            10_000,
        );

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-14".to_string(), b"xt-77777-14".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![0xab, 0xcd]]);
            xt.pending_mailbox.push(compose_proto::MailboxMessage {
                source_chain: 88888,
                destination_chain: 77777,
                sender: Address::repeat_byte(0x33).as_slice().to_vec(),
                receiver: Address::repeat_byte(0x44).as_slice().to_vec(),
                label: "SEND".to_string(),
                payload: vec![1, 2, 3],
                session_id: wire::encode_session_id(U256::ZERO),
                ..Default::default()
            });
            state.pending.insert(xt.id.clone(), xt);
        }

        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            coordinator.register_xt(&mut TransactionChunk {
                instance_id: "xt-77777-14".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("register_xt should not wait for already-fulfilled deps");

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-14").unwrap();
        assert_eq!(xt.local_vote, Some(false));
        assert_eq!(xt.fulfilled_deps.len(), 1);
        assert_eq!(simulator.calls.load(Ordering::SeqCst), 2);
    }
}
