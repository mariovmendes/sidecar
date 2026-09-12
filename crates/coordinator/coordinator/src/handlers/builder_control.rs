//! Helpers for synchronizing XT lifecycle events with the local builder.

use crate::{
    coordinator::{CoordinatorState, DefaultCoordinator},
    model::pending_xt::PendingXt,
    pipeline::delivery::deps_for_chain,
};
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::rlp::Decodable;
use alloy::sol_types::SolCall;
use compose_mailbox::contract::writeMessageCall;
use compose_primitives::{xtflow, ChainId, CrossRollupDependency};
use compose_primitives_traits::CoordinatorError;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub(crate) struct XtBuilderSubmission {
    pub instance_id: String,
    pub period_id: u64,
    pub sequence_number: u64,
    pub transactions: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) enum XtBuilderCommand {
    Release {
        instance_id: String,
        dependencies: Vec<CrossRollupDependency>,
    },
    Abort {
        instance_id: String,
    },
}

impl DefaultCoordinator {
    pub(crate) fn local_builder_submission(&self, xt: &PendingXt) -> Option<XtBuilderSubmission> {
        let transactions = xt.raw_txs.get(&self.chain_id)?.clone();
        if transactions.is_empty() {
            return None;
        }

        let sequence_number = if xt.sequence_num.0 != 0 {
            xt.sequence_num.0
        } else {
            xt.origin_seq.0
        };

        Some(XtBuilderSubmission {
            instance_id: xt.id.to_string(),
            period_id: xt.period_id.0,
            sequence_number,
            transactions,
        })
    }

    pub(crate) fn local_builder_command(
        &self,
        xt: &PendingXt,
        decision: bool,
    ) -> Option<XtBuilderCommand> {
        if xt
            .raw_txs
            .get(&self.chain_id)
            .is_none_or(|transactions| transactions.is_empty())
        {
            return None;
        }

        if decision {
            Some(XtBuilderCommand::Release {
                instance_id: xt.id.to_string(),
                dependencies: deps_for_chain(&xt.fulfilled_deps, self.chain_id),
            })
        } else {
            Some(XtBuilderCommand::Abort {
                instance_id: xt.id.to_string(),
            })
        }
    }

    pub(crate) async fn submit_xt_to_builder(
        &self,
        submission: XtBuilderSubmission,
    ) -> Result<(), CoordinatorError> {
        if let Some(builder) = &self.xt_builder_client {
            builder
                .submit_locked_xt(
                    &submission.instance_id,
                    submission.period_id,
                    submission.sequence_number,
                    submission.transactions,
                )
                .await?;
        }
        Ok(())
    }

    /// Release `transactions` for `instance_id` through the builder's
    /// `releaseXt` endpoint, resyncing the coordinator nonce if the release
    /// call fails.
    async fn release_to_builder(
        &self,
        instance_id: &str,
        transactions: Vec<Vec<u8>>,
        reserved: Option<(u64, usize)>,
    ) -> Result<(), CoordinatorError> {
        let Some(builder) = &self.xt_builder_client else {
            return Ok(());
        };

        xtflow!(
            "builder_release",
            instance_id = instance_id,
            chain = self.chain_id,
            call = "ethera_releaseXt",
            tx_count = transactions.len(),
            txs = crate::pipeline::delivery::describe_local_txs(self.chain_id, &transactions),
        );
        if let Err(err) = builder.release_xt(instance_id, transactions).await {
            xtflow!(
                "builder_release_err",
                instance_id = instance_id,
                chain = self.chain_id,
                call = "ethera_releaseXt",
                error = err,
            );
            if let Some((start, count)) = reserved {
                self.reconcile_nonce_after_rejection(&err, start, count, "release_rejected")
                    .await;
            }
            return Err(err);
        }
        xtflow!(
            "builder_release_ok",
            instance_id = instance_id,
            chain = self.chain_id,
            call = "ethera_releaseXt",
        );

        Ok(())
    }

    /// Build one `putInbox` per dependency on a contiguous nonce range,
    /// returning the transactions and the first nonce of the range.
    async fn build_put_inbox_transactions(
        &self,
        dependencies: &[CrossRollupDependency],
    ) -> Result<(Vec<Vec<u8>>, u64), CoordinatorError> {
        if dependencies.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let builder = self
            .put_inbox_builder
            .as_ref()
            .cloned()
            .ok_or(CoordinatorError::PutInboxNotConfigured)?;
        let nonce_builder = builder.clone();
        let start_nonce = self
            .nonce_manager
            .reserve(dependencies.len(), move || {
                let builder = nonce_builder.clone();
                async move { builder.canonical_nonce_at().await }
            })
            .await?;

        let mut nonce = start_nonce;
        let mut transactions = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            let build_started = std::time::Instant::now();
            match builder
                .build_put_inbox_tx_with_nonce(dependency, nonce)
                .await
            {
                Ok(transaction) => {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .put_inbox_build_duration_seconds
                            .observe(build_started.elapsed().as_secs_f64());
                    }
                    transactions.push(transaction);
                }
                Err(error) => {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .put_inbox_build_duration_seconds
                            .observe(build_started.elapsed().as_secs_f64());
                        metrics.put_inbox_build_error_total.inc();
                    }
                    // None of the range was submitted, so give all of it back.
                    self.recycle_nonce(
                        start_nonce,
                        dependencies.len(),
                        "put_inbox_batch_build_failed",
                    )
                    .await;
                    return Err(error);
                }
            }
            nonce = nonce.saturating_add(1);
        }

        Ok((transactions, start_nonce))
    }

    /// Build a single `putInbox` transaction, returning it together with the
    /// nonce it consumed so the caller can hand that nonce back if the
    /// transaction never makes it into the builder's pool.
    pub(crate) async fn build_put_inbox_transaction_with_nonce(
        &self,
        dependency: &CrossRollupDependency,
    ) -> Result<(Vec<u8>, u64), CoordinatorError> {
        let builder = self
            .put_inbox_builder
            .as_ref()
            .cloned()
            .ok_or(CoordinatorError::PutInboxNotConfigured)?;
        let nonce_builder = builder.clone();
        let nonce = self
            .nonce_manager
            .reserve(1, move || {
                let builder = nonce_builder.clone();
                async move { builder.canonical_nonce_at().await }
            })
            .await?;

        let build_started = std::time::Instant::now();
        match builder
            .build_put_inbox_tx_with_nonce(dependency, nonce)
            .await
        {
            Ok(transaction) => {
                if let Some(metrics) = &self.metrics {
                    metrics
                        .put_inbox_build_duration_seconds
                        .observe(build_started.elapsed().as_secs_f64());
                }
                Ok((transaction, nonce))
            }
            Err(error) => {
                if let Some(metrics) = &self.metrics {
                    metrics
                        .put_inbox_build_duration_seconds
                        .observe(build_started.elapsed().as_secs_f64());
                    metrics.put_inbox_build_error_total.inc();
                }
                // Nothing was signed, so nothing can have reached the builder.
                self.recycle_nonce(nonce, 1, "put_inbox_build_failed").await;
                Err(error)
            }
        }
    }

    /// Reconcile the coordinator nonce after the builder refused a submission.
    ///
    /// A nonce-gap rejection carries the builder's own expected value, and that
    /// answer wins: snapping to it repairs the desync on the next attempt no
    /// matter how it arose. Recycling the refused nonce instead would re-offer
    /// the same rejected value forever. Any other rejection means the transaction never
    /// entered the pool, so its nonce is simply handed back.
    pub(crate) async fn reconcile_nonce_after_rejection(
        &self,
        error: &CoordinatorError,
        start: u64,
        count: usize,
        reason: &str,
    ) {
        if let Some(expected) = error.expected_nonce() {
            self.nonce_manager.force_set(expected).await;
            xtflow!(
                "nonce_resync",
                chain = self.chain_id,
                offered = start,
                expected = expected,
                reason = reason,
            );
            return;
        }

        if error.is_builder_rejection() {
            self.recycle_nonce(start, count, reason).await;
        }

        if let Err(resync_err) = self.resync_put_inbox_nonce_monotonic().await {
            warn!(error = %resync_err, "Failed to resync coordinator nonce after rejection");
        }
    }

    /// Hand a reserved nonce back to the manager and record it, so a dropped
    /// coordinator transaction does not leave a permanent hole in the shared
    /// nonce sequence.
    pub(crate) async fn recycle_nonce(&self, start: u64, count: usize, reason: &str) {
        self.nonce_manager.release(start, count).await;
        xtflow!(
            "nonce_recycled",
            chain = self.chain_id,
            nonce = start,
            count = count,
            reason = reason,
        );
    }

    /// Decodes `tx_bytes` as a signed `writeMessage(Message)` transaction, derives the
    /// resulting `CrossRollupDependency` from its header/payload, builds the matching
    /// putInbox transaction, and submits it directly to the builder via
    /// `ethera_submitFollowup` (bypassing the batched `releaseXt` path).
    pub(crate) async fn submit_put_inbox_for_tx(
        &self,
        instance_id: &str,
        tx_bytes: &Vec<u8>,
    ) -> Result<(), CoordinatorError> {
        let dependency = self.decode_write_message_dependency(tx_bytes)?;
        self.submit_put_inbox_dependency(instance_id, dependency)
            .await
    }

    /// Builds and submits a putInbox transaction directly from an already-known
    /// `CrossRollupDependency` (e.g. an ACK reported by a peer sidecar over HTTP),
    /// skipping tx decoding entirely.
    pub async fn handle_dependency(
        &self,
        instance_id: &str,
        dependency: CrossRollupDependency,
    ) -> Result<(), CoordinatorError> {
        self.submit_put_inbox_dependency(instance_id, dependency)
            .await
    }

    async fn submit_put_inbox_dependency(
        &self,
        instance_id: &str,
        dependency: CrossRollupDependency,
    ) -> Result<(), CoordinatorError> {
        let (transaction, nonce) = match self
            .build_put_inbox_transaction_with_nonce(&dependency)
            .await
        {
            Ok(built) => built,
            Err(error) => {
                if let Err(resync_err) = self.resync_put_inbox_nonce_monotonic().await {
                    warn!(error = %resync_err, "Failed to resync putInbox nonce after build error");
                }
                return Err(error);
            }
        };

        let Some(builder) = &self.xt_builder_client else {
            self.recycle_nonce(nonce, 1, "no_builder_client").await;
            return Err(CoordinatorError::Other(
                "xt builder client not configured".to_string(),
            ));
        };

        xtflow!(
            "builder_followup",
            instance_id = instance_id,
            chain = self.chain_id,
            call = "ethera_submitFollowup",
            txs = crate::pipeline::delivery::describe_tx(self.chain_id, &transaction),
        );
        if let Err(err) = builder
            .submit_followup_xt(instance_id, vec![transaction])
            .await
        {
            xtflow!(
                "builder_followup_err",
                instance_id = instance_id,
                chain = self.chain_id,
                call = "ethera_submitFollowup",
                error = err,
            );
            self.reconcile_nonce_after_rejection(&err, nonce, 1, "followup_rejected")
                .await;
            return Err(err);
        }
        xtflow!(
            "builder_followup_ok",
            instance_id = instance_id,
            chain = self.chain_id,
            call = "ethera_submitFollowup",
        );

        Ok(())
    }

    fn decode_write_message_dependency(
        &self,
        tx_bytes: &[u8],
    ) -> Result<CrossRollupDependency, CoordinatorError> {
        let signed: TxEnvelope = Decodable::decode(&mut &tx_bytes[..])
            .map_err(|e| CoordinatorError::Other(format!("failed to decode tx: {e}")))?;

        let input = signed.input();
        if input.len() < 4 || input[..4] != writeMessageCall::SELECTOR {
            return Err(CoordinatorError::Other(
                "transaction is not a writeMessage call".to_string(),
            ));
        }

        let call = writeMessageCall::abi_decode(input).map_err(|e| {
            CoordinatorError::Other(format!("failed to decode writeMessage call: {e}"))
        })?;
        let header = &call.message.header;
        let dest_chain_id = u64::try_from(header.chainDest)
            .map_err(|_| CoordinatorError::Other("chainDest does not fit in u64".to_string()))?;

        Ok(CrossRollupDependency {
            source_chain_id: self.chain_id,
            dest_chain_id: ChainId(dest_chain_id),
            sender: header.sender,
            receiver: header.receiver,
            label: header.label.clone().into_bytes(),
            data: Some(call.message.payload.to_vec()),
            session_id: header.sessionId,
        })
    }

    /// Reserve the next nonce(s) for the shared coordinator signer (the same
    /// account/nonce space used for `putInbox`), build an `L2BridgeTxBuilder`
    /// transaction with the first nonce and, if `remove_inbox_dependency` is
    /// given, a matching `removeInbox` compensation transaction with the
    /// following nonce, then release both to the builder in a single
    /// `releaseXt` call, resyncing the nonce on any build or release failure.
    async fn release_l2_bridge_tx<F, Fut>(
        &self,
        instance_id: &str,
        build: F,
        remove_inbox_dependency: Option<&CrossRollupDependency>,
    ) -> Result<(), CoordinatorError>
    where
        F: FnOnce(std::sync::Arc<dyn compose_primitives_traits::L2BridgeTxBuilder>, u64) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, CoordinatorError>>,
    {
        let l2_builder = self.l2_bridge_builder.as_ref().cloned().ok_or_else(|| {
            CoordinatorError::Other("l2 bridge builder not configured".to_string())
        })?;

        let nonce_count = if remove_inbox_dependency.is_some() {
            2
        } else {
            1
        };
        let nonce_builder = l2_builder.clone();
        let start_nonce = self
            .nonce_manager
            .reserve(nonce_count, move || {
                let nonce_builder = nonce_builder.clone();
                async move { nonce_builder.canonical_nonce_at().await }
            })
            .await?;

        let tx = match build(l2_builder, start_nonce).await {
            Ok(tx) => tx,
            Err(error) => {
                self.recycle_nonce(start_nonce, nonce_count, "l2_bridge_build_failed")
                    .await;
                if let Err(resync_err) = self.resync_put_inbox_nonce_monotonic().await {
                    warn!(error = %resync_err, "Failed to resync coordinator nonce after l2 bridge build error");
                }
                return Err(error);
            }
        };
        let mut transactions = vec![tx];

        if let Some(dependency) = remove_inbox_dependency {
            let Some(put_inbox_builder) = self.put_inbox_builder.as_ref().cloned() else {
                self.recycle_nonce(start_nonce, nonce_count, "put_inbox_not_configured")
                    .await;
                return Err(CoordinatorError::PutInboxNotConfigured);
            };
            match put_inbox_builder
                .build_remove_inbox_tx_with_nonce(dependency, start_nonce.saturating_add(1))
                .await
            {
                Ok(tx) => transactions.push(tx),
                Err(error) => {
                    self.recycle_nonce(start_nonce, nonce_count, "remove_inbox_build_failed")
                        .await;
                    if let Err(resync_err) = self.resync_put_inbox_nonce_monotonic().await {
                        warn!(error = %resync_err, "Failed to resync coordinator nonce after removeInbox build error");
                    }
                    return Err(error);
                }
            }
        }

        self.release_to_builder(instance_id, transactions, Some((start_nonce, nonce_count)))
            .await
    }

    /// Submit `sendConfirm(sendHeader)` — sender-side finalize once the ACK
    /// has been relayed back. `header` must be the exact header used when the
    /// original SEND message was written.
    pub(crate) async fn submit_send_confirm(
        &self,
        instance_id: &str,
        header: &CrossRollupDependency,
    ) -> Result<(), CoordinatorError> {
        let header = header.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move { builder.build_send_confirm_tx(&header, nonce).await },
            None,
        )
        .await
    }

    /// Submit `sendAbortToken(...)` — sender-side compensation for a rejected
    /// ERC20/CET send, refunding `params.sender`. If `remove_inbox_dependency`
    /// is given, the compensating `removeInbox` transaction is built and
    /// released together with the abort transaction.
    pub(crate) async fn submit_send_abort_token(
        &self,
        instance_id: &str,
        params: &compose_primitives_traits::SendAbortTokenParams,
        remove_inbox_dependency: Option<&CrossRollupDependency>,
    ) -> Result<(), CoordinatorError> {
        let params = params.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move { builder.build_send_abort_token_tx(&params, nonce).await },
            remove_inbox_dependency,
        )
        .await
    }

    /// Submit `sendAbortETH(...)` — sender-side compensation for a rejected
    /// ETH send, refunding `params.sender`. If `remove_inbox_dependency` is
    /// given, the compensating `removeInbox` transaction is built and
    /// released together with the abort transaction.
    pub(crate) async fn submit_send_abort_eth(
        &self,
        instance_id: &str,
        params: &compose_primitives_traits::SendAbortEthParams,
        remove_inbox_dependency: Option<&CrossRollupDependency>,
    ) -> Result<(), CoordinatorError> {
        let params = params.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move { builder.build_send_abort_eth_tx(&params, nonce).await },
            remove_inbox_dependency,
        )
        .await
    }

    /// Submit `recvConfirmToken(msgHeader)` — receiver-side finalize once the
    /// SEND has been marked consumed. `header` is the header of the original
    /// inbound SEND message.
    pub(crate) async fn submit_recv_confirm_token(
        &self,
        instance_id: &str,
        header: &CrossRollupDependency,
    ) -> Result<(), CoordinatorError> {
        let header = header.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move {
                builder.build_recv_confirm_token_tx(&header, nonce).await
            },
            None,
        )
        .await
    }

    /// Submit `recvAbortToken(msgHeader)` — receiver-side compensation for a
    /// rejected token receive. If `remove_inbox_dependency` is given, the
    /// compensating `removeInbox` transaction is built and released together
    /// with the abort transaction.
    pub(crate) async fn submit_recv_abort_token(
        &self,
        instance_id: &str,
        header: &CrossRollupDependency,
        remove_inbox_dependency: Option<&CrossRollupDependency>,
    ) -> Result<(), CoordinatorError> {
        let header = header.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move { builder.build_recv_abort_token_tx(&header, nonce).await },
            remove_inbox_dependency,
        )
        .await
    }

    /// Submit `recvConfirmETH(msgHeader)` — receiver-side finalize for an ETH
    /// receive.
    pub(crate) async fn submit_recv_confirm_eth(
        &self,
        instance_id: &str,
        header: &CrossRollupDependency,
    ) -> Result<(), CoordinatorError> {
        let header = header.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move { builder.build_recv_confirm_eth_tx(&header, nonce).await },
            None,
        )
        .await
    }

    /// Submit `recvAbortETH(msgHeader)` — receiver-side compensation for a
    /// rejected ETH receive. If `remove_inbox_dependency` is given, the
    /// compensating `removeInbox` transaction is built and released together
    /// with the abort transaction.
    pub(crate) async fn submit_recv_abort_eth(
        &self,
        instance_id: &str,
        header: &CrossRollupDependency,
        remove_inbox_dependency: Option<&CrossRollupDependency>,
    ) -> Result<(), CoordinatorError> {
        let header = header.clone();
        self.release_l2_bridge_tx(
            instance_id,
            move |builder, nonce| async move { builder.build_recv_abort_eth_tx(&header, nonce).await },
            remove_inbox_dependency,
        )
        .await
    }

    pub(crate) async fn resync_put_inbox_nonce_monotonic(&self) -> Result<(), CoordinatorError> {
        let Some(builder) = self.put_inbox_builder.as_ref().cloned() else {
            return Ok(());
        };

        self.nonce_manager
            .resync_monotonic(move || {
                let builder = builder.clone();
                async move { builder.canonical_nonce_at().await }
            })
            .await
    }

    pub(crate) async fn apply_builder_command(
        &self,
        command: XtBuilderCommand,
    ) -> Result<(), CoordinatorError> {
        let Some(builder) = &self.xt_builder_client else {
            return Ok(());
        };

        match command {
            XtBuilderCommand::Release {
                instance_id,
                dependencies,
            } => {
                let (put_inbox_transactions, start_nonce) =
                    self.build_put_inbox_transactions(&dependencies).await?;
                let count = put_inbox_transactions.len();
                if let Err(err) = builder
                    .release_xt(&instance_id, put_inbox_transactions)
                    .await
                {
                    if count > 0 {
                        self.reconcile_nonce_after_rejection(
                            &err,
                            start_nonce,
                            count,
                            "release_rejected",
                        )
                        .await;
                    }
                    return Err(err);
                }
            }
            XtBuilderCommand::Abort { instance_id } => {
                builder.abort_xt(&instance_id).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn remove_pending_xt(&self, instance_id: &str) -> bool {
        let mut state = self.state.write().await;
        Self::remove_pending_xt_from_state(&mut state, instance_id)
    }

    /// Handle `POST /ethera/failed`: the builder's EVM refused a transaction
    /// in `instance_id` and has quarantined the instance.
    ///
    /// The builder has stopped *selecting* the instance but has deliberately
    /// not forgotten it — retiring it is ours to do, because
    /// [`CoordinatorError::is_unknown_instance`] reads a forgotten instance as
    /// proof that our own compensation already landed.
    ///
    /// The instance is always aborted, whether or not part of it executed.
    /// The round is lost either way, and the reservations it holds are not
    /// only its own problem: the coordinator signs into every instance, so a
    /// coordinator nonce left stuck here stalls *every* later cross-chain
    /// round on this chain. The round is dead; the chain still has to move.
    ///
    /// `executed` lists the transactions of this instance that ran before the
    /// refusal. It does not change the decision, only what the failure means:
    ///
    /// - **Empty** — nothing reached the chain, so no funds are escrowed (the
    ///   same reasoning as `Compensation::NotApplicable`).
    /// - **Non-empty** — part of the round is on chain and the rest can never
    ///   execute. That state is already out of sync with the counterpart
    ///   chain and no replay repairs it, so there is no compensation to run.
    ///   A later `release_xt` will return `UnknownInstance` and the decision
    ///   pipeline will treat the round as settled, which is the right outcome
    ///   here — logged separately so the case stays visible rather than
    ///   silently inferred.
    ///
    /// Nonces are not recycled explicitly. After the abort the builder no
    /// longer reserves them, so its `true_next_nonce` falls back to the hole
    /// and the next submission is refused with a nonce gap carrying the
    /// builder's own expected value — which `reconcile_nonce_after_rejection`
    /// turns into a `force_set`. That repair uses the builder's answer instead
    /// of our guess, so it cannot recycle a nonce that actually executed.
    pub async fn handle_failed_xt(
        &self,
        instance_id: &str,
        executed: &[String],
        reason: &str,
    ) -> Result<(), CoordinatorError> {
        if let Some(metrics) = &self.metrics {
            metrics.xt_unrecoverable_total.inc();
        }

        xtflow!(
            "builder_unrecoverable",
            instance_id = instance_id,
            chain = self.chain_id,
            executed = executed.len(),
            partial = !executed.is_empty(),
            reason = reason,
        );

        let Some(builder) = &self.xt_builder_client else {
            error!(
                instance_id = %instance_id,
                executed = executed.len(),
                %reason,
                "Builder reported an unrecoverable XT but no builder client is configured; \
                 its nonce reservations cannot be released"
            );
            return Ok(());
        };

        if executed.is_empty() {
            error!(
                instance_id = %instance_id,
                %reason,
                "Builder quarantined an XT that never reached the chain; aborting it"
            );
        } else {
            error!(
                instance_id = %instance_id,
                executed = executed.len(),
                %reason,
                "Builder quarantined a partially executed XT; the round is unrecoverable \
                 and this chain's state now diverges from its counterpart. Aborting to \
                 release the nonces it holds"
            );
        }

        builder.abort_xt(instance_id).await?;
        xtflow!(
            "builder_unrecoverable_aborted",
            instance_id = instance_id,
            chain = self.chain_id,
            executed = executed.len(),
            partial = !executed.is_empty(),
        );

        Ok(())
    }

    pub async fn confirm_included_xts(
        &self,
        instance_ids: &[String],
    ) -> Result<(), CoordinatorError> {
        // 1. Update local state under write lock
        let confirmed: Vec<(Vec<u8>, String)> = {
            let mut state = self.state.write().await;
            let now = std::time::Instant::now();
            let mut confirmed = Vec::new();
            for instance_id in instance_ids {
                if let Some(xt) = state.pending.get_mut(instance_id.as_str()) {
                    xt.confirmed_at = Some(now);
                    xtflow!(
                        "included",
                        instance_id = instance_id,
                        chain = self.chain_id,
                        age_ms = xt.created_at.elapsed().as_millis(),
                        decision = format!("{:?}", xt.decision),
                    );
                    confirmed.push((xt.instance_id.clone(), instance_id.clone()));
                    info!(instance_id = %instance_id, "XT confirmed included by builder");
                } else {
                    xtflow!(
                        "included_unknown",
                        instance_id = instance_id,
                        chain = self.chain_id,
                    );
                    warn!(instance_id = %instance_id, "confirm received for unknown XT");
                }
            }
            confirmed
        }; // write lock released here

        // 2. Forward confirmations to the publisher (best-effort)
        if let Some(publisher) = &self.publisher {
            if publisher.is_connected() {
                for (instance_id_bytes, instance_id_str) in &confirmed {
                    if let Err(e) = publisher
                        .send_confirmed(instance_id_bytes, self.chain_id.0)
                        .await
                    {
                        error!(
                            instance_id = %instance_id_str,
                            error = %e,
                            "Failed to send confirmed to publisher"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn remove_pending_xt_from_state(state: &mut CoordinatorState, instance_id: &str) -> bool {
        let Some(xt) = state.pending.remove(instance_id) else {
            return false;
        };

        state.mailbox_index.remove(xt.instance_id.as_slice());
        state
            .submitted_fingerprints
            .retain(|_, pending_id| pending_id.as_str() != instance_id);
        state.pending_submissions.retain(|_, waiters| {
            waiters.retain(|sender| !sender.is_closed());
            !waiters.is_empty()
        });

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use alloy::primitives::{Address, U256};
    use async_trait::async_trait;
    use compose_primitives::{ChainId, PeriodId, SequenceNumber};
    use compose_primitives_traits::PutInboxBuilder;
    use tokio::sync::Mutex;
    #[derive(Debug)]
    struct TestPutInboxBuilder {
        canonical_nonce: Mutex<u64>,
    }

    impl TestPutInboxBuilder {
        fn new(canonical_nonce: u64) -> Self {
            Self {
                canonical_nonce: Mutex::new(canonical_nonce),
            }
        }

        async fn set_canonical_nonce(&self, nonce: u64) {
            *self.canonical_nonce.lock().await = nonce;
        }
    }

    fn test_dependency() -> CrossRollupDependency {
        CrossRollupDependency {
            source_chain_id: ChainId(177778),
            dest_chain_id: ChainId(188889),
            sender: Address::ZERO,
            receiver: Address::ZERO,
            label: b"dep".to_vec(),
            data: None,
            session_id: U256::ZERO,
        }
    }

    #[async_trait]
    impl PutInboxBuilder for TestPutInboxBuilder {
        fn signer_address(&self) -> Address {
            Address::ZERO
        }

        async fn canonical_nonce_at(&self) -> Result<u64, CoordinatorError> {
            Ok(*self.canonical_nonce.lock().await)
        }

        async fn build_put_inbox_tx_with_nonce(
            &self,
            _dep: &CrossRollupDependency,
            nonce: u64,
        ) -> Result<Vec<u8>, CoordinatorError> {
            Ok(nonce.to_be_bytes().to_vec())
        }

        async fn build_remove_inbox_tx_with_nonce(
            &self,
            _dep: &CrossRollupDependency,
            nonce: u64,
        ) -> Result<Vec<u8>, CoordinatorError> {
            Ok(nonce.to_be_bytes().to_vec())
        }
    }

    #[tokio::test]
    async fn confirm_included_xts_keeps_pending_for_status_polling() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);

        {
            let mut state = coordinator.state.write().await;
            let mut xt = PendingXt::new("xt-77777-1".to_string(), b"xt-77777-1".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            state
                .mailbox_index
                .insert(xt.instance_id.clone(), xt.id.clone());
            state
                .submitted_fingerprints
                .insert("fp-1".to_string(), xt.id.clone());
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .confirm_included_xts(&["xt-77777-1".to_string()])
            .await
            .unwrap();

        // XTs must remain in pending so GET /xt/:id continues to return their
        // committed status while callers poll WaitForDecision. The cleanup loop
        // removes decided XTs after they age out (decided_at + max_age).
        let state = coordinator.state.read().await;
        assert!(state.pending.contains_key("xt-77777-1"));
        assert!(state.mailbox_index.contains_key(b"xt-77777-1".as_slice()));
        assert!(state.submitted_fingerprints.contains_key("fp-1"));
    }

    #[test]
    fn local_builder_submission_prefers_publisher_sequence() {
        let coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        let mut xt = PendingXt::new("xt-77777-2".to_string(), b"xt-77777-2".to_vec());
        xt.period_id = PeriodId(11);
        xt.sequence_num = SequenceNumber(7);
        xt.origin_seq = SequenceNumber(3);
        xt.raw_txs.insert(ChainId(77777), vec![vec![1], vec![2]]);

        let submission = coordinator.local_builder_submission(&xt).unwrap();
        assert_eq!(submission.period_id, 11);
        assert_eq!(submission.sequence_number, 7);
        assert_eq!(submission.transactions.len(), 2);
    }

    #[tokio::test]
    async fn build_put_inbox_transactions_uses_canonical_nonce_source() {
        let mut coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        let builder = Arc::new(TestPutInboxBuilder::new(7));
        coordinator.set_put_inbox_builder(builder.clone());

        let dependencies = vec![test_dependency(), test_dependency()];

        let (transactions, start_nonce) = coordinator
            .build_put_inbox_transactions(&dependencies)
            .await
            .unwrap();

        assert_eq!(start_nonce, 7);
        assert_eq!(transactions.len(), 2);
        assert_eq!(transactions[0], 7_u64.to_be_bytes().to_vec());
        assert_eq!(transactions[1], 8_u64.to_be_bytes().to_vec());

        // The chain moved on past the locally reserved range.
        builder.set_canonical_nonce(11).await;
        coordinator
            .resync_put_inbox_nonce_monotonic()
            .await
            .unwrap();

        let (transactions, _) = coordinator
            .build_put_inbox_transactions(&[test_dependency()])
            .await
            .unwrap();

        assert_eq!(transactions, vec![11_u64.to_be_bytes().to_vec()]);
    }

    #[tokio::test]
    async fn recycled_nonce_is_reused_instead_of_leaving_a_hole() {
        // The chain-A freeze in shape: a coordinator transaction reserves a
        // nonce, its submission is rejected, and later transactions have
        // already taken the nonces above it. Unless the rejected nonce is
        // handed back, the builder's coordinator cursor stops there and every
        // later cross-chain transaction on the chain is stuck behind it.
        let mut coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        coordinator.set_put_inbox_builder(Arc::new(TestPutInboxBuilder::new(20)));

        let (_, first) = coordinator
            .build_put_inbox_transaction_with_nonce(&test_dependency())
            .await
            .unwrap();
        let (_, second) = coordinator
            .build_put_inbox_transaction_with_nonce(&test_dependency())
            .await
            .unwrap();
        assert_eq!((first, second), (20, 21));

        coordinator.recycle_nonce(first, 1, "test").await;

        let (_, refilled) = coordinator
            .build_put_inbox_transaction_with_nonce(&test_dependency())
            .await
            .unwrap();
        assert_eq!(refilled, 20, "the hole must be refilled before extending");

        let (_, next) = coordinator
            .build_put_inbox_transaction_with_nonce(&test_dependency())
            .await
            .unwrap();
        assert_eq!(next, 22);
    }

    #[tokio::test]
    async fn monotonic_resync_keeps_locally_reserved_put_inbox_nonce() {
        let mut coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        let builder = Arc::new(TestPutInboxBuilder::new(7));
        coordinator.set_put_inbox_builder(builder.clone());

        let (transactions, _) = coordinator
            .build_put_inbox_transactions(&[test_dependency(), test_dependency()])
            .await
            .unwrap();
        assert_eq!(transactions[0], 7_u64.to_be_bytes().to_vec());
        assert_eq!(transactions[1], 8_u64.to_be_bytes().to_vec());

        builder.set_canonical_nonce(8).await;
        coordinator
            .resync_put_inbox_nonce_monotonic()
            .await
            .unwrap();

        let (transactions, _) = coordinator
            .build_put_inbox_transactions(&[test_dependency()])
            .await
            .unwrap();
        assert_eq!(transactions, vec![9_u64.to_be_bytes().to_vec()]);
    }

    #[derive(Debug, Default)]
    struct AbortRecordingBuilderClient {
        aborted: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl compose_primitives_traits::XtBuilderClient for AbortRecordingBuilderClient {
        async fn submit_locked_xt(
            &self,
            _instance_id: &str,
            _period_id: u64,
            _sequence_number: u64,
            _transactions: Vec<Vec<u8>>,
        ) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn submit_tx(&self, _tx: &[u8]) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn submit_followup_xt(
            &self,
            _instance_id: &str,
            _put_inbox_transactions: Vec<Vec<u8>>,
        ) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn release_xt(
            &self,
            _instance_id: &str,
            _put_inbox_transactions: Vec<Vec<u8>>,
        ) -> Result<(), CoordinatorError> {
            Ok(())
        }

        async fn abort_xt(&self, instance_id: &str) -> Result<(), CoordinatorError> {
            self.aborted.lock().await.push(instance_id.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn unrecoverable_xt_that_never_reached_the_chain_is_aborted() {
        // Nothing executed, so no funds are escrowed and the round cannot
        // proceed. Aborting is what releases the nonces the builder is still
        // holding for it.
        let mut coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        let builder = Arc::new(AbortRecordingBuilderClient::default());
        coordinator.set_xt_builder_client(builder.clone());

        coordinator
            .handle_failed_xt("xt-77777-1", &[], "EVM refused a transaction")
            .await
            .unwrap();

        assert_eq!(
            builder.aborted.lock().await.as_slice(),
            ["xt-77777-1".to_string()]
        );
    }

    #[tokio::test]
    async fn partially_executed_unrecoverable_xt_is_also_aborted() {
        // Part of this round is on chain and the rest can never execute, so
        // the round is lost and no compensation applies. The abort still has
        // to happen: the coordinator signs into every instance, so a nonce
        // left stuck here would stall every later cross-chain round.
        let mut coordinator =
            DefaultCoordinator::new(ChainId(77777), None, None, None, None, None, 1000);
        let builder = Arc::new(AbortRecordingBuilderClient::default());
        coordinator.set_xt_builder_client(builder.clone());

        coordinator
            .handle_failed_xt(
                "xt-77777-2",
                &["0xdeadbeef".to_string()],
                "EVM refused a transaction",
            )
            .await
            .unwrap();

        assert_eq!(
            builder.aborted.lock().await.as_slice(),
            ["xt-77777-2".to_string()],
            "a stuck coordinator nonce must be released even when the round is lost"
        );
    }
}
