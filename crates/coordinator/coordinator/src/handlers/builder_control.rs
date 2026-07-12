//! Helpers for synchronizing XT lifecycle events with the local builder.

use crate::{
    coordinator::{CoordinatorState, DefaultCoordinator},
    model::pending_xt::PendingXt,
    pipeline::delivery::deps_for_chain,
};
use compose_primitives::CrossRollupDependency;
use compose_primitives_traits::CoordinatorError;
use tracing::{info, warn, error};

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

    async fn build_put_inbox_transactions(
        &self,
        dependencies: &[CrossRollupDependency],
    ) -> Result<Vec<Vec<u8>>, CoordinatorError> {
        if dependencies.is_empty() {
            return Ok(Vec::new());
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
                    return Err(error);
                }
            }
            nonce = nonce.saturating_add(1);
        }

        Ok(transactions)
    }

    pub(crate) async fn resync_put_inbox_nonce(&self) -> Result<(), CoordinatorError> {
        let Some(builder) = self.put_inbox_builder.as_ref().cloned() else {
            return Ok(());
        };

        self.nonce_manager
            .resync(move || {
                let builder = builder.clone();
                async move { builder.canonical_nonce_at().await }
            })
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
                let put_inbox_transactions =
                    self.build_put_inbox_transactions(&dependencies).await?;
                if let Err(err) = builder
                    .release_xt(&instance_id, put_inbox_transactions)
                    .await
                {
                    if let Err(resync_err) = self.resync_put_inbox_nonce().await {
                        warn!(error = %resync_err, "Failed to resync putInbox nonce after release error");
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
                    confirmed.push((xt.instance_id.clone(), instance_id.clone()));
                    info!(instance_id = %instance_id, "XT confirmed included by builder");
                } else {
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

    use crate::coordinator::VerificationConfig;

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
    }

    #[tokio::test]
    async fn confirm_included_xts_keeps_pending_for_status_polling() {
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );

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
        let coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );
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
        let mut coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );
        let builder = Arc::new(TestPutInboxBuilder::new(7));
        coordinator.set_put_inbox_builder(builder.clone());

        let dependencies = vec![test_dependency(), test_dependency()];

        let transactions = coordinator
            .build_put_inbox_transactions(&dependencies)
            .await
            .unwrap();

        assert_eq!(transactions.len(), 2);
        assert_eq!(transactions[0], 7_u64.to_be_bytes().to_vec());
        assert_eq!(transactions[1], 8_u64.to_be_bytes().to_vec());

        builder.set_canonical_nonce(11).await;
        coordinator.resync_put_inbox_nonce().await.unwrap();

        let transactions = coordinator
            .build_put_inbox_transactions(&[test_dependency()])
            .await
            .unwrap();

        assert_eq!(transactions, vec![11_u64.to_be_bytes().to_vec()]);
    }

    #[tokio::test]
    async fn monotonic_resync_keeps_locally_reserved_put_inbox_nonce() {
        let mut coordinator = DefaultCoordinator::new(
            ChainId(77777),
            None,
            None,
            None,
            None,
            None,
            1000,
            VerificationConfig::default(),
        );
        let builder = Arc::new(TestPutInboxBuilder::new(7));
        coordinator.set_put_inbox_builder(builder.clone());

        let transactions = coordinator
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

        let transactions = coordinator
            .build_put_inbox_transactions(&[test_dependency()])
            .await
            .unwrap();
        assert_eq!(transactions, vec![9_u64.to_be_bytes().to_vec()]);
    }
}
