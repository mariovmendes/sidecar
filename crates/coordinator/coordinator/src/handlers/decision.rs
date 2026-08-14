//! Decision message handling and XT finalization updates.

use tracing::info;

use crate::coordinator::DefaultCoordinator;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Record a commit/abort decision for an instance.
    pub async fn on_decision(
        &self,
        instance_id: &str,
        decision: bool,
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;

        let xt = state
            .pending
            .get_mut(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        if xt.decision.is_some() {
            info!(instance_id, "Decision already recorded, ignoring duplicate");
            return Ok(());
        }

        let chain_ids: Vec<_> = xt.raw_txs.keys().copied().collect();
        let latency = xt.created_at.elapsed();
        xt.record_decision(decision);
        let builder_command = self.local_builder_command(xt, decision);

        info!(instance_id, decision, "Decision received");

        if !decision {
            for chain_id in &chain_ids {
                // TODO: Remove this part
                state.chain_overlay.remove(chain_id);
            }
        }

        if let Some(m) = &self.metrics {
            if decision {
                m.xt_decided_commit_total.inc();
            } else {
                m.xt_decided_abort_total.inc();
            }
            m.xt_decision_latency_seconds.observe(latency.as_secs_f64());
            m.xt_pending_count.dec();
        }

        drop(state);

        if let Some(command) = builder_command {
            self.apply_builder_command(command).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::ChainId;

    use crate::coordinator::{DefaultCoordinator, VerificationConfig};
    use crate::model::chain_overlay::ChainOverlay;
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn on_decision_ignores_duplicate() {
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
            state.pending.insert(xt.id.clone(), xt);
        }

        // First decision should succeed.
        coordinator
            .on_decision("xt-77777-1", true)
            .await
            .expect("first decision should succeed");

        // Second (duplicate) decision should also return Ok without error.
        coordinator
            .on_decision("xt-77777-1", true)
            .await
            .expect("duplicate decision should be silently ignored");

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.decision, Some(true));
    }

    #[tokio::test]
    async fn on_decision_abort_clears_chain_overlay() {
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
            state.pending.insert(xt.id.clone(), xt);

            state
                .chain_overlay
                .insert(ChainId(77777), ChainOverlay::new());
        }

        coordinator
            .on_decision("xt-77777-1", false)
            .await
            .expect("abort decision should succeed");

        let state = coordinator.state.read().await;
        assert!(
            !state.chain_overlay.contains_key(&ChainId(77777)),
            "chain overlay should be cleared on abort"
        );
    }
}
