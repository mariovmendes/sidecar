//! Peer vote intake and standalone decision aggregation.

use compose_primitives::ChainId;
use tracing::info;

use crate::coordinator::DefaultCoordinator;
use compose_primitives::xtflow;
use compose_primitives_traits::CoordinatorError;

impl DefaultCoordinator {
    /// Process a vote received from a peer sidecar.
    pub async fn handle_peer_vote(
        &self,
        instance_id: &str,
        chain_id: ChainId,
        vote: bool,
    ) -> Result<(), CoordinatorError> {
        let mut state = self.state.write().await;
        let mut builder_command = None;

        let xt = state
            .pending
            .get_mut(instance_id)
            .ok_or_else(|| CoordinatorError::InstanceNotFound(instance_id.to_string()))?;

        if xt.decision.is_some() {
            info!(
                instance_id,
                peer_chain = %chain_id,
                vote,
                "Decision already recorded, ignoring peer vote"
            );
            return Ok(());
        }

        if xt.peer_votes.contains_key(&chain_id) {
            info!(
                instance_id,
                peer_chain = %chain_id,
                vote,
                "Peer vote already recorded, ignoring duplicate"
            );
            return Ok(());
        }

        xt.peer_votes.insert(chain_id, vote);

        xtflow!(
            "peer_vote_in",
            instance_id = instance_id,
            chain = self.chain_id,
            peer_chain = chain_id,
            vote = vote,
            peer_votes = xt.peer_votes.len(),
            expected_votes = xt.raw_txs.len(),
        );
        info!(
            instance_id,
            peer_chain = %chain_id,
            vote,
            "Received peer vote"
        );

        if let Some((decision, collected, expected)) = self.maybe_make_standalone_decision(xt) {
            builder_command = self.local_builder_command(xt, decision);
            info!(
                instance_id,
                decision,
                votes = collected,
                expected_votes = expected,
                "Made local decision (standalone mode)"
            );
            if let Some(m) = &self.metrics {
                m.xt_decision_latency_seconds
                    .observe(xt.created_at.elapsed().as_secs_f64());
                m.xt_pending_count.dec();
            }
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
    use crate::model::pending_xt::PendingXt;

    #[tokio::test]
    async fn duplicate_peer_vote_is_ignored() {
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
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            xt.local_vote = Some(true);
            xt.vote_sent = true;
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .handle_peer_vote("xt-77777-1", ChainId(88888), true)
            .await
            .unwrap();
        coordinator
            .handle_peer_vote("xt-77777-1", ChainId(88888), false)
            .await
            .unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-1").unwrap();
        assert_eq!(xt.peer_votes.get(&ChainId(88888)), Some(&true));
        assert_eq!(xt.decision, Some(true));
    }

    #[tokio::test]
    async fn peer_abort_vote_decides_immediately() {
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
            let mut xt = PendingXt::new("xt-77777-2".to_string(), b"xt-77777-2".to_vec());
            xt.raw_txs.insert(ChainId(77777), vec![vec![1]]);
            xt.raw_txs.insert(ChainId(88888), vec![vec![2]]);
            state.pending.insert(xt.id.clone(), xt);
        }

        coordinator
            .handle_peer_vote("xt-77777-2", ChainId(88888), false)
            .await
            .unwrap();

        let state = coordinator.state.read().await;
        let xt = state.pending.get("xt-77777-2").unwrap();
        assert_eq!(xt.decision, Some(false));
    }
}
