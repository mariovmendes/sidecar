//! Builder for constructing coordinator instances with pluggable dependencies.

use std::sync::Arc;

use compose_mailbox::traits::MailboxQueue;
use compose_peer::traits::PeerCoordinator;
use compose_primitives::ChainId;
use compose_simulation::traits::Simulator;
use reqwest::Url;

use compose_metrics::SidecarMetrics;
use compose_primitives_traits::{
    CoordinatorError, L2BridgeTxBuilder, MailboxSender, PublisherClient, PutInboxBuilder,
    XtBuilderClient,
};

use crate::coordinator::{DefaultCoordinator, VerificationConfig};

/// Builder for constructing a [`DefaultCoordinator`] with all its dependencies.
pub struct CoordinatorBuilder {
    chain_id: ChainId,
    simulator: Option<Arc<dyn Simulator>>,
    publisher: Option<Arc<dyn PublisherClient>>,
    mailbox_sender: Option<Arc<dyn MailboxSender>>,
    mailbox_queue: Option<Arc<dyn MailboxQueue>>,
    peer_coordinator: Option<Arc<dyn PeerCoordinator>>,
    put_inbox_builder: Option<Arc<dyn PutInboxBuilder>>,
    l2_bridge_builder: Option<Arc<dyn L2BridgeTxBuilder>>,
    xt_builder_client: Option<Arc<dyn XtBuilderClient>>,
    metrics: Option<Arc<SidecarMetrics>>,
    circ_timeout_ms: u64,
    verification: VerificationConfig,
}

impl std::fmt::Debug for CoordinatorBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorBuilder")
            .field("chain_id", &self.chain_id)
            .field("circ_timeout_ms", &self.circ_timeout_ms)
            .finish()
    }
}

impl CoordinatorBuilder {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            simulator: None,
            publisher: None,
            mailbox_sender: None,
            mailbox_queue: None,
            peer_coordinator: None,
            put_inbox_builder: None,
            l2_bridge_builder: None,
            xt_builder_client: None,
            metrics: None,
            circ_timeout_ms: 10_000,
            verification: VerificationConfig::default(),
        }
    }

    pub fn simulator(mut self, s: Arc<dyn Simulator>) -> Self {
        self.simulator = Some(s);
        self
    }

    pub fn publisher(mut self, p: Arc<dyn PublisherClient>) -> Self {
        self.publisher = Some(p);
        self
    }

    pub fn mailbox_sender(mut self, m: Arc<dyn MailboxSender>) -> Self {
        self.mailbox_sender = Some(m);
        self
    }

    pub fn mailbox_queue(mut self, q: Arc<dyn MailboxQueue>) -> Self {
        self.mailbox_queue = Some(q);
        self
    }

    pub fn peer_coordinator(mut self, p: Arc<dyn PeerCoordinator>) -> Self {
        self.peer_coordinator = Some(p);
        self
    }

    pub fn put_inbox_builder(mut self, builder: Arc<dyn PutInboxBuilder>) -> Self {
        self.put_inbox_builder = Some(builder);
        self
    }

    pub fn l2_bridge_builder(mut self, builder: Arc<dyn L2BridgeTxBuilder>) -> Self {
        self.l2_bridge_builder = Some(builder);
        self
    }

    pub fn xt_builder_client(mut self, client: Arc<dyn XtBuilderClient>) -> Self {
        self.xt_builder_client = Some(client);
        self
    }

    pub fn metrics(mut self, m: Arc<SidecarMetrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    pub fn circ_timeout_ms(mut self, ms: u64) -> Self {
        self.circ_timeout_ms = ms;
        self
    }

    pub fn verification_config(mut self, cfg: VerificationConfig) -> Self {
        self.verification = cfg;
        self
    }

    pub fn build(self) -> Result<DefaultCoordinator, CoordinatorError> {
        Self::validate_verification_config(&self.verification)?;
        let mut coord = DefaultCoordinator::new(
            self.chain_id,
            self.simulator,
            self.publisher,
            self.mailbox_sender,
            self.mailbox_queue,
            self.peer_coordinator,
            self.circ_timeout_ms,
            self.verification,
        );
        if let Some(builder) = self.put_inbox_builder {
            coord.set_put_inbox_builder(builder);
        }
        if let Some(builder) = self.l2_bridge_builder {
            coord.set_l2_bridge_builder(builder);
        }
        if let Some(client) = self.xt_builder_client {
            coord.set_xt_builder_client(client);
        }
        if let Some(m) = self.metrics {
            coord.set_metrics(m);
        }
        Ok(coord)
    }

    fn validate_verification_config(
        verification: &VerificationConfig,
    ) -> Result<(), CoordinatorError> {
        if !verification.enabled {
            return Ok(());
        }

        if verification.url.trim().is_empty() {
            return Err(CoordinatorError::Other(
                "verification.enabled requires verification.url".to_string(),
            ));
        }

        Url::parse(&verification.url).map_err(|e| {
            CoordinatorError::Other(format!(
                "invalid verification.url '{}': {e}",
                verification.url
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use compose_primitives::ChainId;

    use super::*;

    #[test]
    fn build_rejects_enabled_verification_without_url() {
        let err = CoordinatorBuilder::new(ChainId(77777))
            .verification_config(VerificationConfig {
                enabled: true,
                url: String::new(),
                timeout_ms: 2_000,
            })
            .build()
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "verification.enabled requires verification.url"
        );
    }
}
