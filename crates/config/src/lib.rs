//! Sidecar configuration via CLI arguments and environment variables.
//!
//! Uses `clap` with `#[arg(env = "...")]` for env-var mapping.

use clap::Parser;
use compose_primitives::ChainId;

mod peer;

pub use peer::{PeerArgs, PeerConfigError, PeerEntry};

/// Ethera sidecar — cross-chain coordination layer.
#[derive(Debug, Clone, Parser)]
#[command(name = "sidecar")]
pub struct SidecarArgs {
    #[command(flatten)]
    pub server: ServerArgs,

    #[command(flatten)]
    pub publisher: PublisherArgs,

    #[command(flatten)]
    pub chain: ChainArgs,

    #[command(flatten)]
    pub peers: PeerArgs,

    #[command(flatten)]
    pub log: LogArgs,

    #[command(flatten)]
    pub verification: VerificationArgs,

    #[command(flatten)]
    pub mock_proof: MockProofArgs,
}

/// HTTP server settings.
#[derive(Debug, Clone, clap::Args)]
pub struct ServerArgs {
    /// HTTP listen address.
    #[arg(
        long = "server.listen-addr",
        env = "SIDECAR_LISTEN_ADDR",
        default_value = "0.0.0.0:8080"
    )]
    pub listen_addr: String,

    /// HTTP read timeout in seconds.
    #[arg(
        long = "server.read-timeout-secs",
        env = "SIDECAR_READ_TIMEOUT_SECS",
        default_value = "30"
    )]
    pub read_timeout_secs: u64,

    /// HTTP write timeout in seconds.
    #[arg(
        long = "server.write-timeout-secs",
        env = "SIDECAR_WRITE_TIMEOUT_SECS",
        default_value = "30"
    )]
    pub write_timeout_secs: u64,
}

/// Publisher (SP) connection settings.
#[derive(Debug, Clone, clap::Args)]
pub struct PublisherArgs {
    /// Enable publisher connection.
    #[arg(
        id = "publisher_enabled",
        long = "publisher.enabled",
        env = "SIDECAR_PUBLISHER_ENABLED",
        default_value = "false",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    pub enabled: bool,

    /// Publisher QUIC address.
    #[arg(
        long = "publisher.addr",
        env = "SIDECAR_PUBLISHER_ADDR",
        default_value = ""
    )]
    pub addr: String,

    /// Reconnect delay in seconds.
    #[arg(
        long = "publisher.reconnect-delay-secs",
        env = "SIDECAR_PUBLISHER_RECONNECT_DELAY_SECS",
        default_value = "5"
    )]
    pub reconnect_delay_secs: u64,

    /// Maximum reconnection attempts.
    #[arg(
        long = "publisher.max-retries",
        env = "SIDECAR_PUBLISHER_MAX_RETRIES",
        default_value = "10"
    )]
    pub max_retries: u32,
}

/// Single-chain configuration (one chain per sidecar container).
#[derive(Debug, Clone, clap::Args)]
pub struct ChainArgs {
    /// Chain ID this sidecar manages.
    #[arg(long = "chain.id", env = "SIDECAR_CHAIN_ID", default_value = "0")]
    pub id: u64,

    /// Chain name.
    #[arg(long = "chain.name", env = "SIDECAR_CHAIN_NAME", default_value = "")]
    pub name: String,

    /// Chain RPC endpoint.
    #[arg(long = "chain.rpc", env = "SIDECAR_CHAIN_RPC", default_value = "")]
    pub rpc: String,

    /// Builder RPC endpoint used for XT lifecycle control and simulation.
    /// Falls back to `chain.rpc` when unset.
    #[arg(
        long = "chain.builder-rpc",
        env = "SIDECAR_CHAIN_BUILDER_RPC",
        default_value = ""
    )]
    pub builder_rpc: String,

    /// `UniversalBridgeMailbox` contract address.
    #[arg(
        long = "chain.universal-bridge-mailbox-address",
        env = "SIDECAR_UNIVERSAL_BRIDGE_MAILBOX_ADDRESS",
        default_value = ""
    )]
    pub universal_bridge_mailbox_address: String,

    /// `ComposeL2ToL2Bridge` contract address.
    #[arg(
        long = "chain.l2-bridge-address",
        env = "SIDECAR_L2_BRIDGE_ADDRESS",
        default_value = ""
    )]
    pub l2_bridge_address: String,

    /// Private key for signing local `putInbox` transactions.
    #[arg(
        long = "chain.coordinator-key",
        env = "SIDECAR_COORDINATOR_KEY",
        default_value = ""
    )]
    pub coordinator_key: String,
}

impl ChainArgs {
    /// Return the chain ID as a [`ChainId`].
    pub fn chain_id(&self) -> ChainId {
        ChainId(self.id)
    }

    /// Return the builder RPC endpoint, falling back to the chain RPC when unset.
    pub fn builder_rpc_url(&self) -> &str {
        if self.builder_rpc.is_empty() {
            &self.rpc
        } else {
            &self.builder_rpc
        }
    }
}

/// Logging settings.
#[derive(Debug, Clone, clap::Args)]
pub struct LogArgs {
    /// Log level: debug, info, warn, error.
    #[arg(long = "log.level", env = "SIDECAR_LOG_LEVEL", default_value = "info")]
    pub level: String,

    /// Log format: json or pretty.
    #[arg(
        long = "log.format",
        env = "SIDECAR_LOG_FORMAT",
        default_value = "json"
    )]
    pub format: String,
}

/// Inbound verification hook (per-destination rollup).
#[derive(Debug, Clone, clap::Args)]
pub struct VerificationArgs {
    /// Enable the external verification hook before voting commit.
    #[arg(
        id = "verification_enabled",
        long = "verification.enabled",
        env = "SIDECAR_VERIFICATION_ENABLED",
        default_value = "false",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    pub enabled: bool,

    /// Verification HTTP endpoint to call on inbound XTs.
    #[arg(
        long = "verification.url",
        env = "SIDECAR_VERIFICATION_URL",
        default_value = ""
    )]
    pub url: String,

    /// Request timeout in milliseconds.
    #[arg(
        long = "verification.timeout-ms",
        env = "SIDECAR_VERIFICATION_TIMEOUT_MS",
        default_value = "2000"
    )]
    pub timeout_ms: u64,
}

/// Mock proof generation (stands in for a real op-succinct prover).
///
/// When enabled, this sidecar periodically submits a fabricated-but-well-formed
/// proof for its own chain to the publisher's `/v1/proofs/op-succinct` HTTP
/// endpoint, so the publisher's proof-collection window can be satisfied
/// without running a real ZK prover. Pairs with a `MockVerifier` deployed on
/// L1 (which accepts any proof unconditionally) so the full pipeline —
/// including the L1 submission — can be exercised end-to-end in local
/// testing.
#[derive(Debug, Clone, clap::Args)]
pub struct MockProofArgs {
    /// Enable mock proof submission.
    #[arg(
        id = "mock_proof_enabled",
        long = "mock-proof.enabled",
        env = "SIDECAR_MOCK_PROOF_ENABLED",
        default_value = "false",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    pub enabled: bool,

    /// Publisher HTTP API address (host:port) to submit mock proofs to.
    #[arg(
        long = "mock-proof.publisher-http-addr",
        env = "SIDECAR_MOCK_PROOF_PUBLISHER_HTTP_ADDR",
        default_value = ""
    )]
    pub publisher_http_addr: String,

    /// Interval between mock proof submissions, in seconds. Should be <= the
    /// publisher's consensus period duration so a proof is always ready
    /// before the next period's collection window closes.
    #[arg(
        long = "mock-proof.interval-secs",
        env = "SIDECAR_MOCK_PROOF_INTERVAL_SECS",
        default_value = "60"
    )]
    pub interval_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_args_are_valid() {
        let args = SidecarArgs::parse_from(["sidecar"]);
        assert_eq!(args.server.listen_addr, "0.0.0.0:8080");
        assert!(!args.publisher.enabled);
        assert_eq!(args.chain.id, 0);
        assert_eq!(args.log.level, "info");
        assert_eq!(args.log.format, "json");
        assert!(!args.verification.enabled);
        assert_eq!(args.verification.url, "");
        assert!(!args.mock_proof.enabled);
        assert_eq!(args.mock_proof.interval_secs, 60);
    }

    #[test]
    fn cli_args_override_defaults() {
        let args = SidecarArgs::parse_from([
            "sidecar",
            "--server.listen-addr",
            "0.0.0.0:9090",
            "--chain.id",
            "77777",
            "--chain.rpc",
            "http://localhost:8545",
            "--publisher.enabled",
            "true",
            "--publisher.addr",
            "publisher:8080",
            "--log.level",
            "debug",
        ]);
        assert_eq!(args.server.listen_addr, "0.0.0.0:9090");
        assert_eq!(args.chain.id, 77777);
        assert_eq!(args.chain.rpc, "http://localhost:8545");
        assert!(args.publisher.enabled);
        assert_eq!(args.publisher.addr, "publisher:8080");
        assert_eq!(args.log.level, "debug");
    }

    #[test]
    fn builder_rpc_falls_back_to_chain_rpc() {
        let args = SidecarArgs::parse_from(["sidecar", "--chain.rpc", "http://localhost:8545"]);
        assert_eq!(args.chain.builder_rpc_url(), "http://localhost:8545");
    }

    #[test]
    fn builder_rpc_overrides_chain_rpc() {
        let args = SidecarArgs::parse_from([
            "sidecar",
            "--chain.rpc",
            "http://op-geth:8545",
            "--chain.builder-rpc",
            "http://op-rbuilder:8545",
        ]);
        assert_eq!(args.chain.builder_rpc_url(), "http://op-rbuilder:8545");
    }
}
