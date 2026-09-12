//! Prometheus metrics definitions for the sidecar.

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

/// All operational metrics exposed by the sidecar.
#[derive(Debug)]
pub struct SidecarMetrics {
    /// Total number of cross-chain transactions submitted.
    pub xt_received_total: Counter<u64>,
    /// XTs that reached a commit decision.
    pub xt_decided_commit_total: Counter<u64>,
    /// XTs that reached an abort decision.
    pub xt_decided_abort_total: Counter<u64>,
    /// Wall-clock time of each simulation call, in seconds.
    pub simulation_duration_seconds: Histogram,
    /// Time spent waiting for mailbox dependencies before retrying simulation.
    pub mailbox_wait_duration_seconds: Histogram,
    /// End-to-end XT latency from submission to decision, in seconds.
    pub xt_decision_latency_seconds: Histogram,
    /// Current number of undecided (in-flight) XTs.
    pub xt_pending_count: Gauge,
    /// Inbound CIRC mailbox messages received.
    pub circ_messages_received_total: Counter<u64>,
    /// Outbound CIRC mailbox messages sent.
    pub circ_messages_sent_total: Counter<u64>,
    /// Vote send failures (publisher unreachable or peer send error).
    pub vote_send_failed_total: Counter<u64>,
    /// RPC simulation errors (distinct from simulation returning failure).
    pub simulation_error_total: Counter<u64>,
    /// Mailbox waits that hit the CIRC timeout before any dependency arrived.
    pub mailbox_wait_timeout_total: Counter<u64>,
    /// Wall-clock time spent building each local `putInbox` transaction.
    pub put_inbox_build_duration_seconds: Histogram,
    /// Failures while building local `putInbox` transactions.
    pub put_inbox_build_error_total: Counter<u64>,
    /// `StartInstance` messages rejected (stale period, active instance, etc.).
    pub xt_rejected_total: Counter<u64>,
    /// Current number of orphan mailbox buffer entries.
    pub mailbox_buffer_size: Gauge,
    /// Transactions the builder's EVM refused outright — no gas money, fee cap
    /// below base fee, nonce already consumed. The builder has quarantined the
    /// instance and will not retry it, so every increment is a cross-chain
    /// round that cannot make progress without intervention.
    pub xt_unrecoverable_total: Counter<u64>,
}

impl SidecarMetrics {
    /// Create and register all metrics into `registry`.
    pub fn new(registry: &mut Registry) -> Self {
        let xt_received_total = Counter::default();
        registry.register(
            "sidecar_xt_received",
            "Total number of cross-chain transactions submitted",
            xt_received_total.clone(),
        );

        let xt_decided_commit_total = Counter::default();
        registry.register(
            "sidecar_xt_decided_commit",
            "Cross-chain transactions that reached a commit decision",
            xt_decided_commit_total.clone(),
        );

        let xt_decided_abort_total = Counter::default();
        registry.register(
            "sidecar_xt_decided_abort",
            "Cross-chain transactions that reached an abort decision",
            xt_decided_abort_total.clone(),
        );

        let simulation_duration_seconds = Histogram::new(
            [
                0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]
            .into_iter(),
        );
        registry.register(
            "sidecar_simulation_duration_seconds",
            "Wall-clock time of each EVM simulation call",
            simulation_duration_seconds.clone(),
        );

        let mailbox_wait_duration_seconds = Histogram::new(
            [
                0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]
            .into_iter(),
        );
        registry.register(
            "sidecar_mailbox_wait_duration_seconds",
            "Time spent waiting for mailbox dependencies before retrying simulation",
            mailbox_wait_duration_seconds.clone(),
        );

        let xt_decision_latency_seconds =
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0].into_iter());
        registry.register(
            "sidecar_xt_decision_latency_seconds",
            "End-to-end XT latency from submission to decision",
            xt_decision_latency_seconds.clone(),
        );

        let xt_pending_count = Gauge::default();
        registry.register(
            "sidecar_xt_pending",
            "Current number of undecided in-flight cross-chain transactions",
            xt_pending_count.clone(),
        );

        let circ_messages_received_total = Counter::default();
        registry.register(
            "sidecar_circ_messages_received",
            "Inbound CIRC mailbox messages received",
            circ_messages_received_total.clone(),
        );

        let circ_messages_sent_total = Counter::default();
        registry.register(
            "sidecar_circ_messages_sent",
            "Outbound CIRC mailbox messages dispatched",
            circ_messages_sent_total.clone(),
        );

        let vote_send_failed_total = Counter::default();
        registry.register(
            "sidecar_vote_send_failed",
            "Vote send failures (publisher unreachable or peer send error)",
            vote_send_failed_total.clone(),
        );

        let simulation_error_total = Counter::default();
        registry.register(
            "sidecar_simulation_error",
            "RPC simulation errors (distinct from simulation returning failure)",
            simulation_error_total.clone(),
        );

        let mailbox_wait_timeout_total = Counter::default();
        registry.register(
            "sidecar_mailbox_wait_timeout",
            "Mailbox waits that reached the CIRC timeout before any dependency arrived",
            mailbox_wait_timeout_total.clone(),
        );

        let put_inbox_build_duration_seconds = Histogram::new(
            [
                0.00025, 0.0005, 0.00075, 0.001, 0.0015, 0.002, 0.003, 0.004, 0.005, 0.0075, 0.01,
                0.025, 0.05,
            ]
            .into_iter(),
        );
        registry.register(
            "sidecar_put_inbox_build_duration_seconds",
            "Wall-clock time spent building each local putInbox transaction",
            put_inbox_build_duration_seconds.clone(),
        );

        let put_inbox_build_error_total = Counter::default();
        registry.register(
            "sidecar_put_inbox_build_error",
            "Failures while building local putInbox transactions",
            put_inbox_build_error_total.clone(),
        );

        let xt_rejected_total = Counter::default();
        registry.register(
            "sidecar_xt_rejected",
            "StartInstance messages rejected (stale period, active instance, etc.)",
            xt_rejected_total.clone(),
        );

        let mailbox_buffer_size = Gauge::default();
        registry.register(
            "sidecar_mailbox_buffer_size",
            "Current number of orphan mailbox buffer entries",
            mailbox_buffer_size.clone(),
        );

        let xt_unrecoverable_total = Counter::default();
        registry.register(
            "sidecar_xt_unrecoverable",
            "Transactions the builder's EVM refused outright, quarantining the instance",
            xt_unrecoverable_total.clone(),
        );

        Self {
            xt_received_total,
            xt_decided_commit_total,
            xt_decided_abort_total,
            simulation_duration_seconds,
            mailbox_wait_duration_seconds,
            xt_decision_latency_seconds,
            xt_pending_count,
            circ_messages_received_total,
            circ_messages_sent_total,
            vote_send_failed_total,
            simulation_error_total,
            mailbox_wait_timeout_total,
            put_inbox_build_duration_seconds,
            put_inbox_build_error_total,
            xt_rejected_total,
            mailbox_buffer_size,
            xt_unrecoverable_total,
        }
    }
}
