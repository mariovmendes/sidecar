//! Sidecar binary entrypoint.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use compose_config::{MockProofArgs, SidecarArgs};
use compose_coordinator::builder::CoordinatorBuilder;
use compose_coordinator::builder_client::HttpXtBuilderClient;
use compose_coordinator::coordinator::{DefaultCoordinator, TransactionChunk, VerificationConfig};
use compose_coordinator::coordinator::ChunkStage::*;
use compose_mailbox::l2_bridge::L2BridgeContractTxBuilder;
use compose_mailbox::put_inbox::PutInboxTxBuilder;
use compose_mailbox::queue::InMemoryQueue;
use compose_metrics::SidecarMetrics;
use compose_peer::coordinator::{HttpPeerCoordinator, PeerEntry as RuntimePeerEntry};
use compose_peer::sender::PeerMailboxSender;
use compose_publisher::PublisherConnection;
use compose_server::handlers::publisher::handle_publisher_message;
use compose_server::router::build_router;
use compose_server::state::AppState;
use compose_simulation::rpc::RpcSimulator;
use compose_simulation::types::ChainRpcConfig;
use compose_transport::client::QuicClient;
use compose_transport::config::ClientConfig;
use compose_transport::traits::Transport;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use compose_coordinator::coordinator::ChunkStage::{Aborted, Registered};

#[tokio::main]
async fn main() -> Result<()> {
    let args = SidecarArgs::parse();

    compose_tracing::init(&args.log.level, &args.log.format);

    info!("Starting sidecar");

    let mut registry = Registry::default();
    let metrics = Arc::new(SidecarMetrics::new(&mut registry));

    let (mut coordinator, quic_client) = build_coordinator(&args, metrics)?;

    coordinator.start().await?;

    let (tx, rx) = mpsc::channel::<String>(300);
    coordinator.set_chunk_sender(tx.clone());

    let coordinator_arc = Arc::new(coordinator);

    if let Some(client) = quic_client {
        spawn_publisher_connection(coordinator_arc.clone(), client, tx.clone());
        spawn_chunk_processor(coordinator_arc.clone(), rx);
    }

    if args.mock_proof.enabled {
        spawn_mock_proof_submitter(args.chain.id, args.mock_proof.clone());
    }

    let state = AppState::from_arc(coordinator_arc).with_registry(registry);
    let router = build_router(state);

    let listener = TcpListener::bind(&args.server.listen_addr).await?;
    info!(addr = %args.server.listen_addr, "HTTP server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Shutting down");
    Ok(())
}

fn build_coordinator(
    args: &SidecarArgs,
    metrics: Arc<SidecarMetrics>,
) -> Result<(DefaultCoordinator, Option<Arc<QuicClient>>)> {
    let chain_id = args.chain.chain_id();

    let mut builder = CoordinatorBuilder::new(chain_id).metrics(metrics);
    let chain_rpc = &args.chain.rpc;
    let builder_rpc = args.chain.builder_rpc_url();
    if !builder_rpc.is_empty() {
        match HttpXtBuilderClient::new(builder_rpc.to_string()) {
            Ok(client) => {
                builder = builder.xt_builder_client(Arc::new(client));
            }
            Err(e) => {
                warn!(error = %e, endpoint = builder_rpc, "Failed to configure builder control client");
            }
        }
    }

    let has_rpc = !chain_rpc.is_empty();
    let universal_bridge_mailbox_address = &args.chain.universal_bridge_mailbox_address;
    let has_mailbox = !universal_bridge_mailbox_address.is_empty();
    let has_key = !args.chain.coordinator_key.is_empty();
    if has_rpc && has_mailbox && has_key {
        //TODO: Configure also removeInbox and check the universal if it is the same as the one developed
        match PutInboxTxBuilder::new(
            chain_id,
            chain_rpc.to_string(),
            universal_bridge_mailbox_address.clone(),
            args.chain.coordinator_key.clone(),
        ) {
            Ok(put_inbox) => {
                builder = builder.put_inbox_builder(Arc::new(put_inbox));
            }
            Err(e) => {
                warn!(error = %e, endpoint = chain_rpc, "Failed to configure putInbox builder");
            }
        }
    } else if has_mailbox || has_key {
        warn!(
            has_rpc,
            has_mailbox,
            has_coordinator_key = has_key,
            "putInbox builder disabled due to incomplete chain config"
        );
    }

    let l2_bridge_address = &args.chain.l2_bridge_address;
    let has_l2_bridge = !l2_bridge_address.is_empty();
    if has_rpc && has_l2_bridge && has_key {
        match L2BridgeContractTxBuilder::new(
            chain_id.0,
            chain_rpc.to_string(),
            l2_bridge_address.clone(),
            args.chain.coordinator_key.clone(),
        ) {
            Ok(l2_bridge) => {
                builder = builder.l2_bridge_builder(Arc::new(l2_bridge));
            }
            Err(e) => {
                warn!(error = %e, endpoint = chain_rpc, "Failed to configure l2 bridge builder");
            }
        }
    } else if has_l2_bridge || has_key {
        warn!(
            has_rpc,
            has_l2_bridge,
            has_coordinator_key = has_key,
            "l2 bridge builder disabled due to incomplete chain config"
        );
    }

    if !builder_rpc.is_empty() {
        let rpc_chains = vec![ChainRpcConfig {
            chain_id,
            rpc_url: builder_rpc.to_string(),
        }];
        let mut sim = RpcSimulator::new(rpc_chains);
        if !universal_bridge_mailbox_address.is_empty() {
            if let Ok(addr) = universal_bridge_mailbox_address.parse() {
                sim = sim.with_mailbox_address(addr);
            }
        }
        builder = builder.simulator(Arc::new(sim));
    }

    builder = builder.mailbox_queue(Arc::new(InMemoryQueue::new()));

    builder = builder.verification_config(VerificationConfig {
        enabled: args.verification.enabled,
        url: args.verification.url.clone(),
        timeout_ms: args.verification.timeout_ms,
    });

    let peer_entries = args.peers.entries()?;
    if !peer_entries.is_empty() {
        let peers: Vec<RuntimePeerEntry> = peer_entries
            .iter()
            .map(|p| RuntimePeerEntry {
                chain_id: p.chain_id,
                addr: p.addr.clone(),
            })
            .collect();
        let pc = Arc::new(HttpPeerCoordinator::new(peers));
        builder = builder.peer_coordinator(pc);

        let mailbox_peers: Vec<RuntimePeerEntry> = peer_entries
            .iter()
            .map(|p| RuntimePeerEntry {
                chain_id: p.chain_id,
                addr: p.addr.clone(),
            })
            .collect();
        builder = builder.mailbox_sender(Arc::new(PeerMailboxSender::with_peer_entries(
            &mailbox_peers,
        )));
    }

    let quic_client = if args.publisher.enabled && !args.publisher.addr.is_empty() {
        let client_config = ClientConfig {
            addr: args.publisher.addr.clone(),
            client_id: chain_id.0.to_string(),
            reconnect_delay: Duration::from_secs(args.publisher.reconnect_delay_secs),
            max_retries: args.publisher.max_retries,
            ..Default::default()
        };
        match QuicClient::new(client_config) {
            Ok(client) => {
                let conn = PublisherConnection::new(client.clone(), chain_id);
                builder = builder.publisher(Arc::new(conn));
                Some(client)
            }
            Err(e) => {
                warn!(error = %e, "Failed to create QUIC client, running without publisher");
                None
            }
        }
    } else {
        None
    };

    Ok((builder.build()?, quic_client))
}

fn spawn_publisher_connection(coordinator: Arc<DefaultCoordinator>, client: Arc<QuicClient>, tx: Sender<String>) {
    tokio::spawn(async move {
        info!("Connecting to publisher");
        if let Err(e) = client.connect_with_retry().await {
            error!(error = %e, "Failed to connect to publisher after retries");
            return;
        }
        info!("Connected to publisher, starting receive loop");

        loop {
            match client.recv().await {
                Ok(data) => {
                    let coord = coordinator.clone();
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        handle_publisher_message(coord, data, tx).await;
                    });
                }
                Err(e) => {
                    warn!(error = %e, "Publisher receive error, connection may be lost");
                    break;
                }
            }
        }

        warn!("Publisher receive loop ended");
    });
}

fn spawn_chunk_processor(coordinator: Arc<DefaultCoordinator>, mut rx: Receiver<String>){
    tokio::spawn(async move {
        info!("Starting chunk processor");
        while let Some(instance_id) = rx.recv().await {
            // Always dispatch on a fresh fetch of the chunk's current state
            // rather than any snapshot carried by the signal itself — the
            // signal only ever means "something changed for this instance,
            // go look." This is what makes register_xt only reachable via
            // the `None` arm: once an instance exists in `inflight_chunks`
            // (even at `Aborted`), a late/duplicate `Registered` signal for
            // it lands in the `Some` arm instead and is re-dispatched on its
            // real current stage, so it can never be mistaken for a fresh
            // registration.
            match coordinator.get_inflight_chunk(&instance_id).await {
                Some(mut chunk) => {
                    if chunk.confirmed_stage == Some(chunk.stage) {
                        // Already fully processed for this stage; a
                        // duplicate/late signal, nothing new to do.
                        continue;
                    }
                    match chunk.stage {
                        WaitingForProcessing => {
                            coordinator.process_xt(&mut chunk).await;
                        }
                        Confirmed => {
                            coordinator.confirm_xt(&mut chunk).await;
                        }
                        Aborted => {
                            coordinator.abort_xt(&mut chunk).await;
                        }
                        _ => {}
                    }
                }
                None => {
                    let mut chunk = TransactionChunk {
                        instance_id: instance_id.clone(),
                        stage: Registered,
                        ..Default::default()
                    };
                    coordinator.register_xt(&mut chunk).await;
                }
            }
        }
    });
}

/// Stands in for a real op-succinct prover: periodically POSTs a
/// fabricated-but-well-formed proof submission to the publisher's
/// `/v1/proofs/op-succinct` endpoint for this sidecar's own chain. The
/// publisher never validates proof content itself (see the "not our job,
/// L1 does it" TODO in `receive_proof`), and the `superblock_number` field
/// is likewise ignored by the publisher — it always finalizes against its
/// own locally tracked `next_superblock_number` once every registered chain
/// has submitted something for the current round. Pairs with a
/// `MockVerifier` contract on L1 that accepts any proof unconditionally, so
/// the full pipeline (including the L1 submission) can be exercised without
/// running a real ZK prover.
fn spawn_mock_proof_submitter(chain_id: u64, cfg: MockProofArgs) {
    if cfg.publisher_http_addr.is_empty() {
        warn!("mock-proof.enabled is set but publisher-http-addr is empty, not starting submitter");
        return;
    }

    let url = format!("http://{}/v1/proofs/op-succinct", cfg.publisher_http_addr);
    let interval = Duration::from_secs(cfg.interval_secs.max(1));

    tokio::spawn(async move {
        info!(url, interval_secs = cfg.interval_secs, "Starting mock proof submitter");
        let client = reqwest::Client::new();
        let mut ticker = tokio::time::interval(interval);
        let mut round: u64 = 0;

        loop {
            ticker.tick().await;
            round += 1;

            // Non-zero, chain- and round-distinguishable placeholder values.
            // Content is never cryptographically checked anywhere in this
            // mock pipeline — only `l1_head != 0` is enforced by the
            // publisher's handler.
            let word = |tag: u64| format!("0x{:064x}", chain_id * 1_000_000_000 + round * 1000 + tag);
            let addr = |tag: u64| format!("0x{:040x}", chain_id * 1_000_000_000 + round * 1000 + tag);

            let body = serde_json::json!({
                "superblock_number": round,
                "chain_id": chain_id,
                "aggregation_outputs": {
                    "l1Head": word(1),
                    "l2PreRoot": word(2),
                    "l2PostRoot": word(3),
                    "l2BlockNumber": round,
                    "rollupConfigHash": word(4),
                    "mailboxRoot": word(5),
                    "multiBlockVKey": word(6),
                    "proverAddress": addr(7),
                },
                "agg_vkey_hash": word(8),
            });

            match client.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(round, "Mock proof submitted");
                }
                Ok(resp) => {
                    warn!(round, status = %resp.status(), "Mock proof submission rejected");
                }
                Err(e) => {
                    warn!(round, error = %e, "Mock proof submission failed");
                }
            }
        }
    });
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("Received shutdown signal");
}
