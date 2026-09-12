//! Peer-facing handler for receiving an ACK dependency to putInbox directly.

use axum::extract::State;
use axum::Json;
use compose_primitives::CrossRollupDependency;
use serde::Deserialize;

use crate::error::ServerError;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct AckRequest {
    pub instance_id: String,
    pub dependency: CrossRollupDependency,
}

/// POST /mailbox/ack — receive an ACK `CrossRollupDependency` reported by a peer
/// sidecar right after it submitted the matching `receiveTokens`/`receiveETH`
/// transaction. Records it and schedules the corresponding putInbox to be
/// built and submitted by the chunk processor, rather than doing it inline
/// here in the request handler.
pub async fn handle_ack(
    State(state): State<AppState>,
    Json(request): Json<AckRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    state
        .coordinator
        .handle_ack_dependency(request.instance_id, request.dependency)
        .await?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}