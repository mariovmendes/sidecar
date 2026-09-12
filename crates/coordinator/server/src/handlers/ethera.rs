//! Builder-facing Ethera callbacks.

use axum::extract::State;
use axum::Json;
use serde::Deserialize;

use crate::error::ServerError;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ConfirmIncludedRequest {
    pub instance_ids: Vec<String>,
}

/// Reported by the builder when its EVM refused a transaction in an instance
/// and the instance was quarantined.
#[derive(Debug, Deserialize)]
pub struct XtFailedRequest {
    pub instance_id: String,
    /// Transactions of this instance that executed before the refusal. Empty
    /// means nothing of the round reached the chain.
    #[serde(default)]
    pub executed: Vec<String>,
    #[serde(default)]
    pub reason: String,
}

/// POST /ethera/confirm — confirm included XT instance IDs back to the sidecar.
pub async fn handle_confirm_included(
    State(state): State<AppState>,
    Json(req): Json<ConfirmIncludedRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    state
        .coordinator
        .confirm_included_xts(&req.instance_ids)
        .await?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// POST /ethera/failed — the builder quarantined an XT instance it could not
/// execute.
pub async fn handle_xt_failed(
    State(state): State<AppState>,
    Json(req): Json<XtFailedRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    state
        .coordinator
        .handle_failed_xt(&req.instance_id, &req.executed, &req.reason)
        .await?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}
