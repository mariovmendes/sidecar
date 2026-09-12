//! HTTP router assembly for sidecar endpoints.

use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

/// Build the HTTP router with all sidecar endpoints.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Builder endpoints
        .route(
            "/ethera/confirm",
            post(handlers::ethera::handle_confirm_included),
        )
        .route("/ethera/failed", post(handlers::ethera::handle_xt_failed))
        // XT endpoints
        .route("/xt", post(handlers::xt::handle_submit_xt))
        .route("/xt/:instance_id", get(handlers::xt::handle_get_xt_status))
        // Peer endpoints
        .route("/xt/forward", post(handlers::peer::handle_forward_xt))
        .route("/xt/vote", post(handlers::peer::handle_peer_vote))
        .route("/mailbox", post(handlers::peer::handle_mailbox))
        .route("/mailbox/ack", post(handlers::ack::handle_ack))
        // Health and observability endpoints
        .route("/health", get(handlers::health::handle_health))
        .route("/ready", get(handlers::health::handle_ready))
        .route("/metrics", get(handlers::metrics::handle_metrics))
        // Middleware
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
