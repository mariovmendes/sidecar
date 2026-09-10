//! Status derivation and API response types for XT state.

use compose_primitives::XtStatus;
use serde::{Deserialize, Serialize};

use crate::model::pending_xt::PendingXt;

/// Response for an XT status query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XtStatusResponse {
    pub instance_id: String,
    pub status: XtStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<bool>,
}

/// Determine the current status of a pending XT.
pub fn determine_xt_status(xt: &PendingXt) -> XtStatus {
    if let Some(decision) = xt.decision {
        return if decision {
            XtStatus::Committed
        } else {
            XtStatus::Aborted
        };
    }
    if xt.vote_sent {
        return XtStatus::Voted;
    }
    if xt.simulated_at.is_some() {
        if !xt.dependencies.is_empty() && xt.fulfilled_deps.len() < xt.dependencies.len() {
            return XtStatus::WaitingCirc;
        }
        return XtStatus::Simulated;
    }

    XtStatus::Pending
}
