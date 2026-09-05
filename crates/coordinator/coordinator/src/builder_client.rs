//! HTTP JSON-RPC client for XT lifecycle control in the local builder.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use alloy::primitives::Bytes;
use async_trait::async_trait;
use compose_primitives_traits::{CoordinatorError, XtBuilderClient};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const BUILDER_RPC_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Serialize)]
struct SubmitXtRequest {
    instance_id: String,
    order: XtOrderKey,
    transactions: Vec<Bytes>,
}

#[derive(Debug, Serialize)]
struct FollowupXtRequest {
    instance_id: String,
    transactions: Vec<Bytes>,
}

#[derive(Debug, Serialize)]
struct ReleaseXtRequest {
    instance_id: String,
    transactions: Vec<Bytes>,
}

#[derive(Debug, Serialize)]
struct AbortXtRequest {
    instance_id: String,
}

#[derive(Debug, Serialize)]
struct XtOrderKey {
    period_id: u64,
    sequence_number: u64,
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<T> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: [T; 1],
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

/// Builder control client backed by the builder's JSON-RPC endpoint.
#[derive(Debug)]
pub struct HttpXtBuilderClient {
    client: Client,
    endpoint: String,
    next_id: AtomicU64,
}

impl HttpXtBuilderClient {
    pub fn new(endpoint: String) -> Result<Self, CoordinatorError> {
        let client = Client::builder()
            .timeout(BUILDER_RPC_TIMEOUT)
            .build()
            .map_err(|err| CoordinatorError::BuilderControl(err.to_string()))?;

        Ok(Self {
            client,
            endpoint,
            next_id: AtomicU64::new(1),
        })
    }

    async fn call<T>(&self, method: &'static str, params: T) -> Result<(), CoordinatorError>
    where
        T: Serialize,
    {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            method,
            params: [params],
        };

        let response = self
            .client
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|err| CoordinatorError::BuilderControl(err.to_string()))?
            .error_for_status()
            .map_err(|err| CoordinatorError::BuilderControl(err.to_string()))?
            .json::<JsonRpcResponse>()
            .await
            .map_err(|err| CoordinatorError::BuilderControl(err.to_string()))?;

        // A JSON-RPC error means the builder saw the request and refused it,
        // as opposed to the transport failures above where the outcome is
        // unknown. Callers use that distinction to decide whether the nonce
        // the transaction reserved can be recycled.
        if let Some(error) = response.error {
            return Err(CoordinatorError::BuilderRejected {
                method: method.to_string(),
                message: format!("code {}: {}", error.code, error.message),
            });
        }

        Ok(())
    }
}

#[async_trait]
impl XtBuilderClient for HttpXtBuilderClient {
    async fn submit_locked_xt(
        &self,
        instance_id: &str,
        period_id: u64,
        sequence_number: u64,
        transactions: Vec<Vec<u8>>,
    ) -> Result<(), CoordinatorError> {
        let request = SubmitXtRequest {
            instance_id: instance_id.to_string(),
            order: XtOrderKey {
                period_id,
                sequence_number,
            },
            transactions: transactions.into_iter().map(Bytes::from).collect(),
        };

        self.call("ethera_submitXt", request).await
    }

    async fn submit_tx(&self, tx: &[u8]) -> Result<(), CoordinatorError> {
        self.call("eth_sendRawTransaction", Bytes::copy_from_slice(tx))
            .await
    }

    async fn submit_followup_xt(
        &self,
        instance_id: &str,
        put_inbox_transactions: Vec<Vec<u8>>,
    ) -> Result<(), CoordinatorError> {
        self.call(
            "ethera_submitFollowup",
            FollowupXtRequest {
                instance_id: instance_id.to_string(),
                transactions: put_inbox_transactions
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            },
        )
        .await
    }

    async fn release_xt(
        &self,
        instance_id: &str,
        put_inbox_transactions: Vec<Vec<u8>>,
    ) -> Result<(), CoordinatorError> {
        self.call(
            "ethera_releaseXt",
            ReleaseXtRequest {
                instance_id: instance_id.to_string(),
                transactions: put_inbox_transactions
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            },
        )
        .await
    }

    async fn abort_xt(&self, instance_id: &str) -> Result<(), CoordinatorError> {
        self.call(
            "ethera_abortXt",
            AbortXtRequest {
                instance_id: instance_id.to_string(),
            },
        )
        .await
    }
}
