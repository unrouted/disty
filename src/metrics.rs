use std::sync::Arc;

use anyhow::Result;
use axum::{
    Router,
    extract::State,
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::encoding::text::encode;
use tokio::task::JoinSet;

use crate::state::RegistryState;

/// Axum handler that returns Prometheus metrics
async fn metrics_handler(State(registry): State<Arc<RegistryState>>) -> Response {
    let mut buffer = String::new();

    if let Err(err) = encode(&mut buffer, &registry.registry) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to encode metrics: {}", err),
        )
            .into_response();
    }

    (
        axum::http::StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4")],
        buffer,
    )
        .into_response()
}

pub(crate) fn start_metrics(
    tasks: &mut JoinSet<Result<()>>,
    state: Arc<RegistryState>,
) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

    let listen_addr = format!(
        "{}:{}",
        state.config.prometheus.address, state.config.prometheus.port
    );

    tasks.spawn(async move {
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    });

    Ok(())
}
