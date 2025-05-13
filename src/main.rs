use extractor::Extractor;
use hiqlite::{Error, NodeConfig};
use hiqlite_macros::embed::*;
use prometheus_client::registry::Registry;
use registry::router;
use serde::{Deserialize, Serialize};
use state::RegistryState;
use std::{fmt::Debug, sync::Arc};
use tokio::task::JoinSet;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod digest;
mod error;
mod extractor;
mod registry;
mod state;
mod webhook;

#[cfg(test)]
mod tests;

#[derive(Embed)]
#[folder = "migrations"]
struct Migrations;

/// Matches our test table for this example.
/// serde derives are needed if you want to use the `query_as()` fn.
#[derive(Debug, Serialize, Deserialize)]
struct Entity {
    pub id: String,
    pub num: i64,
    pub description: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_env_filter(EnvFilter::from("info"))
        .init();

    let mut registry = Registry::with_prefix("disty");

    let config = NodeConfig::from_env_file("config");
    let node_id = config.node_id;
    let client = hiqlite::start_node(config).await?;

    // Let's register our shutdown handle to always perform a graceful shutdown and remove lock files.
    // You can do this manually by calling `.shutdown()` at the end as well, if you already have
    // something like that.
    let mut shutdown_handle = client.shutdown_handle()?;

    info!("Apply our database migrations");
    client.migrate::<Migrations>().await?;

    let extractor = Extractor::new();

    let mut tasks = JoinSet::new();

    let webhooks = crate::webhook::WebhookService::start(&mut tasks, vec![], &mut registry);

    let state = RegistryState {
        node_id,
        client,
        extractor,
        webhooks,
    };
    let app = router(Arc::new(state));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tasks.spawn(async move {
        axum::serve(listener, app).await?;
        Ok(())
    });

    let res = tasks.join_next().await;

    tasks.shutdown().await;

    shutdown_handle.wait().await?;

    Ok(())
}
