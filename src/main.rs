use anyhow::Result;
use extractor::Extractor;
use hiqlite::cache_idx::CacheIndex;
use hiqlite_macros::embed::*;
use prometheus_client::registry::Registry;
use registry::router;
use serde::{Deserialize, Serialize};
use state::RegistryState;
use std::{fmt::Debug, sync::Arc};
use tokio::task::JoinSet;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod config;
mod digest;
mod error;
mod extractor;
mod mirror;
mod notify;
mod registry;
mod state;
mod token;
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

#[derive(Debug, strum::EnumIter)]
enum Cache {
    Dummy,
}

impl CacheIndex for Cache {
    fn to_usize(self) -> usize {
        self as usize
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_env_filter(EnvFilter::from("info"))
        .init();

    let mut registry = Registry::with_prefix("disty");

    let config = crate::config::Configuration::config(None)?;

    let node_id = config.id()?;
    let client = hiqlite::start_node_with_cache::<Cache>(config.clone().into()).await?;

    info!("Apply our database migrations");
    client.migrate::<Migrations>().await?;

    let extractor = Extractor::new();

    let mut tasks = JoinSet::new();

    let webhooks = crate::webhook::WebhookService::start(&mut tasks, vec![], &mut registry);

    let state = Arc::new(RegistryState {
        node_id,
        config,
        client,
        extractor,
        webhooks,
    });

    crate::mirror::start_mirror(&mut tasks, state.clone())?;

    let app = router(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tasks.spawn(async move {
        axum::serve(listener, app).await?;
        Ok(())
    });

    let res = tasks.join_next().await;

    tasks.shutdown().await;

    state.shutdown().await?;

    Ok(())
}
