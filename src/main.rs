use anyhow::{Context, Result};
use axum::{ServiceExt, extract::Request};
use clap::Parser;
use hiqlite::cache_idx::CacheIndex;
use hiqlite_macros::embed::*;
use prometheus_client::registry::Registry;
use registry::router;
use serde::{Deserialize, Serialize};
use state::RegistryState;
use std::{fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tower::Layer;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::{error::format_error, metrics::start_metrics};

mod config;
mod digest;
mod error;
mod extractor;
mod issuer;
mod jwt;
mod metrics;
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

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(short, long, value_parser)]
    pub config: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_env_filter(EnvFilter::from("info"))
        .init();

    let options = Opt::parse();

    let mut registry = Registry::with_prefix("disty");

    let config = crate::config::Configuration::config(crate::config::Configuration::figment(
        options.config,
    ))?;

    let node_id = config.node_id;
    let client = hiqlite::start_node_with_cache::<Cache>(config.clone().try_into()?).await?;

    info!("Apply our database migrations");
    client.migrate::<Migrations>().await?;

    let mut tasks = JoinSet::new();

    let webhooks = crate::webhook::WebhookService::start(&mut tasks, vec![], &mut registry);

    let state = Arc::new(RegistryState {
        node_id,
        config: config.clone(),
        client,
        webhooks,
        registry,
    });

    crate::mirror::start_mirror(&mut tasks, state.clone())?;

    let app = router(state.clone());
    let app = registry::RewriteUriLayer {}.layer(app);
    let app = ServiceExt::<Request>::into_make_service_with_connect_info::<SocketAddr>(app);

    let node = config
        .nodes
        .get(node_id as usize - 1)
        .expect("NodeConfig.node_id not found in NodeConfig.nodes");
    let listen_addr = format!(
        "0.0.0.0:{}",
        node.addr_registry
            .split_once(":")
            .context("Registry address doesn't have a port")?
            .1
    );
    let listener = tokio::net::TcpListener::bind(listen_addr).await.unwrap();
    tasks.spawn(async move {
        axum::serve(listener, app).await?;
        Ok(())
    });

    {
        let state = state.clone();

        tasks.spawn(async move {
            state.client.wait_until_healthy_db().await;

            loop {
                if let Err(err) = state.garbage_collection().await {
                    error!(
                        error = %format_error(&err),
                        backtrace = ?err.backtrace(),
                        "Garbage collection error"
                    );
                }

                tokio::time::sleep(Duration::from_secs(60 * 60)).await;
            }
        });
    }

    start_metrics(&mut tasks, state.clone())?;

    let res = tasks.join_next().await;

    tasks.shutdown().await;

    state.shutdown().await?;

    if let Some(result) = res {
        result
            .context("Error while waiting for service to complete")?
            .context("Error running service")?;
    }

    Ok(())
}
