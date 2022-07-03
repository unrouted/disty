use crate::app::RegistryApp;
use crate::config::Configuration;
use anyhow::{Context, Result};
use clap::Parser;
use raft::{raw_node::RawNode, storage::MemStorage, Config};
use state::RegistryState;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod app;
mod config;
mod extractor;
mod garbage;
mod headers;
mod middleware;
mod mint;
mod mirror;
pub mod network;
mod prometheus;
mod registry;
mod state;
mod types;
pub(crate) mod utils;
mod webhook;

pub type NodeId = u64;

fn create_dir(parent_dir: &str, child_dir: &str) -> std::io::Result<()> {
    let path = std::path::PathBuf::from(&parent_dir).join(child_dir);
    if !path.exists() {
        return std::fs::create_dir_all(path);
    }
    Ok(())
}

pub async fn start_registry_services(
    settings: Configuration,
    node_id: NodeId,
) -> Result<Arc<RegistryApp>> {
    create_dir(&settings.storage, "uploads")?;
    create_dir(&settings.storage, "manifests")?;
    create_dir(&settings.storage, "blobs")?;

    let mut registry = <prometheus_client::registry::Registry>::default();

    let config = Config {
        id: 1,
        check_quorum: true,
        pre_vote: true,
        ..Default::default()
    };
    config
        .validate()
        .context("Unable to configure raft module")?;

    let members: Vec<u64> = settings
        .peers
        .iter()
        .enumerate()
        .map(|(idx, _peer)| (idx + 1) as u64)
        .collect();

    let state = RwLock::new(RegistryState::default());

    let storage = MemStorage::new_with_conf_state((members, vec![]));

    let group = RwLock::new(RawNode::with_default_logger(&config, storage).unwrap());

    let address = &settings.raft.address;
    let port = &settings.raft.port;

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Arc::new(RegistryApp {
        group,
        state,
        settings,
    });

    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    crate::network::server::launch(app.clone(), &mut registry);
    crate::registry::launch(app.clone(), &mut registry);
    crate::prometheus::launch(app.clone(), registry);

    tokio::spawn(crate::app::do_raft_ticks(app.clone(), rx));

    tokio::spawn(crate::garbage::do_garbage_collect(app.clone()));
    tokio::spawn(crate::mirror::do_miroring(app.clone()));

    Ok(app)
}

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,
}

#[rocket::main]
async fn main() -> Result<()> {
    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let settings = crate::config::config();

    let app = start_registry_services(settings, options.id).await?;

    // Temporary hack
    tokio::time::sleep(tokio::time::Duration::from_secs(60 * 60 * 24 * 30)).await;

    Ok(())
}
