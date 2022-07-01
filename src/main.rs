use anyhow::{Context, Result};
use clap::Parser;
use config::Configuration;
use openraft::error::InitializeError;
use openraft::Config;
use openraft::Raft;
use openraft::SnapshotPolicy;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::app::RegistryApp;
use crate::network::raft_network_impl::RegistryNetwork;
use crate::store::RegistryRequest;
use crate::store::RegistryResponse;
use crate::store::RegsistryStore;
use crate::store::Restore;

pub mod app;
pub mod client;
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
pub mod store;
mod types;
pub(crate) mod utils;
mod webhook;

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub RegistryTypeConfig: D = RegistryRequest, R = RegistryResponse, NodeId = NodeId
);

pub type RegistryRaft = Raft<RegistryTypeConfig, RegistryNetwork, Arc<RegsistryStore>>;

fn create_dir(parent_dir: &str, child_dir: &str) -> std::io::Result<()> {
    let path = std::path::PathBuf::from(&parent_dir).join(child_dir);
    if !path.exists() {
        return std::fs::create_dir_all(path);
    }
    Ok(())
}

pub async fn start_registry_services(settings: Configuration, node_id: NodeId) -> Result<Arc<RegistryApp>> {
    create_dir(&settings.storage, "uploads")?;
    create_dir(&settings.storage, "manifests")?;
    create_dir(&settings.storage, "blobs")?;

    let mut registry = <prometheus_client::registry::Registry>::default();

    // Create a configuration for the raft instance.
    let mut config = Config::default().validate().unwrap();
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(500);
    config.max_applied_log_to_keep = 20000;
    config.install_snapshot_timeout = 400;

    let config = Arc::new(config);

    // Create a instance of where the Raft data will be stored.
    let es = RegsistryStore::open_create(node_id);

    //es.load_latest_snapshot().await.unwrap();

    let mut store = Arc::new(es);

    store.restore().await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = RegistryNetwork::new();

    // Create a local raft instance.
    let raft = Raft::new(node_id, config, network, store.clone());

    let members: BTreeSet<NodeId> = settings
        .peers
        .iter()
        .enumerate()
        .map(|(idx, _peer)| idx as u64)
        .collect();

    match raft.initialize(members).await {
        Ok(_) => Ok(()),
        Err(InitializeError::NotAllowed(..)) => Ok(()),
        Err(err) => Err(err),
    }
    .context("Failed to initialize raft state")?;

    let address = &settings.raft.address;
    let port = &settings.raft.port;

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Arc::new(RegistryApp {
        id: node_id,
        addr: format!("http://{address}:{port}"),
        raft,
        store,
        settings,
    });

    crate::network::raft::launch(app.clone(), &mut registry);
    crate::registry::launch(app.clone(), &mut registry);
    crate::prometheus::launch(app.clone(), registry);

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

    start_registry_services(settings, options.id).await?;

    // Temporary hack
    tokio::time::sleep(tokio::time::Duration::from_secs(60 * 60 * 24 * 30)).await;

    Ok(())
}
