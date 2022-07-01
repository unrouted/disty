use clap::Parser;
use openraft::Config;
use openraft::Raft;
use openraft::SnapshotPolicy;
use std::sync::Arc;

use crate::app::ExampleApp;
use crate::network::raft_network_impl::ExampleNetwork;
use crate::store::ExampleRequest;
use crate::store::ExampleResponse;
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
    pub ExampleTypeConfig: D = ExampleRequest, R = ExampleResponse, NodeId = NodeId
);

pub type ExampleRaft = Raft<ExampleTypeConfig, ExampleNetwork, Arc<RegsistryStore>>;

fn create_dir(parent_dir: &str, child_dir: &str) -> std::io::Result<()> {
    let path = std::path::PathBuf::from(&parent_dir).join(child_dir);
    if !path.exists() {
        return std::fs::create_dir_all(path);
    }
    Ok(())
}

pub async fn start_registry_services(
    node_id: NodeId,
    http_addr: String,
) -> std::io::Result<Arc<ExampleApp>> {
    let settings = crate::config::config();

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
    let network = ExampleNetwork::new();

    // Create a local raft instance.
    let raft = Raft::new(node_id, config, network, store.clone());

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Arc::new(ExampleApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        settings: settings,
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

    #[clap(long)]
    pub http_addr: String,
}

#[rocket::main]
async fn main() -> std::io::Result<()> {
    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    start_registry_services(options.id, options.http_addr).await?;

    // Temporary hack
    tokio::time::sleep(tokio::time::Duration::from_secs(60 * 60 * 24 * 30)).await;

    Ok(())
}
