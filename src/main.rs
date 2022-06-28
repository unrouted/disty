use clap::Parser;
use env_logger::Env;
use openraft::Config;
use openraft::Raft;
use openraft::SnapshotPolicy;
use std::sync::Arc;

use crate::app::ExampleApp;
use crate::network::raft_network_impl::ExampleNetwork;
use crate::store::ExampleRequest;
use crate::store::ExampleResponse;
use crate::store::ExampleStore;
use crate::store::Restore;

pub mod app;
pub mod client;
mod config;
pub mod matchengine;
pub mod network;
pub mod store;

pub type ExampleNodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub ExampleTypeConfig: D = ExampleRequest, R = ExampleResponse, NodeId = ExampleNodeId
);

pub type ExampleRaft = Raft<ExampleTypeConfig, ExampleNetwork, Arc<ExampleStore>>;

pub async fn start_example_raft_node(
    node_id: ExampleNodeId,
    http_addr: String,
) -> std::io::Result<()> {
    // Create a configuration for the raft instance.

    let mut config = Config::default().validate().unwrap();
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(500);
    config.max_applied_log_to_keep = 20000;
    config.install_snapshot_timeout = 400;

    let config = Arc::new(config);

    // Create a instance of where the Raft data will be stored.
    let es = ExampleStore::open_create(node_id);

    //es.load_latest_snapshot().await.unwrap();

    let mut store = Arc::new(es);

    store.restore().await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = ExampleNetwork::new();

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone());

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Arc::new(ExampleApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config,
        settings: crate::config::config(),
    });

    crate::network::raft::launch(app);

    Ok(())
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
    // Setup the logger
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    start_example_raft_node(options.id, options.http_addr).await
}
