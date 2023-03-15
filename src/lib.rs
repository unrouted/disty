#![allow(clippy::uninlined_format_args)]

use std::sync::Arc;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web;
use actix_web::web::Data;
use actix_web::App;
use actix_web::HttpServer;
use extractor::Extractor;
use openraft::BasicNode;
use openraft::Config;
use openraft::Raft;
use tokio::task::JoinSet;
use webhook::start_webhook_worker;

use crate::app::RegistryApp;
use crate::network::api;
use crate::network::management;
use crate::network::raft;
use crate::network::raft_network_impl::RegistryNetwork;
use crate::network::registry;
use crate::store::RegistryRequest;
use crate::store::RegistryResponse;
use crate::store::RegistryStore;

pub mod app;
pub mod client;
pub mod config;
pub mod extractor;
pub mod extractors;
pub mod network;
pub mod store;
pub mod types;
pub mod webhook;

pub type RegistryNodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub RegistryTypeConfig: D = RegistryRequest, R = RegistryResponse, NodeId = RegistryNodeId, Node = BasicNode
);

pub type RegistryRaft = Raft<RegistryTypeConfig, RegistryNetwork, Arc<RegistryStore>>;

pub mod typ {
    use openraft::BasicNode;

    use crate::RegistryNodeId;
    use crate::RegistryTypeConfig;

    pub type RaftError<E = openraft::error::Infallible> =
        openraft::error::RaftError<RegistryNodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<RegistryNodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<RegistryNodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<RegistryNodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<RegistryNodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<RegistryNodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<RegistryTypeConfig>;
}

pub async fn start_raft_node(node_id: RegistryNodeId, http_addr: String) -> std::io::Result<()> {
    let mut registry = <prometheus_client::registry::Registry>::default();

    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    let conf = crate::config::config(None);

    let mut path = std::path::Path::new(&conf.storage).to_path_buf();
    path.push("db");

    let db: sled::Db = sled::open(&path).unwrap_or_else(|_| panic!("could not open: {:?}", path));

    // Create a instance of where the Raft data will be stored.
    let store = RegistryStore::new(Arc::new(db)).await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = RegistryNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone())
        .await
        .unwrap();

    let extractor = Arc::new(Extractor::new());

    let webhook_queue = start_webhook_worker(conf.webhooks.clone(), &mut registry);

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Data::new(RegistryApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config: conf,
        extractor,
        webhooks: Arc::new(webhook_queue),
    });

    let app1 = app.clone();
    let app2 = app.clone();

    let mut tasks = JoinSet::new();

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app1.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            // application API
            .service(api::write)
            .service(api::read)
            .service(api::consistent_read)
    })
    .bind((
        app2.config.raft.address.clone().as_str(),
        app2.config.raft.port.clone(),
    ))?
    .run();
    tasks.spawn(server);

    let registry = HttpServer::new(move || {
        let registry_api = web::scope("/v2")
            //   blob upload
            .service(registry::blobs::uploads::delete::delete)
            .service(registry::blobs::uploads::get::get)
            .service(registry::blobs::uploads::patch::patch)
            .service(registry::blobs::uploads::post::post)
            .service(registry::blobs::uploads::put::put)
            // blobs
            .service(registry::blobs::get::get)
            .service(registry::blobs::delete::delete)
            // manifests
            .service(registry::manifests::put::put)
            .service(registry::manifests::get::get)
            .service(registry::manifests::delete::delete)
            // tags
            .service(registry::tags::get::get)
            // roots
            .service(registry::get::get);

        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            .service(registry_api)
    })
    .bind((
        app2.config.registry.address.as_str(),
        app2.config.registry.port.clone(),
    ))?
    .run();
    tasks.spawn(registry);

    tasks.join_next().await.unwrap().unwrap().unwrap();

    Ok(())
}
