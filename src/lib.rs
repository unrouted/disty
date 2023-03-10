#![allow(clippy::uninlined_format_args)]

use std::sync::Arc;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::App;
use actix_web::HttpServer;
use openraft::BasicNode;
use openraft::Config;
use openraft::Raft;

use crate::app::RegistryApp;
use crate::network::api;
use crate::network::management;
use crate::network::raft;
use crate::network::raft_network_impl::RegistryNetwork;
use crate::store::RegistryRequest;
use crate::store::RegistryResponse;
use crate::store::RegistryStore;

pub mod app;
pub mod client;
pub mod network;
pub mod store;
pub mod types;

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

pub async fn start_raft_node(
    node_id: RegistryNodeId,
    http_addr: String,
) -> std::io::Result<()> {
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    let db_path = format!("tmp{node_id}");

    let db: sled::Db = sled::open(&db_path).unwrap_or_else(|_| panic!("could not open: {:?}", db_path));

    // Create a instance of where the Raft data will be stored.
    let store = RegistryStore::new(
        Arc::new(db)
    ).await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = RegistryNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone())
        .await
        .unwrap();

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Data::new(RegistryApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config,
    });

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
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
    });

    let x = server.bind(http_addr)?;

    x.run().await
}
