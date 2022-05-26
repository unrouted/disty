#[macro_use]
extern crate rocket;

mod config;
mod extractor;
mod garbage;
mod headers;
mod log;
mod machine;
mod mint;
mod mirror;
mod prometheus;
mod raft;
mod registry;
mod rpc;
mod storage;
mod types;
mod utils;
mod views;
mod webhook;

use std::sync::Arc;

use machine::Machine;
use raft::Raft;
use tokio::sync::{broadcast::error::RecvError, Mutex};
use types::Broadcast;
use webhook::start_webhook_worker;

use crate::storage::Storage;

fn create_dir(parent_dir: &str, child_dir: &str) -> bool {
    let path = std::path::PathBuf::from(&parent_dir).join(child_dir);
    if !path.exists() {
        return matches!(std::fs::create_dir_all(path), Ok(()));
    }
    true
}

async fn launch() {
    let config = crate::config::config();
    let machine_identifier = config.identifier.clone();

    if !create_dir(&config.storage, "uploads")
        || !create_dir(&config.storage, "manifests")
        || !create_dir(&config.storage, "blobs")
    {
        return;
    }

    let mut registry = <prometheus_client::registry::Registry>::default();

    let (broadcaster, broadcasts) = tokio::sync::broadcast::channel::<Broadcast>(100);

    let webhook_send = start_webhook_worker(
        config.webhooks.clone(),
        &mut registry,
        broadcaster.subscribe(),
    );

    let machine = Arc::new(Mutex::new(Machine::new(config.clone(), &mut registry)));

    let (mut storage, term, mut log) = Storage::new(config.clone()).await;

    {
        let mut machine = machine.lock().await;
        machine.term = term;
        machine.pending_index = log.last_index();
        machine.stored_index = log.last_index();
    }

    let clients = crate::rpc::start_rpc_client(config.clone());

    let raft = Arc::new(Raft::new(
        config.clone(),
        machine.clone(),
        clients,
        &mut registry,
    ));

    let state = Arc::new(crate::types::RegistryState::new(
        webhook_send,
        machine_identifier,
        &mut registry,
    ));

    let rpc_client = Arc::new(rpc::RpcClient::new(
        config.clone(),
        machine.clone(),
        raft.clone(),
        state.clone(),
    ));

    let mut events = raft.events.subscribe();
    let dispatcher = state.clone();

    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    dispatcher.dispatch_entries(event).await;
                }
                Err(RecvError::Closed) => {
                    break;
                }
                Err(RecvError::Lagged(_)) => {
                    warn!("Lagged queue handler");
                }
            }
        }
    });

    tokio::spawn(crate::garbage::do_garbage_collect(
        config.clone(),
        machine,
        state.clone(),
        rpc_client.clone(),
        broadcaster.subscribe(),
    ));

    crate::rpc::start_rpc_server(config.clone(), raft.clone());

    crate::mirror::start_mirroring(
        config.clone(),
        state.clone(),
        rpc_client.clone(),
        broadcaster.subscribe(),
    );

    crate::registry::launch(
        config.clone(),
        &mut registry,
        state.clone(),
        rpc_client.clone(),
    );

    let prometheus_conf = rocket::Config::figment()
        .merge(("port", config.prometheus.port))
        .merge(("address", "0.0.0.0"));

    tokio::spawn(crate::prometheus::configure(rocket::custom(prometheus_conf), registry).launch());

    tokio::spawn(async move {
        loop {
            if let Err(err) = tokio::signal::ctrl_c().await {
                warn!("Error whilst waiting for Ctrl+C: {err:?}");
                continue;
            }

            match broadcaster.send(Broadcast::Shutdown) {
                Ok(_) => return,
                Err(err) => {
                    warn!("Error whilst broadcasting shutdown: {err:?}");
                    continue;
                }
            };
        }
    });

    raft.run(broadcasts, &mut storage, &mut log).await;
}

#[rocket::main]
async fn main() {
    launch().await
}
