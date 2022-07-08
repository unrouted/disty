use crate::app::RegistryApp;
use crate::config::Configuration;
use crate::utils::launch;
use anyhow::{bail, Context, Result};
use fern::colors::{Color, ColoredLevelConfig};
use raft::{raw_node::RawNode, Config};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use store::RegistryStorage;
use tokio::sync::RwLock;
use types::RegistryAction;

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
mod snapshot;
mod state;
mod store;
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

pub async fn start_registry_services(settings: Configuration) -> Result<Arc<RegistryApp>> {
    create_dir(&settings.storage, "uploads")?;
    create_dir(&settings.storage, "manifests")?;
    create_dir(&settings.storage, "blobs")?;

    let mut registry = <prometheus_client::registry::Registry>::default();

    let mut outboxes = HashMap::new();
    for (idx, peer) in settings.peers.iter().cloned().enumerate() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);

        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .user_agent("distribd/raft")
                .build()
                .unwrap();

            let address = peer.raft.address;
            let port = peer.raft.port;
            let url = format!("http://{address}:{port}/raft");

            loop {
                match rx.recv().await {
                    Some(payload) => {
                        client.post(&url).body(payload).send().await;
                    }
                    None => return,
                }
            }
        });

        outboxes.insert((idx + 1) as u64, tx);
    }

    let id = match settings
        .peers
        .iter()
        .position(|peer| peer.name == settings.identifier)
    {
        Some(id) => (id + 1) as u64,
        None => {
            bail!("No peer config for this node");
        }
    };

    let config = Config {
        id,
        check_quorum: true,
        pre_vote: true,
        ..Default::default()
    };
    config
        .validate()
        .context("Unable to configure raft module")?;

    let storage = RegistryStorage::new(&settings).await?;

    let group = RwLock::new(RawNode::with_default_logger(&config, storage).unwrap());

    let (inbox, rx) = tokio::sync::mpsc::channel(1000);

    let (actions_tx, actions_rx) = tokio::sync::mpsc::channel::<Vec<RegistryAction>>(1000);

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app = Arc::new(RegistryApp::new(group, inbox, outboxes, settings));

    app.spawn(launch(crate::network::server::configure(
        app.clone(),
        &mut registry,
    )))
    .await;
    app.spawn(launch(crate::registry::configure(
        app.clone(),
        &mut registry,
    )))
    .await;
    app.spawn(launch(crate::prometheus::configure(app.clone(), registry)))
        .await;

    app.spawn(crate::app::do_raft_ticks(app.clone(), rx, actions_tx))
        .await;

    app.spawn(crate::garbage::do_garbage_collect(app.clone()))
        .await;
    app.spawn(crate::mirror::do_miroring(app.clone(), actions_rx))
        .await;
    app.spawn(crate::snapshot::do_snapshot(app.clone())).await;

    Ok(app)
}

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    /// Name of the person to greet
    #[clap(short, long, value_parser)]
    config: Option<PathBuf>,
}

#[rocket::main]
async fn main() -> Result<()> {
    let colors = ColoredLevelConfig::new()
        .error(Color::Red)
        .warn(Color::Yellow)
        .info(Color::White)
        .debug(Color::White)
        .trace(Color::BrightBlack);

    fern::Dispatch::new()
        .level(log::LevelFilter::Info)
        .level_for("rocket", log::LevelFilter::Error)
        // Rocket uses a target of '_' for some of its logging, which is noisy as hell so drop it
        .level_for("_", log::LevelFilter::Error)
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{color_line}[{date}] [{target}] [{level}{color_line}] {message}\x1B[0m",
                color_line =
                    format_args!("\x1B[{}m", colors.get_color(&record.level()).to_fg_str()),
                date = chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                level = colors.color(record.level()),
                target = record.target(),
                message = message,
            ));
        })
        .chain(std::io::stdout())
        .apply()
        .context("Failed to configure logging")?;

    let opts = Opts::parse();

    let settings = crate::config::config(opts.config);

    let app = start_registry_services(settings).await?;

    app.wait_for_shutdown().await;

    Ok(())
}
