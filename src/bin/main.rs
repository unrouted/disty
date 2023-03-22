use std::path::PathBuf;

use clap::{Parser, Subcommand};
use distribd::client::RegistryClient;
use distribd::network::management::ImportBody;
use distribd::network::raft_network_impl::RegistryNetwork;
use distribd::start_raft_node;
use distribd::store::RegistryStore;
use distribd::utils::{get_blob_path, get_manifest_path};
use distribd::RegistryTypeConfig;
use openraft::Raft;
use serde_json::from_str;
use tokio::signal;
use tracing_subscriber::EnvFilter;

pub type RegistryRaft = Raft<RegistryTypeConfig, RegistryNetwork, RegistryStore>;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(short, long, value_parser)]
    pub config: Option<std::path::PathBuf>,
    #[clap(short, long, value_parser)]
    pub name: Option<String>,

    #[clap(subcommand)]
    pub action: Action,
}

#[derive(Subcommand, Debug)]
pub enum Action {
    Serve,
    Init { address: String, port: u16 },
    AddLearner { id: u64, address: String, port: u16 },
    ChangeMembership { ids: Vec<u64> },
    Import { path: PathBuf },
    Export {},
    Metrics {},
    Fsck {},
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let mut config = distribd::config::config(options.config);
    if let Some(name) = options.name {
        config.identifier = name;
    }

    match options.action {
        Action::Serve => {
            let tasks = start_raft_node(config).await.unwrap();

            match signal::ctrl_c().await {
                Ok(()) => {}
                Err(err) => {
                    eprintln!("Unable to listen for shutdown signal: {}", err);
                    // we also shut down in case of error
                }
            }

            tasks.notify_one();
        }
        Action::Init { address, port } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let address = format!("{}:{}", address, port);
            client.init(address).await?;
            print!("Cluster initialized");
        }
        Action::AddLearner { id, address, port } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let address = format!("{}:{}", address, port);
            client.add_learner((id, address)).await?;
            print!("Learner added");
        }
        Action::ChangeMembership { ids } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let ids = ids.into_iter().collect();
            client.change_membership(&ids).await?;
            print!("Membership changed");
        }
        Action::Import { path } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let payload = tokio::fs::read_to_string(path).await?;
            let body: ImportBody = from_str(&payload)?;
            client.import(&body).await?;
            println!("Data imported");
        }
        Action::Export {} => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let body = client.export().await?;
            println!("{}", serde_json::to_string(&body)?);
        }
        Action::Metrics {} => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let metrics = client.metrics().await?;
            println!("{:?}", metrics);
        }
        Action::Fsck {} => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string());
            let body = client.export().await?;

            println!("Checking {} blobs...", body.blobs.len());
            for (digest, blob) in body.blobs.iter() {
                if blob.locations.contains(&config.identifier) {
                    let path = get_blob_path(&config.identifier, digest);

                    if !path.exists() {
                        println!("BLOB: {}: Does not exist on disk", digest);
                        continue;
                    }

                    // check size is correct
                    // chck hash is correct
                }
            }

            println!("Checking {} manifests...", body.manifests.len());
            for (digest, manifest) in body.manifests.iter() {
                if manifest.locations.contains(&config.identifier) {
                    let path = get_manifest_path(&config.identifier, digest);

                    if !path.exists() {
                        println!("MANIFEST: {}: Does not exist on disk", digest);
                        continue;
                    }

                    // check size is correct
                    // chck hash is correct
                }
            }

            println!("Checking tags in {} repositories...", body.tags.len());
            for (repository, tags) in body.tags.iter() {
                for (tag, digest) in tags.iter() {
                    // check digest is a manifest in repository
                }
            }
        }
    }

    Ok(())
}
