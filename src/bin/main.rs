use std::path::PathBuf;

use chrono::Utc;
use clap::{Parser, Subcommand};
use distribd::client::RegistryClient;
use distribd::network::management::ImportBody;
use distribd::network::raft_network_impl::RegistryNetwork;
use distribd::start_raft_node;
use distribd::store::{RegistryRequest, RegistryStore};
use distribd::types::RegistryAction;
use distribd::utils::{get_blob_path, get_manifest_path};
use distribd::RegistryTypeConfig;
use openraft::Raft;
use reqwest_retry::policies::ExponentialBackoff;
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
    Init {
        address: String,
        port: u16,
    },
    AddLearner {
        id: u64,
        address: String,
        port: u16,
    },
    ChangeMembership {
        ids: Vec<u64>,
    },
    Import {
        path: PathBuf,
    },
    Export {},
    Metrics {},
    Fsck {
        #[clap(short, long, action)]
        repair: bool,
    },
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

    let retry_policy = Some(ExponentialBackoff {
        max_n_retries: 3,
        max_retry_interval: std::time::Duration::from_millis(30),
        min_retry_interval: std::time::Duration::from_millis(100),
        backoff_exponent: 2,
    });

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
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let address = format!("{}:{}", address, port);
            client.init(address).await?;
            print!("Cluster initialized");
        }
        Action::AddLearner { id, address, port } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let address = format!("{}:{}", address, port);
            client.add_learner((id, address)).await?;
            print!("Learner added");
        }
        Action::ChangeMembership { ids } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let ids = ids.into_iter().collect();
            client.change_membership(&ids).await?;
            print!("Membership changed");
        }
        Action::Import { path } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let payload = tokio::fs::read_to_string(path).await?;
            let body: ImportBody = from_str(&payload)?;
            client.import(&body).await?;
            println!("Data imported");
        }
        Action::Export {} => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let body = client.export().await?;
            println!("{}", serde_json::to_string(&body)?);
        }
        Action::Metrics {} => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let metrics = client.metrics().await?;
            println!("{:?}", metrics);
        }
        Action::Fsck { repair } => {
            let client = RegistryClient::new(1, "127.0.0.1:8080".to_string(), retry_policy);
            let mut body = client.export().await?;

            let mut fixes = vec![];

            println!("Checking {} blobs...", body.blobs.len());
            for (digest, blob) in body.blobs.iter_mut() {
                if blob.locations.contains(&config.identifier) {
                    let path = get_blob_path(&config.storage, digest);

                    if !path.exists() {
                        fixes.push(RegistryAction::BlobUnstored {
                            timestamp: Utc::now(),
                            digest: digest.clone(),
                            location: config.identifier.clone(),
                            user: "$fsck".to_string(),
                        });
                        blob.locations.remove(&config.identifier);
                        continue;
                    }

                    // check size is correct
                    // chck hash is correct
                }
            }

            println!("Checking {} manifests...", body.manifests.len());

            let extractor = distribd::extractor::Extractor::new();

            for (digest, manifest) in body.manifests.iter_mut() {
                if manifest.locations.contains(&config.identifier) {
                    let path = get_manifest_path(&config.storage, digest);

                    if !path.exists() {
                        fixes.push(RegistryAction::ManifestUnstored {
                            timestamp: Utc::now(),
                            digest: digest.clone(),
                            location: config.identifier.clone(),
                            user: "$fsck".to_string(),
                        });
                        manifest.locations.remove(&config.identifier);
                        continue;
                    }

                    match extractor
                        .parse_manifest(path.clone(), manifest.content_type.as_ref().unwrap())
                        .await
                    {
                        Ok(extractions) => {
                            for extraction in extractions.iter() {
                                match body.blobs.get(&extraction.digest) {
                                    Some(blob) => {
                                        for repo in manifest.repositories.iter() {
                                            if !blob.repositories.contains(repo) {
                                                println!("Manifest is invalid: {:?}: {}: Not all blobs are available for {}.", manifest.repositories, digest, repo)
                                            }
                                        }
                                        if let Some(size) = extraction.size {
                                            if blob.size != Some(size) {
                                                println!("Manifest is invalid: {:?}: {}: Blob {} is wrong size: {:?} actual vs {:?} declared ({} / {})", manifest.repositories, digest, extraction.digest, blob.size, extraction.size, manifest.created, manifest.updated);
                                            }
                                        }
                                    }
                                    None => {
                                        println!(
                                            "Manifest is invalid: {:?}: {}: Blob {} is missing",
                                            manifest.repositories, digest, extraction.digest,
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("Manifest: Manifest is not valid: {}: {:?}", digest, e);
                        }
                    }

                    // check size is correct
                    // chck hash is correct
                }
            }

            println!("Checking tags in {} repositories...", body.tags.len());
            for (_repository, tags) in body.tags.iter() {
                for (_tag, _digest) in tags.iter() {
                    // check digest is a manifest in repository
                }
            }

            for action in fixes.iter() {
                println!("{:?}", action);
            }

            if repair {
                client
                    .write(&RegistryRequest::Transaction { actions: fixes })
                    .await?;
            }
        }
    }

    Ok(())
}
