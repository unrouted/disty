use clap::Parser;
use distribd::network::raft_network_impl::RegistryNetwork;
use distribd::start_raft_node;
use distribd::store::RegistryStore;
use distribd::RegistryTypeConfig;
use openraft::Raft;
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
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
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

    let tasks = start_raft_node(config).await.unwrap();

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }

    tasks.notify_one();

    Ok(())
}
