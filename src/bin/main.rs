use clap::Parser;
use distribd::network::raft_network_impl::RegistryNetwork;
use distribd::start_raft_node;
use distribd::store::RegistryStore;
use distribd::RegistryTypeConfig;
use openraft::Raft;
use tokio::signal;
use tracing_subscriber::EnvFilter;

pub type RegistryRaft = Raft<RegistryTypeConfig, RegistryNetwork, RegistryStore>;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,
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
    let _options = Opt::parse();

    let config = distribd::config::config(None);

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
