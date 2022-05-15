use std::{collections::HashMap, sync::Arc};

use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    config::{Configuration, RaftConfig},
    machine::{Envelope, Machine, Message},
    raft::{Raft, RaftQueueResult},
    types::RegistryAction,
};

#[derive(Deserialize)]
pub struct Submission {
    pub index: u64,
}

pub struct RpcClient {
    client: Client,
    config: Configuration,
    machine: Arc<Mutex<Machine>>,
    destinations: HashMap<String, Destination>,
    raft: Arc<Raft>,
}

#[derive(Clone, Debug)]
enum Destination {
    Local,
    Remote { address: String, port: u32 },
}

impl RpcClient {
    pub fn new(config: Configuration, machine: Arc<Mutex<Machine>>, raft: Arc<Raft>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("distribd/mirror")
            .build()
            .unwrap();

        let mut destinations = HashMap::new();
        for peer in config.peers {
            if peer.name == config.identifier {
                destinations.insert(peer.name, Destination::Local);
                continue;
            }
            let RaftConfig { address, port } = peer.raft;
            destinations.insert(peer.name, Destination::Remote { address, port });
        }

        RpcClient {
            client,
            config,
            machine,
            raft,
            destinations,
        }
    }

    async fn get_leader(&self) -> Option<Destination> {
        let leader = self.raft.wait_for_leader().await;
        Some(self.destinations.get(leader).unwrap().clone())
    }

    pub async fn submit(&self, actions: Vec<RegistryAction>) -> Result<(), String> {
        let result = match self.get_leader().await {
            Some(Destination::Local) => {
                self.raft
                    .run_envelope(Envelope {
                        source: self.config.identifier.clone(),
                        destination: self.config.identifier.clone(),
                        term: 0,
                        message: Message::AddEntries { entries: actions },
                    })
                    .await
            }
            Some(Destination::Remote { address, port }) => {
                let url = format!("http://{address}:{port}/run");
                let resp = self.client.post(url).json(&actions).send().await;

                match resp {
                    Ok(resp) => {
                        if resp.status() != StatusCode::ACCEPTED {
                            return Err("Submission not accepted by current leader".to_string());
                        }

                        match resp.json().await {
                            Ok(submission) => submission,
                            Err(err) => {
                                return Err(format!("Network error: {err:?}"));
                            }
                        }
                    }
                    Err(err) => return Err(format!("Network error: {err:?}")),
                }
            }
            None => {
                return Err(format!("Unable to find leader"));
            }
        };

        match result {
            Ok(RaftQueueResult { index, term }) => {
                self.state.wait_for_commit(index).await;

                Ok(())
            }
            Err(err) => {
                return Err(format!("State machine error: {err}"));
            }
        }
    }
}
