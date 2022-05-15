use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Mutex;

use crate::{
    config::{Configuration, RaftConfig},
    machine::{Envelope, Machine, Message, LogEntry},
    raft::{Raft, RaftQueueResult, RaftEvent},
    types::{RegistryAction, RegistryState},
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
    registry: Arc<RegistryState>,
}

#[derive(Clone, Debug)]
enum Destination {
    Local,
    Remote { address: String, port: u32 },
}

impl RpcClient {
    pub fn new(
        config: Configuration,
        machine: Arc<Mutex<Machine>>,
        raft: Arc<Raft>,
        registry: Arc<RegistryState>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("distribd/mirror")
            .build()
            .unwrap();

        let mut destinations = HashMap::new();
        for peer in config.peers.iter().cloned() {
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
            registry,
        }
    }

    async fn get_leader(&self) -> Option<Destination> {
        // let leader = self.raft.wait_for_leader().await;
        // Some(self.destinations.get(leader).unwrap().clone())
        Some(Destination::Local)
    }

    pub async fn submit(&self, actions: Vec<RegistryAction>) -> Result<(), String> {
        let mut subscriber = self.registry.events.subscribe();

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
                loop {
                    match subscriber.recv().await {
                        Ok(RaftEvent::Committed {start_index, entries }) => {
                            let last_index = start_index + entries.len() as u64;
                            if index > last_index {
                                continue;
                            }
                            if start_index > index {
                                // We missed the event we wanted
                                return Err("Event stream missed some data".to_string());
                            }
                            let offset = index - start_index;
                            let actual_term = match entries.get(offset as usize) {
                                Some(LogEntry { term, entry: _}) => term,
                                None => {
                                    return Err("Offset missing".to_string());
                                }
                            };

                            if &term != actual_term {
                                return Err("Commit failed".to_string());
                            }
                        }
                        Err(RecvError::Lagged(_)) => {
                            // We ignore log errors - we'll catch up or abort the txn
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            return Err("Event stream closed".to_string());
                        }
                    }
                }
            }
            Err(err) => {
                return Err(format!("State machine error: {err}"));
            }
        }
    }
}
