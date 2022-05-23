use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Mutex;

use crate::{
    config::{Configuration, RaftConfig},
    log::LogEntry,
    machine::{Envelope, Machine, Message},
    raft::{Raft, RaftEvent, RaftQueueResult},
    types::{RegistryAction, RegistryState},
};

#[derive(Deserialize)]
pub struct Submission {
    pub index: usize,
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
        let machine = self.machine.lock().await;

        loop {
            if machine.is_leader() {
                return Some(Destination::Local);
            }

            match &machine.leader {
                Some(leader) => return self.destinations.get(leader).cloned(),
                None => {}
            };

            self.raft.leader.clone().changed().await;
        }
    }

    pub async fn send(&self, actions: Vec<RegistryAction>) -> bool {
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
                            return false;
                            // return Err("Submission not accepted by current leader".to_string());
                        }

                        match resp.json().await {
                            Ok(submission) => submission,
                            Err(_err) => {
                                return false;
                                // return Err(format!("Network error: {err:?}"));
                            }
                        }
                    }
                    Err(_err) => {
                        return false;
                        // return Err(format!("Network error: {err:?}"))
                    }
                }
            }
            None => {
                return false;
                // return Err(format!("Unable to find leader"));
            }
        };

        match result {
            Ok(RaftQueueResult { index, term }) => {
                loop {
                    match subscriber.recv().await {
                        Ok(RaftEvent::Committed {
                            start_index,
                            entries,
                        }) => {
                            let start_index = start_index.unwrap_or(0);

                            let last_index = start_index + entries.len() as usize;
                            if index > last_index {
                                continue;
                            }
                            if start_index > index {
                                // We missed the event we wanted
                                return false;
                                // return Err("Event stream missed some data".to_string());
                            }
                            let offset = index - start_index;
                            let actual_term = match entries.get(offset as usize) {
                                Some(LogEntry { term, entry: _ }) => term,
                                None => {
                                    return false;

                                    // return Err("Offset missing".to_string());
                                }
                            };

                            if &term != actual_term {
                                return false;
                                // return Err("Commit failed".to_string());
                            }

                            return true;
                        }
                        Err(RecvError::Lagged(_)) => {
                            // We ignore log errors - we'll catch up or abort the txn
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            return false;
                            // return Err("Event stream closed".to_string());
                        }
                    }
                }
            }
            Err(_err) => {
                false
                // return Err(format!("State machine error: {err}"));
            }
        }
    }
}

pub fn start_rpc_client(
    config: Configuration,
) -> HashMap<String, tokio::sync::mpsc::Sender<Envelope>> {
    let mut outboxes = HashMap::new();

    for peer in config.peers {
        if peer.name == config.identifier {
            continue;
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Envelope>(100);

        outboxes.insert(peer.name.clone(), tx);

        let RaftConfig { address, port } = peer.raft;
        let url = format!("http://{address}:{port}/queue");
        let client = reqwest::Client::builder().build().unwrap();

        tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let resp = client.post(&url).json(&envelope).send().await;
                match resp {
                    Ok(_resp) => {}
                    Err(_err) => {}
                }
            }
        });
    }

    outboxes
}
