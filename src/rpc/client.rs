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
    urls: HashMap<String, String>,
    raft: Arc<Raft>,
}

impl RpcClient {
    pub fn new(config: Configuration, machine: Arc<Mutex<Machine>>, raft: Arc<Raft>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("distribd/mirror")
            .build()
            .unwrap();

        let mut urls = HashMap::new();
        for peer in config.peers {
            let RaftConfig { address, port } = peer.raft;
            urls.insert(peer.name.clone(), format!("http://{address}:{port}"));
        }

        RpcClient {
            client,
            config,
            machine,
            raft,
            urls,
        }
    }

    pub async fn submit(&self, actions: Vec<RegistryAction>) -> Result<(), String> {
        // FIXME: Wait with a timeout
        let leader = self.raft.wait_for_leader().await;

        {
            let machine = self.machine.lock().await;
            if machine.is_leader() {
                let result = self
                    .raft
                    .run_envelope(Envelope {
                        source: self.config.identifier.clone(),
                        destination: self.config.identifier.clone(),
                        term: 0,
                        message: Message::AddEntries { entries: actions },
                    })
                    .await;
            }
        }

        let url = match self.urls.get(leader) {
            Some(url) => format!("{url}/run"),
            None => return Err("Unknown peer".to_string()),
        };

        let resp = self.client.post(url).json(&actions).send().await;

        let index = match resp {
            Ok(resp) => {
                if resp.status() != StatusCode::ACCEPTED {
                    return Err("Submission not accepted by current leader".to_string());
                }

                let submission: Submission = match resp.json().await {
                    Ok(submission) => submission.index,
                    Err(err) => {
                        return Err(format!("Network error: {err:?}"));
                    }
                };
            }
            Err(err) => Err(format!("Network error: {err:?}")),
        };

        self.state.wait_for_commit(index).await;

        Ok(())
    }
}
