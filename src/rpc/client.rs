use std::{collections::HashMap, sync::Arc};

use figment::providers::Env;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    config::Configuration,
    machine::{Envelope, Machine, Message},
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
    inbox: tokio::sync::mpsc::Sender<Envelope>,
}

impl RpcClient {
    pub fn new(
        config: Configuration,
        machine: Arc<Mutex<Machine>>,
        inbox: tokio::sync::mpsc::Sender<Envelope>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("distribd/mirror")
            .build()
            .unwrap();

        RpcClient {
            client,
            config,
            machine,
            inbox,
            urls: HashMap::new(),
        }
    }

    pub async fn submit(&self, actions: Vec<RegistryAction>) -> Result<(), String> {
        // FIXME: Wait with a timeout
        let leader = self.raft.wait_for_leader().await;

        {
            let machine = self.machine.lock().await;
            if machine.is_leader() {
                self.inbox
                    .send(Envelope {
                        source: self.config.identifier.clone(),
                        destination: self.config.identifier.clone(),
                        term: 0,
                        message: Message::AddEntries { entries: actions },
                    })
                    .await;
            }
        }

        let url = match self.urls.get(leader) {
            Some(url) => url,
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
