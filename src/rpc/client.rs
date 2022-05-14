use std::collections::HashMap;

use reqwest::{Client, StatusCode};
use serde::Deserialize;

use crate::{types::RegistryAction, config::Configuration};

#[derive(Deserialize)]
pub struct Submission {
    pub index: u64,
}

pub struct RpcClient {
    client: Client,
    config: Configuration,
    urls: HashMap<String, String>,
}

impl RpcClient {
    pub async fn submit(&self, actions: Vec<RegistryAction>) -> Result<(), String> {
        // FIXME: Wait with a timeout
        let leader = self.raft.wait_for_leader().await;

        if self.machine.is_leader {
            return self.raft.submit(actions).await;
        }

        let url = match self.urls.get(leader) {
            Some(url) => url,
            None => return Err("Unknown peer".to_string())
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
            Err(err) => Err(format!("Network error: {err:?}"))
        };

        self.state.wait_for_commit(index).await;

        Ok(())
    }
}
