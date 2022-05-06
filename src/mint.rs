use pyo3::prelude::*;
use reqwest::{self, RequestBuilder};
use serde::Deserialize;

use crate::types::RepositoryName;

#[derive(Clone, FromPyObject)]
pub enum MintConfig {
    Minter {
        #[pyo3(item)]
        realm: String,
        #[pyo3(item)]
        service: String,
        #[pyo3(item)]
        username: String,
        #[pyo3(item)]
        password: String,
    },
    None {
        #[pyo3(item)]
        enabled: bool,
    },
}

#[derive(Deserialize)]
struct MintResponse {
    access_code: String,
}

#[derive(Clone)]
pub(crate) struct Mint {
    client: reqwest::Client,
    config: MintConfig,
}

impl Mint {
    pub fn new(config: MintConfig) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("distribd/mint")
            .build()
            .unwrap();

        Mint { client, config }
    }

    pub async fn enrich_request(
        &self,
        builder: RequestBuilder,
        repository: RepositoryName,
    ) -> Result<RequestBuilder, &str> {
        let scope = "repository:{repository}:pull".to_string();

        match &self.config {
            MintConfig::Minter {
                realm,
                service,
                username,
                password,
            } => {
                let response = self
                    .client
                    .get(realm.clone())
                    .basic_auth(username.clone(), Some(password.clone()))
                    .query(&[("service", service.clone()), ("scope", scope)])
                    .send()
                    .await;

                match response {
                    Ok(response) => {
                        if response.status() != 200 {
                            warn!(
                                "Mint: Failed to mint pull token for {repository}: status code 000"
                            );
                            return Err("");
                        }

                        let payload: MintResponse = match response.json().await {
                            Ok(resp) => resp,
                            Err(err) => {
                                warn!("Mint: Failed to mint pull token for {repository}: {err}");
                                return Err("");
                            }
                        };

                        Ok(builder.header("Authorization", payload.access_code))
                    }
                    Err(err) => {
                        warn!("Mint: Failed to mint pull token for {repository}: {err}");
                        Err("")
                    }
                }
            }
            MintConfig::None { enabled: _ } => Ok(builder),
        }
    }
}
