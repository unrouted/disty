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
                        let status_code = response.status();
                        if status_code != reqwest::StatusCode::OK {
                            warn!(
                                "Mint: Failed to mint pull token for {repository}: status code {status_code}"
                            );
                            return Err("");
                        }

                        let MintResponse { access_code } = match response.json().await {
                            Ok(resp) => resp,
                            Err(err) => {
                                warn!("Mint: Failed to mint pull token for {repository}: {err}");
                                return Err("");
                            }
                        };

                        Ok(builder.header("Authorization", format!("Bearer {access_code}")))
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
