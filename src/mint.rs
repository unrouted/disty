use reqwest::{self, RequestBuilder};
use serde::Deserialize;

use crate::config::MintConfig;
use crate::types::RepositoryName;

#[derive(Deserialize)]
struct MintResponse {
    access_token: String,
}

#[derive(Clone)]
pub(crate) struct Mint {
    client: reqwest::Client,
    config: Option<MintConfig>,
}

impl Mint {
    pub fn new(config: Option<MintConfig>) -> Self {
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
        let scope = format!("repository:{repository}:pull");

        match &self.config {
            Some(MintConfig {
                realm,
                service,
                username,
                password,
            }) => {
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

                        let MintResponse { access_token } = match response.json().await {
                            Ok(resp) => resp,
                            Err(err) => {
                                warn!("Mint: Failed to mint pull token for {repository}: {err}");
                                return Err("");
                            }
                        };

                        Ok(builder.header("Authorization", format!("Bearer {access_token}")))
                    }
                    Err(err) => {
                        warn!("Mint: Failed to mint pull token for {repository}: {err}");
                        Err("")
                    }
                }
            }
            None => Ok(builder),
        }
    }
}
