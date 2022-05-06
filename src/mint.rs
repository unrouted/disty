use pyo3::prelude::*;
use reqwest::{self, RequestBuilder};
use serde::Deserialize;

use crate::types::RepositoryName;

pub struct MintConfig {
    pub enabled: bool,
    pub issuer: Option<String>,
    pub service: Option<String>,
    pub realm: Option<String>,
    pub public_key: Option<String>,
}

impl FromPyObject<'_> for MintConfig {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        // FIXME: This should send nice errors back to python if any of the unwraps fail...
        let enabled: bool = dict.get_item("enabled").unwrap().extract().unwrap();
        if !enabled {
            return Ok(MintConfig {
                enabled: false,
                issuer: None,
                service: None,
                realm: None,
                public_key: None,
            });
        }

        Ok(MintConfig {
            enabled: true,
            issuer: Some(dict.get_item("issuer").unwrap().extract().unwrap()),
            service: Some(dict.get_item("service").unwrap().extract().unwrap()),
            realm: Some(dict.get_item("realm").unwrap().extract().unwrap()),
            public_key: Some(dict.get_item("public_key").unwrap().extract().unwrap()),
        })
    }
}

#[derive(Deserialize)]
struct MintResponse {
    access_code: String,
}

#[derive(Clone)]
pub(crate) struct Mint {
    client: reqwest::Client,
    realm: String,
    service: String,
    username: String,
    password: String,
}

impl Mint {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .user_agent("distribd/mint")
            .build()
            .unwrap();

        Mint {
            client,
            realm: String::from(""),
            service: String::from(""),
            username: String::from(""),
            password: String::from(""),
        }
    }

    pub async fn enrich_request(
        &self,
        builder: RequestBuilder,
        repository: RepositoryName,
    ) -> Result<RequestBuilder, &str> {
        let scope = "repository:{repository}:pull".to_string();

        let response = self
            .client
            .get(self.realm.clone())
            .basic_auth(self.username.clone(), Some(self.password.clone()))
            .query(&[("service", self.service.clone()), ("scope", scope)])
            .send()
            .await;

        match response {
            Ok(response) => {
                if response.status() != 200 {
                    warn!("Mint: Failed to mint pull token for {repository}: status code 000");
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
}
