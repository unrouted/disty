use axum::{
    RequestPartsExt,
    body::Body,
    extract::FromRequestParts,
    http::request::Parts,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use headers::{Authorization, authorization::Bearer};
use jwt_simple::prelude::*;
use reqwest::StatusCode;
use std::{collections::HashSet, sync::Arc};
use thiserror::Error;
use tracing::{debug, info};

use crate::{config::acl::Action, state::RegistryState};

#[derive(Debug, Error)]
pub enum TokenError {
    #[error("The authorization token contains invalid data")]
    Invalid,
}

impl IntoResponse for TokenError {
    fn into_response(self) -> axum::response::Response {
        Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Access {
    #[serde(rename = "type")]
    pub type_: String,
    pub name: String,
    pub actions: HashSet<Action>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AdditionalClaims {
    pub access: Vec<Access>,
}

pub(crate) struct Token {
    pub access: Vec<Access>,
    pub sub: String,
    pub validated_token: bool,
    admin: bool,
    realm: Option<String>,
    service: Option<String>,
}

impl Token {
    pub fn get_challenge(&self, access: Vec<Access>) -> String {
        let service = self
            .service
            .as_ref()
            .expect("Service should not start with auth on but no 'service' config");
        let realm = self
            .realm
            .as_ref()
            .expect("Service should not start with auth on but no 'realm' config");

        let mut scopes = vec![];
        for req in access.iter() {
            let repository = &req.name;
            let actions = req
                .actions
                .iter()
                .map(|action| action.to_string())
                .collect::<Vec<_>>()
                .join(",");
            scopes.push(format!("repository:{repository}:{actions}"));
        }
        let scope = scopes.join(" ");

        format!("Bearer realm=\"{realm}\",service=\"{service}\",scope=\"{scope}\"")
    }

    pub fn get_pull_challenge(&self, repository: &str) -> String {
        self.get_challenge(vec![Access {
            type_: "repository".to_string(),
            name: repository.to_string(),
            actions: HashSet::from([Action::Pull]),
        }])
    }

    pub fn get_push_challenge(&self, repository: &str) -> String {
        self.get_challenge(vec![Access {
            type_: "repository".to_string(),
            name: repository.to_string(),
            actions: HashSet::from([Action::Pull, Action::Push]),
        }])
    }

    pub fn get_general_challenge(&self) -> String {
        let service = self
            .service
            .as_ref()
            .expect("Service should not start with auth on but no 'service' config");
        let realm = self
            .realm
            .as_ref()
            .expect("Service should not start with auth on but no 'realm' config");

        format!("Bearer realm=\"{realm}\",service=\"{service}\"")
    }

    pub fn has_permission(&self, repository: &str, permission: &str) -> bool {
        if !self.validated_token {
            debug!("Not a validated token");
            return false;
        }

        if self.admin {
            debug!("Got an admin token");
            return true;
        }

        debug!("Need {permission} for {repository}");

        for access in self.access.iter() {
            debug!("Checking {access:?}");

            if access.name == repository
                && access
                    .actions
                    .contains(&Action::try_from(permission.to_string()).unwrap())
            {
                return true;
            }
        }

        info!("Didn't find a matching access rule");

        false
    }
}

impl FromRequestParts<Arc<RegistryState>> for Token {
    type Rejection = TokenError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<RegistryState>,
    ) -> Result<Self, Self::Rejection> {
        let config = match &state.config.token_server {
            None => {
                return Ok(Token {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: true,
                    validated_token: true,
                    service: None,
                    realm: None,
                });
            }
            Some(config) => config,
        };

        let header = match parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
            Ok(header) => header,
            Err(_) => {
                return Ok(Token {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: false,
                    validated_token: false,
                    service: Some(config.service.clone()),
                    realm: Some(config.realm.clone()),
                });
            }
        };

        let token_bytes = header.0.token();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[config.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[config.service.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<AdditionalClaims> = match config
            .public_key
            .public_key
            .verify_token::<AdditionalClaims>(token_bytes, Some(options))
        {
            Ok(claims) => claims,
            Err(error) => {
                info!("Could not verify token: {error}");
                return Err(TokenError::Invalid);
            }
        };

        let subject = match claims.subject {
            Some(subject) => subject,
            _ => {
                info!("Could not retrieve subject from token");
                return Err(TokenError::Invalid);
            }
        };

        debug!("Validated token for subject \"{subject}\"");

        Ok(Token {
            access: claims.custom.access.clone(),
            sub: subject,
            admin: false,
            validated_token: true,
            service: Some(config.service.clone()),
            realm: Some(config.realm.clone()),
        })
    }
}
