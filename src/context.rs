use axum::{
    RequestPartsExt,
    body::Body,
    extract::{ConnectInfo, FromRequestParts},
    http::request::Parts,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use headers::{Authorization, authorization::Bearer};
use jwt_simple::prelude::*;
use reqwest::StatusCode;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use thiserror::Error;
use tower_http::request_id::RequestId;
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
    pub actions: Vec<Action>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AdditionalClaims {
    pub access: Vec<Access>,
}

#[derive(Clone, Debug)]
pub(crate) struct RequestContext {
    pub access: Vec<Access>,
    pub sub: String,
    pub validated_token: bool,
    pub admin: bool,
    pub realm: Option<String>,
    pub service: Option<String>,
    pub user_agent: Option<String>,
    pub method: String,
    pub request_id: String,
    pub peer: SocketAddr,
}

impl RequestContext {
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
            let mut actions = req
                .actions
                .iter()
                .map(|action| action.to_string())
                .collect::<Vec<_>>();
            actions.sort();
            let actions = actions.join(",");
            scopes.push(format!("repository:{repository}:{actions}"));
        }
        let scope = scopes.join(" ");

        format!("Bearer realm=\"{realm}\",service=\"{service}\",scope=\"{scope}\"")
    }

    pub fn get_pull_challenge(&self, repository: &str) -> String {
        self.get_challenge(vec![Access {
            type_: "repository".to_string(),
            name: repository.to_string(),
            actions: Vec::from([Action::Pull]),
        }])
    }

    pub fn get_push_challenge(&self, repository: &str) -> String {
        self.get_challenge(vec![Access {
            type_: "repository".to_string(),
            name: repository.to_string(),
            actions: Vec::from([Action::Pull, Action::Push]),
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

impl FromRequestParts<Arc<RegistryState>> for RequestContext {
    type Rejection = TokenError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<RegistryState>,
    ) -> Result<Self, Self::Rejection> {
        let method = parts.method.to_string().to_uppercase();

        let request_id = match parts.extensions.get::<RequestId>() {
            Some(request_id) => request_id
                .header_value()
                .to_str()
                .unwrap_or("invalid-request-id")
                .to_string(),
            None => {
                return Err(TokenError::Invalid);
            }
        };

        let user_agent = parts
            .headers
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let peer = match parts.extensions.get::<ConnectInfo<SocketAddr>>() {
            Some(connect_info) => connect_info.0,
            None => return Err(TokenError::Invalid),
        };

        let config = match &state.config.authentication {
            None => {
                return Ok(RequestContext {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: true,
                    validated_token: true,
                    service: None,
                    realm: None,
                    user_agent,
                    method,
                    request_id,
                    peer,
                });
            }
            Some(config) => config,
        };

        let header = match parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
            Ok(header) => header,
            Err(_) => {
                return Ok(RequestContext {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: false,
                    validated_token: false,
                    service: Some(state.config.url.clone()),
                    realm: Some(format!("{}/auth/token", state.config.url)),
                    user_agent,
                    method,
                    request_id,
                    peer,
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
            allowed_issuers: Some(HashSet::from_strings(&[state.config.url.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[state.config.url.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<AdditionalClaims> =
            match config
                .key_pair
                .key_pair
                .public_key()
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

        Ok(RequestContext {
            access: claims.custom.access.clone(),
            sub: subject,
            admin: false,
            validated_token: true,
            service: Some(state.config.url.clone()),
            realm: Some(format!("{}/auth/token", state.config.url)),
            user_agent,
            method,
            request_id,
            peer,
        })
    }
}
