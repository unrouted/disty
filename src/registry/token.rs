use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use anyhow::Result;
use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use headers::{
    Authorization,
    authorization::{self, Basic},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    config::{
        Issuer,
        acl::{AclCheck, Action, RequestContext},
    },
    error::RegistryError,
    issuer::issue_token,
    state::RegistryState,
    token::Access,
};

#[derive(Debug, Deserialize)]
pub(crate) struct TokenRequest {
    service: String,
    scope: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TokenResponse {
    token: String,
    expires_in: u64,
    issued_at: String,
}

pub async fn authenticate(
    issuer: &Issuer,
    req_username: &str,
    req_password: &str,
) -> Result<Option<HashMap<String, String>>> {
    for user in &issuer.users {
        match user {
            crate::config::User::Password { username, password } => {
                if username != req_username {
                    continue;
                }

                if pwhash::unix::verify(req_password, password) {
                    return Ok(Some(HashMap::new()));
                }
            }
            crate::config::User::Token { username, issuer } => {
                if username != req_username {
                    continue;
                }

                return Ok(
                    match issuer
                        .verify::<HashMap<String, String>>(req_password)
                        .await?
                    {
                        Some(claims) => Some(claims.custom),
                        None => None,
                    },
                );
            }
        }
    }
    Ok(None)
}

pub(crate) async fn token(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<TokenRequest>,
    authorization: TypedHeader<Authorization<Basic>>,
    State(registry): State<Arc<RegistryState>>,
) -> Result<Response, RegistryError> {
    let issuer = registry.config.issuer.as_ref().unwrap();

    let Some(claims) =
        authenticate(issuer, authorization.username(), authorization.password()).await?
    else {
        return Ok((StatusCode::UNAUTHORIZED, "Invalid credentials").into_response());
    };

    let mut access_map: HashMap<String, HashSet<Action>> = HashMap::new();

    for scope in &params.scope {
        let parts: Vec<&str> = scope.split(':').collect();
        if parts.len() == 3 && parts[0] == "repository" {
            let repo = parts[1];
            let action = Action::try_from(parts[2].to_string()).unwrap();

            let allowed_actions = issuer.acls.check_access(&RequestContext {
                username: authorization.username().to_string(),
                claims: claims.clone(),
                ip: addr.ip(),
                repository: repo.to_string(),
            });
            if allowed_actions.contains(&action) {
                access_map
                    .entry(repo.to_string())
                    .or_insert_with(HashSet::new)
                    .insert(action);
            }
        }
    }

    let access_entries = access_map
        .into_iter()
        .map(|(repo, actions)| Access {
            type_: "repository".to_string(),
            name: repo,
            actions: actions.into_iter().collect(),
        })
        .collect();

    let token = issue_token(
        registry.config.token_server.as_ref().unwrap(),
        access_entries,
    )?;

    Ok(Json(TokenResponse {
        token: token.token,
        expires_in: token.expires_in.as_secs(),
        issued_at: token
            .issued_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    })
    .into_response())
}
