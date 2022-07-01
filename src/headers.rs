use crate::app::RegistryApp;
use crate::config::Configuration;
use crate::types::RepositoryName;
use jwt_simple::prelude::*;
use log::info;
use rocket::http::Status;
use rocket::request::{self, FromRequest, Outcome, Request};
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) struct ContentRange {
    pub start: u64,
    pub stop: u64,
}

#[derive(Debug)]
pub(crate) enum ContentRangeError {
    Missing,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ContentRange {
    type Error = ContentRangeError;

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let token = req.headers().get_one("content-range");
        match token {
            Some(token) => match token.split_once('-') {
                Some((start, stop)) => {
                    let start: u64 = match start.parse() {
                        Ok(value) => value,
                        _ => {
                            return Outcome::Failure((
                                Status::BadRequest,
                                ContentRangeError::Missing,
                            ))
                        }
                    };
                    let stop: u64 = match stop.parse() {
                        Ok(value) => value,
                        _ => {
                            return Outcome::Failure((
                                Status::BadRequest,
                                ContentRangeError::Missing,
                            ))
                        }
                    };
                    Outcome::Success(ContentRange { start, stop })
                }
                _ => Outcome::Failure((Status::BadRequest, ContentRangeError::Missing)),
            },
            None => Outcome::Failure((Status::BadRequest, ContentRangeError::Missing)),
        }
    }
}

pub(crate) struct ContentType {
    pub content_type: String,
}

#[derive(Debug)]
pub(crate) enum ContentTypeError {
    Missing,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ContentType {
    type Error = ContentTypeError;

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let token = req.headers().get_one("content-type");
        match token {
            Some(token) => Outcome::Success(ContentType {
                content_type: token.to_string(),
            }),
            None => Outcome::Failure((Status::BadRequest, ContentTypeError::Missing)),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Access {
    #[serde(rename = "name")]
    pub repository: RepositoryName,
    #[serde(rename = "actions")]
    pub permissions: HashSet<String>,
}

#[derive(Serialize, Deserialize)]
struct AdditionalClaims {
    access: Vec<Access>,
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
            let repository = &req.repository;
            let actions = req
                .permissions
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",");
            scopes.push(format!("repository:{repository}:{actions}"));
        }
        let scope = scopes.join(" ");

        format!("Bearer realm=\"{realm}\",service=\"{service}\",scope=\"{scope}\"")
    }

    pub fn get_pull_challenge(&self, repository: RepositoryName) -> String {
        self.get_challenge(vec![Access {
            repository,
            permissions: HashSet::from(["pull".to_string()]),
        }])
    }

    pub fn get_push_challenge(&self, repository: RepositoryName) -> String {
        self.get_challenge(vec![Access {
            repository,
            permissions: HashSet::from(["pull".to_string(), "push".to_string()]),
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

    pub fn has_permission(&self, repository: &RepositoryName, permission: &str) -> bool {
        if !self.validated_token {
            info!("Not a validated token");
            return false;
        }

        if self.admin {
            info!("Got an admin token");
            return true;
        }

        info!("Need {permission} for {repository}");

        for access in self.access.iter() {
            info!("Checking {access:?}");

            if &access.repository == repository
                && access.permissions.contains(&permission.to_string())
            {
                return true;
            }
        }

        info!("Didn't find a matching access rule");

        false
    }
}

#[derive(Debug)]
pub(crate) enum TokenError {
    Missing,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Token {
    type Error = TokenError;

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let config = match req.rocket().state::<Arc<RegistryApp>>() {
            Some(value) => &value.settings,
            _ => return Outcome::Failure((Status::BadGateway, TokenError::Missing)),
        };

        let config = match &config.token_server {
            None => {
                return Outcome::Success(Token {
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

        let header = match req.headers().get_one("authorization") {
            Some(header) => header.to_string(),
            _ => {
                return Outcome::Success(Token {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: false,
                    validated_token: false,
                    service: Some(config.service.clone()),
                    realm: Some(config.realm.clone()),
                });
            }
        };

        let (token_type, token_bytes) = match header.split_once(' ') {
            Some(value) => value,
            _ => return Outcome::Failure((Status::Forbidden, TokenError::Missing)),
        };

        if token_type.to_lowercase() != "bearer" {
            info!("Not bearer token");
            return Outcome::Failure((Status::Forbidden, TokenError::Missing));
        }

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

        let claims = match config
            .public_key
            .public_key
            .verify_token::<AdditionalClaims>(token_bytes, Some(options))
        {
            Ok(claims) => claims,
            Err(error) => {
                info!("Could not verify token: {error}");
                return Outcome::Failure((Status::Forbidden, TokenError::Missing));
            }
        };

        let subject = match claims.subject {
            Some(subject) => subject,
            _ => {
                info!("Could not retrieve subject from token");
                return Outcome::Failure((Status::Forbidden, TokenError::Missing));
            }
        };

        info!("Validated token for subject \"{subject}\"");

        Outcome::Success(Token {
            access: claims.custom.access.clone(),
            sub: subject,
            admin: false,
            validated_token: true,
            service: Some(config.service.clone()),
            realm: Some(config.realm.clone()),
        })
    }
}
