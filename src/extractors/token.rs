use crate::app::RegistryApp;
use crate::types::RepositoryName;
use actix_web::{http::Error, web::Data, FromRequest, HttpRequest};
use futures_util::future::{ready, Ready};
use jwt_simple::prelude::*;
use std::collections::HashSet;
use tracing::{debug, info};

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

    pub fn get_pull_challenge(&self, repository: &RepositoryName) -> String {
        self.get_challenge(vec![Access {
            repository: repository.clone(),
            permissions: HashSet::from(["pull".to_string()]),
        }])
    }

    pub fn get_push_challenge(&self, repository: &RepositoryName) -> String {
        self.get_challenge(vec![Access {
            repository: repository.clone(),
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

impl FromRequest for Token {
    type Future = Ready<Result<Self, Self::Error>>;
    type Error = Error;

    fn from_request(
        req: &HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> <Self as FromRequest>::Future {
        let app = req.app_data::<Data<RegistryApp>>().unwrap();

        let config = match &app.config.token_server {
            None => {
                return ready(Ok(Token {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: true,
                    validated_token: true,
                    service: None,
                    realm: None,
                }));
            }
            Some(config) => config,
        };

        let header: String = match req.headers().get("authorization") {
            Some(header) => header.to_str().unwrap().into(),
            _ => {
                return ready(Ok(Token {
                    access: vec![],
                    sub: "anonymous".to_string(),
                    admin: false,
                    validated_token: false,
                    service: Some(config.service.clone()),
                    realm: Some(config.realm.clone()),
                }));
            }
        };

        let (token_type, token_bytes) = match header.split_once(' ') {
            Some(value) => value,
            _ => {
                panic!("MEH");
            } //return Outcome::Failure((Status::Forbidden, TokenError::Missing)),
        };

        if token_type.to_lowercase() != "bearer" {
            info!("Not bearer token");
            // return Outcome::Failure((Status::Forbidden, TokenError::Missing));
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
                // return Outcome::Failure((Status::Forbidden, TokenError::Missing));
                panic!("MEH2");
            }
        };

        let subject = match claims.subject {
            Some(subject) => subject,
            _ => {
                info!("Could not retrieve subject from token");
                // return Outcome::Failure((Status::Forbidden, TokenError::Missing));
                panic!("MEH3")
            }
        };

        debug!("Validated token for subject \"{subject}\"");

        ready(Ok(Token {
            access: claims.custom.access.clone(),
            sub: subject,
            admin: false,
            validated_token: true,
            service: Some(config.service.clone()),
            realm: Some(config.realm.clone()),
        }))
    }
}
