use crate::token::TokenConfig;
use crate::types::RepositoryName;
use jwt_simple::prelude::*;
use rocket::http::Status;
use rocket::request::{self, FromRequest, Outcome, Request};
use std::collections::HashSet;

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
            Some(token) => match token.split_once("-") {
                Some((start, stop)) => {
                    let start: u64 = match start.parse() {
                        Ok(value) => value,
                        _ => {
                            return Outcome::Failure((
                                Status::Unauthorized,
                                ContentRangeError::Missing,
                            ))
                        }
                    };
                    let stop: u64 = match stop.parse() {
                        Ok(value) => value,
                        _ => {
                            return Outcome::Failure((
                                Status::Unauthorized,
                                ContentRangeError::Missing,
                            ))
                        }
                    };
                    Outcome::Success(ContentRange { start, stop })
                }
                _ => Outcome::Failure((Status::Unauthorized, ContentRangeError::Missing)),
            },
            None => Outcome::Failure((Status::Unauthorized, ContentRangeError::Missing)),
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
            None => Outcome::Failure((Status::Unauthorized, ContentTypeError::Missing)),
        }
    }
}

pub(crate) struct Access {
    pub repository: RepositoryName,
    pub permissions: HashSet<String>,
}

pub(crate) struct Token {
    pub access: Vec<Access>,
    pub sub: String,
    admin: bool,
}

impl Token {
    pub fn has_permission(&self, repository: &RepositoryName, permission: &str) -> bool {
        if self.admin {
            return true;
        }

        for access in self.access.iter() {
            if &access.repository == repository
                && access.permissions.contains(&permission.to_string())
            {
                return true;
            }
        }

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
        let config = match req.rocket().state::<TokenConfig>() {
            Some(value) => value,
            _ => return Outcome::Failure((Status::Unauthorized, TokenError::Missing)),
        };

        if !config.enabled {
            return Outcome::Success(Token {
                access: vec![],
                sub: "anonymous".to_string(),
                admin: true,
            });
        }

        let header = match req.headers().get_one("authorization") {
            Some(header) => header.to_string(),
            _ => return Outcome::Failure((Status::Unauthorized, TokenError::Missing)),
        };

        let (token_type, token_bytes) = match header.split_once(" ") {
            Some(value) => value,
            _ => return Outcome::Failure((Status::Unauthorized, TokenError::Missing)),
        };

        if token_type.to_lowercase() != "bearer" {
            return Outcome::Failure((Status::Unauthorized, TokenError::Missing));
        }

        let key_string = std::fs::read_to_string(config.public_key.as_ref().unwrap()).unwrap();

        let key = match ES256PublicKey::from_pem(&key_string) {
            Ok(key) => key,
            _ => return Outcome::Failure((Status::Unauthorized, TokenError::Missing)),
        };

        let mut options = VerificationOptions::default();
        // accept tokens even if they have expired up to 15 minutes after the deadline
        options.time_tolerance = Some(Duration::from_mins(15));
        // reject tokens if they were issued more than 1 hour ago
        options.max_validity = Some(Duration::from_hours(1));
        // reject tokens if they don't include an issuer from that list
        options.allowed_issuers = Some(HashSet::from_strings(&[config.issuer.clone().unwrap()]));

        let claims = match key.verify_token::<NoCustomClaims>(token_bytes, Some(options)) {
            Ok(claims) => claims,
            _ => return Outcome::Failure((Status::Unauthorized, TokenError::Missing)),
        };

        println!("{claims:?}");

        let subject = match claims.subject {
            Some(subject) => subject,
            _ => return Outcome::Failure((Status::Unauthorized, TokenError::Missing)),
        };

        Outcome::Success(Token {
            access: vec![],
            sub: subject,
            admin: false,
        })
    }
}
