use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use anyhow::Result;
use axum::{
    Json,
    extract::{ConnectInfo, State},
    response::{IntoResponse, Response},
};
use axum_extra::{TypedHeader, extract::Query};
use headers::{Authorization, authorization::Basic};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    config::{
        AuthenticationConfig,
        acl::{AclCheck, Action, RepositoryContext, SubjectContext},
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenResponse {
    token: String,
    expires_in: u64,
    issued_at: String,
}

pub async fn authenticate(
    issuer: &AuthenticationConfig,
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
                println!("a");
                if username != req_username {
                    continue;
                }

                println!("b");
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
    Query(TokenRequest { service, scope }): Query<TokenRequest>,
    authorization: TypedHeader<Authorization<Basic>>,
    State(registry): State<Arc<RegistryState>>,
) -> Result<Response, RegistryError> {
    let issuer = registry.config.authentication.as_ref().unwrap();

    let Some(claims) =
        authenticate(issuer, authorization.username(), authorization.password()).await?
    else {
        println!("D");
        return Ok((StatusCode::UNAUTHORIZED, "Invalid credentials").into_response());
    };

    let subject = SubjectContext {
        username: authorization.username().to_string(),
        claims: claims.clone(),
        ip: addr.ip(),
    };

    let mut access_map: HashMap<String, HashSet<Action>> = HashMap::new();

    for scope in &scope {
        let parts: Vec<&str> = scope.split(':').collect();
        if parts.len() == 3 && parts[0] == "repository" {
            let repo = parts[1];
            let actions: Vec<_> = parts[2]
                .split(",")
                .map(|split| Action::try_from(split.to_string()).unwrap())
                .collect();

            let allowed_actions = issuer.acls.check_access(
                &subject,
                &RepositoryContext {
                    repository: repo.to_string(),
                },
            );
            for action in actions {
                if allowed_actions.contains(&action) {
                    access_map
                        .entry(repo.to_string())
                        .or_default()
                        .insert(action);
                }
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

    let token = issue_token(issuer, access_entries)?;

    Ok(Json(TokenResponse {
        token: token.token,
        expires_in: token.expires_in.as_secs(),
        issued_at: token
            .issued_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    })
    .into_response())
}

#[cfg(test)]
mod test {
    use crate::{
        config::{
            User,
            acl::{AccessRule, StringMatch, SubjectMatch},
        },
        jwt::JWKSPublicKey,
        tests::{FixtureBuilder, RegistryFixture},
    };
    use anyhow::Result;
    use assert_json_diff::assert_json_eq;
    use axum::{body::Body, http::Request};
    use base64::engine::{Engine, general_purpose::STANDARD};
    use http_body_util::BodyExt;
    use jwt_simple::prelude::*;
    use serde_json::Value;
    use serde_json::json;
    use std::net::SocketAddr;
    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization() -> Result<()> {
        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Password {
                    username: "username".into(),
                    password: "$6$TVFDl34in89H8PM.$vJ2jhuC0Ijgr9c5.uijvYp31g0K4x2jl6FDpdfw40CVdFjzyO7pJpGLkVIAGtwsbbS1RcWgJ0VNSR83Uf.T..1".into(),
                })
                .build()
                .await?,
        )?;

        let credentials = format!("{}:{}", "username", "password");
        let encoded = STANDARD.encode(credentials);
        let value = format!("Basic {}", encoded);

        let res = fixture
            .request(
                Request::builder()
                    .extension(ConnectInfo::<SocketAddr>("127.0.0.1:8123".parse()?))
                    .method("GET")
                    .uri("/auth/token?service=foo&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.audience.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(claims.custom, json!({"access": []}));

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization_with_matching_acl() -> Result<()> {
        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Password {
                    username: "username".into(),
                    password: "$6$TVFDl34in89H8PM.$vJ2jhuC0Ijgr9c5.uijvYp31g0K4x2jl6FDpdfw40CVdFjzyO7pJpGLkVIAGtwsbbS1RcWgJ0VNSR83Uf.T..1".into(),
                })
                .acl(AccessRule {
                    subject: Some(SubjectMatch {
                        username: Some(StringMatch::Exact("username".into())),
                        ..Default::default()
                    }),
                    actions: [Action::Pull].into_iter().collect(),
                    ..Default::default()
                })
                .build()
                .await?,
        )?;

        let credentials = format!("{}:{}", "username", "password");
        let encoded = STANDARD.encode(credentials);
        let value = format!("Basic {}", encoded);

        let res = fixture
            .request(
                Request::builder()
                    .extension(ConnectInfo::<SocketAddr>("127.0.0.1:8123".parse()?))
                    .method("GET")
                    .uri("/auth/token?service=foo&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.audience.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(
            claims.custom,
            json!({"access": [{
                "name": "badger",
                "type": "repository",
                "actions": ["pull"],
            }]})
        );

        fixture.teardown().await
    }
    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization_with_non_matching_acl() -> Result<()> {
        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Password {
                    username: "username".into(),
                    password: "$6$TVFDl34in89H8PM.$vJ2jhuC0Ijgr9c5.uijvYp31g0K4x2jl6FDpdfw40CVdFjzyO7pJpGLkVIAGtwsbbS1RcWgJ0VNSR83Uf.T..1".into(),
                })
                .acl(AccessRule {
                    subject: Some(SubjectMatch {
                        username: Some(StringMatch::Exact("bob".into())),
                        ..Default::default()
                    }),
                    actions: [Action::Pull].into_iter().collect(),
                    ..Default::default()
                })
                .build()
                .await?,
        )?;

        let credentials = format!("{}:{}", "username", "password");
        let encoded = STANDARD.encode(credentials);
        let value = format!("Basic {}", encoded);

        let res = fixture
            .request(
                Request::builder()
                    .extension(ConnectInfo::<SocketAddr>("127.0.0.1:8123".parse()?))
                    .method("GET")
                    .uri("/auth/token?service=foo&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.audience.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(claims.custom, json!({"access": []}));

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization_with_idtoken() -> Result<()> {
        // Build an IDToken a bit like what GitLab generates
        let key: RS256KeyPair = RS256KeyPair::generate(2048)?.with_key_id("bob");
        let custom_claims = [
            (
                "project_path".to_string(),
                "my-group/my-project".to_string(),
            ),
            ("ref".to_string(), "feature-branch-1".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_mins(10))
            .with_issuer("gitlab.example.com")
            .with_audience("localhost")
            .with_subject("project_path:my-group/my-project:ref_type:branch:ref:feature-branch-1");

        let token = key.sign(claims)?;

        let issuer = JWKSPublicKey::new("http://localhost", "gitlab.example.com", "localhost")
            .with_cache(key.public_key())
            .await;

        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Token {
                    username: "gitlab".into(),
                    issuer,
                })
                .build()
                .await?,
        )?;

        let credentials = format!("{}:{}", "gitlab", token);
        let encoded = STANDARD.encode(credentials);
        let value = format!("Basic {}", encoded);

        let res = fixture
            .request(
                Request::builder()
                    .extension(ConnectInfo::<SocketAddr>("127.0.0.1:8123".parse()?))
                    .method("GET")
                    .uri("/auth/token?service=foo&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.audience.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(claims.custom, json!({"access": []}));

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization_with_idtoken_matching_acl() -> Result<()> {
        // Build an IDToken a bit like what GitLab generates
        let key: RS256KeyPair = RS256KeyPair::generate(2048)?.with_key_id("bob");
        let custom_claims = [
            (
                "project_path".to_string(),
                "my-group/my-project".to_string(),
            ),
            ("ref".to_string(), "feature-branch-1".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_mins(10))
            .with_issuer("gitlab.example.com")
            .with_audience("localhost")
            .with_subject("project_path:my-group/my-project:ref_type:branch:ref:feature-branch-1");

        let token = key.sign(claims)?;

        let issuer = JWKSPublicKey::new("http://localhost", "gitlab.example.com", "localhost")
            .with_cache(key.public_key())
            .await;

        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Token {
                    username: "gitlab".into(),
                    issuer,
                })
                .acl(AccessRule {
                    subject: Some(SubjectMatch {
                        claims: Some(
                            [(
                                "project_path".to_string(),
                                StringMatch::Exact("my-group/my-project".into()),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..Default::default()
                    }),
                    actions: [Action::Pull].into_iter().collect(),
                    ..Default::default()
                })
                .build()
                .await?,
        )?;

        let credentials = format!("{}:{}", "gitlab", token);
        let encoded = STANDARD.encode(credentials);
        let value = format!("Basic {}", encoded);

        let res = fixture
            .request(
                Request::builder()
                    .extension(ConnectInfo::<SocketAddr>("127.0.0.1:8123".parse()?))
                    .method("GET")
                    .uri("/auth/token?service=foo&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.audience.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(
            claims.custom,
            json!({"access": [{
                "name": "badger",
                "type": "repository",
                "actions": ["pull"],
            }]})
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization_with_idtoken_non_matching_acl() -> Result<()>
    {
        // Build an IDToken a bit like what GitLab generates
        let key: RS256KeyPair = RS256KeyPair::generate(2048)?.with_key_id("bob");
        let custom_claims = [
            (
                "project_path".to_string(),
                "my-group/my-project".to_string(),
            ),
            ("ref".to_string(), "feature-branch-1".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_mins(10))
            .with_issuer("gitlab.example.com")
            .with_audience("localhost")
            .with_subject("project_path:my-group/my-project:ref_type:branch:ref:feature-branch-1");

        let token = key.sign(claims)?;

        let issuer = JWKSPublicKey::new("http://localhost", "gitlab.example.com", "localhost")
            .with_cache(key.public_key())
            .await;

        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Token {
                    username: "gitlab".into(),
                    issuer,
                })
                .acl(AccessRule {
                    subject: Some(SubjectMatch {
                        claims: Some(
                            [(
                                "project_path".to_string(),
                                StringMatch::Exact("my-group/bobs-project".into()),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                        ..Default::default()
                    }),
                    actions: [Action::Pull].into_iter().collect(),
                    ..Default::default()
                })
                .build()
                .await?,
        )?;

        let credentials = format!("{}:{}", "gitlab", token);
        let encoded = STANDARD.encode(credentials);
        let value = format!("Basic {}", encoded);

        let res = fixture
            .request(
                Request::builder()
                    .extension(ConnectInfo::<SocketAddr>("127.0.0.1:8123".parse()?))
                    .method("GET")
                    .uri("/auth/token?service=foo&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.audience.clone()])),
            ..Default::default()
        };

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(claims.custom, json!({"access": []}));

        fixture.teardown().await
    }
}
