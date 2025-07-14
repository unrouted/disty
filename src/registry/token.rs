use std::{
    collections::{BTreeMap, HashSet},
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
use serde_json::Value;

use crate::{
    config::{
        Configuration,
        acl::{AclCheck, Action, ResourceContext, SubjectContext},
    },
    context::Access,
    error::RegistryError,
    issuer::issue_token,
    state::RegistryState,
};

#[derive(Debug, Deserialize)]
pub(crate) struct TokenRequest {
    service: String,
    scope: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenResponse {
    token: String,
    expires_in: u64,
    issued_at: String,
}

pub async fn authenticate(
    config: &Configuration,
    req_username: &str,
    req_password: &str,
) -> Result<Option<(String, Option<String>, Value)>> {
    let Some(authentication) = config.authentication.as_ref() else {
        return Ok(None);
    };

    for user in &authentication.users {
        match user {
            crate::config::User::Password { username, password } => {
                if username != req_username {
                    continue;
                }

                if pwhash::unix::verify(req_password, password) {
                    let subject = format!("internal:basic:{username}");
                    return Ok(Some((subject, None, Value::Null)));
                }
            }
            crate::config::User::Token { username, issuer } => {
                if username != req_username {
                    continue;
                }

                return Ok(match issuer.verify::<Value>(config, req_password).await? {
                    Some(claims) => {
                        let subject = format!(
                            "internal:token:{username}:subject:{}",
                            claims.subject.clone().unwrap_or("".to_string())
                        );
                        Some((subject, claims.subject, claims.custom))
                    }
                    None => None,
                });
            }
        }
    }
    Ok(None)
}

pub(crate) async fn token(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(TokenRequest { service, scope }): Query<TokenRequest>,
    authorization: Option<TypedHeader<Authorization<Basic>>>,
    State(registry): State<Arc<RegistryState>>,
) -> Result<Response, RegistryError> {
    if service != registry.config.url {
        return Ok((StatusCode::UNAUTHORIZED, "Invalid service").into_response());
    }

    let token = match authorization {
        Some(authorization) => {
            let Some((token_subject, subject, claims)) = authenticate(
                &registry.config,
                authorization.username(),
                authorization.password(),
            )
            .await?
            else {
                return Ok((StatusCode::UNAUTHORIZED, "Invalid credentials").into_response());
            };

            let subject = SubjectContext {
                username: authorization.username().to_string(),
                subject: subject,
                claims: claims.clone(),
                ip: addr.ip(),
            };

            let mut access_map: BTreeMap<String, HashSet<Action>> = BTreeMap::new();

            if let Some(authentication) = registry.config.authentication.as_ref() {
                if let Some(scope) = scope {
                    for scope in &scope {
                        for scope in scope.split(" ") {
                            let parts: Vec<&str> = scope.split(':').collect();
                            if parts.len() == 3 && parts[0] == "repository" {
                                let repo = parts[1];
                                let actions: Vec<_> = parts[2]
                                    .split(",")
                                    .map(|split| Action::try_from(split.to_string()).unwrap())
                                    .collect();

                                let allowed_actions = authentication.acls.check_access(
                                    &subject,
                                    &ResourceContext {
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
                    }
                }
            }
            let access_entries = access_map
                .into_iter()
                .map(|(repo, actions)| {
                    let mut actions: Vec<_> = actions.into_iter().collect();
                    actions.sort();
                    Access {
                        type_: "repository".to_string(),
                        name: repo,
                        actions,
                    }
                })
                .collect();

            issue_token(&registry.config, &token_subject, access_entries)?
        }
        None => issue_token(&registry.config, "internal:anonymous", vec![])?,
    };

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
            acl::{AccessRule, SubjectMatch},
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
    use std::{collections::HashMap, net::SocketAddr};
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
                    .uri("/auth/token?service=http://localhost&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(claims.custom, json!({"access": []}));

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn user_authentication_and_authorization_multiple_scopes() -> Result<()> {
        let fixture = RegistryFixture::with_state(
            FixtureBuilder::new()
                .authenticated(true)
                .user(User::Password {
                    username: "username".into(),
                    password: "$6$TVFDl34in89H8PM.$vJ2jhuC0Ijgr9c5.uijvYp31g0K4x2jl6FDpdfw40CVdFjzyO7pJpGLkVIAGtwsbbS1RcWgJ0VNSR83Uf.T..1".into(),
                })
                .acl(AccessRule { actions: [Action::Pull, Action::Push].into_iter().collect(), ..Default::default()})
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
                    .uri("/auth/token?scope=repository%3Aconformance-25436bad-9cf6-4010-ad97-d1a033872a18%3Apull%2Cpush+repository%3Amytestorg%2Fmytestrepo%3Apull&service=http://localhost")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(
            claims.custom,
            json!({"access": [
                Access { type_: "repository".into(), name: "conformance-25436bad-9cf6-4010-ad97-d1a033872a18".into(), actions: [Action::Pull, Action::Push].into_iter().collect() },
                Access { type_: "repository".into(), name: "mytestorg/mytestrepo".into(), actions: [Action::Pull].into_iter().collect() },
            ]})
        );

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
                        username: Some(serde_json::from_value(json!("username")).unwrap()),
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
                    .uri("/auth/token?service=http://localhost&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

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
                        username: Some(serde_json::from_value(json!("bob")).unwrap()),
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
                    .uri("/auth/token?service=http://localhost&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

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
            .with_audience("http://localhost")
            .with_subject("project_path:my-group/my-project:ref_type:branch:ref:feature-branch-1");

        let token = key.sign(claims)?;

        let issuer = JWKSPublicKey::new("http://localhost", "gitlab.example.com")
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
                    .uri("/auth/token?service=http://localhost&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

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
            .with_audience("http://localhost")
            .with_subject("project_path:my-group/my-project:ref_type:branch:ref:feature-branch-1");

        let token = key.sign(claims)?;

        let issuer = JWKSPublicKey::new("http://localhost", "gitlab.example.com")
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
                        claims: Some(serde_json::from_value(json!({
                            "pointer": "/project_path",
                            "match": "my-group/my-project",
                        }))?),
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
                    .uri("/auth/token?service=http://localhost&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

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
            .with_audience("http://localhost")
            .with_subject("project_path:my-group/my-project:ref_type:branch:ref:feature-branch-1");

        let token = key.sign(claims)?;

        let issuer = JWKSPublicKey::new("http://localhost", "gitlab.example.com")
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
                        claims: Some(serde_json::from_value(json!({
                            "pointer": "/project_path",
                            "match": "my-group/bobs-project",
                        }))?),
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
                    .uri("/auth/token?service=http://localhost&scope=repository%3Abadger%3Apull%2Cpush")
                    .header("Authorization", value)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: TokenResponse = serde_json::from_slice(&body)?;

        let issuer = "http://localhost".to_string();

        let options = VerificationOptions {
            // accept tokens even if they have expired up to 15 minutes after the deadline
            time_tolerance: Some(Duration::from_mins(15)),
            // reject tokens if they were issued more than 1 hour ago
            max_validity: Some(Duration::from_hours(1)),
            // reject tokens if they don't include an issuer from that list
            allowed_issuers: Some(HashSet::from_strings(&[issuer.clone()])),
            // validate it is a token for us
            allowed_audiences: Some(HashSet::from_strings(&[issuer.clone()])),
            ..Default::default()
        };

        let issuer = fixture.state.config.authentication.as_ref().unwrap();

        let claims: JWTClaims<Value> = issuer
            .key_pair
            .key_pair
            .public_key()
            .verify_token(&value.token, Some(options))?;

        assert_json_eq!(claims.custom, json!({"access": []}));

        fixture.teardown().await
    }
}
