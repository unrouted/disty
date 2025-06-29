use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    response::Response,
};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::{context::RequestContext, error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct TagList {
    repository: String,
}

#[derive(Debug, Deserialize)]
pub struct TagQuery {
    last: Option<String>,
    n: Option<usize>,
}

pub(crate) async fn get(
    Path(TagList { repository }): Path<TagList>,
    Query(TagQuery { last, n }): Query<TagQuery>,
    State(registry): State<Arc<RegistryState>>,
    context: RequestContext,
) -> Result<Response, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_pull_challenge(&repository),
        });
    }

    if !context.has_permission(&repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    if !registry.repository_exists(&repository).await? {
        return Err(RegistryError::RepositoryNotFound {});
    }

    let mut tags = registry.get_tags(&repository).await?;

    if let Some(last) = &last {
        let index = tags.iter().position(|r| r == last).unwrap();
        tags = tags[index..].to_vec();
    }

    let mut include_link = false;

    if let Some(n) = n {
        if n < tags.len() {
            include_link = true;
        }
        tags = tags[..n].to_vec();
    }

    let body = json!(
        {
            "name": repository.clone(),
            "tags": tags,
        }
    )
    .to_string();

    let builder = Response::builder().status(StatusCode::OK);

    let builder = if include_link {
        let mut fragments = vec![];

        if let Some(tag) = tags.last() {
            fragments.push(format!("last={tag}"))
        }

        if let Some(n) = n {
            fragments.push(format!("n={n}"))
        }

        let suffix = if !fragments.is_empty() {
            let joined = fragments.join("&");
            format!("?{joined}")
        } else {
            "".to_string()
        };

        builder.header(
            "Link",
            format!("/v2/{}/tags/list{}; rel=\"next\"", repository, suffix),
        )
    } else {
        builder
    };

    Ok(builder.body(Body::from(body))?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use http_body_util::BodyExt;
    use reqwest::header::CONTENT_TYPE;
    use serde_json::Value;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn get_tags_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/tags/list")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"http://localhost/auth/token\",service=\"http://localhost\",scope=\"repository:bar:pull\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get_tags_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/tags/list")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get_tags() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let payload = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        let res = fixture
            .request(
                Request::builder()
                    .method("PUT")
                    .header(
                        CONTENT_TYPE,
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                    )
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::from(payload.to_string()))?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/foo/tags/list")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&body)?;

        assert_eq!(value, json!({"name": "foo", "tags": ["latest"]}));

        fixture.teardown().await
    }
}
