use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{context::RequestContext, digest::Digest, error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct ManifestDeleteRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn delete(
    Path(ManifestDeleteRequest { repository, tag }): Path<ManifestDeleteRequest>,
    State(registry): State<Arc<RegistryState>>,
    context: RequestContext,
) -> Result<Response, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_push_challenge(&repository),
        });
    }

    if !context.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    if let Ok(digest) = Digest::try_from(tag.clone()) {
        if let Some(manifest) = registry.get_manifest(&digest).await? {
            if !manifest.repositories.contains(&repository) {
                return Err(RegistryError::ManifestNotFound {});
            }

            registry.unmount_manifest(&digest, &repository).await?;

            return Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())?);
        }

        return Err(RegistryError::ManifestNotFound {});
    }

    if let Some(_manifest) = registry.get_tag(&repository, &tag).await? {
        registry.delete_tag(&repository, &tag).await?;

        return Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())?);
    }

    Err(RegistryError::ManifestNotFound {})
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use reqwest::header::CONTENT_TYPE;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn delete_manifest_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("DELETE")
                    .uri("/v2/bar/manifests/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"http://localhost/auth/token\",service=\"http://localhost\",scope=\"repository:bar:pull,push\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn delete_manifests_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("DELETE")
                    .uri("/v2/bar/manifests/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn delete_tag() -> Result<()> {
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
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        let res = fixture
            .request(
                Request::builder()
                    .method("DELETE")
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        fixture.teardown().await
    }
}
