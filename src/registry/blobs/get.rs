use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::Response,
};
use serde::Deserialize;
use tokio_util::io::ReaderStream;
use tracing::debug;

use crate::{context::RequestContext, digest::Digest, error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: String,
    digest: Digest,
}

pub(crate) async fn get(
    Path(BlobRequest { repository, digest }): Path<BlobRequest>,
    State(registry): State<Arc<RegistryState>>,
    context: RequestContext,
) -> Result<Response, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_pull_challenge(&repository),
        });
    }

    if !context.has_permission(&repository, "pull") {
        debug!("Token does not have access to perform this action");
        return Err(RegistryError::AccessDenied {});
    }

    if !registry.repository_exists(&repository).await? {
        return Err(RegistryError::RepositoryNotFound {});
    }

    let blob = match registry.get_blob(&digest).await? {
        Some(blob) => blob,
        None => return Err(RegistryError::BlobNotFound {}),
    };

    if !blob.repositories.contains(&repository) {
        tracing::debug!("Blob exists but not in repostitory: {}", repository);
        return Err(RegistryError::BlobNotFound {});
    }

    /*if !blob.locations.contains(&app.id) {
        app.wait_for_blob(&path.digest).await;
    }*/

    let blob_path = registry.get_blob_path(&digest);
    if !blob_path.is_file() {
        tracing::info!("Blob was not present on disk");
        return Err(RegistryError::BlobNotFound {});
    }

    let blob_file = tokio::fs::File::open(blob_path).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Content-Digest", digest.to_string())
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, blob.size)
        .body(Body::from_stream(ReaderStream::new(blob_file)))?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn get_blob_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"http://localhost\",service=\"http://localhost\",scope=\"repository:bar:pull\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get_blobs_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }
}
