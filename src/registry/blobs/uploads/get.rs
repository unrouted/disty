use std::sync::Arc;

use crate::{context::RequestContext, error::RegistryError, state::RegistryState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

pub(crate) async fn get(
    Path(BlobUploadRequest {
        repository,
        upload_id,
    }): Path<BlobUploadRequest>,
    State(registry): State<Arc<RegistryState>>,
    context: RequestContext,
) -> Result<Response, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_push_challenge(&repository),
        });
    }

    if !context.has_permission(&repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    let filename = registry.upload_path(&upload_id);

    if !tokio::fs::try_exists(&filename).await? {
        return Err(RegistryError::UploadNotFound {});
    }

    let size = match tokio::fs::metadata(filename).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    let range_end = if size > 0 { size - 1 } else { 0 };

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(
            "Location",
            format!("/v2/{}/blobs/uploads/{}", repository, upload_id),
        )
        .header("Range", format!("0-{range_end}"))
        .header("Content-Length", "0")
        .header("Blob-Upload-Session-ID", &upload_id)
        .header("Docker-Upload-UUID", &upload_id)
        .body(Body::empty())
        .unwrap())
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;

    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn get_upload_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/blobs/uploads/foo")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"http://localhost\",service=\"http://localhost\",scope=\"repository:bar:pull,push\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get_uploads_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/blobs/uploads/foo")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }
}
