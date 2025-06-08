use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{error::RegistryError, state::RegistryState, token::Token};

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

pub(crate) async fn delete(
    Path(BlobUploadRequest {
        repository,
        upload_id,
    }): Path<BlobUploadRequest>,
    State(registry): State<Arc<RegistryState>>,
    token: Token,
) -> Result<Response, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&repository),
        });
    }

    if !token.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    let filename = registry.upload_path(&upload_id);

    if !tokio::fs::try_exists(&filename).await? {
        return Err(RegistryError::UploadInvalid {});
    }

    if let Err(err) = tokio::fs::remove_file(&filename).await {
        tracing::warn!("Error whilst deleting file: {err:?}");
        return Err(RegistryError::UploadInvalid {});
    }

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
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
    pub async fn delete_upload_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("DELETE")
                    .uri("/v2/bar/blobs/uploads/foo")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"fixme\",service=\"some-audience\",scope=\"repository:bar:pull,push\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn delete_upload() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture
            .request(
                Request::builder()
                    .method("POST")
                    .uri("/v2/foo/blobs/uploads/")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let upload_id = res
            .headers()
            .get("Docker-Upload-UUID")
            .context("No Docker-Upload-UUID header")?
            .to_str()?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let res = fixture
            .request(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        fixture.teardown().await
    }
}
