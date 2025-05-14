use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

pub(crate) async fn delete(
    Path(BlobUploadRequest {
        repository: _,
        upload_id,
    }): Path<BlobUploadRequest>,
    State(registry): State<Arc<RegistryState>>,
) -> Result<Response, RegistryError> {
    /*if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }*/

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

    use crate::tests::RegistryFixture;

    use super::*;

    #[test(tokio::test)]
    pub async fn upload_multiple() -> Result<()> {
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
