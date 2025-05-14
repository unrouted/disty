use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{digest::Digest, error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: String,
    digest: Digest,
}

pub(crate) async fn delete(
    Path(BlobRequest { repository, digest }): Path<BlobRequest>,
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

    if let Some(blob) = registry.get_blob(&digest).await? {
        if !blob.repositories.contains(&repository) {
            return Err(RegistryError::BlobNotFound {});
        }
    } else {
        return Err(RegistryError::BlobNotFound {});
    }

    registry.unmount_blob(&digest, &repository).await?;

    Ok(Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Body::empty())?)
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use axum::http::Request;
    use test_log::test;

    use crate::tests::RegistryFixture;

    use super::*;

    #[test(tokio::test)]
    pub async fn delete() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/bar/blobs/uploads/?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::from("FOOBAR"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let res = fixture.request(
            Request::builder()
                .method("DELETE")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        fixture.teardown().await
    }
}
