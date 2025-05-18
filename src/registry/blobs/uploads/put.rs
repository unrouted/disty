use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{
    digest::Digest,
    error::RegistryError,
    registry::utils::{upload_part, validate_hash},
    state::RegistryState,
    token::Token,
};

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

#[derive(Debug, Deserialize)]
pub struct BlobUploadPutQuery {
    digest: Digest,
}

pub(crate) async fn put(
    Path(BlobUploadRequest {
        repository,
        upload_id,
    }): Path<BlobUploadRequest>,
    Query(BlobUploadPutQuery { digest }): Query<BlobUploadPutQuery>,
    State(registry): State<Arc<RegistryState>>,
    token: Token,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&repository),
        });
    }

    if !token.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    /*if query.digest.algo != "sha256" {
        return Err(RegistryError::UploadInvalid {});
    }*/

    let filename = registry.upload_path(&upload_id);

    if !tokio::fs::try_exists(&filename).await? {
        return Err(RegistryError::UploadInvalid {});
    }

    upload_part(&filename, body.into_body().into_data_stream()).await?;

    // Validate upload
    if !validate_hash(&filename, &digest).await {
        return Err(RegistryError::DigestInvalid {});
    }

    let dest = registry.get_blob_path(&digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    tokio::fs::rename(filename.clone(), dest.clone()).await?;

    registry
        .insert_blob(&digest, stat.len() as u32, "application/octet-stream")
        .await?;

    registry.mount_blob(&digest, &repository).await?;

    /*
    201 Created
    Location: <blob location>
    Content-Range: <start of range>-<end of range, inclusive>
    Content-Length: 0
    Docker-Content-Digest: <digest>
    */
    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("Location", format!("/v2/{}/blobs/{}", repository, digest))
        .header("Range", "0-0")
        .header("Content-Length", "0")
        .header("Docker-Content-Digest", digest.to_string())
        .body(Body::empty())?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use http_body_util::BodyExt;
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

        for chonk in ["FO", "OB", "AR"] {
            let res = fixture
                .request(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                        .body(Body::from(chonk))?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::ACCEPTED);
        }

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/bar/blobs/uploads/{upload_id}?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"))
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"FOOBAR");

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_multiple_kaniko() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture
            .request(
                Request::builder()
                    .method("POST")
                    .uri("/v2/foo/blobs/uploads/")
                    .header("Host", "10.192.170.146:9080")
                    .header(
                        "User-Agent",
                        "go-containerregistry/v0.8.1-0.20220507185902-82405e5dfa82",
                    )
                    .header("Content-Length", "0")
                    .header("Content-Type", "application/json")
                    .header("Accept-Encoding", "gzip")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let upload_id = res
            .headers()
            .get("Docker-Upload-UUID")
            .context("No Docker-Upload-UUID header")?
            .to_str()?;

        for chonk in ["FO", "OB", "AR"] {
            let res = fixture
                .request(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                        .body(Body::from(chonk))?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::ACCEPTED);
        }

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/bar/blobs/uploads/{upload_id}?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"))
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"FOOBAR");

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_multiple_finish_with_put() -> Result<()> {
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

        for chonk in ["FO", "OB"] {
            let res = fixture
                .request(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                        .body(Body::from(chonk))?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::ACCEPTED);
        }

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/bar/blobs/uploads/{upload_id}?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"))
                .body(Body::from("AR"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"FOOBAR");

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_multiple_finish_with_put_bad_digest() -> Result<()> {
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

        for chonk in ["FO", "OB", "OB"] {
            let res = fixture
                .request(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                        .body(Body::from(chonk))?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::ACCEPTED);
        }

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/bar/blobs/uploads/{upload_id}?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"))
                .body(Body::from("AR"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_multiple_bad_digest() -> Result<()> {
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

        for chonk in ["FO", "OB", "OB", "AR"] {
            let res = fixture
                .request(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v2/bar/blobs/uploads/{upload_id}"))
                        .body(Body::from(chonk))?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::ACCEPTED);
        }

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/bar/blobs/uploads/{upload_id}?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"))
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        fixture.teardown().await
    }
}
