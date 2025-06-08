use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::acl::Action;
use crate::digest::Digest;
use crate::error::RegistryError;
use crate::registry::utils::{upload_part, validate_hash};
use crate::state::RegistryState;
use crate::token::{Access, Token};
#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
}
#[derive(Debug, Deserialize)]
pub struct BlobUploadPostQuery {
    mount: Option<Digest>,
    from: Option<String>,
    digest: Option<Digest>,
}

pub(crate) async fn post(
    Path(BlobUploadRequest { repository }): Path<BlobUploadRequest>,
    Query(BlobUploadPostQuery {
        mount,
        from,
        digest,
    }): Query<BlobUploadPostQuery>,
    State(registry): State<Arc<RegistryState>>,
    token: Token,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    if !token.validated_token {
        let mut access = vec![Access {
            type_: "repository".to_string(),
            name: repository.clone(),
            actions: HashSet::from([Action::Pull, Action::Push]),
        }];

        if let Some(from) = &from {
            access.push(Access {
                type_: "repository".to_string(),
                name: from.clone(),
                actions: HashSet::from([Action::Pull]),
            });
        }

        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_challenge(access),
        });
    }

    if !token.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    if let (Some(mount), Some(from)) = (mount, &from) {
        if from == &repository {
            return Err(RegistryError::UploadInvalid {});
        }

        if !token.has_permission(from, "pull") {
            return Err(RegistryError::UploadInvalid {});
        }

        if let Some(blob) = registry.get_blob(&mount).await? {
            if blob.repositories.contains(from) {
                registry.mount_blob(&mount, &repository).await?;

                /*
                201 Created
                Location: <blob location>
                Content-Range: <start of range>-<end of range, inclusive>
                Content-Length: 0
                Docker-Content-Digest: <digest>
                */
                return Ok(Response::builder()
                    .status(StatusCode::CREATED)
                    .header("Location", format!("/v2/{}/blobs/{}", repository, mount))
                    .header("Range", "0-0")
                    .header("Content-Length", "0")
                    .header("Docker-Content-Digest", mount.to_string())
                    .body(Body::empty())?);
            }
        }
    }
    let upload_id = Uuid::new_v4().as_hyphenated().to_string();

    match &digest {
        Some(digest) => {
            let filename = registry.upload_path(&upload_id);

            let parent = filename
                .parent()
                .context("Couldn't find parent directory")?;
            tokio::fs::create_dir_all(parent).await?;

            upload_part(&filename, body.into_body().into_data_stream())
                .await
                .context("Unable to upload part")?;

            // Validate upload
            if !validate_hash(&filename, digest).await {
                info!("Upload rejecte due to invalid hash");
                return Err(RegistryError::DigestInvalid {});
            }

            let dest = registry.get_blob_path(digest);

            let stat = match tokio::fs::metadata(&filename).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(RegistryError::UploadInvalid {});
                }
            };

            let parent = dest.parent().context("Couldn't find parent directory")?;
            tokio::fs::create_dir_all(parent).await?;

            match tokio::fs::rename(&filename, &dest).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to rename file {filename:?} to {dest:?} ({e:?}");
                    return Err(RegistryError::UploadInvalid {});
                }
            }

            registry
                .insert_blob(&repository, digest, stat.len() as u32, &token.sub)
                .await?;

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
        _ => {
            // Nothing was uploaded, but a session was started...
            let filename = registry.upload_path(&upload_id);

            let parent = filename
                .parent()
                .context("Couldn't find parent directory")?;
            tokio::fs::create_dir_all(parent).await?;

            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)
                .await
            {
                Ok(file) => drop(file),
                _ => return Err(RegistryError::UploadInvalid {}),
            }

            Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .header(
                    "Location",
                    format!("/v2/{}/blobs/uploads/{}", repository, upload_id),
                )
                .header("Range", format!("0-{}", 0))
                .header("Content-Length", "0")
                .header("Docker-Upload-UUID", upload_id)
                .body(Body::empty())?)
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn post_upload_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("POST")
                    .uri("/v2/bar/blobs/uploads/")
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
    pub async fn upload_whole_blob() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::from("FOOBAR"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        // FIXME: Test returned headers are correct

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"FOOBAR");

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_whole_blob_invalid_digest() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::from("FOOBARR"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn cross_mount() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::from("FOOBAR"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/bar/blobs/uploads/?from=foo&mount=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        // Test old location still accessible
        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        // Test new location still accessible
        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"FOOBAR");

        // Test not accessible elsewhere
        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/baz/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                .body(Body::empty())?
            ).await?;

        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        fixture.teardown().await
    }
}
