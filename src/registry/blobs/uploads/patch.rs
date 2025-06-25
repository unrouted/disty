use std::sync::Arc;

use anyhow::Context;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum_extra::TypedHeader;
use serde::Deserialize;

use crate::context::RequestContext;
use crate::error::RegistryError;
use crate::registry::content_range::ContentRange;
use crate::registry::utils::upload_part;
use crate::state::RegistryState;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

pub(crate) async fn patch(
    Path(BlobUploadRequest {
        repository,
        upload_id,
    }): Path<BlobUploadRequest>,
    State(registry): State<Arc<RegistryState>>,
    context: RequestContext,
    content_range: Option<TypedHeader<ContentRange>>,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_push_challenge(&repository),
        });
    }

    if !context.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    let filename = registry.upload_path(&upload_id);

    let parent = filename
        .parent()
        .context("Couldn't find parent directory")?;
    tokio::fs::create_dir_all(parent).await?;

    if !tokio::fs::try_exists(&filename).await? {
        return Err(RegistryError::UploadInvalid {});
    }

    if let Some(content_range) = content_range {
        let start = content_range.0.first_byte;
        let stop = content_range.0.last_byte;

        let size = match tokio::fs::metadata(&filename).await {
            Ok(value) => value.len(),
            _ => 0,
        };

        if stop < start {
            return Err(RegistryError::RangeNotSatisfiable {
                repository: repository.clone(),
                upload_id: upload_id.clone(),
                size,
            });
        }

        if start != size {
            return Err(RegistryError::RangeNotSatisfiable {
                repository: repository.clone(),
                upload_id: upload_id.clone(),
                size,
            });
        }
    }

    upload_part(&filename, body.into_body().into_data_stream()).await?;

    let size = match tokio::fs::metadata(filename).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    /*
    204 No Content
    Location: /v2/<name>/blobs/uploads/<uuid>
    Range: 0-<offset>
    Content-Length: 0
    Docker-Upload-UUID: <uuid>
    */

    let range_end = if size > 0 { size - 1 } else { 0 };

    Ok(Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(
            "Location",
            format!("/v2/{}/blobs/uploads/{}", repository, upload_id),
        )
        .header("Range", format!("0-{range_end}"))
        .header("Content-Length", "0")
        .header("Blob-Upload-Session-ID", &upload_id)
        .header("Docker-Upload-UUID", &upload_id)
        .body(Body::empty())?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;

    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn patch_upload_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("PATCH")
                    .uri("/v2/bar/blobs/uploads/foo")
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
    pub async fn patch_uploads_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("PATCH")
                    .uri("/v2/bar/blobs/uploads/foo")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }
}
