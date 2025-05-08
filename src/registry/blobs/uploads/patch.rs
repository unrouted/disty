use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum_extra::TypedHeader;
use headers::ContentRange;
use serde::Deserialize;

use crate::error::RegistryError;
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
    content_range: Option<TypedHeader<ContentRange>>,
    body: Request<Body>,
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

    if let Some(content_range) = content_range {
        if let Some((start, stop)) = content_range.bytes_range() {
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
