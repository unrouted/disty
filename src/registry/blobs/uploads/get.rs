use std::sync::Arc;

use crate::{error::RegistryError, state::RegistryState};
use axum::{body::Body, extract::{Path, State}, http::StatusCode, response::Response};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}


pub(crate) async fn get(
    Path(BlobUploadRequest { repository, upload_id }): Path<BlobUploadRequest>,
    State(registry): State<Arc<RegistryState>>,
) -> Result<Response, RegistryError> {
    /*if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }*/

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

    let range_end = size - 1;

    Ok(Response::builder().status(StatusCode::NO_CONTENT)
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