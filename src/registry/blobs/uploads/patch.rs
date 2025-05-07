use std::sync::Arc;

use crate::error::RegistryError;
use crate::state::RegistryState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

fn get_http_range(req: &HttpRequest) -> Option<(u64, u64)> {
    let token = req.headers().get("content-range");
    match token {
        Some(token) => match token.to_str().unwrap().split_once('-') {
            Some((start, stop)) => {
                let start: u64 = match start.parse() {
                    Ok(value) => value,
                    _ => return None,
                };
                let stop: u64 = match stop.parse() {
                    Ok(value) => value,
                    _ => return None,
                };
                Some((start, stop))
            }
            _ => None,
        },
        None => None,
    }
}

pub(crate) async fn patch(
    Path(BlobUploadRequest { repository, upload_id }): Path<BlobUploadRequest>,
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

    if let Some((start, stop)) = get_http_range(&req) {
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

    if !upload_part(&filename, body).await {
        return Err(RegistryError::UploadInvalid {});
    }

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

    Ok(Response::builder().status(StatusCode::ACCEPTED)
        .header(
            "Location",
            format!("/v2/{}/blobs/uploads/{}", repository, upload_id),
        )
        .header("Range", format!("0-{range_end}"))
        .header("Content-Length", "0")
        .header("Blob-Upload-Session-ID", &upload_id)
        .header("Docker-Upload-UUID", &upload_id)
        .unwrap())
}