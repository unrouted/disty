use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::registry::utils::upload_part;
use crate::types::RepositoryName;
use actix_web::http::StatusCode;
use actix_web::web::Payload;
use actix_web::{
    patch,
    web::{Data, Path},
    HttpResponseBuilder,
};
use actix_web::{HttpRequest, Responder};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
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

#[patch("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn patch(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobUploadRequest>,
    body: Payload,
    token: Token,
) -> Result<impl Responder, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    let filename = app.get_upload_path(&path.upload_id);

    if !filename.is_file() {
        return Err(RegistryError::UploadInvalid {});
    }

    if let Some((start, stop)) = get_http_range(&req) {
        let size = match tokio::fs::metadata(&filename).await {
            Ok(value) => value.len(),
            _ => 0,
        };

        if stop < start {
            return Err(RegistryError::RangeNotSatisfiable {
                repository: path.repository.clone(),
                upload_id: path.upload_id.clone(),
                size,
            });
        }

        if start != size {
            return Err(RegistryError::RangeNotSatisfiable {
                repository: path.repository.clone(),
                upload_id: path.upload_id.clone(),
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

    let range_end = size - 1;

    Ok(HttpResponseBuilder::new(StatusCode::ACCEPTED)
        .append_header((
            "Location",
            format!("/v2/{}/blobs/uploads/{}", path.repository, path.upload_id),
        ))
        .append_header(("Range", format!("0-{range_end}")))
        .append_header(("Content-Length", "0"))
        .append_header(("Blob-Upload-Session-ID", path.upload_id.clone()))
        .append_header(("Docker-Upload-UUID", path.upload_id.clone()))
        .finish())
}
