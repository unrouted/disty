use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::types::RepositoryName;
use actix_web::http::StatusCode;
use actix_web::{
    get,
    web::{Data, Path},
    HttpResponse, HttpResponseBuilder,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
    upload_id: String,
}

#[get("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn get(
    app: Data<RegistryApp>,
    path: Path<BlobUploadRequest>,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    let filename = app.get_upload_path(&path.upload_id);
    if !filename.is_file() {
        return Err(RegistryError::UploadNotFound {});
    }

    let size = match tokio::fs::metadata(filename).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    let range_end = size - 1;

    Ok(HttpResponseBuilder::new(StatusCode::NO_CONTENT)
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
