use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::{app::RegistryApp, types::RepositoryName};
use actix_web::http::StatusCode;
use actix_web::Responder;
use actix_web::{
    delete,
    web::{Data, Path},
    HttpResponseBuilder,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
    upload_id: String,
}

#[delete("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn delete(
    app: Data<RegistryApp>,
    path: Path<BlobUploadRequest>,
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

    if let Err(err) = tokio::fs::remove_file(filename).await {
        tracing::warn!("Error whilst deleting file: {err:?}");
        return Err(RegistryError::UploadInvalid {});
    }

    Ok(HttpResponseBuilder::new(StatusCode::NO_CONTENT).finish())
}
