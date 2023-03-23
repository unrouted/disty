use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::types::Digest;
use crate::types::RepositoryName;
use actix_files::NamedFile;
use actix_web::head;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpRequest;
use actix_web::HttpResponseBuilder;
use actix_web::Responder;
use serde::Deserialize;
use tracing::debug;

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: RepositoryName,
    digest: Digest,
}

#[head("/{repository:[^{}]+}/blobs/{digest}")]
pub(crate) async fn head(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobRequest>,
    token: Token,
) -> Result<impl Responder, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        debug!("Token does not have access to perform this action");
        return Err(RegistryError::AccessDenied {});
    }

    let blob = match app.get_blob(&path.digest) {
        Some(blob) => blob,
        None => return Err(RegistryError::BlobNotFound {}),
    };

    if !blob.repositories.contains(&path.repository) {
        tracing::debug!("Blob exists but not in repostitory: {}", path.repository);
        return Err(RegistryError::BlobNotFound {});
    }

    if !blob.locations.contains(&app.config.identifier) {
        app.wait_for_blob(&path.digest).await;
    }

    let content_type = match blob.content_type {
        Some(content_type) => content_type,
        _ => "application/octet-steam".to_string(),
    };

    let _content_length = match blob.size {
        Some(content_length) => content_length,
        _ => {
            tracing::debug!("Blob was present but size not available");
            return Err(RegistryError::BlobNotFound {});
        }
    };

    let blob_path = app.get_blob_path(&path.digest);
    if !blob_path.is_file() {
        tracing::info!("Blob was not present on disk");
        return Err(RegistryError::BlobNotFound {});
    }

    let blob = NamedFile::open_async(blob_path)
        .await
        .unwrap()
        .into_response(&req);

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Docker-Content-Digest", path.digest.to_string()))
        .body(blob.into_body()))
}
