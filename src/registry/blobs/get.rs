use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::Response,
};
use serde::Deserialize;
use tokio_util::io::ReaderStream;
use tracing::debug;

use crate::{digest::Digest, error::RegistryError, state::RegistryState, token::Token};

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: String,
    digest: Digest,
}

pub(crate) async fn get(
    Path(BlobRequest { repository, digest }): Path<BlobRequest>,
    State(registry): State<Arc<RegistryState>>,
    token: Token,
) -> Result<Response, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&repository),
        });
    }

    if !token.has_permission(&repository, "pull") {
        debug!("Token does not have access to perform this action");
        return Err(RegistryError::AccessDenied {});
    }

    if !registry.repository_exists(&repository).await? {
        return Err(RegistryError::RepositoryNotFound {});
    }

    let blob = match registry.get_blob(&digest).await? {
        Some(blob) => blob,
        None => return Err(RegistryError::BlobNotFound {}),
    };

    if !blob.repositories.contains(&repository) {
        tracing::debug!("Blob exists but not in repostitory: {}", repository);
        return Err(RegistryError::BlobNotFound {});
    }

    /*if !blob.locations.contains(&app.id) {
        app.wait_for_blob(&path.digest).await;
    }*/

    let blob_path = registry.get_blob_path(&digest);
    if !blob_path.is_file() {
        tracing::info!("Blob was not present on disk");
        return Err(RegistryError::BlobNotFound {});
    }

    let blob_file = tokio::fs::File::open(blob_path).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Content-Digest", digest.to_string())
        .header(header::CONTENT_TYPE, blob.media_type)
        .header(header::CONTENT_LENGTH, blob.size)
        .body(Body::from_stream(ReaderStream::new(blob_file)))?)
}
