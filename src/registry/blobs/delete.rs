use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{digest::Digest, error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: String,
    digest: Digest,
}

pub(crate) async fn delete(
    Path(BlobRequest { repository, digest }): Path<BlobRequest>,
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

    if let Some(blob) = registry.get_blob(&digest).await? {
        if !blob.repositories.contains(&repository) {
            return Err(RegistryError::BlobNotFound {});
        }
    } else {
        return Err(RegistryError::BlobNotFound {});
    }

    registry.unmount_blob(&digest, &repository).await?;

    Ok(Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Body::empty())?)
}
