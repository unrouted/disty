use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct ManifestDeleteRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn delete(
    Path(ManifestDeleteRequest { repository, tag }): Path<ManifestDeleteRequest>,
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

    if let Some(manifest) = registry.get_manifest(&digest).await? {
        if !manifest.repositories.contains(&repository) {
            return Err(RegistryError::ManifestNotFound {});
        }

        registry
            .unmount_manifest(&manifest.digest, &repository)
            .await?;

        return Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())?);
    }

    Err(RegistryError::ManifestNotFound {})
}
