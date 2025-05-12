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

    if let Ok(digest) = Digest::try_from(tag.clone()) {
        if let Some(_manifest) = registry.get_manifest(&repository, &digest).await? {
            registry.delete_manifest(&repository, &digest).await?;

            return Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())?);
        }

        return Err(RegistryError::ManifestNotFound {});
    }

    if let Some(manifest) = registry.get_tag(&repository, &tag).await? {
        registry.delete_tag(&repository, &tag).await?;

        return Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())?);
    }

    Err(RegistryError::ManifestNotFound {})
}
