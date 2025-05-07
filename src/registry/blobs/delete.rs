use crate::app::RegistryApp;
use crate::extractors::Token;

use crate::registry::errors::RegistryError;
use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use actix_web::delete;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use chrono::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: RepositoryName,
    digest: Digest,
}

#[delete("/{repository:[^{}]+}/blobs/{digest}")]
pub(crate) async fn delete(
    app: Data<RegistryApp>,
    path: Path<BlobRequest>,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    if let Some(blob) = app.get_blob(&path.digest) {
        if !blob.repositories.contains(&path.repository) {
            return Err(RegistryError::BlobNotFound {});
        }
    } else {
        return Err(RegistryError::BlobNotFound {});
    }

    let actions = vec![RegistryAction::BlobUnmounted {
        timestamp: Utc::now(),
        digest: path.digest.clone(),
        repository: path.repository.clone(),
        user: token.sub.clone(),
    }];

    if !app.consistent_write(actions).await {
        // FIXME
        return Err(RegistryError::BlobNotFound {});
    }

    Ok(HttpResponseBuilder::new(StatusCode::ACCEPTED).finish())
}