use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::network::registry::errors::RegistryError;
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
pub struct ManifestDeleteRequestDigest {
    repository: RepositoryName,
    digest: Digest,
}

#[delete("/{repository:[^{}]+}/manifests/{digest}")]
pub(crate) async fn delete(
    app: Data<RegistryApp>,
    path: Path<ManifestDeleteRequestDigest>,
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

    if let Some(manifest) = app.get_manifest(&path.digest).await {
        if !manifest.repositories.contains(&path.repository) {
            return Err(RegistryError::ManifestNotFound {});
        }
    } else {
        return Err(RegistryError::ManifestNotFound {});
    }

    let actions = vec![RegistryAction::ManifestUnmounted {
        timestamp: Utc::now(),
        digest: path.digest.clone(),
        repository: path.repository.clone(),
        user: token.sub.clone(),
    }];

    if !app.submit(actions).await {
        // FIXME
        return Err(RegistryError::ManifestInvalid {});
    }

    Ok(HttpResponseBuilder::new(StatusCode::ACCEPTED).finish())
}

#[derive(Debug, Deserialize)]
pub struct ManifestDeleteRequestTag {
    repository: RepositoryName,
    tag: String,
}

#[delete("/{repository:[^{}]+}/manifests/{tag}")]
pub(crate) async fn delete_by_tag(
    app: Data<RegistryApp>,
    path: Path<ManifestDeleteRequestTag>,
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

    let digest = match app.get_tag(&path.repository, &path.tag).await {
        Some(tag) => tag,
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    if let Some(manifest) = app.get_manifest(&digest).await {
        if !manifest.repositories.contains(&path.repository) {
            return Err(RegistryError::ManifestNotFound {});
        }
    } else {
        return Err(RegistryError::ManifestNotFound {});
    }

    let actions = vec![RegistryAction::ManifestUnmounted {
        timestamp: Utc::now(),
        digest,
        repository: path.repository.clone(),
        user: token.sub.clone(),
    }];

    if !app.submit(actions).await {
        // FIXME
        return Err(RegistryError::ManifestInvalid {});
    }

    Ok(HttpResponseBuilder::new(StatusCode::ACCEPTED).finish())
}
