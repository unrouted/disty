use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::types::Digest;
use crate::types::RepositoryName;
use actix_web::head;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use serde::Deserialize;
use tracing::debug;

/*
200 OK
Docker-Content-Digest: <digest>
Content-Type: <media type of manifest>

{
   "name": <name>,
   "tag": <tag>,
   "fsLayers": [
      {
         "blobSum": "<digest>"
      },
      ...
    ]
   ],
   "history": <v1 images>,
   "signature": <JWS>
}
*/

#[derive(Debug, Deserialize)]
pub struct ManifestGetRequestDigest {
    repository: RepositoryName,
    digest: Digest,
}

#[head("/{repository:[^{}]+}/manifests/{digest:sha256:.*}")]
pub(crate) async fn head(
    app: Data<RegistryApp>,
    path: Path<ManifestGetRequestDigest>,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    let manifest = match app.get_manifest(&path.digest) {
        Some(manifest) => {
            if !manifest.repositories.contains(&path.repository) {
                return Err(RegistryError::ManifestNotFound {});
            }
            manifest
        }
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            debug!("Could not extract content type from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => {
            debug!("Could not extract content length from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Content-Length", content_length))
        .append_header(("Docker-Content-Digest", path.digest.to_string()))
        .finish())
}

#[derive(Debug, Deserialize)]
pub struct ManifestGetRequestTag {
    repository: RepositoryName,
    tag: String,
}

#[head("/{repository:[^{}]+}/manifests/{tag}")]
pub(crate) async fn head_by_tag(
    app: Data<RegistryApp>,
    path: Path<ManifestGetRequestTag>,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    let digest = match app.get_tag(&path.repository, &path.tag) {
        Some(tag) => tag,
        None => {
            debug!("No such tag");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let manifest = match app.get_manifest(&digest) {
        Some(manifest) => {
            if !manifest.repositories.contains(&path.repository) {
                return Err(RegistryError::ManifestNotFound {});
            }
            manifest
        }
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            debug!("Could not extract content type from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => {
            debug!("Could not extract content length from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Content-Length", content_length))
        .append_header(("Docker-Content-Digest", digest.to_string()))
        .finish())
}
