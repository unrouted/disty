use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::types::Digest;
use crate::types::RepositoryName;
use actix_files::NamedFile;
use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpRequest;
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

#[get("/{repository:[^{}]+}/manifests/{digest:sha256:.*}")]
pub(crate) async fn get(
    app: Data<RegistryApp>,
    req: HttpRequest,
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

    let manifest = match app.get_manifest(&path.digest).await {
        Some(manifest) => {
            if !manifest.repositories.contains(&path.repository) {
                return Err(RegistryError::ManifestNotFound {});
            }
            manifest
        }
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    if !manifest.locations.contains(&app.config.identifier) {
        app.wait_for_manifest(&path.digest).await;
    }

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            debug!("Could not extract content type from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let _content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => {
            debug!("Could not extract content length from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let manifest_path = app.get_manifest_path(&path.digest);
    if !manifest_path.is_file() {
        debug!("Expected manifest file does not exist");
        return Err(RegistryError::ManifestNotFound {});
    }

    let manifest = NamedFile::open_async(manifest_path)
        .await
        .unwrap()
        .into_response(&req);

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Docker-Content-Digest", path.digest.to_string()))
        .body(manifest.into_body()))
}

#[derive(Debug, Deserialize)]
pub struct ManifestGetRequestTag {
    repository: RepositoryName,
    tag: String,
}

#[get("/{repository:[^{}]+}/manifests/{tag}")]
pub(crate) async fn get_by_tag(
    app: Data<RegistryApp>,
    req: HttpRequest,
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

    let digest = match app.get_tag(&path.repository, &path.tag).await {
        Some(tag) => tag,
        None => {
            debug!("No such tag");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let manifest = match app.get_manifest(&digest).await {
        Some(manifest) => {
            if !manifest.repositories.contains(&path.repository) {
                return Err(RegistryError::ManifestNotFound {});
            }
            manifest
        }
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    if !manifest.locations.contains(&app.config.identifier) {
        app.wait_for_manifest(&digest).await;
    }

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            debug!("Could not extract content type from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let _content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => {
            debug!("Could not extract content length from graph");
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let manifest_path = app.get_manifest_path(&digest);
    if !manifest_path.is_file() {
        debug!("Expected manifest file does not exist");
        return Err(RegistryError::ManifestNotFound {});
    }

    let manifest = NamedFile::open_async(manifest_path)
        .await
        .unwrap()
        .into_response(&req);

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Docker-Content-Digest", digest.to_string()))
        .body(manifest.into_body()))
}
