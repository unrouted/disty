use std::collections::HashSet;

use crate::extractors::token::Access;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::registry::utils::{upload_part, validate_hash};
use crate::types::{Digest, RegistryAction};
use crate::{app::RegistryApp, types::RepositoryName};
use actix_web::http::StatusCode;
use actix_web::post;
use actix_web::web::{Payload, Query};
use actix_web::{
    web::{Data, Path},
    HttpResponse, HttpResponseBuilder,
};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
}
#[derive(Debug, Deserialize)]
pub struct BlobUploadPostQuery {
    mount: Option<Digest>,
    from: Option<RepositoryName>,
    digest: Option<Digest>,
}

#[post("/{repository:[^{}]+}/blobs/uploads")]
pub(crate) async fn post(
    app: Data<RegistryApp>,
    path: Path<BlobUploadRequest>,
    query: Query<BlobUploadPostQuery>,
    body: Payload,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        let mut access = vec![Access {
            repository: path.repository.clone(),
            permissions: HashSet::from(["pull".to_string(), "push".to_string()]),
        }];

        if let Some(from) = &query.from {
            access.push(Access {
                repository: from.clone(),
                permissions: HashSet::from(["pull".to_string()]),
            });
        }

        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_challenge(access),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    if let (Some(mount), Some(from)) = (&query.mount, &query.from) {
        if from == &path.repository {
            return Err(RegistryError::UploadInvalid {});
        }

        if !token.has_permission(from, "pull") {
            return Err(RegistryError::UploadInvalid {});
        }

        if let Some(blob) = app.get_blob(mount).await {
            if blob.repositories.contains(from) {
                let actions = vec![RegistryAction::BlobMounted {
                    timestamp: Utc::now(),
                    digest: mount.clone(),
                    repository: path.repository.clone(),
                    user: token.sub.clone(),
                }];

                if !app.submit(actions).await {
                    return Err(RegistryError::UploadInvalid {});
                }

                /*
                201 Created
                Location: <blob location>
                Content-Range: <start of range>-<end of range, inclusive>
                Content-Length: 0
                Docker-Content-Digest: <digest>
                */
                return Ok(HttpResponseBuilder::new(StatusCode::CREATED)
                    .append_header((
                        "Location",
                        format!("/v2/{}/blobs/{}", path.repository, mount),
                    ))
                    .append_header(("Range", "0-0"))
                    .append_header(("Content-Length", "0"))
                    .append_header(("Docker-Content-Digest", mount.to_string()))
                    .finish());
            }
        }
    }
    let upload_id = Uuid::new_v4().as_hyphenated().to_string();

    match &query.digest {
        Some(digest) => {
            let filename = app.get_upload_path(&upload_id);

            if !upload_part(&filename, body).await {
                return Err(RegistryError::UploadInvalid {});
            }

            // Validate upload
            if !validate_hash(&filename, digest).await {
                return Err(RegistryError::DigestInvalid {});
            }

            let dest = app.get_blob_path(digest);

            let stat = match tokio::fs::metadata(&filename).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(RegistryError::UploadInvalid {});
                }
            };

            match std::fs::rename(filename, dest) {
                Ok(_) => {}
                Err(_) => {
                    return Err(RegistryError::UploadInvalid {});
                }
            }

            let actions = vec![
                RegistryAction::BlobMounted {
                    timestamp: Utc::now(),
                    digest: digest.clone(),
                    repository: path.repository.clone(),
                    user: token.sub.clone(),
                },
                RegistryAction::BlobStat {
                    timestamp: Utc::now(),
                    digest: digest.clone(),
                    size: stat.len(),
                },
                RegistryAction::BlobStored {
                    timestamp: Utc::now(),
                    digest: digest.clone(),
                    location: app.config.identifier.clone(),
                    user: token.sub.clone(),
                },
            ];

            if !app.submit(actions).await {
                return Err(RegistryError::UploadInvalid {});
            }

            /*
            201 Created
            Location: <blob location>
            Content-Range: <start of range>-<end of range, inclusive>
            Content-Length: 0
            Docker-Content-Digest: <digest>
            */
            Ok(HttpResponseBuilder::new(StatusCode::CREATED)
                .append_header((
                    "Location",
                    format!("/v2/{}/blobs/{}", path.repository, digest),
                ))
                .append_header(("Range", "0-0"))
                .append_header(("Content-Length", "0"))
                .append_header(("Docker-Content-Digest", digest.to_string()))
                .finish())
        }
        _ => {
            // Nothing was uploaded, but a session was started...
            let filename = app.get_upload_path(&upload_id);

            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)
                .await
            {
                Ok(file) => drop(file),
                _ => return Err(RegistryError::UploadInvalid {}),
            }

            Ok(HttpResponseBuilder::new(StatusCode::ACCEPTED)
                .append_header((
                    "Location",
                    format!("/v2/{}/blobs/uploads/{}", path.repository, upload_id),
                ))
                .append_header(("Range", format!("0-{}", 0)))
                .append_header(("Content-Length", "0"))
                .append_header(("Docker-Upload-UUID", upload_id))
                .finish())
        }
    }
}
