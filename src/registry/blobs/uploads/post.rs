use std::sync::Arc;

use crate::digest::Digest;
use crate::error::RegistryError;
use crate::registry::utils::{upload_part, validate_hash};
use crate::state::RegistryState;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use hiqlite_macros::params;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
}
#[derive(Debug, Deserialize)]
pub struct BlobUploadPostQuery {
    mount: Option<String>,
    from: Option<String>,
    digest: Option<Digest>,
}

pub(crate) async fn post(
    Path(BlobUploadRequest { repository }): Path<BlobUploadRequest>,
    Query(BlobUploadPostQuery {
        mount,
        from,
        digest,
    }): Query<BlobUploadPostQuery>,
    State(registry): State<Arc<RegistryState>>,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    /*if !token.validated_token {
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
    }*/

    if let (Some(mount), Some(from)) = (mount, &from) {
        if from == &repository {
            return Err(RegistryError::UploadInvalid {});
        }

        //if !token.has_permission(from, "pull") {
        //    return Err(RegistryError::UploadInvalid {});
        //}

        if let Some(blob) = app.get_blob(mount) {
            if blob.repositories.contains(from) {
                // INSERT OR IGNORE INTO repositories (name)
                // VALUES ('library/ubuntu');

                // Insert repository id
                //registry.client.execute(
                //    "INSERT INTO blobs (digest, repository_id, size, media_type, location) VALUES ($1, $2, $3, $4, $5);"
                //    , params!(digest.to_string(), 1, stat.len() as u32, "application/octet-stream", 1)
                //).await?;

                let actions = vec![RegistryAction::BlobMounted {
                    timestamp: Utc::now(),
                    digest: mount.clone(),
                    repository: path.repository.clone(),
                    user: token.sub.clone(),
                }];

                if !app.consistent_write(actions).await {
                    return Err(RegistryError::UploadInvalid {});
                }

                /*
                201 Created
                Location: <blob location>
                Content-Range: <start of range>-<end of range, inclusive>
                Content-Length: 0
                Docker-Content-Digest: <digest>
                */
                return Ok(Response::builder().status(StatusCode::CREATED)
                    .header(
                        "Location",
                        format!("/v2/{}/blobs/{}", repository, mount),
                    )
                    .header("Range", "0-0")
                    .header("Content-Length", "0")
                    .header("Docker-Content-Digest", mount.to_string())
                    .body(Body::empty())?);
            }
        }
    }
    let upload_id = Uuid::new_v4().as_hyphenated().to_string();

    match &digest {
        Some(digest) => {
            let filename = registry.upload_path(&upload_id);

            upload_part(&filename, body.into_body().into_data_stream()).await?; 

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

            match tokio::fs::rename(filename, dest).await {
                Ok(_) => {}
                Err(_) => {
                    return Err(RegistryError::UploadInvalid {});
                }
            }

            registry.client.execute(
                "INSERT INTO blobs (digest, repository_id, size, media_type, location) VALUES ($1, $2, $3, $4, $5);"
                , params!(digest.to_string(), 1, stat.len() as u32, "application/octet-stream", 1)
            ).await?;

            /*
            201 Created
            Location: <blob location>
            Content-Range: <start of range>-<end of range, inclusive>
            Content-Length: 0
            Docker-Content-Digest: <digest>
            */
            Ok(Response::builder()
                .status(StatusCode::CREATED)
                .header(
                    "Location",
                    format!("/v2/{}/blobs/{}", repository, digest),
                )
                .header("Range", "0-0")
                .header("Content-Length", "0")
                .header("Docker-Content-Digest", digest.to_string())
                .body(Body::empty())?)
        }
        _ => {
            // Nothing was uploaded, but a session was started...
            let filename = registry.upload_path(&upload_id);

            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)
                .await
            {
                Ok(file) => drop(file),
                _ => return Err(RegistryError::UploadInvalid {}),
            }

            Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .header(
                    "Location",
                    format!("/v2/{}/blobs/uploads/{}", repository, upload_id),
                )
                .header("Range", format!("0-{}", 0))
                .header("Content-Length", "0")
                .header("Docker-Upload-UUID", upload_id)
                .body(Body::empty())?)
        }
    }
}
