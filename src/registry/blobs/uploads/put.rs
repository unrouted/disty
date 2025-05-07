use std::sync::Arc;

use axum::{extract::{Path, State}, response::Response};
use serde::Deserialize;

use crate::{error::RegistryError, state::RegistryState};

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: String,
    upload_id: String,
}

#[derive(Debug, Deserialize)]
pub struct BlobUploadPutQuery {
    digest: Digest,
}

pub(crate) async fn put(
    Path(BlobUploadRequest { repository, upload_id }): Path<BlobUploadRequest>,
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

    /*if query.digest.algo != "sha256" {
        return Err(RegistryError::UploadInvalid {});
    }*/

    let filename = registry.upload_path(&upload_id);

    if !tokio::fs::try_exists(&filename).await? {
        return Err(RegistryError::UploadInvalid {});
    }

    if !upload_part(&filename, body).await {
        return Err(RegistryError::UploadInvalid {});
    }

    // Validate upload
    if !validate_hash(&filename, &digest).await {
        return Err(RegistryError::DigestInvalid {});
    }

    let dest = app.get_blob_path(&query.digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    match tokio::fs::rename(filename.clone(), dest.clone()).await {
        Ok(_) => {}
        Err(_e) => {
            return Err(RegistryError::UploadInvalid {});
        }
    }

    let actions = vec![
        RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            repository: path.repository.clone(),
            user: token.sub.clone(),
        },
        RegistryAction::BlobStat {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            size: stat.len(),
        },
        RegistryAction::BlobStored {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            location: app.id,
            user: token.sub.clone(),
        },
    ];

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
    Ok(HttpResponseBuilder::new(StatusCode::CREATED)
        .append_header((
            "Location",
            format!("/v2/{}/blobs/{}", path.repository, query.digest),
        ))
        .append_header(("Range", "0-0"))
        .append_header(("Content-Length", "0"))
        .append_header(("Docker-Content-Digest", query.digest.to_string()))
        .finish())
}