use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::Response,
};
use hiqlite_macros::params;
use serde::Deserialize;

use crate::{
    digest::Digest,
    error::RegistryError,
    registry::utils::{upload_part, validate_hash},
    state::RegistryState,
};

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
    Path(BlobUploadRequest {
        repository,
        upload_id,
    }): Path<BlobUploadRequest>,
    Query(BlobUploadPutQuery { digest }): Query<BlobUploadPutQuery>,
    State(registry): State<Arc<RegistryState>>,
    body: Request<Body>,
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

    upload_part(&filename, body.into_body().into_data_stream()).await?;

    // Validate upload
    if !validate_hash(&filename, &digest).await {
        return Err(RegistryError::DigestInvalid {});
    }

    let dest = registry.get_blob_path(&digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    tokio::fs::rename(filename.clone(), dest.clone()).await?;

    registry
        .insert_blob(&digest, stat.len() as u32, "application/octet-stream")
        .await?;

    registry.mount_blob(&digest, &repository).await?;

    /*
    201 Created
    Location: <blob location>
    Content-Range: <start of range>-<end of range, inclusive>
    Content-Length: 0
    Docker-Content-Digest: <digest>
    */
    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("Location", format!("/v2/{}/blobs/{}", repository, digest))
        .header("Range", "0-0")
        .header("Content-Length", "0")
        .header("Docker-Content-Digest", digest.to_string())
        .body(Body::empty())?)
}
