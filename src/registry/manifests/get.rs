use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::Response,
};
use serde::Deserialize;
use tokio_util::io::ReaderStream;
use tracing::error;

use crate::{digest::Digest, error::RegistryError, state::RegistryState, token::Token};

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
pub struct ManifestGetRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn get(
    Path(ManifestGetRequest { repository, tag }): Path<ManifestGetRequest>,
    State(registry): State<Arc<RegistryState>>,
    token: Token,
) -> Result<Response, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&repository),
        });
    }

    if !token.has_permission(&repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    let manifest = match Digest::try_from(tag.clone()) {
        Ok(digest) => registry.get_manifest(&repository, &digest).await?,
        Err(_) => registry.get_tag(&repository, &tag).await?,
    };

    let manifest = match manifest {
        Some(manifest) => manifest,
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    /*if !manifest.locations.contains(&app.id) {
        app.wait_for_manifest(&path.digest).await;
    }*/

    let manifest_path = registry.get_manifest_path(&manifest.digest);
    if !manifest_path.is_file() {
        error!("Expected manifest file does not exist: {:?}", manifest_path);
        return Err(RegistryError::ManifestNotFound {});
    }

    let blob_file = tokio::fs::File::open(manifest_path).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Content-Digest", manifest.digest.to_string())
        .header(header::CONTENT_TYPE, manifest.media_type)
        .header(header::CONTENT_LENGTH, manifest.size)
        .body(Body::from_stream(ReaderStream::new(blob_file)))?)
}
