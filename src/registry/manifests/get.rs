use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::Response,
};
use serde::Deserialize;
use tracing::debug;

use crate::{error::RegistryError, state::RegistryState};

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
) -> Result<Response, RegistryError> {
    /*if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }*/

    let manifest = match registry.get_manifest(&path.digest).await? {
        Some(manifest) => {
            if !manifest.repositories.contains(&repository) {
                return Err(RegistryError::ManifestNotFound {});
            }
            manifest
        }
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    /*if !manifest.locations.contains(&app.id) {
        app.wait_for_manifest(&path.digest).await;
    }*/

    let manifest_path = registry.get_manifest_path(&manifest.digest);
    if !manifest_path.is_file() {
        debug!("Expected manifest file does not exist");
        return Err(RegistryError::ManifestNotFound {});
    }

    let blob_file = tokio::fs::File::open(blob_path).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Content-Digest", digest.to_string())
        .header(header::CONTENT_TYPE, blob.media_type)
        .header(header::CONTENT_LENGTH, blob.size)
        .body(Body::from_stream(ReaderStream::new(blob_file)))?)
}
