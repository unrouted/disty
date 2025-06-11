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
        Ok(digest) => registry.get_manifest(&digest).await?,
        Err(_) => registry.get_tag(&repository, &tag).await?,
    };

    let manifest = match manifest {
        Some(manifest) => manifest,
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    if !manifest.repositories.contains(&repository) {
        return Err(RegistryError::ManifestNotFound {});
    }

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

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn get_manifest_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/manifests/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"fixme\",service=\"some-audience\",scope=\"repository:bar:pull\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get_manifests_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/manifests/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }
}
