use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::Response,
};
use serde::{Deserialize, Serialize};

use crate::{digest::Digest, error::RegistryError, state::RegistryState, token::Token};

/*
200 OK
Content-Type: application/vnd.oci.image.index.v1+json

{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "size": 1234,
      "digest": "sha256:a1a1a1...",
      "artifactType": "application/vnd.example.sbom.v1",
      "annotations": {
        "org.opencontainers.artifact.created": "2022-01-01T14:42:55Z",
        "org.example.sbom.format": "json"
      }
    }
  ],
  "annotations": {
    "org.opencontainers.referrers.filtersApplied": "artifactType"
  }
}
*/

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManifestIndexItem {
    pub media_type: String,
    pub size: u32,
    pub digest: Digest,
    pub artifact_type: Option<String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManifestIndex {
    pub schema_version: u64,
    pub media_type: String,
    pub manifests: Vec<ManifestIndexItem>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ReferrerRequest {
    repository: String,
    digest: Digest,
}

pub(crate) async fn get(
    Path(ReferrerRequest { repository, digest }): Path<ReferrerRequest>,
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

    let manifest = match registry.get_manifest(&digest).await? {
        Some(manifest) => manifest,
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    if !manifest.repositories.contains(&repository) {
        return Err(RegistryError::ManifestNotFound {});
    }

    let mut manifests = vec![];

    for manifest in registry.get_referrer(&digest).await? {
        manifests.push(ManifestIndexItem {
            media_type: manifest.media_type,
            size: manifest.size,
            digest: manifest.digest,
            artifact_type: None,
            annotations: BTreeMap::new(),
        })
    }

    let index = ManifestIndex {
        schema_version: 2,
        media_type: "application/vnd.oci.image.index.v1+json".into(),
        manifests,
        annotations: BTreeMap::new(),
    };

    let body = Body::from(serde_json::to_string(&index)?);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, manifest.media_type)
        .header(header::CONTENT_LENGTH, manifest.size)
        .body(body)?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn get_referrers_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/referrers/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
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
    pub async fn get_referrers_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/bar/referrers/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }
}
