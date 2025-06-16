use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Body,
    extract::{Path, Query, State},
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManifestIndexItem {
    pub media_type: String,
    pub size: u32,
    pub digest: Digest,
    pub artifact_type: Option<String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerFilters {
    artifact_type: Option<String>,
}

pub(crate) async fn get(
    Path(ReferrerRequest { repository, digest }): Path<ReferrerRequest>,
    Query(ReferrerFilters { artifact_type }): Query<ReferrerFilters>,
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

    if !registry.repository_exists(&repository).await? {
        return Err(RegistryError::RepositoryNotFound {});
    }

    let mut manifests = vec![];

    if let Some(manifest) = registry.get_manifest(&digest).await? {
        if manifest.repositories.contains(&repository) {
            for manifest in registry.get_referrer(&digest).await? {
                if manifest.repositories.contains(&repository) {
                    if let Some(artifact_type) = &artifact_type {
                        if manifest.artifact_type.as_ref() != Some(artifact_type) {
                            continue;
                        }
                    }

                    manifests.push(ManifestIndexItem {
                        media_type: manifest.media_type,
                        size: manifest.size,
                        digest: manifest.digest,
                        artifact_type: manifest.artifact_type,
                        annotations: manifest.annotations,
                    })
                }
            }
        }
    };

    let index = ManifestIndex {
        schema_version: 2,
        media_type: "application/vnd.oci.image.index.v1+json".into(),
        manifests,
        annotations: BTreeMap::new(),
    };

    let resp = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        "application/vnd.oci.image.index.v1+json",
    );

    let mut filters = vec![];
    if artifact_type.is_some() {
        filters.push("artifactType");
    }

    let resp = match filters.is_empty() {
        true => resp,
        false => resp.header("OCI-Filters-Applied", filters.join(",")),
    };

    let body = Body::from(serde_json::to_string(&index)?);

    Ok(resp.body(body)?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use http_body_util::BodyExt;
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

    #[test(tokio::test)]
    pub async fn referrers_do_refer() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a")
                .body(Body::from("{}"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:ee29d2e91da0e5dbf6536f5b369148a83ef59b0ce96e49da65dd6c25eb1fa44f")
                .body(Body::from("NHL Peanut Butter on my NHL bagel"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:e997eca5fa72577f59c173736ab505ee66777324419b3b4b338bd0e2286f8287")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_3.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:0a5e7446398586dc0f057cab5eecc2fa78d0ed6d0daa73a84595f8b69647062f")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_2.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:e15e91042b162252646726a7a9550e718fd09edacaa9820a4b1f05af05656acd")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_6.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:70b5c11f98715d740c6babeae1a31c0c19c091671c73ea67b2ad9b6344f1aa6a")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_7.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:b11cfd6ca08252387b328a5340932d2cee3ce8d012b370131b379c9d8b76f232")
                .header("Content-Type", "application/vnd.oci.image.index.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_8.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        // The spec allows out of order pushes which is unfortunate
        // https://github.com/opencontainers/distribution-spec/issues/459
        // This has implications for the DAG and garbage collection

        let res = fixture
            .request(
                Request::builder()
                    .method("PUT")
                    .uri("/v2/foo/manifests/tagtest0")
                    .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                    .body(Body::from(std::fs::read_to_string(
                        "./fixtures/manifests/ref_test_5.json",
                    )?))?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/foo/referrers/sha256:71ea9d131e44595bc882d0538103b67d45b99cbd0785bbe11d10428254fb8a1d")
                .body(Body::empty())?
        ).await?;

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: ManifestIndex = serde_json::from_slice(&body)?;

        assert_eq!(value.manifests.len(), 5);

        for manifest in value.manifests.iter() {
            assert_eq!(manifest.annotations.len(), 1);
        }

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn referrers_do_refer_filtered() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a")
                .body(Body::from("{}"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("POST")
                .uri("/v2/foo/blobs/uploads/?digest=sha256:ee29d2e91da0e5dbf6536f5b369148a83ef59b0ce96e49da65dd6c25eb1fa44f")
                .body(Body::from("NHL Peanut Butter on my NHL bagel"))?
            ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:e997eca5fa72577f59c173736ab505ee66777324419b3b4b338bd0e2286f8287")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_3.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:0a5e7446398586dc0f057cab5eecc2fa78d0ed6d0daa73a84595f8b69647062f")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_2.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:e15e91042b162252646726a7a9550e718fd09edacaa9820a4b1f05af05656acd")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_6.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:70b5c11f98715d740c6babeae1a31c0c19c091671c73ea67b2ad9b6344f1aa6a")
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_7.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("PUT")
                .uri("/v2/foo/manifests/sha256:b11cfd6ca08252387b328a5340932d2cee3ce8d012b370131b379c9d8b76f232")
                .header("Content-Type", "application/vnd.oci.image.index.v1+json")
                .body(Body::from(std::fs::read_to_string("./fixtures/manifests/ref_test_8.json")?))?
        ).await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        // The spec allows out of order pushes which is unfortunate
        // https://github.com/opencontainers/distribution-spec/issues/459
        // This has implications for the DAG and garbage collection

        let res = fixture
            .request(
                Request::builder()
                    .method("PUT")
                    .uri("/v2/foo/manifests/tagtest0")
                    .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                    .body(Body::from(std::fs::read_to_string(
                        "./fixtures/manifests/ref_test_5.json",
                    )?))?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture.request(
            Request::builder()
                .method("GET")
                .uri("/v2/foo/referrers/sha256:71ea9d131e44595bc882d0538103b67d45b99cbd0785bbe11d10428254fb8a1d?artifactType=application/vnd.nhl.peanut.butter.bagel")
                .body(Body::empty())?
        ).await?;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get("OCI-Filters-Applied")
                .unwrap()
                .to_str()
                .unwrap(),
            "artifactType"
        );

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let value: ManifestIndex = serde_json::from_slice(&body)?;

        assert_eq!(value.manifests.len(), 2);

        for manifest in value.manifests.iter() {
            assert_eq!(manifest.annotations.len(), 1);
        }

        fixture.teardown().await
    }
}
