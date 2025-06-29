use std::sync::Arc;

use crate::{
    context::RequestContext,
    error::RegistryError,
    extractor::parse_manifest,
    registry::utils::{get_hash, upload_part},
    state::RegistryState,
};
use anyhow::Context;
use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::StatusCode,
    response::Response,
};
use axum_extra::TypedHeader;
use headers::ContentType;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ManifestPutRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn put(
    Path(ManifestPutRequest { repository, tag }): Path<ManifestPutRequest>,
    content_type: TypedHeader<ContentType>,
    State(registry): State<Arc<RegistryState>>,
    context: RequestContext,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_push_challenge(&repository),
        });
    }

    if !context.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    let upload_path = registry.get_temp_path();

    let parent = upload_path
        .parent()
        .context("Couldn't find parent directory")?;
    tokio::fs::create_dir_all(parent).await?;

    upload_part(&upload_path, body.into_body().into_data_stream()).await?;

    let _size = match tokio::fs::metadata(&upload_path).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Err(RegistryError::ManifestInvalid {});
        }
    };

    let digest = match get_hash(&upload_path).await {
        Some(digest) => digest,
        _ => {
            return Err(RegistryError::ManifestInvalid {});
        }
    };

    let Ok(extracted) = parse_manifest(&tokio::fs::read_to_string(&upload_path).await?) else {
        tracing::error!(
            "Extraction failed: {:?}",
            tokio::fs::read_to_string(&upload_path).await?
        );
        return Err(RegistryError::ManifestInvalid {});
    };

    let content_type = content_type.to_string();
    if let Some(media_type) = &extracted.media_type {
        if &content_type != media_type {
            tracing::error!("Content-Type doesn't match mediaType");
            return Err(RegistryError::ManifestInvalid {});
        }
    }

    let dest = registry.get_manifest_path(&digest);

    let parent = dest.parent().context("Couldn't find parent directory")?;
    tokio::fs::create_dir_all(parent).await?;

    match tokio::fs::rename(upload_path, dest).await {
        Ok(_) => {}
        Err(_) => {
            return Err(RegistryError::ManifestInvalid {});
        }
    }

    registry
        .insert_manifest(
            &repository,
            &tag,
            &digest,
            &content_type,
            &extracted,
            &context,
        )
        .await?;

    /*
    201 Created
    Location: <url>
    Content-Length: 0
    Docker-Content-Digest: <digest>
    */
    let resp = Response::builder()
        .status(StatusCode::CREATED)
        .header(
            "Location",
            format!("/v2/{}/manifests/{}", repository, digest),
        )
        .header("Docker-Content-Digest", digest.to_string());

    let resp = match extracted.subject {
        Some(subject) => resp.header("OCI-Subject", subject.digest.to_string()),
        None => resp,
    };

    Ok(resp.body(Body::empty())?)
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use axum::http::Request;
    use reqwest::header::CONTENT_TYPE;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn put_manifest_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("PUT")
                    .header(
                        CONTENT_TYPE,
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                    )
                    .uri("/v2/bar/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"http://localhost/auth/token\",service=\"http://localhost\",scope=\"repository:bar:pull,push\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn put_manifests_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("PUT")
                    .header(
                        CONTENT_TYPE,
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                    )
                    .uri("/v2/bar/manifests/latest")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_manifest() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let payload = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        let res = fixture
            .request(
                Request::builder()
                    .method("PUT")
                    .header(
                        CONTENT_TYPE,
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                    )
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::from(payload.to_string()))?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::CREATED);

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn upload_manifest_twice() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let payload = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        for _i in 0..2 {
            let res = fixture
                .request(
                    Request::builder()
                        .method("PUT")
                        .header(
                            CONTENT_TYPE,
                            "application/vnd.docker.distribution.manifest.list.v2+json",
                        )
                        .uri("/v2/foo/manifests/latest")
                        .body(Body::from(payload.to_string()))?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::CREATED);

            let res = fixture
                .request(
                    Request::builder()
                        .method("GET")
                        .uri("/v2/foo/manifests/latest")
                        .body(Body::empty())?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::OK);
        }

        fixture.teardown().await
    }
}
