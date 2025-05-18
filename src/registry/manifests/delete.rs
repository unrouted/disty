use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

use crate::{digest::Digest, error::RegistryError, state::RegistryState, token::Token};

#[derive(Debug, Deserialize)]
pub struct ManifestDeleteRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn delete(
    Path(ManifestDeleteRequest { repository, tag }): Path<ManifestDeleteRequest>,
    State(registry): State<Arc<RegistryState>>,
    token: Token,
) -> Result<Response, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&repository),
        });
    }

    if !token.has_permission(&repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    if let Ok(digest) = Digest::try_from(tag.clone()) {
        if let Some(_manifest) = registry.get_manifest(&repository, &digest).await? {
            registry.delete_manifest(&repository, &digest).await?;

            return Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())?);
        }

        return Err(RegistryError::ManifestNotFound {});
    }

    if let Some(_manifest) = registry.get_tag(&repository, &tag).await? {
        registry.delete_tag(&repository, &tag).await?;

        return Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())?);
    }

    Err(RegistryError::ManifestNotFound {})
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use axum::http::Request;
    use reqwest::header::CONTENT_TYPE;
    use test_log::test;

    use crate::tests::RegistryFixture;

    use super::*;

    #[test(tokio::test)]
    pub async fn delete_tag() -> Result<()> {
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

        let res = fixture
            .request(
                Request::builder()
                    .method("DELETE")
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/foo/manifests/latest")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        fixture.teardown().await
    }
}
