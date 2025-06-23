use std::sync::Arc;

use anyhow::Result;
use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode},
};

use crate::{context::RequestContext, error::RegistryError, state::RegistryState};

pub async fn get(
    State(_registry): State<Arc<RegistryState>>,
    context: RequestContext,
) -> Result<Response<Body>, RegistryError> {
    if !context.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: context.get_general_challenge(),
        });
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Distribution-Api-Version", "registry/2.0")
        .body(Body::empty())?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use axum::http::Request;
    use test_log::test;

    use crate::tests::{FixtureBuilder, RegistryFixture};

    use super::*;

    #[test(tokio::test)]
    pub async fn get_root_please_auth() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/")
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()
                .get("Www-Authenticate")
                .context("Missing header")?,
            "Bearer realm=\"fixme\",service=\"some-audience\""
        );

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get_root_no_acl() -> Result<()> {
        let fixture =
            RegistryFixture::with_state(FixtureBuilder::new().authenticated(true).build().await?)?;

        let res = fixture
            .request(
                Request::builder()
                    .method("GET")
                    .uri("/v2/")
                    .header("Authorization", fixture.bearer_header(vec![])?)
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        fixture.teardown().await
    }

    #[test(tokio::test)]
    pub async fn get() -> Result<()> {
        let fixture = RegistryFixture::new().await?;

        let res = fixture
            .request(Request::builder().uri("/v2/").body(Body::empty())?)
            .await?;

        assert_eq!(res.status(), StatusCode::OK);

        fixture.teardown().await
    }
}
