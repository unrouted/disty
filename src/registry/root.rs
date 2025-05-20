use std::sync::Arc;

use anyhow::Result;
use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode},
};

use crate::{error::RegistryError, state::RegistryState, token::Token};

pub async fn get(
    State(_registry): State<Arc<RegistryState>>,
    token: Token,
) -> Result<Response<Body>, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_general_challenge(),
        });
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Distribution-Api-Version", "registry/2.0")
        .body(Body::empty())?)
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use axum::http::Request;
    use test_log::test;

    use crate::tests::RegistryFixture;

    use super::*;

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
