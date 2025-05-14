use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode},
};

use crate::state::RegistryState;

pub async fn get(State(_registry): State<Arc<RegistryState>>) -> Response<Body> {
    /*if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_general_challenge(),
        });
    }*/

    Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Distribution-Api-Version", "registry/2.0")
        .body(Body::empty())
        .unwrap()
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
