use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode},
};

use crate::state::RegistryState;

pub async fn get(State(registry): State<Arc<RegistryState>>) -> Response<Body> {
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
