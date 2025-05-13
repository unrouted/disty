use std::sync::Arc;

use axum::{
    Router,
    routing::{delete, get, head, patch, post, put},
};

use crate::state::RegistryState;

mod blobs;
mod manifests;
mod root;
mod utils;
// pub mod tags;

pub fn router(state: Arc<RegistryState>) -> Router {
    Router::new()
        .route("/v2/", get(root::get))
        .route(
            "/v2/{repository}/blobs/uploads/{upload_id}",
            delete(blobs::uploads::delete::delete)
                .get(blobs::uploads::get::get)
                .patch(blobs::uploads::patch::patch)
                .put(blobs::uploads::put::put),
        )
        .route(
            "/v2/{repository}/blobs/uploads",
            post(blobs::uploads::post::post),
        )
        .route(
            "/{repository}/blobs/{digest}",
            head(blobs::head::head)
                .get(blobs::get::get)
                .delete(blobs::delete::delete),
        )
        .route(
            "/{repository}/manifests/{digest}",
            head(manifests::head::head)
                .get(manifests::get::get)
                .delete(manifests::delete::delete)
                .put(manifests::put::put),
        )
        .with_state(state)
}
