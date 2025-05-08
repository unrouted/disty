use std::sync::Arc;

use axum::{
    Router,
    routing::{delete, get, patch, post, put},
};

use crate::state::RegistryState;

pub mod blobs;
mod root;
mod utils;
// pub mod manifests;
// pub mod tags;

pub fn router(state: RegistryState) -> Router {
    Router::new()
        .route("/v2/", get(root::get))
        .route(
            "/v2/:repository/blobs/uploads/:upload_id",
            delete(blobs::uploads::delete::delete),
        )
        .route(
            "/v2/:repository/blobs/uploads/:upload_id",
            get(blobs::uploads::get::get),
        )
        .route(
            "/v2/:repository/blobs/uploads/:upload_id",
            patch(blobs::uploads::patch::patch),
        )
        .route(
            "/v2/:repository/blobs/uploads/:upload_id",
            put(blobs::uploads::put::put),
        )
        .route(
            "/v2/:repository/blobs/uploads",
            post(blobs::uploads::post::post),
        )
        //.route("/v2/:repository/:image/blobs/:digest", head(blob_check))
        //.route("/v2/:repository/:image/blobs/:digest", get(blob_get))
        //.route(
        //    "/v2/:repository/:image/manifests/:reference",
        //    put(manifest_put),
        // )
        //.route(
        //    "/v2/:repository/:image/manifests/:reference",
        //    get(manifest_get),
        // )
        .with_state(Arc::new(state))
}
