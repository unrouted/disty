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
        .route("/:repository/blobs/:digest", head(blobs::head::head))
        .route("/:repository/blobs/:digest", get(blobs::get::get))
        .route("/:repository/blobs/:digest", delete(blobs::delete::delete))
        .route(
            "/:repository/manifests/:digest",
            head(manifests::head::head),
        )
        .route("/:repository/manifests/:digest", get(manifests::get::get))
        .route(
            "/:repository/manifests/:digest",
            delete(manifests::delete::delete),
        )
        .route("/:repository/manifests/:tag", put(manifests::put::put))
        .with_state(Arc::new(state))
}
