use std::sync::Arc;

use axum::{
    Router,
    routing::{delete, get, head, post},
};

use crate::state::RegistryState;

pub(crate) use middleware::RewriteUriLayer;

mod blobs;
mod content_range;
mod manifests;
mod middleware;
mod referrers;
mod root;
mod tags;
mod token;
mod utils;

pub fn router(state: Arc<RegistryState>) -> Router {
    Router::new()
        .route("/auth/token", get(token::token))
        .route("/v2/", get(root::get).head(root::get))
        .route(
            "/v2/{repository}/blobs/uploads/{upload_id}",
            delete(blobs::uploads::delete::delete)
                .get(blobs::uploads::get::get)
                .patch(blobs::uploads::patch::patch)
                .put(blobs::uploads::put::put),
        )
        .route(
            "/v2/{repository}/blobs/uploads/",
            post(blobs::uploads::post::post),
        )
        .route(
            "/v2/{repository}/blobs/{digest}",
            head(blobs::head::head)
                .get(blobs::get::get)
                .delete(blobs::delete::delete),
        )
        .route(
            "/v2/{repository}/manifests/{tag}",
            head(manifests::head::head)
                .get(manifests::get::get)
                .delete(manifests::delete::delete)
                .put(manifests::put::put),
        )
        .route("/v2/{repository}/referrers/{tag}", get(referrers::get))
        .route("/v2/{repository}/tags/list", get(tags::get::get))
        .with_state(state)
}
