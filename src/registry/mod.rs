use std::sync::Arc;

use axum::{
    Router,
    http::HeaderValue,
    routing::{delete, get, head, post},
};
use tower_http::request_id::{MakeRequestId, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;
use uuid::Uuid;

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

use tower_http::request_id::RequestId;

#[derive(Clone)]
struct RequestIdGenerator;

impl MakeRequestId for RequestIdGenerator {
    fn make_request_id<B>(&mut self, _request: &http::Request<B>) -> Option<RequestId> {
        let uuid = Uuid::new_v4().to_string();
        Some(RequestId::from(HeaderValue::from_str(&uuid).unwrap()))
    }
}

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
        .route("/v2/{repository}/referrers/{digest}", get(referrers::get))
        .route("/v2/{repository}/tags/list", get(tags::get::get))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(RequestIdGenerator))
}
