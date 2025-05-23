use std::task::{Context, Poll};

use axum::{
    body::Body,
    http::{Request, Response, Uri},
};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use regex::{Captures, Regex};
use tower::Service;

#[derive(Clone)]
pub(crate) struct RewriteUriLayer;

impl<S> tower::Layer<S> for RewriteUriLayer {
    type Service = RewriteUriService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RewriteUriService { inner }
    }
}

#[derive(Clone)]
pub(crate) struct RewriteUriService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RewriteUriService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        let path = req.uri().path().to_string();

        let re = Regex::new(r"(^/v2/)(.+)(/(manifests|blobs|tags).*$)").unwrap();

        println!("{}", path);

        let result = re.replace(&path, |caps: &Captures| {
            let prefix = &caps[1];
            let encoded = utf8_percent_encode(&caps[2], NON_ALPHANUMERIC).to_string();
            let suffix = &caps[3];

            format!("{prefix}{encoded}{suffix}")
        });

        println!("{}", result);

        let mut parts = req.uri().clone().into_parts();
        parts.path_and_query = Some(result.parse().unwrap());
        let new_uri = Uri::from_parts(parts).unwrap();
        *req.uri_mut() = new_uri;

        println!("{}", req.uri().path());

        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::RegistryError;

    use super::*;
    use axum::{Router, body::Body, extract::Path, http::Request, response::Response};
    use http_body_util::BodyExt;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use tower::ServiceExt;

    #[derive(Debug, Deserialize)]
    pub struct ManifestGetRequest {
        repository: String,
        tag: String,
    }

    pub(crate) async fn echo_path(
        Path(ManifestGetRequest { repository, tag }): Path<ManifestGetRequest>,
    ) -> Result<Response, RegistryError> {
        let resp = format!("{repository}:{tag}");
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(resp))?)
    }

    #[tokio::test]
    async fn test_uri_rewriting() {
        let app = Router::new().route(
            "/v2/{repository}/manifests/{tag}",
            axum::routing::get(echo_path),
        );

        let app = tower::ServiceBuilder::new()
            .layer(RewriteUriLayer)
            .service(app);

        let req = Request::builder()
            .uri("/v2/some/repo/manifests/latest")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        println!("{:?}", body);
        assert_eq!(&body[..], b"some/repo:latest");
    }
}
