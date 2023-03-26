use std::{
    future::{ready, Ready},
    time::Instant,
};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use actix_web::{http::Method, web::Data};
use futures_util::future::LocalBoxFuture;
use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{
        counter::Counter,
        family::Family,
        histogram::{exponential_buckets, Histogram},
    },
};

use crate::app::RegistryApp;

// There are two steps in middleware processing.
// 1. Middleware initialization, middleware factory gets called with
//    next service in chain as parameter.
// 2. Middleware's call method gets called with normal request.

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelValue, Debug)]
enum HttpMethod {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
    Options,
    Trace,
    Connect,
}

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelValue, Debug)]
pub enum Port {
    Raft,
    Prometheus,
    Registry,
}

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct HttpRequestLabels {
    port: Port,
    method: HttpMethod,
    path: String,
    status: String,
}

#[derive(Clone)]
pub(crate) struct PrometheusHttpMetrics {
    http_requests_total: Family<HttpRequestLabels, Counter>,
    http_requests_duration_seconds: Family<HttpRequestLabels, Histogram>,
    port: Port,
}

impl PrometheusHttpMetrics {
    pub fn new(app: Data<RegistryApp>, port: Port) -> Self {
        let mut registry = app.registry.lock().unwrap();

        let http_requests_total = Family::<HttpRequestLabels, Counter>::default();
        registry.register(
            "distribd_http_requests",
            "Number of HTTP requests",
            http_requests_total.clone(),
        );

        let http_requests_duration_seconds =
            Family::<HttpRequestLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 2.0, 10))
            });
        registry.register(
            "distribd_http_requests_duration_seconds",
            "Duration of HTTP requests",
            http_requests_total.clone(),
        );

        Self {
            http_requests_total,
            http_requests_duration_seconds,
            port,
        }
    }
}

// Middleware factory is `Transform` trait
// `S` - type of the next service
// `B` - type of response's body
impl<S, B> Transform<S, ServiceRequest> for PrometheusHttpMetrics
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = PrometheusHttpMetricsMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(PrometheusHttpMetricsMiddleware {
            service,
            inner: self.clone(),
        }))
    }
}

pub struct PrometheusHttpMetricsMiddleware<S> {
    service: S,
    inner: PrometheusHttpMetrics,
}

impl<S, B> Service<ServiceRequest> for PrometheusHttpMetricsMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let path = req.path().to_string();
        let method = match *req.method() {
            Method::CONNECT => HttpMethod::Connect,
            Method::DELETE => HttpMethod::Delete,
            Method::GET => HttpMethod::Get,
            Method::HEAD => HttpMethod::Head,
            Method::OPTIONS => HttpMethod::Options,
            Method::PATCH => HttpMethod::Patch,
            Method::POST => HttpMethod::Post,
            Method::PUT => HttpMethod::Put,
            Method::TRACE => HttpMethod::Trace,
            _ => HttpMethod::Trace,
        };
        let start = Instant::now();

        let fut = self.service.call(req);
        let inner = self.inner.clone();

        Box::pin(async move {
            let res = fut.await?;

            let status = res.status().as_u16().to_string();
            let labels = HttpRequestLabels {
                method,
                path,
                status,
                port: inner.port.clone(),
            };

            inner.http_requests_total.get_or_create(&labels).inc();
            inner
                .http_requests_duration_seconds
                .get_or_create(&labels)
                .observe(start.elapsed().as_secs_f64());

            Ok(res)
        })
    }
}
