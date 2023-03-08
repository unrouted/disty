use std::time::Instant;

use prometheus_client::encoding::{EncodeLabelValue,EncodeLabelSet};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use rocket::request::Request;
use rocket::response::Response;
use rocket::{
    fairing::{Fairing, Info, Kind},
    Data,
};

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

pub(crate) struct HttpMetrics {
    http_requests_total: Family<HttpRequestLabels, Counter>,
    http_requests_duration_seconds: Family<HttpRequestLabels, Histogram>,
    port: Port,
}

impl HttpMetrics {
    pub fn new(registry: &mut Registry, port: Port) -> Self {
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

        HttpMetrics {
            http_requests_total,
            http_requests_duration_seconds,
            port,
        }
    }
}

struct TimerStart(Option<Instant>);

#[rocket::async_trait]
impl Fairing for HttpMetrics {
    fn info(&self) -> Info {
        Info {
            name: "Prometheus metric collection",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, _: &mut Data<'_>) {
        request.local_cache(|| TimerStart(Some(Instant::now())));
    }

    async fn on_response<'r>(&self, request: &'r Request<'_>, response: &mut Response<'r>) {
        // Don't touch metrics if the request didn't match a route.
        if request.route().is_none() {
            return;
        }

        let method = match request.method() {
            rocket::http::Method::Get => HttpMethod::Get,
            rocket::http::Method::Post => HttpMethod::Post,
            rocket::http::Method::Put => HttpMethod::Put,
            rocket::http::Method::Patch => HttpMethod::Patch,
            rocket::http::Method::Delete => HttpMethod::Delete,
            rocket::http::Method::Head => HttpMethod::Head,
            rocket::http::Method::Options => HttpMethod::Options,
            rocket::http::Method::Trace => HttpMethod::Trace,
            rocket::http::Method::Connect => HttpMethod::Connect,
        };

        let path = request.route().unwrap().uri.to_string();
        let status = response.status().code.to_string();

        let labels = HttpRequestLabels {
            method,
            path,
            status,
            port: self.port.clone(),
        };

        self.http_requests_total.get_or_create(&labels).inc();

        let start_time = request.local_cache(|| TimerStart(None));
        if let Some(duration) = start_time.0.map(|st| st.elapsed()) {
            let duration_secs = duration.as_secs_f64();
            self.http_requests_duration_seconds
                .get_or_create(&labels)
                .observe(duration_secs);
        }
    }
}
