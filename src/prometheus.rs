use std::sync::{Arc, Mutex};
use std::time::Instant;

use prometheus_client::encoding::text::{encode, Encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use rocket::{
    fairing::{Fairing, Info, Kind},
    Data,
};
use rocket::{Build, Rocket, Route};

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
enum HttpMethod {
    GET,
    HEAD,
    POST,
    PUT,
    PATCH,
    DELETE,
    OPTIONS,
    TRACE,
    CONNECT,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
struct HttpRequestLabels {
    method: HttpMethod,
    path: String,
    status: String,
}

pub(crate) struct HttpMetrics {
    http_requests_total: Family<HttpRequestLabels, Counter>,
    http_requests_duration_seconds: Family<HttpRequestLabels, Histogram>,
}

impl HttpMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let http_requests_total = Family::<HttpRequestLabels, Counter>::default();
        registry.register(
            "distribd_http_requests_total",
            "Number of HTTP requests",
            Box::new(http_requests_total.clone()),
        );

        let http_requests_duration_seconds =
            Family::<HttpRequestLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 2.0, 10))
            });
        registry.register(
            "distribd_http_requests_duration_seconds",
            "Duration of HTTP requests",
            Box::new(http_requests_total.clone()),
        );

        HttpMetrics {
            http_requests_total,
            http_requests_duration_seconds,
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
            rocket::http::Method::Get => HttpMethod::GET,
            rocket::http::Method::Post => HttpMethod::POST,
            rocket::http::Method::Put => HttpMethod::PUT,
            rocket::http::Method::Patch => HttpMethod::PATCH,
            rocket::http::Method::Delete => HttpMethod::DELETE,
            rocket::http::Method::Head => HttpMethod::HEAD,
            rocket::http::Method::Options => HttpMethod::OPTIONS,
            rocket::http::Method::Trace => HttpMethod::TRACE,
            rocket::http::Method::Connect => HttpMethod::CONNECT,
        };

        let path = request.route().unwrap().uri.to_string();
        let status = response.status().code.to_string();

        let labels = HttpRequestLabels {
            method,
            path,
            status,
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

pub(crate) enum Responses {
    Ok {},
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::Ok {} => Response::build().status(Status::Ok).ok(),
        }
    }
}

#[derive(Responder)]
#[response(
    status = 200,
    content_type = "application/openmetrics-text; version=1.0.0; charset=utf-8"
)]
struct Metrics(Vec<u8>);

#[get("/metrics")]
async fn metrics(registry: &State<Arc<Mutex<Registry>>>) -> Metrics {
    let mut encoded = Vec::new();
    encode(&mut encoded, &registry.lock().unwrap()).unwrap();
    Metrics(encoded)
}

#[get("/healthz")]
pub(crate) async fn healthz() -> Responses {
    Responses::Ok {}
}

fn routes() -> Vec<Route> {
    routes![metrics, healthz]
}

pub(crate) fn configure(rocket: Rocket<Build>, registry: Registry) -> Rocket<Build> {
    rocket
        .mount("/", routes())
        .manage(Arc::new(Mutex::new(registry)))
}

#[cfg(test)]
mod test {
    use prometheus_client::registry::Registry;
    use rocket::{http::Status, local::blocking::Client};

    fn client() -> Client {
        let server = super::configure(rocket::build(), <Registry>::default());
        Client::tracked(server).expect("valid rocket instance")
    }

    #[test]
    fn test_404() {
        // check that server is actually 404ing, not just 200ing everything
        let client = client();
        let response = client.get("/404").dispatch();
        assert_eq!(response.status(), Status::NotFound);
    }

    #[test]
    fn test_metrics() {
        let client = client();
        let response = client.get("/metrics").dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert!(matches!(
            response.headers().get_one("Content-Type"),
            Some("application/openmetrics-text; version=1.0.0; charset=utf-8")
        ));
    }

    #[test]
    fn test_healthz() {
        let client = client();
        let response = client.get("/healthz").dispatch();
        assert_eq!(response.status(), Status::Ok);
    }
}
