use actix_web::http::StatusCode;
use actix_web::{get, web, HttpResponse, HttpResponseBuilder};
use prometheus_client::encoding::text::encode;

use crate::app::RegistryApp;

#[get("/metrics")]
async fn metrics(app: web::Data<RegistryApp>) -> HttpResponse {
    let registry = app.registry.lock().unwrap();
    let mut encoded = String::new();
    encode(&mut encoded, &registry).unwrap();
    HttpResponseBuilder::new(StatusCode::OK)
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(encoded)
}

#[get("/healthz")]
pub(crate) async fn healthz() -> HttpResponse {
    HttpResponseBuilder::new(StatusCode::OK).finish()
}

#[cfg(test)]
mod test {
    /*
        use prometheus_client::registry::Registry;
        use rocket::{http::Status, local::blocking::Client};

    fn client() -> Client {
        let server = super::configure(<Registry>::default());
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
    */
}
