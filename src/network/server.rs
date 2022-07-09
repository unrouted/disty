use log::error;
use prometheus_client::registry::Registry;
use raft::eraftpb::Message;
use rocket::serde::json::Json;
use rocket::Route;
use rocket::{post, Build, Rocket, State};
use std::sync::Arc;

use crate::app::{Msg, RegistryApp};
use crate::middleware::prometheus::{HttpMetrics, Port};
use crate::types::RegistryAction;

#[post("/submit", data = "<body>")]
async fn submit(
    app: &State<Arc<RegistryApp>>,
    body: Json<Vec<RegistryAction>>,
) -> Json<Option<u64>> {
    let app: &RegistryApp = app.inner();

    match app.submit_local(body.to_vec()).await {
        Ok(res) => Json(Some(res)),
        Err(err) => {
            error!("Could not queue incoming raft message: {}", err.to_string());
            return Json(None);
        }
    }
}

#[post("/raft", data = "<body>")]
async fn post(app: &State<Arc<RegistryApp>>, body: Vec<u8>) -> Json<bool> {
    let app: &RegistryApp = app.inner();

    let payload = <Message as protobuf::Message>::parse_from_bytes(&body).unwrap();
    if let Err(err) = app.inbox.send(Msg::Raft(payload)).await {
        error!("Could not queue incoming raft message: {}", err.to_string());
    }

    Json(true)
}

fn routes() -> Vec<Route> {
    rocket::routes![post, submit]
}

pub(crate) fn configure(app: Arc<RegistryApp>, registry: &mut Registry) -> Rocket<Build> {
    let fig = rocket::Config::figment()
        .merge(("port", app.settings.raft.port))
        .merge(("address", app.settings.raft.address.clone()))
        .merge(("limits.bytes", 8 * 1024 * 1024));

    rocket::custom(fig)
        .mount("/", routes())
        .manage(app)
        .attach(HttpMetrics::new(registry, Port::Raft))
}
