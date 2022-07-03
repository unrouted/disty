use prometheus_client::registry::Registry;
use rocket::serde::json::Json;
use rocket::Route;
use rocket::{post, Build, Rocket, State};
use std::sync::Arc;

use crate::app::RegistryApp;
use crate::middleware::prometheus::{HttpMetrics, Port};

#[post("/propose", data = "<body>")]
async fn propose(
    app: &State<Arc<RegistryApp>>,
    body: Json<Msg::Proposal>,
) -> Json<Result<bool, Error>> {
    let app: &RegistryApp = app.inner();

    match app.proposals.send(body).await {}

    // let res = app.raft.vote(body.0).await;
    let res = Ok(1);

    Json(res)
}

#[post("/post", data = "<body>")]
async fn post(
    app: &State<Arc<RegistryApp>>,
    body: Json<VoteRequest<NodeId>>,
) -> Json<Result<VoteResponse<u64>, VoteError<u64>>> {
    let app: &RegistryApp = app.inner();
    // let res = app.raft.vote(body.0).await;
    let res = Ok(1);
    Json(res)
}

fn routes() -> Vec<Route> {
    rocket::routes![propose, post]
}

pub(crate) fn configure(
    rocket: Rocket<Build>,
    app: Arc<RegistryApp>,
    registry: &mut Registry,
) -> Rocket<Build> {
    rocket
        .mount("/", routes())
        .manage(app)
        .attach(HttpMetrics::new(registry, Port::Raft))
}

pub(crate) fn launch(app: Arc<RegistryApp>, registry: &mut Registry) {
    let fig = rocket::Config::figment()
        .merge(("port", app.settings.raft.port))
        .merge(("address", app.settings.raft.address.clone()));

    tokio::spawn(configure(rocket::custom(fig), app, registry).launch());
}
