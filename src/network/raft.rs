use std::sync::Arc;

use openraft::error::{AppendEntriesError, InstallSnapshotError, VoteError};
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotResponse, VoteResponse,
};
use prometheus_client::registry::Registry;
use rocket::serde::json::Json;
use rocket::Route;
use rocket::{post, Build, Rocket, State};

use crate::app::RegistryApp;
use crate::middleware::prometheus::{HttpMetrics, Port};
use crate::NodeId;
use crate::RegistryTypeConfig;

#[post("/raft-vote", data = "<body>")]
async fn vote(
    app: &State<Arc<RegistryApp>>,
    body: Json<VoteRequest<NodeId>>,
) -> Json<Result<VoteResponse<u64>, VoteError<u64>>> {
    let app: &RegistryApp = app.inner();
    let res = app.raft.vote(body.0).await;
    Json(res)
}

#[post("/raft-append", data = "<body>")]
async fn append(
    app: &State<Arc<RegistryApp>>,
    body: Json<AppendEntriesRequest<RegistryTypeConfig>>,
) -> Json<Result<AppendEntriesResponse<u64>, AppendEntriesError<u64>>> {
    let app: &RegistryApp = app.inner();
    let res = app.raft.append_entries(body.0).await;
    Json(res)
}

#[post("/raft-snapshot", data = "<body>")]
async fn snapshot(
    app: &State<Arc<RegistryApp>>,
    body: Json<InstallSnapshotRequest<RegistryTypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<u64>, InstallSnapshotError<u64>>> {
    let app: &RegistryApp = app.inner();
    let res = app.raft.install_snapshot(body.0).await;
    Json(res)
}

fn routes() -> Vec<Route> {
    rocket::routes![vote, append, snapshot]
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
