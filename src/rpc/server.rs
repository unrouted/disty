use std::sync::Arc;

use rocket::serde::json::Json;
use rocket::State;
use rocket::{Build, Rocket, Route};

use crate::raft::{Raft, RaftQueueResult};
use crate::{config::Configuration, machine::Envelope};

#[post("/queue", data = "<envelope>")]
pub(crate) async fn queue(
    envelope: Json<Envelope>,
    raft: &State<Arc<Raft>>,
) -> Json<Result<RaftQueueResult, String>> {
    Json(raft.run_envelope(envelope.0).await)
}

#[post("/run", data = "<envelope>")]
pub(crate) async fn run(
    envelope: Json<Envelope>,
    raft: &State<Arc<Raft>>,
) -> Json<Result<RaftQueueResult, String>> {
    Json(raft.run_envelope(envelope.0).await)
}

fn routes() -> Vec<Route> {
    routes![queue, run]
}

fn configure(rocket: Rocket<Build>, raft: Arc<Raft>) -> Rocket<Build> {
    rocket.mount("/", routes()).manage(raft)
}

pub(crate) fn start_rpc_server(config: Configuration, raft: Arc<Raft>) {
    let figment = rocket::Config::figment().merge(("port", config.raft.port));

    tokio::spawn(configure(rocket::custom(figment), raft).launch());
}
