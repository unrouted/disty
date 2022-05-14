use rocket::serde::json::Json;
use rocket::State;
use rocket::{Build, Rocket, Route};
use tokio::sync::mpsc::Sender;

use crate::{config::Configuration, machine::Envelope};

#[post("/append")]
async fn append() -> String {
    "ok".to_string()
}

#[post("/queue", data = "<envelope>")]
pub(crate) async fn queue(envelope: Json<Envelope>, inbox: &State<Sender<Envelope>>) -> String {
    inbox.send(envelope.0).await;

    "ok".to_string()
}

fn routes() -> Vec<Route> {
    routes![append, queue]
}

fn configure(rocket: Rocket<Build>, inbox: Sender<Envelope>) -> Rocket<Build> {
    rocket.mount("/", routes()).manage(inbox)
}

pub(crate) fn start_rpc_server(config: Configuration, inbox: Sender<Envelope>) {
    let figment = rocket::Config::figment().merge(("port", config.raft.port));

    tokio::spawn(configure(rocket::custom(figment), inbox).launch());
}
