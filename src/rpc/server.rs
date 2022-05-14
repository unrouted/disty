use rocket::{Build, Rocket, Route};

use crate::config::Configuration;

#[post("/append")]
async fn append() -> String {
    "ok".to_string()
}

#[post("/queue")]
pub(crate) async fn queue() -> String {
    "ok".to_string()
}

fn routes() -> Vec<Route> {
    routes![append, queue]
}

fn configure(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount("/", routes())
}

pub(crate) fn start_rpc_server(config: Configuration) {
    let figment = rocket::Config::figment().merge(("port", config.raft.port));

    tokio::spawn(configure(rocket::custom(figment)).launch());
}
