use std::sync::Arc;

use log::{error, info};
use rocket::fairing::{Fairing, Info, Kind};
use rocket::{Orbit, Rocket};

use crate::app::RegistryApp;

pub(crate) struct Lifecycle {}

#[rocket::async_trait]
impl Fairing for Lifecycle {
    fn info(&self) -> Info {
        Info {
            name: "Distribd lifecycle integration",
            kind: Kind::Liftoff,
        }
    }

    async fn on_liftoff(&self, rocket: &Rocket<Orbit>) {
        let app = rocket
            .state::<Arc<RegistryApp>>()
            .expect("Expect some registry app");
        let mut lifecycle = app.subscribe_lifecycle();

        let shutdown = rocket.shutdown().clone();

        tokio::spawn(async move {
            match lifecycle.recv().await {
                Ok(_) => {
                    info!("HTTP endpoint: Graceful shutdown");
                }
                Err(err) => {
                    error!("{err}");
                }
            }
            shutdown.notify();
        });
    }
}
