use actix_web::web::Data;
use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};

use crate::app::RegistryApp;

#[derive(Clone, Debug)]
pub struct StorageMetrics {
    pub hs_index: Gauge,
    pub hs_term: Gauge,
    pub applied_index: Gauge,
    pub first_index: Gauge,
    pub last_index: Gauge,
    pub snapshot_index: Gauge,
    pub snapshot_term: Gauge,
    pub flushed_bytes: Counter,
}

impl StorageMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let registry = registry.sub_registry_with_prefix("distribd_storage");

        let hs_index = Gauge::default();
        registry.register("commit_index", "The most recently commit", hs_index.clone());

        let hs_term = Gauge::default();
        registry.register("term", "The most recent term", hs_term.clone());

        let applied_index = Gauge::default();
        registry.register(
            "applied_index",
            "The latest applied log entry in the journal",
            applied_index.clone(),
        );

        let first_index = Gauge::default();
        registry.register(
            "first_index",
            "The first log entry in the journal",
            first_index.clone(),
        );

        let last_index = Gauge::default();
        registry.register(
            "last_index",
            "The last log entry in the journal",
            last_index.clone(),
        );

        let snapshot_index = Gauge::default();
        registry.register(
            "snapshot_index",
            "The last log entry of the most recent snapshot",
            snapshot_index.clone(),
        );

        let snapshot_term = Gauge::default();
        registry.register(
            "snapshot_term",
            "The term of the most recent snapshot",
            snapshot_term.clone(),
        );

        let flushed_bytes = Counter::default();
        registry.register(
            "flushed_bytes",
            "Journal data flushed to disk",
            flushed_bytes.clone(),
        );

        Self {
            hs_index,
            hs_term,
            applied_index,
            first_index,
            last_index,
            snapshot_index,
            snapshot_term,
            flushed_bytes,
        }
    }
}

pub(crate) fn start_watching_metrics(app: Data<RegistryApp>) {
    let mut receiver = app.raft.metrics();

    tokio::spawn(async move {
        while let Ok(_) = receiver.changed().await {
            let metrics = receiver.borrow().clone();

            let mout = app.store.metrics.clone();

            if let Some(last_applied) = metrics.last_applied {
                mout.applied_index
                    .set(last_applied.index.clone().try_into().unwrap());
            }
        }
    });
}
