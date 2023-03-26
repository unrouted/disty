use actix_web::web::Data;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

use crate::app::RegistryApp;

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
pub struct RaftPeerLabels {
    node: u64,
}

#[derive(Clone, Debug)]
pub struct StorageMetrics {
    pub hs_index: Gauge,
    pub hs_term: Gauge,
    pub applied_index: Gauge,
    pub applied_term: Gauge,
    pub matched_term: Family<RaftPeerLabels, Gauge>,
    pub matched_index: Family<RaftPeerLabels, Gauge>,
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

        let applied_term = Gauge::default();
        registry.register(
            "applied_term",
            "The latest applied term",
            applied_term.clone(),
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

        let matched_index = Family::<RaftPeerLabels, Gauge>::default();
        registry.register("matched_index", "Agreed indexes", matched_index.clone());

        let matched_term = Family::<RaftPeerLabels, Gauge>::default();
        registry.register("matched_term", "Agreed terms", matched_term.clone());

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
            applied_term,
            matched_index,
            matched_term,
            snapshot_index,
            snapshot_term,
            flushed_bytes,
        }
    }
}

pub(crate) fn start_watching_metrics(app: Data<RegistryApp>) {
    let mut receiver = app.raft.metrics();

    tokio::spawn(async move {
        while receiver.changed().await.is_ok() {
            let metrics = receiver.borrow().clone();

            let mout = app.store.metrics.clone();

            if let Ok(current_term) = metrics.current_term.try_into() {
                mout.hs_term.set(current_term);
            }

            if let Some(last_log_index) = metrics.last_log_index {
                if let Ok(last_log_index) = last_log_index.try_into() {
                    mout.hs_index.set(last_log_index);
                }
            }

            if let Some(last_applied) = metrics.last_applied {
                if let Ok(last_applied) = last_applied.index.try_into() {
                    mout.applied_index.set(last_applied);
                }

                if let Ok(term) = last_applied.leader_id.term.try_into() {
                    mout.applied_term.set(term);
                }
            }

            if let Some(snapshot) = metrics.snapshot {
                if let Ok(index) = snapshot.index.try_into() {
                    mout.snapshot_index.set(index);
                }

                if let Ok(term) = snapshot.leader_id.term.try_into() {
                    mout.snapshot_term.set(term);
                }
            }

            if let Some(replication) = metrics.replication {
                let labels = RaftPeerLabels { node: metrics.id };

                if let Ok(current_term) = metrics.current_term.try_into() {
                    mout.matched_term.get_or_create(&labels).set(current_term);
                }

                if let Some(last_log_index) = metrics.last_log_index {
                    if let Ok(last_log_index) = last_log_index.try_into() {
                        mout.matched_index
                            .get_or_create(&labels)
                            .set(last_log_index);
                    }
                }

                for (peer, peer_metrics) in &replication.data().replication {
                    let labels = RaftPeerLabels { node: peer.clone() };
                    if let Ok(last_log_index) = peer_metrics.matched().index.try_into() {
                        mout.matched_index
                            .get_or_create(&labels)
                            .set(last_log_index);
                    }

                    if let Ok(term) = peer_metrics.matched().leader_id.term.try_into() {
                        mout.matched_term.get_or_create(&labels).set(term);
                    }
                }
            } else {
                mout.matched_index.clear();
                mout.matched_term.clear();
            }
        }
    });
}
