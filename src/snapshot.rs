use std::sync::Arc;

use log::info;
use raft::Storage;
use tokio::select;

use crate::app::RegistryApp;

pub async fn do_snapshot(app: Arc<RegistryApp>) {
    let mut lifecycle = app.subscribe_lifecycle();

    loop {
        select! {
            _ = tokio::time::sleep(core::time::Duration::from_secs(60)) => {},
            Ok(_ev) = lifecycle.recv() => {
                info!("Snapshotter: Graceful shutdown");
                return;
            }
        };

        let mut group = app.group.write().await;
        let store = group.mut_store();
        if store.snapshot_metadata.index < store.applied_index {
            info!("Snapshotter: Snapshot is stale - updating");
            store.store_snapshot().await;
        }
    }
}
