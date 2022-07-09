use std::sync::Arc;

use anyhow::Context;
use log::{error, info};
use tokio::select;

use crate::app::RegistryApp;

pub async fn do_snapshot(app: Arc<RegistryApp>) -> anyhow::Result<()> {
    let mut lifecycle = app.subscribe_lifecycle();

    loop {
        select! {
            _ = tokio::time::sleep(core::time::Duration::from_secs(60)) => {},
            Ok(_ev) = lifecycle.recv() => {
                info!("Snapshotter: Graceful shutdown");
                break;
            }
        };

        let mut group = app.group.write().await;
        let store = group.mut_store();
        if store.snapshot_metadata.index < store.applied_index {
            info!("Snapshotter: Snapshot is stale - updating");
            if let Err(err) = store
                .store_snapshot()
                .await
                .context("Snapshotter: Failed to update snapshot")
            {
                error!("{err:?}");
            }
        }
    }

    Ok(())
}
