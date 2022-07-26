//! Scrub for manifests and blobs.
//!
//! distribd automatically scrubs data to detect and repair corrupt data.

use std::io::ErrorKind;
use std::sync::Arc;

use chrono::Utc;
use log::{info, warn};
use tokio::select;

use crate::app::RegistryApp;
use crate::{
    types::RegistryAction,
    utils::{get_blob_path, get_manifest_path},
};

async fn do_scrub_pass(app: &Arc<RegistryApp>) {
    let mut actions = Vec::new();

    for digest in app.get_all_blobs().await {
        if let Some(blob) = app.get_blob_directly(&digest).await {
            let path = get_blob_path(&app.settings.storage, &digest);

            let metadata = match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    match err.kind() {
                        ErrorKind::NotFound => {
                            info!("Scrubber: Blob {digest} noes not exist on disk; removing location from graph.");

                            actions.push(RegistryAction::BlobUnstored {
                                timestamp: Utc::now(),
                                digest: digest.clone(),
                                location: app.settings.identifier.clone(),
                                user: "$system:scrubber".to_string(),
                            });
                        }
                        _ => {
                            warn!("Scrub: Unable to get metadata for {path:?}: {err:?}");
                        }
                    };
                    continue;
                }
            };

            if Some(metadata.len()) != blob.size {
                warn!(
                    "Scrub: Blob {digest}: Wrong length {} on disk vs {:?} in db",
                    metadata.len(),
                    blob.size
                );
            }
        }
    }

    for digest in app.get_all_manifests().await {
        if let Some(blob) = app.get_manifest_directly(&digest).await {
            let path = get_manifest_path(&app.settings.storage, &digest);

            let metadata = match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    match err.kind() {
                        ErrorKind::NotFound => {
                            info!("Scrubber: Manifest {digest} noes not exist on disk; removing location from graph.");

                            actions.push(RegistryAction::ManifestUnstored {
                                timestamp: Utc::now(),
                                digest: digest.clone(),
                                location: app.settings.identifier.clone(),
                                user: "$system:scrubber".to_string(),
                            });
                        }
                        _ => {
                            warn!("Scrub: Unable to get metadata for {path:?}: {err:?}");
                        }
                    };
                    continue;
                }
            };

            if Some(metadata.len()) != blob.size {
                warn!(
                    "Scrub: Manifest {digest}: Wrong length {} on disk vs {:?} in db",
                    metadata.len(),
                    blob.size
                );
            }
        }
    }
}

pub async fn do_scrub(app: Arc<RegistryApp>) -> anyhow::Result<()> {
    let mut lifecycle = app.subscribe_lifecycle();

    loop {
        let leader_id = app.group.read().await.raft.leader_id;

        if leader_id > 0 {
            do_scrub_pass(&app).await;
        } else {
            info!("Scrub: Skipped as leader not known");
        }

        select! {
            _ = tokio::time::sleep(core::time::Duration::from_secs(60)) => {},
            Ok(_ev) = lifecycle.recv() => {
                info!("Scrub: Graceful shutdown");
                break;
            }
        };
    }

    Ok(())
}
