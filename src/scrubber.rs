//! Scrub for manifests and blobs.
//!
//! distribd automatically scrubs data to detect and repair corrupt data.

use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use log::{error, info, warn};
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

fn _cleanup_folder(path: &PathBuf) -> anyhow::Result<()> {
    for child in path.read_dir()? {
        let child = child?;
        let file_type = child.file_type()?;
        if file_type.is_dir() {
            _cleanup_folder(&child.path())?;
        }
    }

    let is_empty = path.read_dir()?.next().is_none();
    if is_empty {
        let modified = path.metadata()?.modified()?;
        let now = SystemTime::now();

        // This can Err if the clock goes backwards.
        // We ignore that and hope that eventually in a later scrub it won't go back
        // At least not enough to trigger an error
        if let Ok(duration) = now.duration_since(modified) {
            if duration > Duration::from_secs(60 * 60) {
                info!("Scrubber: {:?}: Deleting empty folder", path);
                std::fs::remove_dir(path)?;
            }
        }
    }

    Ok(())
}

pub async fn do_scrub_empty_folders(app: &Arc<RegistryApp>) {
    let path = PathBuf::from(&app.settings.storage);

    let blob_directory = path.join("blobs");
    let blob_handle = tokio::task::spawn_blocking(move || {
        if let Err(err) = _cleanup_folder(&blob_directory) {
            error!("Scrubber: Error removing empty blob folders: {:?}", err);
        }
    });

    let manifest_directory = path.join("manifests");
    let manifest_handle = tokio::task::spawn_blocking(move || {
        if let Err(err) = _cleanup_folder(&manifest_directory) {
            error!("Scrubber: Error removing empty manifest folders: {:?}", err);
        }
    });

    if let Err(err) = blob_handle.await {
        error!(
            "Scrubber: Join error removing empty blob folders: {:?}",
            err
        );
    }

    if let Err(err) = manifest_handle.await {
        error!(
            "Scrubber: Join error removing empty manifest folders: {:?}",
            err
        );
    }
}

pub async fn do_scrub(app: Arc<RegistryApp>) -> anyhow::Result<()> {
    if !app.settings.scrubber.enabled {
        info!({ "Scrubber: Disabled in config and will not be started" });
        return Ok(());
    }

    let mut lifecycle = app.subscribe_lifecycle();

    loop {
        let leader_id = app.group.read().await.raft.leader_id;

        if leader_id > 0 {
            do_scrub_pass(&app).await;
            do_scrub_empty_folders(&app).await;
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
