use std::sync::Arc;

use anyhow::Result;
use tokio::task::JoinSet;
use tracing::debug;

use crate::{notify::Notification, state::RegistryState};

pub(crate) fn start_mirror(
    tasks: &mut JoinSet<Result<()>>,
    state: Arc<RegistryState>,
) -> Result<()> {
    if state.config.nodes.len() <= 1 {
        debug!("not starting mirror as not enough nodes");
        return Ok(());
    }

    tasks.spawn(async move {
        loop {
            let _: Notification = state.client.listen_after_start().await?;

            for blobs in state.get_missing_blobs().await? {}
        }
    });

    Ok(())
}
