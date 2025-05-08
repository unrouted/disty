use anyhow::Result;
use axum::body::BodyDataStream;
use futures_util::StreamExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn upload_part(
    filename: &std::path::Path,
    mut body: BodyDataStream,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&filename)
        .await?;

    while let Some(item) = body.next().await {
        let item = item?;
        file.write_all(&item).await?;
    }

    file.sync_all().await?;

    Ok(())
}
