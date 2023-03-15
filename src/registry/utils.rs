use crate::types::Digest;
use actix_web::web::Payload;
use futures_util::StreamExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn upload_part(filename: &std::path::Path, mut body: Payload) -> bool {
    let result = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&filename)
        .await;

    let mut file = match result {
        Ok(file) => file,
        _ => {
            return false;
        }
    };

    while let Some(item) = body.next().await {
        let item = item.unwrap();
        println!("Chunk: {:?}", &item);
        if file.write_all(&item).await.is_err() {
            return false;
        };
    }

    true
}

pub(crate) async fn get_hash(filename: &std::path::Path) -> Option<Digest> {
    match File::open(&filename).await {
        Ok(file) => {
            let mut buffer = [0; 1024];
            let mut reader = tokio::io::BufReader::new(file);
            let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

            loop {
                let len = match reader.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(size) => size,
                    _ => {
                        return None;
                    }
                };
                hasher.update(&buffer[..len]);
            }

            Some(Digest::from_sha256(&hasher.finish()))
        }
        _ => None,
    }
}

pub(crate) async fn validate_hash(filename: &std::path::Path, expected_hash: &Digest) -> bool {
    match get_hash(filename).await {
        Some(actual_digest) => &actual_digest == expected_hash,
        None => false,
    }
}

pub(crate) fn simple_oci_error(code: &str, message: &str) -> String {
    serde_json::json!({
        "errors": [{
            "code": code,
            "message": message
        }]
    })
    .to_string()
}
