use crate::types::Digest;
use rocket::data::ByteUnit;
use rocket::data::Data;
use rocket::tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

pub(crate) async fn upload_part(filename: &std::path::Path, body: Data<'_>) -> bool {
    let result = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&filename)
        .await;

    let file = match result {
        Ok(file) => file,
        _ => {
            return false;
        }
    };

    let result = body
        .open(ByteUnit::Megabyte(500))
        .stream_to(&mut tokio::io::BufWriter::new(file))
        .await;

    matches!(result, Ok(_))
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
