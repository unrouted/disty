use crate::types::Digest;
use rocket::data::ByteUnit;
use rocket::data::Data;
use rocket::tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

pub(crate) async fn upload_part(filename: &String, body: Data<'_>) -> bool {
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

    match result {
        Ok(_) => true,
        _ => false,
    }
}

pub(crate) async fn validate_hash(filename: &String, expected_hash: &Digest) -> bool {
    let actual_digest = match File::open(&filename).await {
        Ok(file) => {
            let mut buffer = [0; 1024];
            let mut reader = tokio::io::BufReader::new(file);
            let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

            loop {
                let len = match reader.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(size) => size,
                    _ => {
                        return false;
                    }
                };
                hasher.update(&buffer[..len]);
            }

            Digest::from_sha256(&hasher.finish())
        }
        _ => {
            return false;
        }
    };

    &actual_digest == expected_hash
}
