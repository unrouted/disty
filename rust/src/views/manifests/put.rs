use crate::responses::ManifestCreated;
use crate::types::Digest;
use crate::types::RepositoryName;
use ring::digest;
use rocket::data::ByteUnit;
use rocket::data::Data;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};

#[put("/<repository>/manifests/<_tag>", data = "<body>")]
pub(crate) async fn put(
    repository: RepositoryName,
    _tag: String,
    body: Data<'_>,
) -> ManifestCreated {
    let result = body
        .open(ByteUnit::Megabyte(500))
        .into_bytes()
        .await
        .unwrap()
        .into_inner();

    let digest = Digest::from_sha256(&digest::digest(&digest::SHA256, &result));

    let filename = format!("manifests/{digest}", digest = digest);

    let mut options = OpenOptions::new();
    let file = options
        .write(true)
        .create(true)
        .open(filename)
        .await
        .unwrap();

    {
        let mut writer = BufWriter::new(file);
        writer.write(&result).await.unwrap();
        writer.flush().await.unwrap();
    }

    ManifestCreated { repository, digest }
}
