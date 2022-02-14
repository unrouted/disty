use crate::headers::ContentRange;
use crate::responses;
use crate::responses::ManifestCreated;
use crate::responses::UploadAccepted;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::Repositories;
use crate::types::RepositoryName;
use ring::digest;
use rocket::data::ByteUnit;
use rocket::data::Data;
use rocket::tokio::fs::File;
use rocket::State;
use rocket::{http::Status, Route};
use std::sync::{Arc, Mutex};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;

pub(crate) struct RegistryDir(String);

#[post("/<repository>/blobs/uploads")]
async fn post_upload(repository: RepositoryName) -> UploadAccepted {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();

    UploadAccepted {
        repository,
        uuid: uuid,
    }
}

#[patch("/<repository>/blobs/uploads/<upload_id>", data = "<body>")]
async fn patch_upload(
    repository: RepositoryName,
    upload_id: String,
    _range: Option<ContentRange>,
    body: Data<'_>,
) -> UploadAccepted {
    let filename = format!("upload/{upload_id}", upload_id = upload_id);

    let mut options = OpenOptions::new();
    let mut file = options
        .append(true)
        .create(true)
        .open(filename)
        .await
        .unwrap();

    let _result = body
        .open(ByteUnit::Megabyte(500))
        .stream_to(&mut tokio::io::BufWriter::new(&mut file))
        .await;

    UploadAccepted {
        repository,
        uuid: upload_id,
    }
}

#[put("/<_repository>/blobs/uploads/<upload_id>?<digest>")]
async fn put_upload(
    _repository: String,
    upload_id: String,
    digest: Digest,
    _range: Option<ContentRange>,
) -> Status {
    // FIXME: Assert push permission

    assert!(digest.algo == "sha256");

    // FIXME: Assert digest is correct

    let filename = format!("upload/{upload_id}", upload_id = upload_id);
    let dest = format!("blobs/{digest}", digest = digest);

    std::fs::rename(filename, dest).unwrap();

    Status::Ok
}

#[delete("/<_repository>/blobs/uploads/<_upload_id>")]
fn delete_upload(_repository: String, _upload_id: String) -> Status {
    Status::NotAcceptable
}

#[get("/<_repository>/blobs/uploads/<_upload_id>")]
fn get_upload(_repository: String, _upload_id: String) -> Status {
    Status::NotAcceptable
}

#[get("/<repository>/blobs/<digest>")]
async fn get_blob(
    repository: RepositoryName,
    digest: Digest,
    state: &State<RegistryState>,
) -> responses::GetBlobResponses {
    let state: &RegistryState = state.inner();

    /*
    images_directory = request.app["images_directory"]
    registry_state = request.app["registry_state"]

    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])
    */

    if !state.is_blob_available(&repository, &digest) {
        return responses::GetBlobResponses::BlobNotFound(responses::BlobNotFound {});
    }

    /*
    if not registry_state.is_blob_available(repository, hash):
    raise exceptions.BlobUnknown(hash=hash)

    hash_path = get_blob_path(images_directory, hash)
    if not hash_path.is_file():
    raise exceptions.BlobUnknown(hash=hash)

    size = os.path.getsize(hash_path)

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": hash,
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size),
        },
        path=hash_path,
    )
    */

    let filename = format!("blobs/{digest}", digest = digest);
    let file = File::open(filename).await.unwrap();

    responses::GetBlobResponses::Blob(responses::Blob {
        content_type: "application/vnd.docker.distribution.manifest.v2+json".to_string(),
        digest,
        file,
    })
}

#[put("/<repository>/manifests/<tag>", data = "<body>")]
async fn put_manifest(repository: RepositoryName, tag: String, body: Data<'_>) -> ManifestCreated {
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

#[get("/<_repository>/manifests/<digest>")]
async fn get_manifest(_repository: RepositoryName, digest: Digest) -> responses::Manifest {
    let filename = format!("manifests/{digest}", digest = digest);
    let file = File::open(filename).await.unwrap();

    responses::Manifest {
        content_type: "application/vnd.docker.distribution.manifest.v2+json".to_string(),
        digest,
        file,
    }
}

pub fn routes() -> Vec<Route> {
    routes![
        // Uploads
        post_upload,
        patch_upload,
        put_upload,
        delete_upload,
        get_upload,
        // Blobs
        get_blob,
        // Manifests
        put_manifest,
        get_manifest,
    ]
}

#[cfg(test)]
mod test {
    use rocket::http::Status;
    use rocket::local::blocking::Client;

    fn client() -> Client {
        let server = rocket::build().mount("/", super::routes());
        Client::tracked(server).expect("valid rocket instance")
    }

    #[test]
    fn put_sha_query_param_fail() {
        let client = client();
        let response = client
            .put("/REPOSITORY/blobs/uploads/UPLOADID?digest=sha255:hello")
            .dispatch();
        assert_eq!(response.status(), Status::NotFound);
    }

    #[test]
    fn put() {}
}
