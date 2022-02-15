use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use rocket::data::ByteUnit;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::tokio::fs::File;
use rocket::State;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

pub(crate) enum Responses {
    AccessDenied {},
    DigestInvalid {},
    UploadInvalid {},
    Ok {
        repository: RepositoryName,
        digest: Digest,
    },
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::AccessDenied {} => Response::build().status(Status::Forbidden).ok(),
            Responses::DigestInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::UploadInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::Ok { repository, digest } => {
                /*
                204 No Content
                Location: <blob location>
                Content-Range: <start of range>-<end of range, inclusive>
                Content-Length: 0
                Docker-Content-Digest: <digest>
                */

                Response::build()
                    .header(Header::new(
                        "Location",
                        format!("/v2/{repository}/blobs/{digest}"),
                    ))
                    .header(Header::new("Range", "0-0"))
                    .header(Header::new("Content-Length", "0"))
                    .header(Header::new("Docker-Content-Digest", digest.to_string()))
                    .status(Status::NoContent)
                    .ok()
            }
        }
    }
}

#[put("/<repository>/blobs/uploads/<upload_id>?<digest>", data = "<body>")]
pub(crate) async fn put(
    repository: RepositoryName,
    upload_id: String,
    digest: Digest,
    state: &State<RegistryState>,
    body: Data<'_>,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !state.check_token(&repository, &"push".to_string()) {
        return Responses::AccessDenied {};
    }

    if false {
        return Responses::UploadInvalid {};
    }

    if digest.algo != "sha256" {
        return Responses::UploadInvalid {};
    }

    let filename = format!("upload/{upload_id}", upload_id = upload_id);
    /*
    uploads = images_directory / "uploads"
    if not uploads.exists():
        os.makedirs(uploads)
        upload_path = uploads / session_id
    */

    let dest = get_blob_path(&state.repository_path, &digest);

    // Actually upload a chunk of file
    let mut options = OpenOptions::new();
    let mut file = options
        .append(true)
        .create(true)
        .open(&filename)
        .await
        .unwrap();

    let _result = body
        .open(ByteUnit::Megabyte(500))
        .stream_to(&mut tokio::io::BufWriter::new(&mut file))
        .await;

    // Validate upload
    let actual_digest = match File::open(&dest).await {
        Ok(file) => {
            let mut buffer = [0; 1024];
            let mut reader = tokio::io::BufReader::new(file);
            let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

            loop {
                let len = reader.read(&mut buffer).await.expect("Failed to read");
                if len == 0 {
                    break;
                }

                hasher.update(&buffer[..len]);
            }

            Digest::from_sha256(&hasher.finish())
        }
        _ => {
            return Responses::UploadInvalid {};
        }
    };

    if actual_digest != digest {
        return Responses::DigestInvalid {};
    }

    /*
    blob_path = get_blob_path(images_directory, digest)
    blob_dir = blob_path.parent
    if not blob_dir.exists():
    os.makedirs(blob_dir)
    */

    match std::fs::rename(filename, dest) {
        Ok(_) => {}
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    }

    /*
    send_action = request.app["send_action"]
    identifier = request.app["identifier"]

    success = await send_action(
        [
            {
                "type": RegistryActions.BLOB_MOUNTED,
                "hash": digest,
                "repository": repository,
                "user": request["user"],
            },
            {
                "type": RegistryActions.BLOB_STAT,
                "hash": digest,
                "size": session["size"],
            },
            {
                "type": RegistryActions.BLOB_STORED,
                "hash": digest,
                "location": identifier,
                "user": request["user"],
            },
        ]
    )

    if not success:
        raise exceptions.BlobUploadInvalid()
     */

    Responses::Ok {
        repository,
        digest: actual_digest,
    }
}
