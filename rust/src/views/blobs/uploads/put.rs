use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;

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

    // FIXME: Check if upload_id is actually permitted
    if false {
        return Responses::UploadInvalid {};
    }

    if digest.algo != "sha256" {
        return Responses::UploadInvalid {};
    }

    /*
    uploads = images_directory / "uploads"
    */
    let filename = format!("upload/{upload_id}", upload_id = upload_id);

    if !crate::views::utils::upload_part(&filename, body).await {
        return Responses::UploadInvalid {};
    }

    // Validate upload
    if !crate::views::utils::validate_hash(&filename, &digest).await {
        return Responses::DigestInvalid {};
    }

    let dest = get_blob_path(&state.repository_path, &digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    };

    match std::fs::rename(filename, dest) {
        Ok(_) => {}
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    }

    let actions = vec![
        RegistryAction::BlobMounted {
            digest: digest.clone(),
            repository: repository.clone(),
            user: "FIXME".to_string(),
        },
        RegistryAction::BlobStat {
            digest: digest.clone(),
            size: stat.len(),
        },
        RegistryAction::BlobStored {
            digest: digest.clone(),
            location: "FIXME".to_string(),
            user: "FIXME".to_string(),
        },
    ];

    if !state.send_actions(actions).await {
        return Responses::UploadInvalid {};
    }

    Responses::Ok { repository, digest }
}
