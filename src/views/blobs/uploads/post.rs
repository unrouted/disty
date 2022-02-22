use crate::headers::Token;
use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use crate::utils::get_upload_path;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use std::io::Cursor;
use uuid::Uuid;
pub(crate) enum Responses {
    AccessDenied {},
    UploadInvalid {},
    DigestInvalid {},
    UploadComplete {
        repository: RepositoryName,
        digest: Digest,
    },
    Ok {
        repository: RepositoryName,
        uuid: String,
        size: u64,
    },
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::AccessDenied {} => {
                let body = crate::views::utils::simple_oci_error(
                    "DENIED",
                    "requested access to the resource is denied",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::Forbidden)
                    .ok()
            }
            Responses::DigestInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::UploadInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::UploadComplete { repository, digest } => {
                /*
                201 Created
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
                    .status(Status::Created)
                    .ok()
            }
            Responses::Ok {
                repository,
                uuid,
                size,
            } => {
                let location = Header::new(
                    "Location",
                    format!("/v2/{}/blobs/uploads/{}", repository, uuid),
                );
                let range = Header::new("Range", format!("0-{size}"));
                let length = Header::new("Content-Length", "0");
                let upload_uuid = Header::new("Docker-Upload-UUID", uuid);

                Response::build()
                    .header(location)
                    .header(range)
                    .header(length)
                    .header(upload_uuid)
                    .status(Status::Accepted)
                    .ok()
            }
        }
    }
}

#[post("/<repository>/blobs/uploads?<mount>&<from>&<digest>", data = "<body>")]
pub(crate) async fn post(
    repository: RepositoryName,
    mount: Option<Digest>,
    from: Option<RepositoryName>,
    digest: Option<Digest>,
    state: &State<RegistryState>,
    token: Token,
    body: Data<'_>,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !token.has_permission(&repository, &"push".to_string()) {
        return Responses::AccessDenied {};
    }

    match (mount, from) {
        (Some(mount), Some(from)) => {
            if from == repository {
                return Responses::UploadInvalid {};
            }

            if !token.has_permission(&from, &"pull".to_string()) {
                return Responses::UploadInvalid {};
            }

            if state.is_blob_available(&from, &mount) {
                let actions = vec![RegistryAction::BlobMounted {
                    digest: mount.clone(),
                    repository: from.clone(),
                    user: token.sub.clone(),
                }];

                if !state.send_actions(actions).await {
                    return Responses::UploadInvalid {};
                }

                return Responses::UploadComplete {
                    repository,
                    digest: mount,
                };
            }
        }
        _ => {}
    }

    let upload_id = Uuid::new_v4().to_hyphenated().to_string();

    match digest {
        Some(digest) => {
            let filename = get_upload_path(&state.repository_path, &upload_id);

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
                    user: token.sub.clone(),
                },
                RegistryAction::BlobStat {
                    digest: digest.clone(),
                    size: stat.len(),
                },
                RegistryAction::BlobStored {
                    digest: digest.clone(),
                    location: "FIXME".to_string(),
                    user: token.sub.clone(),
                },
            ];

            if !state.send_actions(actions).await {
                return Responses::UploadInvalid {};
            }
        }
        _ => {
            // Nothing was uploaded, but a session was started...
            let filename = get_upload_path(&state.repository_path, &upload_id);

            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)
                .await
            {
                Ok(file) => drop(file),
                _ => return Responses::UploadInvalid {},
            }
        }
    }

    Responses::Ok {
        repository,
        uuid: upload_id,
        size: 0,
    }
}
