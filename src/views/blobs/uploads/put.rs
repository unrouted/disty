use crate::config::Configuration;
use crate::headers::Token;
use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use crate::utils::get_upload_path;
use chrono::prelude::*;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use std::io::Cursor;
use std::sync::Arc;

pub(crate) enum Responses {
    MustAuthenticate {
        challenge: String,
    },
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
            Responses::MustAuthenticate { challenge } => {
                let body = crate::views::utils::simple_oci_error(
                    "UNAUTHORIZED",
                    "authentication required",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .header(Header::new("Www-Authenticate", challenge))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::Unauthorized)
                    .ok()
            }
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
            Responses::DigestInvalid {} => {
                let body = crate::views::utils::simple_oci_error(
                    "DIGEST_INVALID",
                    "provided digest did not match uploaded content",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::BadRequest)
                    .ok()
            }
            Responses::UploadInvalid {} => {
                let body = crate::views::utils::simple_oci_error(
                    "BLOB_UPLOAD_INVALID",
                    "the upload was invalid",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::BadRequest)
                    .ok()
            }
            Responses::Ok { repository, digest } => {
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
        }
    }
}

#[put("/<repository>/blobs/uploads/<upload_id>?<digest>", data = "<body>")]
pub(crate) async fn put(
    repository: RepositoryName,
    upload_id: String,
    digest: Digest,
    config: &State<Configuration>,
    state: &State<Arc<RegistryState>>,
    token: Token,
    body: Data<'_>,
) -> Responses {
    let config: &Configuration = config.inner();
    let state: &RegistryState = state.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_push_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    if digest.algo != "sha256" {
        return Responses::UploadInvalid {};
    }

    let filename = get_upload_path(&config.storage, &upload_id);

    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    if !crate::views::utils::upload_part(&filename, body).await {
        return Responses::UploadInvalid {};
    }

    // Validate upload
    if !crate::views::utils::validate_hash(&filename, &digest).await {
        return Responses::DigestInvalid {};
    }

    let dest = get_blob_path(&config.storage, &digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    };

    match std::fs::rename(filename.clone(), dest.clone()) {
        Ok(_) => {}
        Err(_e) => {
            return Responses::UploadInvalid {};
        }
    }

    let actions = vec![
        RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            digest: digest.clone(),
            repository: repository.clone(),
            user: token.sub.clone(),
        },
        RegistryAction::BlobStat {
            timestamp: Utc::now(),
            digest: digest.clone(),
            size: stat.len(),
        },
        RegistryAction::BlobStored {
            timestamp: Utc::now(),
            digest: digest.clone(),
            location: state.machine_identifier.clone(),
            user: token.sub.clone(),
        },
    ];

    if !state.send_actions(actions).await {
        return Responses::UploadInvalid {};
    }

    Responses::Ok { repository, digest }
}
