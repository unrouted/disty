use crate::headers::Token;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_upload_path;
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
    UploadInvalid {},
    Ok {
        repository: RepositoryName,
        upload_id: String,
        size: u64,
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
            Responses::Ok {
                repository,
                upload_id,
                size,
            } => {
                /*
                204 No Content
                Location: /v2/<name>/blobs/uploads/<uuid>
                Range: 0-<offset>
                Content-Length: 0
                Docker-Upload-UUID: <uuid>
                */

                let range_end = size - 1;

                Response::build()
                    .header(Header::new(
                        "Location",
                        format!("/v2/{repository}/blobs/uploads/{upload_id}"),
                    ))
                    .header(Header::new("Range", format!("0-{range_end}")))
                    .header(Header::new("Content-Length", "0"))
                    .header(Header::new("Blob-Upload-Session-ID", upload_id.clone()))
                    .header(Header::new("Docker-Upload-UUID", upload_id))
                    .status(Status::Accepted)
                    .ok()
            }
        }
    }
}

#[get("/<repository>/blobs/uploads/<upload_id>")]
pub(crate) async fn get(
    repository: RepositoryName,
    upload_id: String,
    state: &State<Arc<RegistryState>>,
    token: Token,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_push_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        return Responses::AccessDenied {};
    }

    let filename = get_upload_path(&state.repository_path, &upload_id);
    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    let size = match tokio::fs::metadata(filename).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    };

    Responses::Ok {
        repository,
        upload_id,
        size,
    }
}
