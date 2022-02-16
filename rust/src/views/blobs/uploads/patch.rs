use crate::headers::ContentRange;
use crate::headers::Token;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_upload_path;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
pub(crate) enum Responses {
    AccessDenied {},
    UploadInvalid {},
    RangeNotSatisfiable {
        repository: RepositoryName,
        upload_id: String,
        size: u64,
    },
    Ok {
        repository: RepositoryName,
        upload_id: String,
        size: u64,
    },
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::AccessDenied {} => Response::build().status(Status::Forbidden).ok(),
            Responses::UploadInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::RangeNotSatisfiable {
                repository,
                upload_id,
                size,
            } => {
                /*
                416 Range Not Satisfiable
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
                    .status(Status::RangeNotSatisfiable)
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

#[patch("/<repository>/blobs/uploads/<upload_id>", data = "<body>")]
pub(crate) async fn patch(
    repository: RepositoryName,
    upload_id: String,
    range: Option<ContentRange>,
    state: &State<RegistryState>,
    token: &State<Token>,
    body: Data<'_>,
) -> Responses {
    let state: &RegistryState = state.inner();

    let token: &Token = token.inner();
    if !token.has_permission(&repository, &"push".to_string()) {
        return Responses::AccessDenied {};
    }

    let filename = get_upload_path(&state.repository_path, &upload_id);

    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    match range {
        Some(range) => {
            let size = match tokio::fs::metadata(&filename).await {
                Ok(value) => value.len(),
                _ => 0,
            };

            if range.start != size {
                return Responses::RangeNotSatisfiable {
                    repository,
                    upload_id,
                    size,
                };
            }
        }
        _ => {}
    }

    if !crate::views::utils::upload_part(&filename, body).await {
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
