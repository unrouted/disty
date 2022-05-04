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
    MustAuthenticate { challenge: String },
    AccessDenied {},
    UploadInvalid {},
    Ok {},
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
            Responses::UploadInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::Ok {} => {
                /*
                204 No Content
                Content-Length: 0
                */
                Response::build()
                    .header(Header::new("Content-Length", "0"))
                    .status(Status::NoContent)
                    .ok()
            }
        }
    }
}

#[delete("/<repository>/blobs/uploads/<upload_id>")]
pub(crate) async fn delete(
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

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    let filename = get_upload_path(&state.repository_path, &upload_id);

    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    Responses::Ok {}
}
