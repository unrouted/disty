use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_upload_path;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;

pub(crate) enum Responses {
    AccessDenied {},
    UploadInvalid {},
    Ok {},
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::AccessDenied {} => Response::build().status(Status::Forbidden).ok(),
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
    state: &State<RegistryState>,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !state.check_token(&repository, &"push".to_string()) {
        return Responses::AccessDenied {};
    }

    let filename = get_upload_path(&state.repository_path, &upload_id);

    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    Responses::Ok {}
}
