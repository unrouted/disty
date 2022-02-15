use crate::types::RegistryState;
use crate::types::RepositoryName;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use crate::headers::ContentRange;

pub(crate) enum Responses {
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
            Responses::AccessDenied {} => Response::build().status(Status::Forbidden).ok(),
            Responses::UploadInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::Ok { repository, upload_id, size } => {
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

    /*
    uploads = images_directory / "uploads"
    */
    let filename = format!("upload/{upload_id}", upload_id = upload_id);

    match range {
        Some(_range) => {
            /*
            size = 0
            if os.path.exists(upload_path):
            size = os.path.getsize(upload_path)
            
            content_range = request.headers["Content-Range"]
            left, right = content_range.split("-")
            
            if int(left) != size:
            raise web.HTTPRequestRangeNotSatisfiable(
                headers={
                    "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
                    "Range": f"0-{size}",
                    "Content-Length": "0",
                    "Blob-Upload-Session-ID": session_id,
                }
            )
            */
        }
        _ => {}
    }

    if !crate::views::utils::upload_part(&filename, body).await {
        return Responses::UploadInvalid {};
    }

    let size = match tokio::fs::metadata(&filename).await {
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
