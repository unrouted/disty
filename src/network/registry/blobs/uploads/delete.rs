use crate::{app::RegistryApp, types::RepositoryName};
use actix_web::http::StatusCode;
use actix_web::{
    delete,
    web::{Data, Path},
    HttpRequest, HttpResponse, HttpResponseBuilder,
};
use serde::Deserialize;

/*
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
                let body = crate::registry::utils::simple_oci_error(
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
                let body = crate::registry::utils::simple_oci_error(
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
*/

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
    upload_id: String,
}

#[delete("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn delete(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobUploadRequest>,
) -> HttpResponse {
    /*
    let app: &RegistryApp = app.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_push_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    let filename = get_upload_path(&app.settings.storage, &upload_id);

    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    if let Err(err) = tokio::fs::remove_file(filename).await {
        warn!("Error whilst deleting file: {err:?}");
        return Responses::UploadInvalid {};
    }

    Responses::Ok {}
    */

    HttpResponseBuilder::new(StatusCode::ACCEPTED).finish()
}
