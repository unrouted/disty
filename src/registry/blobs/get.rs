use crate::app::RegistryApp;

use crate::headers::Token;
use crate::types::Digest;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use log::debug;
use log::info;
use rocket::get;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::tokio::fs::File;
use rocket::State;
use std::io::Cursor;
use std::sync::Arc;

pub(crate) enum Responses {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
    BlobNotFound {},
    Ok {
        digest: Digest,
        content_type: String,
        content_length: u64,
        file: File,
    },
}

/*
200 OK
Docker-Content-Digest: <digest>
Content-Type: <media type of blob>

...
*/

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
            Responses::BlobNotFound {} => {
                let content_type = Header::new("Content-Type", "application/json; charset=utf-8");

                Response::build()
                    .header(content_type)
                    .status(Status::NotFound)
                    .ok()
            }
            Responses::Ok {
                content_type,
                content_length,
                digest,
                file,
            } => {
                let content_type = Header::new("Content-Type", content_type);
                let digest = Header::new("Docker-Content-Digest", digest.to_string());

                debug!("Starting stream of {digest} with size {content_length}");

                Response::build()
                    .header(content_type)
                    .header(digest)
                    .status(Status::Ok)
                    .sized_body(Some(content_length as usize), file)
                    .ok()
            }
        }
    }
}

#[get("/<repository>/blobs/<digest>")]
pub(crate) async fn get(
    repository: RepositoryName,
    digest: Digest,
    app: &State<Arc<RegistryApp>>,
    token: Token,
) -> Responses {
    let app: &Arc<RegistryApp> = app.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_pull_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        info!("Token does not have access to perform this action");
        return Responses::AccessDenied {};
    }

    let blob = match app.get_blob(&repository, &digest).await {
        Some(blob) => blob,
        None => {
            info!("get_blob returned no blob found");
            return Responses::BlobNotFound {};
        }
    };

    app.wait_for_blob(&digest).await;

    let content_type = match blob.content_type {
        Some(content_type) => content_type,
        _ => "application/octet-steam".to_string(),
    };

    let content_length = match blob.size {
        Some(content_length) => content_length,
        _ => {
            info!("Blob was present but size not available");
            return Responses::BlobNotFound {};
        }
    };

    let path = get_blob_path(&app.settings.storage, &digest);
    if !path.is_file() {
        info!("Blob was not present on disk");
        return Responses::BlobNotFound {};
    }

    match File::open(path).await {
        Ok(file) => Responses::Ok {
            content_type,
            content_length,
            digest,
            file,
        },
        _ => Responses::BlobNotFound {},
    }
}
