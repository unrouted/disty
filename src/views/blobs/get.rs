use crate::headers::Token;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use log::info;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::tokio::fs::File;
use rocket::State;
use std::io::Cursor;

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
                let content_length = Header::new("Content-Length", content_length.to_string());

                Response::build()
                    .header(content_type)
                    .header(digest)
                    .header(content_length)
                    .status(Status::Ok)
                    .streamed_body(file)
                    .ok()
            }
        }
    }
}

#[get("/<repository>/blobs/<digest>")]
pub(crate) async fn get(
    repository: RepositoryName,
    digest: Digest,
    state: &State<RegistryState>,
    token: Token,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_pull_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        info!("Token does not have access to perform this action");
        return Responses::AccessDenied {};
    }

    let blob = match state.get_blob(&repository, &digest) {
        Some(blob) => blob,
        None => {
            return Responses::BlobNotFound {};
        }
    };

    if matches!(blob.content_type, None) || matches!(blob.size, None) {
        info!("Blob was present but content_type and size not available");
        return Responses::BlobNotFound {};
    }

    let path = get_blob_path(&state.repository_path, &digest);
    if !path.is_file() {
        return Responses::BlobNotFound {};
    }

    match File::open(path).await {
        Ok(file) => Responses::Ok {
            content_type: blob.content_type.unwrap(),
            content_length: blob.size.unwrap(),
            digest,
            file,
        },
        _ => Responses::BlobNotFound {},
    }
}
