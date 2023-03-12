use crate::types::RepositoryName;
use crate::RegistryApp;
use actix_web::http::StatusCode;
use actix_web::put;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use serde::Deserialize;

/*
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
            Responses::DigestInvalid {} => {
                let body = crate::registry::utils::simple_oci_error(
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
                let body = crate::registry::utils::simple_oci_error(
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
*/

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
    upload_id: String,
}

// #[put("/<repository>/blobs/uploads/<upload_id>?<digest>", data = "<body>")]
#[put("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn put(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobUploadRequest>,
) -> HttpResponse {
    /*

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

    let filename = get_upload_path(&app.settings.storage, &upload_id);

    if !filename.is_file() {
        return Responses::UploadInvalid {};
    }

    if !crate::registry::utils::upload_part(&filename, body).await {
        return Responses::UploadInvalid {};
    }

    // Validate upload
    if !crate::registry::utils::validate_hash(&filename, &digest).await {
        return Responses::DigestInvalid {};
    }

    let dest = get_blob_path(&app.settings.storage, &digest);

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
            location: app.settings.identifier.clone(),
            user: token.sub.clone(),
        },
        ];

        if !app.submit(actions).await {
            return Responses::UploadInvalid {};
        }

        Responses::Ok { repository, digest }
        */

    HttpResponseBuilder::new(StatusCode::ACCEPTED).finish()
}
