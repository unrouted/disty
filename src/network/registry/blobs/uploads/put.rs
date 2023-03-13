use crate::network::registry::utils::upload_part;
use crate::network::registry::utils::validate_hash;
use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::RegistryApp;
use actix_web::http::StatusCode;
use actix_web::put;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::web::Payload;
use actix_web::web::Query;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use chrono::Utc;
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

#[derive(Debug, Deserialize)]
pub struct BlobUploadPutQuery {
    digest: Digest,
}

// #[put("/<repository>/blobs/uploads/<upload_id>?<digest>", data = "<body>")]
#[put("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn put(
    app: Data<RegistryApp>,
    path: Path<BlobUploadRequest>,
    query: Query<BlobUploadPutQuery>,
    body: Payload,
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
    */

    if query.digest.algo != "sha256" {
        return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
    }

    let filename = app.get_upload_path(&path.upload_id);

    if !filename.is_file() {
        return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
    }

    if !upload_part(&filename, body).await {
        return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
    }

    // Validate upload
    if !validate_hash(&filename, &query.digest).await {
        return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
    }

    let dest = app.get_blob_path(&query.digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
        }
    };

    match std::fs::rename(filename.clone(), dest.clone()) {
        Ok(_) => {}
        Err(_e) => {
            return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
        }
    }

    let actions = vec![
        RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            repository: path.repository.clone(),
            user: "nobody".to_string(),
        },
        RegistryAction::BlobStat {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            size: stat.len(),
        },
        RegistryAction::BlobStored {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            location: "location".to_string(),
            user: "nobody".to_string(),
        },
    ];

    if !app.submit(actions).await {
        return HttpResponseBuilder::new(StatusCode::NOT_IMPLEMENTED).finish();
    }

    HttpResponseBuilder::new(StatusCode::ACCEPTED).finish()
}
