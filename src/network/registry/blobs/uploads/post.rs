use crate::{app::RegistryApp, types::RepositoryName};
use actix_web::http::StatusCode;
use actix_web::{
    web::{Data, Path},
    HttpRequest, HttpResponse, HttpResponseBuilder,
};
use serde::Deserialize;

/*
pub(crate) enum Responses {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
    UploadInvalid {},
    ServiceUnavailable {},
    DigestInvalid {},
    UploadComplete {
        repository: RepositoryName,
        digest: Digest,
    },
    Ok {
        repository: RepositoryName,
        uuid: String,
        size: u64,
    },
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::ServiceUnavailable {} => {
                Response::build().status(Status::ServiceUnavailable).ok()
            }
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
            Responses::UploadComplete { repository, digest } => {
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
            Responses::Ok {
                repository,
                uuid,
                size,
            } => {
                let location = Header::new(
                    "Location",
                    format!("/v2/{}/blobs/uploads/{}", repository, uuid),
                );
                let range = Header::new("Range", format!("0-{size}"));
                let length = Header::new("Content-Length", "0");
                let upload_uuid = Header::new("Docker-Upload-UUID", uuid);

                Response::build()
                    .header(location)
                    .header(range)
                    .header(length)
                    .header(upload_uuid)
                    .status(Status::Accepted)
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

// #[post("/<repository>/blobs/uploads?<mount>&<from>&<digest>", data = "<body>")]
pub(crate) async fn post(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobUploadRequest>,
) -> HttpResponse {
    /*
    if !token.validated_token {
        let mut access = vec![Access {
            repository,
            permissions: HashSet::from(["pull".to_string(), "push".to_string()]),
        }];

        if let Some(from) = from {
            access.push(Access {
                repository: from,
                permissions: HashSet::from(["pull".to_string()]),
            });
        }

        return Responses::MustAuthenticate {
            challenge: token.get_challenge(access),
        };
    }

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    if let (Some(mount), Some(from)) = (mount, from) {
        if from == repository {
            return Responses::UploadInvalid {};
        }

        if !token.has_permission(&from, "pull") {
            return Responses::UploadInvalid {};
        }

        match app.is_blob_available(&from, &mount).await {
            Err(_) => {
                return Responses::ServiceUnavailable {};
            }
            Ok(false) => {}
            Ok(true) => {
                let actions = vec![RegistryAction::BlobMounted {
                    timestamp: Utc::now(),
                    digest: mount.clone(),
                    repository: repository.clone(),
                    user: token.sub.clone(),
                }];

                if !app.submit(actions).await {
                    return Responses::UploadInvalid {};
                }

                return Responses::UploadComplete {
                    repository,
                    digest: mount,
                };
            }
        }
    }

    let upload_id = Uuid::new_v4().as_hyphenated().to_string();

    match digest {
        Some(digest) => {
            let filename = get_upload_path(&app.settings.storage, &upload_id);

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

            match std::fs::rename(filename, dest) {
                Ok(_) => {}
                Err(_) => {
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

                return Responses::UploadComplete { repository, digest };
            }
            _ => {
                // Nothing was uploaded, but a session was started...
                let filename = get_upload_path(&app.settings.storage, &upload_id);

                match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)
                .await
                {
                    Ok(file) => drop(file),
                    _ => return Responses::UploadInvalid {},
                }
            }
        }

        Responses::Ok {
            repository,
            uuid: upload_id,
            size: 0,
        }
        */

    HttpResponseBuilder::new(StatusCode::OK).finish()
}
