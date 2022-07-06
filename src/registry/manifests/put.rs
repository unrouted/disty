use crate::app::RegistryApp;
use crate::extractor::Extractor;
use crate::headers::ContentType;
use crate::headers::Token;
use crate::registry::utils::get_hash;
use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::utils::get_manifest_path;
use crate::webhook::Event;
// use crate::webhook::Event;
use chrono::prelude::*;
use log::error;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::put;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub(crate) enum Responses {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
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
            Responses::UploadInvalid {} => {
                let body = crate::registry::utils::simple_oci_error(
                    "MANIFEST_INVALID",
                    "upload was invalid",
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
                Location: <url>
                Content-Length: 0
                Docker-Content-Digest: <digest>
                */
                let location = Header::new(
                    "Location",
                    format!("/v2/{}/manifests/{}", repository, digest),
                );
                let length = Header::new("Content-Length", "0");
                let upload_uuid = Header::new("Docker-Content-Digest", digest.to_string());

                Response::build()
                    .header(location)
                    .header(length)
                    .header(upload_uuid)
                    .status(Status::Created)
                    .ok()
            }
        }
    }
}

#[put("/<repository>/manifests/<tag>", data = "<body>")]
pub(crate) async fn put(
    repository: RepositoryName,
    tag: String,
    app: &State<Arc<RegistryApp>>,
    extractor: &State<Extractor>,
    webhook_queue: &State<Sender<Event>>,
    content_type: ContentType,
    token: Token,
    body: Data<'_>,
) -> Responses {
    let app: &RegistryApp = app.inner();
    let extractor: &Extractor = extractor.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_push_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    let upload_path = crate::utils::get_temp_path(&app.settings.storage);

    if !crate::registry::utils::upload_part(&upload_path, body).await {
        return Responses::UploadInvalid {};
    }

    let size = match tokio::fs::metadata(&upload_path).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    };

    let digest = match get_hash(&upload_path).await {
        Some(digest) => digest,
        _ => {
            return Responses::UploadInvalid {};
        }
    };

    let extracted = extractor
        .extract(
            app,
            &repository,
            &digest,
            &content_type.content_type,
            &upload_path,
        )
        .await;

    let mut actions = vec![
        RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            digest: digest.clone(),
            repository: repository.clone(),
            user: token.sub.clone(),
        },
        RegistryAction::ManifestStored {
            timestamp: Utc::now(),
            digest: digest.clone(),
            location: app.settings.identifier.clone(),
            user: token.sub.clone(),
        },
        RegistryAction::ManifestStat {
            timestamp: Utc::now(),
            digest: digest.clone(),
            size,
        },
    ];

    let extracted = match extracted {
        Ok(extracted_actions) => extracted_actions,
        _ => {
            return Responses::UploadInvalid {};
        }
    };
    actions.append(&mut extracted.clone());
    actions.append(&mut vec![RegistryAction::HashTagged {
        timestamp: Utc::now(),
        repository: repository.clone(),
        digest: digest.clone(),
        tag: tag.clone(),
        user: token.sub.clone(),
    }]);

    let dest = get_manifest_path(&app.settings.storage, &digest);

    match std::fs::rename(upload_path, dest) {
        Ok(_) => {}
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    }

    if !app.submit(actions).await {
        return Responses::UploadInvalid {};
    }

    let webhook_queue: &Sender<Event> = webhook_queue.inner();
    let resp = webhook_queue
        .send(Event {
            repository: repository.clone(),
            digest: digest.clone(),
            tag,
            content_type: content_type.content_type,
        })
        .await;

    if let Err(err) = resp {
        error!("Error queueing webhook: {err}");
    }

    Responses::Ok { repository, digest }
}
