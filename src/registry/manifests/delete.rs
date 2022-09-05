use crate::app::RegistryApp;
use crate::headers::Token;
use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use chrono::prelude::*;
use rocket::delete;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use std::io::Cursor;
use std::sync::Arc;

pub(crate) enum Responses {
    ServiceUnavailable {},
    MustAuthenticate { challenge: String },
    AccessDenied {},
    NotFound {},
    Failed {},
    Ok {},
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
            Responses::NotFound {} => Response::build().status(Status::NotFound).ok(),
            Responses::Failed {} => Response::build().status(Status::NotFound).ok(),
            Responses::Ok {} => Response::build()
                .header(Header::new("Content-Length", "0"))
                .status(Status::Accepted)
                .ok(),
        }
    }
}
#[delete("/<repository>/manifests/<digest>")]
pub(crate) async fn delete(
    repository: RepositoryName,
    digest: Digest,
    app: &State<Arc<RegistryApp>>,
    token: Token,
) -> Responses {
    let app: &RegistryApp = app.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_push_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    match app.is_manifest_available(&repository, &digest).await {
        Ok(true) => {}
        Ok(false) => {
            return Responses::NotFound {};
        }
        Err(_) => {
            return Responses::ServiceUnavailable {};
        }
    }

    let actions = vec![RegistryAction::ManifestUnmounted {
        timestamp: Utc::now(),
        digest,
        repository,
        user: token.sub.clone(),
    }];

    if !app.submit(actions).await {
        return Responses::Failed {};
    }

    Responses::Ok {}
}

#[delete("/<repository>/manifests/<tag>", rank = 2)]
pub(crate) async fn delete_by_tag(
    repository: RepositoryName,
    tag: String,
    app: &State<Arc<RegistryApp>>,
    token: Token,
) -> Responses {
    let app: &RegistryApp = app.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_push_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "push") {
        return Responses::AccessDenied {};
    }

    let digest = match app.get_tag(&repository, &tag).await {
        Ok(Some(tag)) => tag,
        Ok(None) => return Responses::NotFound {},
        Err(_) => return Responses::ServiceUnavailable {},
    };

    match app.is_manifest_available(&repository, &digest).await {
        Ok(true) => {}
        Ok(false) => return Responses::NotFound {},
        Err(_) => return Responses::ServiceUnavailable {},
    }

    let actions = vec![RegistryAction::ManifestUnmounted {
        timestamp: Utc::now(),
        digest,
        repository,
        user: token.sub.clone(),
    }];

    if !app.submit(actions).await {
        return Responses::Failed {};
    }

    Responses::Ok {}
}
