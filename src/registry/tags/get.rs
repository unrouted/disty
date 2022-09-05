use crate::app::RegistryApp;
use crate::headers::Token;
use crate::types::RepositoryName;
use rocket::get;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use serde_json::json;
use std::io::Cursor;
use std::sync::Arc;

pub(crate) enum Responses {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
    ServiceUnavailable {},
    NoSuchRepository {},
    Ok {
        repository: RepositoryName,
        n: Option<usize>,
        include_link: bool,
        tags: Vec<String>,
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
            Responses::ServiceUnavailable {} => {
                Response::build().status(Status::ServiceUnavailable).ok()
            }
            Responses::NoSuchRepository {} => Response::build().status(Status::NotFound).ok(),
            Responses::Ok {
                repository,
                include_link,
                n,
                tags,
            } => {
                let mut builder = Response::build();

                let body = json!(
                    {
                        "name": repository,
                        "tags": tags,
                    }
                )
                .to_string();

                builder.header(Header::new("Content-Length", body.len().to_string()));

                if include_link {
                    let mut fragments = vec![];

                    if let Some(tag) = tags.last() {
                        fragments.push(format!("last={tag}"))
                    }

                    if let Some(n) = n {
                        fragments.push(format!("n={n}"))
                    }

                    let suffix = if !fragments.is_empty() {
                        let joined = fragments.join("&");
                        format!("?{joined}")
                    } else {
                        "".to_string()
                    };

                    builder.header(Header::new(
                        "Link",
                        format!("/v2/{repository}/tags/list{suffix}; rel=\"next\""),
                    ));
                }

                builder
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::Ok)
                    .ok()
            }
        }
    }
}

#[get("/<repository>/tags/list?<last>&<n>")]
pub(crate) async fn get(
    repository: RepositoryName,
    last: Option<String>,
    n: Option<usize>,
    app: &State<Arc<RegistryApp>>,
    token: Token,
) -> Responses {
    let app: &RegistryApp = app.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_pull_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        return Responses::AccessDenied {};
    }

    match app.get_tags(&repository).await {
        Err(_) => return Responses::ServiceUnavailable {},
        Ok(Some(mut tags)) => {
            tags.sort();

            if let Some(last) = last {
                let index = tags.iter().position(|r| r == &last).unwrap();
                tags = tags[index..].to_vec();
            }

            let mut include_link = false;

            if let Some(n) = n {
                if n < tags.len() {
                    include_link = true;
                }
                tags = tags[..n].to_vec();
            }

            Responses::Ok {
                repository,
                include_link,
                n,
                tags,
            }
        }
        Ok(None) => Responses::NoSuchRepository {},
    }
}
