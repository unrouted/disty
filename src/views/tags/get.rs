use crate::headers::Token;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use serde_json::json;
use std::io::Cursor;

pub(crate) enum Responses {
    AccessDenied {},
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
    state: &State<RegistryState>,
    token: Token,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !token.has_permission(&repository, &"pull".to_string()) {
        return Responses::AccessDenied {};
    }

    match state.get_tags(&repository) {
        Some(mut tags) => {
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
        None => Responses::NoSuchRepository {},
    }
}
