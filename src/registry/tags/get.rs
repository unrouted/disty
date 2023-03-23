use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::types::RepositoryName;
use actix_web::http::StatusCode;
use actix_web::web::{Data, Path};
use actix_web::{get, HttpRequest, HttpResponse, HttpResponseBuilder};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct TagRequest {
    repository: RepositoryName,
}

#[derive(Debug, Deserialize)]
pub struct TagQuery {
    last: Option<String>,
    n: Option<usize>,
}

#[get("/{repository:[^{}]+}/tags/list")]
pub(crate) async fn get(
    app: Data<RegistryApp>,
    _req: HttpRequest,
    path: Path<TagRequest>,
    query: actix_web::web::Query<TagQuery>,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_pull_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "pull") {
        return Err(RegistryError::AccessDenied {});
    }

    match app.get_tags(&path.repository) {
        Some(mut tags) => {
            tags.sort();

            if let Some(last) = &query.last {
                let index = tags.iter().position(|r| r == last).unwrap();
                tags = tags[index..].to_vec();
            }

            let mut include_link = false;

            if let Some(n) = query.n {
                if n < tags.len() {
                    include_link = true;
                }
                tags = tags[..n].to_vec();
            }

            let body = json!(
                {
                    "name": path.repository.clone(),
                    "tags": tags,
                }
            )
            .to_string();

            let mut builder = HttpResponseBuilder::new(StatusCode::OK);

            if include_link {
                let mut fragments = vec![];

                if let Some(tag) = tags.last() {
                    fragments.push(format!("last={tag}"))
                }

                if let Some(n) = query.n {
                    fragments.push(format!("n={n}"))
                }

                let suffix = if !fragments.is_empty() {
                    let joined = fragments.join("&");
                    format!("?{joined}")
                } else {
                    "".to_string()
                };

                builder.append_header((
                    "Link",
                    format!("/v2/{}/tags/list{}; rel=\"next\"", path.repository, suffix),
                ));
            }

            Ok(builder.body(body))
        }
        None => Err(RegistryError::RepositoryNotFound {}),
    }
}
