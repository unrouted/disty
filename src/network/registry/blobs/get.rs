use crate::app::RegistryApp;
use crate::types::Digest;
use crate::types::RepositoryName;
use actix_files::NamedFile;
use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    repository: RepositoryName,
    digest: Digest,
}

/*
200 OK
Docker-Content-Digest: <digest>
Content-Type: <media type of blob>

...
*/

/*
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
*/

#[get("/{repository:[^{}]+}/blobs/{digest}")]
pub(crate) async fn get(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobRequest>,
) -> HttpResponse {
    /*
    let app: &Arc<RegistryApp> = app.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_pull_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        debug!("Token does not have access to perform this action");
        return Responses::AccessDenied {};
    }
    */

    let blob = match app.get_blob(&path.digest).await {
        Some(blob) => blob,
        None => return HttpResponseBuilder::new(StatusCode::NOT_FOUND).finish(),
    };

    if !blob.repositories.contains(&path.repository) {
        tracing::debug!("Blob exists but not in repostitory: {}", path.repository);
        return HttpResponseBuilder::new(StatusCode::NOT_FOUND).finish();
    }

    // app.wait_for_blob(&digest).await;

    let content_type = match blob.content_type {
        Some(content_type) => content_type,
        _ => "application/octet-steam".to_string(),
    };

    let content_length = match blob.size {
        Some(content_length) => content_length,
        _ => {
            tracing::debug!("Blob was present but size not available");
            return HttpResponseBuilder::new(StatusCode::NOT_FOUND).finish();
        }
    };

    let blob_path = app.get_blob_path(&path.digest);
    if !blob_path.is_file() {
        tracing::info!("Blob was not present on disk");
        return HttpResponseBuilder::new(StatusCode::NOT_FOUND).finish();
    }

    let blob = NamedFile::open_async(blob_path)
        .await
        .unwrap()
        .into_response(&req);

    HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Docker-Content-Digest", path.digest.to_string()))
        .body(blob.into_body())
}
