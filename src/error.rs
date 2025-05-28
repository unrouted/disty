use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::error;

pub(crate) enum RegistryError {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
    RepositoryNotFound {},
    ManifestNotFound {},
    ManifestInvalid {},
    DigestInvalid {},
    BlobNotFound {},
    UploadNotFound {},
    UploadInvalid {},
    RangeNotSatisfiable {
        repository: String,
        upload_id: String,
        size: u64,
    },
    Unhandled(anyhow::Error),
}

fn format_error(e: &anyhow::Error) -> String {
    let mut s = String::new();
    s.push_str(&format!("{}", e));
    for cause in e.chain().skip(1) {
        s.push_str(&format!("\nCaused by: {}", cause));
    }
    s
}

pub(crate) fn simple_oci_error(code: &str, message: &str) -> Body {
    Body::from(
        serde_json::json!({
            "errors": [{
                "code": code,
                "message": message
            }]
        })
        .to_string(),
    )
}

impl IntoResponse for RegistryError {
    fn into_response(self) -> Response {
        match self {
            Self::MustAuthenticate { challenge } => {
                let body = simple_oci_error("UNAUTHORIZED", "authentication required");

                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("Www-Authenticate", challenge.clone())
                    .body(body)
            }
            Self::AccessDenied {} => {
                let body = simple_oci_error("DENIED", "requested access to the resource is denied");

                Response::builder().status(StatusCode::FORBIDDEN).body(body)
            }
            Self::RepositoryNotFound {} => {
                let body =
                    simple_oci_error("NAME_UNKNOWN", "repository name not known to registry");

                Response::builder().status(StatusCode::NOT_FOUND).body(body)
            }
            Self::ManifestNotFound {} => {
                let body = simple_oci_error("MANIFEST_NOT_FOUND", "manifest not found");

                Response::builder().status(StatusCode::NOT_FOUND).body(body)
            }
            Self::ManifestInvalid {} => {
                let body = simple_oci_error("MANIFEST_INVALID", "upload was invalid");

                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
            }
            Self::DigestInvalid {} => {
                let body = simple_oci_error(
                    "DIGEST_INVALID",
                    "provided digest did not match uploaded content",
                );

                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
            }
            Self::BlobNotFound {} => {
                let body = simple_oci_error("BLOB_NOT_FOUND", "blob not found");

                Response::builder().status(StatusCode::NOT_FOUND).body(body)
            }
            Self::UploadNotFound {} => {
                let body =
                    simple_oci_error("BLOB_UPLOAD_UNKNOWN", "blob upload unknown to registry");

                Response::builder().status(StatusCode::NOT_FOUND).body(body)
            }
            Self::UploadInvalid {} => {
                let body = simple_oci_error("BLOB_UPLOAD_INVALID", "the upload was invalid");

                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
            }
            Self::RangeNotSatisfiable {
                repository,
                upload_id,
                size,
            } => {
                /*
                416 Range Not Satisfiable
                Location: /v2/<name>/blobs/uploads/<uuid>
                Range: 0-<offset>
                Content-Length: 0
                Docker-Upload-UUID: <uuid>
                */

                let range_end = if size > 0 { size - 1 } else { 0 };

                Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(
                        "Location",
                        format!("/v2/{repository}/blobs/uploads/{upload_id}"),
                    )
                    .header("Range", format!("0-{range_end}"))
                    .header("Content-Length", "0")
                    .header("Blob-Upload-Session-ID", upload_id.clone())
                    .header("Docker-Upload-UUID", upload_id.clone())
                    .body(Body::empty())
            }
            Self::Unhandled(err) => {
                error!(
                    error = %format_error(&err),
                    backtrace = ?err.backtrace(),
                    "Registry error"
                );
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
            }
        }
        .unwrap_or_else(|err| {
            let err = err.into();
            error!(
                error = %format_error(&err),
                backtrace = ?err.backtrace(),
                "Registry error"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Body::empty()).into_response()
        })
    }
}

impl<E> From<E> for RegistryError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        let err = err.into();
        println!("{}", format_error(&err));
        error!(
            error = %format_error(&err),
            backtrace = ?err.backtrace(),
            "Registry error"
        );
        Self::Unhandled(err)
    }
}
