use actix_web::http::StatusCode;
use actix_web::{HttpResponse, HttpResponseBuilder, ResponseError};

use crate::network::registry::utils::simple_oci_error;
use crate::types::RepositoryName;

#[derive(Debug)]
pub(crate) enum RegistryError {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
    ManifestNotFound {},
    ManifestInvalid {},
    DigestInvalid {},
    BlobNotFound {},
    UploadNotFound {},
    UploadInvalid {},
    RangeNotSatisfiable {
        repository: RepositoryName,
        upload_id: String,
        size: u64,
    },
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Customize so only `x` and `y` are denoted.
        write!(f, "{:?}", self)
    }
}

impl ResponseError for RegistryError {
    fn error_response(&self) -> HttpResponse<actix_web::body::BoxBody> {
        match self {
            Self::MustAuthenticate { challenge } => {
                let body = simple_oci_error("UNAUTHORIZED", "authentication required");

                HttpResponseBuilder::new(StatusCode::UNAUTHORIZED)
                    .append_header(("Www-Authenticate", challenge.clone()))
                    .body(body)
            }
            Self::AccessDenied {} => {
                let body = simple_oci_error("DENIED", "requested access to the resource is denied");

                HttpResponseBuilder::new(StatusCode::FORBIDDEN).body(body)
            }
            Self::ManifestNotFound {} => {
                let body = simple_oci_error("MANIFEST_NOT_FOUND", "manifest not found");

                HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(body)
            }
            Self::ManifestInvalid {} => {
                let body = simple_oci_error("MANIFEST_INVALID", "upload was invalid");

                HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(body)
            }
            Self::DigestInvalid {} => {
                let body = simple_oci_error(
                    "DIGEST_INVALID",
                    "provided digest did not match uploaded content",
                );

                HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(body)
            }
            Self::BlobNotFound {} => {
                let body = simple_oci_error("BLOB_NOT_FOUND", "blob not found");

                HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(body)
            }
            Self::UploadNotFound {} => {
                let body =
                    simple_oci_error("BLOB_UPLOAD_UNKNOWN", "blob upload unknown to registry");

                HttpResponseBuilder::new(StatusCode::NOT_FOUND).body(body)
            }
            Self::UploadInvalid {} => {
                let body = simple_oci_error("BLOB_UPLOAD_INVALID", "the upload was invalid");

                HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(body)
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

                let range_end = if size > &0 { size - 1 } else { 0 };

                HttpResponseBuilder::new(StatusCode::RANGE_NOT_SATISFIABLE)
                    .append_header((
                        "Location",
                        format!("/v2/{repository}/blobs/uploads/{upload_id}"),
                    ))
                    .append_header(("Range", format!("0-{range_end}")))
                    .append_header(("Content-Length", "0"))
                    .append_header(("Blob-Upload-Session-ID", upload_id.clone()))
                    .append_header(("Docker-Upload-UUID", upload_id.clone()))
                    .finish()
            }
        }
    }
}
