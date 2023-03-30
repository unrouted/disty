use crate::extractors::Token;
use crate::registry::errors::RegistryError;
use crate::registry::utils::upload_part;
use crate::registry::utils::validate_hash;
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
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use chrono::Utc;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlobUploadRequest {
    repository: RepositoryName,
    upload_id: String,
}

#[derive(Debug, Deserialize)]
pub struct BlobUploadPutQuery {
    digest: Digest,
}

#[put("/{repository:[^{}]+}/blobs/uploads/{upload_id}")]
pub(crate) async fn put(
    app: Data<RegistryApp>,
    path: Path<BlobUploadRequest>,
    query: Query<BlobUploadPutQuery>,
    body: Payload,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    if query.digest.algo != "sha256" {
        return Err(RegistryError::UploadInvalid {});
    }

    let filename = app.get_upload_path(&path.upload_id);

    if !filename.is_file() {
        return Err(RegistryError::UploadInvalid {});
    }

    if !upload_part(&filename, body).await {
        return Err(RegistryError::UploadInvalid {});
    }

    // Validate upload
    if !validate_hash(&filename, &query.digest).await {
        return Err(RegistryError::DigestInvalid {});
    }

    let dest = app.get_blob_path(&query.digest);

    let stat = match tokio::fs::metadata(&filename).await {
        Ok(result) => result,
        Err(_) => {
            return Err(RegistryError::UploadInvalid {});
        }
    };

    match tokio::fs::rename(filename.clone(), dest.clone()).await {
        Ok(_) => {}
        Err(_e) => {
            return Err(RegistryError::UploadInvalid {});
        }
    }

    let actions = vec![
        RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            repository: path.repository.clone(),
            user: token.sub.clone(),
        },
        RegistryAction::BlobStat {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            size: stat.len(),
        },
        RegistryAction::BlobStored {
            timestamp: Utc::now(),
            digest: query.digest.clone(),
            location: app.config.identifier.clone(),
            user: token.sub.clone(),
        },
    ];

    if !app.consistent_write(actions).await {
        return Err(RegistryError::UploadInvalid {});
    }

    /*
    201 Created
    Location: <blob location>
    Content-Range: <start of range>-<end of range, inclusive>
    Content-Length: 0
    Docker-Content-Digest: <digest>
    */
    Ok(HttpResponseBuilder::new(StatusCode::CREATED)
        .append_header((
            "Location",
            format!("/v2/{}/blobs/{}", path.repository, query.digest),
        ))
        .append_header(("Range", "0-0"))
        .append_header(("Content-Length", "0"))
        .append_header(("Docker-Content-Digest", query.digest.to_string()))
        .finish())
}
