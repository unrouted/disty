use actix_files::NamedFile;
use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use actix_web::Responder;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use serde::Deserialize;
use web::Json;

use crate::app::RegistryApp;
use crate::registry::errors::RegistryError;
use crate::types::Digest;
use crate::RegistryNodeId;
use crate::RegistryTypeConfig;

// --- Raft communication

#[post("/raft-vote")]
pub async fn vote(
    app: Data<RegistryApp>,
    req: Json<VoteRequest<RegistryNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<RegistryApp>,
    req: Json<AppendEntriesRequest<RegistryTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<RegistryApp>,
    req: Json<InstallSnapshotRequest<RegistryTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}

#[derive(Debug, Deserialize)]
pub struct BlobRequest {
    digest: Digest,
}

#[get("/blobs/{digest}")]
pub(crate) async fn get_blob(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<BlobRequest>,
) -> Result<impl Responder, RegistryError> {
    let blob = match app.get_blob(&path.digest) {
        Some(blob) => blob,
        None => return Err(RegistryError::BlobNotFound {}),
    };

    if !blob.locations.contains(&app.config.identifier) {
        // Nodes should only hit this endpoint if their records indicate we *do* have it
        // So either we removed it again (and likely waiting will fail)
        // Or something is broken
        // Either way, don't block!
        return Err(RegistryError::BlobNotFound {});
    }

    let content_type = match blob.content_type {
        Some(content_type) => content_type,
        _ => "application/octet-steam".to_string(),
    };

    let _content_length = match blob.size {
        Some(content_length) => content_length,
        _ => {
            tracing::debug!("Blob was present but size not available");
            return Err(RegistryError::BlobNotFound {});
        }
    };

    let blob_path = app.get_blob_path(&path.digest);
    if !blob_path.is_file() {
        tracing::info!("Blob was not present on disk");
        return Err(RegistryError::BlobNotFound {});
    }

    let blob = NamedFile::open_async(blob_path)
        .await
        .unwrap()
        .into_response(&req);

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Docker-Content-Digest", path.digest.to_string()))
        .body(blob.into_body()))
}

#[derive(Debug, Deserialize)]
pub struct ManifestGetRequestDigest {
    digest: Digest,
}

#[get("/manifests/{digest}")]
pub(crate) async fn get_manifest(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<ManifestGetRequestDigest>,
) -> Result<HttpResponse, RegistryError> {
    let manifest = match app.get_manifest(&path.digest) {
        Some(manifest) => manifest,
        None => return Err(RegistryError::ManifestNotFound {}),
    };

    if !manifest.locations.contains(&app.config.identifier) {
        // Nodes should only hit this endpoint if their records indicate we *do* have it
        // So either we removed it again (and likely waiting will fail)
        // Or something is broken
        // Either way, don't block!
        return Err(RegistryError::ManifestNotFound {});
    }

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let _content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => {
            return Err(RegistryError::ManifestNotFound {});
        }
    };

    let manifest_path = app.get_manifest_path(&path.digest);
    if !manifest_path.is_file() {
        return Err(RegistryError::ManifestNotFound {});
    }

    let manifest = NamedFile::open_async(manifest_path)
        .await
        .unwrap()
        .into_response(&req);

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .content_type(content_type)
        .append_header(("Docker-Content-Digest", path.digest.to_string()))
        .body(manifest.into_body()))
}
