use std::sync::Arc;

use crate::{
    error::RegistryError,
    extractor::Report,
    registry::utils::{get_hash, upload_part},
    state::RegistryState,
    webhook::Event,
};
use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::StatusCode,
    response::Response,
};
use axum_extra::TypedHeader;
use headers::ContentType;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ManifestPutRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn put(
    Path(ManifestPutRequest { repository, tag }): Path<ManifestPutRequest>,
    content_type: TypedHeader<ContentType>,
    State(registry): State<Arc<RegistryState>>,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    let extractor = &registry.extractor;

    /*if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }*/

    let upload_path = app.get_temp_path();

    upload_part(&upload_path, body.into_body().into_data_stream()).await?;

    let size = match tokio::fs::metadata(&upload_path).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Err(RegistryError::ManifestInvalid {});
        }
    };

    let digest = match get_hash(&upload_path).await {
        Some(digest) => digest,
        _ => {
            return Err(RegistryError::ManifestInvalid {});
        }
    };

    let extracted = extractor
        .extract(
            &registry,
            &repository,
            &digest,
            &content_type.to_string(),
            &upload_path,
        )
        .await;

    let extracted = match extracted {
        Ok(extracted_actions) => extracted_actions,
        Err(e) => {
            tracing::error!("Extraction failed: {:?}", e);
            return Err(RegistryError::ManifestInvalid {});
        }
    };

    actions.append(&mut vec![RegistryAction::HashTagged {
        timestamp: Utc::now(),
        repository: path.repository.clone(),
        digest: digest.clone(),
        tag: path.tag.clone(),
        user: token.sub.clone(),
    }]);

    let dest = registry.get_manifest_path(&digest);

    match tokio::fs::rename(upload_path, dest).await {
        Ok(_) => {}
        Err(_) => {
            return Err(RegistryError::ManifestInvalid {});
        }
    }

    registry
        .insert_manifest(&digest, size, &content_type.to_string())
        .await?;

    for report in extracted {
        match report {
            Report::Manifest {
                digest,
                content_type: _,
                dependencies,
            } => {
                registry
                    .insert_manifest_dependencies(&digest, dependencies)
                    .await?;
            }
            _ => {}
        }
    }

    registry
        .webhooks
        .send(&repository, &digest, &tag, &content_type.to_string())
        .await;

    /*
    201 Created
    Location: <url>
    Content-Length: 0
    Docker-Content-Digest: <digest>
    */
    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header(
            "Location",
            format!("/v2/{}/manifests/{}", repository, digest),
        )
        .header("Docker-Content-Digest", digest.to_string())
        .body(Body::empty())?)
}
