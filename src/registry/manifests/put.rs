use std::sync::Arc;

use crate::{
    error::RegistryError,
    registry::utils::{get_hash, upload_part},
    state::RegistryState,
};
use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ManifestPutRequest {
    repository: String,
    tag: String,
}

pub(crate) async fn put(
    Path(ManifestPutRequest { repository, tag }): Path<ManifestPutRequest>,
    State(registry): State<Arc<RegistryState>>,
    body: Request<Body>,
) -> Result<Response, RegistryError> {
    let extractor = &app.extractor;

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

    let content_type = req.headers().get("content-type").unwrap().to_str().unwrap();

    let extracted = extractor
        .extract(&app, &path.repository, &digest, content_type, &upload_path)
        .await;

    let mut actions = vec![
        RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            digest: digest.clone(),
            repository: path.repository.clone(),
            user: token.sub.clone(),
        },
        RegistryAction::ManifestStored {
            timestamp: Utc::now(),
            digest: digest.clone(),
            location: app.id,
            user: token.sub.clone(),
        },
        RegistryAction::ManifestStat {
            timestamp: Utc::now(),
            digest: digest.clone(),
            size,
        },
    ];

    let extracted = match extracted {
        Ok(extracted_actions) => extracted_actions,
        Err(e) => {
            tracing::error!("Extraction failed: {:?}", e);
            return Err(RegistryError::ManifestInvalid {});
        }
    };
    actions.append(&mut extracted.clone());
    actions.append(&mut vec![RegistryAction::HashTagged {
        timestamp: Utc::now(),
        repository: path.repository.clone(),
        digest: digest.clone(),
        tag: path.tag.clone(),
        user: token.sub.clone(),
    }]);

    let dest = app.get_manifest_path(&digest);

    match tokio::fs::rename(upload_path, dest).await {
        Ok(_) => {}
        Err(_) => {
            return Err(RegistryError::ManifestInvalid {});
        }
    }

    if !app.consistent_write(actions).await {
        tracing::error!("Raft storage failed");
        return Err(RegistryError::ManifestInvalid {});
    }

    let resp = app
        .webhooks
        .send(Event {
            repository: path.repository.clone(),
            digest: digest.clone(),
            tag: path.tag.to_owned(),
            content_type: content_type.to_owned(),
        })
        .await;

    if let Err(err) = resp {
        tracing::error!("Error queueing webhook: {err}");
    }

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
