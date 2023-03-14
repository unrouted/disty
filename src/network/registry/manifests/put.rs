use crate::app::RegistryApp;
use crate::extractors::Token;
use crate::network::registry::errors::RegistryError;
use crate::registry::utils::get_hash;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::webhook::Event;
use actix_web::http::StatusCode;
use actix_web::put;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::web::Payload;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpResponseBuilder;
use chrono::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ManifestPutRequest {
    repository: RepositoryName,
    tag: String,
}

#[put("/{repository:[^{}]+}/manifests/{tag}")]
pub(crate) async fn put(
    app: Data<RegistryApp>,
    req: HttpRequest,
    path: Path<ManifestPutRequest>,
    body: Payload,
    token: Token,
) -> Result<HttpResponse, RegistryError> {
    let extractor = &app.extractor;

    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_push_challenge(&path.repository),
        });
    }

    if !token.has_permission(&path.repository, "push") {
        return Err(RegistryError::AccessDenied {});
    }

    let upload_path = app.get_temp_path();

    if !crate::registry::utils::upload_part(&upload_path, body).await {
        return Err(RegistryError::ManifestInvalid {});
    }

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
            location: "FIXME".to_string(),
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
        _ => {
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

    match std::fs::rename(upload_path, dest) {
        Ok(_) => {}
        Err(_) => {
            return Err(RegistryError::ManifestInvalid {});
        }
    }

    if !app.submit(actions).await {
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
    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .append_header((
            "Location",
            format!("/v2/{}/manifests/{}", path.repository, digest),
        ))
        .append_header(("Docker-Content-Digest", digest.to_string()))
        .finish())
}
