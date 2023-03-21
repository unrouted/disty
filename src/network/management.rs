use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::HttpResponseBuilder;
use actix_web::Responder;
use chrono::Utc;
use openraft::error::Infallible;
use openraft::BasicNode;
use openraft::RaftMetrics;
use serde::Deserialize;
use web::Json;

use crate::app::RegistryApp;
use crate::types::Blob;
use crate::types::Digest;
use crate::types::Manifest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::RegistryNodeId;

// --- Cluster management

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
#[post("/add-learner")]
pub async fn add_learner(
    app: Data<RegistryApp>,
    req: Json<(RegistryNodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = BasicNode {
        addr: req.0 .1.clone(),
    };
    let res = app.raft.add_learner(node_id, node, true).await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[post("/change-membership")]
pub async fn change_membership(
    app: Data<RegistryApp>,
    req: Json<BTreeSet<RegistryNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, false).await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[post("/init")]
pub async fn init(app: Data<RegistryApp>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let node = BasicNode {
        addr: req.0.clone(),
    };
    let mut nodes = BTreeMap::new();
    nodes.insert(app.id, node);
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

/// Get the latest metrics of the cluster
#[get("/metrics")]
pub async fn metrics(app: Data<RegistryApp>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<RegistryNodeId, BasicNode>, Infallible> = Ok(metrics);
    Ok(Json(res))
}

#[derive(Deserialize)]
pub struct ImportBody {
    blobs: HashMap<Digest, Blob>,
    manifests: HashMap<Digest, Manifest>,
    tags: HashMap<RepositoryName, HashMap<String, Digest>>,
}

/// Import a v2 snapshot
#[post("/import")]
pub async fn import(
    app: Data<RegistryApp>,
    payload: web::Json<ImportBody>,
) -> actix_web::Result<impl Responder> {
    let mut actions = vec![];

    for (digest, blob) in payload.blobs.iter() {
        for repository in blob.repositories.iter() {
            actions.push(RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                digest: digest.clone(),
                repository: repository.clone(),
                user: "$import".to_string(),
            });
        }
        for location in blob.locations.iter() {
            actions.push(RegistryAction::BlobStored {
                timestamp: Utc::now(),
                digest: digest.clone(),
                location: location.clone(),
                user: "$import".to_string(),
            });
        }
        if let (Some(dependencies), Some(content_type)) = (&blob.dependencies, &blob.content_type) {
            actions.push(RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest: digest.clone(),
                dependencies: dependencies.clone(),
                content_type: content_type.clone(),
            });
        }
        if let Some(size) = blob.size {
            actions.push(RegistryAction::BlobStat {
                timestamp: Utc::now(),
                digest: digest.clone(),
                size,
            });
        }
    }
    for (digest, manifest) in payload.manifests.iter() {
        for repository in manifest.repositories.iter() {
            actions.push(RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                digest: digest.clone(),
                repository: repository.clone(),
                user: "$import".to_string(),
            });
        }
        for location in manifest.locations.iter() {
            actions.push(RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                digest: digest.clone(),
                location: location.clone(),
                user: "$import".to_string(),
            });
        }
        if let (Some(dependencies), Some(content_type)) =
            (&manifest.dependencies, &manifest.content_type)
        {
            actions.push(RegistryAction::ManifestInfo {
                timestamp: Utc::now(),
                digest: digest.clone(),
                dependencies: dependencies.clone(),
                content_type: content_type.clone(),
            });
        }
        if let Some(size) = manifest.size {
            actions.push(RegistryAction::ManifestStat {
                timestamp: Utc::now(),
                digest: digest.clone(),
                size,
            });
        }
    }
    for (repo, tags) in payload.tags.iter() {
        for (tag, digest) in tags.iter() {
            actions.push(RegistryAction::HashTagged {
                timestamp: Utc::now(),
                digest: digest.clone(),
                repository: repo.clone(),
                tag: tag.clone(),
                user: "$import".to_string(),
            });
        }
    }

    if !app.submit(actions).await {
        return Ok(HttpResponseBuilder::new(StatusCode::FAILED_DEPENDENCY));
    }

    Ok(HttpResponseBuilder::new(StatusCode::OK))
}
