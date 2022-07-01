/*
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use openraft::error::Infallible;
use openraft::Node;
use openraft::RaftMetrics;

use crate::app::RegistryApp;
use crate::NodeId;
use crate::RegistryTypeConfig;

#[post("/add-learner")]
pub async fn add_learner(
    app: Data<RegistryApp>,
    req: Json<(NodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = Node {
        addr: req.0 .1.clone(),
        ..Default::default()
    };
    let res = app.raft.add_learner(node_id, Some(node), true).await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[post("/change-membership")]
pub async fn change_membership(
    app: Data<RegistryApp>,
    req: Json<BTreeSet<NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, true, false).await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[post("/init")]
pub async fn init(app: Data<RegistryApp>) -> actix_web::Result<impl Responder> {
    let mut nodes = BTreeMap::new();
    nodes.insert(app.id, Node {
        addr: app.addr.clone(),
        data: Default::default(),
    });
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

/// Get the latest metrics of the cluster
#[get("/metrics")]
pub async fn metrics(app: Data<RegistryApp>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<RegistryTypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(res))
}
*/
