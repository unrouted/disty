use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_manifest_path;
use crate::views::utils::get_hash;
use rocket::data::Data;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;

pub(crate) enum Responses {
    AccessDenied {},
    UploadInvalid {},
    Ok {
        repository: RepositoryName,
        digest: Digest,
    },
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::AccessDenied {} => Response::build().status(Status::Forbidden).ok(),
            Responses::UploadInvalid {} => Response::build().status(Status::BadRequest).ok(),
            Responses::Ok { repository, digest } => {
                /*
                201 Created
                Location: <url>
                Content-Length: 0
                Docker-Content-Digest: <digest>
                */
                let location = Header::new(
                    "Location",
                    format!("/v2/{}/manifests/{}", repository, digest),
                );
                let length = Header::new("Content-Length", "0");
                let upload_uuid = Header::new("Docker-Content-Digest", digest.to_string());

                Response::build()
                    .header(location)
                    .header(length)
                    .header(upload_uuid)
                    .status(Status::Created)
                    .ok()
            }
        }
    }
}

#[put("/<repository>/manifests/<tag>", data = "<body>")]
pub(crate) async fn put(
    repository: RepositoryName,
    tag: String,
    state: &State<RegistryState>,
    body: Data<'_>,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !state.check_token(&repository, &"push".to_string()) {
        return Responses::AccessDenied {};
    }

    let upload_path = crate::utils::get_temp_path(&state.repository_path);

    /*
    content_type = request.headers.get("Content-Type", "")
    if not content_type:
        raise exceptions.ManifestInvalid(reason="no_content_type")

    content_size = len(manifest)
    */

    /*
    # This makes sure the manifest is somewhat valid
    try:
        direct_deps, dependencies = await recursive_analyze(
            mirrorer, repository, content_type, manifest
        )
    except Exception:
        logger.exception("Error while validating manifest")
        raise
    */

    if !crate::views::utils::upload_part(&upload_path, body).await {
        return Responses::UploadInvalid {};
    }

    let size = match tokio::fs::metadata(&upload_path).await {
        Ok(result) => result.len(),
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    };

    let digest = match get_hash(&upload_path).await {
        Some(digest) => digest,
        _ => {
            return Responses::UploadInvalid {};
        }
    };

    let dest = get_manifest_path(&state.repository_path, &digest);

    match std::fs::rename(upload_path, dest) {
        Ok(_) => {}
        Err(_) => {
            return Responses::UploadInvalid {};
        }
    }

    let actions = vec![
        RegistryAction::ManifestMounted {
            digest: digest.clone(),
            repository: repository.clone(),
            user: "FIXME".to_string(),
        },
        RegistryAction::ManifestStored {
            digest: digest.clone(),
            location: "FIXME".to_string(),
            user: "FIXME".to_string(),
        },
        RegistryAction::ManifestStat {
            digest: digest.clone(),
            size: size,
        },
        RegistryAction::ManifestInfo {
            digest: digest.clone(),
            dependencies: vec![],         // FIXME
            content_type: "".to_string(), // FIXME
        },
        RegistryAction::HashTagged {
            repository: repository.clone(),
            digest: digest.clone(),
            tag: tag,
            user: "FIXME".to_string(),
        },
        ];

        if !state.send_actions(actions).await {
            return Responses::UploadInvalid {};
        }

    /*
    request.app["wh_manager"].send(
        {
            "id": str(uuid.uuid4()),
            "timestamp": "2016-03-09T14:44:26.402973972-08:00",
            "action": "push",
            "target": {
                "mediaType": content_type,
                "size": 708,
                "digest": prefixed_hash,
                "length": 708,
            "repository": repository,
            "url": f"/v2/{repository}/manifests/{prefixed_hash}",
            "tag": tag,
        },
        "request": {
            "id": str(uuid.uuid4()),
            # "addr": "192.168.64.11:42961",
            # "host": "192.168.100.227:5000",
            "method": "PUT",
            # "useragent": "curl/7.38.0",
        },
        "actor": {},
        # "source": {
            #    "addr": "xtal.local:5000",
            #    "instanceID": "a53db899-3b4b-4a62-a067-8dd013beaca4",
            # },
        }
    )
    */

    Responses::Ok { repository, digest }
}
