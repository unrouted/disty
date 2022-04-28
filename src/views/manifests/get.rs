use crate::headers::Token;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_manifest_path;
use log::debug;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::tokio::fs::File;
use rocket::State;
use std::io::Cursor;

pub(crate) enum Responses {
    MustAuthenticate {
        challenge: String,
    },
    AccessDenied {},
    ManifestNotFound {},
    Ok {
        digest: Digest,
        content_type: String,
        content_length: u64,
        file: File,
    },
}

/*
200 OK
Docker-Content-Digest: <digest>
Content-Type: <media type of manifest>

{
   "name": <name>,
   "tag": <tag>,
   "fsLayers": [
      {
         "blobSum": "<digest>"
      },
      ...
    ]
   ],
   "history": <v1 images>,
   "signature": <JWS>
}
*/

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::MustAuthenticate { challenge } => {
                let body = crate::views::utils::simple_oci_error(
                    "UNAUTHORIZED",
                    "authentication required",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .header(Header::new("Www-Authenticate", challenge))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::Unauthorized)
                    .ok()
            }
            Responses::AccessDenied {} => {
                let body = crate::views::utils::simple_oci_error(
                    "DENIED",
                    "requested access to the resource is denied",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::Forbidden)
                    .ok()
            }
            Responses::ManifestNotFound {} => {
                let content_type = Header::new("Content-Type", "application/json; charset=utf-8");

                Response::build()
                    .header(content_type)
                    .status(Status::NotFound)
                    .ok()
            }
            Responses::Ok {
                content_type,
                content_length,
                digest,
                file,
            } => {
                let content_type = Header::new("Content-Type", content_type);
                let digest = Header::new("Docker-Content-Digest", digest.to_string());

                Response::build()
                    .header(content_type)
                    .header(digest)
                    .status(Status::Ok)
                    .sized_body(Some(content_length as usize), file)
                    .ok()
            }
        }
    }
}

#[get("/<repository>/manifests/<digest>")]
pub(crate) async fn get(
    repository: RepositoryName,
    digest: Digest,
    state: &State<RegistryState>,
    token: Token,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_pull_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        return Responses::AccessDenied {};
    }

    if !state.is_manifest_available(&repository, &digest) {
        return Responses::ManifestNotFound {};
    }

    let manifest = match state.get_manifest(&repository, &digest) {
        Some(manifest) => manifest,
        _ => {
            debug!("Failed to return manifest from graph for {digest} (via {repository}");

            return Responses::ManifestNotFound {};
        }
    };

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => return Responses::ManifestNotFound {},
    };

    let content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => return Responses::ManifestNotFound {},
    };

    let path = get_manifest_path(&state.repository_path, &digest);
    if !path.is_file() {
        return Responses::ManifestNotFound {};
    }

    match File::open(path).await {
        Ok(file) => Responses::Ok {
            content_type,
            content_length,
            digest,
            file,
        },
        _ => Responses::ManifestNotFound {},
    }
}

#[get("/<repository>/manifests/<tag>", rank = 2)]
pub(crate) async fn get_by_tag(
    repository: RepositoryName,
    tag: String,
    state: &State<RegistryState>,
    token: Token,
) -> Responses {
    let state: &RegistryState = state.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_pull_challenge(repository),
        };
    }

    if !token.has_permission(&repository, "pull") {
        debug!("User does not have permission");
        return Responses::AccessDenied {};
    }

    let digest = match state.get_tag(&repository, &tag) {
        Some(tag) => tag,
        None => {
            debug!("No such tag");
            return Responses::ManifestNotFound {};
        }
    };

    if !state.is_manifest_available(&repository, &digest) {
        debug!("Manifest not known to graph");
        return Responses::ManifestNotFound {};
    }

    let manifest = match state.get_manifest(&repository, &digest) {
        Some(manifest) => manifest,
        _ => {
            debug!("Could not retrieve manifest info from graph");
            return Responses::ManifestNotFound {};
        }
    };

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            debug!("Could not extract content type from graph");
            return Responses::ManifestNotFound {};
        }
    };

    let content_length = match manifest.size {
        Some(content_length) => content_length,
        _ => {
            debug!("Could not extract content length from graph");
            return Responses::ManifestNotFound {};
        }
    };

    let path = get_manifest_path(&state.repository_path, &digest);
    if !path.is_file() {
        debug!("Expected manifest file does not exist");
        return Responses::ManifestNotFound {};
    }

    match File::open(path).await {
        Ok(file) => Responses::Ok {
            content_type,
            content_length,
            digest,
            file,
        },
        _ => {
            debug!("Could not open file");
            Responses::ManifestNotFound {}
        }
    }
}
