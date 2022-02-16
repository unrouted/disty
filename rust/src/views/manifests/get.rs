use crate::headers::Token;
use crate::responses;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_manifest_path;
use rocket::tokio::fs::File;
use rocket::State;
use log::{debug};

#[get("/<repository>/manifests/<digest>")]
pub(crate) async fn get(
    repository: RepositoryName,
    digest: Digest,
    state: &State<RegistryState>,
    token: Token,
) -> responses::GetManifestResponses {
    let state: &RegistryState = state.inner();

    if !token.has_permission(&repository, &"pull".to_string()) {
        return responses::GetManifestResponses::AccessDenied(responses::AccessDenied {});
    }

    if !state.is_manifest_available(&repository, &digest) {
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    let manifest = match state.get_manifest(&repository, &digest) {
        Some(manifest) => manifest,
        _ => {
            debug!("Failed to return manifest from graph for {digest} (via {repository}");

            return responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    };

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            return responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    };

    let path = get_manifest_path(&state.repository_path, &digest);
    if !path.is_file() {
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    match File::open(path).await {
        Ok(file) => responses::GetManifestResponses::Manifest(responses::Manifest {
            content_type,
            digest,
            file,
        }),
        _ => responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {}),
    }
}

#[get("/<repository>/manifests/<tag>", rank = 2)]
pub(crate) async fn get_by_tag(
    repository: RepositoryName,
    tag: String,
    state: &State<RegistryState>,
    token: Token,
) -> responses::GetManifestResponses {
    let state: &RegistryState = state.inner();

    if !token.has_permission(&repository, &"push".to_string()) {
        debug!("User does not have permission");
        return responses::GetManifestResponses::AccessDenied(responses::AccessDenied {});
    }

    let digest = match state.get_tag(&repository, &tag) {
        Some(tag) => tag,
        None => {
            debug!("No such tag");
            return responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    };

    if !state.is_manifest_available(&repository, &digest) {
        debug!("Manifest not known to graph");
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    let manifest = match state.get_manifest(&repository, &digest) {
        Some(manifest) => manifest,
        _ => {
            debug!("Could not retrieve manifest info from graph");
            return responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    };

    let content_type = match manifest.content_type {
        Some(content_type) => content_type,
        _ => {
            debug!("Could not extract content type from graph");
            return responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    };

    let path = get_manifest_path(&state.repository_path, &digest);
    if !path.is_file() {
        debug!("Expected manifest file does not exist");
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    match File::open(path).await {
        Ok(file) => responses::GetManifestResponses::Manifest(responses::Manifest {
            content_type,
            digest,
            file,
        }),
        _ => {
            debug!("Could not open file");
            responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {})
        },
    }
}
