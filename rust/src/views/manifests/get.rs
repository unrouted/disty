use crate::responses;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_manifest_path;
use rocket::tokio::fs::File;
use rocket::State;

#[get("/<repository>/manifests/<digest>")]
pub(crate) async fn get(
    repository: RepositoryName,
    digest: Digest,
    state: &State<RegistryState>,
) -> responses::GetManifestResponses {
    let state: &RegistryState = state.inner();

    if !state.check_token(&repository, &"pull".to_string()) {
        return responses::GetManifestResponses::AccessDenied(responses::AccessDenied {});
    }

    if !state.is_manifest_available(&repository, &digest) {
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    let manifest = match state.get_manifest(&repository, &digest) {
        Some(manifest) => manifest,
        _ => {
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

#[get("/<repository>/manifests/<tag>")]
pub(crate) async fn get_by_tag(
    repository: RepositoryName,
    tag: String,
    state: &State<RegistryState>,
) -> responses::GetManifestResponses {
    let state: &RegistryState = state.inner();

    if !state.check_token(&repository, &"pull".to_string()) {
        return responses::GetManifestResponses::AccessDenied(responses::AccessDenied {});
    }

    let digest = match state.get_tag(&repository, &tag) {
        Some(tag) => tag,
        None => {
            return responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    };

    if !state.is_manifest_available(&repository, &digest) {
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    let manifest = match state.get_manifest(&repository, &digest) {
        Some(manifest) => manifest,
        _ => {
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
