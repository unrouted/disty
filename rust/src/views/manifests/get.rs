use crate::responses;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::{get_manifest_path};
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

    let path = get_manifest_path(&state.repository_path, &digest);
    if !path.is_file() {
        return responses::GetManifestResponses::ManifestNotFound(responses::ManifestNotFound {});
    }

    match File::open(path).await {
        Ok(file) => responses::GetManifestResponses::Manifest(responses::Manifest {
            content_type: "application/vnd.docker.distribution.manifest.v2+json".to_string(),
            digest,
            file,
        }),
        _ => {
            responses::GetManifestResponses::ManifestNotFound(
                responses::ManifestNotFound {},
            )
        }
    }
}