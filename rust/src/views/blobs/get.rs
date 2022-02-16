use crate::headers::Token;
use crate::responses;
use crate::types::Digest;
use crate::types::RegistryState;
use crate::types::RepositoryName;
use crate::utils::get_blob_path;
use rocket::tokio::fs::File;
use rocket::State;

#[get("/<repository>/blobs/<digest>")]
pub(crate) async fn get(
    repository: RepositoryName,
    digest: Digest,
    state: &State<RegistryState>,
    token: Token,
) -> responses::GetBlobResponses {
    let state: &RegistryState = state.inner();

    if !token.has_permission(&repository, &"pull".to_string()) {
        return responses::GetBlobResponses::AccessDenied(responses::AccessDenied {});
    }

    if !state.is_blob_available(&repository, &digest) {
        return responses::GetBlobResponses::BlobNotFound(responses::BlobNotFound {});
    }

    let path = get_blob_path(&state.repository_path, &digest);
    if !path.is_file() {
        return responses::GetBlobResponses::BlobNotFound(responses::BlobNotFound {});
    }

    match File::open(path).await {
        Ok(file) => responses::GetBlobResponses::Blob(responses::Blob {
            content_type: "application/octet-steam".to_string(),
            digest,
            file,
        }),
        _ => responses::GetBlobResponses::BlobNotFound(responses::BlobNotFound {}),
    }
}
