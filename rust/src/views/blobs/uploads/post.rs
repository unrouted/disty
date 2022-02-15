use crate::responses::UploadAccepted;
use crate::types::RepositoryName;
use uuid::Uuid;

#[post("/<repository>/blobs/uploads")]
pub(crate) async fn post(repository: RepositoryName) -> UploadAccepted {
    let uuid = Uuid::new_v4().to_hyphenated().to_string();

    UploadAccepted {
        repository,
        uuid,
    }
}