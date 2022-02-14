mod blob;
mod blob_not_found;
mod manifest;
mod manifest_created;
mod upload_accepted;

pub(crate) use self::blob::Blob;
pub(crate) use self::blob_not_found::BlobNotFound;
pub(crate) use self::manifest::Manifest;
pub(crate) use self::manifest_created::ManifestCreated;
pub(crate) use self::upload_accepted::UploadAccepted;

#[derive(Responder)]
pub(crate) enum GetBlobResponses {
    Blob(Blob),
    BlobNotFound(BlobNotFound),
}
