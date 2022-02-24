mod access_denied;
mod blob;
mod blob_not_found;

pub(crate) use self::access_denied::AccessDenied;
pub(crate) use self::blob::Blob;
pub(crate) use self::blob_not_found::BlobNotFound;

#[derive(Responder)]
pub(crate) enum GetBlobResponses {
    Blob(Blob),
    BlobNotFound(BlobNotFound),
    AccessDenied(AccessDenied),
}
