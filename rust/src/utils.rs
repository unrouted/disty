use crate::types::Digest;
use std::path::Path;
use uuid::Uuid;

pub fn get_blob_path(images_directory: &String, digest: &Digest) -> std::path::PathBuf {
    let mut path = std::path::Path::new(&images_directory).to_path_buf();
    let digest_string = &digest.hash;

    path.push("blobs");
    path.push(&digest_string[0..2]);
    path.push(&digest_string[2..4]);
    path.push(&digest_string[4..6]);
    path.push(&digest_string[6..]);

    path
}

pub fn get_manifest_path(images_directory: &String, digest: &Digest) -> std::path::PathBuf {
    let mut path = std::path::Path::new(&images_directory).to_path_buf();
    let digest_string = &digest.hash;

    path.push("manifests");
    path.push(&digest_string[0..2]);
    path.push(&digest_string[2..4]);
    path.push(&digest_string[4..6]);
    path.push(&digest_string[6..]);

    path
}

pub fn get_upload_path(images_directory: &String, upload_id: &String) -> std::path::PathBuf {
    let mut path = Path::new(&images_directory).to_path_buf();
    path.push("uploads");
    path.push(format!("blob-{upload_id}"));

    path
}

pub fn get_temp_path(images_directory: &String) -> std::path::PathBuf {
    let upload_id = Uuid::new_v4().to_hyphenated().to_string();

    let mut path = Path::new(&images_directory).to_path_buf();
    path.push("uploads");
    path.push(format!("manifest-{upload_id}"));

    path
}
