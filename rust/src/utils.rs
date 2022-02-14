use crate::types::Digest;

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

    path.push("blobs");
    path.push(&digest_string[0..2]);
    path.push(&digest_string[2..4]);
    path.push(&digest_string[4..6]);
    path.push(&digest_string[6..]);

    path
}
