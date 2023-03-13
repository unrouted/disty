use crate::types::Digest;
use anyhow::Context;
use rocket::{Build, Rocket};
use std::path::Path;
use uuid::Uuid;

pub fn get_temp_mirror_path(images_directory: &str) -> std::path::PathBuf {
    let upload_id = Uuid::new_v4().as_hyphenated().to_string();

    let mut path = Path::new(&images_directory).to_path_buf();
    path.push("uploads");
    path.push(format!("mirror-{upload_id}"));

    path
}

