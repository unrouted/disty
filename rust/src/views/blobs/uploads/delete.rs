use rocket::http::Status;

#[delete("/<_repository>/blobs/uploads/<_upload_id>")]
pub(crate) async fn delete(_repository: String, _upload_id: String) -> Status {
    Status::NotAcceptable
}
