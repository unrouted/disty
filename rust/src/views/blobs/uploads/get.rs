use rocket::http::Status;

#[get("/<_repository>/blobs/uploads/<_upload_id>")]
pub(crate) fn get(_repository: String, _upload_id: String) -> Status {
    Status::NotAcceptable
}
