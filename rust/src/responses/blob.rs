use crate::types::Digest;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::tokio::fs::File;

pub(crate) struct Blob {
    pub digest: Digest,
    pub content_type: String,
    pub file: File,
}

/*
200 OK
Docker-Content-Digest: <digest>
Content-Type: <media type of blob>

...
*/

impl<'r> Responder<'r, 'static> for Blob {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        let content_type = Header::new("Content-Type", self.content_type);
        let digest = Header::new("Docker-Content-Digest", self.digest.to_string());

        Response::build()
            .header(content_type)
            .header(digest)
            .status(Status::Accepted)
            .streamed_body(self.file)
            .ok()
    }
}
