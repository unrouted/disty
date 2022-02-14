use crate::types::RepositoryName;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};

pub(crate) struct UploadAccepted {
    pub repository: RepositoryName,
    pub uuid: String,
}

/*
202 Accepted
Location: /v2/<name>/blobs/uploads/<uuid>
Range: bytes=0-<offset>
Content-Length: 0
Docker-Upload-UUID: <uuid>
*/

impl<'r> Responder<'r, 'static> for UploadAccepted {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        let location = Header::new(
            "Location",
            format!("/v2/{}/blobs/uploads/{}", self.repository, self.uuid),
        );
        let range = Header::new("Range", "0-0");
        let length = Header::new("Content-Length", "0");
        let upload_uuid = Header::new("Docker-Upload-UUID", self.uuid);

        Response::build()
            .header(location)
            .header(range)
            .header(length)
            .header(upload_uuid)
            .status(Status::Accepted)
            .ok()
    }
}
