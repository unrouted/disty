use crate::types::Digest;
use crate::types::RepositoryName;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};

pub(crate) struct ManifestCreated {
    pub repository: RepositoryName,
    pub digest: Digest,
}

/*
201 Created
Location: <url>
Content-Length: 0
Docker-Content-Digest: <digest>
*/

impl<'r> Responder<'r, 'static> for ManifestCreated {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        let location = Header::new(
            "Location",
            format!("/v2/{}/manifests/{}", self.repository, self.digest),
        );
        let length = Header::new("Content-Length", "0");
        let upload_uuid = Header::new("Docker-Content-Digest", self.digest.to_string());

        Response::build()
            .header(location)
            .header(length)
            .header(upload_uuid)
            .status(Status::Created)
            .ok()
    }
}
