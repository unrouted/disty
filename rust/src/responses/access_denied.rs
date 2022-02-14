use crate::types::RepositoryName;
use rocket::http::{Header, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};

pub(crate) struct AccessDenied {}

/*
403 Forbidden
Content-Length: <length>
Content-Type: application/json; charset=utf-8

{
    "errors:" [
        {
            "code": <error code>,
            "message": "<error message>",
            "detail": ...
        },
        ...
    ]
}
*/

impl<'r> Responder<'r, 'static> for AccessDenied {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        let ctype = Header::new("Content-Type", "application/json; charset=utf-8");

        Response::build()
            .header(ctype)
            .status(Status::Forbidden)
            .ok()
    }
}
