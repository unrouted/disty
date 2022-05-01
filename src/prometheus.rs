use rocket::Route;

use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};

pub(crate) enum Responses {
    Ok {},
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::Ok {} => Response::build().status(Status::Ok).ok(),
        }
    }
}

#[get("/healthz")]
pub(crate) async fn healthz() -> Responses {
    Responses::Ok {}
}

pub fn routes() -> Vec<Route> {
    routes![healthz,]
}
