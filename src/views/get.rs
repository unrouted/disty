use crate::types::RegistryState;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;

pub(crate) enum Responses {
    Ok {},
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::Ok {} => Response::build()
                .header(Header::new("Content-Length", "0"))
                .status(Status::Ok)
                .ok(),
        }
    }
}

#[get("/")]
pub(crate) async fn get(state: &State<RegistryState>) -> Responses {
    let _state: &RegistryState = state.inner();

    Responses::Ok {}
}
