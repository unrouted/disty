use crate::headers::Token;
use crate::types::RegistryState;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;
use std::io::Cursor;
use std::sync::Arc;

pub(crate) enum Responses {
    MustAuthenticate { challenge: String },
    Ok {},
}

impl<'r> Responder<'r, 'static> for Responses {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        match self {
            Responses::MustAuthenticate { challenge } => {
                let body = crate::views::utils::simple_oci_error(
                    "UNAUTHORIZED",
                    "authentication required",
                );
                Response::build()
                    .header(Header::new("Content-Length", body.len().to_string()))
                    .header(Header::new("Www-Authenticate", challenge))
                    .sized_body(body.len(), Cursor::new(body))
                    .status(Status::Unauthorized)
                    .ok()
            }
            Responses::Ok {} => Response::build()
                .header(Header::new("Content-Length", "0"))
                .header(Header::new(
                    "Docker-Distribution-Api-Version",
                    "registry/2.0",
                ))
                .status(Status::Ok)
                .ok(),
        }
    }
}

#[get("/")]
pub(crate) async fn get(state: &State<Arc<RegistryState>>, token: Token) -> Responses {
    let _state: &RegistryState = state.inner();

    if !token.validated_token {
        return Responses::MustAuthenticate {
            challenge: token.get_general_challenge(),
        };
    }

    Responses::Ok {}
}
