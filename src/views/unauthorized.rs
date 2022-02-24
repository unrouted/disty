use crate::headers::Token;
use crate::types::RegistryState;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::State;

pub(crate) struct MustAuthenticate {
}

impl<'r> Responder<'r, 'static> for MustAuthenticate {
    fn respond_to(self, _req: &Request) -> Result<Response<'static>, Status> {
        // let validation_errors = req.local_cache::<Option<ValidationErrors>, _>(|| None);

        Response::build()
              .header(Header::new(
                  "Www-Authenticate", 
                  "Bearer realm=\"https://auth.docker.io/token\",service=\"registry.docker.io\",scope=\"repository:samalba/my-app:pull,push\"",
               ))
              .status(Status::Unauthorized).ok()
    }
}


#[catch(401)]
fn catch_401(_req: &Request) -> MustAuthenticate {
    MustAuthenticate {}
}