use rocket::http::Status;
use rocket::request::{self, FromRequest, Outcome, Request};

pub(crate) struct ContentRange(String);

#[derive(Debug)]
pub(crate) enum ApiTokenError {
    Missing,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ContentRange {
    type Error = ApiTokenError;

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let token = req.headers().get_one("content-range");
        match token {
            Some(token) => {
                // check validity
                Outcome::Success(ContentRange(token.to_string()))
            }
            None => Outcome::Failure((Status::Unauthorized, ApiTokenError::Missing)),
        }
    }
}
