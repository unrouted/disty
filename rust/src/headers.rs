use rocket::http::Status;
use rocket::request::{self, FromRequest, Outcome, Request};

pub(crate) struct ContentRange {
    pub start: u64,
    pub stop: u64,
}

#[derive(Debug)]
pub(crate) enum ContentRangeError {
    Missing,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ContentRange {
    type Error = ContentRangeError;

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let token = req.headers().get_one("content-range");
        match token {
            Some(token) => match token.split_once("-") {
                Some((start, stop)) => {
                    let start: u64 = match start.parse() {
                        Ok(value) => value,
                        _ => {
                            return Outcome::Failure((
                                Status::Unauthorized,
                                ContentRangeError::Missing,
                            ))
                        }
                    };
                    let stop: u64 = match stop.parse() {
                        Ok(value) => value,
                        _ => {
                            return Outcome::Failure((
                                Status::Unauthorized,
                                ContentRangeError::Missing,
                            ))
                        }
                    };
                    Outcome::Success(ContentRange { start, stop })
                }
                _ => Outcome::Failure((Status::Unauthorized, ContentRangeError::Missing)),
            },
            None => Outcome::Failure((Status::Unauthorized, ContentRangeError::Missing)),
        }
    }
}

pub(crate) struct ContentType {
    pub content_type: String,
}

#[derive(Debug)]
pub(crate) enum ContentTypeError {
    Missing,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ContentType {
    type Error = ContentTypeError;

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let token = req.headers().get_one("content-type");
        match token {
            Some(token) => Outcome::Success(ContentType {
                content_type: token.to_string(),
            }),
            None => Outcome::Failure((Status::Unauthorized, ContentTypeError::Missing)),
        }
    }
}
