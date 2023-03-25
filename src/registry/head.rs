use actix_web::http::StatusCode;
use actix_web::{head, HttpResponse, HttpResponseBuilder};

use crate::extractors::Token;

use super::errors::RegistryError;

#[head("")]
pub(crate) async fn head(token: Token) -> Result<HttpResponse, RegistryError> {
    if !token.validated_token {
        return Err(RegistryError::MustAuthenticate {
            challenge: token.get_general_challenge(),
        });
    }

    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .append_header(("Docker-Distribution-Api-Version", "registry/2.0"))
        .finish())
}
