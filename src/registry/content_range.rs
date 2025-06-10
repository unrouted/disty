use axum::http::{HeaderName, HeaderValue};
use headers::{Error, Header};

pub(crate) struct ContentRange {
    pub first_byte: u64,
    pub last_byte: u64,
}

impl Header for ContentRange {
    fn name() -> &'static HeaderName {
        &::axum::http::header::CONTENT_RANGE
    }

    fn decode<'i, I: Iterator<Item = &'i HeaderValue>>(values: &mut I) -> Result<Self, Error> {
        values
            .next()
            .and_then(|v| v.to_str().ok())
            .and_then(|range| {
                let Some((first_byte, last_byte)) = range.split_once("-") else {
                    return None;
                };
                let first_byte = first_byte.parse().ok()?;
                let last_byte = last_byte.parse().ok()?;
                if last_byte < first_byte {
                    return None;
                }

                Some(ContentRange {
                    first_byte,
                    last_byte,
                })
            })
            .ok_or_else(Error::invalid)
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let value = format!("{}-{}", self.first_byte, self.last_byte);
        if let Ok(header_value) = HeaderValue::from_str(&value) {
            values.extend(std::iter::once(header_value));
        }
    }
}
