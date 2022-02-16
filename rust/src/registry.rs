use rocket::Route;

pub fn routes() -> Vec<Route> {
    routes![
        // Uploads
        crate::views::blobs::uploads::delete::delete,
        crate::views::blobs::uploads::patch::patch,
        crate::views::blobs::uploads::post::post,
        crate::views::blobs::uploads::put::put,
        crate::views::blobs::uploads::get::get,
        // Blobs
        crate::views::blobs::get::get,
        // Manifests
        crate::views::manifests::put::put,
        crate::views::manifests::get::get,
        crate::views::manifests::get::get_by_tag,
        crate::views::manifests::get::delete,
    ]
}

#[cfg(test)]
mod test {
    use rocket::http::Status;
    use rocket::local::blocking::Client;

    fn client() -> Client {
        let server = rocket::build().mount("/", super::routes());
        Client::tracked(server).expect("valid rocket instance")
    }

    #[test]
    fn put_sha_query_param_fail() {
        let client = client();
        let response = client
            .put("/REPOSITORY/blobs/uploads/UPLOADID?digest=sha255:hello")
            .dispatch();
        assert_eq!(response.status(), Status::NotFound);
    }

    #[test]
    fn put() {}
}
