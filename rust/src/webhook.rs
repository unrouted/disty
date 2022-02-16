use crate::types::{Digest, RepositoryName};
use serde_json::json;
use tokio::sync::mpsc;

pub struct Event {
    pub repository: RepositoryName,
    pub digest: Digest,
    pub content_type: String,
    pub tag: String,
}

pub fn start_webhook_worker() -> tokio::sync::mpsc::Sender<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Some(Event {
                    repository,
                    digest,
                    tag,
                    content_type,
                }) => {
                    // FIXME: This is just enough webhook for what I personally need. Sorry if
                    // you need it to be valid!
                    let _payload = json!({
                        "id": "",
                        "timestamp": "2016-03-09T14:44:26.402973972-08:00",
                        "action": "push",
                        "target": {
                            "mediaType": content_type,
                            "size": 708,
                            "digest": digest,
                            "length": 708,
                            "repository": repository,
                            "url": format!("/v2/{repository}/manifests/{digest}"),
                            "tag": tag,
                        },
                        "request": {
                            "id": "",
                            "addr": "192.168.64.11:42961",
                            "host": "192.168.100.227:5000",
                            "method": "PUT",
                            "useragent": "curl/7.38.0",
                        },
                        "actor": {},
                        "source": {
                            "addr": "xtal.local:5000",
                            "instanceID": "a53db899-3b4b-4a62-a067-8dd013beaca4",
                        },
                    });
                }
                None => {}
            }
        }
    });

    return tx;
}
