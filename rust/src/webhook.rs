use crate::types::{Digest, RepositoryName};
use regex::Regex;
use reqwest;
use serde_json::json;
use tokio::sync::mpsc;
use pyo3::{prelude::*};

#[derive(Debug)]
pub struct WebhookConfig {
    pub url: String,
    pub matcher: Regex,
}

impl FromPyObject<'_> for WebhookConfig {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        // FIXME: This should send nice errors back to python if any of the unwraps fail...
        let url: String = dict.get_item("url").unwrap().extract().unwrap();
        let matcher: &str = dict.get_item("matcher").unwrap().extract().unwrap();
        Ok(WebhookConfig {
            url: url,
            matcher: Regex::new(matcher).unwrap(),
        })
    }
}

pub struct Event {
    pub repository: RepositoryName,
    pub digest: Digest,
    pub content_type: String,
    pub tag: String,
}

pub fn start_webhook_worker(webhooks: Vec<WebhookConfig>) -> tokio::sync::mpsc::Sender<Event> {
    let (tx, mut rx) = mpsc::channel::<Event>(100);

    println!("{webhooks:?}");

    let runtime = pyo3_asyncio::tokio::get_runtime();
    runtime.spawn(async move {
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
                    let payload = json!({
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

                    for hook in &webhooks {
                        if !hook.matcher.is_match("foo") {
                            continue;
                        }
                        let resp = reqwest::Client::new()
                            .post(&hook.url)
                            .json(&payload)
                            .send()
                            .await;

                        match resp {
                            Ok(resp) => {
                                if resp.status() != 200 {
                                    // FIXME: Log failures here
                                }
                            }
                            _ => {
                                // FIXME: Log failure
                            }
                        }
                    }
                }
                None => {}
            }
        }
    });

    return tx;
}
