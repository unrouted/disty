use anyhow::Result;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::digest::Digest;

pub(crate) struct WebhookConfig {
    matcher: regex::Regex,
    url: String,
}

pub struct Event {
    pub repository: String,
    pub digest: Digest,
    pub content_type: String,
    pub tag: String,
}

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct WebhookMetricLabels {
    status: String,
}

pub(crate) struct WebhookService {
    tx: tokio::sync::mpsc::Sender<Event>,
}

impl WebhookService {
    pub fn start(
        tasks: &mut JoinSet<Result<()>>,
        webhooks: Vec<WebhookConfig>,
        registry: &mut Registry,
    ) -> Self {
        let webhooks_total = Family::<WebhookMetricLabels, Counter>::default();
        registry.register(
            "webhooks_post",
            "Number of webhooks sent",
            webhooks_total.clone(),
        );

        let (tx, mut rx) = mpsc::channel::<Event>(100);

        tasks.spawn(async move {
            loop {
                match rx.recv().await {
                    None => {
                        return Ok(());
                    }
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

                        let match_target = format!("{repository}:{tag}");

                        for hook in &webhooks {
                            if !hook.matcher.is_match(&match_target) {
                                continue;
                            }
                            let resp = reqwest::Client::new()
                                .post(&hook.url)
                                .header(
                                    "Content-Type",
                                    "application/vnd.docker.distribution.events.v2+json",
                                )
                                .json(&payload)
                                .send()
                                .await;

                            if let Ok(resp) = resp {
                                let labels = WebhookMetricLabels {
                                    status: resp.status().to_string(),
                                };
                                webhooks_total.get_or_create(&labels).inc();

                                if resp.status() != 200 {
                                    // FIXME: Log failures here
                                }
                            } else {
                                let labels = WebhookMetricLabels {
                                    status: String::from("000"),
                                };
                                webhooks_total.get_or_create(&labels).inc();
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub(crate) async fn send(
        &self,
        repository: &str,
        digest: &Digest,
        tag: &str,
        content_type: &str,
    ) -> Result<()> {
        self.tx
            .send(Event {
                repository: repository.to_string(),
                digest: digest.clone(),
                tag: tag.to_string(),
                content_type: content_type.to_string(),
            })
            .await?;

        Ok(())
    }
}
