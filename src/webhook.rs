use anyhow::Result;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

use crate::digest::Digest;

pub(crate) struct WebhookConfig {
    pub matcher: regex::Regex,
    pub url: String,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub repository: String,
    pub digest: Digest,
    pub content_type: String,
    pub tag: String,
}

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct WebhookMetricLabels {
    status: String,
    url: String,
}

#[derive(Clone)]
struct WebhookWorker {
    tx: mpsc::Sender<Event>,
}

pub(crate) struct WebhookService {
    workers: HashMap<String, WebhookWorker>, // Keyed by webhook URL
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

        let webhooks_total = &webhooks_total; // Reference for safe use
        let mut workers = HashMap::new();

        for config in webhooks {
            let (tx, mut rx) = mpsc::channel::<Event>(100);
            let url = config.url.clone();
            let matcher = config.matcher.clone();

            // Clone only what is safe
            let webhooks_total = webhooks_total.clone();

            tasks.spawn(async move {
                let client = reqwest::Client::new();
                while let Some(event) = rx.recv().await {
                    let match_target = format!("{}:{}", event.repository, event.tag);
                    if !matcher.is_match(&match_target) {
                        continue;
                    }

                    let payload = json!({
                        "id": "",
                        "timestamp": "2016-03-09T14:44:26.402973972-08:00",
                        "action": "push",
                        "target": {
                            "mediaType": event.content_type,
                            "size": 708,
                            "digest": event.digest,
                            "length": 708,
                            "repository": event.repository,
                            "url": format!("/v2/{}/manifests/{}", event.repository, event.digest),
                            "tag": event.tag,
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

                    let mut attempts = 0;

                    loop {
                        let resp = client
                            .post(&url)
                            .header(
                                "Content-Type",
                                "application/vnd.docker.distribution.events.v2+json",
                            )
                            .json(&payload)
                            .send()
                            .await;

                        match resp {
                            Ok(r) => {
                                let status = r.status().as_u16().to_string();
                                webhooks_total
                                    .get_or_create(&WebhookMetricLabels {
                                        status,
                                        url: url.clone(),
                                    })
                                    .inc();

                                if r.status().is_success() {
                                    break;
                                }
                            }
                            Err(_) => {
                                webhooks_total
                                    .get_or_create(&WebhookMetricLabels {
                                        status: "000".to_string(),
                                        url: url.clone(),
                                    })
                                    .inc();
                            }
                        }

                        attempts += 1;
                        let backoff = Duration::from_secs((1 << attempts).min(30));
                        sleep(backoff).await;
                    }
                }

                Ok(())
            });

            workers.insert(config.url.clone(), WebhookWorker { tx });
        }

        Self { workers }
    }

    pub(crate) async fn send(
        &self,
        repository: &str,
        digest: &Digest,
        tag: &str,
        content_type: &str,
    ) -> Result<()> {
        let event = Event {
            repository: repository.to_string(),
            digest: digest.clone(),
            tag: tag.to_string(),
            content_type: content_type.to_string(),
        };

        for worker in self.workers.values() {
            worker.tx.send(event.clone()).await.ok();
        }

        Ok(())
    }
}
