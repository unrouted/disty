use anyhow::Result;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until};
use tracing::error;

use crate::config::WebhookConfig;
use crate::context::RequestContext;
use crate::extractor::Descriptor;

#[derive(Clone, Debug)]
pub struct Event {
    pub context: RequestContext,
    pub repository: String,
    pub tag: String,
    pub descriptor: Descriptor,
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

pub struct WebhookService {
    workers: HashMap<String, WebhookWorker>,
}

impl WebhookService {
    pub fn start(
        tasks: &mut JoinSet<Result<()>>,
        webhooks: &[WebhookConfig],
        registry: &mut Registry,
    ) -> Self {
        let webhooks_total = Family::<WebhookMetricLabels, Counter>::default();
        registry.register(
            "webhooks_post",
            "Number of webhooks sent",
            webhooks_total.clone(),
        );

        let mut workers = HashMap::new();

        for config in webhooks.iter().cloned() {
            let (tx, mut rx) = mpsc::channel::<Event>(100);
            let url = config.url.clone();
            let matcher = config.matcher.clone();
            let webhooks_total = webhooks_total.clone();

            tasks.spawn(async move {
                let client = reqwest::Client::builder().timeout(config.timeout).build().unwrap();
                let mut buffer = Vec::new();
                let mut deadline = Instant::now() + config.flush_interval;

                loop {
                    tokio::select! {
                        maybe_event = rx.recv() => {
                            match maybe_event {
                                Some(event) => {
                                    if matcher.is_match(&format!("{}:{}", event.repository, event.tag)) {
                                        buffer.push(event);
                                    }

                                    if buffer.len() >= 10 {
                                        flush_batch(&client, &buffer, &url, &webhooks_total, config.retry_base).await;
                                        buffer.clear();
                                        deadline = Instant::now() + config.flush_interval;
                                    }
                                }
                                None => {
                                    if !buffer.is_empty() {
                                        flush_batch(&client, &buffer, &url, &webhooks_total, config.retry_base).await;
                                    }
                                    break;
                                }
                            }
                        }
                        _ = sleep_until(deadline) => {
                            if !buffer.is_empty() {
                                flush_batch(&client, &buffer, &url, &webhooks_total, config.retry_base).await;
                                buffer.clear();
                            }
                            deadline = Instant::now() + config.flush_interval;
                        }
                    }
                }

                Ok(())
            });

            workers.insert(config.url.clone(), WebhookWorker { tx });
        }

        Self { workers }
    }

    pub async fn send(
        &self,
        context: &RequestContext,
        repository: &str,
        descriptor: &Descriptor,
        tag: &str,
    ) -> Result<()> {
        let event = Event {
            context: context.clone(),
            repository: repository.to_string(),
            descriptor: descriptor.clone(),
            tag: tag.to_string(),
        };

        for worker in self.workers.values() {
            let _ = worker.tx.send(event.clone()).await;
        }

        Ok(())
    }
}

async fn flush_batch(
    client: &reqwest::Client,
    batch: &[Event],
    url: &str,
    webhooks_total: &Family<WebhookMetricLabels, Counter>,
    retry_base: Duration,
) {
    let events_json: Vec<_> = batch
        .iter()
        .map(|event| {
            json!({
                "id": "",
                "timestamp": "2016-03-09T14:44:26.402973972-08:00",
                "action": "push",
                "target": {
                    "mediaType": event.descriptor.media_type,
                    "size": event.descriptor.size,
                    "digest": event.descriptor.digest,
                    "length": event.descriptor.size,
                    "repository": event.repository,
                    "url": format!("/v2/{}/manifests/{}", event.repository, event.descriptor.digest),
                    "tag": event.tag,
                },
                "request": {
                    "id": event.context.request_id.clone(),
                    "addr": event.context.peer.to_string(),
                    "host": "192.168.100.227:5000",
                    "method": event.context.method.clone(),
                    "useragent": event.context.user_agent.clone().unwrap_or("".into()),
                },
                "actor": {
                    "name": event.context.sub.clone(),
                },
                "source": {
                    "addr": "xtal.local:5000",
                    "instanceID": "a53db899-3b4b-4a62-a067-8dd013beaca4",
                },
            })
        })
        .collect();

    let payload = json!({ "events": events_json });

    let mut attempts = 0;
    loop {
        let resp = client
            .post(url)
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
                        url: url.to_string(),
                    })
                    .inc();

                if r.status().is_success() {
                    break;
                }
            }
            Err(e) => {
                error!("Error whilst sending webhook: {e:?}");
                webhooks_total
                    .get_or_create(&WebhookMetricLabels {
                        status: "000".to_string(),
                        url: url.to_string(),
                    })
                    .inc();
            }
        }

        attempts += 1;
        let backoff = retry_base * (1 << attempts).min(10);
        sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use regex::Regex;
    use test_log::test;
    use tokio::time::{self, Duration, timeout};
    use wiremock::matchers::*;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn wait_for_requests(
        server: &MockServer,
        min_count: usize,
        timeout_dur: Duration,
    ) -> bool {
        timeout(timeout_dur, async {
            loop {
                if server.received_requests().await.unwrap().len() >= min_count {
                    break true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    #[test(tokio::test)]
    async fn test_webhook_batching_and_retry_virtual_time() {
        time::pause();

        let server = MockServer::start().await;

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_2 = call_count.clone();
        // First failure
        Mock::given(method("POST"))
            .and(path("/webhook"))
            .respond_with(move |_: &wiremock::Request| {
                let n = call_count_2.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    ResponseTemplate::new(500)
                } else {
                    ResponseTemplate::new(200)
                }
            })
            .expect(2)
            .mount(&server)
            .await;

        let mut registry = Registry::default();
        let mut tasks = JoinSet::new();

        let service = WebhookService::start(
            &mut tasks,
            &[WebhookConfig {
                matcher: Regex::new(".*").unwrap(),
                url: format!("{}/webhook", server.uri()),
                timeout: Duration::from_millis(100),
                flush_interval: Duration::from_millis(100),
                retry_base: Duration::from_millis(50),
            }],
            &mut registry,
        );

        let req = RequestContext {
            access: vec![],
            sub: "anonymous".to_string(),
            admin: true,
            validated_token: true,
            service: None,
            realm: None,
            user_agent: Some("Foo".into()),
            method: "put".into(),
            request_id: "abcdef".into(),
            peer: "127.0.0.1:80".parse().unwrap(),
        };

        for _ in 0..5 {
            let desc = Descriptor {
                digest: "sha256:fea8895f450959fa676bcc1df0611ea93823a735a01205fd8622846041d0c7cf"
                    .parse()
                    .unwrap(),
                media_type: "application/vnd.docker.distribution.manifest.v2+json".into(),
                size: Some(32),
                platform: None,
            };

            service
                .send(&req, "library/myapp", &desc, "latest")
                .await
                .unwrap();
        }

        // Advance virtual time to trigger batching + retry
        time::advance(Duration::from_secs(1)).await;

        // Wait for expectations
        assert!(
            wait_for_requests(&server, 2, Duration::from_secs(10)).await,
            "Did not receive expected webhook calls"
        );

        tasks.shutdown().await;
    }
}
