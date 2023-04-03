use std::sync::{Arc, RwLock};

use anyhow::Result;
use futures::{
    channel::mpsc::{channel, Receiver},
    SinkExt, StreamExt,
};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use rustls::sign::CertifiedKey;
use rustls::{server::ResolvesServerCert, sign::RsaSigningKey};
use rustls::{Certificate, PrivateKey};
use rustls_pemfile::{certs, pkcs8_private_keys};

pub struct ServerCertificate {
    pub key_path: String,
    pub chain_path: String,
    certified_key: Arc<RwLock<Arc<CertifiedKey>>>,
    watcher: RecommendedWatcher,
}

pub async fn get_certified_key(key_path: &String, chain_path: &String) -> Result<CertifiedKey> {
    let key_string = tokio::fs::read_to_string(&key_path).await?;
    let mut key_bytes = key_string.as_bytes();
    let key = PrivateKey(pkcs8_private_keys(&mut key_bytes)?.remove(0));
    let signing_key = RsaSigningKey::new(&key)?;

    let chain_string = tokio::fs::read_to_string(&chain_path).await?;
    let mut chain_bytes = chain_string.as_bytes();
    let chain = certs(&mut chain_bytes)?
        .into_iter()
        .map(Certificate)
        .collect();

    Ok(CertifiedKey::new(chain, Arc::new(signing_key)))
}

impl ServerCertificate {
    pub async fn new(key_path: String, chain_path: String) -> Result<Self> {
        let certified_key = Arc::new(RwLock::new(Arc::new(
            get_certified_key(&key_path, &chain_path).await?,
        )));

        let (watcher, mut rx) = async_watcher()?;

        let mut server_certificate = ServerCertificate {
            key_path: key_path.clone(),
            chain_path: chain_path.clone(),
            certified_key: certified_key.clone(),
            watcher,
        };

        server_certificate
            .watcher
            .watch(key_path.as_ref(), RecursiveMode::Recursive)?;
        server_certificate
            .watcher
            .watch(chain_path.as_ref(), RecursiveMode::Recursive)?;

        tokio::spawn(async move {
            while let Some(res) = rx.next().await {
                match res {
                    Ok(_) => match get_certified_key(&key_path, &chain_path).await {
                        Ok(new_certified_key) => {
                            tracing::info!("Reloaded certified key");
                            *certified_key.write().unwrap() = Arc::new(new_certified_key);
                        }
                        Err(e) => {
                            tracing::error!("Error loading new key: {:?}", e);
                        }
                    },
                    Err(e) => tracing::error!("watch error: {:?}", e),
                }
            }
        });

        Ok(server_certificate)
    }
}

impl ResolvesServerCert for ServerCertificate {
    fn resolve(
        &self,
        _: rustls::server::ClientHello,
    ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
        if let Ok(certified_key) = self.certified_key.read() {
            return Some(certified_key.clone());
        }
        None
    }
}

fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<Event>>)> {
    let (mut tx, rx) = channel(1);

    // Automatically select the best implementation for your platform.
    // You can also access each implementation directly e.g. INotifyWatcher.
    let watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                tx.send(res).await.unwrap();
            })
        },
        Config::default(),
    )?;

    Ok((watcher, rx))
}
