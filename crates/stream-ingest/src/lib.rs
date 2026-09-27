use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use stream_model::{NormalizedItem, Source, SourceKind};

#[async_trait]
pub trait SourceAdapter: Send + Sync {
    fn kind(&self) -> SourceKind;

    fn can_handle(&self, source: &Source) -> bool {
        source.kind == self.kind()
    }

    fn parse(&self, source: &Source, body: &[u8], fetched_at: DateTime<Utc>) -> Result<Vec<NormalizedItem>>;
}

#[derive(Debug, Clone)]
pub struct HttpFetcher {
    client: Client,
}

impl Default for HttpFetcher {
    fn default() -> Self {
        Self {
            client: Client::builder()
                .user_agent("stream-runtime/0.1.0")
                .build()
                .expect("reqwest client should build"),
        }
    }
}

impl HttpFetcher {
    pub async fn fetch(&self, source: &Source) -> Result<Vec<u8>> {
        let response = self.client.get(source.endpoint.clone()).send().await?;
        let response = response.error_for_status()?;
        Ok(response.bytes().await?.to_vec())
    }
}

pub struct AdapterRegistry {
    adapters: Vec<Box<dyn SourceAdapter>>,
}

impl AdapterRegistry {
    pub fn new(adapters: Vec<Box<dyn SourceAdapter>>) -> Self {
        Self { adapters }
    }

    pub fn adapter_for(&self, source: &Source) -> Result<&dyn SourceAdapter> {
        self.adapters
            .iter()
            .find(|adapter| adapter.can_handle(source))
            .map(|adapter| adapter.as_ref())
            .ok_or_else(|| anyhow!("no source adapter registered for {}", source.kind))
    }
}
