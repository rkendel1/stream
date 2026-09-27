use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use std::time::Duration;
use stream_model::{NormalizedItem, Source, SourceKind};
use url::Url;

/// Upper bound on a fetched document. Stream observes documents, it does not crawl.
pub const MAX_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;

#[async_trait]
pub trait SourceAdapter: Send + Sync {
    fn kind(&self) -> SourceKind;

    /// Adapters are selected by the source's ingestion format, never by its
    /// user-facing kind: a GitHub source observed through Atom uses the Atom adapter.
    fn can_handle(&self, source: &Source) -> bool {
        source.adapter_kind == self.kind()
    }

    fn parse(&self, source: &Source, body: &[u8], fetched_at: DateTime<Utc>) -> Result<Vec<NormalizedItem>>;
}

/// A fetched document plus the transport facts needed to understand it.
#[derive(Debug, Clone)]
pub struct FetchedDocument {
    pub requested_url: Url,
    pub final_url: Url,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct HttpFetcher {
    client: Client,
}

impl Default for HttpFetcher {
    fn default() -> Self {
        Self {
            client: Client::builder()
                .user_agent(concat!("stream-runtime/", env!("CARGO_PKG_VERSION")))
                .timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client should build"),
        }
    }
}

impl HttpFetcher {
    pub async fn fetch(&self, source: &Source) -> Result<Vec<u8>> {
        Ok(self.fetch_url(&source.endpoint).await?.body)
    }

    pub async fn fetch_url(&self, url: &Url) -> Result<FetchedDocument> {
        let response = self.client.get(url.clone()).send().await?;
        let response = response.error_for_status()?;
        let final_url = response.url().clone();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_ascii_lowercase());
        let body = response.bytes().await?;
        if body.len() > MAX_DOCUMENT_BYTES {
            return Err(anyhow!("document exceeds {} bytes", MAX_DOCUMENT_BYTES));
        }
        Ok(FetchedDocument {
            requested_url: url.clone(),
            final_url,
            content_type,
            body: body.to_vec(),
            fetched_at: Utc::now(),
        })
    }
}

/// The wire format of a fetched document, detected from its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentFormat {
    Rss,
    Atom,
    JsonFeed,
    Html,
    Unknown,
}

impl DocumentFormat {
    pub fn adapter_kind(self) -> SourceKind {
        match self {
            DocumentFormat::Rss => SourceKind::Rss,
            DocumentFormat::Atom => SourceKind::Atom,
            DocumentFormat::JsonFeed => SourceKind::JsonFeed,
            DocumentFormat::Html | DocumentFormat::Unknown => SourceKind::Web,
        }
    }

    pub fn is_feed(self) -> bool {
        matches!(self, DocumentFormat::Rss | DocumentFormat::Atom | DocumentFormat::JsonFeed)
    }
}

/// Sniff the document format. The body wins over the declared content type,
/// because feeds are routinely served as `text/html` or `text/plain`.
pub fn detect_format(content_type: Option<&str>, body: &[u8]) -> DocumentFormat {
    let head = String::from_utf8_lossy(&body[..body.len().min(4096)]).to_ascii_lowercase();
    let trimmed = head.trim_start_matches('\u{feff}').trim_start();
    if trimmed.starts_with('{') && head.contains("jsonfeed.org/version") {
        return DocumentFormat::JsonFeed;
    }
    if trimmed.starts_with('<') {
        if head.contains("<rss") || head.contains("<rdf:rdf") {
            return DocumentFormat::Rss;
        }
        if head.contains("<feed") && head.contains("http://www.w3.org/2005/atom") {
            return DocumentFormat::Atom;
        }
        if head.contains("<html") || head.contains("<!doctype html") || head.contains("<head") || head.contains("<body") {
            return DocumentFormat::Html;
        }
    }
    match content_type {
        Some(value) if value.contains("rss") => DocumentFormat::Rss,
        Some(value) if value.contains("atom") => DocumentFormat::Atom,
        Some(value) if value.contains("feed+json") => DocumentFormat::JsonFeed,
        Some(value) if value.contains("html") => DocumentFormat::Html,
        _ => DocumentFormat::Unknown,
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
            .ok_or_else(|| anyhow!("no source adapter registered for {}", source.adapter_kind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_formats_from_bytes_not_declared_type() {
        let rss = br#"<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>"#;
        let atom = br#"<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"></feed>"#;
        let json = br#"{"version": "https://jsonfeed.org/version/1.1", "items": []}"#;
        let html = b"<!DOCTYPE html><html><head><title>x</title></head></html>";
        assert_eq!(detect_format(Some("text/html"), rss), DocumentFormat::Rss);
        assert_eq!(detect_format(Some("text/xml"), atom), DocumentFormat::Atom);
        assert_eq!(detect_format(Some("application/json"), json), DocumentFormat::JsonFeed);
        assert_eq!(detect_format(None, html), DocumentFormat::Html);
        assert_eq!(detect_format(Some("application/octet-stream"), b"\x00\x01"), DocumentFormat::Unknown);
    }
}
