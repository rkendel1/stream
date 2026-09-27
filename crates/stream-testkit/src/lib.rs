//! Test support for Stream: an isolated FeltDB namespace per test and a
//! local HTTP server that plays the part of the web, so the full
//! URL → signal loop runs without network access.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use stream_core::{FeltDbConfig, FeltDbStore, StreamRuntime};
use stream_ingest::AdapterRegistry;

/// The repository root, where `node_modules/@feltdb/core` lives.
pub fn repo_root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .canonicalize()
        .expect("repo root")
}

/// A fresh, isolated FeltDB location. Reusing it with [`runtime_at`] is a
/// runtime restart against the same durable state.
#[derive(Debug, Clone)]
pub struct Isolated {
    pub namespace: String,
    pub path: PathBuf,
}

impl Isolated {
    pub fn new() -> Self {
        let id = uuid::Uuid::new_v4();
        Self {
            namespace: format!("stream-test-{id}"),
            path: std::env::temp_dir().join(format!("stream-test-{id}")),
        }
    }

    pub fn config(&self) -> FeltDbConfig {
        FeltDbConfig::at(repo_root(), self.namespace.clone(), self.path.clone())
    }
}

impl Default for Isolated {
    fn default() -> Self {
        Self::new()
    }
}

pub fn adapters() -> AdapterRegistry {
    let mut adapters = stream_rss::default_adapters();
    adapters.extend(stream_web::web_adapters());
    AdapterRegistry::new(adapters)
}

pub fn runtime_at(isolated: &Isolated) -> StreamRuntime {
    StreamRuntime::new(FeltDbStore::new(isolated.config()), adapters())
}

#[derive(Clone)]
struct Response {
    status: u16,
    content_type: String,
    body: String,
}

/// A local HTTP server with mutable routes.
#[derive(Clone)]
pub struct FixtureServer {
    base: String,
    routes: Arc<Mutex<HashMap<String, Response>>>,
}

impl FixtureServer {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        let routes: Arc<Mutex<HashMap<String, Response>>> = Default::default();
        let shared = routes.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let routes = shared.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).is_err() {
                        return;
                    }
                    loop {
                        let mut header = String::new();
                        match reader.read_line(&mut header) {
                            Ok(0) | Err(_) => break,
                            Ok(_) if header == "\r\n" || header == "\n" => break,
                            Ok(_) => {}
                        }
                    }
                    let path = request_line.split_whitespace().nth(1).unwrap_or("/").to_owned();
                    let response = routes.lock().unwrap().get(&path).cloned().unwrap_or(Response {
                        status: 404,
                        content_type: "text/plain".into(),
                        body: "not found".into(),
                    });
                    let reason = if response.status == 200 { "OK" } else { "Error" };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.status,
                        reason,
                        response.content_type,
                        response.body.len(),
                        response.body
                    );
                });
            }
        });
        Self { base, routes }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    /// Serve `body` at `path`. `{base}` in the body is replaced with the server's base URL.
    pub fn route(&self, path: &str, content_type: &str, body: &str) -> String {
        self.routes.lock().unwrap().insert(
            path.to_owned(),
            Response {
                status: 200,
                content_type: content_type.to_owned(),
                body: body.replace("{base}", &self.base),
            },
        );
        self.url(path)
    }

    pub fn html(&self, path: &str, body: &str) -> String {
        self.route(path, "text/html; charset=utf-8", body)
    }

    pub fn failing(&self, path: &str, status: u16) -> String {
        self.routes.lock().unwrap().insert(
            path.to_owned(),
            Response { status, content_type: "text/plain".into(), body: "failure".into() },
        );
        self.url(path)
    }
}

/// A news article about Apple Container, advertising an RSS feed.
pub const APPLE_CONTAINER_ARTICLE: &str = r#"<!doctype html>
<html><head>
  <title>Apple Container adds portable Linux VMs | Example News</title>
  <meta name="description" content="Apple's open source container tool runs each Linux container in its own lightweight virtual machine.">
  <meta property="article:published_time" content="2026-09-25T09:00:00Z">
  <link rel="canonical" href="{base}/news/apple-container">
  <link rel="alternate" type="application/rss+xml" title="Example News" href="{base}/news/feed.xml">
</head><body>
  <nav><a href="/">Home</a></nav>
  <article>
    <h1>Apple Container adds portable Linux VMs</h1>
    <p>Apple Container runs every Linux container inside its own lightweight virtual machine on macOS.</p>
    <p>The runtime makes compute portable across Macs and the cloud, and it speaks the OCI image format.</p>
  </article>
</body></html>"#;

/// A second, independent report of the same underlying change.
pub const APPLE_CONTAINER_SECOND_REPORT: &str = r#"<!doctype html>
<html><head>
  <title>Apple's Container tool brings Linux VMs to macOS</title>
  <meta property="og:title" content="Apple's Container tool brings Linux VMs to macOS">
</head><body>
  <main>
    <p>The Apple Container project runs each Linux container inside a lightweight virtual machine on macOS.</p>
    <p>Developers get portable compute with OCI images and no shared kernel.</p>
  </main>
</body></html>"#;

/// Unrelated to anything the tests' user cares about.
pub const BAKERY_ARTICLE: &str = r#"<!doctype html>
<html><head><title>Local bakery wins bread award</title></head>
<body><article><p>A small bakery won a regional prize for sourdough bread.</p></article></body></html>"#;

/// The feed advertised by the Apple Container article: the article itself,
/// one context-relevant entry, and one irrelevant entry.
pub const NEWS_FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Example News</title>
  <link>{base}/news</link>
  <description>News</description>
  <item>
    <guid>apple-container</guid>
    <title>Apple Container adds portable Linux VMs</title>
    <link>{base}/news/apple-container</link>
    <description>Apple Container runs every Linux container inside its own lightweight virtual machine.</description>
    <pubDate>Fri, 25 Sep 2026 09:00:00 GMT</pubDate>
  </item>
  <item>
    <guid>attn-release</guid>
    <title>Attn ships handoff reports for autonomous software</title>
    <link>{base}/news/attn-release</link>
    <description>Attn introduces handoff reports so autonomous agents can pass work to people.</description>
    <pubDate>Thu, 24 Sep 2026 09:00:00 GMT</pubDate>
  </item>
  <item>
    <guid>office-party</guid>
    <title>Photos from the office party</title>
    <link>{base}/news/office-party</link>
    <description>Our team had cake.</description>
    <pubDate>Wed, 23 Sep 2026 09:00:00 GMT</pubDate>
  </item>
</channel></rss>"#;
