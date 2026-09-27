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

/// Test runtimes talk to local fixture servers, so they opt into private
/// (loopback) addresses; the default policy refuses them.
pub fn runtime_at(isolated: &Isolated) -> StreamRuntime {
    StreamRuntime::new(FeltDbStore::new(isolated.config()), adapters())
        .with_network_policy(stream_ingest::NetworkPolicy::default().allowing_private_network())
}

#[derive(Clone)]
struct Response {
    status: u16,
    content_type: String,
    body: String,
    retry_after: Option<u64>,
}

/// A local HTTP server with mutable routes.
#[derive(Clone)]
pub struct FixtureServer {
    base: String,
    routes: Arc<Mutex<HashMap<String, Response>>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl FixtureServer {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        let routes: Arc<Mutex<HashMap<String, Response>>> = Default::default();
        let shared = routes.clone();
        let log: Arc<Mutex<Vec<String>>> = Default::default();
        let shared_log = log.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let routes = shared.clone();
                let log = shared_log.clone();
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
                    log.lock().unwrap().push(path.clone());
                    let response = routes.lock().unwrap().get(&path).cloned().unwrap_or(Response {
                        status: 404,
                        content_type: "text/plain".into(),
                        body: "not found".into(),
                        retry_after: None,
                    });
                    let retry_after = response.retry_after.map(|s| format!("Retry-After: {s}\r\n")).unwrap_or_default();
                    let reason = if response.status == 200 { "OK" } else { "Error" };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n{}",
                        response.status,
                        reason,
                        response.content_type,
                        response.body.len(),
                        retry_after,
                        response.body
                    );
                });
            }
        });
        Self { base, routes, log }
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
                retry_after: None,
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
            Response { status, content_type: "text/plain".into(), body: "failure".into(), retry_after: None },
        );
        self.url(path)
    }

    /// Answer 429 with a Retry-After header.
    pub fn rate_limited(&self, path: &str, retry_after_seconds: u64) -> String {
        self.routes.lock().unwrap().insert(
            path.to_owned(),
            Response { status: 429, content_type: "text/plain".into(), body: "slow down".into(), retry_after: Some(retry_after_seconds) },
        );
        self.url(path)
    }

    /// Every path requested so far, in order.
    pub fn requested(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
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

/// How the fake model behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelMode {
    /// Grounded structured output, plus one fabricated claim for the gate to refuse.
    Cooperative,
    /// HTTP 500 on every request.
    Failing,
    /// Prose instead of structured output.
    Malformed,
}

/// A local OpenAI-compatible chat completions server standing in for a model,
/// so the real HTTP provider path is exercised without a real model.
#[derive(Clone)]
pub struct FakeModelServer {
    base: String,
    mode: Arc<Mutex<ModelMode>>,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
}

fn completion(content: &str) -> String {
    serde_json::json!({
        "id": "fake", "object": "chat.completion",
        "choices": [{ "index": 0, "finish_reason": "stop", "message": { "role": "assistant", "content": content } }]
    })
    .to_string()
}

fn user_payload(request: &serde_json::Value) -> serde_json::Value {
    let text = request["messages"][1]["content"].as_str().unwrap_or_default();
    let json = text.find('{').map(|i| &text[i..]).unwrap_or("{}");
    serde_json::from_str(json).unwrap_or_default()
}

fn first_sentence(text: &str) -> String {
    text.split(['.', '\n']).map(str::trim).find(|s| s.split_whitespace().count() >= 4).unwrap_or(text).to_owned()
}

fn fake_interpretation(request: &serde_json::Value) -> String {
    let payload = user_payload(request);
    let title = payload["item"]["title"].as_str().unwrap_or_default().to_owned();
    let content = payload["item"]["content"].as_str().unwrap_or_default().to_owned();
    let haystack = format!("{title} {content}").to_lowercase();
    let words = title.split_whitespace().collect::<Vec<_>>();
    let subject = words.iter().take(2).cloned().collect::<Vec<_>>().join(" ").trim_start_matches("Apple's").trim().to_owned();
    let subject = if subject.is_empty() { title.clone() } else { subject };
    let sentence = first_sentence(&content);
    let quote = |text: &str, location: &str| serde_json::json!({ "text": text, "location": location });
    let mut connections = Vec::new();
    let mut context_ids = Vec::new();
    for context in payload["user_contexts"].as_array().cloned().unwrap_or_default() {
        let name = context["name"].as_str().unwrap_or_default().to_lowercase();
        if name.split_whitespace().any(|word| word.len() > 3 && haystack.contains(word)) {
            let id = context["id"].as_str().unwrap_or_default().to_owned();
            connections.push(serde_json::json!({
                "context_id": id, "strength": 0.8,
                "explanation": format!("The source describes {} in terms of {}.", subject, context["name"].as_str().unwrap_or_default()),
                "quotes": [quote(&title, "title")]
            }));
            context_ids.push(id);
        }
    }
    let related = payload["existing_stream_items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|prior| prior["title"].as_str().unwrap_or_default().to_lowercase().contains("container"))
        .map(|prior| serde_json::json!({
            "item_id": prior["id"], "relation": "same_change", "strength": 0.9,
            "explanation": "Both report per-container Linux VMs in Apple Container.",
            "quotes": [quote(&title, "title")]
        }))
        .collect::<Vec<_>>();
    serde_json::json!({
        "topic": { "label": "Compute & Infrastructure", "quotes": [quote(&title, "title")] },
        "subject": { "label": subject, "quotes": [quote(&title, "title")] },
        "change": { "kind": "adds", "statement": format!("Adds {}", words.iter().skip(2).cloned().collect::<Vec<_>>().join(" ")), "quotes": [quote(&title, "title")] },
        "why_it_matters": if context_ids.is_empty() { serde_json::Value::Null } else { serde_json::json!({
            "text": "It may let you drop a custom isolation layer from the runtime you are building.",
            "context_ids": context_ids, "quotes": [quote(&sentence, "content")] }) },
        "context_connections": connections,
        "related_items": related,
        "claims": [
            { "basis": "observed", "statement": format!("{subject} runs Linux containers in lightweight VMs."), "rationale": "stated by the source",
              "confidence": 0.9, "context_ids": [], "item_ids": [], "quotes": [quote(&sentence, "content")] },
            { "basis": "inferred", "statement": "Per-container VMs may remove the need for a separate isolation layer.", "rationale": "follows from the VM boundary",
              "confidence": 0.6, "context_ids": context_ids, "item_ids": [], "quotes": [quote(&sentence, "content")] },
            { "basis": "hypothesis", "statement": "The runtime you are building could drop its sandbox entirely.", "rationale": "if VM isolation holds across platforms",
              "confidence": 0.3, "context_ids": context_ids, "item_ids": [], "quotes": [] },
            { "basis": "observed", "statement": "Apple acquired Docker.", "rationale": "fabricated for the evidence gate",
              "confidence": 0.99, "context_ids": [], "item_ids": [], "quotes": [quote("Apple announced it has acquired Docker", "content")] }
        ]
    })
    .to_string()
}

fn fake_synthesis(request: &serde_json::Value) -> String {
    let payload = user_payload(request);
    let observations = payload["observations"].as_array().cloned().unwrap_or_default();
    let firsts = observations.iter().filter_map(|o| o["evidence"][0]["id"].as_str().map(ToOwned::to_owned)).collect::<Vec<_>>();
    serde_json::json!({
        "agreements": [
            { "statement": format!("{} sources agree Apple Container runs Linux containers in VMs.", firsts.len()), "basis": "observed", "evidence_ids": firsts },
            { "statement": "Every analyst agrees this ends Docker.", "basis": "observed", "evidence_ids": ["evidence_that_does_not_exist"] }
        ],
        "new_information": [],
        "differences": [],
        "uncertainties": [{ "statement": "Whether this ships beyond macOS is unknown.", "basis": "hypothesis", "evidence_ids": [] }]
    })
    .to_string()
}

fn fake_answer(request: &serde_json::Value) -> String {
    let payload = user_payload(request);
    let evidence = payload["evidence"].as_array().cloned().unwrap_or_default();
    let first = evidence.first().and_then(|e| e["id"].as_str()).map(ToOwned::to_owned);
    let mut statements = Vec::new();
    if let Some(id) = &first {
        statements.push(serde_json::json!({ "text": "The sources report per-container Linux VMs in Apple Container.", "basis": "observed", "evidence_ids": [id], "signal_ids": [], "context_ids": [] }));
        statements.push(serde_json::json!({ "text": "That overlaps with the portable runtime you are building.", "basis": "inferred", "evidence_ids": [id], "signal_ids": [], "context_ids": [] }));
    }
    statements.push(serde_json::json!({ "text": "Apple will open-source macOS next year.", "basis": "observed", "evidence_ids": ["evidence_invented"], "signal_ids": [], "context_ids": [] }));
    statements.push(serde_json::json!({ "text": "You might be able to remove your sandbox layer.", "basis": "hypothesis", "evidence_ids": [], "signal_ids": [], "context_ids": [] }));
    serde_json::json!({
        "summary": "Apple Container's per-container VMs overlap with your portable runtime work.",
        "sufficiency": if first.is_some() { "sufficient" } else { "insufficient" },
        "statements": statements,
        "uncertainties": ["Only macOS is covered by the sources."],
        "follow_ups": ["What evidence supports that?"]
    })
    .to_string()
}

impl FakeModelServer {
    pub fn start(mode: ModelMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake model");
        let base = format!("http://{}/v1", listener.local_addr().expect("addr"));
        let mode = Arc::new(Mutex::new(mode));
        let requests: Arc<Mutex<Vec<serde_json::Value>>> = Default::default();
        let (shared_mode, shared_requests) = (mode.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mode = *shared_mode.lock().unwrap();
                let requests = shared_requests.clone();
                std::thread::spawn(move || {
                    use std::io::Read;
                    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line);
                    let mut length = 0usize;
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                            break;
                        }
                        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                            length = value.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; length];
                    let _ = reader.read_exact(&mut body);
                    let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
                    requests.lock().unwrap().push(request.clone());
                    let schema = request["response_format"]["json_schema"]["name"].as_str().unwrap_or_default().to_owned();
                    let (status, payload) = match mode {
                        ModelMode::Failing => (500, "{\"error\":\"model crashed\"}".to_owned()),
                        ModelMode::Malformed => (200, completion("Sure! This article is about containers and it matters a lot.")),
                        ModelMode::Cooperative => (200, completion(&match schema.as_str() {
                            "stream_interpretation" => fake_interpretation(&request),
                            "stream_synthesis" => fake_synthesis(&request),
                            "stream_answer" => fake_answer(&request),
                            _ => "{}".to_owned(),
                        })),
                    };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        payload.len(),
                        payload
                    );
                });
            }
        });
        Self { base, mode, requests }
    }

    pub fn base_url(&self) -> url::Url {
        url::Url::parse(&self.base).expect("fake model url")
    }

    pub fn set_mode(&self, mode: ModelMode) {
        *self.mode.lock().unwrap() = mode;
    }

    pub fn requests(&self) -> Vec<serde_json::Value> {
        self.requests.lock().unwrap().clone()
    }

    pub fn provider(&self) -> std::sync::Arc<dyn stream_core::ModelProviderHandle> {
        std::sync::Arc::new(stream_core::OpenAiCompatibleProvider::new(
            self.base_url(),
            "fake-model",
            None,
            std::time::Duration::from_secs(10),
        ))
    }
}

/// A runtime whose intelligence is backed by the given fake model.
pub fn model_runtime_at(isolated: &Isolated, model: &FakeModelServer) -> StreamRuntime {
    runtime_at(isolated).with_model_provider(model.provider())
}

/// A third independent report of the same change, adding a new detail.
pub const APPLE_CONTAINER_THIRD_REPORT: &str = r#"<!doctype html>
<html><head><title>Apple Container gives each Linux container its own VM</title></head>
<body><article>
  <p>Apple Container gives every Linux container its own lightweight virtual machine on macOS.</p>
  <p>The project is open source under the Apache 2.0 license and boots containers in under a second.</p>
</article></body></html>"#;

/// A report that disputes the change.
pub const APPLE_CONTAINER_DENIAL: &str = r#"<!doctype html>
<html><head><title>Apple Container Linux VM support delayed</title></head>
<body><article>
  <p>Apple denies that Apple Container runs each Linux container in a lightweight virtual machine yet; the feature was delayed.</p>
</article></body></html>"#;

/// A small product site with the surfaces discovery should find, and some
/// it should not (pricing, about, a robots-disallowed section, other sites).
pub fn product_site(server: &FixtureServer, with_changelog_link: bool) {
    let changelog_link = if with_changelog_link { r#"<a href="/changelog">Changelog</a>"# } else { "" };
    server.html(
        "/",
        &format!(
            r#"<!doctype html><html><head><title>Widget Co — portable compute runtime</title>
<link rel="alternate" type="application/rss+xml" title="Widget Co" href="/feed.xml">
</head><body>
<nav><a href="/">Home</a> <a href="/blog">Blog</a> {changelog_link} <a href="/docs">Docs</a> <a href="/pricing">Pricing</a> <a href="/about">About</a> <a href="/private/updates">Updates</a></nav>
<main><h1>Widget runs your workloads anywhere</h1>
<p>Widget is a portable compute runtime with lightweight VMs for every workload.</p>
<p>Source code on <a href="https://github.com/widgetco/widget">GitHub</a>. Also see <a href="https://other.example.net/blog">a partner blog</a>.</p></main>
</body></html>"#
        ),
    );
    server.route("/robots.txt", "text/plain", "User-agent: *\nDisallow: /private\nSitemap: {base}/sitemap.xml\n");
    server.route(
        "/sitemap.xml",
        "application/xml",
        r#"<?xml version="1.0"?><urlset>
<url><loc>{base}/docs/intro</loc></url><url><loc>{base}/docs/api</loc></url><url><loc>{base}/docs/cli</loc></url>
<url><loc>{base}/research/paper-1</loc></url><url><loc>{base}/about</loc></url></urlset>"#,
    );
    server.html(
        "/blog",
        r#"<!doctype html><html><head><title>Widget Blog</title>
<link rel="alternate" type="application/atom+xml" href="/blog/atom.xml"></head>
<body><main><h1>Widget Blog</h1><ul><li><a href="/blog/hello-widget">Hello, Widget</a></li></ul></main></body></html>"#,
    );
    server.html("/blog/hello-widget", r#"<!doctype html><html><head><title>Hello, Widget</title></head><body><article><p>Widget is here to run workloads anywhere.</p></article></body></html>"#);
    server.html("/docs", r#"<!doctype html><html><head><title>Widget Docs</title></head><body><main><h1>Docs</h1><p>Install Widget with one command.</p></main></body></html>"#);
    server.html("/pricing", "<html><body><p>Pricing</p></body></html>");
    server.html("/about", "<html><body><p>About</p></body></html>");
    server.html(
        "/changelog",
        r#"<!doctype html><html><head><title>Widget Changelog</title></head><body><main><h1>Changelog</h1>
<h2>Version 1.4.1</h2><p>Fixes a crash when starting VMs on older Macs.</p></main></body></html>"#,
    );
    server.route("/feed.xml", "application/rss+xml", &widget_feed(&[WIDGET_POST_WELCOME]));
    server.route("/blog/atom.xml", "application/atom+xml", r#"<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://www.w3.org/2005/Atom"><title>Widget Blog</title><id>{base}/blog</id><updated>2026-09-01T00:00:00Z</updated></feed>"#);
}

/// (guid, title, description, pubDate) of a feed entry.
pub type FeedEntry<'a> = (&'a str, &'a str, &'a str, &'a str);

pub const WIDGET_POST_WELCOME: FeedEntry<'static> = (
    "welcome",
    "Welcome to the Widget blog",
    "We will write about Widget here.",
    "Mon, 01 Sep 2026 09:00:00 GMT",
);

pub const WIDGET_POST_TWO_ZERO: FeedEntry<'static> = (
    "widget-2",
    "Introducing Widget 2.0",
    "Widget 2.0 introduces portable compute snapshots that move running workloads between machines.",
    "Sat, 26 Sep 2026 09:00:00 GMT",
);

/// An RSS feed of Widget blog posts, links resolved against `{base}`.
pub fn widget_feed(entries: &[FeedEntry<'_>]) -> String {
    let items = entries
        .iter()
        .map(|(guid, title, description, date)| {
            format!(
                "<item><guid>{guid}</guid><title>{title}</title><link>{{base}}/blog/{guid}</link><description>{description}</description><pubDate>{date}</pubDate></item>"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Widget Co</title><link>{{base}}/</link><description>News</description>{items}</channel></rss>"#)
}
