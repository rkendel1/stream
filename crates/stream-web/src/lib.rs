//! Web page understanding for Stream: turn an HTML document into the
//! canonical item model, and discover feeds a page advertises so the source
//! can be observed over time.
//!
//! This is not a crawler. It reads exactly the document it is given.

use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, Utc};
use scraper::{ElementRef, Html, Selector};
use stream_ingest::{DocumentFormat, SourceAdapter};
use stream_model::{canonicalize_url, normalize_whitespace, NormalizedItem, Source, SourceKind};
use url::Url;

/// Main-text budget per page. Enough to understand the change, bounded so a
/// single page cannot dominate storage.
pub const MAX_CONTENT_CHARS: usize = 20_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredFeed {
    pub url: Url,
    pub format: DocumentFormat,
    pub title: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WebPage {
    pub url: Url,
    pub canonical_url: Url,
    pub title: String,
    pub site_name: Option<String>,
    pub description: Option<String>,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub text: String,
    pub feeds: Vec<DiscoveredFeed>,
}

impl WebPage {
    pub fn to_normalized_item(&self, kind: SourceKind) -> NormalizedItem {
        let content_text = match &self.description {
            Some(description) if !self.text.contains(description.as_str()) => {
                format!("{}\n{}", description, self.text).trim().to_owned()
            }
            _ => self.text.clone(),
        };
        NormalizedItem {
            source_kind: kind,
            canonical_identity: self.canonical_url.as_str().to_owned(),
            canonical_url: Some(self.canonical_url.clone()),
            title: self.title.clone(),
            content_text,
            content_html: None,
            author: self.author.clone(),
            published_at: self.published_at,
            original_identifier: self.url.as_str().to_owned(),
            source_url: self.url.clone(),
            parser: "web".into(),
            transformations: vec!["html-main-text".into(), "web-normalized".into()],
        }
    }
}

fn selector(value: &str) -> Selector {
    Selector::parse(value).expect("static selector must parse")
}

fn meta_content(document: &Html, names: &[&str]) -> Option<String> {
    let meta = selector("meta");
    for name in names {
        for element in document.select(&meta) {
            let value = element.value();
            let key = value.attr("property").or_else(|| value.attr("name")).or_else(|| value.attr("itemprop"));
            if key.map(|key| key.eq_ignore_ascii_case(name)).unwrap_or(false) {
                if let Some(content) = value.attr("content") {
                    let content = normalize_whitespace(content);
                    if !content.is_empty() {
                        return Some(content);
                    }
                }
            }
        }
    }
    None
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .ok()
        .or_else(|| DateTime::parse_from_rfc2822(value).map(|value| value.with_timezone(&Utc)).ok())
        .or_else(|| {
            NaiveDate::parse_from_str(value.get(..10)?, "%Y-%m-%d")
                .ok()?
                .and_hms_opt(0, 0, 0)
                .map(|value| value.and_utc())
        })
}

const SKIPPED_ANCESTORS: &[&str] = &["nav", "footer", "header", "aside", "script", "style", "noscript", "form", "template"];

fn is_boilerplate(element: &ElementRef) -> bool {
    element.ancestors().any(|node| {
        node.value()
            .as_element()
            .map(|element| SKIPPED_ANCESTORS.contains(&element.name()))
            .unwrap_or(false)
    })
}

fn main_text(document: &Html) -> String {
    let roots = ["article", "main", "[role=main]", "body"];
    let blocks = selector("h1, h2, h3, h4, p, li, blockquote, pre, figcaption, dd");
    for root_selector in roots {
        let Some(root) = document.select(&selector(root_selector)).next() else {
            continue;
        };
        let mut lines: Vec<String> = Vec::new();
        let mut length = 0;
        for block in root.select(&blocks) {
            if is_boilerplate(&block) {
                continue;
            }
            // Nested blocks (a <p> inside an <li>) are covered by the outer block.
            if block.ancestors().skip(1).filter_map(ElementRef::wrap).any(|ancestor| {
                matches!(ancestor.value().name(), "p" | "li" | "blockquote" | "pre" | "dd")
            }) {
                continue;
            }
            let text = normalize_whitespace(&block.text().collect::<Vec<_>>().join(" "));
            if text.is_empty() || lines.last() == Some(&text) {
                continue;
            }
            length += text.len() + 1;
            lines.push(text);
            if length >= MAX_CONTENT_CHARS {
                break;
            }
        }
        if !lines.is_empty() {
            let mut joined = lines.join("\n");
            if joined.len() > MAX_CONTENT_CHARS {
                let mut cut = MAX_CONTENT_CHARS;
                while !joined.is_char_boundary(cut) {
                    cut -= 1;
                }
                joined.truncate(cut);
            }
            return joined;
        }
    }
    String::new()
}

fn discover_feeds(document: &Html, base: &Url) -> Vec<DiscoveredFeed> {
    let mut feeds = Vec::new();
    for link in document.select(&selector("link[href]")) {
        let value = link.value();
        let rel = value.attr("rel").unwrap_or_default().to_ascii_lowercase();
        if !rel.split_whitespace().any(|token| token == "alternate") {
            continue;
        }
        let kind = value.attr("type").unwrap_or_default().to_ascii_lowercase();
        let format = if kind.contains("rss") {
            DocumentFormat::Rss
        } else if kind.contains("atom") {
            DocumentFormat::Atom
        } else if kind.contains("feed+json") || kind == "application/json" && value.attr("title").is_some() {
            DocumentFormat::JsonFeed
        } else {
            continue;
        };
        let Some(url) = value.attr("href").and_then(|href| base.join(href).ok()) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https") || feeds.iter().any(|feed: &DiscoveredFeed| feed.url == url) {
            continue;
        }
        feeds.push(DiscoveredFeed {
            url,
            format,
            title: value.attr("title").map(normalize_whitespace).filter(|title| !title.is_empty()),
        });
    }
    feeds
}

/// Parse an HTML document fetched from `url`.
pub fn parse_page(url: &Url, html: &str) -> Result<WebPage> {
    let document = Html::parse_document(html);

    let title = meta_content(&document, &["og:title", "twitter:title"])
        .or_else(|| {
            document
                .select(&selector("title"))
                .next()
                .map(|element| normalize_whitespace(&element.text().collect::<String>()))
        })
        .or_else(|| {
            document
                .select(&selector("h1"))
                .next()
                .map(|element| normalize_whitespace(&element.text().collect::<String>()))
        })
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| url.as_str().to_owned());

    let canonical_url = document
        .select(&selector("link[rel=canonical][href]"))
        .next()
        .and_then(|link| link.value().attr("href"))
        .and_then(|href| url.join(href).ok())
        .or_else(|| meta_content(&document, &["og:url"]).and_then(|href| url.join(&href).ok()))
        .and_then(|candidate| canonicalize_url(candidate.as_str()).ok())
        .or_else(|| canonicalize_url(url.as_str()).ok())
        .ok_or_else(|| anyhow!("page URL cannot be canonicalized: {url}"))?;

    let published_at = meta_content(
        &document,
        &["article:published_time", "og:published_time", "datePublished", "pubdate", "date", "dc.date"],
    )
    .and_then(|value| parse_datetime(&value))
    .or_else(|| {
        document
            .select(&selector("time[datetime]"))
            .next()
            .and_then(|element| element.value().attr("datetime"))
            .and_then(parse_datetime)
    });

    Ok(WebPage {
        url: url.clone(),
        canonical_url,
        title,
        site_name: meta_content(&document, &["og:site_name", "application-name"]),
        description: meta_content(&document, &["description", "og:description", "twitter:description"]),
        author: meta_content(&document, &["author", "article:author", "twitter:creator"]),
        published_at,
        text: main_text(&document),
        feeds: discover_feeds(&document, url),
    })
}

/// Normalizes a single web page into one canonical item.
pub struct WebPageAdapter;

impl SourceAdapter for WebPageAdapter {
    fn kind(&self) -> SourceKind {
        SourceKind::Web
    }

    fn parse(&self, source: &Source, body: &[u8], _fetched_at: DateTime<Utc>) -> Result<Vec<NormalizedItem>> {
        let page = parse_page(&source.endpoint, &String::from_utf8_lossy(body))?;
        Ok(vec![page.to_normalized_item(source.kind)])
    }
}

pub fn web_adapters() -> Vec<Box<dyn SourceAdapter>> {
    vec![Box::new(WebPageAdapter)]
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = r#"<!doctype html>
<html><head>
  <title>Fallback title</title>
  <meta property="og:title" content="Apple Container adds portable Linux runtime">
  <meta name="description" content="Apple's container tool now runs Linux workloads portably.">
  <meta property="article:published_time" content="2026-09-20T10:00:00Z">
  <meta name="author" content="Jane Doe">
  <link rel="canonical" href="/news/apple-container?utm_source=feed">
  <link rel="alternate" type="application/rss+xml" title="News" href="/feed.xml">
  <link rel="alternate" type="application/atom+xml" href="https://example.com/atom.xml">
</head><body>
  <nav><p>Home · About · Subscribe</p></nav>
  <article>
    <h1>Apple Container adds portable Linux runtime</h1>
    <p>Apple Container introduces lightweight virtual machines for each container.</p>
    <p>The change makes compute portable across Macs.</p>
    <ul><li>Adds OCI image support</li></ul>
  </article>
  <footer><p>Copyright</p></footer>
</body></html>"#;

    #[test]
    fn parses_page_metadata_and_main_text() {
        let url = Url::parse("https://www.example.com/news/apple-container?utm_medium=x").unwrap();
        let page = parse_page(&url, PAGE).unwrap();
        assert_eq!(page.title, "Apple Container adds portable Linux runtime");
        assert_eq!(page.canonical_url.as_str(), "https://example.com/news/apple-container");
        assert_eq!(page.author.as_deref(), Some("Jane Doe"));
        assert!(page.published_at.is_some());
        assert!(page.text.contains("lightweight virtual machines"));
        assert!(page.text.contains("Adds OCI image support"));
        assert!(!page.text.contains("Subscribe"), "navigation is boilerplate");
        assert!(!page.text.contains("Copyright"), "footer is boilerplate");
    }

    #[test]
    fn discovers_advertised_feeds() {
        let url = Url::parse("https://example.com/news/apple-container").unwrap();
        let page = parse_page(&url, PAGE).unwrap();
        assert_eq!(page.feeds.len(), 2);
        assert_eq!(page.feeds[0].url.as_str(), "https://example.com/feed.xml");
        assert_eq!(page.feeds[0].format, DocumentFormat::Rss);
        assert_eq!(page.feeds[1].format, DocumentFormat::Atom);
    }

    #[test]
    fn normalizes_into_the_canonical_item_model() {
        let url = Url::parse("https://example.com/news/apple-container").unwrap();
        let item = parse_page(&url, PAGE).unwrap().to_normalized_item(SourceKind::Web);
        assert_eq!(item.parser, "web");
        assert_eq!(item.canonical_identity, "https://example.com/news/apple-container");
        assert!(item.content_text.starts_with("Apple's container tool"));
    }
}
