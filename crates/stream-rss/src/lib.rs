use anyhow::Result;
use atom_syndication::Feed as AtomFeed;
use chrono::{DateTime, Utc};
use jsonfeed::{Content as JsonContent, Feed as JsonFeed};
use rss::Channel;
use stream_ingest::SourceAdapter;
use stream_model::{canonicalize_url, NormalizedItem, Source, SourceKind};
use url::Url;

/// Entry links are canonicalized so the same document observed through a
/// feed and as a web page (or with tracking parameters) is one item.
fn canonical_link(link: &str) -> Option<Url> {
    canonicalize_url(link).ok()
}

pub struct RssAdapter;
pub struct AtomAdapter;
pub struct JsonFeedAdapter;

impl SourceAdapter for RssAdapter {
    fn kind(&self) -> SourceKind {
        SourceKind::Rss
    }

    fn parse(&self, _source: &Source, body: &[u8], _fetched_at: DateTime<Utc>) -> Result<Vec<NormalizedItem>> {
        let channel = Channel::read_from(&body[..])?;
        channel
            .items()
            .iter()
            .map(|entry| {
                let raw_url = entry.link().and_then(|link| Url::parse(link).ok());
                let canonical_url = entry.link().and_then(canonical_link);
                let published_at = entry
                    .pub_date()
                    .and_then(|date| DateTime::parse_from_rfc2822(date).ok())
                    .map(|value| value.with_timezone(&Utc));
                let author = entry
                    .author()
                    .map(|author| author.split('(').next().unwrap_or(author).trim().to_owned())
                    .filter(|value| !value.is_empty());
                let title = entry.title().unwrap_or("untitled").trim().to_owned();
                let content_text = entry.description().unwrap_or_default().trim().to_owned();
                let original_identifier = entry
                    .guid()
                    .map(|guid| guid.value().to_owned())
                    .or_else(|| canonical_url.as_ref().map(|url| url.as_str().to_owned()))
                    .unwrap_or_else(|| title.clone());
                let source_url = raw_url.unwrap_or_else(|| Url::parse("https://example.invalid/").unwrap());
                Ok(NormalizedItem {
                    source_kind: SourceKind::Rss,
                    canonical_identity: canonical_url
                        .as_ref()
                        .map(|url| url.as_str().to_owned())
                        .unwrap_or_else(|| original_identifier.clone()),
                    canonical_url,
                    title,
                    content_text,
                    content_html: None,
                    author,
                    published_at,
                    original_identifier,
                    source_url,
                    parser: "rss".into(),
                    transformations: vec!["rss-normalized".into()],
                })
            })
            .collect()
    }
}

impl SourceAdapter for AtomAdapter {
    fn kind(&self) -> SourceKind {
        SourceKind::Atom
    }

    fn parse(&self, _source: &Source, body: &[u8], _fetched_at: DateTime<Utc>) -> Result<Vec<NormalizedItem>> {
        let feed = AtomFeed::read_from(&body[..])?;
        feed.entries()
            .iter()
            .map(|entry| {
                let raw_url = entry.links().first().and_then(|link| Url::parse(link.href()).ok());
                let canonical_url = entry.links().first().and_then(|link| canonical_link(link.href()));
                let published_at = entry
                    .published()
                    .cloned()
                    .or_else(|| Some(entry.updated().to_owned()))
                    .map(|value| value.with_timezone(&Utc));
                let source_url = raw_url.unwrap_or_else(|| Url::parse(entry.id()).unwrap_or_else(|_| Url::parse("https://example.invalid/").unwrap()));
                Ok(NormalizedItem {
                    source_kind: SourceKind::Atom,
                    canonical_identity: canonical_url
                        .as_ref()
                        .map(|url| url.as_str().to_owned())
                        .unwrap_or_else(|| entry.id().to_owned()),
                    canonical_url,
                    title: entry.title().to_string(),
                    content_text: entry
                        .summary()
                        .map(|summary| summary.to_string())
                        .or_else(|| entry.content().and_then(|content| content.value().map(ToOwned::to_owned)))
                        .unwrap_or_default(),
                    content_html: entry.content().and_then(|content| content.value().map(ToOwned::to_owned)),
                    author: entry.authors().first().map(|author| author.name().to_owned()),
                    published_at,
                    original_identifier: entry.id().to_owned(),
                    source_url,
                    parser: "atom".into(),
                    transformations: vec!["atom-normalized".into()],
                })
            })
            .collect()
    }
}

impl SourceAdapter for JsonFeedAdapter {
    fn kind(&self) -> SourceKind {
        SourceKind::JsonFeed
    }

    fn parse(&self, _source: &Source, body: &[u8], _fetched_at: DateTime<Utc>) -> Result<Vec<NormalizedItem>> {
        let feed: JsonFeed = serde_json::from_slice(body)?;
        feed.items
            .into_iter()
            .map(|entry| {
                let link = entry.url.as_ref().or(entry.external_url.as_ref());
                let canonical_url = link.and_then(|url| canonical_link(url));
                let source_url = link
                    .and_then(|url| Url::parse(url).ok())
                    .unwrap_or_else(|| Url::parse("https://example.invalid/").unwrap());
                let published_at = entry.date_published.as_ref().and_then(parse_rfc3339_datetime);
                let title = entry.title.unwrap_or_else(|| "untitled".into());
                let (content_text, content_html) = match entry.content {
                    JsonContent::Text(text) => (text, None),
                    JsonContent::Html(html) => (String::new(), Some(html)),
                    JsonContent::Both(html, text) => (text, Some(html)),
                };
                let original_identifier = entry.id.clone();
                Ok(NormalizedItem {
                    source_kind: SourceKind::JsonFeed,
                    canonical_identity: canonical_url
                        .as_ref()
                        .map(|url| url.as_str().to_owned())
                        .unwrap_or_else(|| original_identifier.clone()),
                    canonical_url,
                    title,
                    content_text,
                    content_html,
                    author: entry.author.and_then(author_name),
                    published_at,
                    original_identifier,
                    source_url,
                    parser: "json_feed".into(),
                    transformations: vec!["json-feed-normalized".into()],
                })
            })
            .collect()
    }
}

fn parse_rfc3339_datetime(value: &String) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value).ok().map(|value| value.with_timezone(&Utc))
}

fn author_name(author: jsonfeed::Author) -> Option<String> {
    serde_json::to_value(author)
        .ok()
        .and_then(|value| value.get("name").and_then(|value| value.as_str()).map(ToOwned::to_owned))
}

pub fn default_adapters() -> Vec<Box<dyn SourceAdapter>> {
    vec![Box::new(RssAdapter), Box::new(AtomAdapter), Box::new(JsonFeedAdapter)]
}

#[cfg(test)]
mod tests {
    use super::{AtomAdapter, JsonFeedAdapter, RssAdapter};
    use chrono::Utc;
    use std::fs;
    use std::path::PathBuf;
    use stream_ingest::SourceAdapter;
    use stream_model::{Source, SourceKind};
    use url::Url;

    fn fixture(name: &str) -> Vec<u8> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures")
            .join(name);
        fs::read(path).unwrap()
    }

    fn source(kind: SourceKind, endpoint: &str) -> Source {
        Source::new(kind, Url::parse(endpoint).unwrap())
    }

    #[test]
    fn parses_rss_items() {
        let items = RssAdapter
            .parse(&source(SourceKind::Rss, "https://example.com/feed.xml"), &fixture("sample.rss"), Utc::now())
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "Rust release");
        assert_eq!(items[0].parser, "rss");
    }

    #[test]
    fn parses_atom_items() {
        let items = AtomAdapter
            .parse(&source(SourceKind::Atom, "https://example.com/atom.xml"), &fixture("sample.atom"), Utc::now())
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "AppPort update");
        assert_eq!(items[0].parser, "atom");
    }

    #[test]
    fn parses_json_feed_items() {
        let items = JsonFeedAdapter
            .parse(
                &source(SourceKind::JsonFeed, "https://example.com/feed.json"),
                &fixture("sample.jsonfeed.json"),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "JSON Feed update");
        assert_eq!(items[0].parser, "json_feed");
    }
}
