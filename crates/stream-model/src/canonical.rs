use crate::SourceKind;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CanonicalUrlError {
    #[error("empty URL")]
    Empty,
    #[error("unsupported URL scheme '{0}': Stream observes http(s) URLs")]
    UnsupportedScheme(String),
    #[error("invalid URL: {0}")]
    Invalid(String),
}

/// Query parameters that identify a click, not a document.
const TRACKING_PARAMS: &[&str] = &[
    "fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid", "igshid", "ref", "ref_src", "ref_url", "_hsenc",
    "_hsmi", "yclid", "si",
];

/// Normalize a user-supplied URL into Stream's durable identity for it.
///
/// Two URLs that point at the same document through cosmetic differences —
/// scheme omitted, `www.`, tracking parameters, fragments, trailing slashes,
/// parameter order, `youtu.be` short links — canonicalize to the same value.
pub fn canonicalize_url(input: &str) -> Result<Url, CanonicalUrlError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(CanonicalUrlError::Empty);
    }
    let with_scheme = if trimmed.contains("://") {
        trimmed.to_owned()
    } else {
        format!("https://{trimmed}")
    };
    let mut url = Url::parse(&with_scheme).map_err(|error| CanonicalUrlError::Invalid(error.to_string()))?;
    match url.scheme() {
        "http" | "https" => {}
        other => return Err(CanonicalUrlError::UnsupportedScheme(other.to_owned())),
    }
    let host = url
        .host_str()
        .ok_or_else(|| CanonicalUrlError::Invalid("URL has no host".into()))?
        .to_ascii_lowercase();

    // youtu.be/<id> is the same document as youtube.com/watch?v=<id>.
    if host == "youtu.be" {
        let video = url.path().trim_matches('/').to_owned();
        if !video.is_empty() {
            url = Url::parse(&format!("https://youtube.com/watch?v={video}"))
                .map_err(|error| CanonicalUrlError::Invalid(error.to_string()))?;
        }
    }

    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let bare_host = host.strip_prefix("www.").unwrap_or(&host).to_owned();
    if bare_host != host {
        url.set_host(Some(&bare_host))
            .map_err(|error| CanonicalUrlError::Invalid(error.to_string()))?;
    }
    // Identity is scheme-independent: http and https name the same document.
    if url.scheme() == "http" {
        let _ = url.set_scheme("https");
    }
    if url.port() == Some(443) || url.port() == Some(80) {
        let _ = url.set_port(None);
    }
    url.set_fragment(None);
    let _ = url.set_username("");
    let _ = url.set_password(None);

    let mut params = url
        .query_pairs()
        .filter(|(key, _)| {
            let key = key.to_ascii_lowercase();
            !key.starts_with("utm_") && !TRACKING_PARAMS.contains(&key.as_str())
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    params.sort();
    if params.is_empty() {
        url.set_query(None);
    } else {
        url.query_pairs_mut().clear().extend_pairs(params);
    }

    let path = url.path().to_owned();
    if path.len() > 1 && path.ends_with('/') {
        url.set_path(path.trim_end_matches('/'));
    }
    Ok(url)
}

/// Classify what a URL *is* for the user, independent of how it is observed.
///
/// This is only a first guess from the URL itself; observation refines it
/// (for example a URL that returns an RSS document becomes an RSS source).
pub fn classify_url(url: &Url) -> SourceKind {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    let path = url.path().to_ascii_lowercase();

    if host == "github.com" || host.ends_with(".github.com") {
        return SourceKind::Github;
    }
    if host == "youtube.com" || host.ends_with(".youtube.com") || host == "youtu.be" {
        return SourceKind::Youtube;
    }
    if host == "arxiv.org"
        || host.ends_with(".arxiv.org")
        || host == "doi.org"
        || host.ends_with("biorxiv.org")
        || host.ends_with("openreview.net")
        || host.ends_with("semanticscholar.org")
        || path.ends_with(".pdf")
    {
        return SourceKind::Research;
    }
    if host.starts_with("docs.")
        || host.ends_with(".readthedocs.io")
        || host == "docs.rs"
        || path.starts_with("/docs/")
        || path == "/docs"
        || path.contains("/documentation/")
    {
        return SourceKind::Documentation;
    }
    if path.ends_with(".rss") || path.ends_with("/rss") || path.ends_with("/rss.xml") {
        return SourceKind::Rss;
    }
    if path.ends_with(".atom") || path.ends_with("/atom.xml") {
        return SourceKind::Atom;
    }
    if path.ends_with("feed.json") {
        return SourceKind::JsonFeed;
    }
    SourceKind::Web
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equivalent_urls_share_a_canonical_identity() {
        let variants = [
            "https://www.Example.com/posts/rust-release/?utm_source=x&b=2&a=1#section",
            "example.com/posts/rust-release?a=1&b=2",
            "http://example.com:80/posts/rust-release?fbclid=abc&a=1&b=2",
            "  https://example.com/posts/rust-release/?b=2&a=1  ",
        ];
        let canonical = variants
            .iter()
            .map(|value| canonicalize_url(value).unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(canonical.iter().all(|value| value == "https://example.com/posts/rust-release?a=1&b=2"), "{canonical:?}");
    }

    #[test]
    fn distinct_documents_stay_distinct() {
        let a = canonicalize_url("https://example.com/a").unwrap();
        let b = canonicalize_url("https://example.com/b").unwrap();
        let q = canonicalize_url("https://example.com/a?id=2").unwrap();
        assert_ne!(a, b);
        assert_ne!(a, q);
    }

    #[test]
    fn youtube_short_links_resolve_to_watch_urls() {
        assert_eq!(
            canonicalize_url("https://youtu.be/abc123?si=tracking").unwrap().as_str(),
            "https://youtube.com/watch?v=abc123"
        );
    }

    #[test]
    fn rejects_non_web_urls() {
        assert_eq!(canonicalize_url("   "), Err(CanonicalUrlError::Empty));
        assert!(matches!(canonicalize_url("ftp://example.com/x"), Err(CanonicalUrlError::UnsupportedScheme(_))));
    }

    #[test]
    fn classification_does_not_require_the_user_to_know_the_format() {
        let kind = |value: &str| classify_url(&canonicalize_url(value).unwrap());
        assert_eq!(kind("https://github.com/rust-lang/rust/releases"), SourceKind::Github);
        assert_eq!(kind("https://youtu.be/abc"), SourceKind::Youtube);
        assert_eq!(kind("https://arxiv.org/abs/2401.00001"), SourceKind::Research);
        assert_eq!(kind("https://docs.example.com/guide"), SourceKind::Documentation);
        assert_eq!(kind("https://example.com/feed.rss"), SourceKind::Rss);
        assert_eq!(kind("https://example.com/article"), SourceKind::Web);
    }
}
