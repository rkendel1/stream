//! Bounded, explainable discovery of the information surfaces downstream of
//! a URL the user asked Stream to watch.
//!
//! This is not a crawler. From one seed it looks at: the feeds the page
//! declares, a few conventional feed locations, robots.txt and the sitemap,
//! the page's own links to obvious sections (blog, changelog, docs, ...),
//! linked GitHub repositories, and — one hop only — the feeds those sections
//! declare. Every surface it reports says how it was found, from where, how
//! confident Stream is, and why.

mod resolver;

pub use resolver::{x_account, DiscoveryPlan, DiscoveryStrategy, ObservationTargetResolver, ProviderEndpoints};

use std::collections::{BTreeMap, HashSet};
use stream_ingest::{detect_format, DocumentFormat, FetchError, HttpFetcher};
use stream_model::{canonicalize_url, DiscoveryConfidence, DiscoveryMethod, SourceKind, SurfaceKind};
use url::Url;

#[derive(Debug, Clone)]
pub struct DiscoveryPolicy {
    /// Most surfaces one discovery may report (including the seed).
    pub max_surfaces: usize,
    /// Section pages fetched to look for their own feeds (one hop).
    pub max_section_fetches: usize,
    /// Conventional feed locations probed when the seed declares none.
    pub max_feed_probes: usize,
    /// Sitemap URLs scanned (not fetched) for section roots.
    pub max_sitemap_urls: usize,
    /// Linked GitHub repositories considered.
    pub max_repositories: usize,
}

impl Default for DiscoveryPolicy {
    fn default() -> Self {
        Self { max_surfaces: 30, max_section_fetches: 6, max_feed_probes: 6, max_sitemap_urls: 5_000, max_repositories: 2 }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredSurface {
    /// Canonical identity of the surface.
    pub url: Url,
    /// Where to fetch it.
    pub fetch_url: Url,
    pub title: Option<String>,
    pub kind: SurfaceKind,
    pub adapter: SourceKind,
    pub method: DiscoveryMethod,
    pub confidence: DiscoveryConfidence,
    pub discovered_from: Url,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct Robots {
    pub disallow: Vec<String>,
    pub allow: Vec<String>,
    pub sitemaps: Vec<String>,
}

impl Robots {
    /// Longest-match allow/disallow for `User-agent: *`.
    pub fn allows(&self, path: &str) -> bool {
        let best = |rules: &[String]| rules.iter().filter(|rule| !rule.is_empty() && path.starts_with(rule.as_str())).map(|r| r.len()).max();
        match (best(&self.disallow), best(&self.allow)) {
            (Some(deny), Some(allow)) => allow >= deny,
            (Some(_), None) => false,
            _ => true,
        }
    }
}

#[derive(Debug, Default)]
pub struct DiscoveryReport {
    pub seed: Option<Url>,
    pub seed_title: Option<String>,
    pub seed_reachable: bool,
    pub surfaces: Vec<DiscoveredSurface>,
    /// Surfaces seen but not registered, and why.
    pub skipped: Vec<(Url, String)>,
    /// Fetch failures encountered along the way (none of them fatal except the seed).
    pub failures: Vec<(Url, FetchError)>,
    /// The target cannot currently be observed at all, and why (e.g. a
    /// provider without a public observation mechanism).
    pub unavailable: Option<String>,
}

impl DiscoveryReport {
    pub fn seed_failure(&self) -> Option<&FetchError> {
        let seed = self.seed.as_ref()?;
        self.failures.iter().find(|(url, _)| url == seed).map(|(_, error)| error)
    }
}

/// Keywords that mark a path segment or anchor as an information surface.
const SECTIONS: &[(SurfaceKind, &[&str])] = &[
    (SurfaceKind::Changelog, &["changelog", "changelogs", "change-log", "changes", "release-notes", "releasenotes", "whats-new", "what-s-new", "whatsnew"]),
    (SurfaceKind::Releases, &["releases", "release", "downloads"]),
    (SurfaceKind::Blog, &["blog", "blogs", "posts", "articles", "journal", "writing"]),
    (SurfaceKind::News, &["news", "press", "newsroom", "media"]),
    (SurfaceKind::Announcements, &["announcements", "announce", "announcement"]),
    (SurfaceKind::Updates, &["updates", "update"]),
    (SurfaceKind::Documentation, &["docs", "documentation", "doc", "guide", "guides", "manual", "reference", "developers", "developer", "api-docs"]),
    (SurfaceKind::Status, &["status"]),
    (SurfaceKind::Research, &["research", "papers", "publications"]),
    (SurfaceKind::Engineering, &["engineering", "tech-blog", "techblog"]),
    (SurfaceKind::Roadmap, &["roadmap"]),
    (SurfaceKind::Product, &["product", "products", "features"]),
    (SurfaceKind::Projects, &["projects", "open-source", "opensource", "oss"]),
];

const CONVENTIONAL_FEEDS: &[&str] = &["/feed.xml", "/rss.xml", "/atom.xml", "/index.xml", "/feed", "/rss", "/feed.json", "/blog/feed.xml"];

fn section_kind(word: &str) -> Option<SurfaceKind> {
    let word = word.trim().to_ascii_lowercase();
    SECTIONS.iter().find(|(_, words)| words.contains(&word.as_str())).map(|(kind, _)| *kind)
}

fn host_key(url: &Url) -> String {
    url.host_str().unwrap_or_default().trim_start_matches("www.").to_ascii_lowercase()
}

/// Same site: the same host (ignoring `www.`) and port, or a subdomain of it.
pub fn same_site(seed: &Url, url: &Url) -> bool {
    let (seed_host, host) = (host_key(seed), host_key(url));
    if url.port_or_known_default() != seed.port_or_known_default() && host == seed_host {
        return false;
    }
    host == seed_host || host.ends_with(&format!(".{seed_host}"))
}

fn is_subdomain(seed: &Url, url: &Url) -> bool {
    let (seed_host, host) = (host_key(seed), host_key(url));
    host != seed_host && host.ends_with(&format!(".{seed_host}"))
}

/// Classify a same-site link as a section root, if it is one.
/// Returns the kind, confidence, and the evidence for it.
pub fn classify_link(seed: &Url, url: &Url, text: &str) -> Option<(SurfaceKind, DiscoveryConfidence, String)> {
    if !same_site(seed, url) {
        return None;
    }
    let segments = url.path_segments().map(|s| s.filter(|p| !p.is_empty()).collect::<Vec<_>>()).unwrap_or_default();
    // A subdomain named like a section: docs.example.com, blog.example.com.
    if is_subdomain(seed, url) && segments.is_empty() {
        let label = host_key(url).split('.').next().unwrap_or_default().to_owned();
        if let Some(kind) = section_kind(&label) {
            return Some((kind, DiscoveryConfidence::High, format!("subdomain “{}”", host_key(url))));
        }
    }
    // A section root: its last segment names the section, at most two deep
    // (/blog, /en/blog, /company/news) — not an individual post.
    if (1..=2).contains(&segments.len()) {
        let last = segments[segments.len() - 1].trim_end_matches(".html").trim_end_matches(".htm");
        if let Some(kind) = section_kind(last) {
            return Some((kind, DiscoveryConfidence::High, format!("path /{}", segments.join("/"))));
        }
    }
    // The anchor text names a section even though the path does not.
    let words = text.split(|c: char| !c.is_alphanumeric() && c != '-').filter(|w| !w.is_empty()).collect::<Vec<_>>();
    if !words.is_empty() && words.len() <= 3 && segments.len() <= 2 {
        let joined = words.join("-");
        if let Some(kind) = section_kind(&joined).or_else(|| words.iter().find_map(|w| section_kind(w))) {
            return Some((kind, DiscoveryConfidence::Medium, format!("link text “{}”", text.trim())));
        }
    }
    None
}

/// Recognize a GitHub repository URL (`github.com/owner/repo`).
pub fn github_repository(url: &Url) -> Option<(String, String)> {
    if host_key(url) != "github.com" {
        return None;
    }
    let segments = url.path_segments()?.filter(|s| !s.is_empty()).collect::<Vec<_>>();
    let reserved = ["orgs", "topics", "features", "about", "pricing", "login", "marketplace", "sponsors", "settings", "explore"];
    match segments.as_slice() {
        [owner, repo, ..] if !reserved.contains(owner) => Some((owner.to_string(), repo.trim_end_matches(".git").to_string())),
        _ => None,
    }
}

/// The information surfaces of a GitHub repository worth observing, with why.
/// Commits and issues are deliberately not observed: too noisy to be signal.
pub fn github_surfaces(owner: &str, repo: &str, from: &Url, confidence: DiscoveryConfidence) -> Vec<DiscoveredSurface> {
    let base = format!("https://github.com/{owner}/{repo}");
    let url = |path: &str| Url::parse(&format!("{base}{path}")).expect("github url");
    vec![
        DiscoveredSurface {
            url: url("/releases.atom"),
            fetch_url: url("/releases.atom"),
            title: Some(format!("{owner}/{repo} releases")),
            kind: SurfaceKind::Releases,
            adapter: SourceKind::Atom,
            method: DiscoveryMethod::GithubSurface,
            confidence,
            discovered_from: from.clone(),
            reason: format!("release feed of the GitHub repository {owner}/{repo}"),
        },
        DiscoveredSurface {
            url: url(""),
            fetch_url: url(""),
            title: Some(format!("{owner}/{repo}")),
            kind: SurfaceKind::Repository,
            adapter: SourceKind::Web,
            method: DiscoveryMethod::GithubSurface,
            confidence: DiscoveryConfidence::Medium,
            discovered_from: from.clone(),
            reason: format!("repository page (README and description) of {owner}/{repo}"),
        },
    ]
}

pub fn parse_robots(text: &str) -> Robots {
    let mut robots = Robots::default();
    let mut applies = false;
    let mut in_agent_block = false;
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or_default().trim();
        let Some((key, value)) = line.split_once(':') else { continue };
        let (key, value) = (key.trim().to_ascii_lowercase(), value.trim().to_owned());
        match key.as_str() {
            "user-agent" => {
                if !in_agent_block {
                    applies = false;
                }
                in_agent_block = true;
                applies |= value == "*" || value.to_ascii_lowercase().contains("stream");
            }
            "disallow" => {
                in_agent_block = false;
                if applies {
                    robots.disallow.push(value);
                }
            }
            "allow" => {
                in_agent_block = false;
                if applies {
                    robots.allow.push(value);
                }
            }
            "sitemap" => robots.sitemaps.push(value.trim().to_owned()),
            _ => in_agent_block = false,
        }
    }
    robots
}

/// `<loc>` entries of a sitemap or sitemap index, and whether it is an index.
pub fn parse_sitemap(xml: &str, limit: usize) -> (Vec<String>, bool) {
    let index = xml.contains("<sitemapindex");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<loc>") {
        let after = &rest[start + 5..];
        let Some(end) = after.find("</loc>") else { break };
        out.push(after[..end].trim().replace("&amp;", "&"));
        rest = &after[end + 6..];
        if out.len() >= limit {
            break;
        }
    }
    (out, index)
}

/// Section roots implied by sitemap URLs: /docs/intro and /docs/api imply /docs.
pub fn sitemap_sections(seed: &Url, locations: &[String]) -> Vec<(Url, SurfaceKind, usize)> {
    let mut roots: BTreeMap<String, (Url, SurfaceKind, usize)> = BTreeMap::new();
    for location in locations {
        let Ok(url) = Url::parse(location) else { continue };
        if !same_site(seed, &url) {
            continue;
        }
        let segments = url.path_segments().map(|s| s.filter(|p| !p.is_empty()).collect::<Vec<_>>()).unwrap_or_default();
        for depth in 0..segments.len().min(2) {
            if let Some(kind) = section_kind(segments[depth]) {
                let mut root = url.clone();
                root.set_path(&format!("/{}", segments[..=depth].join("/")));
                root.set_query(None);
                let entry = roots.entry(root.to_string()).or_insert((root, kind, 0));
                entry.2 += 1;
                break;
            }
        }
    }
    let mut out = roots.into_values().collect::<Vec<_>>();
    out.sort_by(|a, b| b.2.cmp(&a.2));
    out
}

struct Collector<'a> {
    seed: Url,
    policy: &'a DiscoveryPolicy,
    robots: Robots,
    report: DiscoveryReport,
    seen: HashSet<String>,
}

impl Collector<'_> {
    fn add(&mut self, mut surface: DiscoveredSurface) -> bool {
        let key = canonicalize_url(surface.url.as_str()).map(|u| u.to_string()).unwrap_or_else(|_| surface.url.to_string());
        if let Ok(canonical) = canonicalize_url(surface.url.as_str()) {
            surface.url = canonical;
        }
        if self.seen.contains(&key) {
            return false;
        }
        if same_site(&self.seed, &surface.fetch_url) && !self.robots.allows(surface.fetch_url.path()) {
            self.report.skipped.push((surface.fetch_url.clone(), "disallowed by robots.txt".into()));
            self.seen.insert(key);
            return false;
        }
        if self.report.surfaces.len() >= self.policy.max_surfaces {
            self.report.skipped.push((surface.fetch_url.clone(), format!("beyond the {}-surface limit", self.policy.max_surfaces)));
            return false;
        }
        self.seen.insert(key);
        self.report.surfaces.push(surface);
        true
    }

    fn feed_surface(&self, url: Url, format: DocumentFormat, title: Option<String>, from: &Url, method: DiscoveryMethod, confidence: DiscoveryConfidence, reason: String) -> DiscoveredSurface {
        DiscoveredSurface {
            url: url.clone(),
            fetch_url: url,
            title,
            kind: SurfaceKind::Feed,
            adapter: format.adapter_kind(),
            method,
            confidence,
            discovered_from: from.clone(),
            reason,
        }
    }
}

async fn fetch_text(fetcher: &HttpFetcher, url: &Url, report: &mut DiscoveryReport) -> Option<(Url, DocumentFormat, String)> {
    match fetcher.fetch_document(url).await {
        Ok(document) => {
            let format = detect_format(document.content_type.as_deref(), &document.body);
            Some((document.final_url, format, String::from_utf8_lossy(&document.body).into_owned()))
        }
        Err(error) => {
            report.failures.push((url.clone(), error));
            None
        }
    }
}

/// Discover the information surfaces downstream of `seed`.
pub async fn discover(fetcher: &HttpFetcher, seed: &Url, policy: &DiscoveryPolicy) -> DiscoveryReport {
    let mut collector = Collector {
        seed: seed.clone(),
        policy,
        robots: Robots::default(),
        report: DiscoveryReport { seed: Some(seed.clone()), ..Default::default() },
        seen: HashSet::new(),
    };

    // robots.txt first: it constrains what Stream will register, and may name the sitemap.
    let origin = Url::parse(&seed.origin().ascii_serialization()).ok();
    if let Some(origin) = &origin {
        if let Ok(robots_url) = origin.join("/robots.txt") {
            if let Ok(document) = fetcher.fetch_document(&robots_url).await {
                collector.robots = parse_robots(&String::from_utf8_lossy(&document.body));
            }
        }
    }

    // The seed itself.
    let Some((seed_final, seed_format, seed_body)) = fetch_text(fetcher, seed, &mut collector.report).await else {
        return collector.report;
    };
    collector.report.seed_reachable = true;
    if seed_format.is_feed() {
        let surface = collector.feed_surface(
            seed.clone(),
            seed_format,
            None,
            seed,
            DiscoveryMethod::Seed,
            DiscoveryConfidence::High,
            "the URL you added is a feed".into(),
        );
        collector.add(surface);
        return collector.report;
    }
    let page = stream_web::parse_page(&seed_final, &seed_body).ok();
    collector.report.seed_title = page.as_ref().map(|p| p.title.clone());
    let seed_kind = page
        .as_ref()
        .and_then(|_| classify_link(seed, seed, "").map(|(kind, _, _)| kind))
        .unwrap_or(SurfaceKind::Homepage);
    collector.add(DiscoveredSurface {
        url: seed.clone(),
        fetch_url: seed.clone(),
        title: collector.report.seed_title.clone(),
        kind: seed_kind,
        adapter: SourceKind::Web,
        method: DiscoveryMethod::Seed,
        confidence: DiscoveryConfidence::High,
        discovered_from: seed.clone(),
        reason: "the URL you added".into(),
    });

    // 1. Feeds the page declares.
    let mut feed_found = false;
    if let Some(page) = &page {
        for feed in &page.feeds {
            feed_found = true;
            let surface = collector.feed_surface(
                feed.url.clone(),
                feed.format,
                feed.title.clone(),
                seed,
                DiscoveryMethod::HtmlFeedDeclaration,
                DiscoveryConfidence::High,
                format!("declared by the page as its {} feed", format!("{:?}", feed.format).to_uppercase()),
            );
            collector.add(surface);
        }
    }

    // 2. Obvious sections linked from the page.
    let mut sections: Vec<(Url, SurfaceKind, DiscoveryConfidence, String)> = Vec::new();
    let mut repositories: Vec<(String, String)> = Vec::new();
    if let Some((owner, repo)) = github_repository(seed) {
        repositories.push((owner, repo));
    }
    if let Some(page) = &page {
        for link in &page.links {
            if let Some((owner, repo)) = github_repository(&link.url) {
                if !repositories.contains(&(owner.clone(), repo.clone())) && repositories.len() < policy.max_repositories {
                    repositories.push((owner, repo));
                }
                continue;
            }
            if let Some((kind, confidence, evidence)) = classify_link(seed, &link.url, &link.text) {
                if !sections.iter().any(|(url, ..)| url == &link.url) {
                    let place = if link.in_navigation { "site navigation" } else { "the page" };
                    sections.push((link.url.clone(), kind, confidence, format!("linked from {place} as a {} ({evidence})", kind.label().to_lowercase())));
                }
            }
        }
    }
    for (url, kind, confidence, reason) in &sections {
        let method = if is_subdomain(seed, url) { DiscoveryMethod::SubdomainLink } else { DiscoveryMethod::NavigationLink };
        collector.add(DiscoveredSurface {
            url: url.clone(),
            fetch_url: url.clone(),
            title: None,
            kind: *kind,
            adapter: SourceKind::Web,
            method,
            confidence: *confidence,
            discovered_from: seed.clone(),
            reason: reason.clone(),
        });
    }

    // 3. GitHub repositories: the seed itself, or linked from it.
    for (index, (owner, repo)) in repositories.iter().enumerate() {
        let confidence = if index == 0 && github_repository(seed).is_some() { DiscoveryConfidence::High } else { DiscoveryConfidence::Medium };
        for surface in github_surfaces(owner, repo, seed, confidence) {
            collector.add(surface);
        }
    }

    // 4. The sitemap: a map of sections, never a list of pages to fetch.
    if let Some(origin) = &origin {
        let mut sitemaps = collector.robots.sitemaps.iter().filter_map(|s| Url::parse(s).ok()).map(|u| (u, DiscoveryMethod::RobotsSitemap)).collect::<Vec<_>>();
        if sitemaps.is_empty() {
            if let Ok(url) = origin.join("/sitemap.xml") {
                sitemaps.push((url, DiscoveryMethod::Sitemap));
            }
        }
        let mut locations = Vec::new();
        let mut visited = 0;
        while let Some((sitemap, method)) = sitemaps.pop() {
            if visited >= 4 || locations.len() >= policy.max_sitemap_urls {
                break;
            }
            visited += 1;
            let Some((_, _, body)) = fetch_text(fetcher, &sitemap, &mut collector.report).await else { continue };
            let (entries, is_index) = parse_sitemap(&body, policy.max_sitemap_urls);
            if is_index {
                sitemaps.extend(entries.iter().filter_map(|e| Url::parse(e).ok()).take(3).map(|u| (u, method)));
            } else {
                locations.extend(entries.into_iter().map(|entry| (entry, method)));
            }
        }
        let method_of = locations.first().map(|(_, m)| *m).unwrap_or(DiscoveryMethod::Sitemap);
        let urls = locations.into_iter().map(|(u, _)| u).collect::<Vec<_>>();
        for (url, kind, pages) in sitemap_sections(seed, &urls) {
            let confidence = if pages >= 3 { DiscoveryConfidence::High } else { DiscoveryConfidence::Medium };
            let source = if method_of == DiscoveryMethod::RobotsSitemap { "the sitemap robots.txt points to" } else { "sitemap.xml" };
            let added = collector.add(DiscoveredSurface {
                url: url.clone(),
                fetch_url: url.clone(),
                title: None,
                kind,
                adapter: SourceKind::Web,
                method: method_of,
                confidence,
                discovered_from: seed.clone(),
                reason: format!("{pages} page(s) under {} listed in {source}; classified as {}", url.path(), kind.label().to_lowercase()),
            });
            if added {
                sections.push((url, kind, confidence, String::new()));
            }
        }
    }

    // 5. One hop: feeds declared by the sections themselves (e.g. /blog → /blog/feed.xml).
    let mut fetched = 0;
    for (url, kind, confidence, _) in sections.iter().filter(|(_, _, c, _)| *c == DiscoveryConfidence::High) {
        if fetched >= policy.max_section_fetches {
            break;
        }
        if kind == &SurfaceKind::Documentation {
            continue; // reference material rarely has feeds worth an extra fetch
        }
        if !collector.robots.allows(url.path()) {
            continue;
        }
        fetched += 1;
        let Some((final_url, format, body)) = fetch_text(fetcher, url, &mut collector.report).await else { continue };
        if format.is_feed() {
            continue;
        }
        let Ok(section_page) = stream_web::parse_page(&final_url, &body) else { continue };
        for feed in section_page.feeds {
            feed_found = true;
            let surface = collector.feed_surface(
                feed.url.clone(),
                feed.format,
                feed.title.clone(),
                url,
                DiscoveryMethod::HtmlFeedDeclaration,
                *confidence,
                format!("declared by the {} section as its feed", kind.label().to_lowercase()),
            );
            collector.add(surface);
        }
    }

    // 6. Conventional feed locations, only when nothing declared a feed.
    if !feed_found {
        if let Some(origin) = &origin {
            for path in CONVENTIONAL_FEEDS.iter().take(policy.max_feed_probes) {
                let Ok(url) = origin.join(path) else { continue };
                if !collector.robots.allows(url.path()) {
                    continue;
                }
                // Probe failures (404s) are expected and not recorded.
                let Ok(document) = fetcher.fetch_document(&url).await else { continue };
                let format = detect_format(document.content_type.as_deref(), &document.body);
                if format.is_feed() {
                    let surface = collector.feed_surface(
                        url,
                        format,
                        None,
                        seed,
                        DiscoveryMethod::ConventionalFeedPath,
                        DiscoveryConfidence::Medium,
                        format!("a {} feed at the conventional location {path}", format!("{format:?}").to_uppercase()),
                    );
                    collector.add(surface);
                    break;
                }
            }
        }
    }

    collector.report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> Url {
        Url::parse(value).unwrap()
    }

    #[test]
    fn classifies_section_roots_not_posts() {
        let seed = url("https://example.com/");
        let kind = |u: &str, text: &str| classify_link(&seed, &url(u), text).map(|(k, c, _)| (k, c));
        assert_eq!(kind("https://example.com/blog", ""), Some((SurfaceKind::Blog, DiscoveryConfidence::High)));
        assert_eq!(kind("https://www.example.com/en/changelog", ""), Some((SurfaceKind::Changelog, DiscoveryConfidence::High)));
        assert_eq!(kind("https://docs.example.com/", ""), Some((SurfaceKind::Documentation, DiscoveryConfidence::High)));
        assert_eq!(kind("https://example.com/whats-up", "What's new"), Some((SurfaceKind::Changelog, DiscoveryConfidence::Medium)));
        assert_eq!(kind("https://example.com/company", "About us"), None);
        assert_eq!(kind("https://example.com/updates-log", "Changelog"), Some((SurfaceKind::Changelog, DiscoveryConfidence::Medium)));
        assert_eq!(kind("https://example.com/blog/2026/my-post", ""), None, "individual posts are not sections");
        assert_eq!(kind("https://other.com/blog", ""), None, "other sites are not downstream");
        assert_eq!(kind("https://example.com/pricing", "Pricing"), None);
    }

    #[test]
    fn parses_robots_and_respects_it() {
        let robots = parse_robots("User-agent: *\nDisallow: /private\nAllow: /private/ok\n\nUser-agent: otherbot\nDisallow: /\nSitemap: https://example.com/sm.xml\n");
        assert!(!robots.allows("/private/x"));
        assert!(robots.allows("/private/ok/y"));
        assert!(robots.allows("/blog"));
        assert_eq!(robots.sitemaps, vec!["https://example.com/sm.xml"]);
    }

    #[test]
    fn sitemaps_imply_sections_without_listing_pages() {
        let seed = url("https://example.com/");
        let xml = r#"<urlset><url><loc>https://example.com/docs/intro</loc></url><url><loc>https://example.com/docs/api</loc></url>
            <url><loc>https://example.com/docs/cli</loc></url><url><loc>https://example.com/blog/a-post</loc></url>
            <url><loc>https://example.com/about</loc></url><url><loc>https://elsewhere.com/docs/x</loc></url></urlset>"#;
        let (locations, index) = parse_sitemap(xml, 100);
        assert!(!index);
        let sections = sitemap_sections(&seed, &locations);
        assert_eq!(sections[0].0.as_str(), "https://example.com/docs");
        assert_eq!(sections[0].2, 3);
        assert_eq!(sections[1].1, SurfaceKind::Blog);
        assert_eq!(sections.len(), 2);
    }

    #[test]
    fn github_repositories_yield_release_feeds() {
        assert_eq!(github_repository(&url("https://github.com/rust-lang/rust/releases")), Some(("rust-lang".into(), "rust".into())));
        assert_eq!(github_repository(&url("https://github.com/orgs/rust-lang")), None);
        let surfaces = github_surfaces("apple", "container", &url("https://example.com/"), DiscoveryConfidence::Medium);
        assert_eq!(surfaces[0].url.as_str(), "https://github.com/apple/container/releases.atom");
        assert_eq!(surfaces[0].adapter, SourceKind::Atom);
        assert!(surfaces[0].reason.contains("apple/container"));
    }
}
