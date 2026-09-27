use stream_discovery::{discover, DiscoveryPolicy};
use stream_ingest::{HttpFetcher, NetworkPolicy};
use stream_model::{DiscoveryConfidence, DiscoveryMethod, SourceKind, SurfaceKind};
use stream_testkit::{product_site, FixtureServer};
use url::Url;

fn fetcher() -> HttpFetcher {
    HttpFetcher::new(NetworkPolicy::default().allowing_private_network())
}

#[tokio::test]
async fn discovers_the_information_surfaces_downstream_of_a_site() {
    let server = FixtureServer::start();
    product_site(&server, true);
    let seed = Url::parse(&server.url("/")).unwrap();
    let report = discover(&fetcher(), &seed, &DiscoveryPolicy::default()).await;
    assert!(report.seed_reachable);
    assert_eq!(report.seed_title.as_deref(), Some("Widget Co — portable compute runtime"));

    let find = |kind: SurfaceKind, path: &str| {
        report
            .surfaces
            .iter()
            .find(|s| s.kind == kind && s.fetch_url.as_str().ends_with(path))
            .unwrap_or_else(|| panic!("missing {kind:?} {path} in {:#?}", report.surfaces))
    };
    let homepage = find(SurfaceKind::Homepage, "/");
    assert_eq!(homepage.method, DiscoveryMethod::Seed);

    let feed = find(SurfaceKind::Feed, "/feed.xml");
    assert_eq!((feed.method, feed.confidence, feed.adapter), (DiscoveryMethod::HtmlFeedDeclaration, DiscoveryConfidence::High, SourceKind::Rss));

    for (kind, path) in [(SurfaceKind::Blog, "/blog"), (SurfaceKind::Changelog, "/changelog"), (SurfaceKind::Documentation, "/docs")] {
        let surface = find(kind, path);
        assert_eq!(surface.method, DiscoveryMethod::NavigationLink);
        assert!(surface.reason.contains("site navigation"), "{}", surface.reason);
    }

    let blog_feed = find(SurfaceKind::Feed, "/blog/atom.xml");
    assert_eq!(blog_feed.adapter, SourceKind::Atom);
    assert!(blog_feed.discovered_from.as_str().ends_with("/blog"), "one hop: declared by the blog section");

    let research = find(SurfaceKind::Research, "/research");
    assert_eq!(research.method, DiscoveryMethod::RobotsSitemap);
    assert!(research.reason.contains("sitemap"), "{}", research.reason);

    let releases = find(SurfaceKind::Releases, "/widgetco/widget/releases.atom");
    assert_eq!(releases.method, DiscoveryMethod::GithubSurface);

    // Bounded and relevant: no pricing/about pages, no other sites, robots respected.
    for surface in &report.surfaces {
        let url = surface.fetch_url.as_str();
        assert!(!url.contains("pricing") && !url.contains("about") && !url.contains("other.example.net"), "{url}");
        assert!(!url.contains("/private"), "robots.txt disallows {url}");
    }
    assert!(report.skipped.iter().any(|(url, why)| url.path() == "/private/updates" && why.contains("robots")));
    assert!(!report.surfaces.iter().any(|s| s.fetch_url.path() == "/docs/intro"), "the sitemap maps sections; it is not a crawl list");
}

#[tokio::test]
async fn conventional_feed_locations_are_probed_only_when_nothing_is_declared() {
    let server = FixtureServer::start();
    server.html("/", "<html><head><title>Plain</title></head><body><p>Hello there, plain page.</p></body></html>");
    server.route("/atom.xml", "application/atom+xml", r#"<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>x</title><id>x</id><updated>2026-09-01T00:00:00Z</updated></feed>"#);
    let report = discover(&fetcher(), &Url::parse(&server.url("/")).unwrap(), &DiscoveryPolicy::default()).await;
    let feed = report.surfaces.iter().find(|s| s.kind == SurfaceKind::Feed).expect("conventional feed");
    assert_eq!(feed.method, DiscoveryMethod::ConventionalFeedPath);
    assert_eq!(feed.confidence, DiscoveryConfidence::Medium);
}

#[tokio::test]
async fn discovery_is_bounded_and_failures_are_reported() {
    let server = FixtureServer::start();
    product_site(&server, true);
    let seed = Url::parse(&server.url("/")).unwrap();
    let report = discover(&fetcher(), &seed, &DiscoveryPolicy { max_surfaces: 3, ..Default::default() }).await;
    assert_eq!(report.surfaces.len(), 3);
    assert!(report.skipped.iter().any(|(_, why)| why.contains("limit")));

    let missing = Url::parse(&server.url("/nope")).unwrap();
    let report = discover(&fetcher(), &missing, &DiscoveryPolicy::default()).await;
    assert!(!report.seed_reachable);
    assert_eq!(report.seed_failure().and_then(|e| e.status()), Some(404));

    // The default network policy refuses loopback targets outright.
    let strict = discover(&HttpFetcher::new(NetworkPolicy::default()), &seed, &DiscoveryPolicy::default()).await;
    assert!(matches!(strict.seed_failure(), Some(stream_ingest::FetchError::Policy(_))));
}
