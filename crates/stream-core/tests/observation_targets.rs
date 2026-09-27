//! Observation targets: `https://example.com` watches a resource,
//! `https://example.com/*` watches the information surface beneath it.
//! Against real FeltDB and a local web.

use stream_core::{RunOptions, SourceHealth, StreamRuntime};
use stream_discovery::ProviderEndpoints;
use stream_model::{
    DiscoveryMethod, ObservationScope, PageChange, SurfaceKind, TargetKind, TargetProvider, TargetStatus,
};
use stream_testkit::{
    product_site, runtime_at, widget_feed, FixtureServer, Isolated, WIDGET_POST_TWO_ZERO, WIDGET_POST_WELCOME,
};

fn site(with_changelog: bool) -> FixtureServer {
    let server = FixtureServer::start();
    product_site(&server, with_changelog);
    server
}

fn force(target: &stream_model::TargetId) -> RunOptions {
    RunOptions { trigger: "test".into(), target_id: Some(target.clone()), force: true }
}

async fn signal_titles(runtime: &StreamRuntime) -> Vec<String> {
    runtime
        .list_signals()
        .await
        .unwrap()
        .into_iter()
        .map(|s| format!("{} — {}", s.signal.subject.label, s.signal.change.statement))
        .collect()
}

async fn kinds(runtime: &StreamRuntime, target: &stream_model::TargetId) -> Vec<(SurfaceKind, String)> {
    runtime
        .target_sources(target)
        .await
        .unwrap()
        .into_iter()
        .map(|w| (w.source.surface_kind.unwrap(), w.source.canonical_url.path().to_owned()))
        .collect()
}

#[tokio::test]
async fn scope_is_parsed_once_and_is_part_of_identity() {
    let server = site(true);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);

    let resource = runtime.add_target(&server.url("/"), "test").await.unwrap();
    let surface = runtime.add_target(&server.url("/*"), "test").await.unwrap();
    assert_eq!(resource.target.scope, ObservationScope::Resource);
    assert_eq!(surface.target.scope, ObservationScope::Descendants);
    assert_ne!(resource.target.id, surface.target.id, "the resource and its information surface are different targets");
    assert!(!surface.target.seed_url.as_str().contains('*'), "the operator is syntax, never part of a URL");
    assert!(surface.target.display_url().ends_with("/*"));
    assert_eq!(surface.target.status, TargetStatus::Pending, "durable before anything is discovered");

    for same in [server.url("//*"), server.url("/?utm_source=x/*"), format!("  {}  ", server.url("/*"))] {
        let again = runtime.add_target(&same, "test").await.unwrap();
        assert!(again.existing, "{same}");
        assert_eq!(again.target.id, surface.target.id);
    }

    for bad in [server.url("/*/foo"), server.url("/foo*bar"), "https://user:secret@example.com/*".into(), "file:///etc/passwd".into()] {
        assert!(runtime.add_target(&bad, "test").await.is_err(), "{bad}");
    }
    let public_only = runtime_at(&isolated).with_network_policy(stream_ingest::NetworkPolicy::default());
    assert!(public_only.add_target(&server.url("/*"), "test").await.is_err(), "private targets need an explicit opt-in");

    // Parsed scope is persisted, not re-derived from strings.
    let restarted = runtime_at(&isolated);
    let targets = restarted.list_targets().await.unwrap();
    assert_eq!(targets.len(), 2);
    assert!(targets.iter().any(|t| t.scope == ObservationScope::Descendants && t.id == surface.target.id));
    assert!(server.requested().is_empty(), "adding a target fetches nothing");
}

#[tokio::test]
async fn resource_scope_observes_exactly_the_resource() {
    let server = site(true);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let outcome = runtime.add_and_watch(&server.url("/changelog"), "test", None).await.unwrap();
    assert_eq!(outcome.target.scope, ObservationScope::Resource);
    assert_eq!(outcome.target.identity.kind, TargetKind::Resource);
    assert_eq!(outcome.target.source_ids.len(), 1);
    let report = outcome.report.expect("a resource is observed right away");
    assert!(report.primary_signal_id.is_some(), "stream add <url> still builds its signal");
    assert!(outcome.discovery.is_none());
    assert!(!server.requested().iter().any(|p| p == "/robots.txt" || p == "/sitemap.xml" || p == "/blog"), "no discovery for a resource");

    // Later, a change to the resource is a change observation.
    runtime.observe_source(&outcome.target.source_ids[0], None).await.unwrap();
    server.html(
        "/changelog",
        r#"<!doctype html><html><head><title>Widget Changelog</title></head><body><main><h1>Changelog</h1>
<h2>Version 1.5.0</h2><p>Adds portable compute snapshots that move running workloads between machines.</p>
<h2>Version 1.4.1</h2><p>Fixes a crash when starting VMs on older Macs.</p></main></body></html>"#,
    );
    let changed = runtime.observe_source(&outcome.target.source_ids[0], None).await.unwrap();
    assert_eq!(changed.page_change, Some(PageChange::MateriallyChanged));
}

#[tokio::test]
async fn descendants_discover_and_observe_the_information_surface() {
    let server = site(true);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);

    let outcome = runtime.add_and_watch(&server.url("/*"), "test", None).await.unwrap();
    let target = outcome.target;
    assert_eq!(target.scope, ObservationScope::Descendants);
    assert_eq!(target.identity.provider, TargetProvider::Web);
    assert_eq!(target.identity.kind, TargetKind::Site);
    assert_eq!(target.status, TargetStatus::Active);
    assert_eq!(target.title.as_deref(), Some("Widget Co — portable compute runtime"));
    assert!(target.last_discovered_at.is_some() && target.next_discovery_at.unwrap() > target.last_discovered_at.unwrap());

    let found = kinds(&runtime, &target.id).await;
    for (kind, path) in [
        (SurfaceKind::Homepage, "/"),
        (SurfaceKind::Blog, "/blog"),
        (SurfaceKind::Changelog, "/changelog"),
        (SurfaceKind::Documentation, "/docs"),
        (SurfaceKind::Feed, "/feed.xml"),
    ] {
        assert!(found.contains(&(kind, path.to_owned())), "missing {kind:?} {path} in {found:?}");
    }

    // Why am I watching this?
    let watched = runtime.target_sources(&target.id).await.unwrap();
    let feed = watched.iter().find(|w| w.source.canonical_url.path() == "/feed.xml").unwrap();
    assert_eq!(feed.source.discovery_method, Some(DiscoveryMethod::HtmlFeedDeclaration));
    assert_eq!(feed.source.target_id.as_ref(), Some(&target.id));
    assert!(feed.why.contains("feed the page declares"), "{}", feed.why);
    assert_eq!(feed.health, SourceHealth::Healthy);
    let relations = runtime.relations_of_target(&target.id).await.unwrap();
    assert!(relations.len() >= watched.len());

    // Observe broadly, surface selectively: the baseline of feeds and
    // sections is context, not a burst of signals.
    let run = outcome.run.unwrap();
    assert!(run.sources_observed >= 5, "{run:?}");
    assert!(run.signals_created <= 1, "baseline observations must not flood: {:?}", signal_titles(&runtime).await);

    // Unchanged world: a forced pass produces nothing new.
    let quiet = runtime.run_observation(force(&target.id)).await.unwrap();
    assert_eq!((quiet.new_items, quiet.page_changes, quiet.signals_created), (0, 0, 0), "{quiet:?}");
    let changelog = watched.iter().find(|w| w.source.canonical_url.path() == "/changelog").unwrap();
    let snapshots = runtime.snapshots_of(&changelog.source.id).await.unwrap();
    assert_eq!(snapshots[0].change, PageChange::Unchanged);
}

#[tokio::test]
async fn later_discovery_finds_surfaces_that_appear() {
    let server = site(false);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let outcome = runtime.add_and_watch(&server.url("/*"), "test", None).await.unwrap();
    let target = outcome.target.id.clone();
    assert!(!kinds(&runtime, &target).await.iter().any(|(kind, _)| *kind == SurfaceKind::Changelog));

    product_site(&server, true);
    let rediscovered = runtime.discover_target(&target).await.unwrap();
    let changelog = kinds(&runtime, &target).await;
    assert!(changelog.contains(&(SurfaceKind::Changelog, "/changelog".into())), "{changelog:?}");
    assert_eq!(rediscovered.new_sources.len(), 1, "only the new surface is new");

    // …and the scheduler observes it without the user adding anything.
    let run = runtime.run_observation(RunOptions { trigger: "test".into(), ..Default::default() }).await.unwrap();
    assert!(run.sources_observed >= 1);
    let watched = runtime.target_sources(&target).await.unwrap();
    assert!(watched.iter().find(|w| w.source.canonical_url.path() == "/changelog").unwrap().source.last_observed_at.is_some());
}

#[tokio::test]
async fn new_downstream_information_enters_the_pipeline() {
    let server = site(true);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let target = runtime.add_and_watch(&server.url("/*"), "test", None).await.unwrap().target.id;
    let before = runtime.list_signals().await.unwrap().len();

    // A new blog post in the feed: only it is new.
    server.route("/feed.xml", "application/rss+xml", &widget_feed(&[WIDGET_POST_TWO_ZERO, WIDGET_POST_WELCOME]));
    let run = runtime.run_observation(force(&target)).await.unwrap();
    assert_eq!(run.new_items, 1, "{run:?}");
    let titles = signal_titles(&runtime).await;
    assert_eq!(titles.len(), before + 1, "{titles:?}");
    assert!(titles.iter().any(|t| t.contains("Widget 2.0") || t.contains("snapshots")), "{titles:?}");

    // The changelog changes: a change observation with evidence, not a new HTTP response.
    server.html(
        "/changelog",
        r#"<!doctype html><html><head><title>Widget Changelog</title></head><body><main><h1>Changelog</h1>
<h2>Version 2.0.0</h2><p>Introduces portable compute snapshots that move running workloads between machines.</p>
<h2>Version 1.4.1</h2><p>Fixes a crash when starting VMs on older Macs.</p></main></body></html>"#,
    );
    let run = runtime.run_observation(force(&target)).await.unwrap();
    assert_eq!(run.page_changes, 1, "{run:?}");
    let watched = runtime.target_sources(&target).await.unwrap();
    let changelog = watched.iter().find(|w| w.source.canonical_url.path() == "/changelog").unwrap();
    let change = changelog.last_change.as_ref().unwrap();
    assert_eq!(change.change, PageChange::MateriallyChanged);
    assert!(change.added.iter().any(|line| line.contains("Version 2.0.0")), "{change:?}");
    let snapshot = &runtime.snapshots_of(&changelog.source.id).await.unwrap()[0];
    assert!(snapshot.item_id.is_some(), "the change is an item with provenance");

    // Blog, feed and changelog describe one change: presented once.
    let after = signal_titles(&runtime).await;
    assert!(after.len() <= before + 2, "consolidation keeps the change dense: {after:?}");
}

#[tokio::test]
async fn targets_and_schedules_survive_restart() {
    let server = site(true);
    let isolated = Isolated::new();
    let target = {
        let runtime = runtime_at(&isolated);
        runtime.add_and_watch(&server.url("/*"), "test", None).await.unwrap().target
    };
    let restarted = runtime_at(&isolated);
    let reloaded = restarted.get_target(&target.id).await.unwrap().expect("target in FeltDB");
    assert_eq!(reloaded.scope, ObservationScope::Descendants);
    let watched = restarted.target_sources(&target.id).await.unwrap();
    assert!(watched.len() >= 5);
    assert!(watched.iter().all(|w| w.source.next_observation_at.unwrap() > chrono::Utc::now()), "next observation is durable");

    // Nothing is due: an unforced pass after restart observes nothing.
    let run = restarted.run_observation(RunOptions { trigger: "test".into(), ..Default::default() }).await.unwrap();
    assert_eq!(run.sources_observed, 0, "{run:?}");
    assert_eq!(run.targets_discovered, 0);
    let status = restarted.observation_status().await.unwrap();
    assert_eq!(status.due_sources, 0);
    assert!(status.next_due_at.is_some());

    // A discovery a dead process left "running" is picked up again.
    restarted.pause_target(&target.id).await.unwrap();
    assert_eq!(restarted.get_target(&target.id).await.unwrap().unwrap().status, TargetStatus::Paused);
    let resumed = restarted.resume_target(&target.id).await.unwrap();
    assert_eq!(resumed.status, TargetStatus::Active);
}

#[tokio::test]
async fn failures_are_durable_explained_and_never_drop_sources() {
    let server = site(true);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let target = runtime.add_and_watch(&server.url("/*"), "test", None).await.unwrap().target.id;
    let docs = runtime.target_sources(&target).await.unwrap().into_iter().find(|w| w.source.canonical_url.path() == "/docs").unwrap().source;

    server.rate_limited("/docs", 600);
    let report = runtime.observe_source(&docs.id, None).await.unwrap();
    assert!(report.failure.as_deref().unwrap_or_default().contains("Rate limited"), "{report:?}");
    let limited = runtime.get_source(&docs.id).await.unwrap().unwrap();
    let wait = limited.next_observation_at.unwrap() - chrono::Utc::now();
    assert!(wait > chrono::Duration::seconds(500), "Retry-After is respected: {wait}");

    server.failing("/docs", 404);
    for _ in 0..3 {
        runtime.observe_source(&docs.id, None).await.unwrap();
    }
    let watched = runtime.target_sources(&target).await.unwrap();
    let gone = watched.iter().find(|w| w.source.id == docs.id).expect("never silently removed");
    assert_eq!(gone.health, SourceHealth::Unavailable);
    assert!(gone.source.last_error_message.as_deref().unwrap_or_default().contains("404"));
}

#[tokio::test]
async fn x_status_scope_resolves_to_the_account() {
    let isolated = Isolated::new();
    // No X endpoint configured: durable, honest, and nothing fetched.
    let runtime = runtime_at(&isolated).with_provider_endpoints(ProviderEndpoints::default());
    let outcome = runtime.add_and_watch("https://x.com/devxritesh/status/*", "test", None).await.unwrap();
    let target = outcome.target;
    assert_eq!(target.seed_url.as_str(), "https://x.com/devxritesh/status/");
    assert_eq!(target.scope, ObservationScope::Descendants);
    assert_eq!((target.identity.provider, target.identity.kind), (TargetProvider::X, TargetKind::Account));
    assert_eq!(target.identity.display_name, "@devxritesh");
    assert_eq!(target.identity.watching, "Watching posts");
    assert_eq!(target.status, TargetStatus::Unavailable);
    assert!(target.discovery_detail.as_deref().unwrap().contains("Observation unavailable"));
    assert!(runtime.target_sources(&target.id).await.unwrap().is_empty(), "never pretends to be watching");
    assert!(runtime.list_sources().await.unwrap().is_empty(), "no literal /status/* URL becomes a source");

    // With a public feed endpoint configured, posts flow into the pipeline.
    let server = FixtureServer::start();
    let posts = |entries: &[(&str, &str, &str, &str)]| {
        let items = entries
            .iter()
            .map(|(id, title, text, date)| format!("<item><guid>{id}</guid><title>{title}</title><link>https://x.com/devxritesh/status/{id}</link><description>{text}</description><pubDate>{date}</pubDate></item>"))
            .collect::<String>();
        format!(r#"<?xml version="1.0"?><rss version="2.0"><channel><title>@devxritesh</title><link>https://x.com/devxritesh</link><description>posts</description>{items}</channel></rss>"#)
    };
    let first = ("100", "Shipping notes", "Working on the new release.", "Mon, 21 Sep 2026 09:00:00 GMT");
    server.route("/x/devxritesh/rss", "application/rss+xml", &posts(&[first]));
    let runtime = runtime_at(&isolated)
        .with_provider_endpoints(ProviderEndpoints { x_feed_template: Some(server.url("/x/{handle}/rss")) });
    let rediscovered = runtime.discover_target(&target.id).await.unwrap();
    assert!(rediscovered.failure.is_none(), "{rediscovered:?}");
    assert_eq!(rediscovered.target.status, TargetStatus::Active);
    let watched = runtime.target_sources(&target.id).await.unwrap();
    assert_eq!(watched.len(), 1);
    assert_eq!(watched[0].source.surface_kind, Some(SurfaceKind::Posts));
    assert_eq!(watched[0].source.discovery_method, Some(DiscoveryMethod::ProviderApi));
    runtime.run_observation(force(&target.id)).await.unwrap();
    assert!(runtime.list_signals().await.unwrap().is_empty(), "existing posts are the baseline");

    let second = ("101", "Released Widget 2.0", "Widget 2.0 released today with portable compute snapshots.", "Sat, 26 Sep 2026 09:00:00 GMT");
    server.route("/x/devxritesh/rss", "application/rss+xml", &posts(&[second, first]));
    let run = runtime.run_observation(force(&target.id)).await.unwrap();
    assert_eq!(run.new_items, 1, "{run:?}");
    assert_eq!(runtime.list_signals().await.unwrap().len(), 1, "a new post became a signal");
    assert!(server.requested().iter().all(|p| !p.contains('*')));
}

#[tokio::test]
async fn github_repository_scope_resolves_repository_surfaces() {
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let added = runtime.add_target("https://github.com/widgetco/widget/*", "test").await.unwrap();
    assert_eq!((added.target.identity.provider, added.target.identity.kind), (TargetProvider::Github, TargetKind::Repository));
    let discovered = runtime.discover_target(&added.target.id).await.unwrap();
    assert!(discovered.failure.is_none());
    let found = kinds(&runtime, &added.target.id).await;
    assert!(found.contains(&(SurfaceKind::Releases, "/widgetco/widget/releases.atom".into())), "{found:?}");
    assert!(found.contains(&(SurfaceKind::Repository, "/widgetco/widget".into())), "{found:?}");
    assert!(found.len() <= 4, "bounded: {found:?}");
}
