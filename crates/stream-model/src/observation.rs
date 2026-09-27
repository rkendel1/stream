//! Observation: what the user asked Stream to keep an eye on, what Stream
//! discovered there, and the durable state of watching it.
//!
//! URL → ObservationTarget → discovery → Sources (information surfaces) →
//! observations (items, page changes) → signals. The target is the user's
//! durable intent; sources are the concrete surfaces Stream observes; the
//! discovery relations explain why each one is watched.

use crate::intelligence::text_enum;
use crate::{canonicalize_url, CanonicalUrlError, ContextId, ItemId, SourceId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use url::Url;
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
            pub fn generate() -> Self {
                Self(format!("{}_{}", $prefix, Uuid::new_v4()))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(TargetId, "target");
id_type!(SnapshotId, "snapshot");
id_type!(ObservationRunId, "run");

fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))[..32].to_owned()
}

impl TargetId {
    /// Deterministic per canonical URL and scope: adding the same place with
    /// the same scope twice is the same durable intent, while the resource
    /// and the information surface beneath it are different targets.
    pub fn for_url(canonical: &str, scope: ObservationScope) -> Self {
        match scope {
            ObservationScope::Resource => Self(format!("target_{}", digest(canonical))),
            ObservationScope::Descendants => Self(format!("target_{}", digest(&format!("{canonical}\u{0}/*")))),
        }
    }
}

// Whether the user asked for the resource itself, or for the information
// surface beneath it (`/*`).
text_enum!(ObservationScope {
    Resource => "resource",
    Descendants => "descendants",
});

/// The observation operator: a terminal `/*` on a URL.
pub const SCOPE_OPERATOR: &str = "/*";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ObservationTargetError {
    #[error("empty URL")]
    Empty,
    #[error("'*' is only meaningful as a terminal /* (\"watch the information surface beneath this URL\"); it is not a wildcard")]
    MisplacedOperator,
    #[error(transparent)]
    Url(#[from] CanonicalUrlError),
}

/// What the user typed, parsed once into URL + scope. The `*` never
/// survives parsing: it is syntax for the scope, not part of any URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedObservationTarget {
    /// The URL to resolve, as given (minus the operator). Never contains `*`.
    pub seed_url: Url,
    /// Stream's durable identity for it.
    pub canonical_url: Url,
    pub scope: ObservationScope,
}

impl ParsedObservationTarget {
    /// The user-facing form: the canonical URL, plus `/*` for descendants.
    pub fn display(&self) -> String {
        display_target(&self.canonical_url, self.scope)
    }
}

/// The user-facing form of a target: `https://example.com/*`.
pub fn display_target(canonical: &Url, scope: ObservationScope) -> String {
    match scope {
        ObservationScope::Resource => canonical.to_string(),
        ObservationScope::Descendants => format!("{}{SCOPE_OPERATOR}", canonical.as_str().trim_end_matches('/')),
    }
}

/// The one parser for observation input. `https://example.com` is the
/// resource; `https://example.com/*` (or `…/path/*`) is the information
/// surface beneath it. Any other `*` is rejected rather than guessed at.
pub fn parse_observation_target(input: &str) -> Result<ParsedObservationTarget, ObservationTargetError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ObservationTargetError::Empty);
    }
    let (body, scope) = match trimmed.strip_suffix('*') {
        Some(rest) if rest.ends_with('/') => (rest, ObservationScope::Descendants),
        _ => (trimmed, ObservationScope::Resource),
    };
    if body.contains('*') || body.to_ascii_lowercase().contains("%2a") {
        return Err(ObservationTargetError::MisplacedOperator);
    }
    let with_scheme = if body.contains("://") { body.to_owned() } else { format!("https://{body}") };
    let mut seed_url = Url::parse(&with_scheme).map_err(|error| CanonicalUrlError::Invalid(error.to_string()))?;
    if scope == ObservationScope::Descendants {
        // `https://example.com//*` means the same as `https://example.com/*`.
        let mut path = seed_url.path().to_owned();
        while path.contains("//") {
            path = path.replace("//", "/");
        }
        seed_url.set_path(&path);
    }
    let canonical_url = canonicalize_url(seed_url.as_str())?;
    Ok(ParsedObservationTarget { seed_url, canonical_url, scope })
}

// Which provider-specific strategy understands a target.
text_enum!(TargetProvider {
    Web => "web",
    Github => "github",
    X => "x",
});

impl TargetProvider {
    pub fn label(self) -> &'static str {
        match self {
            TargetProvider::Web => "Web",
            TargetProvider::Github => "GitHub",
            TargetProvider::X => "X",
        }
    }
}

// What a target is, once resolved.
text_enum!(TargetKind {
    Resource => "resource",
    Site => "site",
    Section => "section",
    Repository => "repository",
    Account => "account",
});

/// Who or what a target is about, as resolved by the provider strategy:
/// `x / account / devxritesh`, `github / repository / org/repo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetIdentity {
    pub provider: TargetProvider,
    pub kind: TargetKind,
    /// Provider-level key: the account handle, `owner/repo`, or host + path.
    pub key: String,
    /// Friendly name: `@devxritesh`, `widgetco/widget`, `example.com`.
    pub display_name: String,
    /// What Stream watches there, in words: "Watching posts".
    pub watching: String,
}

/// The id of the discovery relation between a target and a source.
pub fn discovery_relation_id(target: &TargetId, source: &SourceId) -> String {
    format!("discovery_{}", digest(&format!("{target}\u{0}{source}")))
}

text_enum!(TargetStatus {
    Pending => "pending",
    Discovering => "discovering",
    Active => "active",
    Paused => "paused",
    Failed => "failed",
    Unavailable => "unavailable",
});

text_enum!(DiscoveryStatus {
    NotStarted => "not_started",
    Running => "running",
    Complete => "complete",
    Failed => "failed",
});

// What kind of information surface a source is, for the user.
text_enum!(SurfaceKind {
    Homepage => "homepage",
    Feed => "feed",
    Blog => "blog",
    News => "news",
    Changelog => "changelog",
    Releases => "releases",
    Documentation => "documentation",
    Announcements => "announcements",
    Updates => "updates",
    Status => "status",
    Research => "research",
    Engineering => "engineering",
    Roadmap => "roadmap",
    Product => "product",
    Projects => "projects",
    Repository => "repository",
    Posts => "posts",
    Page => "page",
});

impl SurfaceKind {
    pub fn label(self) -> &'static str {
        match self {
            SurfaceKind::Homepage => "Homepage",
            SurfaceKind::Feed => "Feed",
            SurfaceKind::Blog => "Blog",
            SurfaceKind::News => "News",
            SurfaceKind::Changelog => "Changelog",
            SurfaceKind::Releases => "Releases",
            SurfaceKind::Documentation => "Documentation",
            SurfaceKind::Announcements => "Announcements",
            SurfaceKind::Updates => "Updates",
            SurfaceKind::Status => "Status",
            SurfaceKind::Research => "Research",
            SurfaceKind::Engineering => "Engineering",
            SurfaceKind::Roadmap => "Roadmap",
            SurfaceKind::Product => "Product",
            SurfaceKind::Projects => "Projects",
            SurfaceKind::Repository => "Repository",
            SurfaceKind::Posts => "Posts",
            SurfaceKind::Page => "Page",
        }
    }

    /// Sections whose new pages are themselves new information (posts,
    /// releases, announcements) rather than reference material.
    pub fn publishes_entries(self) -> bool {
        matches!(
            self,
            SurfaceKind::Blog
                | SurfaceKind::News
                | SurfaceKind::Changelog
                | SurfaceKind::Releases
                | SurfaceKind::Announcements
                | SurfaceKind::Updates
                | SurfaceKind::Research
                | SurfaceKind::Engineering
                | SurfaceKind::Projects
                | SurfaceKind::Posts
        )
    }
}

text_enum!(DiscoveryMethod {
    Seed => "seed",
    UserAdded => "user_added",
    HtmlFeedDeclaration => "html_feed_declaration",
    ConventionalFeedPath => "conventional_feed_path",
    RobotsSitemap => "robots_sitemap",
    Sitemap => "sitemap",
    NavigationLink => "navigation_link",
    SubdomainLink => "subdomain_link",
    GithubSurface => "github_surface",
    ProviderResolution => "provider_resolution",
    ProviderApi => "provider_api",
    PageLink => "page_link",
});

impl DiscoveryMethod {
    pub fn label(self) -> &'static str {
        match self {
            DiscoveryMethod::Seed => "the URL you added",
            DiscoveryMethod::UserAdded => "added by you",
            DiscoveryMethod::HtmlFeedDeclaration => "a feed the page declares",
            DiscoveryMethod::ConventionalFeedPath => "a conventional feed location",
            DiscoveryMethod::RobotsSitemap => "the sitemap listed in robots.txt",
            DiscoveryMethod::Sitemap => "sitemap.xml",
            DiscoveryMethod::NavigationLink => "a link on the page",
            DiscoveryMethod::SubdomainLink => "a link to a related subdomain",
            DiscoveryMethod::GithubSurface => "the linked GitHub repository",
            DiscoveryMethod::ProviderResolution => "what the target resolves to on its provider",
            DiscoveryMethod::ProviderApi => "the provider's configured public feed",
            DiscoveryMethod::PageLink => "a new page that appeared in an observed section",
        }
    }
}

text_enum!(DiscoveryConfidence {
    High => "high",
    Medium => "medium",
    Low => "low",
});

// The outcome of comparing a page observation with the previous one.
text_enum!(PageChange {
    Baseline => "baseline",
    Unchanged => "unchanged",
    Changed => "changed",
    MateriallyChanged => "materially_changed",
});

/// How often a target is observed and rediscovered, and how far discovery
/// may reach.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationPolicy {
    pub observe_every_minutes: u32,
    pub discover_every_hours: u32,
    pub max_sources: usize,
}

impl Default for ObservationPolicy {
    fn default() -> Self {
        Self { observe_every_minutes: 60, discover_every_hours: 24, max_sources: 30 }
    }
}

/// The user's durable intent: "watch this part of the world".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationTarget {
    pub id: TargetId,
    /// The URL to resolve (never contains the `*` operator).
    pub seed_url: Url,
    pub canonical_seed_url: Url,
    pub scope: ObservationScope,
    pub identity: TargetIdentity,
    pub title: Option<String>,
    pub status: TargetStatus,
    pub discovery_status: DiscoveryStatus,
    pub discovery_detail: Option<String>,
    pub discovery_started_at: Option<DateTime<Utc>>,
    pub last_discovered_at: Option<DateTime<Utc>>,
    pub next_discovery_at: Option<DateTime<Utc>>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub next_observation_at: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub discovery_policy: ObservationPolicy,
    pub context_ids: Vec<ContextId>,
    pub source_ids: Vec<SourceId>,
    pub provenance: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ObservationTarget {
    pub fn new(parsed: ParsedObservationTarget, identity: TargetIdentity, provenance: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: TargetId::for_url(parsed.canonical_url.as_str(), parsed.scope),
            seed_url: parsed.seed_url,
            canonical_seed_url: parsed.canonical_url,
            scope: parsed.scope,
            identity,
            title: None,
            status: TargetStatus::Pending,
            discovery_status: DiscoveryStatus::NotStarted,
            discovery_detail: None,
            discovery_started_at: None,
            last_discovered_at: None,
            // Due immediately: the first discovery happens as soon as a worker runs.
            next_discovery_at: Some(now),
            last_observed_at: None,
            next_observation_at: Some(now),
            consecutive_failures: 0,
            discovery_policy: ObservationPolicy::default(),
            context_ids: vec![],
            source_ids: vec![],
            provenance: provenance.into(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_watching(&self) -> bool {
        !matches!(self.status, TargetStatus::Paused)
    }

    /// The user-facing form, `https://example.com/*` for descendants.
    pub fn display_url(&self) -> String {
        display_target(&self.canonical_seed_url, self.scope)
    }
}

/// Why Stream watches a source: the durable edge of the observation graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryRelation {
    pub id: String,
    pub target_id: TargetId,
    pub source_id: SourceId,
    pub discovered_from: Url,
    pub from_source_id: Option<SourceId>,
    pub method: DiscoveryMethod,
    pub confidence: DiscoveryConfidence,
    pub surface_kind: SurfaceKind,
    pub reason: String,
    pub discovered_at: DateTime<Utc>,
    pub last_confirmed_at: DateTime<Utc>,
}

/// A normalized observation of a web page, and how it differs from the
/// previous one. The evidence for "what changed on this page".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageSnapshot {
    pub id: SnapshotId,
    pub source_id: SourceId,
    pub url: Url,
    pub title: String,
    pub observed_at: DateTime<Utc>,
    pub content_hash: String,
    pub blocks: Vec<String>,
    pub links: Vec<String>,
    pub change: PageChange,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub summary: String,
    pub item_id: Option<ItemId>,
}

/// One pass of the observation worker, recorded durably.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationRun {
    pub id: ObservationRunId,
    pub trigger: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub targets_discovered: usize,
    pub sources_observed: usize,
    pub sources_failed: usize,
    pub new_items: usize,
    pub page_changes: usize,
    pub signals_created: usize,
    pub signals_corroborated: usize,
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: &str) -> ParsedObservationTarget {
        parse_observation_target(value).unwrap_or_else(|error| panic!("{value}: {error}"))
    }

    #[test]
    fn parses_scope_once() {
        let resource = parse("https://example.com");
        assert_eq!(resource.scope, ObservationScope::Resource);
        assert_eq!(resource.canonical_url.as_str(), "https://example.com/");

        let site = parse("https://example.com/*");
        assert_eq!(site.scope, ObservationScope::Descendants);
        assert_eq!(site.canonical_url.as_str(), "https://example.com/");
        assert!(!site.seed_url.as_str().contains('*'));
        assert_eq!(site.display(), "https://example.com/*");

        let path = parse("https://example.com/path/*");
        assert_eq!(path.scope, ObservationScope::Descendants);
        assert_eq!(path.canonical_url.as_str(), "https://example.com/path");

        let x = parse("https://x.com/devxritesh/status/*");
        assert_eq!(x.scope, ObservationScope::Descendants);
        assert_eq!(x.seed_url.as_str(), "https://x.com/devxritesh/status/");
        assert_eq!(x.display(), "https://x.com/devxritesh/status/*");
    }

    #[test]
    fn rejects_non_terminal_operators() {
        for bad in ["https://example.com/*/foo", "https://example.com/foo*bar", "https://example.com*", "https://example.com/foo*", "https://example.com/%2A", ""] {
            assert!(parse_observation_target(bad).is_err(), "{bad}");
        }
        assert!(matches!(parse_observation_target("ftp://example.com/*"), Err(ObservationTargetError::Url(_))));
    }

    #[test]
    fn equivalent_forms_collapse_but_scope_is_identity() {
        let id = |value: &str| {
            let parsed = parse(value);
            TargetId::for_url(parsed.canonical_url.as_str(), parsed.scope)
        };
        let site = id("https://example.com/*");
        for same in ["https://example.com//*", "https://example.com/?utm_source=x/*", "https://www.example.com/*", "example.com/*", "http://example.com/*"] {
            assert_eq!(id(same), site, "{same}");
        }
        assert_ne!(id("https://example.com"), site, "the resource and its information surface are different targets");
        assert_eq!(id("https://example.com"), id("https://example.com/?utm_source=x"));
    }
}
