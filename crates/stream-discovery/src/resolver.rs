//! The observation target resolver: seed URL + scope → who or what the
//! target is on its provider → the strategy that discovers its information
//! surfaces → a bounded discovery plan.
//!
//! Provider knowledge lives here, in the runtime — never in the CLI or UI.

use crate::{discover, github_repository, github_surfaces, DiscoveredSurface, DiscoveryPolicy, DiscoveryReport};
use stream_ingest::HttpFetcher;
use stream_model::{
    canonicalize_url, DiscoveryConfidence, DiscoveryMethod, ObservationScope, SourceKind, SurfaceKind, TargetIdentity,
    TargetKind, TargetProvider,
};
use url::Url;

/// How the information surface of a target is discovered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryStrategy {
    /// Scope `resource`: observe exactly this resource; no discovery.
    Resource,
    /// Bounded generic web discovery from the seed page.
    GenericWeb,
    /// The surfaces of one GitHub repository.
    GithubRepository { owner: String, repo: String },
    /// The posts of one X account.
    XAccount { handle: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryPlan {
    pub identity: TargetIdentity,
    pub strategy: DiscoveryStrategy,
    /// The URL discovery starts from — `None` when the provider is resolved
    /// without fetching the seed (X).
    pub root: Option<Url>,
}

/// Public observation endpoints for providers that have no open feed of
/// their own. Configured by the user; Stream never stores credentials.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderEndpoints {
    /// A feed URL template for an X account, with `{handle}` substituted,
    /// e.g. a feed bridge the user trusts. Unset → X targets are durable but
    /// report "observation unavailable".
    pub x_feed_template: Option<String>,
}

impl ProviderEndpoints {
    /// `STREAM_X_FEED_TEMPLATE=https://bridge.example/{handle}/rss`
    pub fn from_env() -> Self {
        Self {
            x_feed_template: std::env::var("STREAM_X_FEED_TEMPLATE")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| value.contains("{handle}")),
        }
    }
}

const X_HOSTS: &[&str] = &["x.com", "twitter.com", "mobile.twitter.com", "mobile.x.com"];
/// First path segments on X that are not accounts.
const X_RESERVED: &[&str] = &[
    "home", "i", "search", "explore", "settings", "notifications", "messages", "compose", "hashtag", "intent", "share",
    "login", "logout", "signup", "tos", "privacy", "about", "jobs", "download",
];

fn host(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    host.strip_prefix("www.").map(str::to_owned).unwrap_or(host)
}

/// The X account a URL is about: `x.com/<handle>`, `x.com/<handle>/status/…`.
pub fn x_account(url: &Url) -> Option<String> {
    if !X_HOSTS.contains(&host(url).as_str()) {
        return None;
    }
    let handle = url.path_segments()?.find(|segment| !segment.is_empty())?;
    let valid = !handle.is_empty()
        && handle.len() <= 15
        && handle.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !X_RESERVED.contains(&handle.to_ascii_lowercase().as_str());
    valid.then(|| handle.to_owned())
}

fn site_name(url: &Url) -> String {
    let path = url.path().trim_end_matches('/');
    if path.is_empty() {
        host(url)
    } else {
        format!("{}{path}", host(url))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObservationTargetResolver {
    pub endpoints: ProviderEndpoints,
}

impl ObservationTargetResolver {
    pub fn new(endpoints: ProviderEndpoints) -> Self {
        Self { endpoints }
    }

    /// Resolve a target. Pure: no network access.
    pub fn resolve(&self, seed: &Url, scope: ObservationScope) -> DiscoveryPlan {
        let name = site_name(seed);
        if scope == ObservationScope::Resource {
            return DiscoveryPlan {
                identity: TargetIdentity {
                    provider: provider_of(seed),
                    kind: TargetKind::Resource,
                    key: name.clone(),
                    display_name: name,
                    watching: "Watching this resource".into(),
                },
                strategy: DiscoveryStrategy::Resource,
                root: Some(seed.clone()),
            };
        }
        if let Some(handle) = x_account(seed) {
            return DiscoveryPlan {
                identity: TargetIdentity {
                    provider: TargetProvider::X,
                    kind: TargetKind::Account,
                    key: handle.to_ascii_lowercase(),
                    display_name: format!("@{handle}"),
                    watching: "Watching posts".into(),
                },
                strategy: DiscoveryStrategy::XAccount { handle },
                root: None,
            };
        }
        if let Some((owner, repo)) = github_repository(seed) {
            return DiscoveryPlan {
                identity: TargetIdentity {
                    provider: TargetProvider::Github,
                    kind: TargetKind::Repository,
                    key: format!("{owner}/{repo}").to_ascii_lowercase(),
                    display_name: format!("{owner}/{repo}"),
                    watching: "Watching this project".into(),
                },
                strategy: DiscoveryStrategy::GithubRepository { owner, repo },
                root: Some(seed.clone()),
            };
        }
        let kind = if seed.path().trim_matches('/').is_empty() { TargetKind::Site } else { TargetKind::Section };
        DiscoveryPlan {
            identity: TargetIdentity {
                provider: TargetProvider::Web,
                kind,
                key: name.clone(),
                display_name: name,
                watching: "Watching this information surface".into(),
            },
            strategy: DiscoveryStrategy::GenericWeb,
            root: Some(seed.clone()),
        }
    }

    /// Carry out a plan's discovery, bounded by `policy`.
    pub async fn discover(&self, fetcher: &HttpFetcher, plan: &DiscoveryPlan, seed: &Url, policy: &DiscoveryPolicy) -> DiscoveryReport {
        match &plan.strategy {
            DiscoveryStrategy::Resource => DiscoveryReport { seed: Some(seed.clone()), seed_reachable: true, ..Default::default() },
            DiscoveryStrategy::GenericWeb => discover(fetcher, seed, policy).await,
            DiscoveryStrategy::GithubRepository { owner, repo } => {
                // Known provider surfaces: nothing needs to be fetched to find them.
                let mut surfaces = github_surfaces(owner, repo, seed, DiscoveryConfidence::High);
                for surface in &mut surfaces {
                    surface.method = DiscoveryMethod::ProviderResolution;
                    if surface.kind == SurfaceKind::Repository {
                        surface.confidence = DiscoveryConfidence::High;
                    }
                }
                let base = format!("https://github.com/{owner}/{repo}");
                let tags = Url::parse(&format!("{base}/tags.atom")).expect("github url");
                surfaces.push(DiscoveredSurface {
                    url: tags.clone(),
                    fetch_url: tags,
                    title: Some(format!("{owner}/{repo} tags")),
                    kind: SurfaceKind::Releases,
                    adapter: SourceKind::Atom,
                    method: DiscoveryMethod::ProviderResolution,
                    confidence: DiscoveryConfidence::Medium,
                    discovered_from: seed.clone(),
                    reason: format!("tag feed of the GitHub repository {owner}/{repo}"),
                });
                surfaces.truncate(policy.max_surfaces);
                DiscoveryReport { seed: Some(seed.clone()), seed_title: Some(format!("{owner}/{repo}")), seed_reachable: true, surfaces, ..Default::default() }
            }
            DiscoveryStrategy::XAccount { handle } => {
                let mut report = DiscoveryReport { seed: Some(seed.clone()), seed_title: Some(format!("@{handle}")), ..Default::default() };
                let Some(template) = &self.endpoints.x_feed_template else {
                    report.unavailable = Some(format!(
                        "Observation unavailable: X offers no public feed Stream can read without credentials. \
                         Stream keeps this target and will start observing @{handle} once an X feed endpoint is configured (STREAM_X_FEED_TEMPLATE)."
                    ));
                    return report;
                };
                let Ok(feed) = Url::parse(&template.replace("{handle}", handle)) else {
                    report.unavailable = Some("Observation unavailable: the configured X feed endpoint is not a valid URL.".into());
                    return report;
                };
                report.seed_reachable = true;
                report.surfaces.push(DiscoveredSurface {
                    url: canonicalize_url(feed.as_str()).unwrap_or_else(|_| feed.clone()),
                    fetch_url: feed,
                    title: Some(format!("@{handle} posts")),
                    kind: SurfaceKind::Posts,
                    adapter: SourceKind::Rss,
                    method: DiscoveryMethod::ProviderApi,
                    confidence: DiscoveryConfidence::High,
                    discovered_from: seed.clone(),
                    reason: format!("posts of the X account @{handle}, through the configured public feed endpoint"),
                });
                report
            }
        }
    }
}

fn provider_of(url: &Url) -> TargetProvider {
    if x_account(url).is_some() {
        TargetProvider::X
    } else if github_repository(url).is_some() {
        TargetProvider::Github
    } else {
        TargetProvider::Web
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_model::parse_observation_target;

    fn plan(input: &str) -> DiscoveryPlan {
        let parsed = parse_observation_target(input).unwrap();
        ObservationTargetResolver::default().resolve(&parsed.seed_url, parsed.scope)
    }

    #[test]
    fn resolves_x_status_scope_to_the_account() {
        let plan = plan("https://x.com/devxritesh/status/*");
        assert_eq!(plan.identity.provider, TargetProvider::X);
        assert_eq!(plan.identity.kind, TargetKind::Account);
        assert_eq!(plan.identity.display_name, "@devxritesh");
        assert_eq!(plan.strategy, DiscoveryStrategy::XAccount { handle: "devxritesh".into() });
        assert!(plan.root.is_none(), "an X account is resolved, not fetched");
    }

    #[test]
    fn resolves_github_and_web() {
        let github = plan("https://github.com/foo/bar/*");
        assert_eq!(github.identity.kind, TargetKind::Repository);
        assert_eq!(github.strategy, DiscoveryStrategy::GithubRepository { owner: "foo".into(), repo: "bar".into() });
        assert_eq!(plan("https://example.com/*").identity.kind, TargetKind::Site);
        assert_eq!(plan("https://example.com/product/*").identity.kind, TargetKind::Section);
        assert_eq!(plan("https://example.com").strategy, DiscoveryStrategy::Resource);
        assert_eq!(plan("https://x.com/devxritesh").strategy, DiscoveryStrategy::Resource);
        assert_eq!(plan("https://x.com/home/*").strategy, DiscoveryStrategy::GenericWeb);
    }

    #[tokio::test]
    async fn x_without_endpoint_is_unavailable_and_fetches_nothing() {
        let resolver = ObservationTargetResolver::default();
        let parsed = parse_observation_target("https://x.com/devxritesh/status/*").unwrap();
        let plan = resolver.resolve(&parsed.seed_url, parsed.scope);
        // Deny-all network policy: any fetch would fail loudly in `failures`.
        let fetcher = HttpFetcher::new(stream_ingest::NetworkPolicy::default());
        let report = resolver.discover(&fetcher, &plan, &parsed.seed_url, &DiscoveryPolicy::default()).await;
        assert!(report.unavailable.is_some());
        assert!(report.failures.is_empty() && report.surfaces.is_empty());
    }
}
