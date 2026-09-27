//! Continuous observation: a URL becomes an observation target, discovery
//! finds the information surfaces downstream of it, and a durable scheduler
//! keeps observing them — detecting new feed items, page changes, and new
//! downstream pages, and feeding what is new into the signal pipeline.
//!
//! Everything that decides what happens next lives in FeltDB: targets and
//! their discovery schedule, sources and their `next_observation_at`, page
//! snapshots, discovery relations, and observation runs. A restarted process
//! simply asks FeltDB what is due.

use super::intelligence::{AddUrlOutcome, Novelty, ObservationReport, ProgressFn};
use super::records::*;
use super::{fetch_attempt_record, source_record, StreamRuntime};
use anyhow::{anyhow, bail, Context as _, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use stream_discovery::{same_site, DiscoveredSurface, DiscoveryPolicy, DiscoveryStrategy};
use stream_ingest::{check_url, detect_format, FetchError};
use stream_model::{
    canonicalize_url, classify_url, parse_observation_target, ObservationScope, discovery_relation_id, DiscoveryConfidence, DiscoveryMethod, DiscoveryRelation,
    DiscoveryStatus, FailureCategory, FetchAttempt, FetchStatus, Item, NormalizedItem, ObservationRun, ObservationRunId,
    ObservationTarget, PageChange, PageSnapshot, ProcessingStage, SnapshotId, Source, SourceId, SourceKind,
    SourceStatus, SurfaceKind, TargetId, TargetStatus,
};
use url::Url;

/// New downstream pages registered per observation of a section.
pub const MAX_NEW_PAGES_PER_OBSERVATION: usize = 5;
/// Sources observed per scheduler pass.
pub const MAX_SOURCES_PER_RUN: usize = 50;
/// A discovery marked running longer than this is presumed abandoned
/// (the process died) and is due again.
pub const STALE_DISCOVERY_MINUTES: i64 = 10;
/// Consecutive failures after which a source is shown as unavailable. It is
/// still retried, slowly, and never removed.
pub const UNAVAILABLE_AFTER_FAILURES: u32 = 3;
const LEASE_ID: &str = "observation-worker";
const LEASE_MINUTES: i64 = 10;
/// Words changed before a page change counts as material.
const MATERIAL_WORDS: usize = 6;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddTargetOutcome {
    pub target: ObservationTarget,
    /// The resource (scope `resource`) or the seed surface discovery starts
    /// from. `None` for targets resolved without a seed page (X accounts).
    pub seed_source: Option<Source>,
    pub existing: bool,
}

/// The health of a watched source, for people.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealth {
    Pending,
    Healthy,
    Retrying,
    Unavailable,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastChange {
    pub change: PageChange,
    pub summary: String,
    pub observed_at: DateTime<Utc>,
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

/// A source as part of a target's observation graph, with the answer to
/// "why is Stream watching this?".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchedSource {
    pub source: Source,
    pub relations: Vec<DiscoveryRelation>,
    pub why: String,
    pub health: SourceHealth,
    pub last_change: Option<LastChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetView {
    pub target: ObservationTarget,
    pub sources: Vec<WatchedSource>,
    pub signal_ids: Vec<stream_model::SignalId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryOutcome {
    pub target: ObservationTarget,
    pub sources: Vec<SourceId>,
    pub new_sources: Vec<SourceId>,
    pub skipped: Vec<(String, String)>,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunOptions {
    #[serde(default)]
    pub trigger: String,
    /// Limit the run to one target.
    #[serde(default)]
    pub target_id: Option<TargetId>,
    /// Discover and observe now, even if not yet due.
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLease {
    pub holder: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationStatus {
    pub targets: usize,
    pub watching_targets: usize,
    pub sources: usize,
    pub due_sources: usize,
    pub failing_sources: usize,
    pub next_due_at: Option<DateTime<Utc>>,
    pub last_run: Option<ObservationRun>,
    pub worker: Option<WorkerLease>,
}

fn hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let cut = text.char_indices().nth(max).map(|(i, _)| i).unwrap_or(text.len());
    format!("{}…", text[..cut].trim_end())
}

/// Plain-language description of a fetch failure.
pub fn describe_failure(error: &FetchError) -> String {
    match error {
        FetchError::Http { status: 404 | 410, .. } => format!("Source unavailable (HTTP {})", error.status().unwrap_or(404)),
        FetchError::Http { status: 429, retry_after } => match retry_after {
            Some(seconds) => format!("Rate limited (HTTP 429); asked to wait {seconds}s"),
            None => "Rate limited (HTTP 429)".into(),
        },
        FetchError::Http { status, .. } if *status >= 500 => format!("Server error (HTTP {status})"),
        FetchError::Http { status, .. } => format!("HTTP {status}"),
        FetchError::Dns(detail) => format!("Could not resolve the host ({detail})"),
        FetchError::Timeout => "Timed out".into(),
        FetchError::Policy(detail) => format!("Not fetched: {detail}"),
        FetchError::TooLarge(bytes) => format!("Document larger than {} MB", bytes / (1024 * 1024)),
        FetchError::Redirects => "Too many redirects".into(),
        FetchError::Network(detail) => format!("Network error ({detail})"),
    }
}

fn failure_category(error: &FetchError) -> FailureCategory {
    match error {
        FetchError::Http { status: 401, .. } => FailureCategory::Authentication,
        FetchError::Http { status: 403, .. } | FetchError::Policy(_) => FailureCategory::Authorization,
        _ => FailureCategory::Network,
    }
}

/// When to try again after a failure: the server's Retry-After if given;
/// otherwise exponential backoff — quick for temporary failures, slower for
/// permanent ones, capped at a day. Sources are never dropped.
pub fn retry_delay(interval_minutes: u32, failures: u32, error: &FetchError) -> Duration {
    if let Some(seconds) = error.retry_after() {
        return Duration::seconds(seconds.max(60) as i64);
    }
    let exponent = failures.saturating_sub(1).min(10);
    let base = if error.is_temporary() { 5 } else { interval_minutes.max(5) as i64 };
    Duration::minutes((base * 2i64.pow(exponent)).min(24 * 60))
}

struct Comparison {
    change: PageChange,
    hash: String,
    blocks: Vec<String>,
    added: Vec<String>,
    removed: Vec<String>,
    summary: String,
    previous: Option<PageSnapshot>,
}

fn compare(page: &stream_web::WebPage, previous: Option<PageSnapshot>) -> Comparison {
    let lines = page.text.lines().map(str::trim).filter(|l| !l.is_empty()).collect::<Vec<_>>();
    let blocks = lines.iter().map(|l| stream_web::normalize_block(l)).collect::<Vec<_>>();
    let digest = hash(&blocks.join("\n"));
    let Some(previous) = previous else {
        return Comparison { change: PageChange::Baseline, hash: digest, blocks, added: vec![], removed: vec![], summary: "First observation recorded as the baseline".into(), previous: None };
    };
    if previous.content_hash == digest {
        return Comparison { change: PageChange::Unchanged, hash: digest, blocks, added: vec![], removed: vec![], summary: "No change".into(), previous: Some(previous) };
    }
    let before = previous.blocks.iter().collect::<HashSet<_>>();
    let now = blocks.iter().collect::<HashSet<_>>();
    let added = lines
        .iter()
        .zip(blocks.iter())
        .filter(|(_, block)| !before.contains(block))
        .map(|(line, _)| (*line).to_owned())
        .collect::<Vec<_>>();
    let removed = previous.blocks.iter().filter(|block| !now.contains(block)).cloned().collect::<Vec<_>>();
    let words = added.iter().chain(removed.iter()).map(|l| l.split_whitespace().count()).sum::<usize>();
    let versioned = added.iter().any(|l| l.split_whitespace().any(|w| w.chars().filter(|c| *c == '.').count() >= 1 && w.chars().any(|c| c.is_ascii_digit())));
    let change = if words >= MATERIAL_WORDS || versioned { PageChange::MateriallyChanged } else { PageChange::Changed };
    let summary = match (added.first(), removed.first()) {
        (Some(a), Some(r)) => format!("Now: “{}” — previously: “{}”", truncate(a, 120), truncate(r, 120)),
        (Some(a), None) => format!("New: “{}”", truncate(a, 160)),
        (None, Some(r)) => format!("Removed: “{}”", truncate(r, 160)),
        (None, None) => "Reordered content".into(),
    };
    Comparison { change, hash: digest, blocks, added, removed, summary, previous: Some(previous) }
}

impl StreamRuntime {
    // ------------------------------------------------------------- targets

    /// Add an observation target from what the user typed. `/*` is parsed
    /// here, once, into the target's scope; the target is durable before
    /// anything is fetched, and discovery and observation follow.
    ///
    /// - `https://example.com` → watch this resource.
    /// - `https://example.com/*` → watch the information surface beneath it.
    pub async fn add_target(&self, raw: &str, provenance: &str) -> Result<AddTargetOutcome> {
        let parsed = parse_observation_target(raw).map_err(|error| anyhow!(error))?;
        // Never persist what Stream must never fetch (credentials, other schemes, private
        // addresses unless the policy allows them).
        check_url(&parsed.seed_url, self.network_policy()).map_err(|error| anyhow!("{error}"))?;
        let plan = self.resolver.resolve(&parsed.seed_url, parsed.scope);

        let id = TargetId::for_url(parsed.canonical_url.as_str(), parsed.scope);
        if let Some(existing) = self.get_target(&id).await? {
            let seed = match existing.source_ids.first() {
                Some(seed) => self.get_source(seed).await?,
                None => None,
            };
            return Ok(AddTargetOutcome { target: existing, seed_source: seed, existing: true });
        }

        let mut target = ObservationTarget::new(parsed.clone(), plan.identity.clone(), provenance);
        let mut seed = None;
        match plan.strategy {
            DiscoveryStrategy::Resource => {
                // The resource itself: exactly what `stream add` has always observed.
                let AddUrlOutcome { source: mut resource, .. } = self.add_url(parsed.seed_url.as_str(), provenance).await?;
                if resource.target_id.is_none() {
                    resource.target_id = Some(target.id.clone());
                    resource.discovery_method = Some(DiscoveryMethod::UserAdded);
                    resource.discovery_confidence = Some(DiscoveryConfidence::High);
                    resource.discovered_from = Some(parsed.canonical_url.clone());
                    resource.discovery_reason = Some("the resource you added".into());
                    resource.surface_kind = Some(SurfaceKind::Page);
                    resource.relevant = true;
                    resource.next_observation_at.get_or_insert(Utc::now());
                    resource.updated_at = Utc::now();
                    self.store.update("Source", resource.id.as_str(), source_record(&resource)).await?;
                }
                target.source_ids = vec![resource.id.clone()];
                // Nothing to discover for a resource.
                target.discovery_status = DiscoveryStatus::Complete;
                target.discovery_detail = Some(target.identity.watching.clone());
                target.next_discovery_at = None;
                target.status = TargetStatus::Active;
                seed = Some(resource);
            }
            DiscoveryStrategy::XAccount { .. } => {
                // Resolved, not fetched: the account is the target, not a literal
                // `/status/` page.
                target.title = Some(target.identity.display_name.clone());
            }
            DiscoveryStrategy::GenericWeb | DiscoveryStrategy::GithubRepository { .. } => {
                let root = plan.root.clone().unwrap_or_else(|| parsed.seed_url.clone());
                let identity = canonicalize_url(root.as_str()).map_err(|error| anyhow!(error))?;
                let surface = DiscoveredSurface {
                    url: identity.clone(),
                    fetch_url: root,
                    title: None,
                    kind: if matches!(plan.strategy, DiscoveryStrategy::GithubRepository { .. }) { SurfaceKind::Repository } else { SurfaceKind::Homepage },
                    adapter: SourceKind::Web,
                    method: DiscoveryMethod::Seed,
                    confidence: DiscoveryConfidence::High,
                    discovered_from: identity,
                    reason: "the URL you added".into(),
                };
                let (source, _) = self.register_surface(&target, &surface, None).await?;
                target.source_ids = vec![source.id.clone()];
                seed = Some(source);
            }
        }
        match self.store.insert("ObservationTarget", target.id.as_str(), target_record(&target), true).await {
            Ok(_) => {}
            Err(error) => match self.get_target(&target.id).await? {
                Some(existing) => return Ok(AddTargetOutcome { target: existing, seed_source: seed, existing: true }),
                None => return Err(error),
            },
        }
        if let Some(source) = &seed {
            let method = source.discovery_method.unwrap_or(DiscoveryMethod::Seed);
            let kind = source.surface_kind.unwrap_or(SurfaceKind::Page);
            self.upsert_relation(&target, source, &parsed.canonical_url, None, method, DiscoveryConfidence::High, kind, source.discovery_reason.as_deref().unwrap_or("the URL you added"))
                .await?;
        }
        Ok(AddTargetOutcome { target, seed_source: seed, existing: false })
    }

    pub async fn get_target(&self, id: &TargetId) -> Result<Option<ObservationTarget>> {
        self.store.get("ObservationTarget", id.as_str()).await?.map(target_from_value).transpose()
    }

    pub async fn list_targets(&self) -> Result<Vec<ObservationTarget>> {
        let mut targets = self
            .store
            .all("ObservationTarget")
            .await?
            .into_iter()
            .map(target_from_value)
            .collect::<Result<Vec<_>>>()?;
        targets.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(targets)
    }

    async fn save_target(&self, target: &mut ObservationTarget) -> Result<()> {
        target.updated_at = Utc::now();
        self.store.update("ObservationTarget", target.id.as_str(), target_record(target)).await?;
        Ok(())
    }

    pub async fn pause_target(&self, id: &TargetId) -> Result<ObservationTarget> {
        let mut target = self.get_target(id).await?.ok_or_else(|| anyhow!("unknown target: {id}"))?;
        target.status = TargetStatus::Paused;
        self.save_target(&mut target).await?;
        Ok(target)
    }

    pub async fn resume_target(&self, id: &TargetId) -> Result<ObservationTarget> {
        let mut target = self.get_target(id).await?.ok_or_else(|| anyhow!("unknown target: {id}"))?;
        target.status = if target.last_discovered_at.is_some() { TargetStatus::Active } else { TargetStatus::Pending };
        let now = Utc::now();
        target.next_discovery_at = Some(target.next_discovery_at.map(|at| at.min(now)).unwrap_or(now));
        self.save_target(&mut target).await?;
        Ok(target)
    }

    #[allow(clippy::too_many_arguments)]
    async fn upsert_relation(
        &self,
        target: &ObservationTarget,
        source: &Source,
        from: &Url,
        from_source: Option<&SourceId>,
        method: DiscoveryMethod,
        confidence: DiscoveryConfidence,
        kind: SurfaceKind,
        reason: &str,
    ) -> Result<DiscoveryRelation> {
        let id = discovery_relation_id(&target.id, &source.id);
        let now = Utc::now();
        if let Some(value) = self.store.get("DiscoveryRelation", &id).await? {
            let mut relation = relation_from_value(value)?;
            relation.last_confirmed_at = now;
            self.store.update("DiscoveryRelation", &id, relation_record(&relation)).await?;
            return Ok(relation);
        }
        let relation = DiscoveryRelation {
            id: id.clone(),
            target_id: target.id.clone(),
            source_id: source.id.clone(),
            discovered_from: from.clone(),
            from_source_id: from_source.cloned(),
            method,
            confidence,
            surface_kind: kind,
            reason: reason.to_owned(),
            discovered_at: now,
            last_confirmed_at: now,
        };
        self.store.insert("DiscoveryRelation", &id, relation_record(&relation), true).await?;
        Ok(relation)
    }

    pub async fn relations_of_target(&self, id: &TargetId) -> Result<Vec<DiscoveryRelation>> {
        self.store
            .find("DiscoveryRelation", json!({ "target": id.as_str() }))
            .await?
            .into_iter()
            .map(relation_from_value)
            .collect()
    }

    /// Register a discovered surface as a durable source (or confirm it).
    /// Returns the source and whether it is new.
    async fn register_surface(
        &self,
        target: &ObservationTarget,
        surface: &DiscoveredSurface,
        from_source: Option<&SourceId>,
    ) -> Result<(Source, bool)> {
        let identity = surface.url.as_str();
        let id = SourceId::for_identity(identity);
        let existing = match self.get_source(&id).await? {
            Some(found) => Some(found),
            None => self.find_source_by_identity(identity).await?,
        };
        let (source, is_new) = match existing {
            Some(mut source) => {
                let mut changed = false;
                if source.target_id.is_none() {
                    source.target_id = Some(target.id.clone());
                    source.discovery_method = Some(surface.method);
                    source.discovery_confidence = Some(surface.confidence);
                    source.discovered_from = Some(surface.discovered_from.clone());
                    source.discovery_reason = Some(surface.reason.clone());
                    source.next_observation_at.get_or_insert(Utc::now());
                    changed = true;
                }
                if source.target_id.as_ref() == Some(&target.id) && source.surface_kind != Some(surface.kind) && surface.method == DiscoveryMethod::Seed {
                    source.surface_kind = Some(surface.kind);
                    changed = true;
                }
                if source.title.is_none() && surface.title.is_some() {
                    source.title = surface.title.clone();
                    changed = true;
                }
                if source.target_id.as_ref() == Some(&target.id) && source.next_observation_at.is_none() {
                    source.next_observation_at = Some(Utc::now());
                    changed = true;
                }
                if changed {
                    source.updated_at = Utc::now();
                    self.store.update("Source", source.id.as_str(), source_record(&source)).await?;
                }
                (source, false)
            }
            None => {
                let user_kind = classify_url(&surface.url);
                let kind = if user_kind == SourceKind::Web && surface.adapter.is_feed_format() { surface.adapter } else { user_kind };
                let mut source = Source::new(kind, surface.fetch_url.clone());
                source.id = id;
                source.identity = identity.to_owned();
                source.canonical_url = surface.url.clone();
                source.adapter_kind = surface.adapter;
                source.title = surface.title.clone();
                source.provenance = "discovery".into();
                source.target_id = Some(target.id.clone());
                source.surface_kind = Some(surface.kind);
                source.discovery_method = Some(surface.method);
                source.discovery_confidence = Some(surface.confidence);
                source.discovered_from = Some(surface.discovered_from.clone());
                source.discovery_reason = Some(surface.reason.clone());
                source.relevant = surface.confidence != DiscoveryConfidence::Low;
                source.refresh_minutes = target.discovery_policy.observe_every_minutes;
                source.next_observation_at = Some(Utc::now());
                match self.store.insert("Source", source.id.as_str(), source_record(&source), true).await {
                    Ok(_) => (source, true),
                    Err(_) => (self.get_source(&source.id).await?.context("source vanished during registration")?, false),
                }
            }
        };
        self.upsert_relation(target, &source, &surface.discovered_from, from_source, surface.method, surface.confidence, surface.kind, &surface.reason)
            .await?;
        Ok((source, is_new))
    }

    /// Discovery: what information surfaces exist downstream of this target?
    /// Separate from observation, with its own cadence.
    pub async fn discover_target(&self, id: &TargetId) -> Result<DiscoveryOutcome> {
        let mut target = self.get_target(id).await?.ok_or_else(|| anyhow!("unknown target: {id}"))?;
        if target.scope == ObservationScope::Resource {
            // A resource is observed, not discovered beneath.
            return Ok(DiscoveryOutcome { sources: target.source_ids.clone(), target, new_sources: vec![], skipped: vec![], failure: None });
        }
        let now = Utc::now();
        let previous_status = target.status;
        target.status = TargetStatus::Discovering;
        target.discovery_status = DiscoveryStatus::Running;
        target.discovery_started_at = Some(now);
        self.save_target(&mut target).await?;

        let policy = DiscoveryPolicy { max_surfaces: target.discovery_policy.max_sources, ..DiscoveryPolicy::default() };
        let plan = self.resolver.resolve(&target.seed_url, target.scope);
        let report = self.resolver.discover(&self.fetcher, &plan, &target.seed_url, &policy).await;
        let skipped = report.skipped.iter().map(|(url, why)| (url.to_string(), why.clone())).collect::<Vec<_>>();

        if let Some(reason) = report.unavailable.clone() {
            // Durable, honest, and retried: never pretend to be watching.
            target.status = if previous_status == TargetStatus::Paused { TargetStatus::Paused } else { TargetStatus::Unavailable };
            target.discovery_status = DiscoveryStatus::Complete;
            target.discovery_detail = Some(reason.clone());
            target.last_discovered_at = Some(now);
            target.next_discovery_at = Some(now + Duration::hours(target.discovery_policy.discover_every_hours as i64));
            self.save_target(&mut target).await?;
            return Ok(DiscoveryOutcome { sources: target.source_ids.clone(), target, new_sources: vec![], skipped, failure: Some(reason) });
        }

        if !report.seed_reachable {
            let error = report.seed_failure().cloned().unwrap_or(FetchError::Network("unreachable".into()));
            target.consecutive_failures += 1;
            target.discovery_status = DiscoveryStatus::Failed;
            let detail = format!("{} — Stream will try again", describe_failure(&error));
            target.discovery_detail = Some(detail.clone());
            target.status = if target.last_discovered_at.is_some() && previous_status != TargetStatus::Paused {
                TargetStatus::Active
            } else if previous_status == TargetStatus::Paused {
                TargetStatus::Paused
            } else {
                TargetStatus::Failed
            };
            target.next_discovery_at = Some(Utc::now() + retry_delay(target.discovery_policy.discover_every_hours * 60, target.consecutive_failures, &error));
            self.save_target(&mut target).await?;
            return Ok(DiscoveryOutcome { sources: target.source_ids.clone(), target, new_sources: vec![], skipped, failure: Some(detail) });
        }

        let mut sources = Vec::new();
        let mut new_sources = Vec::new();
        let by_url = report.surfaces.iter().map(|s| (s.url.to_string(), s)).collect::<HashMap<_, _>>();
        for surface in &report.surfaces {
            // Feeds declared by a section point back at that section's source.
            let from_source = by_url
                .get(canonicalize_url(surface.discovered_from.as_str()).map(|u| u.to_string()).unwrap_or_default().as_str())
                .filter(|parent| parent.url != surface.url)
                .map(|parent| SourceId::for_identity(parent.url.as_str()));
            let (source, is_new) = self.register_surface(&target, surface, from_source.as_ref()).await?;
            if is_new {
                new_sources.push(source.id.clone());
            }
            if !sources.contains(&source.id) {
                sources.push(source.id.clone());
            }
        }
        for id in &sources {
            if !target.source_ids.contains(id) {
                target.source_ids.push(id.clone());
            }
        }
        let now = Utc::now();
        target.title = report.seed_title.clone().or(target.title.take());
        target.status = if previous_status == TargetStatus::Paused { TargetStatus::Paused } else { TargetStatus::Active };
        target.discovery_status = DiscoveryStatus::Complete;
        target.consecutive_failures = 0;
        target.last_discovered_at = Some(now);
        target.next_discovery_at = Some(now + Duration::hours(target.discovery_policy.discover_every_hours as i64));
        target.discovery_detail = Some(format!(
            "Found {}{}",
            if target.source_ids.len() == 1 { "1 information surface".to_owned() } else { format!("{} information surfaces", target.source_ids.len()) },
            if new_sources.is_empty() { String::new() } else { format!(" ({} new)", new_sources.len()) }
        ));
        self.save_target(&mut target).await?;
        Ok(DiscoveryOutcome { target, sources, new_sources, skipped, failure: None })
    }

    // ------------------------------------------------------ watched sources

    async fn latest_snapshot(&self, source: &SourceId) -> Result<Option<PageSnapshot>> {
        let mut snapshots = self
            .store
            .find("PageSnapshot", json!({ "source": source.as_str() }))
            .await?
            .into_iter()
            .map(snapshot_from_value)
            .collect::<Result<Vec<_>>>()?;
        snapshots.sort_by(|a, b| b.observed_at.cmp(&a.observed_at));
        Ok(snapshots.into_iter().next())
    }

    pub async fn snapshots_of(&self, source: &SourceId) -> Result<Vec<PageSnapshot>> {
        let mut snapshots = self
            .store
            .find("PageSnapshot", json!({ "source": source.as_str() }))
            .await?
            .into_iter()
            .map(snapshot_from_value)
            .collect::<Result<Vec<_>>>()?;
        snapshots.sort_by(|a, b| b.observed_at.cmp(&a.observed_at));
        Ok(snapshots)
    }

    pub(crate) async fn observe_interval(&self, source: &Source) -> Result<u32> {
        Ok(match &source.target_id {
            Some(id) => self.get_target(id).await?.map(|t| t.discovery_policy.observe_every_minutes).unwrap_or(source.refresh_minutes),
            None => source.refresh_minutes,
        }
        .max(1))
    }

    /// Record a failed observation: durable, explained, retried with backoff.
    async fn watched_failure(
        &self,
        source: &mut Source,
        attempt: &mut FetchAttempt,
        error: &FetchError,
        report: &mut ObservationReport,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<()> {
        let now = Utc::now();
        let description = describe_failure(error);
        attempt.status = FetchStatus::Failed;
        attempt.completed_at = Some(now);
        attempt.failure_category = Some(failure_category(error));
        attempt.diagnostics = json!({ "message": description, "category": failure_category(error).to_string(), "status": error.status(), "temporary": error.is_temporary() });
        self.store.update("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(attempt)).await?;
        source.consecutive_failures += 1;
        source.last_failure_at = Some(now);
        source.last_error_category = Some(failure_category(error));
        source.last_error_message = Some(description.clone());
        source.status = if source.consecutive_failures >= UNAVAILABLE_AFTER_FAILURES { SourceStatus::Failed } else { SourceStatus::Active };
        let interval = self.observe_interval(source).await?;
        source.next_observation_at = Some(now + retry_delay(interval, source.consecutive_failures, error));
        let last_ok = source
            .last_success_at
            .map(|at| format!("last successful observation {}", at.format("%b %-d, %H:%M UTC")))
            .unwrap_or_else(|| "never observed successfully".into());
        let detail = format!("{description} — {last_ok}");
        report.failure = Some(detail.clone());
        self.set_stage(source, ProcessingStage::Failed, Some(detail), progress).await
    }

    /// Observe one surface of an observation target.
    pub(crate) async fn observe_watched(
        &self,
        source: &mut Source,
        report: &mut ObservationReport,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<()> {
        source.last_checked_at = Some(Utc::now());
        self.set_stage(source, ProcessingStage::Fetching, None, progress).await?;
        let mut attempt = FetchAttempt::started(source.id.clone());
        self.store.insert("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(&attempt), true).await?;

        let document = match self.fetcher.fetch_document(&source.endpoint).await {
            Ok(document) => document,
            Err(error) => return self.watched_failure(source, &mut attempt, &error, report, progress).await,
        };
        let format = detect_format(document.content_type.as_deref(), &document.body);
        let baseline = source.last_observed_at.is_none();
        let mut candidates: Vec<(Item, Novelty)> = Vec::new();
        let mut detail;

        if format.is_feed() {
            if source.adapter_kind != format.adapter_kind() {
                source.adapter_kind = format.adapter_kind();
            }
            let items = match self.parse_feed(source, &document.body) {
                Ok(items) => items,
                Err(error) => {
                    let failure = FetchError::Network(format!("invalid feed: {error:#}"));
                    return self.watched_failure(source, &mut attempt, &failure, report, progress).await;
                }
            };
            attempt.diagnostics = json!({ "format": format!("{format:?}").to_lowercase(), "final_url": document.final_url.as_str() });
            let (persisted, _, outcomes) = self.persist_observation(source.clone(), attempt, items).await?;
            *source = persisted;
            let fresh = outcomes.iter().filter(|o| o.is_new).count();
            report.items_observed = outcomes.len();
            report.new_item_ids = outcomes.iter().filter(|o| o.is_new).map(|o| o.item.id.clone()).collect();
            for outcome in outcomes.into_iter().filter(|o| o.is_new) {
                candidates.push((outcome.item, if baseline { Novelty::Baseline } else { Novelty::Fresh }));
            }
            detail = match (baseline, fresh) {
                (true, n) => format!("Baseline: {n} existing entries recorded"),
                (false, 0) => "No new entries".to_owned(),
                (false, 1) => "1 new entry".to_owned(),
                (false, n) => format!("{n} new entries"),
            };
        } else {
            let html = String::from_utf8_lossy(&document.body).into_owned();
            let page = match stream_web::parse_page(&document.final_url, &html) {
                Ok(page) => page,
                Err(error) => {
                    let failure = FetchError::Network(format!("unreadable page: {error:#}"));
                    return self.watched_failure(source, &mut attempt, &failure, report, progress).await;
                }
            };
            if source.title.is_none() {
                source.title = Some(page.title.clone());
            }
            let comparison = compare(&page, self.latest_snapshot(&source.id).await?);
            report.page_change = Some(comparison.change);
            let method = source.discovery_method.unwrap_or(DiscoveryMethod::Seed);
            let mut items = Vec::new();
            let mut novelty = Novelty::Fresh;
            match comparison.change {
                PageChange::Baseline if method == DiscoveryMethod::Seed => {
                    items.push(page.to_normalized_item(source.kind));
                    novelty = Novelty::Primary;
                }
                PageChange::Baseline if method == DiscoveryMethod::PageLink => items.push(page.to_normalized_item(source.kind)),
                PageChange::MateriallyChanged if !comparison.added.is_empty() => {
                    items.push(NormalizedItem {
                        source_kind: source.kind,
                        canonical_identity: format!("{}#change-{}", page.canonical_url, &comparison.hash[..12]),
                        canonical_url: Some(page.canonical_url.clone()),
                        title: format!("{}: {}", page.title, truncate(&comparison.added[0], 140)),
                        content_text: comparison.added.join("\n"),
                        content_html: None,
                        author: page.author.clone(),
                        published_at: None,
                        original_identifier: format!("{}#change-{}", document.final_url, &comparison.hash[..12]),
                        source_url: document.final_url.clone(),
                        parser: "web-change".into(),
                        transformations: vec!["html-main-text".into(), "normalized-diff".into()],
                    });
                }
                _ => {}
            }
            attempt.diagnostics = json!({ "format": "html", "change": comparison.change.as_str(), "final_url": document.final_url.as_str() });
            let (persisted, _, outcomes) = self.persist_observation(source.clone(), attempt, items).await?;
            *source = persisted;
            let item_id = outcomes.first().map(|o| o.item.id.clone());
            report.items_observed = outcomes.len();
            report.new_item_ids = outcomes.iter().filter(|o| o.is_new).map(|o| o.item.id.clone()).collect();
            if novelty == Novelty::Primary {
                report.primary_item_id = item_id.clone();
            }
            for outcome in outcomes {
                if outcome.is_new || novelty == Novelty::Primary {
                    candidates.push((outcome.item, novelty));
                }
            }

            // New pages that appeared in a section Stream watches.
            let links = page
                .links
                .iter()
                .filter(|link| same_site(&source.canonical_url, &link.url))
                .map(|link| link.url.to_string())
                .take(300)
                .collect::<Vec<_>>();
            if let (Some(previous), Some(kind)) = (&comparison.previous, source.surface_kind) {
                if kind.publishes_entries() && comparison.change != PageChange::Unchanged {
                    report.registered_sources = self.register_new_pages(source, kind, &page, &previous.links).await?;
                }
            }

            let snapshot = PageSnapshot {
                id: SnapshotId::generate(),
                source_id: source.id.clone(),
                url: document.final_url.clone(),
                title: page.title.clone(),
                observed_at: Utc::now(),
                content_hash: comparison.hash,
                blocks: comparison.blocks,
                links,
                change: comparison.change,
                added: comparison.added,
                removed: comparison.removed,
                summary: comparison.summary.clone(),
                item_id,
            };
            self.store.insert("PageSnapshot", snapshot.id.as_str(), snapshot_record(&snapshot), true).await?;
            detail = match comparison.change {
                PageChange::Baseline => "Baseline recorded".to_owned(),
                PageChange::Unchanged => "Unchanged".to_owned(),
                PageChange::Changed => format!("Minor change: {}", comparison.summary),
                PageChange::MateriallyChanged => format!("Changed: {}", comparison.summary),
            };
            if !report.registered_sources.is_empty() {
                detail.push_str(&format!("; {} new page(s) found", report.registered_sources.len()));
            }
        }

        self.understand(source, candidates, report, progress).await?;
        let now = Utc::now();
        source.last_observed_at = Some(now);
        source.status = SourceStatus::Active;
        source.next_observation_at = Some(now + Duration::minutes(self.observe_interval(source).await? as i64));
        match (report.signals_created.len(), report.signals_corroborated.len()) {
            (0, 0) => {}
            (created, 0) => detail.push_str(&format!("; {created} signal(s) built")),
            (0, corroborated) => detail.push_str(&format!("; corroborated {corroborated} signal(s)")),
            (created, corroborated) => detail.push_str(&format!("; {created} signal(s) built, {corroborated} corroborated")),
        }
        self.set_stage(source, ProcessingStage::Observed, Some(detail), progress).await
    }

    /// Register pages that newly appeared in a watched section as sources.
    async fn register_new_pages(
        &self,
        section: &Source,
        kind: SurfaceKind,
        page: &stream_web::WebPage,
        previous_links: &[String],
    ) -> Result<Vec<SourceId>> {
        let Some(target_id) = &section.target_id else { return Ok(vec![]) };
        let Some(target) = self.get_target(target_id).await? else { return Ok(vec![]) };
        let known = previous_links.iter().collect::<HashSet<_>>();
        let section_path = section.canonical_url.path().trim_end_matches('/').to_owned();
        let mut registered = Vec::new();
        for link in &page.links {
            if registered.len() >= MAX_NEW_PAGES_PER_OBSERVATION || target.source_ids.len() + registered.len() >= target.discovery_policy.max_sources {
                break;
            }
            let path = link.url.path();
            let downstream = same_site(&section.canonical_url, &link.url)
                && path.starts_with(&format!("{section_path}/"))
                && path.len() > section_path.len() + 1
                && !known.contains(&link.url.to_string());
            if !downstream || link.in_navigation {
                continue;
            }
            let Ok(canonical) = canonicalize_url(link.url.as_str()) else { continue };
            let surface = DiscoveredSurface {
                url: canonical,
                fetch_url: link.url.clone(),
                title: (!link.text.is_empty()).then(|| link.text.clone()),
                kind: SurfaceKind::Page,
                adapter: SourceKind::Web,
                method: DiscoveryMethod::PageLink,
                confidence: DiscoveryConfidence::Medium,
                discovered_from: section.canonical_url.clone(),
                reason: format!("a new page that appeared in the {} Stream watches", kind.label().to_lowercase()),
            };
            let (source, is_new) = self.register_surface(&target, &surface, Some(&section.id)).await?;
            if is_new {
                registered.push(source.id);
            }
        }
        if !registered.is_empty() {
            let mut target = self.get_target(target_id).await?.context("target vanished")?;
            target.source_ids.extend(registered.iter().cloned());
            self.save_target(&mut target).await?;
        }
        Ok(registered)
    }

    // ---------------------------------------------------------- scheduler

    async fn acquire_lease(&self) -> Result<bool> {
        let now = Utc::now();
        if let Some(value) = self.store.get("ObservationLease", LEASE_ID).await? {
            let holder = value.get("holder").and_then(|v| v.as_str()).unwrap_or_default().to_owned();
            let expires = value
                .get("expires_at")
                .and_then(|v| v.as_str())
                .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                .map(|v| v.with_timezone(&Utc));
            if holder != self.worker_id && expires.map(|at| at > now).unwrap_or(false) {
                return Ok(false);
            }
            self.store
                .update("ObservationLease", LEASE_ID, json!({ "__id": LEASE_ID, "holder": self.worker_id, "expires_at": (now + Duration::minutes(LEASE_MINUTES)).to_rfc3339() }))
                .await?;
        } else {
            self.store
                .insert("ObservationLease", LEASE_ID, json!({ "__id": LEASE_ID, "holder": self.worker_id, "expires_at": (now + Duration::minutes(LEASE_MINUTES)).to_rfc3339() }), false)
                .await?;
        }
        Ok(true)
    }

    async fn release_lease(&self) -> Result<()> {
        self.store
            .update("ObservationLease", LEASE_ID, json!({ "__id": LEASE_ID, "holder": self.worker_id, "expires_at": Utc::now().to_rfc3339() }))
            .await?;
        Ok(())
    }

    async fn lease(&self) -> Result<Option<WorkerLease>> {
        Ok(self.store.get("ObservationLease", LEASE_ID).await?.and_then(|value| {
            Some(WorkerLease {
                holder: value.get("holder")?.as_str()?.to_owned(),
                expires_at: DateTime::parse_from_rfc3339(value.get("expires_at")?.as_str()?).ok()?.with_timezone(&Utc),
            })
        }))
    }

    /// One pass of the observation worker: discover targets that are due,
    /// observe sources that are due, schedule what comes next. Everything it
    /// needs to know is read from FeltDB, so any process can run it after a
    /// restart and pick up exactly where the last one left off.
    pub async fn run_observation(&self, options: RunOptions) -> Result<ObservationRun> {
        let mut run = ObservationRun {
            id: ObservationRunId::generate(),
            trigger: if options.trigger.is_empty() { "manual".into() } else { options.trigger.clone() },
            started_at: Utc::now(),
            completed_at: None,
            targets_discovered: 0,
            sources_observed: 0,
            sources_failed: 0,
            new_items: 0,
            page_changes: 0,
            signals_created: 0,
            signals_corroborated: 0,
            detail: String::new(),
        };
        // The lease keeps two processes from running the schedule at once. A
        // run the user asked for, for one target, does not wait for it.
        let scheduled = options.target_id.is_none();
        if scheduled && !self.acquire_lease().await? {
            run.completed_at = Some(Utc::now());
            run.detail = "Another Stream process is observing right now; nothing to do here.".into();
            return Ok(run);
        }
        self.store.insert("ObservationRun", run.id.as_str(), run_record(&run), true).await?;
        let result = self.run_pass(&options, &mut run).await;
        run.completed_at = Some(Utc::now());
        if let Err(error) = &result {
            run.detail = format!("stopped early: {error:#}");
        }
        self.store.update("ObservationRun", run.id.as_str(), run_record(&run)).await?;
        if scheduled {
            self.release_lease().await?;
        }
        result.map(|_| run)
    }

    async fn run_pass(&self, options: &RunOptions, run: &mut ObservationRun) -> Result<()> {
        let now = Utc::now();
        let targets = self.list_targets().await?;
        let in_scope = |target: &ObservationTarget| options.target_id.as_ref().map(|id| id == &target.id).unwrap_or(true);

        for target in targets.iter().filter(|t| in_scope(t)) {
            if target.status == TargetStatus::Paused && !options.force {
                continue;
            }
            let stale = target.discovery_status == DiscoveryStatus::Running
                && target.discovery_started_at.map(|at| now - at > Duration::minutes(STALE_DISCOVERY_MINUTES)).unwrap_or(true);
            let due = target.next_discovery_at.map(|at| at <= now).unwrap_or(true)
                && target.discovery_status != DiscoveryStatus::Running;
            // Discovery keeps its own schedule; `force` forces observation only.
            if due || stale {
                self.discover_target(&target.id).await?;
                run.targets_discovered += 1;
            }
        }

        let watching = self
            .list_targets()
            .await?
            .into_iter()
            .filter(|t| t.status != TargetStatus::Paused && in_scope(t))
            .map(|t| t.id)
            .collect::<HashSet<_>>();
        let mut due = self
            .list_sources()
            .await?
            .into_iter()
            .filter(|s| s.relevant && s.status != SourceStatus::Paused)
            .filter(|s| s.target_id.as_ref().map(|t| watching.contains(t)).unwrap_or(false))
            .filter(|s| options.force || s.next_observation_at.map(|at| at <= now).unwrap_or(true))
            .collect::<Vec<_>>();
        due.sort_by(|a, b| a.next_observation_at.cmp(&b.next_observation_at));
        due.truncate(MAX_SOURCES_PER_RUN);

        let mut observed_targets = HashSet::new();
        // Two passes: sources registered during the first (new downstream
        // pages) are observed right away.
        for _ in 0..2 {
            let mut registered = Vec::new();
            for source in &due {
                let report = match self.observe_source(&source.id, None).await {
                    Ok(report) => report,
                    Err(error) => {
                        run.sources_failed += 1;
                        run.detail = format!("{}: {error:#}", source.id);
                        continue;
                    }
                };
                run.sources_observed += 1;
                if report.failure.is_some() {
                    run.sources_failed += 1;
                }
                run.new_items += report.new_item_ids.len();
                if matches!(report.page_change, Some(PageChange::MateriallyChanged)) {
                    run.page_changes += 1;
                }
                run.signals_created += report.signals_created.len();
                run.signals_corroborated += report.signals_corroborated.len();
                if let Some(target) = &source.target_id {
                    observed_targets.insert(target.clone());
                }
                registered.extend(report.registered_sources);
            }
            if registered.is_empty() {
                break;
            }
            due = Vec::new();
            for id in registered {
                if let Some(source) = self.get_source(&id).await? {
                    due.push(source);
                }
            }
        }

        let sources = self.list_sources().await?;
        for id in observed_targets {
            if let Some(mut target) = self.get_target(&id).await? {
                target.last_observed_at = Some(Utc::now());
                target.next_observation_at = sources
                    .iter()
                    .filter(|s| s.target_id.as_ref() == Some(&id) && s.relevant)
                    .filter_map(|s| s.next_observation_at)
                    .min();
                self.save_target(&mut target).await?;
            }
        }
        if run.detail.is_empty() {
            run.detail = format!(
                "Discovered {} target(s); observed {} source(s): {} new item(s), {} page change(s), {} signal(s) built, {} corroborated, {} failure(s)",
                run.targets_discovered, run.sources_observed, run.new_items, run.page_changes, run.signals_created, run.signals_corroborated, run.sources_failed
            );
        }
        Ok(())
    }

    /// The observation worker: run passes until `stop` is set, sleeping until
    /// the next durable due time (at most `max_idle`). Restart-safe because
    /// the due times live in FeltDB, not here.
    pub async fn run_worker(&self, stop: std::sync::Arc<std::sync::atomic::AtomicBool>, max_idle: std::time::Duration) {
        use std::sync::atomic::Ordering;
        while !stop.load(Ordering::SeqCst) {
            if let Err(error) = self.run_observation(RunOptions { trigger: "worker".into(), ..Default::default() }).await {
                eprintln!("stream: observation pass failed: {error:#}");
            }
            let wait = match self.observation_status().await.ok().and_then(|s| s.next_due_at) {
                Some(at) => (at - Utc::now()).to_std().unwrap_or_default().clamp(std::time::Duration::from_secs(2), max_idle),
                None => max_idle,
            };
            let deadline = std::time::Instant::now() + wait;
            while std::time::Instant::now() < deadline && !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }

    pub async fn observation_runs(&self, limit: usize) -> Result<Vec<ObservationRun>> {
        let mut runs = self.store.all("ObservationRun").await?.into_iter().map(run_from_value).collect::<Result<Vec<_>>>()?;
        runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        runs.truncate(limit);
        Ok(runs)
    }

    pub async fn observation_status(&self) -> Result<ObservationStatus> {
        let now = Utc::now();
        let targets = self.list_targets().await?;
        let watching = targets.iter().filter(|t| t.status != TargetStatus::Paused).map(|t| t.id.clone()).collect::<HashSet<_>>();
        let sources = self
            .list_sources()
            .await?
            .into_iter()
            .filter(|s| s.target_id.as_ref().map(|t| watching.contains(t)).unwrap_or(false) && s.relevant)
            .collect::<Vec<_>>();
        let next_source = sources.iter().filter_map(|s| s.next_observation_at).min();
        let next_discovery = targets.iter().filter(|t| t.status != TargetStatus::Paused).filter_map(|t| t.next_discovery_at).min();
        Ok(ObservationStatus {
            targets: targets.len(),
            watching_targets: watching.len(),
            due_sources: sources.iter().filter(|s| s.next_observation_at.map(|at| at <= now).unwrap_or(true)).count(),
            failing_sources: sources.iter().filter(|s| s.consecutive_failures > 0).count(),
            sources: sources.len(),
            next_due_at: match (next_source, next_discovery) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            },
            last_run: self.observation_runs(1).await?.into_iter().next(),
            worker: self.lease().await?,
        })
    }

    // -------------------------------------------------------------- views

    fn why(source: &Source, relations: &[DiscoveryRelation], titles: &HashMap<String, String>) -> String {
        let Some(relation) = relations.first() else {
            return "Added directly as a source.".into();
        };
        if relation.method == DiscoveryMethod::Seed {
            return "You asked Stream to watch this URL.".into();
        }
        let from = titles
            .get(relation.discovered_from.as_str())
            .cloned()
            .unwrap_or_else(|| {
                let host = relation.discovered_from.host_str().unwrap_or_default().trim_start_matches("www.").to_owned();
                let path = relation.discovered_from.path();
                if path == "/" { host } else { format!("{host}{path}") }
            });
        let _ = source;
        format!(
            "Discovered from {from} via {} — {} ({} confidence).",
            relation.method.label(),
            relation.reason,
            relation.confidence
        )
    }

    fn health(source: &Source, paused: bool) -> SourceHealth {
        if paused || source.status == SourceStatus::Paused {
            SourceHealth::Paused
        } else if source.consecutive_failures >= UNAVAILABLE_AFTER_FAILURES {
            SourceHealth::Unavailable
        } else if source.consecutive_failures > 0 {
            SourceHealth::Retrying
        } else if source.last_observed_at.is_none() {
            SourceHealth::Pending
        } else {
            SourceHealth::Healthy
        }
    }

    pub async fn target_sources(&self, id: &TargetId) -> Result<Vec<WatchedSource>> {
        let target = self.get_target(id).await?.ok_or_else(|| anyhow!("unknown target: {id}"))?;
        let relations = self.relations_of_target(id).await?;
        let sources = self.list_sources().await?;
        let titles = sources
            .iter()
            .filter_map(|s| s.title.clone().map(|t| (s.canonical_url.to_string(), t)))
            .collect::<HashMap<_, _>>();
        let paused = target.status == TargetStatus::Paused;
        let mut watched = Vec::new();
        for source in sources.into_iter().filter(|s| target.source_ids.contains(&s.id)) {
            let mine = relations.iter().filter(|r| r.source_id == source.id).cloned().collect::<Vec<_>>();
            let last_change = if source.adapter_kind == SourceKind::Web {
                self.snapshots_of(&source.id)
                    .await?
                    .into_iter()
                    .find(|s| !matches!(s.change, PageChange::Unchanged))
                    .map(|s| LastChange { change: s.change, summary: s.summary, observed_at: s.observed_at, added: s.added, removed: s.removed })
            } else {
                None
            };
            watched.push(WatchedSource {
                why: Self::why(&source, &mine, &titles),
                health: Self::health(&source, paused),
                relations: mine,
                last_change,
                source,
            });
        }
        let order = |s: &WatchedSource| match s.source.discovery_method {
            Some(DiscoveryMethod::Seed) => 0,
            Some(DiscoveryMethod::PageLink) => 2,
            _ => 1,
        };
        watched.sort_by(|a, b| order(a).cmp(&order(b)).then(a.source.discovered_at.cmp(&b.source.discovered_at)));
        Ok(watched)
    }

    pub async fn target_view(&self, id: &TargetId) -> Result<Option<TargetView>> {
        let Some(target) = self.get_target(id).await? else { return Ok(None) };
        let sources = self.target_sources(id).await?;
        let source_ids = sources.iter().map(|s| s.source.id.clone()).collect::<HashSet<_>>();
        let mut signal_ids = self
            .store
            .all("Evidence")
            .await?
            .into_iter()
            .filter(|e| e.get("source").and_then(|v| v.as_str()).map(|s| source_ids.contains(&SourceId::new(s))).unwrap_or(false))
            .filter_map(|e| e.get("signal").and_then(|v| v.as_str()).map(stream_model::SignalId::new))
            .collect::<Vec<_>>();
        signal_ids.sort();
        signal_ids.dedup();
        Ok(Some(TargetView { target, sources, signal_ids }))
    }

    /// Add a target and bring it up to date now. A resource is observed
    /// exactly as `stream add` always has; a descendants target is discovered
    /// and every surface found is observed. Used by `stream add` and the
    /// desktop add flow.
    pub async fn add_and_watch(&self, raw: &str, provenance: &str, progress: Option<ProgressFn<'_>>) -> Result<WatchOutcome> {
        let added = self.add_target(raw, provenance).await?;
        if added.target.scope == ObservationScope::Resource {
            let report = match &added.seed_source {
                Some(source) => Some(self.observe_source(&source.id, progress).await?),
                None => None,
            };
            let target = self.get_target(&added.target.id).await?.unwrap_or(added.target.clone());
            return Ok(WatchOutcome { target, added, discovery: None, run: None, report });
        }
        let discovery = self.discover_target(&added.target.id).await?;
        let run = self
            .run_observation(RunOptions { trigger: "add".into(), target_id: Some(added.target.id.clone()), force: true })
            .await?;
        let target = self.get_target(&added.target.id).await?.unwrap_or(discovery.target.clone());
        Ok(WatchOutcome { target, added, discovery: Some(discovery), run: Some(run), report: None })
    }
}

/// One surface of a target, for people.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceSummary {
    pub source_id: SourceId,
    pub kind: SurfaceKind,
    pub label: String,
    pub title: Option<String>,
    pub url: Url,
    pub health: SourceHealth,
}

/// A target as clients present it: the parsed URL and scope, side by side,
/// never as a string the client must re-interpret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetSummary {
    pub id: TargetId,
    /// The canonical URL (never contains the `/*` operator).
    pub url: Url,
    pub scope: ObservationScope,
    /// The user-facing form: `https://example.com/*` for descendants.
    pub display_url: String,
    pub identity: stream_model::TargetIdentity,
    pub title: Option<String>,
    pub status: TargetStatus,
    pub discovery_status: DiscoveryStatus,
    pub discovery_detail: Option<String>,
    /// "Watching 6 information surfaces", "Watching posts", "Discovering…".
    pub watching: String,
    pub surfaces: Vec<SurfaceSummary>,
    pub signal_count: usize,
    pub last_discovered_at: Option<DateTime<Utc>>,
    pub next_discovery_at: Option<DateTime<Utc>>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub next_observation_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// How Stream understands an input before it is added.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetPreview {
    pub id: TargetId,
    pub url: Url,
    pub scope: ObservationScope,
    pub display_url: String,
    pub identity: stream_model::TargetIdentity,
}

/// What Stream says it is doing for a target.
pub fn watching_label(target: &ObservationTarget, surfaces: usize) -> String {
    match target.status {
        TargetStatus::Paused => return "Paused".into(),
        TargetStatus::Unavailable => return "Observation unavailable".into(),
        TargetStatus::Failed => return "Could not reach it yet — Stream will try again".into(),
        TargetStatus::Pending | TargetStatus::Discovering if surfaces <= 1 && target.scope == ObservationScope::Descendants => {
            return "Discovering…".into()
        }
        _ => {}
    }
    match (target.scope, target.identity.kind) {
        (ObservationScope::Resource, _) => "Watching this resource".into(),
        (_, stream_model::TargetKind::Account) => target.identity.watching.clone(),
        _ if surfaces == 1 => "Watching 1 information surface".into(),
        _ => format!("Watching {surfaces} information surfaces"),
    }
}

impl StreamRuntime {
    /// A target as clients present it.
    pub async fn target_summary(&self, id: &TargetId) -> Result<Option<TargetSummary>> {
        let Some(view) = self.target_view(id).await? else { return Ok(None) };
        let surfaces = view
            .sources
            .iter()
            .map(|w| {
                let kind = w.source.surface_kind.unwrap_or(SurfaceKind::Page);
                SurfaceSummary {
                    source_id: w.source.id.clone(),
                    kind,
                    label: kind.label().to_owned(),
                    title: w.source.title.clone(),
                    url: w.source.canonical_url.clone(),
                    health: w.health,
                }
            })
            .collect::<Vec<_>>();
        let target = view.target;
        Ok(Some(TargetSummary {
            id: target.id.clone(),
            url: target.canonical_seed_url.clone(),
            scope: target.scope,
            display_url: target.display_url(),
            watching: watching_label(&target, surfaces.len()),
            identity: target.identity.clone(),
            title: target.title.clone(),
            status: target.status,
            discovery_status: target.discovery_status,
            discovery_detail: target.discovery_detail.clone(),
            signal_count: view.signal_ids.len(),
            surfaces,
            last_discovered_at: target.last_discovered_at,
            next_discovery_at: target.next_discovery_at,
            last_observed_at: target.last_observed_at,
            next_observation_at: target.next_observation_at,
            created_at: target.created_at,
        }))
    }

    pub async fn target_summaries(&self) -> Result<Vec<TargetSummary>> {
        let mut summaries = Vec::new();
        for target in self.list_targets().await? {
            if let Some(summary) = self.target_summary(&target.id).await? {
                summaries.push(summary);
            }
        }
        Ok(summaries)
    }

    /// Parse and resolve what the user typed without adding anything: how
    /// Stream understands it (URL, scope, provider identity).
    pub fn preview_target(&self, raw: &str) -> Result<TargetPreview> {
        let parsed = parse_observation_target(raw).map_err(|error| anyhow!(error))?;
        check_url(&parsed.seed_url, self.network_policy()).map_err(|error| anyhow!("{error}"))?;
        let plan = self.resolver.resolve(&parsed.seed_url, parsed.scope);
        Ok(TargetPreview {
            id: TargetId::for_url(parsed.canonical_url.as_str(), parsed.scope),
            display_url: parsed.display(),
            url: parsed.canonical_url,
            scope: parsed.scope,
            identity: plan.identity,
        })
    }

    /// Find a target by id or unique id prefix (`target_1a2b`, `1a2b`).
    pub async fn find_target(&self, id_or_prefix: &str) -> Result<Option<ObservationTarget>> {
        let wanted = id_or_prefix.trim();
        let wanted = if wanted.starts_with("target_") { wanted.to_owned() } else { format!("target_{wanted}") };
        let matches = self.list_targets().await?.into_iter().filter(|t| t.id.as_str().starts_with(&wanted)).collect::<Vec<_>>();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            n => bail!("{id_or_prefix} matches {n} targets; use more of the id"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchOutcome {
    /// The target as it stands after this call.
    pub target: ObservationTarget,
    pub added: AddTargetOutcome,
    pub discovery: Option<DiscoveryOutcome>,
    pub run: Option<ObservationRun>,
    /// For a resource: the observation of the resource itself.
    pub report: Option<ObservationReport>,
}

/// Validate that an id names a target (for surfaces that take ids as text).
pub fn target_id(value: &str) -> Result<TargetId> {
    if !value.starts_with("target_") {
        bail!("not a target id: {value}");
    }
    Ok(TargetId::new(value))
}
