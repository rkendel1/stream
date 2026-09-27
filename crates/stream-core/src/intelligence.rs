//! The Stream product loop: a URL becomes a durable source, its content
//! becomes canonical items, advisory interpretation turns items into
//! evidence-backed signals, and signals connect to what the user cares about.
//!
//! Authority never moves: sources, items, provenance, context, signals,
//! evidence, and connections are all FeltDB records. The interpreter only
//! proposes; [`stream_semantic::verify`] decides what may be persisted.

use super::records::*;
use super::{
    fetch_attempt_record, item_from_value, item_relation_record, provenance_from_value, rule_from_value,
    source_from_value, source_record, value_string, value_string_opt, StreamRuntime,
};
use anyhow::{anyhow, bail, Context as _, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use stream_ingest::detect_format;
use stream_model::{
    canonicalize_url, slug, ClaimKind, Connection, ConnectionId, ConnectionRelation,
    ConnectionTargetKind, ContextEntry, ContextId, ContextKind, Evidence, EvidenceId, EvidenceLocator, FailureCategory,
    FetchAttempt, FetchStatus, Item, ItemId, ItemRelation, ItemRelationId, NormalizedItem, ProcessingStage,
    Provenance, ProvenanceId, RelationKind, RuleAction, SemanticDecisionId, Signal, SignalId, SignalStatus, Source,
    SourceId, SourceKind, SourceStatus, Subject, Topic,
};
use stream_rules::ranking::{explain_rank, RankingExplanation, RankingInput};
use stream_semantic::{verify, Excerpt, InterpretationInput, LinkKind, PriorItem, Rejection, Verified};
use url::Url;

/// Newest feed entries considered per observation. Stream observes change;
/// it does not import archives.
pub const MAX_FEED_ITEMS_PER_OBSERVATION: usize = 25;
/// Earlier signal items offered to the interpreter as connection candidates.
pub const MAX_PRIOR_ITEMS: usize = 200;

pub type ProgressFn<'a> = &'a (dyn Fn(ProcessingStage) + Send + Sync);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddUrlOutcome {
    pub source: Source,
    /// True when the URL canonicalized to a source Stream already had.
    pub existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectedInterpretation {
    pub item_id: ItemId,
    pub rejections: Vec<Rejection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationReport {
    pub source: Source,
    pub stage: ProcessingStage,
    pub failure: Option<String>,
    pub items_observed: usize,
    pub new_item_ids: Vec<ItemId>,
    pub primary_item_id: Option<ItemId>,
    /// The signal the user's own URL contributes to, if any.
    pub primary_signal_id: Option<SignalId>,
    pub signals_created: Vec<SignalId>,
    pub signals_corroborated: Vec<SignalId>,
    pub understood_without_signal: Vec<ItemId>,
    pub rejected: Vec<RejectedInterpretation>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NewContext {
    pub name: String,
    #[serde(default)]
    pub kind: Option<ContextKind>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Related context IDs or names.
    #[serde(default)]
    pub related: Vec<String>,
}

/// A signal as presentation layers see it. The interpreter that produced it
/// is deliberately absent: the UI must not depend on the semantic provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalView {
    pub id: SignalId,
    pub item_id: ItemId,
    pub topic: Topic,
    pub subject: Subject,
    pub change: stream_model::Change,
    pub why_it_matters: Option<String>,
    pub status: SignalStatus,
    pub advisory: bool,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<&Signal> for SignalView {
    fn from(signal: &Signal) -> Self {
        Self {
            id: signal.id.clone(),
            item_id: signal.item_id.clone(),
            topic: signal.topic.clone(),
            subject: signal.subject.clone(),
            change: signal.change.clone(),
            why_it_matters: signal.why_it_matters.clone(),
            status: signal.status,
            advisory: true,
            confidence: signal.confidence,
            created_at: signal.created_at,
            updated_at: signal.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedLabel {
    pub kind: ConnectionTargetKind,
    pub id: String,
    pub label: String,
    pub relation: ConnectionRelation,
    pub strength: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    pub id: SourceId,
    pub kind: SourceKind,
    pub adapter_kind: SourceKind,
    pub title: Option<String>,
    pub canonical_url: Url,
    pub original_url: Url,
}

impl From<&Source> for SourceRef {
    fn from(source: &Source) -> Self {
        Self {
            id: source.id.clone(),
            kind: source.kind,
            adapter_kind: source.adapter_kind,
            title: source.title.clone(),
            canonical_url: source.canonical_url.clone(),
            original_url: source.original_url.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemRef {
    pub id: ItemId,
    pub title: String,
    pub url: Option<Url>,
    pub published_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub source: Option<SourceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalSummary {
    pub signal: SignalView,
    /// 1-based position in Today; 0 when not shown in Today.
    pub position: usize,
    pub connected_to: Vec<ConnectedLabel>,
    pub source_count: usize,
    pub observation_count: usize,
    pub evidence_count: usize,
    pub primary: Option<ItemRef>,
    pub last_observed_at: DateTime<Utc>,
    pub ranking: RankingExplanation,
}

/// Signal → Evidence → Item → Source → URL, fully resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceTrace {
    pub evidence: Evidence,
    pub item: Option<ItemRef>,
    pub source: Option<SourceRef>,
    pub provenance: Option<Provenance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalDetail {
    pub summary: SignalSummary,
    pub evidence: Vec<EvidenceTrace>,
    pub connections: Vec<Connection>,
    pub observations: Vec<ItemRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextView {
    pub context: ContextEntry,
    pub related: Vec<ConnectedLabel>,
    pub signal_ids: Vec<SignalId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionView {
    pub connection: Connection,
    pub signal_subject: Option<String>,
    pub item_title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRef {
    pub id: SignalId,
    pub subject: String,
    pub change: String,
    pub strength: f32,
    pub relation: ConnectionRelation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextNode {
    pub context: ContextEntry,
    pub related: Vec<ConnectedLabel>,
    pub signals: Vec<SignalRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectNode {
    pub key: String,
    pub label: String,
    pub signals: Vec<SignalRef>,
    pub observation_count: usize,
}

/// Stream's information graph as the user sees it: what they care about and
/// what is changing around each of those things.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionGraph {
    pub contexts: Vec<ContextNode>,
    pub subjects: Vec<SubjectNode>,
    pub item_relations: Vec<ItemRelation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDetail {
    pub source: Source,
    pub attempts: Vec<FetchAttempt>,
    pub items: Vec<ItemRef>,
    pub signal_ids: Vec<SignalId>,
}

/// Everything needed to present signals, loaded once from FeltDB.
struct Snapshot {
    signals: Vec<Signal>,
    evidence: Vec<Evidence>,
    connections: Vec<Connection>,
    contexts: Vec<ContextEntry>,
    items: HashMap<ItemId, Item>,
    sources: HashMap<SourceId, Source>,
    provenance: HashMap<ProvenanceId, Provenance>,
    rule_hits: HashMap<ItemId, Vec<String>>,
}

impl Snapshot {
    fn source_ref(&self, id: &SourceId) -> Option<SourceRef> {
        self.sources.get(id).map(SourceRef::from)
    }

    fn item_ref(&self, id: &ItemId) -> Option<ItemRef> {
        let item = self.items.get(id)?;
        Some(ItemRef {
            id: item.id.clone(),
            title: item.title.clone(),
            url: item.canonical_url.clone(),
            published_at: item.published_at,
            observed_at: item.created_at,
            source: self.source_ref(&item.source_id),
        })
    }

    fn evidence_for<'a>(&'a self, signal: &'a SignalId) -> impl Iterator<Item = &'a Evidence> + 'a {
        self.evidence.iter().filter(move |evidence| &evidence.signal_id == signal)
    }

    fn connections_for<'a>(&'a self, signal: &'a SignalId) -> impl Iterator<Item = &'a Connection> + 'a {
        self.connections
            .iter()
            .filter(move |connection| connection.signal_id.as_ref() == Some(signal))
    }

    fn connected_to(&self, signal: &SignalId) -> Vec<ConnectedLabel> {
        let mut best: BTreeMap<String, ConnectedLabel> = BTreeMap::new();
        for connection in self.connections_for(signal) {
            if !matches!(connection.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project) {
                continue;
            }
            let label = self
                .contexts
                .iter()
                .find(|context| context.id.as_str() == connection.target_id)
                .map(|context| context.name.clone())
                .unwrap_or_else(|| connection.label.clone());
            let candidate = ConnectedLabel {
                kind: connection.target_kind,
                id: connection.target_id.clone(),
                label,
                relation: connection.relation,
                strength: connection.strength,
            };
            let replace = match best.get(&connection.target_id) {
                None => true,
                Some(existing) => {
                    (existing.relation == ConnectionRelation::Via && candidate.relation != ConnectionRelation::Via)
                        || (existing.relation == candidate.relation && candidate.strength > existing.strength)
                }
            };
            if replace {
                best.insert(connection.target_id.clone(), candidate);
            }
        }
        let mut labels = best.into_values().collect::<Vec<_>>();
        labels.sort_by(|a, b| {
            (a.relation == ConnectionRelation::Via)
                .cmp(&(b.relation == ConnectionRelation::Via))
                .then(b.strength.partial_cmp(&a.strength).unwrap_or(std::cmp::Ordering::Equal))
        });
        labels
    }

    fn summarize(&self, signal: &Signal, now: DateTime<Utc>) -> SignalSummary {
        let evidence = self.evidence_for(&signal.id).collect::<Vec<_>>();
        let sources = evidence.iter().map(|e| e.source_id.clone()).collect::<HashSet<_>>();
        let items = evidence.iter().map(|e| e.item_id.clone()).collect::<HashSet<_>>();
        let last_observed_at = evidence.iter().map(|e| e.observed_at).max().unwrap_or(signal.created_at);

        let mut contexts: BTreeMap<String, (String, f32)> = BTreeMap::new();
        let mut connection_strength = 0.0f32;
        for connection in self.connections_for(&signal.id) {
            match connection.target_kind {
                ConnectionTargetKind::Context | ConnectionTargetKind::Project
                    if connection.relation == ConnectionRelation::Matches =>
                {
                    let name = self
                        .contexts
                        .iter()
                        .find(|context| context.id.as_str() == connection.target_id)
                        .map(|context| context.name.clone())
                        .unwrap_or_else(|| connection.label.clone());
                    let entry = contexts.entry(connection.target_id.clone()).or_insert((name, 0.0));
                    entry.1 = entry.1.max(connection.strength);
                }
                ConnectionTargetKind::Item => connection_strength = connection_strength.max(connection.strength),
                _ => {}
            }
        }
        let subject_key = slug(&signal.subject.label);
        let prior_signals_on_subject = self
            .signals
            .iter()
            .filter(|other| {
                other.id != signal.id && other.created_at < signal.created_at && slug(&other.subject.label) == subject_key
            })
            .count();
        let mut matched_rules = items
            .iter()
            .filter_map(|item| self.rule_hits.get(item))
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        matched_rules.sort();
        matched_rules.dedup();

        let ranking = explain_rank(&RankingInput {
            contexts: contexts.into_values().collect(),
            sources: sources.len(),
            connection_strength,
            change_kind: signal.change.kind,
            prior_signals_on_subject,
            matched_rules,
            last_observed_at,
            now,
            status: signal.status,
        });

        SignalSummary {
            signal: SignalView::from(signal),
            position: 0,
            connected_to: self.connected_to(&signal.id),
            source_count: sources.len(),
            observation_count: items.len(),
            evidence_count: evidence.len(),
            primary: self.item_ref(&signal.item_id),
            last_observed_at,
            ranking,
        }
    }

    fn ranked(&self, now: DateTime<Utc>) -> Vec<SignalSummary> {
        let mut summaries = self.signals.iter().map(|signal| self.summarize(signal, now)).collect::<Vec<_>>();
        summaries.sort_by(|a, b| {
            b.ranking
                .eligible_for_today
                .cmp(&a.ranking.eligible_for_today)
                .then(b.ranking.total.partial_cmp(&a.ranking.total).unwrap_or(std::cmp::Ordering::Equal))
                .then(b.last_observed_at.cmp(&a.last_observed_at))
                .then(a.signal.id.cmp(&b.signal.id))
        });
        let mut position = 0;
        for summary in summaries.iter_mut() {
            if summary.ranking.eligible_for_today {
                position += 1;
                summary.position = position;
            }
        }
        summaries
    }

    fn trace(&self, evidence: &Evidence) -> EvidenceTrace {
        EvidenceTrace {
            evidence: evidence.clone(),
            item: self.item_ref(&evidence.item_id),
            source: self.source_ref(&evidence.source_id),
            provenance: evidence.provenance_id.as_ref().and_then(|id| self.provenance.get(id)).cloned(),
        }
    }
}

fn excerpt_evidence(
    signal_id: &SignalId,
    item: &Item,
    provenance: &Option<Provenance>,
    claim: ClaimKind,
    excerpt: &Excerpt,
) -> Evidence {
    let now = Utc::now();
    Evidence {
        id: EvidenceId::generate(),
        signal_id: signal_id.clone(),
        item_id: item.id.clone(),
        source_id: provenance.as_ref().map(|p| p.source_id.clone()).unwrap_or_else(|| item.source_id.clone()),
        provenance_id: provenance.as_ref().map(|p| p.id.clone()),
        url: provenance
            .as_ref()
            .map(|p| p.source_url.clone())
            .or_else(|| item.canonical_url.clone())
            .unwrap_or_else(|| Url::parse("https://stream.invalid/unknown").expect("static url")),
        claim,
        locator: excerpt.locator,
        excerpt: excerpt.text.clone(),
        observed_at: provenance.as_ref().map(|p| p.observed_at).unwrap_or(item.created_at),
        created_at: now,
    }
}

fn context_target(context: &ContextEntry) -> ConnectionTargetKind {
    if context.kind == ContextKind::Project {
        ConnectionTargetKind::Project
    } else {
        ConnectionTargetKind::Context
    }
}

/// Contexts one hop away from `context`, in either direction.
fn neighbours<'a>(context: &'a ContextEntry, contexts: &'a [ContextEntry]) -> Vec<&'a ContextEntry> {
    contexts
        .iter()
        .filter(|other| other.id != context.id && (context.related.contains(&other.id) || other.related.contains(&context.id)))
        .collect()
}

impl StreamRuntime {
    // ----------------------------------------------------------------- sources

    /// Establish a durable source for a user-supplied URL. The source exists
    /// before anything is fetched, so the URL is never lost, even if
    /// observation later fails.
    pub async fn add_url(&self, raw: &str, provenance: &str) -> Result<AddUrlOutcome> {
        let canonical = canonicalize_url(raw).map_err(|error| anyhow!(error))?;
        let trimmed = raw.trim();
        let original = if trimmed.contains("://") {
            Url::parse(trimmed)?
        } else {
            Url::parse(&format!("https://{trimmed}"))?
        };
        let source = Source::from_user_url(original, canonical, provenance);
        if let Some(existing) = self.get_source(&source.id).await? {
            return Ok(AddUrlOutcome { source: existing, existing: true });
        }
        if let Some(existing) = self.find_source_by_identity(&source.identity).await? {
            return Ok(AddUrlOutcome { source: existing, existing: true });
        }
        match self.store.insert("Source", source.id.as_str(), source_record(&source), true).await {
            Ok(_) => Ok(AddUrlOutcome { source, existing: false }),
            // A concurrent add of the same canonical URL won the race.
            Err(error) => match self.get_source(&source.id).await? {
                Some(existing) => Ok(AddUrlOutcome { source: existing, existing: true }),
                None => Err(error),
            },
        }
    }

    /// Add a URL and observe it to completion.
    pub async fn add_and_observe_url(
        &self,
        raw: &str,
        provenance: &str,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<ObservationReport> {
        let added = self.add_url(raw, provenance).await?;
        self.observe_source(&added.source.id, progress).await
    }

    async fn find_source_by_identity(&self, identity: &str) -> Result<Option<Source>> {
        self.store
            .find("Source", json!({ "identity": identity }))
            .await?
            .into_iter()
            .next()
            .map(source_from_value)
            .transpose()
    }

    async fn set_stage(
        &self,
        source: &mut Source,
        stage: ProcessingStage,
        detail: Option<String>,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<()> {
        source.stage = stage;
        source.stage_detail = detail;
        source.updated_at = Utc::now();
        self.store.update("Source", source.id.as_str(), source_record(source)).await?;
        if let Some(progress) = progress {
            progress(stage);
        }
        Ok(())
    }

    /// Record a failed observation durably: the source keeps its URL and
    /// history, the attempt records why, and the source shows as failed.
    async fn fail_observation(
        &self,
        source: &mut Source,
        attempt: &mut FetchAttempt,
        category: FailureCategory,
        message: String,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<()> {
        let now = Utc::now();
        attempt.status = FetchStatus::Failed;
        attempt.completed_at = Some(now);
        attempt.failure_category = Some(category);
        attempt.diagnostics = json!({ "message": message, "category": category.to_string() });
        self.store
            .update("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(attempt))
            .await?;
        source.status = SourceStatus::Failed;
        source.last_failure_at = Some(now);
        source.last_error_category = Some(category);
        source.last_error_message = Some(message.clone());
        source.consecutive_failures += 1;
        self.set_stage(source, ProcessingStage::Failed, Some(message), progress).await
    }

    /// Observe a source: fetch, normalize, understand, connect, and build
    /// signals. Expected failures (network, parsing) end in a durable
    /// `failed` stage and are reported, not raised.
    pub async fn observe_source(&self, source_id: &SourceId, progress: Option<ProgressFn<'_>>) -> Result<ObservationReport> {
        let mut source = self
            .get_source(source_id)
            .await?
            .ok_or_else(|| anyhow!("unknown source: {}", source_id))?;
        let mut report = ObservationReport {
            source: source.clone(),
            stage: source.stage,
            failure: None,
            items_observed: 0,
            new_item_ids: vec![],
            primary_item_id: None,
            primary_signal_id: None,
            signals_created: vec![],
            signals_corroborated: vec![],
            understood_without_signal: vec![],
            rejected: vec![],
        };
        if let Err(error) = self.observe_inner(&mut source, &mut report, progress).await {
            let message = format!("{error:#}");
            source.status = SourceStatus::Failed;
            source.last_failure_at = Some(Utc::now());
            source.last_error_message = Some(message.clone());
            let _ = self.set_stage(&mut source, ProcessingStage::Failed, Some(message), progress).await;
            return Err(error);
        }
        report.stage = source.stage;
        report.source = source;
        Ok(report)
    }

    async fn observe_inner(
        &self,
        source: &mut Source,
        report: &mut ObservationReport,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<()> {
        source.last_checked_at = Some(Utc::now());
        self.set_stage(source, ProcessingStage::Fetching, None, progress).await?;
        let mut attempt = FetchAttempt::started(source.id.clone());
        self.store
            .insert("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(&attempt), true)
            .await?;

        let first_observation = source.last_observed_at.is_none();
        let mut normalized: Vec<NormalizedItem> = Vec::new();
        let mut has_primary = false;
        let mut feed_parsed = false;
        let mut diagnostics = serde_json::Map::new();

        if first_observation || !source.adapter_kind.is_feed_format() {
            let target = if first_observation { source.original_url.clone() } else { source.endpoint.clone() };
            let document = match self.fetcher.fetch_url(&target).await {
                Ok(document) => document,
                Err(error) => {
                    let message = format!("could not fetch {target}: {error:#}");
                    self.fail_observation(source, &mut attempt, FailureCategory::Network, message.clone(), progress)
                        .await?;
                    report.failure = Some(message);
                    return Ok(());
                }
            };
            let format = detect_format(document.content_type.as_deref(), &document.body);
            diagnostics.insert("final_url".into(), json!(document.final_url.as_str()));
            diagnostics.insert("format".into(), json!(format!("{format:?}").to_lowercase()));

            if format.is_feed() {
                source.adapter_kind = format.adapter_kind();
                source.endpoint = document.final_url.clone();
                if source.kind == SourceKind::Web {
                    source.kind = source.adapter_kind;
                }
                match self.parse_feed(source, &document.body) {
                    Ok(items) => normalized.extend(items),
                    Err(error) => {
                        let message = format!("could not parse {} feed: {error:#}", source.adapter_kind);
                        self.fail_observation(source, &mut attempt, FailureCategory::Parsing, message.clone(), progress)
                            .await?;
                        report.failure = Some(message);
                        return Ok(());
                    }
                }
                feed_parsed = true;
            } else if matches!(format, stream_ingest::DocumentFormat::Html | stream_ingest::DocumentFormat::Unknown)
                && std::str::from_utf8(&document.body[..document.body.len().min(2048)]).is_ok()
            {
                let html = String::from_utf8_lossy(&document.body);
                let page = match stream_web::parse_page(&document.final_url, &html) {
                    Ok(page) => page,
                    Err(error) => {
                        let message = format!("could not read page: {error:#}");
                        self.fail_observation(source, &mut attempt, FailureCategory::Parsing, message.clone(), progress)
                            .await?;
                        report.failure = Some(message);
                        return Ok(());
                    }
                };
                source.title = Some(page.title.clone());
                if !source.adapter_kind.is_feed_format() {
                    source.endpoint = document.final_url.clone();
                }
                if first_observation {
                    if let Some(feed) = page.feeds.first() {
                        source.endpoint = feed.url.clone();
                        source.adapter_kind = feed.format.adapter_kind();
                        diagnostics.insert("discovered_feed".into(), json!(feed.url.as_str()));
                    }
                }
                normalized.push(page.to_normalized_item(source.kind));
                has_primary = true;
            } else {
                let message = format!(
                    "unsupported document ({}); Stream understands web pages and RSS, Atom, and JSON feeds",
                    document.content_type.unwrap_or_else(|| "unknown type".into())
                );
                self.fail_observation(source, &mut attempt, FailureCategory::Parsing, message.clone(), progress)
                    .await?;
                report.failure = Some(message);
                return Ok(());
            }
        }

        if source.adapter_kind.is_feed_format() && !feed_parsed {
            let parsed = match self.fetcher.fetch_url(&source.endpoint).await {
                Ok(document) => self.parse_feed(source, &document.body),
                Err(error) => Err(error),
            };
            match parsed {
                Ok(items) => normalized.extend(items),
                Err(error) if has_primary => {
                    // The page itself was observed; the feed stays a
                    // best-effort way to keep observing the source.
                    diagnostics.insert("feed_error".into(), json!(format!("{error:#}")));
                }
                Err(error) => {
                    let message = format!("could not observe feed {}: {error:#}", source.endpoint);
                    self.fail_observation(source, &mut attempt, FailureCategory::Network, message.clone(), progress)
                        .await?;
                    report.failure = Some(message);
                    return Ok(());
                }
            }
        }

        attempt.diagnostics = Value::Object(diagnostics);
        let (persisted_source, _attempt, outcomes) =
            self.persist_observation(source.clone(), attempt, normalized).await?;
        *source = persisted_source;
        report.items_observed = outcomes.len();
        report.new_item_ids = outcomes.iter().filter(|o| o.is_new).map(|o| o.item.id.clone()).collect();

        let mut candidates: Vec<(Item, bool)> = Vec::new();
        for (index, outcome) in outcomes.into_iter().enumerate() {
            let primary = has_primary && index == 0;
            if primary {
                report.primary_item_id = Some(outcome.item.id.clone());
            }
            if (primary || outcome.is_new) && !candidates.iter().any(|(item, _)| item.id == outcome.item.id) {
                candidates.push((outcome.item, primary));
            }
        }

        self.understand(source, candidates, report, progress).await?;

        source.last_observed_at = Some(Utc::now());
        source.status = SourceStatus::Active;
        let detail = match (report.signals_created.len(), report.signals_corroborated.len()) {
            (0, 0) if report.items_observed == 0 => "Nothing new observed".to_owned(),
            (0, 0) => format!("{} observed; nothing connected to your context yet", plural(report.items_observed, "item")),
            (created, 0) => format!("{} built", plural(created, "signal")),
            (0, corroborated) => format!("Corroborated {}", plural(corroborated, "existing signal")),
            (created, corroborated) => format!("{} built, {} corroborated", plural(created, "signal"), corroborated),
        };
        self.set_stage(source, ProcessingStage::Observed, Some(detail), progress).await
    }

    fn parse_feed(&self, source: &Source, body: &[u8]) -> Result<Vec<NormalizedItem>> {
        let adapter = self.adapters.adapter_for(source)?;
        let mut items = adapter.parse(source, body, Utc::now())?;
        items.sort_by(|a, b| b.published_at.cmp(&a.published_at));
        items.truncate(MAX_FEED_ITEMS_PER_OBSERVATION);
        Ok(items)
    }

    // ----------------------------------------------------------- understanding

    async fn prior_items(&self) -> Result<Vec<PriorItem>> {
        let signals = self.all_signals().await?;
        let evidence = self.all_evidence().await?;
        let items = self.item_map().await?;
        let signal_subject = signals
            .iter()
            .map(|signal| (signal.id.clone(), signal.subject.label.clone()))
            .collect::<HashMap<_, _>>();
        let mut seen = HashSet::new();
        let mut prior = Vec::new();
        let mut ordered = evidence.iter().collect::<Vec<_>>();
        ordered.sort_by(|a, b| b.observed_at.cmp(&a.observed_at));
        for evidence in ordered {
            if !seen.insert(evidence.item_id.clone()) {
                continue;
            }
            let Some(item) = items.get(&evidence.item_id) else { continue };
            prior.push(PriorItem {
                item_id: item.id.clone(),
                signal_id: Some(evidence.signal_id.clone()),
                title: item.title.clone(),
                content_text: item.content_text.clone(),
                subject: signal_subject.get(&evidence.signal_id).cloned(),
            });
            if prior.len() >= MAX_PRIOR_ITEMS {
                break;
            }
        }
        Ok(prior)
    }

    async fn understand(
        &self,
        source: &mut Source,
        candidates: Vec<(Item, bool)>,
        report: &mut ObservationReport,
        progress: Option<ProgressFn<'_>>,
    ) -> Result<()> {
        self.set_stage(source, ProcessingStage::Understanding, None, progress).await?;
        let contexts = self.list_contexts().await?;
        let mut prior = self.prior_items().await?;
        let mut signal_of_item = self
            .all_evidence()
            .await?
            .into_iter()
            .map(|evidence| (evidence.item_id, evidence.signal_id))
            .collect::<HashMap<_, _>>();
        let mut stage = ProcessingStage::Understanding;

        for (item, primary) in candidates.into_iter().take(MAX_FEED_ITEMS_PER_OBSERVATION + 1) {
            if let Some(signal_id) = signal_of_item.get(&item.id) {
                if primary {
                    report.primary_signal_id = Some(signal_id.clone());
                }
                continue;
            }
            let proposal = self
                .interpreter
                .interpret(&InterpretationInput { item: &item, contexts: &contexts, prior: &prior })
                .await;
            let verified = match proposal {
                Err(error) => {
                    let rejections = vec![Rejection { claim: "interpretation".into(), reason: format!("{error:#}") }];
                    self.record_rejection(&item, &rejections).await?;
                    report.rejected.push(RejectedInterpretation { item_id: item.id.clone(), rejections });
                    continue;
                }
                Ok(proposal) => match verify(proposal, &item, &contexts, &prior) {
                    Ok(verified) => verified,
                    Err(rejections) => {
                        self.record_rejection(&item, &rejections).await?;
                        report.rejected.push(RejectedInterpretation { item_id: item.id.clone(), rejections });
                        continue;
                    }
                },
            };

            if stage == ProcessingStage::Understanding {
                stage = ProcessingStage::Connecting;
                self.set_stage(source, stage, None, progress).await?;
            }
            let interpretation = &verified.interpretation;
            let consolidation = interpretation
                .item_links
                .iter()
                .filter(|link| link.kind == LinkKind::SameChange)
                .find_map(|link| {
                    prior
                        .iter()
                        .find(|candidate| candidate.item_id == link.item_id)
                        .and_then(|candidate| candidate.signal_id.clone())
                        .map(|signal_id| (signal_id, link.clone()))
                });
            let meaningful = primary || !interpretation.context_matches.is_empty() || consolidation.is_some();
            self.record_decisions(&item, &verified, meaningful).await?;
            if !meaningful {
                report.understood_without_signal.push(item.id.clone());
                continue;
            }

            if stage != ProcessingStage::BuildingSignal {
                stage = ProcessingStage::BuildingSignal;
                self.set_stage(source, stage, None, progress).await?;
            }
            let signal_id = match consolidation {
                Some((signal_id, link)) => {
                    self.corroborate_signal(&signal_id, &item, &verified, &link, &contexts).await?;
                    report.signals_corroborated.push(signal_id.clone());
                    signal_id
                }
                None => {
                    let signal_id = self.create_signal(&item, &verified, &contexts).await?;
                    report.signals_created.push(signal_id.clone());
                    signal_id
                }
            };
            if primary {
                report.primary_signal_id = Some(signal_id.clone());
            }
            signal_of_item.insert(item.id.clone(), signal_id.clone());
            prior.insert(
                0,
                PriorItem {
                    item_id: item.id.clone(),
                    signal_id: Some(signal_id),
                    title: item.title.clone(),
                    content_text: item.content_text.clone(),
                    subject: Some(interpretation.subject.value.label.clone()),
                },
            );
        }
        Ok(())
    }

    async fn latest_provenance(&self, item_id: &ItemId) -> Result<Option<Provenance>> {
        let mut records = self
            .store
            .find("Provenance", json!({ "item": item_id.as_str() }))
            .await?
            .into_iter()
            .map(provenance_from_value)
            .collect::<Result<Vec<_>>>()?;
        records.sort_by(|a, b| b.observed_at.cmp(&a.observed_at));
        Ok(records.into_iter().next())
    }

    async fn insert_evidence(&self, evidence: &Evidence) -> Result<()> {
        self.store
            .insert("Evidence", evidence.id.as_str(), evidence_record(evidence), true)
            .await?;
        Ok(())
    }

    async fn insert_connection(&self, connection: &Connection) -> Result<()> {
        self.store
            .insert("Connection", connection.id.as_str(), connection_record(connection), true)
            .await?;
        Ok(())
    }

    async fn insert_claim_evidence(
        &self,
        signal_id: &SignalId,
        item: &Item,
        provenance: &Option<Provenance>,
        claim: ClaimKind,
        excerpts: &[Excerpt],
    ) -> Result<Vec<EvidenceId>> {
        let mut ids = Vec::new();
        for excerpt in excerpts {
            let evidence = excerpt_evidence(signal_id, item, provenance, claim, excerpt);
            self.insert_evidence(&evidence).await?;
            ids.push(evidence.id);
        }
        Ok(ids)
    }

    #[allow(clippy::too_many_arguments)]
    fn connection(
        signal_id: &SignalId,
        item: &Item,
        target_kind: ConnectionTargetKind,
        target_id: String,
        label: String,
        relation: ConnectionRelation,
        strength: f32,
        rationale: String,
        evidence_ids: Vec<EvidenceId>,
    ) -> Connection {
        Connection {
            id: ConnectionId::generate(),
            signal_id: Some(signal_id.clone()),
            item_id: item.id.clone(),
            target_kind,
            target_id,
            label,
            relation,
            strength,
            rationale,
            evidence_ids,
            created_at: Utc::now(),
        }
    }

    /// Persist context connections (direct matches plus one hop through the
    /// user's own context relationships) for a signal.
    async fn connect_contexts(
        &self,
        signal_id: &SignalId,
        item: &Item,
        provenance: &Option<Provenance>,
        verified: &Verified,
        contexts: &[ContextEntry],
        already: &HashSet<String>,
    ) -> Result<()> {
        let mut connected = already.clone();
        let matches = &verified.interpretation.context_matches;
        for candidate in matches {
            let Some(context) = contexts.iter().find(|context| context.id == candidate.context_id) else {
                continue;
            };
            if connected.contains(context.id.as_str()) {
                continue;
            }
            let evidence_ids = self
                .insert_claim_evidence(signal_id, item, provenance, ClaimKind::Connection, &candidate.excerpts)
                .await?;
            self.insert_connection(&Self::connection(
                signal_id,
                item,
                context_target(context),
                context.id.to_string(),
                context.name.clone(),
                ConnectionRelation::Matches,
                candidate.strength,
                candidate.rationale.clone(),
                evidence_ids.clone(),
            ))
            .await?;
            connected.insert(context.id.to_string());

            for neighbour in neighbours(context, contexts) {
                if connected.contains(neighbour.id.as_str()) || matches.iter().any(|m| m.context_id == neighbour.id) {
                    continue;
                }
                self.insert_connection(&Self::connection(
                    signal_id,
                    item,
                    context_target(neighbour),
                    neighbour.id.to_string(),
                    neighbour.name.clone(),
                    ConnectionRelation::Via,
                    candidate.strength * 0.5,
                    format!("{} is related to {}", neighbour.name, context.name),
                    evidence_ids.clone(),
                ))
                .await?;
                connected.insert(neighbour.id.to_string());
            }
        }
        Ok(())
    }

    async fn create_signal(&self, item: &Item, verified: &Verified, contexts: &[ContextEntry]) -> Result<SignalId> {
        let interpretation = &verified.interpretation;
        let now = Utc::now();
        let signal = Signal {
            id: SignalId::generate(),
            item_id: item.id.clone(),
            topic: interpretation.topic.value.clone(),
            subject: interpretation.subject.value.clone(),
            change: interpretation.change.value.clone(),
            why_it_matters: interpretation.why_it_matters.as_ref().map(|claim| claim.value.clone()),
            status: SignalStatus::Open,
            interpreter: self.interpreter.id().to_owned(),
            confidence: (interpretation.topic.confidence + interpretation.subject.confidence + interpretation.change.confidence)
                / 3.0,
            created_at: now,
            updated_at: now,
        };
        self.store.insert("Signal", signal.id.as_str(), signal_record(&signal), true).await?;
        let provenance = self.latest_provenance(&item.id).await?;

        let topic_evidence = self
            .insert_claim_evidence(&signal.id, item, &provenance, ClaimKind::Topic, &interpretation.topic.excerpts)
            .await?;
        let subject_evidence = self
            .insert_claim_evidence(&signal.id, item, &provenance, ClaimKind::Subject, &interpretation.subject.excerpts)
            .await?;
        self.insert_claim_evidence(&signal.id, item, &provenance, ClaimKind::Change, &interpretation.change.excerpts)
            .await?;
        if let Some(why) = &interpretation.why_it_matters {
            self.insert_claim_evidence(&signal.id, item, &provenance, ClaimKind::WhyItMatters, &why.excerpts)
                .await?;
        }

        self.insert_connection(&Self::connection(
            &signal.id,
            item,
            ConnectionTargetKind::Topic,
            slug(&signal.topic.label),
            signal.topic.label.clone(),
            ConnectionRelation::About,
            interpretation.topic.confidence,
            interpretation.topic.rationale.clone(),
            topic_evidence,
        ))
        .await?;
        self.insert_connection(&Self::connection(
            &signal.id,
            item,
            ConnectionTargetKind::Subject,
            slug(&signal.subject.label),
            signal.subject.label.clone(),
            ConnectionRelation::About,
            interpretation.subject.confidence,
            interpretation.subject.rationale.clone(),
            subject_evidence,
        ))
        .await?;
        self.connect_contexts(&signal.id, item, &provenance, verified, contexts, &HashSet::new())
            .await?;

        for link in interpretation.item_links.iter() {
            self.relate_items(&signal.id, item, &provenance, link, RelationKind::Related).await?;
        }
        Ok(signal.id)
    }

    /// Record an Item → Item relationship through the existing ItemRelation
    /// infrastructure, and mirror it into the signal's connection graph.
    async fn relate_items(
        &self,
        signal_id: &SignalId,
        item: &Item,
        provenance: &Option<Provenance>,
        link: &stream_semantic::ItemLink,
        relation: RelationKind,
    ) -> Result<()> {
        let relation_record = ItemRelation {
            id: ItemRelationId::generate(),
            from_item_id: item.id.clone(),
            to_item_id: link.item_id.clone(),
            relation,
            evidence: link.rationale.clone(),
            created_at: Utc::now(),
        };
        self.store
            .insert("ItemRelation", relation_record.id.as_str(), item_relation_record(&relation_record), true)
            .await?;
        let claim = if relation == RelationKind::SameStory { ClaimKind::Corroboration } else { ClaimKind::Connection };
        let excerpts = if link.excerpts.is_empty() {
            vec![Excerpt { locator: EvidenceLocator::Title, text: item.title.clone() }]
        } else {
            link.excerpts.clone()
        };
        let evidence_ids = self.insert_claim_evidence(signal_id, item, provenance, claim, &excerpts).await?;
        let target_title = match self.store.get("Item", link.item_id.as_str()).await? {
            Some(value) => item_from_value(value)?.title,
            None => link.item_id.to_string(),
        };
        self.insert_connection(&Self::connection(
            signal_id,
            item,
            ConnectionTargetKind::Item,
            link.item_id.to_string(),
            target_title,
            if relation == RelationKind::SameStory { ConnectionRelation::SameChange } else { ConnectionRelation::Related },
            link.strength,
            link.rationale.clone(),
            evidence_ids,
        ))
        .await
    }

    /// Deduplicate information, not evidence: a new observation of an
    /// existing change becomes more evidence on the existing signal.
    async fn corroborate_signal(
        &self,
        signal_id: &SignalId,
        item: &Item,
        verified: &Verified,
        link: &stream_semantic::ItemLink,
        contexts: &[ContextEntry],
    ) -> Result<()> {
        let mut signal = self
            .get_signal_record(signal_id)
            .await?
            .ok_or_else(|| anyhow!("unknown signal: {signal_id}"))?;
        let interpretation = &verified.interpretation;
        let provenance = self.latest_provenance(&item.id).await?;

        self.insert_claim_evidence(signal_id, item, &provenance, ClaimKind::Subject, &interpretation.subject.excerpts)
            .await?;
        self.insert_claim_evidence(signal_id, item, &provenance, ClaimKind::Change, &interpretation.change.excerpts)
            .await?;
        let mut same_change = link.clone();
        same_change.item_id = signal.item_id.clone();
        self.relate_items(signal_id, item, &provenance, &same_change, RelationKind::SameStory).await?;

        let already = self
            .connections_of_signal(signal_id)
            .await?
            .into_iter()
            .filter(|c| matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project))
            .filter(|c| c.relation == ConnectionRelation::Matches)
            .map(|c| c.target_id)
            .collect::<HashSet<_>>();
        self.connect_contexts(signal_id, item, &provenance, verified, contexts, &already).await?;

        if signal.why_it_matters.is_none() {
            if let Some(why) = &interpretation.why_it_matters {
                self.insert_claim_evidence(signal_id, item, &provenance, ClaimKind::WhyItMatters, &why.excerpts)
                    .await?;
                signal.why_it_matters = Some(why.value.clone());
            }
        }
        signal.updated_at = Utc::now();
        self.store.update("Signal", signal.id.as_str(), signal_record(&signal)).await?;
        Ok(())
    }

    async fn record_decision(&self, item: &Item, decision_type: &str, decision: &str, state: &str, confidence: f32, explanation: &str) -> Result<()> {
        let id = SemanticDecisionId::generate();
        self.store
            .insert(
                "SemanticDecision",
                id.as_str(),
                json!({
                    "__id": id.as_str(),
                    "item": item.id.as_str(),
                    "model": self.interpreter.id(),
                    "decision_type": decision_type,
                    "decision": decision,
                    "advisory_state": state,
                    "confidence": format!("{confidence:.3}"),
                    "explanation": explanation,
                    "created_at": Utc::now().to_rfc3339(),
                }),
                true,
            )
            .await?;
        Ok(())
    }

    /// Every proposal is recorded as an advisory SemanticDecision, whether or
    /// not it became a signal.
    async fn record_decisions(&self, item: &Item, verified: &Verified, meaningful: bool) -> Result<()> {
        let i = &verified.interpretation;
        self.record_decision(item, "topic", &i.topic.value.label, "advisory", i.topic.confidence, &i.topic.rationale)
            .await?;
        self.record_decision(item, "subject", &i.subject.value.label, "advisory", i.subject.confidence, &i.subject.rationale)
            .await?;
        self.record_decision(item, "change", &i.change.value.statement, "advisory", i.change.confidence, &i.change.rationale)
            .await?;
        if let Some(why) = &i.why_it_matters {
            self.record_decision(item, "why_it_matters", &why.value, "advisory", why.confidence, &why.rationale)
                .await?;
        }
        if !verified.rejections.is_empty() {
            let reasons = serde_json::to_string(&verified.rejections)?;
            self.record_decision(item, "evidence_gate", "partially_rejected", "rejected", 0.0, &reasons).await?;
        }
        if !meaningful {
            self.record_decision(item, "signal", "not_created", "advisory", 0.0, "not connected to any context or existing signal")
                .await?;
        }
        Ok(())
    }

    async fn record_rejection(&self, item: &Item, rejections: &[Rejection]) -> Result<()> {
        let reasons = serde_json::to_string(rejections)?;
        self.record_decision(item, "evidence_gate", "rejected", "rejected", 0.0, &reasons).await
    }

    // ----------------------------------------------------------------- context

    pub async fn list_contexts(&self) -> Result<Vec<ContextEntry>> {
        let mut contexts = self
            .store
            .all("ContextEntry")
            .await?
            .into_iter()
            .map(context_from_value)
            .collect::<Result<Vec<_>>>()?;
        contexts.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.name.cmp(&b.name)));
        Ok(contexts)
    }

    pub async fn get_context(&self, id: &ContextId) -> Result<Option<ContextEntry>> {
        self.store.get("ContextEntry", id.as_str()).await?.map(context_from_value).transpose()
    }

    /// Add (or refine) something the user cares about. Adding context
    /// re-evaluates existing signals against it, so connections to things
    /// Stream has already seen appear immediately.
    pub async fn add_context(&self, input: NewContext) -> Result<ContextEntry> {
        let name = stream_model::normalize_whitespace(&input.name);
        if name.is_empty() {
            bail!("context name must not be empty");
        }
        let existing = self.list_contexts().await?;
        let mut related = Vec::new();
        for reference in &input.related {
            let target = existing
                .iter()
                .find(|context| context.id.as_str() == reference || context.name.eq_ignore_ascii_case(reference.trim()))
                .ok_or_else(|| anyhow!("unknown related context: {reference}"))?;
            related.push(target.id.clone());
        }

        let mut entry = match existing.iter().find(|context| slug(&context.name) == slug(&name)) {
            Some(found) => {
                let mut updated = found.clone();
                if let Some(kind) = input.kind {
                    updated.kind = kind;
                }
                if let Some(description) = input.description.as_deref().filter(|d| !d.trim().is_empty()) {
                    updated.description = description.trim().to_owned();
                }
                updated.updated_at = Utc::now();
                updated
            }
            None => ContextEntry::new(
                name,
                input.kind.unwrap_or(ContextKind::Interest),
                input.description.unwrap_or_default(),
            ),
        };
        for alias in input.aliases {
            let alias = stream_model::normalize_whitespace(&alias);
            if !alias.is_empty() && !entry.aliases.iter().any(|a| a.eq_ignore_ascii_case(&alias)) {
                entry.aliases.push(alias);
            }
        }
        for id in related {
            if id != entry.id && !entry.related.contains(&id) {
                entry.related.push(id);
            }
        }

        if existing.iter().any(|context| context.id == entry.id) {
            self.store.update("ContextEntry", entry.id.as_str(), context_record(&entry)).await?;
        } else {
            self.store
                .insert("ContextEntry", entry.id.as_str(), context_record(&entry), true)
                .await?;
        }
        self.reevaluate_for_context(&entry).await?;
        Ok(entry)
    }

    async fn reevaluate_for_context(&self, entry: &ContextEntry) -> Result<()> {
        let contexts = self.list_contexts().await?;
        let signals = self.all_signals().await?;
        let connections = self.all_connections().await?;
        for signal in signals.into_iter().filter(|s| s.status == SignalStatus::Open).take(MAX_PRIOR_ITEMS) {
            let connected = connections
                .iter()
                .filter(|c| c.signal_id.as_ref() == Some(&signal.id))
                .filter(|c| matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project))
                .filter(|c| c.relation == ConnectionRelation::Matches)
                .map(|c| c.target_id.clone())
                .collect::<HashSet<_>>();
            if connected.contains(entry.id.as_str()) {
                continue;
            }
            let Some(item) = self.store.get("Item", signal.item_id.as_str()).await?.map(item_from_value).transpose()? else {
                continue;
            };
            let only = std::slice::from_ref(entry);
            let Ok(proposal) = self
                .interpreter
                .interpret(&InterpretationInput { item: &item, contexts: only, prior: &[] })
                .await
            else {
                continue;
            };
            let Ok(verified) = verify(proposal, &item, only, &[]) else { continue };
            if verified.interpretation.context_matches.is_empty() {
                continue;
            }
            let provenance = self.latest_provenance(&item.id).await?;
            self.connect_contexts(&signal.id, &item, &provenance, &verified, &contexts, &connected)
                .await?;
            if signal.why_it_matters.is_none() {
                if let Some(why) = &verified.interpretation.why_it_matters {
                    self.insert_claim_evidence(&signal.id, &item, &provenance, ClaimKind::WhyItMatters, &why.excerpts)
                        .await?;
                    let mut updated = signal.clone();
                    updated.why_it_matters = Some(why.value.clone());
                    updated.updated_at = Utc::now();
                    self.store.update("Signal", updated.id.as_str(), signal_record(&updated)).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn context_views(&self) -> Result<Vec<ContextView>> {
        let contexts = self.list_contexts().await?;
        let connections = self.all_connections().await?;
        Ok(contexts
            .iter()
            .map(|context| {
                let mut signal_ids = connections
                    .iter()
                    .filter(|c| c.target_id == context.id.as_str())
                    .filter_map(|c| c.signal_id.clone())
                    .collect::<Vec<_>>();
                signal_ids.sort();
                signal_ids.dedup();
                ContextView {
                    context: context.clone(),
                    related: neighbours(context, &contexts)
                        .into_iter()
                        .map(|other| ConnectedLabel {
                            kind: context_target(other),
                            id: other.id.to_string(),
                            label: other.name.clone(),
                            relation: ConnectionRelation::Related,
                            strength: 1.0,
                        })
                        .collect(),
                    signal_ids,
                }
            })
            .collect())
    }

    // ----------------------------------------------------------------- signals

    async fn all_signals(&self) -> Result<Vec<Signal>> {
        self.store.all("Signal").await?.into_iter().map(signal_from_value).collect()
    }

    async fn all_evidence(&self) -> Result<Vec<Evidence>> {
        self.store.all("Evidence").await?.into_iter().map(evidence_from_value).collect()
    }

    async fn all_connections(&self) -> Result<Vec<Connection>> {
        self.store.all("Connection").await?.into_iter().map(connection_from_value).collect()
    }

    async fn connections_of_signal(&self, signal_id: &SignalId) -> Result<Vec<Connection>> {
        self.store
            .find("Connection", json!({ "signal": signal_id.as_str() }))
            .await?
            .into_iter()
            .map(connection_from_value)
            .collect()
    }

    async fn item_map(&self) -> Result<HashMap<ItemId, Item>> {
        Ok(self
            .store
            .all("Item")
            .await?
            .into_iter()
            .map(item_from_value)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|item| (item.id.clone(), item))
            .collect())
    }

    async fn get_signal_record(&self, id: &SignalId) -> Result<Option<Signal>> {
        self.store.get("Signal", id.as_str()).await?.map(signal_from_value).transpose()
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let rules = self
            .store
            .all("Rule")
            .await?
            .into_iter()
            .map(rule_from_value)
            .collect::<Result<Vec<_>>>()?;
        let promoting = rules
            .into_iter()
            .filter(|rule| matches!(rule.action, RuleAction::MarkImportant | RuleAction::CreateAttentionEvent | RuleAction::Save))
            .map(|rule| (rule.id.to_string(), rule.name))
            .collect::<HashMap<_, _>>();
        let mut rule_hits: HashMap<ItemId, Vec<String>> = HashMap::new();
        if !promoting.is_empty() {
            for execution in self.store.find("RuleExecution", json!({ "result": "matched" })).await? {
                let rule = value_string(&execution, "rule")?;
                if let Some(name) = promoting.get(&rule) {
                    rule_hits
                        .entry(ItemId::new(value_string(&execution, "item")?))
                        .or_default()
                        .push(name.clone());
                }
            }
        }
        Ok(Snapshot {
            signals: self.all_signals().await?,
            evidence: self.all_evidence().await?,
            connections: self.all_connections().await?,
            contexts: self.list_contexts().await?,
            items: self.item_map().await?,
            sources: self
                .list_sources()
                .await?
                .into_iter()
                .map(|source| (source.id.clone(), source))
                .collect(),
            provenance: self
                .store
                .all("Provenance")
                .await?
                .into_iter()
                .map(provenance_from_value)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|p| (p.id.clone(), p))
                .collect(),
            rule_hits,
        })
    }

    /// All signals, highest information density first. Signals not eligible
    /// for Today (resolved, dismissed) follow with position 0.
    pub async fn list_signals(&self) -> Result<Vec<SignalSummary>> {
        Ok(self.snapshot().await?.ranked(Utc::now()))
    }

    /// The Today view: open signals only, ranked.
    pub async fn today(&self) -> Result<Vec<SignalSummary>> {
        Ok(self
            .list_signals()
            .await?
            .into_iter()
            .filter(|summary| summary.ranking.eligible_for_today)
            .collect())
    }

    pub async fn get_signal(&self, id: &SignalId) -> Result<Option<SignalDetail>> {
        let snapshot = self.snapshot().await?;
        let ranked = snapshot.ranked(Utc::now());
        let Some(summary) = ranked.into_iter().find(|summary| &summary.signal.id == id) else {
            return Ok(None);
        };
        let evidence = snapshot.evidence_for(id).map(|e| snapshot.trace(e)).collect::<Vec<_>>();
        let mut observation_ids = Vec::new();
        for trace in &evidence {
            if !observation_ids.contains(&trace.evidence.item_id) {
                observation_ids.push(trace.evidence.item_id.clone());
            }
        }
        Ok(Some(SignalDetail {
            summary,
            observations: observation_ids.iter().filter_map(|id| snapshot.item_ref(id)).collect(),
            connections: snapshot.connections_for(id).cloned().collect(),
            evidence,
        }))
    }

    /// Resolve a single piece of evidence down to its item, source, and URL.
    pub async fn get_evidence(&self, id: &EvidenceId) -> Result<Option<EvidenceTrace>> {
        let Some(value) = self.store.get("Evidence", id.as_str()).await? else {
            return Ok(None);
        };
        let evidence = evidence_from_value(value)?;
        let item = match self.store.get("Item", evidence.item_id.as_str()).await? {
            Some(value) => Some(item_from_value(value)?),
            None => None,
        };
        let source = self.get_source(&evidence.source_id).await?;
        let provenance = match &evidence.provenance_id {
            Some(pid) => self.store.get("Provenance", pid.as_str()).await?.map(provenance_from_value).transpose()?,
            None => None,
        };
        Ok(Some(EvidenceTrace {
            item: item.map(|item| ItemRef {
                id: item.id.clone(),
                title: item.title.clone(),
                url: item.canonical_url.clone(),
                published_at: item.published_at,
                observed_at: item.created_at,
                source: source.as_ref().map(SourceRef::from),
            }),
            source: source.as_ref().map(SourceRef::from),
            provenance,
            evidence,
        }))
    }

    pub async fn set_signal_status(&self, id: &SignalId, status: SignalStatus) -> Result<SignalView> {
        let mut signal = self
            .get_signal_record(id)
            .await?
            .ok_or_else(|| anyhow!("unknown signal: {id}"))?;
        signal.status = status;
        signal.updated_at = Utc::now();
        self.store.update("Signal", signal.id.as_str(), signal_record(&signal)).await?;
        Ok(SignalView::from(&signal))
    }

    // ------------------------------------------------------------- connections

    /// Connections touching a signal, item, or context (by ID), or all
    /// connections when `target` is `None`.
    pub async fn list_connections(&self, target: Option<&str>) -> Result<Vec<ConnectionView>> {
        let snapshot = self.snapshot().await?;
        let subject_of = snapshot
            .signals
            .iter()
            .map(|s| (s.id.clone(), s.subject.label.clone()))
            .collect::<HashMap<_, _>>();
        let related_items = match target {
            Some(id) if id.starts_with("item_") => {
                let item = ItemId::new(id);
                snapshot
                    .evidence
                    .iter()
                    .filter(|e| e.item_id == item)
                    .map(|e| e.signal_id.clone())
                    .collect::<HashSet<_>>()
            }
            _ => HashSet::new(),
        };
        let mut views = snapshot
            .connections
            .iter()
            .filter(|c| match target {
                None => true,
                Some(id) => {
                    c.signal_id.as_ref().map(|s| s.as_str() == id).unwrap_or(false)
                        || c.item_id.as_str() == id
                        || c.target_id == id
                        || c.signal_id.as_ref().map(|s| related_items.contains(s)).unwrap_or(false)
                }
            })
            .map(|c| ConnectionView {
                connection: c.clone(),
                signal_subject: c.signal_id.as_ref().and_then(|s| subject_of.get(s).cloned()),
                item_title: snapshot.items.get(&c.item_id).map(|i| i.title.clone()),
            })
            .collect::<Vec<_>>();
        views.sort_by(|a, b| {
            b.connection
                .strength
                .partial_cmp(&a.connection.strength)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.connection.created_at.cmp(&b.connection.created_at))
        });
        Ok(views)
    }

    pub async fn connection_graph(&self) -> Result<ConnectionGraph> {
        let snapshot = self.snapshot().await?;
        let signal_by_id = snapshot.signals.iter().map(|s| (s.id.clone(), s)).collect::<HashMap<_, _>>();
        let signal_ref = |id: &SignalId, strength: f32, relation: ConnectionRelation| {
            signal_by_id.get(id).map(|s| SignalRef {
                id: s.id.clone(),
                subject: s.subject.label.clone(),
                change: s.change.statement.clone(),
                strength,
                relation,
            })
        };

        let contexts = snapshot
            .contexts
            .iter()
            .map(|context| {
                let mut signals: Vec<SignalRef> = Vec::new();
                for connection in snapshot.connections.iter().filter(|c| c.target_id == context.id.as_str()) {
                    let Some(signal_id) = &connection.signal_id else { continue };
                    match signals.iter_mut().find(|s| &s.id == signal_id) {
                        Some(existing) if connection.relation == ConnectionRelation::Matches => {
                            existing.relation = ConnectionRelation::Matches;
                            existing.strength = existing.strength.max(connection.strength);
                        }
                        Some(_) => {}
                        None => signals.extend(signal_ref(signal_id, connection.strength, connection.relation)),
                    }
                }
                signals.sort_by(|a, b| {
                    (a.relation == ConnectionRelation::Via)
                        .cmp(&(b.relation == ConnectionRelation::Via))
                        .then(b.strength.partial_cmp(&a.strength).unwrap_or(std::cmp::Ordering::Equal))
                });
                ContextNode {
                    context: context.clone(),
                    related: neighbours(context, &snapshot.contexts)
                        .into_iter()
                        .map(|other| ConnectedLabel {
                            kind: context_target(other),
                            id: other.id.to_string(),
                            label: other.name.clone(),
                            relation: ConnectionRelation::Related,
                            strength: 1.0,
                        })
                        .collect(),
                    signals,
                }
            })
            .collect();

        let mut subjects: BTreeMap<String, SubjectNode> = BTreeMap::new();
        for signal in &snapshot.signals {
            let key = slug(&signal.subject.label);
            let observations = snapshot
                .evidence_for(&signal.id)
                .map(|e| e.item_id.clone())
                .collect::<HashSet<_>>()
                .len();
            let node = subjects.entry(key.clone()).or_insert_with(|| SubjectNode {
                key,
                label: signal.subject.label.clone(),
                signals: vec![],
                observation_count: 0,
            });
            node.observation_count += observations;
            node.signals
                .extend(signal_ref(&signal.id, signal.confidence, ConnectionRelation::About));
        }
        let mut subjects = subjects.into_values().collect::<Vec<_>>();
        subjects.sort_by(|a, b| b.observation_count.cmp(&a.observation_count).then(a.label.cmp(&b.label)));

        let item_relations = self
            .store
            .all("ItemRelation")
            .await?
            .into_iter()
            .map(item_relation_from_value)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|relation| relation.from_item_id != relation.to_item_id)
            .collect();

        Ok(ConnectionGraph { contexts, subjects, item_relations })
    }

    // ------------------------------------------------------------ source views

    pub async fn source_detail(&self, id: &SourceId) -> Result<Option<SourceDetail>> {
        let Some(source) = self.get_source(id).await? else {
            return Ok(None);
        };
        let mut attempts = self
            .store
            .find("FetchAttempt", json!({ "source": id.as_str() }))
            .await?
            .into_iter()
            .map(fetch_attempt_from_value)
            .collect::<Result<Vec<_>>>()?;
        attempts.sort_by(|a, b| b.attempted_at.cmp(&a.attempted_at));
        let observed = self
            .store
            .find("ItemSource", json!({ "source": id.as_str() }))
            .await?
            .into_iter()
            .filter_map(|value| value_string_opt(&value, "item").ok().flatten())
            .collect::<HashSet<_>>();
        let items = self.item_map().await?;
        let source_ref = SourceRef::from(&source);
        let mut item_refs = observed
            .iter()
            .filter_map(|id| items.get(&ItemId::new(id.clone())))
            .map(|item| ItemRef {
                id: item.id.clone(),
                title: item.title.clone(),
                url: item.canonical_url.clone(),
                published_at: item.published_at,
                observed_at: item.created_at,
                source: Some(source_ref.clone()),
            })
            .collect::<Vec<_>>();
        item_refs.sort_by(|a, b| b.published_at.cmp(&a.published_at).then(b.observed_at.cmp(&a.observed_at)));
        let mut signal_ids = self
            .all_evidence()
            .await?
            .into_iter()
            .filter(|e| observed.contains(e.item_id.as_str()))
            .map(|e| e.signal_id)
            .collect::<Vec<_>>();
        signal_ids.sort();
        signal_ids.dedup();
        Ok(Some(SourceDetail { source, attempts, items: item_refs, signal_ids }))
    }

    /// Sources that are part of the product experience, most recent first.
    pub async fn list_source_views(&self) -> Result<Vec<Source>> {
        let mut sources = self.list_sources().await?;
        sources.sort_by(|a, b| b.discovered_at.cmp(&a.discovered_at));
        Ok(sources)
    }

    /// Re-read the underlying item and check that an evidence excerpt is
    /// verbatim in it: interpretation must stay traceable to source material.
    pub async fn evidence_is_grounded(&self, evidence: &Evidence) -> Result<bool> {
        let item = self
            .store
            .get("Item", evidence.item_id.as_str())
            .await?
            .map(item_from_value)
            .transpose()?
            .with_context(|| format!("evidence {} points at a missing item", evidence.id))?;
        Ok(stream_semantic::excerpt_is_grounded(
            &item,
            &Excerpt { locator: evidence.locator, text: evidence.excerpt.clone() },
        ))
    }
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}
