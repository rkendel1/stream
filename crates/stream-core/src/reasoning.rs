//! Thinking with Stream: retrieval, grounded answers, and saved reasoning.
//!
//! Question → Stream retrieval → evidence/context bundle → reasoner →
//! answer gate → answer with provenance. Conversations are an interface,
//! not memory: asking writes nothing (beyond intelligence events when the
//! model misbehaves). Only an explicit "save" turns reasoning into durable,
//! advisory Stream state — an [`Insight`].

use super::intelligence::{ConnectedLabel, EvidenceTrace, Snapshot, SourceRef};
use super::records::{insight_from_value, insight_record};
use super::StreamRuntime;
use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use stream_model::{
    slug, ClaimBasis, ClaimKind, ConnectionRelation, ConnectionTargetKind, ContextId, ContextKind, EvidenceId, Insight,
    InsightId, InsightKind, InsightStatus, ItemId, RelationKind, SignalId, SignalStatus, SourceId,
};
use stream_reason::{
    content_terms, verify_answer, Bundle, BundleClaim, BundleConnection, BundleContext, BundleEvidence, BundleInsight,
    BundleSignal, Intent, LocalReasoner, Reasoner, RelatedPair, Scope, Statement, Sufficiency, TimelineEntry,
};

/// Signals and evidence handed to a reasoner per question: enough to think
/// with, never the whole database.
pub const MAX_BUNDLE_SIGNALS: usize = 8;
pub const MAX_BUNDLE_EVIDENCE: usize = 60;
pub const MAX_EVIDENCE_PER_SIGNAL: usize = 16;

/// What the user pointed at ("Ask Stream" from a signal, source, item, or context).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Focus {
    #[serde(default)]
    pub signal_ids: Vec<SignalId>,
    #[serde(default)]
    pub context_ids: Vec<ContextId>,
    #[serde(default)]
    pub item_ids: Vec<ItemId>,
    #[serde(default)]
    pub source_ids: Vec<SourceId>,
}

impl Focus {
    pub fn is_empty(&self) -> bool {
        self.signal_ids.is_empty() && self.context_ids.is_empty() && self.item_ids.is_empty() && self.source_ids.is_empty()
    }
}

/// A previous turn, held by the client. It lets "that" and "these" refer to
/// what the last answer was about; it is never stored.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HistoryTurn {
    pub question: String,
    #[serde(default)]
    pub signal_ids: Vec<SignalId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AskRequest {
    pub question: String,
    #[serde(default)]
    pub focus: Focus,
    #[serde(default)]
    pub history: Vec<HistoryTurn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnswerSignal {
    pub id: SignalId,
    pub topic: String,
    pub subject: String,
    pub change: String,
}

/// What Stream looked at to answer — so an answer never rests on anything
/// the user cannot see.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retrieved {
    pub intent: Intent,
    pub focused: bool,
    pub signal_ids: Vec<SignalId>,
    pub context_ids: Vec<ContextId>,
    pub evidence_count: usize,
    pub time_window: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Answer {
    pub question: String,
    pub summary: String,
    pub sufficiency: Sufficiency,
    pub statements: Vec<Statement>,
    pub uncertainties: Vec<String>,
    pub follow_ups: Vec<String>,
    /// Every piece of evidence cited by a statement, traced to its source.
    pub evidence: Vec<EvidenceTrace>,
    pub signals: Vec<AnswerSignal>,
    pub connected_to: Vec<ConnectedLabel>,
    pub sources: Vec<SourceRef>,
    pub retrieved: Retrieved,
    /// Plain-language notes, e.g. when the model was unavailable.
    pub notices: Vec<String>,
    pub answered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NewInsight {
    pub kind: Option<InsightKind>,
    pub statement: String,
    #[serde(default)]
    pub basis: Option<ClaimBasis>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub signal_ids: Vec<SignalId>,
    #[serde(default)]
    pub context_ids: Vec<ContextId>,
    #[serde(default)]
    pub confidence: Option<f32>,
    #[serde(default)]
    pub uncertainty: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightView {
    pub insight: Insight,
    pub evidence: Vec<EvidenceTrace>,
    pub signals: Vec<AnswerSignal>,
    pub contexts: Vec<ConnectedLabel>,
}

const ANAPHORA: &[&str] = &["that", "this", "it", "these", "those", "they", "them"];

fn is_anaphoric(question: &str) -> bool {
    question
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .any(|word| ANAPHORA.contains(&word))
}

fn time_window(question: &str, now: DateTime<Utc>) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let q = question.to_lowercase();
    let days = if q.contains("today") {
        1
    } else if q.contains("this week") || q.contains("past week") || q.contains("last week") {
        7
    } else if q.contains("this month") || q.contains("past month") || q.contains("last month") {
        31
    } else if q.contains("recent") || q.contains("lately") || q.contains("what's new") || q.contains("what is new") {
        14
    } else {
        return None;
    };
    Some((now - Duration::days(days), now))
}

fn claim_priority(claim: ClaimKind) -> u8 {
    match claim {
        ClaimKind::Change => 0,
        ClaimKind::Corroboration => 1,
        ClaimKind::Contradiction => 2,
        ClaimKind::Statement => 3,
        ClaimKind::Detail => 3,
        ClaimKind::WhyItMatters => 4,
        ClaimKind::Connection => 5,
        ClaimKind::Subject => 6,
        ClaimKind::Topic => 7,
    }
}

/// A publisher label: the web origin's host, with the port when it is not
/// the default (different ports are different publishers).
fn source_label(snapshot: &Snapshot, id: &SourceId, url: &url::Url) -> String {
    let url = snapshot.sources.get(id).map(|s| &s.canonical_url).unwrap_or(url);
    let host = url.host_str().unwrap_or_default().trim_start_matches("www.");
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}

/// Build the evidence/context bundle for a question from a snapshot of
/// Stream. Pure: the same snapshot and request always give the same bundle.
pub(crate) fn build_bundle(snapshot: &Snapshot, request: &AskRequest, now: DateTime<Utc>) -> Bundle {
    let question = request.question.trim().to_owned();
    let intent = Intent::detect(&question);
    let terms = content_terms(&question).into_iter().collect::<HashSet<_>>();
    let window = time_window(&question, now);
    let ranked = snapshot.ranked(now);
    let rank_of = ranked.iter().map(|s| (s.signal.id.clone(), s)).collect::<HashMap<_, _>>();

    // Scope: what the user pointed at, or what the previous answer was about.
    let mut focus = request.focus.clone();
    if focus.is_empty() && is_anaphoric(&question) {
        if let Some(last) = request.history.iter().rev().find(|turn| !turn.signal_ids.is_empty()) {
            focus.signal_ids = last.signal_ids.clone();
        }
    }
    let mut focus_signals = focus.signal_ids.iter().cloned().collect::<HashSet<_>>();
    for evidence in &snapshot.evidence {
        if focus.item_ids.contains(&evidence.item_id) || focus.source_ids.contains(&evidence.source_id) {
            focus_signals.insert(evidence.signal_id.clone());
        }
    }
    for connection in &snapshot.connections {
        if let Some(signal) = &connection.signal_id {
            if focus.context_ids.iter().any(|c| c.as_str() == connection.target_id) {
                focus_signals.insert(signal.clone());
            }
        }
    }
    let focused = !focus.is_empty();

    // Contexts the question names, by name or alias; "things I'm building" means projects.
    let mut mentioned = snapshot
        .contexts
        .iter()
        .filter(|context| {
            std::iter::once(&context.name).chain(context.aliases.iter()).any(|phrase| {
                let phrase_terms = content_terms(phrase);
                !phrase_terms.is_empty() && phrase_terms.iter().all(|t| terms.contains(t))
            })
        })
        .map(|c| c.id.clone())
        .collect::<Vec<_>>();
    mentioned.extend(focus.context_ids.iter().cloned());
    let q = question.to_lowercase();
    if q.contains("building") || q.contains("project") {
        mentioned.extend(snapshot.contexts.iter().filter(|c| c.kind == ContextKind::Project).map(|c| c.id.clone()));
    }
    mentioned.sort();
    mentioned.dedup();

    let general = terms.is_empty()
        || matches!(intent, Intent::Changes | Intent::Evolution | Intent::Investigate | Intent::Patterns | Intent::Uncertainty)
            && mentioned.is_empty();

    let mut scored: Vec<(f32, Vec<String>, &stream_model::Signal)> = Vec::new();
    for signal in &snapshot.signals {
        let mut score = 0.0f32;
        let mut reasons = Vec::new();
        if focused {
            if focus_signals.contains(&signal.id) {
                score += 10.0;
                reasons.push("you asked about this".into());
            } else {
                continue;
            }
        }
        for connection in snapshot.connections_for(&signal.id) {
            if !matches!(connection.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project) {
                continue;
            }
            if mentioned.iter().any(|m| m.as_str() == connection.target_id) {
                let add = if connection.relation == ConnectionRelation::Via { 2.0 } else { 3.0 + 6.0 * connection.strength };
                score += add;
                reasons.push(format!("connected to {}", connection.label));
            }
        }
        let mut text = format!(
            "{} {} {} {}",
            signal.topic.label,
            signal.subject.label,
            signal.change.statement,
            signal.why_it_matters.clone().unwrap_or_default()
        );
        if let Some(item) = snapshot.items.get(&signal.item_id) {
            text.push(' ');
            text.push_str(&item.title);
        }
        let signal_terms = content_terms(&text).into_iter().collect::<HashSet<_>>();
        let shared = terms.intersection(&signal_terms).count();
        if shared > 0 {
            score += 1.5 * shared as f32;
            reasons.push(format!("shares {shared} term(s) with the question"));
        }
        if !focused && general && score == 0.0 && signal.status == SignalStatus::Open {
            if let Some(summary) = rank_of.get(&signal.id) {
                score += 0.5 + (summary.ranking.total as f32 / 20.0);
                reasons.push(format!("#{} in Today", summary.position));
            }
        }
        if let Some((from, _)) = window {
            let last = snapshot.evidence_for(&signal.id).map(|e| e.observed_at).max().unwrap_or(signal.created_at);
            if last < from {
                continue;
            }
        }
        if score > 0.0 {
            scored.push((score, reasons, signal));
        }
    }
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.2.updated_at.cmp(&a.2.updated_at))
    });
    scored.truncate(MAX_BUNDLE_SIGNALS);

    let kept = scored.iter().map(|(_, _, s)| s.id.clone()).collect::<HashSet<_>>();
    let mut evidence = Vec::new();
    for (_, _, signal) in &scored {
        // Round-robin across claim kinds so every kind of claim (change,
        // corroboration, why-it-matters, connection, ...) stays supported
        // within the per-signal budget.
        let mut by_claim: BTreeMap<u8, Vec<&stream_model::Evidence>> = BTreeMap::new();
        for row in snapshot.evidence_for(&signal.id) {
            by_claim.entry(claim_priority(row.claim)).or_default().push(row);
        }
        for rows in by_claim.values_mut() {
            rows.sort_by(|a, b| a.observed_at.cmp(&b.observed_at));
        }
        let depth = by_claim.values().map(Vec::len).max().unwrap_or(0);
        let rows = (0..depth)
            .flat_map(|round| by_claim.values().filter_map(move |rows| rows.get(round).copied()))
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        for row in rows {
            if evidence.len() >= MAX_BUNDLE_EVIDENCE || seen.len() >= MAX_EVIDENCE_PER_SIGNAL {
                break;
            }
            if !seen.insert((row.item_id.clone(), row.claim, row.excerpt.clone())) {
                continue;
            }
            let item = snapshot.items.get(&row.item_id);
            evidence.push(BundleEvidence {
                id: row.id.clone(),
                signal_id: row.signal_id.clone(),
                claim: row.claim,
                excerpt: row.excerpt.clone(),
                item_id: row.item_id.clone(),
                item_title: item.map(|i| i.title.clone()).unwrap_or_default(),
                source_id: row.source_id.clone(),
                source_label: source_label(snapshot, &row.source_id, &row.url),
                url: row.url.to_string(),
                observed_at: row.observed_at,
                published_at: item.and_then(|i| i.published_at),
            });
        }
    }
    let evidence_ids = evidence.iter().map(|e| e.id.clone()).collect::<HashSet<_>>();
    let keep_cited = |ids: &[EvidenceId]| ids.iter().filter(|id| evidence_ids.contains(*id)).cloned().collect::<Vec<_>>();

    let signals = scored
        .iter()
        .map(|(score, reasons, signal)| {
            let rows = snapshot.evidence_for(&signal.id).collect::<Vec<_>>();
            let summary = rank_of.get(&signal.id);
            BundleSignal {
                id: signal.id.clone(),
                topic: signal.topic.label.clone(),
                subject: signal.subject.label.clone(),
                change: signal.change.statement.clone(),
                change_kind: signal.change.kind,
                why_it_matters: signal.why_it_matters.clone(),
                status: signal.status,
                first_observed_at: rows.iter().map(|e| e.observed_at).min().unwrap_or(signal.created_at),
                last_observed_at: rows.iter().map(|e| e.observed_at).max().unwrap_or(signal.created_at),
                source_count: rows.iter().map(|e| &e.source_id).collect::<HashSet<_>>().len(),
                observation_count: rows.iter().map(|e| &e.item_id).collect::<HashSet<_>>().len(),
                position: summary.map(|s| s.position).unwrap_or(0),
                connections: snapshot
                    .connections_for(&signal.id)
                    .filter(|c| !matches!(c.target_kind, ConnectionTargetKind::Topic | ConnectionTargetKind::Subject))
                    .map(|c| BundleConnection {
                        target_kind: c.target_kind,
                        target_id: c.target_id.clone(),
                        label: c.label.clone(),
                        relation: c.relation,
                        strength: c.strength,
                        rationale: c.rationale.clone(),
                        evidence_ids: keep_cited(&c.evidence_ids),
                    })
                    .collect(),
                claims: snapshot
                    .claims
                    .iter()
                    .filter(|c| c.signal_id == signal.id)
                    .map(|c| BundleClaim {
                        id: c.id.clone(),
                        basis: c.basis,
                        statement: c.statement.clone(),
                        evidence_ids: keep_cited(&c.evidence_ids),
                        context_ids: c.context_ids.clone(),
                    })
                    .collect(),
                synthesis: snapshot.syntheses.get(&signal.id).map(|s| {
                    let mut s = s.clone();
                    for point in s.agreements.iter_mut().chain(s.new_information.iter_mut()).chain(s.differences.iter_mut()).chain(s.uncertainties.iter_mut()) {
                        point.evidence_ids = keep_cited(&point.evidence_ids);
                    }
                    s
                }),
                relevance: *score,
                reasons: reasons.clone(),
            }
        })
        .collect::<Vec<_>>();

    // Contexts: named in the question, or connected to what was retrieved.
    let mut context_ids = mentioned.iter().cloned().collect::<HashSet<_>>();
    for signal in &signals {
        for c in &signal.connections {
            if matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project) {
                context_ids.insert(ContextId::new(c.target_id.clone()));
            }
        }
    }
    let contexts = snapshot
        .contexts
        .iter()
        .filter(|c| context_ids.contains(&c.id))
        .map(|c| BundleContext {
            id: c.id.clone(),
            name: c.name.clone(),
            kind: c.kind,
            description: c.description.clone(),
            related: snapshot
                .contexts
                .iter()
                .filter(|o| c.related.contains(&o.id) || o.related.contains(&c.id))
                .map(|o| o.name.clone())
                .collect(),
            created_at: c.created_at,
        })
        .collect::<Vec<_>>();

    // Timeline from durable history: sources, observations, signals, context, insights.
    let mut timeline = Vec::new();
    for signal in &signals {
        let mut first_by_item: BTreeMap<DateTime<Utc>, &BundleEvidence> = BTreeMap::new();
        let mut seen_items = HashSet::new();
        let mut rows = evidence.iter().filter(|e| e.signal_id == signal.id).collect::<Vec<_>>();
        rows.sort_by(|a, b| a.observed_at.cmp(&b.observed_at).then(claim_priority(a.claim).cmp(&claim_priority(b.claim))));
        for row in rows {
            if seen_items.insert(row.item_id.clone()) {
                first_by_item.insert(row.observed_at, row);
            }
        }
        for (index, (at, row)) in first_by_item.into_iter().enumerate() {
            let disputed = evidence.iter().any(|e| e.item_id == row.item_id && e.signal_id == signal.id && e.claim == ClaimKind::Contradiction);
            let (kind, verb) = match (index, disputed) {
                (0, _) => ("observed", "first reported"),
                (_, true) => ("disputed", "disputed it"),
                _ => ("corroborated", "corroborated it"),
            };
            timeline.push(TimelineEntry {
                at,
                kind: kind.into(),
                description: format!("{} {}: “{}”", row.source_label, verb, row.excerpt),
                signal_id: Some(signal.id.clone()),
                source_id: Some(row.source_id.clone()),
                evidence_id: Some(row.id.clone()),
            });
        }
        if let Some(record) = snapshot.signals.iter().find(|s| s.id == signal.id) {
            timeline.push(TimelineEntry {
                at: record.created_at,
                kind: "signal_created".into(),
                description: format!("Stream built the signal “{}: {}”", signal.subject, signal.change),
                signal_id: Some(signal.id.clone()),
                source_id: None,
                evidence_id: None,
            });
        }
    }
    let bundle_sources = evidence.iter().map(|e| e.source_id.clone()).collect::<HashSet<_>>();
    for source in snapshot.sources.values().filter(|s| bundle_sources.contains(&s.id)) {
        timeline.push(TimelineEntry {
            at: source.discovered_at,
            kind: "source_added".into(),
            description: format!("You added {}", source.title.clone().unwrap_or_else(|| source.canonical_url.to_string())),
            signal_id: None,
            source_id: Some(source.id.clone()),
            evidence_id: None,
        });
    }
    for context in &contexts {
        timeline.push(TimelineEntry {
            at: context.created_at,
            kind: "context_added".into(),
            description: format!("You told Stream you care about {}", context.name),
            signal_id: None,
            source_id: None,
            evidence_id: None,
        });
    }
    let insights = snapshot
        .insights
        .iter()
        .filter(|i| {
            i.signal_ids.iter().any(|s| kept.contains(s))
                || i.context_ids.iter().any(|c| context_ids.contains(c))
                || (intent == Intent::Investigate && i.status == InsightStatus::Open)
        })
        .map(|i| BundleInsight {
            id: i.id.clone(),
            kind: i.kind,
            status: i.status,
            statement: i.statement.clone(),
            basis: i.basis,
            signal_ids: i.signal_ids.clone(),
            evidence_ids: keep_cited(&i.evidence_ids),
            created_at: i.created_at,
        })
        .collect::<Vec<_>>();
    for insight in &insights {
        timeline.push(TimelineEntry {
            at: insight.created_at,
            kind: "insight_saved".into(),
            description: format!("You saved a {}: {}", insight.kind.as_str().replace('_', " "), insight.statement),
            signal_id: insight.signal_ids.first().cloned(),
            source_id: None,
            evidence_id: None,
        });
    }
    if let Some((from, _)) = window {
        timeline.retain(|entry| entry.at >= from || entry.kind == "context_added");
    }
    timeline.sort_by(|a, b| a.at.cmp(&b.at));

    // How the retrieved signals relate to each other.
    let items_of = |signal: &SignalId| evidence.iter().filter(|e| &e.signal_id == signal).map(|e| e.item_id.clone()).collect::<HashSet<_>>();
    let mut related_pairs = Vec::new();
    for (i, a) in signals.iter().enumerate() {
        for b in signals.iter().skip(i + 1) {
            let (items_a, items_b) = (items_of(&a.id), items_of(&b.id));
            let change_evidence = |s: &BundleSignal| {
                evidence.iter().filter(|e| e.signal_id == s.id && e.claim == ClaimKind::Change).map(|e| e.id.clone()).take(1).collect::<Vec<_>>()
            };
            if let Some(relation) = snapshot.item_relations.iter().find(|r| {
                (items_a.contains(&r.from_item_id) && items_b.contains(&r.to_item_id))
                    || (items_b.contains(&r.from_item_id) && items_a.contains(&r.to_item_id))
            }) {
                let kind = match relation.relation {
                    RelationKind::SameStory if relation.evidence.starts_with("disputes") => "contradicts",
                    RelationKind::SameStory => "same_change",
                    _ => "related",
                };
                let mut ids = change_evidence(a);
                ids.extend(change_evidence(b));
                related_pairs.push(RelatedPair {
                    a: a.id.clone(),
                    b: b.id.clone(),
                    relation: kind.into(),
                    rationale: format!("{} and {} are linked by Stream's observations: {}.", a.subject, b.subject, relation.evidence.trim_end_matches('.')),
                    evidence_ids: ids,
                });
                continue;
            }
            let shared = a
                .connections
                .iter()
                .filter(|c| c.relation == ConnectionRelation::Matches)
                .filter_map(|c| {
                    b.connections
                        .iter()
                        .find(|d| d.relation == ConnectionRelation::Matches && d.target_id == c.target_id)
                        .map(|d| (c, d))
                })
                .next();
            if let Some((ca, cb)) = shared {
                let mut ids = ca.evidence_ids.iter().take(1).cloned().collect::<Vec<_>>();
                ids.extend(cb.evidence_ids.iter().take(1).cloned());
                related_pairs.push(RelatedPair {
                    a: a.id.clone(),
                    b: b.id.clone(),
                    relation: "shared_context".into(),
                    rationale: format!(
                        "{} and {} both connect to {}, but no source links them to each other directly.",
                        a.subject, b.subject, ca.label
                    ),
                    evidence_ids: ids,
                });
            }
        }
    }

    Bundle {
        question,
        intent,
        scope: Scope {
            signal_ids: focus_signals.into_iter().collect(),
            context_ids: focus.context_ids.clone(),
            item_ids: focus.item_ids.clone(),
            source_ids: focus.source_ids.clone(),
            focused,
        },
        time_window: window,
        mentioned_contexts: mentioned,
        contexts,
        signals,
        evidence,
        timeline,
        insights,
        related_pairs,
    }
}

fn default_basis(kind: InsightKind) -> ClaimBasis {
    match kind {
        InsightKind::Insight | InsightKind::DecisionCandidate => ClaimBasis::Inferred,
        InsightKind::Question | InsightKind::Hypothesis | InsightKind::Investigation => ClaimBasis::Hypothesis,
    }
}

impl StreamRuntime {
    /// The Stream retrieval layer: the knowledge a question should be
    /// answered from.
    pub async fn retrieve(&self, request: &AskRequest) -> Result<Bundle> {
        let snapshot = self.snapshot().await?;
        Ok(build_bundle(&snapshot, request, Utc::now()))
    }

    /// Ask Stream. The answer is grounded in retrieved Stream evidence, and
    /// says explicitly when that evidence is not enough.
    pub async fn ask(&self, request: AskRequest) -> Result<Answer> {
        if request.question.trim().is_empty() {
            bail!("ask Stream a question");
        }
        let snapshot = self.snapshot().await?;
        let bundle = build_bundle(&snapshot, &request, Utc::now());

        let mut notices = Vec::new();
        let mut reasoners: Vec<std::sync::Arc<dyn Reasoner>> = vec![self.reasoner.clone()];
        reasoners.extend(self.fallback_reasoner.clone());
        let mut proposal = None;
        for (attempt, reasoner) in reasoners.iter().enumerate() {
            match reasoner.answer(&bundle).await {
                Ok(answer) => {
                    if attempt > 0 {
                        notices.push("Answered with Stream's local reasoning because the model was unavailable.".into());
                    }
                    proposal = Some((answer, reasoner.id().to_owned()));
                    break;
                }
                Err(error) => {
                    self.record_event("reason", "failed", None, &format!("{}: {error:#}", bundle.question), reasoner.id())
                        .await?;
                }
            }
        }
        let (proposal, reasoner_id) = match proposal {
            Some(found) => found,
            None => {
                notices.push("Stream's reasoning is unavailable right now; showing only what was retrieved.".into());
                (LocalReasoner::compose(&bundle), LocalReasoner::ID.to_owned())
            }
        };
        let (answer, rejections) = verify_answer(proposal, &bundle);
        if !rejections.is_empty() {
            self.record_event("reason", "rejected", None, &serde_json::to_string(&rejections)?, &reasoner_id)
                .await?;
        }

        let cited = answer.statements.iter().flat_map(|s| s.evidence_ids.iter().cloned()).collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let evidence = cited
            .iter()
            .filter(|id| seen.insert((*id).clone()))
            .filter_map(|id| snapshot.evidence.iter().find(|e| &e.id == id))
            .map(|e| snapshot.trace(e))
            .collect::<Vec<_>>();
        let mut signal_ids = Vec::new();
        for id in answer.statements.iter().flat_map(|s| s.signal_ids.iter()) {
            if !signal_ids.contains(id) {
                signal_ids.push(id.clone());
            }
        }
        let signals = signal_ids
            .iter()
            .filter_map(|id| bundle.signals.iter().find(|s| &s.id == id))
            .map(|s| AnswerSignal { id: s.id.clone(), topic: s.topic.clone(), subject: s.subject.clone(), change: s.change.clone() })
            .collect::<Vec<_>>();
        let mut connected: BTreeMap<String, ConnectedLabel> = BTreeMap::new();
        for id in &signal_ids {
            for label in snapshot.connected_to(id) {
                connected.entry(label.id.clone()).or_insert(label);
            }
        }
        let mut sources = Vec::new();
        for trace in &evidence {
            if let Some(source) = &trace.source {
                if !sources.iter().any(|s: &SourceRef| s.id == source.id) {
                    sources.push(source.clone());
                }
            }
        }

        Ok(Answer {
            question: bundle.question.clone(),
            summary: answer.summary,
            sufficiency: answer.sufficiency,
            statements: answer.statements,
            uncertainties: answer.uncertainties,
            follow_ups: answer.follow_ups,
            evidence,
            signals,
            connected_to: connected.into_values().collect(),
            sources,
            retrieved: Retrieved {
                intent: bundle.intent,
                focused: bundle.scope.focused,
                signal_ids: bundle.signals.iter().map(|s| s.id.clone()).collect(),
                context_ids: bundle.contexts.iter().map(|c| c.id.clone()).collect(),
                evidence_count: bundle.evidence.len(),
                time_window: bundle.time_window,
            },
            notices,
            answered_at: Utc::now(),
        })
    }

    /// Save reasoning as durable, explicitly advisory Stream knowledge. Every
    /// reference is checked against Stream; nothing can cite evidence that
    /// does not exist.
    pub async fn save_insight(&self, input: NewInsight) -> Result<Insight> {
        let statement = stream_model::normalize_whitespace(&input.statement);
        if statement.is_empty() {
            bail!("an insight needs a statement");
        }
        let kind = input.kind.unwrap_or(InsightKind::Insight);
        let basis = input.basis.unwrap_or_else(|| default_basis(kind));
        let snapshot = self.snapshot().await?;
        let mut signal_ids = input.signal_ids.clone();
        for id in &input.evidence_ids {
            let evidence = snapshot
                .evidence
                .iter()
                .find(|e| &e.id == id)
                .ok_or_else(|| anyhow!("unknown evidence: {id}"))?;
            if !signal_ids.contains(&evidence.signal_id) {
                signal_ids.push(evidence.signal_id.clone());
            }
        }
        for id in &signal_ids {
            if !snapshot.signals.iter().any(|s| &s.id == id) {
                bail!("unknown signal: {id}");
            }
        }
        for id in &input.context_ids {
            if !snapshot.contexts.iter().any(|c| &c.id == id) {
                bail!("unknown context: {id}");
            }
        }
        if basis.requires_evidence() && input.evidence_ids.is_empty() {
            bail!("a {} insight needs supporting evidence; save it as a hypothesis or question instead", basis);
        }
        let now = Utc::now();
        let insight = Insight {
            id: InsightId::generate(),
            kind,
            status: InsightStatus::Open,
            statement,
            basis,
            question: input.question.map(|q| q.trim().to_owned()).filter(|q| !q.is_empty()),
            evidence_ids: input.evidence_ids,
            signal_ids,
            context_ids: input.context_ids,
            confidence: input.confidence.map(|c| c.clamp(0.0, 1.0)),
            uncertainty: input.uncertainty.filter(|u| !u.trim().is_empty()),
            reasoner: self.reasoner.id().to_owned(),
            model_backed: self.model_backed(),
            created_at: now,
            updated_at: now,
        };
        self.store.insert("Insight", insight.id.as_str(), insight_record(&insight), true).await?;
        Ok(insight)
    }

    pub async fn list_insights(&self, signal: Option<&SignalId>) -> Result<Vec<Insight>> {
        let mut insights = self
            .store
            .all("Insight")
            .await?
            .into_iter()
            .map(insight_from_value)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|i| signal.map(|s| i.signal_ids.contains(s)).unwrap_or(true))
            .collect::<Vec<_>>();
        insights.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(insights)
    }

    pub async fn get_insight(&self, id: &InsightId) -> Result<Option<InsightView>> {
        let Some(value) = self.store.get("Insight", id.as_str()).await? else { return Ok(None) };
        let insight = insight_from_value(value)?;
        let snapshot = self.snapshot().await?;
        let evidence = insight
            .evidence_ids
            .iter()
            .filter_map(|id| snapshot.evidence.iter().find(|e| &e.id == id))
            .map(|e| snapshot.trace(e))
            .collect();
        let signals = insight
            .signal_ids
            .iter()
            .filter_map(|id| snapshot.signals.iter().find(|s| &s.id == id))
            .map(|s| AnswerSignal {
                id: s.id.clone(),
                topic: s.topic.label.clone(),
                subject: s.subject.label.clone(),
                change: s.change.statement.clone(),
            })
            .collect();
        let contexts = insight
            .context_ids
            .iter()
            .filter_map(|id| snapshot.contexts.iter().find(|c| &c.id == id))
            .map(|c| ConnectedLabel {
                kind: if c.kind == ContextKind::Project { ConnectionTargetKind::Project } else { ConnectionTargetKind::Context },
                id: c.id.to_string(),
                label: c.name.clone(),
                relation: ConnectionRelation::Related,
                strength: 1.0,
            })
            .collect();
        Ok(Some(InsightView { insight, evidence, signals, contexts }))
    }

    pub async fn set_insight_status(&self, id: &InsightId, status: InsightStatus) -> Result<Insight> {
        let mut insight = self
            .store
            .get("Insight", id.as_str())
            .await?
            .map(insight_from_value)
            .transpose()?
            .ok_or_else(|| anyhow!("unknown insight: {id}"))?;
        insight.status = status;
        insight.updated_at = Utc::now();
        self.store.update("Insight", insight.id.as_str(), insight_record(&insight)).await?;
        Ok(insight)
    }
}

/// Stable key of a subject, re-exported for clients that group signals.
pub fn subject_key(label: &str) -> String {
    slug(label)
}
