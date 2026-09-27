//! Reasoning over retrieved Stream knowledge.
//!
//! A [`Reasoner`] sees exactly one [`Bundle`] — the signals, evidence,
//! connections, contexts, timeline, and insights Stream retrieved for a
//! question — and proposes an answer. It has no access to storage, so an
//! answer cannot depend on hidden database state. [`verify_answer`] then
//! enforces that every statement cites only what is in the bundle, and that
//! factual and inferred statements cite evidence at all.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use stream_model::{
    ChangeKind, ClaimBasis, ClaimId, ClaimKind, ConnectionRelation, ConnectionTargetKind, ContextId, ContextKind,
    EvidenceId, InsightId, InsightKind, InsightStatus, ItemId, SignalId, SignalStatus, SourceId, Synthesis,
};
use stream_semantic::provider::{strip_code_fence, ModelError, ModelProvider, ModelRequest};
use stream_semantic::{stem, Rejection};

/// What the question is asking for. Detected deterministically; it shapes
/// retrieval (e.g. temporal questions pull the timeline) and local answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    WhyMatters,
    Evidence,
    Connections,
    Uncertainty,
    Changes,
    Evolution,
    Investigate,
    Patterns,
    Relatedness,
    Contradictions,
    General,
}

impl Intent {
    pub fn detect(question: &str) -> Intent {
        let q = question.to_lowercase();
        let has = |needles: &[&str]| needles.iter().any(|n| q.contains(n));
        if has(&["contradict", "disagree", "dispute", "conflict"]) {
            Intent::Contradictions
        } else if has(&["evidence", "support", "source says", "sources say", "prove", "why do you think", "why does stream think", "how do you know"]) {
            Intent::Evidence
        } else if has(&["uncertain", "unknown", "unclear", "don't know", "do not know", "not sure", "confidence", "doubt"]) {
            Intent::Uncertainty
        } else if has(&["investigate", "look into", "dig into", "follow up", "next step", "what should"]) {
            Intent::Investigate
        } else if has(&["evolve", "evolved", "over time", "before", "timeline", "history", "when did", "picture"]) {
            Intent::Evolution
        } else if has(&["actually related", "are these", "related to each other", "same thing", "same change"]) {
            Intent::Relatedness
        } else if has(&["pattern", "trend", "theme", "shift", "across"]) {
            Intent::Patterns
        } else if has(&["why does", "why do", "why is", "matter", "relevant", "important"]) {
            Intent::WhyMatters
        } else if has(&["connect", "relate", "building", "link", "tie"]) {
            Intent::Connections
        } else if has(&["changed", "what's new", "what is new", "recent", "lately", "this week", "this month", "today", "following"]) {
            Intent::Changes
        } else {
            Intent::General
        }
    }

    pub fn is_temporal(self) -> bool {
        matches!(self, Intent::Changes | Intent::Evolution)
    }
}

/// Words that shape intent but say nothing about the subject.
pub const QUESTION_WORDS: &[&str] = &[
    "what", "why", "how", "which", "who", "when", "where", "does", "do", "did", "is", "are", "was", "were", "this",
    "that", "these", "those", "it", "they", "them", "matter", "matters", "mattered", "connect", "connects",
    "connected", "connection", "connections", "evidence", "support", "supports", "supporting", "changed", "change",
    "changes", "uncertain", "uncertainty", "remain", "remains", "learn", "learned", "learnt", "know", "known",
    "investigate", "further", "should", "pattern", "patterns", "appearing", "across", "source", "sources", "thing",
    "things", "building", "build", "follow", "following", "month", "week", "today", "recent", "recently", "lately",
    "actually", "related", "relate", "understanding", "evolve", "evolved", "picture", "new", "believe", "believed",
    "before", "appeared", "become", "became", "relevant", "stream", "think", "conclusion", "me", "my", "we", "our",
    "us", "i", "you", "your", "about", "the", "a", "an", "of", "to", "in", "on", "for", "with", "and", "or", "be",
    "been", "have", "has", "had", "there", "here", "any", "all", "other", "else", "contradict", "contradicts", "work",
];

pub fn content_terms(question: &str) -> Vec<String> {
    question
        .split(|c: char| !(c.is_alphanumeric() || c == '-' || c == '.'))
        .map(|w| w.trim_matches(|c: char| c == '.' || c == '-').to_lowercase())
        .filter(|w| w.len() >= 2 && !QUESTION_WORDS.contains(&w.as_str()))
        .map(|w| stem(&w))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Scope {
    pub signal_ids: Vec<SignalId>,
    pub context_ids: Vec<ContextId>,
    pub item_ids: Vec<ItemId>,
    pub source_ids: Vec<SourceId>,
    /// True when retrieval was constrained to something the user pointed at.
    pub focused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleContext {
    pub id: ContextId,
    pub name: String,
    pub kind: ContextKind,
    pub description: String,
    pub related: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleConnection {
    pub target_kind: ConnectionTargetKind,
    pub target_id: String,
    pub label: String,
    pub relation: ConnectionRelation,
    pub strength: f32,
    pub rationale: String,
    pub evidence_ids: Vec<EvidenceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleClaim {
    pub id: ClaimId,
    pub basis: ClaimBasis,
    pub statement: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub context_ids: Vec<ContextId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleSignal {
    pub id: SignalId,
    pub topic: String,
    pub subject: String,
    pub change: String,
    pub change_kind: ChangeKind,
    pub why_it_matters: Option<String>,
    pub status: SignalStatus,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub source_count: usize,
    pub observation_count: usize,
    pub position: usize,
    pub connections: Vec<BundleConnection>,
    pub claims: Vec<BundleClaim>,
    pub synthesis: Option<Synthesis>,
    pub relevance: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleEvidence {
    pub id: EvidenceId,
    pub signal_id: SignalId,
    pub claim: ClaimKind,
    pub excerpt: String,
    pub item_id: ItemId,
    pub item_title: String,
    pub source_id: SourceId,
    pub source_label: String,
    pub url: String,
    pub observed_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub at: DateTime<Utc>,
    pub kind: String,
    pub description: String,
    pub signal_id: Option<SignalId>,
    pub source_id: Option<SourceId>,
    pub evidence_id: Option<EvidenceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleInsight {
    pub id: InsightId,
    pub kind: InsightKind,
    pub status: InsightStatus,
    pub statement: String,
    pub basis: ClaimBasis,
    pub signal_ids: Vec<SignalId>,
    pub evidence_ids: Vec<EvidenceId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelatedPair {
    pub a: SignalId,
    pub b: SignalId,
    /// same_change | contradicts | related | shared_context
    pub relation: String,
    pub rationale: String,
    pub evidence_ids: Vec<EvidenceId>,
}

/// Everything a reasoner may use to answer one question — and nothing else.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub question: String,
    pub intent: Intent,
    pub scope: Scope,
    pub time_window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub mentioned_contexts: Vec<ContextId>,
    pub contexts: Vec<BundleContext>,
    pub signals: Vec<BundleSignal>,
    pub evidence: Vec<BundleEvidence>,
    pub timeline: Vec<TimelineEntry>,
    pub insights: Vec<BundleInsight>,
    pub related_pairs: Vec<RelatedPair>,
}

impl Bundle {
    pub fn evidence_of<'a>(&'a self, signal: &'a SignalId) -> impl Iterator<Item = &'a BundleEvidence> + 'a {
        self.evidence.iter().filter(move |e| &e.signal_id == signal)
    }

    fn evidence_with(&self, signal: &SignalId, claims: &[ClaimKind]) -> Vec<EvidenceId> {
        let mut ids = Vec::new();
        for claim in claims {
            ids.extend(self.evidence_of(signal).filter(|e| e.claim == *claim).map(|e| e.id.clone()));
        }
        ids.dedup();
        ids
    }

    fn context(&self, id: &str) -> Option<&BundleContext> {
        self.contexts.iter().find(|c| c.id.as_str() == id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sufficiency {
    Sufficient,
    Partial,
    Insufficient,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub text: String,
    pub basis: ClaimBasis,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub signal_ids: Vec<SignalId>,
    #[serde(default)]
    pub context_ids: Vec<ContextId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnswerProposal {
    pub summary: String,
    pub sufficiency: Sufficiency,
    pub statements: Vec<Statement>,
    pub uncertainties: Vec<String>,
    pub follow_ups: Vec<String>,
}

#[async_trait]
pub trait Reasoner: Send + Sync {
    fn id(&self) -> &str;
    async fn answer(&self, bundle: &Bundle) -> Result<AnswerProposal>;
}

pub const INSUFFICIENT: &str = "I don't have enough evidence in Stream to establish that yet.";

/// The answer gate. Statements may cite only evidence, signals, and contexts
/// that are in the bundle. Observed, inferred, and connected statements must
/// cite evidence; hypotheses may not, but stay labelled as hypotheses.
/// An answer with no supported statements is an explicit "not enough evidence".
pub fn verify_answer(proposal: AnswerProposal, bundle: &Bundle) -> (AnswerProposal, Vec<Rejection>) {
    let evidence = bundle.evidence.iter().map(|e| &e.id).collect::<BTreeSet<_>>();
    let signals = bundle.signals.iter().map(|s| &s.id).collect::<BTreeSet<_>>();
    let contexts = bundle.contexts.iter().map(|c| &c.id).collect::<BTreeSet<_>>();
    let mut rejections = Vec::new();
    let mut statements = Vec::new();
    for mut statement in proposal.statements.into_iter().take(12) {
        let cited = statement.evidence_ids.len() + statement.signal_ids.len() + statement.context_ids.len();
        statement.evidence_ids.retain(|id| evidence.contains(id));
        statement.signal_ids.retain(|id| signals.contains(id));
        statement.context_ids.retain(|id| contexts.contains(id));
        if statement.evidence_ids.len() + statement.signal_ids.len() + statement.context_ids.len() != cited {
            rejections.push(Rejection { claim: "answer".into(), reason: "cited something Stream did not retrieve".into() });
        }
        if statement.text.trim().is_empty() {
            continue;
        }
        if statement.basis.requires_evidence() && statement.evidence_ids.is_empty() {
            rejections.push(Rejection {
                claim: format!("answer:{}", statement.basis),
                reason: "statement is not supported by any retrieved evidence".into(),
            });
            continue;
        }
        // Signals follow from the evidence cited.
        for id in &statement.evidence_ids {
            if let Some(e) = bundle.evidence.iter().find(|e| &e.id == id) {
                if !statement.signal_ids.contains(&e.signal_id) {
                    statement.signal_ids.push(e.signal_id.clone());
                }
            }
        }
        statements.push(statement);
    }

    let grounded = statements.iter().filter(|s| s.basis.requires_evidence()).count();
    let mut uncertainties = proposal.uncertainties;
    let (summary, sufficiency) = if statements.is_empty() {
        (INSUFFICIENT.to_owned(), Sufficiency::Insufficient)
    } else if grounded == 0 {
        uncertainties.insert(0, "Everything above is a hypothesis; no retrieved source states it.".into());
        (proposal.summary, Sufficiency::Partial)
    } else {
        (proposal.summary, proposal.sufficiency)
    };
    if !rejections.is_empty() {
        uncertainties.push(format!(
            "{} proposed statement(s) were withheld because Stream's evidence does not support them.",
            rejections.iter().filter(|r| r.claim.starts_with("answer:")).count().max(1)
        ));
    }
    (
        AnswerProposal {
            summary: if summary.trim().is_empty() { INSUFFICIENT.into() } else { summary },
            sufficiency,
            statements,
            uncertainties,
            follow_ups: proposal.follow_ups.into_iter().take(5).collect(),
        },
        rejections,
    )
}

fn statement(text: String, basis: ClaimBasis, evidence_ids: Vec<EvidenceId>, signal: &SignalId, contexts: Vec<ContextId>) -> Statement {
    Statement { text, basis, evidence_ids, signal_ids: vec![signal.clone()], context_ids: contexts }
}

fn lower_first(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn date(at: DateTime<Utc>) -> String {
    at.format("%b %-d, %Y").to_string()
}

/// A deterministic reasoner that composes answers directly from the bundle.
/// It never says more than the bundle supports.
#[derive(Debug, Default, Clone)]
pub struct LocalReasoner;

impl LocalReasoner {
    pub const ID: &'static str = "stream.reason.local.v1";

    fn focus_contexts<'a>(bundle: &'a Bundle, signal: &'a BundleSignal) -> Vec<&'a BundleConnection> {
        signal
            .connections
            .iter()
            .filter(|c| matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project))
            .filter(|c| bundle.mentioned_contexts.is_empty() || bundle.mentioned_contexts.iter().any(|m| m.as_str() == c.target_id))
            .collect()
    }

    fn observed(bundle: &Bundle, signal: &BundleSignal) -> Option<Statement> {
        let evidence = bundle.evidence_with(&signal.id, &[ClaimKind::Change, ClaimKind::Corroboration, ClaimKind::Subject]);
        (!evidence.is_empty()).then(|| {
            let sources = if signal.source_count > 1 { format!(" ({} sources)", signal.source_count) } else { String::new() };
            statement(
                format!("{}: {}{}.", signal.subject, lower_first(&signal.change), sources),
                ClaimBasis::Observed,
                evidence.into_iter().take(3).collect(),
                &signal.id,
                vec![],
            )
        })
    }

    fn connected(bundle: &Bundle, signal: &BundleSignal) -> Vec<Statement> {
        Self::focus_contexts(bundle, signal)
            .into_iter()
            .filter(|c| !c.evidence_ids.is_empty())
            .map(|c| {
                let text = if c.relation == ConnectionRelation::Via {
                    format!("{} connects to {} only indirectly: {}.", signal.subject, c.label, c.rationale.trim_end_matches('.'))
                } else {
                    format!("{} connects to {}: the source {}.", signal.subject, c.label, c.rationale.trim_end_matches('.'))
                };
                statement(text, ClaimBasis::Connected, c.evidence_ids.clone(), &signal.id, vec![ContextId::new(c.target_id.clone())])
            })
            .collect()
    }

    fn why(bundle: &Bundle, signal: &BundleSignal) -> Option<Statement> {
        let why = signal.why_it_matters.as_ref()?;
        let evidence = bundle.evidence_with(&signal.id, &[ClaimKind::WhyItMatters, ClaimKind::Connection]);
        (!evidence.is_empty()).then(|| statement(why.clone(), ClaimBasis::Inferred, evidence.into_iter().take(3).collect(), &signal.id, vec![]))
    }

    fn uncertainties(bundle: &Bundle, signal: &BundleSignal) -> Vec<Statement> {
        let mut out = Vec::new();
        if let Some(synthesis) = &signal.synthesis {
            for point in synthesis.differences.iter().chain(synthesis.uncertainties.iter()) {
                out.push(statement(point.statement.clone(), point.basis, point.evidence_ids.clone(), &signal.id, vec![]));
            }
        } else if signal.source_count <= 1 {
            let evidence = bundle.evidence_with(&signal.id, &[ClaimKind::Change]);
            if !evidence.is_empty() {
                out.push(statement(
                    format!("Only one source reports that {} {}; it has not been independently corroborated.", signal.subject, lower_first(&signal.change)),
                    ClaimBasis::Inferred,
                    evidence,
                    &signal.id,
                    vec![],
                ));
            }
        }
        for claim in signal.claims.iter().filter(|c| matches!(c.basis, ClaimBasis::Inferred | ClaimBasis::Hypothesis)) {
            let text = match claim.basis {
                ClaimBasis::Hypothesis => format!("Unverified hypothesis: {}", claim.statement),
                _ => format!("Stream's inference, not stated by a source: {}", claim.statement),
            };
            out.push(statement(text, claim.basis, claim.evidence_ids.clone(), &signal.id, claim.context_ids.clone()));
        }
        for connection in signal.connections.iter().filter(|c| {
            matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project) && c.strength < 0.5 && !c.evidence_ids.is_empty()
        }) {
            out.push(statement(
                format!("The link between {} and {} is weak (strength {:.2}).", signal.subject, connection.label, connection.strength),
                ClaimBasis::Inferred,
                connection.evidence_ids.clone(),
                &signal.id,
                vec![ContextId::new(connection.target_id.clone())],
            ));
        }
        out
    }

    pub fn compose(bundle: &Bundle) -> AnswerProposal {
        let follow = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        if bundle.signals.is_empty() {
            let mut follow_ups = vec!["Add a URL about this so Stream can observe it.".to_owned()];
            if bundle.contexts.is_empty() {
                follow_ups.push("Tell Stream what you care about with + Context.".into());
            }
            return AnswerProposal {
                summary: INSUFFICIENT.into(),
                sufficiency: Sufficiency::Insufficient,
                statements: vec![],
                uncertainties: vec!["Nothing Stream has observed matches this question.".into()],
                follow_ups,
            };
        }
        let top = bundle.signals.iter().take(4).collect::<Vec<_>>();
        let lead = top[0];
        let mut statements = Vec::new();
        let mut uncertainties = Vec::new();
        let mut sufficiency = Sufficiency::Sufficient;
        let context_names = bundle
            .mentioned_contexts
            .iter()
            .filter_map(|id| bundle.context(id.as_str()))
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let about = if context_names.is_empty() { "this".to_owned() } else { context_names.join(" and ") };

        let (summary, follow_ups) = match bundle.intent {
            Intent::WhyMatters => {
                for signal in &top {
                    statements.extend(Self::observed(bundle, signal));
                    statements.extend(Self::connected(bundle, signal));
                    statements.extend(Self::why(bundle, signal));
                }
                if statements.iter().all(|s| s.basis == ClaimBasis::Observed) {
                    sufficiency = Sufficiency::Partial;
                    uncertainties.push(format!("Stream has observed these, but none is connected to {about} by evidence yet."));
                }
                (
                    format!(
                        "{} bear{} on {}. The strongest is {}: {}.",
                        if top.len() == 1 { "One signal".to_owned() } else { format!("{} signals", top.len()) },
                        if top.len() == 1 { "s" } else { "" },
                        about,
                        lead.subject,
                        lower_first(&lead.change)
                    ),
                    follow(&["What evidence supports that?", "What remains uncertain?", "How does this connect to the other things I'm building?"]),
                )
            }
            Intent::Evidence => {
                for signal in &top {
                    for e in bundle
                        .evidence_of(&signal.id)
                        .filter(|e| matches!(e.claim, ClaimKind::Change | ClaimKind::Corroboration | ClaimKind::Contradiction | ClaimKind::Statement | ClaimKind::WhyItMatters | ClaimKind::Connection))
                    {
                        if statements.len() >= 8 || statements.iter().any(|s: &Statement| s.text.contains(&e.excerpt)) {
                            continue;
                        }
                        let verb = if e.claim == ClaimKind::Contradiction { "disputes it" } else { "says" };
                        statements.push(statement(
                            format!("{} {verb}: “{}”", e.source_label, e.excerpt),
                            ClaimBasis::Observed,
                            vec![e.id.clone()],
                            &signal.id,
                            vec![],
                        ));
                    }
                }
                let sources = bundle.evidence.iter().map(|e| &e.source_id).collect::<BTreeSet<_>>().len();
                (
                    format!("Here is what the sources themselves say — {} excerpt(s) from {} source(s).", statements.len(), sources),
                    follow(&["What remains uncertain?", "What contradicts this?"]),
                )
            }
            Intent::Connections => {
                for signal in &top {
                    for c in signal.connections.iter().filter(|c| {
                        matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project) && !c.evidence_ids.is_empty()
                    }) {
                        let text = match c.relation {
                            ConnectionRelation::Via => format!("{} reaches {} only through a context you related to it.", signal.subject, c.label),
                            _ => format!("{} connects to {} ({}): {}.", signal.subject, c.label, c.target_kind, c.rationale.trim_end_matches('.')),
                        };
                        statements.push(statement(text, ClaimBasis::Connected, c.evidence_ids.clone(), &signal.id, vec![ContextId::new(c.target_id.clone())]));
                    }
                }
                for pair in &bundle.related_pairs {
                    if !pair.evidence_ids.is_empty() {
                        statements.push(Statement {
                            text: pair.rationale.clone(),
                            basis: ClaimBasis::Connected,
                            evidence_ids: pair.evidence_ids.clone(),
                            signal_ids: vec![pair.a.clone(), pair.b.clone()],
                            context_ids: vec![],
                        });
                    }
                }
                let projects = bundle.contexts.iter().filter(|c| c.kind == ContextKind::Project).map(|c| c.name.clone()).collect::<Vec<_>>();
                if statements.is_empty() {
                    sufficiency = Sufficiency::Insufficient;
                }
                (
                    if projects.is_empty() {
                        "These are the connections Stream can support with evidence.".to_owned()
                    } else {
                        format!("Here is how this connects to what you're building ({}).", projects.join(", "))
                    },
                    follow(&["Are these actually related?", "What evidence supports that?"]),
                )
            }
            Intent::Uncertainty | Intent::Contradictions => {
                for signal in &top {
                    let mut items = Self::uncertainties(bundle, signal);
                    if bundle.intent == Intent::Contradictions {
                        items.retain(|s| s.text.contains("disput") || s.text.contains("disagree"));
                    }
                    statements.extend(items);
                }
                if statements.is_empty() {
                    if bundle.intent == Intent::Contradictions {
                        sufficiency = Sufficiency::Partial;
                        uncertainties.push("No source Stream has observed disputes this.".into());
                    } else {
                        uncertainties.push("Stream has no recorded uncertainty here, which is not the same as certainty.".into());
                        sufficiency = Sufficiency::Partial;
                    }
                }
                (
                    if bundle.intent == Intent::Contradictions {
                        "Where the sources disagree:".to_owned()
                    } else {
                        "What remains uncertain, and why:".to_owned()
                    },
                    follow(&["What should I investigate further?", "What evidence supports that?"]),
                )
            }
            Intent::Changes => {
                let mut ordered = top.clone();
                ordered.sort_by(|a, b| b.last_observed_at.cmp(&a.last_observed_at));
                for signal in ordered {
                    let evidence = bundle.evidence_with(&signal.id, &[ClaimKind::Change, ClaimKind::Corroboration]);
                    if !evidence.is_empty() {
                        statements.push(statement(
                            format!("{} — {}: {}.", date(signal.last_observed_at), signal.subject, lower_first(&signal.change)),
                            ClaimBasis::Observed,
                            evidence.into_iter().take(2).collect(),
                            &signal.id,
                            vec![],
                        ));
                    }
                }
                let window = bundle
                    .time_window
                    .map(|(from, _)| format!(" since {}", date(from)))
                    .unwrap_or_default();
                (format!("{} change(s) observed{}.", statements.len(), window), follow(&["Why does this matter to me?", "What patterns are appearing across these sources?"]))
            }
            Intent::Evolution => {
                let mut entries = bundle.timeline.iter().filter(|t| t.evidence_id.is_some() || t.kind == "context_added").collect::<Vec<_>>();
                entries.sort_by(|a, b| a.at.cmp(&b.at));
                for entry in entries.into_iter().take(10) {
                    match (&entry.evidence_id, &entry.signal_id) {
                        (Some(evidence), Some(signal)) => statements.push(statement(
                            format!("{} — {}", date(entry.at), entry.description),
                            ClaimBasis::Observed,
                            vec![evidence.clone()],
                            signal,
                            vec![],
                        )),
                        _ => statements.push(Statement {
                            text: format!("{} — {} (Stream evaluates everything against it from then on).", date(entry.at), entry.description),
                            basis: ClaimBasis::Hypothesis,
                            evidence_ids: vec![],
                            signal_ids: vec![],
                            context_ids: vec![],
                        }),
                    }
                }
                (
                    format!("How Stream's picture of {} formed over time:", if about == "this" { lead.subject.clone() } else { about.clone() }),
                    follow(&["What new evidence changed the picture?", "What remains uncertain?"]),
                )
            }
            Intent::Investigate => {
                for signal in &top {
                    for claim in signal.claims.iter().filter(|c| c.basis == ClaimBasis::Hypothesis) {
                        statements.push(statement(format!("Investigate whether {}", lower_first(&claim.statement)), ClaimBasis::Hypothesis, claim.evidence_ids.clone(), &signal.id, claim.context_ids.clone()));
                    }
                    for item in Self::uncertainties(bundle, signal).into_iter().take(2) {
                        statements.push(Statement { text: format!("Resolve: {}", item.text), ..item });
                    }
                }
                for insight in bundle.insights.iter().filter(|i| i.status == InsightStatus::Open && i.kind.is_open_question()) {
                    statements.push(Statement {
                        text: format!("Open {}: {}", insight.kind.as_str().replace('_', " "), insight.statement),
                        basis: insight.basis,
                        evidence_ids: insight.evidence_ids.clone(),
                        signal_ids: insight.signal_ids.clone(),
                        context_ids: vec![],
                    });
                }
                ("Worth investigating next, based on what Stream cannot yet establish:".to_owned(), follow(&["What evidence supports that?"]))
            }
            Intent::Patterns => {
                let mut by_context: BTreeMap<String, (String, Vec<&BundleSignal>, Vec<EvidenceId>)> = BTreeMap::new();
                for signal in &bundle.signals {
                    for c in signal.connections.iter().filter(|c| {
                        matches!(c.target_kind, ConnectionTargetKind::Context | ConnectionTargetKind::Project)
                            && c.relation == ConnectionRelation::Matches
                    }) {
                        let entry = by_context.entry(c.target_id.clone()).or_insert_with(|| (c.label.clone(), vec![], vec![]));
                        entry.1.push(signal);
                        entry.2.extend(c.evidence_ids.iter().take(1).cloned());
                    }
                }
                for (id, (label, signals, evidence)) in by_context.into_iter().filter(|(_, v)| v.1.len() >= 2) {
                    let subjects = signals.iter().map(|s| s.subject.clone()).collect::<Vec<_>>();
                    statements.push(Statement {
                        text: format!("{} separate developments connect to {}: {}.", signals.len(), label, subjects.join(", ")),
                        basis: ClaimBasis::Connected,
                        evidence_ids: evidence,
                        signal_ids: signals.iter().map(|s| s.id.clone()).collect(),
                        context_ids: vec![ContextId::new(id)],
                    });
                }
                let mut by_topic: BTreeMap<&str, Vec<&BundleSignal>> = BTreeMap::new();
                for signal in &bundle.signals {
                    by_topic.entry(signal.topic.as_str()).or_default().push(signal);
                }
                for (topic, signals) in by_topic.into_iter().filter(|(_, v)| v.len() >= 2) {
                    let evidence = signals.iter().flat_map(|s| bundle.evidence_with(&s.id, &[ClaimKind::Change]).into_iter().take(1)).collect::<Vec<_>>();
                    statements.push(Statement {
                        text: format!("Activity is clustering in {}: {} signals.", topic, signals.len()),
                        basis: ClaimBasis::Inferred,
                        evidence_ids: evidence,
                        signal_ids: signals.iter().map(|s| s.id.clone()).collect(),
                        context_ids: vec![],
                    });
                }
                if statements.is_empty() {
                    sufficiency = Sufficiency::Insufficient;
                    uncertainties.push("A pattern needs at least two related signals; Stream doesn't have them yet.".into());
                }
                ("Patterns Stream can support with evidence:".to_owned(), follow(&["Are these actually related?", "What should I investigate further?"]))
            }
            Intent::Relatedness => {
                let ids = top.iter().map(|s| &s.id).collect::<Vec<_>>();
                for (i, a) in ids.iter().enumerate() {
                    for b in ids.iter().skip(i + 1) {
                        let pair = bundle.related_pairs.iter().find(|p| (&&p.a == a && &&p.b == b) || (&&p.a == b && &&p.b == a));
                        let subject = |id: &SignalId| bundle.signals.iter().find(|s| &s.id == id).map(|s| s.subject.clone()).unwrap_or_default();
                        match pair {
                            Some(pair) if !pair.evidence_ids.is_empty() => statements.push(Statement {
                                text: pair.rationale.clone(),
                                basis: if pair.relation == "shared_context" { ClaimBasis::Inferred } else { ClaimBasis::Connected },
                                evidence_ids: pair.evidence_ids.clone(),
                                signal_ids: vec![(*a).clone(), (*b).clone()],
                                context_ids: vec![],
                            }),
                            _ => {
                                sufficiency = Sufficiency::Partial;
                                uncertainties.push(format!("Nothing Stream has observed connects {} and {} directly.", subject(a), subject(b)));
                            }
                        }
                    }
                }
                if top.len() < 2 {
                    sufficiency = Sufficiency::Partial;
                    uncertainties.push("Point Stream at two or more signals to compare them.".into());
                }
                ("Whether these are actually related:".to_owned(), follow(&["What evidence supports that?"]))
            }
            Intent::General => {
                for signal in &top {
                    statements.extend(Self::observed(bundle, signal));
                    statements.extend(Self::connected(bundle, signal).into_iter().take(1));
                    statements.extend(Self::why(bundle, signal));
                }
                (
                    format!(
                        "Stream has {} relevant signal{}{}.",
                        top.len(),
                        if top.len() == 1 { "" } else { "s" },
                        if context_names.is_empty() { String::new() } else { format!(" about {}", about) }
                    ),
                    follow(&["Why does this matter to me?", "What evidence supports that?", "What remains uncertain?"]),
                )
            }
        };
        // The local reasoner never proposes what it cannot support.
        statements.retain(|s| !s.basis.requires_evidence() || !s.evidence_ids.is_empty());
        let mut seen = BTreeSet::new();
        statements.retain(|s| seen.insert(s.text.clone()));
        if statements.is_empty() && sufficiency == Sufficiency::Sufficient {
            sufficiency = Sufficiency::Insufficient;
        }
        AnswerProposal { summary, sufficiency, statements, uncertainties, follow_ups }
    }
}

#[async_trait]
impl Reasoner for LocalReasoner {
    fn id(&self) -> &str {
        Self::ID
    }

    async fn answer(&self, bundle: &Bundle) -> Result<AnswerProposal> {
        Ok(Self::compose(bundle))
    }
}

const REASON_SYSTEM: &str = "You are Stream's reasoning layer. Answer the user's question using ONLY the Stream \
knowledge in the input: signals, evidence excerpts (with ids), connections, contexts, timeline, and saved insights. \
You are not a general-purpose assistant; if the input does not contain enough to answer, say so with \
sufficiency=insufficient and no statements.

Rules:
1. Every statement has a basis: observed = what a cited source excerpt states; inferred = your conclusion from \
cited evidence; connected = a relationship to the user's contexts or between signals, citing evidence; \
hypothesis = a possibility worth investigating (may cite no evidence).
2. Cite evidence by the exact evidence ids given. Never invent ids, sources, facts, or quotes.
3. Never present an inference or hypothesis as an established fact. Do not overstate confidence.
4. List what remains uncertain. Suggest at most three short follow-up questions.
5. summary is one or two sentences that only restate supported statements.";

fn answer_schema() -> serde_json::Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["summary", "sufficiency", "statements", "uncertainties", "follow_ups"],
        "properties": {
            "summary": { "type": "string" },
            "sufficiency": { "type": "string", "enum": ["sufficient", "partial", "insufficient"] },
            "statements": { "type": "array", "items": {
                "type": "object", "additionalProperties": false,
                "required": ["text", "basis", "evidence_ids", "signal_ids", "context_ids"],
                "properties": {
                    "text": { "type": "string" },
                    "basis": { "type": "string", "enum": ["observed", "inferred", "connected", "hypothesis"] },
                    "evidence_ids": { "type": "array", "items": { "type": "string" } },
                    "signal_ids": { "type": "array", "items": { "type": "string" } },
                    "context_ids": { "type": "array", "items": { "type": "string" } }
                } } },
            "uncertainties": { "type": "array", "items": { "type": "string" } },
            "follow_ups": { "type": "array", "items": { "type": "string" } }
        }
    })
}

/// A compact, id-addressed rendering of the bundle for a model.
pub fn bundle_prompt(bundle: &Bundle) -> String {
    let payload = json!({
        "question": bundle.question,
        "scope": if bundle.scope.focused { "the user is asking about specific signals" } else { "all of Stream" },
        "time_window": bundle.time_window,
        "contexts": bundle.contexts.iter().map(|c| json!({
            "id": c.id, "name": c.name, "kind": c.kind, "description": c.description, "related_to": c.related
        })).collect::<Vec<_>>(),
        "signals": bundle.signals.iter().map(|s| json!({
            "id": s.id, "topic": s.topic, "subject": s.subject, "change": s.change,
            "why_it_matters (inferred)": s.why_it_matters, "status": s.status,
            "first_observed_at": s.first_observed_at, "last_observed_at": s.last_observed_at,
            "sources": s.source_count,
            "connections": s.connections.iter().map(|c| json!({
                "to": c.label, "kind": c.target_kind, "relation": c.relation, "strength": c.strength,
                "explanation": c.rationale, "evidence_ids": c.evidence_ids
            })).collect::<Vec<_>>(),
            "claims": s.claims.iter().map(|c| json!({ "basis": c.basis, "statement": c.statement, "evidence_ids": c.evidence_ids })).collect::<Vec<_>>(),
            "synthesis": s.synthesis,
        })).collect::<Vec<_>>(),
        "evidence": bundle.evidence.iter().map(|e| json!({
            "id": e.id, "signal_id": e.signal_id, "supports": e.claim, "excerpt": e.excerpt,
            "source": e.source_label, "observed_at": e.observed_at
        })).collect::<Vec<_>>(),
        "timeline": bundle.timeline.iter().take(30).map(|t| json!({ "at": t.at, "event": t.description, "evidence_id": t.evidence_id })).collect::<Vec<_>>(),
        "saved_insights": bundle.insights.iter().map(|i| json!({ "kind": i.kind, "status": i.status, "statement": i.statement, "basis": i.basis })).collect::<Vec<_>>(),
        "related_signal_pairs": bundle.related_pairs,
    });
    serde_json::to_string_pretty(&payload).unwrap_or_default()
}

/// Model-backed reasoning over the same bundle, behind the same gate.
pub struct ModelReasoner {
    provider: Arc<dyn ModelProvider>,
    id: String,
}

impl ModelReasoner {
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        let id = format!("stream.reason.model.v1+{}", provider.id());
        Self { provider, id }
    }

    pub fn request(bundle: &Bundle) -> ModelRequest {
        ModelRequest {
            system: REASON_SYSTEM.into(),
            user: format!("Stream knowledge retrieved for this question:\n{}", bundle_prompt(bundle)),
            schema_name: "stream_answer".into(),
            schema: answer_schema(),
            max_tokens: 1_800,
        }
    }
}

#[async_trait]
impl Reasoner for ModelReasoner {
    fn id(&self) -> &str {
        &self.id
    }

    async fn answer(&self, bundle: &Bundle) -> Result<AnswerProposal> {
        if bundle.signals.is_empty() && bundle.insights.is_empty() {
            // Nothing retrieved: no model call can add grounded knowledge.
            return Ok(LocalReasoner::compose(bundle));
        }
        let raw = self.provider.complete_json(&Self::request(bundle)).await?;
        let proposal: AnswerProposal =
            serde_json::from_str(strip_code_fence(&raw)).map_err(|error| ModelError::Malformed(error.to_string()))?;
        Ok(proposal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle() -> Bundle {
        let signal = SignalId::new("signal_1");
        let now = Utc::now();
        Bundle {
            question: "Why does this matter to portable compute?".into(),
            intent: Intent::WhyMatters,
            scope: Scope::default(),
            time_window: None,
            mentioned_contexts: vec![ContextId::new("context_pc")],
            contexts: vec![BundleContext {
                id: ContextId::new("context_pc"),
                name: "Portable compute".into(),
                kind: ContextKind::Interest,
                description: String::new(),
                related: vec![],
                created_at: now,
            }],
            signals: vec![BundleSignal {
                id: signal.clone(),
                topic: "Compute".into(),
                subject: "Apple Container".into(),
                change: "Adds portable Linux VMs".into(),
                change_kind: ChangeKind::Adds,
                why_it_matters: Some("It may simplify portable runtimes.".into()),
                status: SignalStatus::Open,
                first_observed_at: now,
                last_observed_at: now,
                source_count: 1,
                observation_count: 1,
                position: 1,
                connections: vec![BundleConnection {
                    target_kind: ConnectionTargetKind::Context,
                    target_id: "context_pc".into(),
                    label: "Portable compute".into(),
                    relation: ConnectionRelation::Matches,
                    strength: 0.8,
                    rationale: "mentions “portable”".into(),
                    evidence_ids: vec![EvidenceId::new("e_conn")],
                }],
                claims: vec![],
                synthesis: None,
                relevance: 1.0,
                reasons: vec![],
            }],
            evidence: ["e_change", "e_conn", "e_why"]
                .iter()
                .zip([ClaimKind::Change, ClaimKind::Connection, ClaimKind::WhyItMatters])
                .map(|(id, claim)| BundleEvidence {
                    id: EvidenceId::new(*id),
                    signal_id: signal.clone(),
                    claim,
                    excerpt: "Apple Container adds portable Linux VMs".into(),
                    item_id: ItemId::new("item_1"),
                    item_title: "Apple Container adds portable Linux VMs".into(),
                    source_id: SourceId::new("source_1"),
                    source_label: "news.example".into(),
                    url: "https://news.example/a".into(),
                    observed_at: now,
                    published_at: None,
                })
                .collect(),
            timeline: vec![],
            insights: vec![],
            related_pairs: vec![],
        }
    }

    #[test]
    fn intents_are_detected() {
        assert_eq!(Intent::detect("Why does this matter to AppPort?"), Intent::WhyMatters);
        assert_eq!(Intent::detect("What evidence supports that?"), Intent::Evidence);
        assert_eq!(Intent::detect("How does this connect to the other things I'm building?"), Intent::Connections);
        assert_eq!(Intent::detect("What remains uncertain?"), Intent::Uncertainty);
        assert_eq!(Intent::detect("What changed in the things I've been following this month?"), Intent::Changes);
        assert_eq!(Intent::detect("How has our understanding of this topic evolved?"), Intent::Evolution);
        assert_eq!(Intent::detect("What should I investigate further?"), Intent::Investigate);
        assert_eq!(Intent::detect("What patterns are appearing across these sources?"), Intent::Patterns);
        assert_eq!(Intent::detect("Are these three things actually related?"), Intent::Relatedness);
        assert_eq!(Intent::detect("What have we learned about portable compute?"), Intent::General);
        assert_eq!(content_terms("What have we learned about portable compute?"), vec!["compute", "portable"]);
    }

    #[test]
    fn local_answers_cite_evidence_for_every_grounded_statement() {
        let (answer, rejections) = verify_answer(LocalReasoner::compose(&bundle()), &bundle());
        assert!(rejections.is_empty(), "{rejections:?}");
        assert_eq!(answer.sufficiency, Sufficiency::Sufficient);
        let bases = answer.statements.iter().map(|s| s.basis).collect::<Vec<_>>();
        assert_eq!(bases, vec![ClaimBasis::Observed, ClaimBasis::Connected, ClaimBasis::Inferred]);
        assert!(answer.statements.iter().all(|s| !s.evidence_ids.is_empty()));
    }

    #[test]
    fn an_empty_bundle_is_an_explicit_insufficient_evidence_answer() {
        let mut empty = bundle();
        empty.signals.clear();
        empty.evidence.clear();
        let (answer, _) = verify_answer(LocalReasoner::compose(&empty), &empty);
        assert_eq!(answer.sufficiency, Sufficiency::Insufficient);
        assert_eq!(answer.summary, INSUFFICIENT);
        assert!(answer.statements.is_empty());
    }

    #[test]
    fn the_answer_gate_refuses_statements_beyond_the_bundle() {
        let proposal = AnswerProposal {
            summary: "Apple will acquire Docker.".into(),
            sufficiency: Sufficiency::Sufficient,
            statements: vec![
                Statement { text: "Apple will acquire Docker.".into(), basis: ClaimBasis::Observed, evidence_ids: vec![EvidenceId::new("evidence_made_up")], signal_ids: vec![], context_ids: vec![] },
                Statement { text: "This is certainly transformative.".into(), basis: ClaimBasis::Inferred, evidence_ids: vec![], signal_ids: vec![], context_ids: vec![] },
            ],
            uncertainties: vec![],
            follow_ups: vec![],
        };
        let (answer, rejections) = verify_answer(proposal, &bundle());
        assert!(answer.statements.is_empty());
        assert_eq!(answer.sufficiency, Sufficiency::Insufficient);
        assert_eq!(answer.summary, INSUFFICIENT, "an unsupported summary is not kept");
        assert!(rejections.len() >= 2);
    }

    #[test]
    fn hypotheses_alone_are_only_partial_answers() {
        let proposal = AnswerProposal {
            summary: "Maybe the runtime layer can go.".into(),
            sufficiency: Sufficiency::Sufficient,
            statements: vec![Statement { text: "The custom runtime could be removed.".into(), basis: ClaimBasis::Hypothesis, evidence_ids: vec![], signal_ids: vec![], context_ids: vec![] }],
            uncertainties: vec![],
            follow_ups: vec![],
        };
        let (answer, _) = verify_answer(proposal, &bundle());
        assert_eq!(answer.sufficiency, Sufficiency::Partial);
        assert!(answer.uncertainties[0].contains("hypothesis"));
    }
}
