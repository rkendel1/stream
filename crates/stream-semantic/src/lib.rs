//! Stream's semantic boundary.
//!
//! An [`Interpreter`] reads durable items and the user's durable context and
//! *proposes* an [`Interpretation`]: topic, subject, change, why it matters,
//! and connections. It has no access to storage and cannot mutate authority.
//!
//! Every proposal passes through [`verify`] before Stream persists anything:
//! a claim survives only if its excerpts are verbatim in the underlying item.
//! That is what keeps interpretation traceable back to evidence, whichever
//! model or provider produced it.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use stream_model::{
    normalize_whitespace, slug, Change, ChangeKind, ContextEntry, ContextId, EvidenceLocator, Item, ItemId,
    SignalId, Subject, Topic,
};

mod heuristic;
mod text;

pub use heuristic::HeuristicInterpreter;
pub use text::{key_terms, sentences, stem};

/// An earlier item Stream already understands, offered as connection candidates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorItem {
    pub item_id: ItemId,
    pub signal_id: Option<SignalId>,
    pub title: String,
    pub content_text: String,
    pub subject: Option<String>,
}

pub struct InterpretationInput<'a> {
    pub item: &'a Item,
    pub contexts: &'a [ContextEntry],
    pub prior: &'a [PriorItem],
}

/// A verbatim quote from the item that supports a claim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Excerpt {
    pub locator: EvidenceLocator,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Claim<T> {
    pub value: T,
    pub excerpts: Vec<Excerpt>,
    pub confidence: f32,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextMatch {
    pub context_id: ContextId,
    pub matched_terms: Vec<String>,
    pub strength: f32,
    pub excerpts: Vec<Excerpt>,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkKind {
    /// The two items describe the same underlying change.
    SameChange,
    /// The two items are about related things.
    Related,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemLink {
    pub item_id: ItemId,
    pub kind: LinkKind,
    pub strength: f32,
    pub shared_terms: Vec<String>,
    pub excerpts: Vec<Excerpt>,
    pub rationale: String,
}

/// An advisory proposal. Nothing here is authoritative until verified and
/// persisted through the normal Stream/FeltDB path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Interpretation {
    pub topic: Claim<Topic>,
    pub subject: Claim<Subject>,
    pub change: Claim<Change>,
    pub context_matches: Vec<ContextMatch>,
    pub why_it_matters: Option<Claim<String>>,
    pub item_links: Vec<ItemLink>,
}

/// A replaceable semantic provider. Presentation layers never learn which
/// implementation produced an interpretation.
#[async_trait]
pub trait Interpreter: Send + Sync {
    /// Stable identifier recorded with advisory decisions for audit.
    fn id(&self) -> &str;

    async fn interpret(&self, input: &InterpretationInput<'_>) -> Result<Interpretation>;
}

/// Why part of a proposal was refused by the evidence gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rejection {
    pub claim: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Verified {
    pub interpretation: Interpretation,
    pub rejections: Vec<Rejection>,
}

fn located_text<'a>(item: &'a Item, locator: EvidenceLocator) -> std::borrow::Cow<'a, str> {
    match locator {
        EvidenceLocator::Title => std::borrow::Cow::Borrowed(item.title.as_str()),
        EvidenceLocator::Content => std::borrow::Cow::Borrowed(item.content_text.as_str()),
        EvidenceLocator::Url => std::borrow::Cow::Owned(
            item.canonical_url.as_ref().map(|url| url.to_string()).unwrap_or_default(),
        ),
    }
}

/// Is this excerpt actually present in the item where it claims to be?
pub fn excerpt_is_grounded(item: &Item, excerpt: &Excerpt) -> bool {
    let needle = normalize_whitespace(&excerpt.text).to_lowercase();
    if needle.len() < 2 {
        return false;
    }
    normalize_whitespace(&located_text(item, excerpt.locator))
        .to_lowercase()
        .contains(&needle)
}

fn grounded(item: &Item, excerpts: &[Excerpt]) -> Vec<Excerpt> {
    excerpts.iter().filter(|excerpt| excerpt_is_grounded(item, excerpt)).cloned().collect()
}

/// The evidence gate between advisory interpretation and durable state.
///
/// * topic, subject, and change must each keep at least one grounded excerpt,
///   otherwise the whole interpretation is refused;
/// * context matches must name a known context and keep grounded excerpts;
/// * why-it-matters survives only when a verified context match supports it;
/// * item links must point at offered prior items.
pub fn verify(
    interpretation: Interpretation,
    item: &Item,
    contexts: &[ContextEntry],
    prior: &[PriorItem],
) -> std::result::Result<Verified, Vec<Rejection>> {
    let mut rejections = Vec::new();
    let mut out = interpretation;

    macro_rules! require {
        ($claim:expr, $name:literal) => {{
            $claim.excerpts = grounded(item, &$claim.excerpts);
            if $claim.excerpts.is_empty() {
                rejections.push(Rejection {
                    claim: $name.into(),
                    reason: "no excerpt is present in the underlying item".into(),
                });
            }
        }};
    }
    require!(out.topic, "topic");
    require!(out.subject, "subject");
    require!(out.change, "change");
    if out.topic.value.label.trim().is_empty() || out.subject.value.label.trim().is_empty() || out.change.value.statement.trim().is_empty() {
        rejections.push(Rejection {
            claim: "interpretation".into(),
            reason: "topic, subject, and change must be non-empty".into(),
        });
    }
    if !rejections.is_empty() {
        return Err(rejections);
    }

    out.context_matches = out
        .context_matches
        .into_iter()
        .filter_map(|mut candidate| {
            if !contexts.iter().any(|context| context.id == candidate.context_id) {
                rejections.push(Rejection {
                    claim: format!("context:{}", candidate.context_id),
                    reason: "unknown context".into(),
                });
                return None;
            }
            candidate.excerpts = grounded(item, &candidate.excerpts);
            if candidate.excerpts.is_empty() {
                rejections.push(Rejection {
                    claim: format!("context:{}", candidate.context_id),
                    reason: "no grounded excerpt".into(),
                });
                return None;
            }
            candidate.strength = candidate.strength.clamp(0.0, 1.0);
            Some(candidate)
        })
        .collect();

    out.why_it_matters = match out.why_it_matters.take() {
        Some(mut claim) if !out.context_matches.is_empty() => {
            claim.excerpts = grounded(item, &claim.excerpts);
            if claim.excerpts.is_empty() {
                // Fall back to the verified context evidence that motivated it.
                claim.excerpts = out.context_matches.iter().flat_map(|m| m.excerpts.clone()).take(3).collect();
            }
            Some(claim)
        }
        Some(_) => {
            rejections.push(Rejection {
                claim: "why_it_matters".into(),
                reason: "not supported by any verified context match".into(),
            });
            None
        }
        None => None,
    };

    out.item_links = out
        .item_links
        .into_iter()
        .filter_map(|mut link| {
            if link.item_id == item.id || !prior.iter().any(|candidate| candidate.item_id == link.item_id) {
                rejections.push(Rejection {
                    claim: format!("link:{}", link.item_id),
                    reason: "link target was not offered as a prior item".into(),
                });
                return None;
            }
            link.excerpts = grounded(item, &link.excerpts);
            link.strength = link.strength.clamp(0.0, 1.0);
            Some(link)
        })
        .collect();

    Ok(Verified { interpretation: out, rejections })
}

/// The graph key for a subject label.
pub fn subject_key(subject: &Subject) -> String {
    slug(&subject.label)
}

/// Topic claims are always phrased as a broad area label.
pub fn topic(label: &str) -> Topic {
    Topic { label: label.to_owned() }
}

pub fn change(kind: ChangeKind, statement: impl Into<String>) -> Change {
    Change { kind, statement: statement.into() }
}
