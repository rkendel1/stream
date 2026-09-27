//! Stream's intelligence model: what the user cares about, and the derived,
//! evidence-backed interpretation of what changed.
//!
//! Everything in this module except [`ContextEntry`] is *derived*: a
//! [`Signal`] is an advisory interpretation of durable items, and it must
//! always be traceable back to them through [`Evidence`].

use crate::{
    ClaimId, ConnectionId, ContextId, EvidenceId, InsightId, IntelligenceEventId, ItemId, ProvenanceId, SignalId,
    SourceId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use url::Url;

macro_rules! text_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text),+
                }
            }

            pub fn parse(value: &str) -> Option<Self> {
                match value {
                    $($text => Some($name::$variant),)+
                    _ => None,
                }
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use text_enum;

text_enum!(ProcessingStage {
    Queued => "queued",
    Fetching => "fetching",
    Understanding => "understanding",
    Connecting => "connecting",
    BuildingSignal => "building_signal",
    Observed => "observed",
    Failed => "failed",
});

impl ProcessingStage {
    pub fn label(self) -> &'static str {
        match self {
            ProcessingStage::Queued => "Queued",
            ProcessingStage::Fetching => "Fetching source",
            ProcessingStage::Understanding => "Understanding content",
            ProcessingStage::Connecting => "Finding connections",
            ProcessingStage::BuildingSignal => "Building signal",
            ProcessingStage::Observed => "Observed",
            ProcessingStage::Failed => "Failed",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, ProcessingStage::Observed | ProcessingStage::Failed)
    }
}

text_enum!(ContextKind {
    Interest => "interest",
    Project => "project",
    Concern => "concern",
});

text_enum!(ChangeKind {
    Introduces => "introduces",
    Adds => "adds",
    Changes => "changes",
    Removes => "removes",
    Deprecates => "deprecates",
    Releases => "releases",
    Fixes => "fixes",
    SignalsMovement => "signals_movement",
    Describes => "describes",
});

impl ChangeKind {
    /// Observable, deterministic magnitude of a change kind in `[0, 1]`.
    /// A release or removal changes the world more than a description of it.
    pub fn magnitude(self) -> f64 {
        match self {
            ChangeKind::Introduces | ChangeKind::Removes | ChangeKind::Releases => 1.0,
            ChangeKind::Adds | ChangeKind::Deprecates => 0.8,
            ChangeKind::Changes | ChangeKind::SignalsMovement => 0.6,
            ChangeKind::Fixes => 0.4,
            ChangeKind::Describes => 0.15,
        }
    }

    pub fn verb(self) -> &'static str {
        match self {
            ChangeKind::Introduces => "Introduces",
            ChangeKind::Adds => "Adds",
            ChangeKind::Changes => "Changes",
            ChangeKind::Removes => "Removes",
            ChangeKind::Deprecates => "Deprecates",
            ChangeKind::Releases => "Releases",
            ChangeKind::Fixes => "Fixes",
            ChangeKind::SignalsMovement => "Signals movement toward",
            ChangeKind::Describes => "Describes",
        }
    }
}

text_enum!(SignalStatus {
    Open => "open",
    Resolved => "resolved",
    Dismissed => "dismissed",
});

// Which claim of a signal a piece of evidence supports.
text_enum!(ClaimKind {
    Topic => "topic",
    Subject => "subject",
    Change => "change",
    WhyItMatters => "why_it_matters",
    Connection => "connection",
    Corroboration => "corroboration",
    Contradiction => "contradiction",
    Statement => "statement",
    Detail => "detail",
});

// How a claim relates to the source material. Only `Observed` is a fact
// stated by a source; everything else is Stream's advisory reasoning.
text_enum!(ClaimBasis {
    Observed => "observed",
    Inferred => "inferred",
    Connected => "connected",
    Hypothesis => "hypothesis",
});

impl ClaimBasis {
    pub fn label(self) -> &'static str {
        match self {
            ClaimBasis::Observed => "Observed",
            ClaimBasis::Inferred => "Inferred",
            ClaimBasis::Connected => "Connected",
            ClaimBasis::Hypothesis => "Hypothesis",
        }
    }

    /// Observed, inferred, and connected claims must cite evidence;
    /// a hypothesis is explicitly an open possibility.
    pub fn requires_evidence(self) -> bool {
        !matches!(self, ClaimBasis::Hypothesis)
    }
}

text_enum!(InsightKind {
    Question => "question",
    Hypothesis => "hypothesis",
    Insight => "insight",
    DecisionCandidate => "decision_candidate",
    Investigation => "investigation",
});

impl InsightKind {
    /// Kinds that represent unresolved work and so raise a signal's rank.
    pub fn is_open_question(self) -> bool {
        matches!(self, InsightKind::Question | InsightKind::Hypothesis | InsightKind::Investigation)
    }
}

text_enum!(InsightStatus {
    Open => "open",
    Resolved => "resolved",
    Dropped => "dropped",
});

// Where inside the underlying item an evidence excerpt was found.
text_enum!(EvidenceLocator {
    Title => "title",
    Content => "content",
    Url => "url",
});

text_enum!(ConnectionTargetKind {
    Topic => "topic",
    Subject => "subject",
    Context => "context",
    Project => "project",
    Item => "item",
});

text_enum!(ConnectionRelation {
    About => "about",
    Concerns => "concerns",
    Matches => "matches",
    Via => "via",
    SameChange => "same_change",
    Contradicts => "contradicts",
    Related => "related",
});

/// Durable user/world context: something the user cares about.
///
/// These are not UI tags. Semantic processing evaluates every item against
/// them, and they are persisted in FeltDB like any other authoritative state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEntry {
    pub id: ContextId,
    pub name: String,
    pub kind: ContextKind,
    pub description: String,
    pub aliases: Vec<String>,
    pub related: Vec<ContextId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ContextEntry {
    pub fn new(name: impl Into<String>, kind: ContextKind, description: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: ContextId::generate(),
            name: crate::normalize_whitespace(&name.into()),
            kind,
            description: description.into().trim().to_owned(),
            aliases: Vec::new(),
            related: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Topic {
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Subject {
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub kind: ChangeKind,
    pub statement: String,
}

/// The first-class information-density object: the useful interpretation of
/// one underlying change, observed through one or more items.
///
/// A signal is derived interpretation, not authority. FeltDB items remain
/// authoritative; every claim here is backed by [`Evidence`] rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signal {
    pub id: SignalId,
    /// The item that first established this signal.
    pub item_id: ItemId,
    pub topic: Topic,
    pub subject: Subject,
    pub change: Change,
    pub why_it_matters: Option<String>,
    pub status: SignalStatus,
    /// Recorded for audit only; presentation layers never see it.
    pub interpreter: String,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A traceable link from one claim of a signal to the source material.
///
/// Evidence → Item → Source → URL. Each observation stays individually
/// addressable even when many observations consolidate into one signal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    pub id: EvidenceId,
    pub signal_id: SignalId,
    pub item_id: ItemId,
    pub source_id: SourceId,
    pub provenance_id: Option<ProvenanceId>,
    pub url: Url,
    pub claim: ClaimKind,
    pub locator: EvidenceLocator,
    pub excerpt: String,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// An edge in Stream's information graph.
///
/// Item → Topic, Item → Subject, Item → Context/Project, Item → Item. When the
/// edge belongs to a signal's interpretation, `signal_id` is set, which makes
/// it a Signal → Context / Signal → Project edge as well.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Connection {
    pub id: ConnectionId,
    pub signal_id: Option<SignalId>,
    pub item_id: ItemId,
    pub target_kind: ConnectionTargetKind,
    pub target_id: String,
    pub label: String,
    pub relation: ConnectionRelation,
    pub strength: f32,
    pub rationale: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub created_at: DateTime<Utc>,
}

/// A proposition about a signal, labelled with its basis so Stream never
/// presents an inference or hypothesis as an established fact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalClaim {
    pub id: ClaimId,
    pub signal_id: SignalId,
    pub item_id: ItemId,
    pub basis: ClaimBasis,
    pub statement: String,
    pub confidence: f32,
    pub rationale: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub context_ids: Vec<ContextId>,
    pub created_at: DateTime<Utc>,
}

impl SignalClaim {
    /// Presentation order: facts first, then connections, inferences, hypotheses.
    pub fn basis_rank(&self) -> u8 {
        match self.basis {
            ClaimBasis::Observed => 0,
            ClaimBasis::Connected => 1,
            ClaimBasis::Inferred => 2,
            ClaimBasis::Hypothesis => 3,
        }
    }
}

/// One point of a cross-source synthesis, always citing evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SynthesisPoint {
    pub statement: String,
    pub basis: ClaimBasis,
    pub evidence_ids: Vec<EvidenceId>,
}

/// What several observations of one change agree on, add, differ on, and
/// leave uncertain. Derived; the underlying evidence is never replaced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Synthesis {
    pub signal_id: SignalId,
    pub agreements: Vec<SynthesisPoint>,
    pub new_information: Vec<SynthesisPoint>,
    pub differences: Vec<SynthesisPoint>,
    pub uncertainties: Vec<SynthesisPoint>,
    pub source_count: usize,
    pub observation_count: usize,
    /// Audit only: kept in FeltDB, never serialized to any presentation or
    /// AppPort surface.
    #[serde(skip)]
    pub generated_by: String,
    pub updated_at: DateTime<Utc>,
}

/// A reasoning artifact the user chose to keep: derived and advisory, with
/// the question that produced it and the evidence it rests on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Insight {
    pub id: InsightId,
    pub kind: InsightKind,
    pub status: InsightStatus,
    pub statement: String,
    pub basis: ClaimBasis,
    pub question: Option<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub signal_ids: Vec<SignalId>,
    pub context_ids: Vec<ContextId>,
    pub confidence: Option<f32>,
    pub uncertainty: Option<String>,
    /// Which reasoner produced the underlying answer. Audit metadata: kept
    /// in FeltDB, never serialized to presentation or AppPort surfaces.
    #[serde(skip)]
    pub reasoner: String,
    /// Whether a model (rather than local reasoning) backed it.
    pub model_backed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A durable, observable record of what the intelligence layer did when it
/// did not simply succeed: provider failures, refusals, fallbacks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntelligenceEvent {
    pub id: IntelligenceEventId,
    /// interpret | synthesize | reason
    pub operation: String,
    /// failed | rejected | fallback
    pub status: String,
    pub subject_id: Option<String>,
    pub detail: String,
    pub provider: String,
    pub created_at: DateTime<Utc>,
}

/// Normalize a label into a stable graph key ("Portable Compute" → "portable-compute").
pub fn slug(label: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for character in label.chars() {
        if character.is_alphanumeric() {
            out.extend(character.to_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_end_matches('-').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_enums_round_trip() {
        for stage in ProcessingStage::ALL {
            assert_eq!(ProcessingStage::parse(stage.as_str()), Some(*stage));
        }
        for kind in ChangeKind::ALL {
            assert_eq!(ChangeKind::parse(kind.as_str()), Some(*kind));
        }
    }

    #[test]
    fn slugs_are_stable_graph_keys() {
        assert_eq!(slug("Portable  Compute!"), "portable-compute");
        assert_eq!(slug("Rust 1.99"), "rust-1-99");
    }
}
