//! Cross-source synthesis: when several observations describe one change,
//! say what they agree on, what each adds, where they differ, and what
//! remains uncertain — citing evidence for every point. The synthesis sits
//! beside the evidence; it never replaces it.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;
use stream_model::{ClaimBasis, ClaimKind, EvidenceId, ItemId, SignalId, SourceId, SynthesisPoint};
use stream_semantic::provider::{strip_code_fence, ModelProvider, ModelRequest};
use stream_semantic::Rejection;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationEvidence {
    pub id: EvidenceId,
    pub claim: ClaimKind,
    pub excerpt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub item_id: ItemId,
    pub title: String,
    pub source_id: SourceId,
    pub source_label: String,
    /// Publisher host; observations from one host may not be independent.
    pub host: String,
    pub observed_at: DateTime<Utc>,
    pub contradicts: bool,
    pub evidence: Vec<ObservationEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SynthesisInput {
    pub signal_id: SignalId,
    pub subject: String,
    pub change: String,
    pub why_it_matters: Option<String>,
    /// Oldest first.
    pub observations: Vec<Observation>,
}

impl SynthesisInput {
    fn evidence_ids(&self) -> BTreeSet<&EvidenceId> {
        self.observations.iter().flat_map(|o| o.evidence.iter().map(|e| &e.id)).collect()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SynthesisProposal {
    pub agreements: Vec<SynthesisPoint>,
    pub new_information: Vec<SynthesisPoint>,
    pub differences: Vec<SynthesisPoint>,
    pub uncertainties: Vec<SynthesisPoint>,
}

#[async_trait]
pub trait Synthesizer: Send + Sync {
    fn id(&self) -> &str;
    async fn synthesize(&self, input: &SynthesisInput) -> Result<SynthesisProposal>;
}

/// Keep only points that cite evidence belonging to this signal. Observed and
/// inferred points must cite at least one piece; hypotheses may cite none.
pub fn verify_synthesis(proposal: SynthesisProposal, input: &SynthesisInput) -> (SynthesisProposal, Vec<Rejection>) {
    let known = input.evidence_ids();
    let mut rejections = Vec::new();
    let mut check = |points: Vec<SynthesisPoint>, section: &str| -> Vec<SynthesisPoint> {
        points
            .into_iter()
            .filter_map(|mut point| {
                let cited = point.evidence_ids.len();
                point.evidence_ids.retain(|id| known.contains(id));
                point.evidence_ids.dedup();
                let label = format!("synthesis:{section}");
                if point.evidence_ids.len() != cited {
                    rejections.push(Rejection { claim: label.clone(), reason: "cited evidence outside this signal".into() });
                }
                if point.statement.trim().is_empty() {
                    return None;
                }
                if point.basis.requires_evidence() && point.evidence_ids.is_empty() {
                    rejections.push(Rejection { claim: label, reason: "no supporting evidence".into() });
                    return None;
                }
                Some(point)
            })
            .collect()
    };
    let verified = SynthesisProposal {
        agreements: check(proposal.agreements, "agreements"),
        new_information: check(proposal.new_information, "new_information"),
        differences: check(proposal.differences, "differences"),
        uncertainties: check(proposal.uncertainties, "uncertainties"),
    };
    (verified, rejections)
}

fn point(statement: String, basis: ClaimBasis, evidence_ids: Vec<EvidenceId>) -> SynthesisPoint {
    SynthesisPoint { statement, basis, evidence_ids }
}

fn lowercase_first(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Deterministic synthesis from the evidence Stream already holds.
#[derive(Debug, Default, Clone)]
pub struct LocalSynthesizer;

impl LocalSynthesizer {
    pub const ID: &'static str = "stream.synthesis.local.v1";

    pub fn run(input: &SynthesisInput) -> SynthesisProposal {
        let mut out = SynthesisProposal::default();
        let supporters = input.observations.iter().filter(|o| !o.contradicts).collect::<Vec<_>>();
        let disputers = input.observations.iter().filter(|o| o.contradicts).collect::<Vec<_>>();
        let headline = |o: &Observation| {
            o.evidence
                .iter()
                .find(|e| matches!(e.claim, ClaimKind::Change))
                .or_else(|| o.evidence.iter().find(|e| matches!(e.claim, ClaimKind::Corroboration | ClaimKind::Subject)))
                .or_else(|| o.evidence.first())
                .map(|e| e.id.clone())
        };

        if supporters.len() >= 2 {
            let hosts = supporters.iter().map(|o| o.host.as_str()).collect::<BTreeSet<_>>();
            out.agreements.push(point(
                format!(
                    "{} observations from {} agree that {} {}.",
                    supporters.len(),
                    if hosts.len() == 1 { "one publisher".to_owned() } else { format!("{} publishers", hosts.len()) },
                    input.subject,
                    lowercase_first(&input.change)
                ),
                ClaimBasis::Observed,
                supporters.iter().filter_map(|o| headline(o)).collect(),
            ));
        }

        // What each later observation adds: the details Stream recorded as
        // not covered by earlier observations.
        for observation in input.observations.iter().skip(1).filter(|o| !o.contradicts) {
            for evidence in observation.evidence.iter().filter(|e| e.claim == ClaimKind::Detail).take(2) {
                out.new_information.push(point(
                    format!("{} adds: “{}”", observation.source_label, evidence.excerpt),
                    ClaimBasis::Observed,
                    vec![evidence.id.clone()],
                ));
            }
        }

        for observation in &disputers {
            let disputing = observation
                .evidence
                .iter()
                .find(|e| e.claim == ClaimKind::Contradiction)
                .or_else(|| observation.evidence.first());
            if let Some(evidence) = disputing {
                out.differences.push(point(
                    format!("{} disputes it: “{}”", observation.source_label, evidence.excerpt),
                    ClaimBasis::Observed,
                    vec![evidence.id.clone()],
                ));
            }
        }

        if !disputers.is_empty() {
            let mut cited = disputers.iter().filter_map(|o| headline(o)).collect::<Vec<_>>();
            cited.extend(supporters.iter().filter_map(|o| headline(o)));
            out.uncertainties.push(point(
                format!(
                    "Sources disagree about whether {} {}; neither side settles it yet.",
                    input.subject,
                    lowercase_first(&input.change)
                ),
                ClaimBasis::Inferred,
                cited,
            ));
        }
        if supporters.len() == 1 {
            let only = supporters[0];
            out.uncertainties.push(point(
                format!("Only {} reports this so far; it has not been independently corroborated.", only.source_label),
                ClaimBasis::Inferred,
                headline(only).into_iter().collect(),
            ));
        } else if supporters.len() >= 2 && supporters.iter().map(|o| o.host.as_str()).collect::<BTreeSet<_>>().len() == 1 {
            out.uncertainties.push(point(
                format!("All observations come from {}; they may not be independent.", supporters[0].host),
                ClaimBasis::Inferred,
                supporters.iter().filter_map(|o| headline(o)).collect(),
            ));
        }
        if input.why_it_matters.is_some() {
            let why = input
                .observations
                .iter()
                .flat_map(|o| o.evidence.iter())
                .filter(|e| e.claim == ClaimKind::WhyItMatters)
                .map(|e| e.id.clone())
                .take(3)
                .collect::<Vec<_>>();
            if !why.is_empty() {
                out.uncertainties.push(point(
                    "Why this matters to you is Stream's inference from your context; no source states it.".into(),
                    ClaimBasis::Inferred,
                    why,
                ));
            }
        }
        out
    }
}

#[async_trait]
impl Synthesizer for LocalSynthesizer {
    fn id(&self) -> &str {
        Self::ID
    }

    async fn synthesize(&self, input: &SynthesisInput) -> Result<SynthesisProposal> {
        Ok(Self::run(input))
    }
}

const SYNTHESIS_SYSTEM: &str = "You are Stream's synthesis step. Several observations (from different sources) \
describe one underlying change. Say what they agree on, what each adds, where they differ or contradict, and what \
remains uncertain. Cite ONLY the evidence ids given in the input; every agreement, addition, and difference must \
cite at least one. basis=observed only for what sources state; basis=inferred for your conclusions; \
basis=hypothesis for open possibilities. Never resolve a contradiction by picking a side without evidence. Be concise.";

fn synthesis_schema() -> serde_json::Value {
    let points = json!({ "type": "array", "items": {
        "type": "object", "additionalProperties": false, "required": ["statement", "basis", "evidence_ids"],
        "properties": {
            "statement": { "type": "string" },
            "basis": { "type": "string", "enum": ["observed", "inferred", "connected", "hypothesis"] },
            "evidence_ids": { "type": "array", "items": { "type": "string" } }
        } } });
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["agreements", "new_information", "differences", "uncertainties"],
        "properties": {
            "agreements": points, "new_information": points, "differences": points, "uncertainties": points
        }
    })
}

/// Model-backed synthesis behind the same boundary and the same gate.
pub struct ModelSynthesizer {
    provider: Arc<dyn ModelProvider>,
    id: String,
}

impl ModelSynthesizer {
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        let id = format!("stream.synthesis.model.v1+{}", provider.id());
        Self { provider, id }
    }
}

#[async_trait]
impl Synthesizer for ModelSynthesizer {
    fn id(&self) -> &str {
        &self.id
    }

    async fn synthesize(&self, input: &SynthesisInput) -> Result<SynthesisProposal> {
        let request = ModelRequest {
            system: SYNTHESIS_SYSTEM.into(),
            user: format!("Synthesize these observations.\n{}", serde_json::to_string_pretty(input)?),
            schema_name: "stream_synthesis".into(),
            schema: synthesis_schema(),
            max_tokens: 1_500,
        };
        let raw = self.provider.complete_json(&request).await?;
        let proposal: SynthesisProposal = serde_json::from_str(strip_code_fence(&raw))
            .map_err(|error| stream_semantic::ModelError::Malformed(error.to_string()))?;
        Ok(proposal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(id: &str, claim: ClaimKind, excerpt: &str) -> ObservationEvidence {
        ObservationEvidence { id: EvidenceId::new(id), claim, excerpt: excerpt.into() }
    }

    fn observation(item: &str, host: &str, contradicts: bool, evidence: Vec<ObservationEvidence>) -> Observation {
        Observation {
            item_id: ItemId::new(item),
            title: item.into(),
            source_id: SourceId::new(format!("source_{item}")),
            source_label: host.into(),
            host: host.into(),
            observed_at: Utc::now(),
            contradicts,
            evidence,
        }
    }

    fn input() -> SynthesisInput {
        SynthesisInput {
            signal_id: SignalId::new("signal_1"),
            subject: "Apple Container".into(),
            change: "Adds portable Linux VMs".into(),
            why_it_matters: None,
            observations: vec![
                observation("a", "news.example", false, vec![evidence("e1", ClaimKind::Change, "Apple Container adds portable Linux VMs")]),
                observation("b", "blog.example", false, vec![
                    evidence("e2", ClaimKind::Corroboration, "Apple's Container tool brings Linux VMs to macOS"),
                    evidence("e3", ClaimKind::Detail, "Developers get OCI images with no shared kernel between containers"),
                ]),
                observation("c", "rumors.example", true, vec![evidence("e4", ClaimKind::Contradiction, "Apple denies the VM feature is shipping this year")]),
            ],
        }
    }

    #[test]
    fn synthesis_answers_agree_new_differ_uncertain_with_evidence() {
        let proposal = LocalSynthesizer::run(&input());
        assert_eq!(proposal.agreements.len(), 1);
        assert_eq!(proposal.agreements[0].evidence_ids.len(), 2);
        assert!(proposal.new_information.iter().any(|p| p.evidence_ids == vec![EvidenceId::new("e3")]));
        assert_eq!(proposal.differences[0].evidence_ids, vec![EvidenceId::new("e4")]);
        assert!(proposal.uncertainties.iter().any(|p| p.statement.contains("disagree")));
        let (verified, rejections) = verify_synthesis(proposal.clone(), &input());
        assert_eq!(verified, proposal);
        assert!(rejections.is_empty());
    }

    #[test]
    fn the_synthesis_gate_refuses_foreign_or_missing_evidence() {
        let proposal = SynthesisProposal {
            agreements: vec![point("Everyone agrees.".into(), ClaimBasis::Observed, vec![EvidenceId::new("evidence_elsewhere")])],
            uncertainties: vec![point("Maybe it slips to next year.".into(), ClaimBasis::Hypothesis, vec![])],
            ..Default::default()
        };
        let (verified, rejections) = verify_synthesis(proposal, &input());
        assert!(verified.agreements.is_empty());
        assert_eq!(verified.uncertainties.len(), 1, "hypotheses may stand without evidence, labelled as such");
        assert_eq!(rejections.len(), 2);
    }
}
