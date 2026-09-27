//! A model-backed interpreter behind the same [`Interpreter`] boundary.
//!
//! The model receives source metadata, the normalized item, the relevant
//! user contexts, and relevant existing Stream knowledge, and returns a
//! strict JSON document. Nothing it says is trusted: the result is an
//! ordinary advisory [`Interpretation`] that still has to pass the evidence
//! gate before anything becomes durable.

use crate::provider::{strip_code_fence, ModelError, ModelProvider, ModelRequest};
use crate::text::key_terms;
use crate::{
    Claim, ContextMatch, Excerpt, Interpretation, InterpretationInput, Interpreter, ItemLink, LinkKind, PriorItem,
    ProposedClaim, Stance,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use stream_model::{Change, ChangeKind, ClaimBasis, ContextId, EvidenceLocator, ItemId, Subject, Topic};

/// Content budget sent to the model per item.
pub const MAX_ITEM_CHARS: usize = 12_000;
/// Earlier items offered as connection candidates.
pub const MAX_PRIOR_FOR_MODEL: usize = 12;

const SYSTEM: &str = "You are the semantic interpreter of Stream, an information system that turns sources into \
durable, evidence-backed understanding for one user. You read ONE source item and propose an interpretation.

Rules:
1. Distinguish facts from interpretation. basis=observed only for what the source itself states. basis=inferred \
for conclusions you draw from what it states. basis=connected for relationships to the user's contexts or to \
existing Stream items. basis=hypothesis for useful possibilities that need investigation.
2. Every quote must be copied VERBATIM from the item's title or content (exact characters, no paraphrase, no \
ellipses). Quotes that are not found in the source are discarded by Stream, and so is any claim that depends on them.
3. Only use context ids and item ids that appear in the input. Never invent ids.
4. For existing Stream items: relation=same_change when this item reports the same underlying change, \
contradicts when it disputes or denies that change, related otherwise.
5. Do not connect the item to a context just because a word overlaps. A connection needs a quote that shows the \
relationship, or an explicit explanation of it. If nothing genuinely connects, return no connections and \
why_it_matters = null.
6. topic is a broad area (e.g. \"Developer Tooling\", \"AI\"). subject is the specific thing (a product, project, \
organization, workflow). change says what is actually new or different, starting with a verb such as Introduces, \
Adds, Changes, Removes, Deprecates, Releases, Fixes, or \"Signals movement toward\"; never a generic summary.
7. Be concise. Prefer fewer, stronger claims.";

fn quote_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["text", "location"],
        "properties": {
            "text": { "type": "string" },
            "location": { "type": "string", "enum": ["title", "content"] }
        }
    })
}

/// The JSON Schema the model must satisfy.
pub fn interpretation_schema() -> Value {
    let quotes = json!({ "type": "array", "items": quote_schema() });
    let change_kinds = ChangeKind::ALL.iter().map(|kind| kind.as_str()).collect::<Vec<_>>();
    let bases = ClaimBasis::ALL.iter().map(|basis| basis.as_str()).collect::<Vec<_>>();
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["topic", "subject", "change", "why_it_matters", "context_connections", "related_items", "claims"],
        "properties": {
            "topic": { "type": "object", "additionalProperties": false, "required": ["label", "quotes"],
                "properties": { "label": { "type": "string" }, "quotes": quotes } },
            "subject": { "type": "object", "additionalProperties": false, "required": ["label", "quotes"],
                "properties": { "label": { "type": "string" }, "quotes": quotes } },
            "change": { "type": "object", "additionalProperties": false, "required": ["kind", "statement", "quotes"],
                "properties": {
                    "kind": { "type": "string", "enum": change_kinds },
                    "statement": { "type": "string" },
                    "quotes": quotes } },
            "why_it_matters": { "anyOf": [ { "type": "null" }, {
                "type": "object", "additionalProperties": false, "required": ["text", "context_ids", "quotes"],
                "properties": {
                    "text": { "type": "string" },
                    "context_ids": { "type": "array", "items": { "type": "string" } },
                    "quotes": quotes } } ] },
            "context_connections": { "type": "array", "items": {
                "type": "object", "additionalProperties": false,
                "required": ["context_id", "explanation", "strength", "quotes"],
                "properties": {
                    "context_id": { "type": "string" },
                    "explanation": { "type": "string" },
                    "strength": { "type": "number" },
                    "quotes": quotes } } },
            "related_items": { "type": "array", "items": {
                "type": "object", "additionalProperties": false,
                "required": ["item_id", "relation", "explanation", "strength", "quotes"],
                "properties": {
                    "item_id": { "type": "string" },
                    "relation": { "type": "string", "enum": ["same_change", "contradicts", "related"] },
                    "explanation": { "type": "string" },
                    "strength": { "type": "number" },
                    "quotes": quotes } } },
            "claims": { "type": "array", "items": {
                "type": "object", "additionalProperties": false,
                "required": ["basis", "statement", "rationale", "confidence", "context_ids", "item_ids", "quotes"],
                "properties": {
                    "basis": { "type": "string", "enum": bases },
                    "statement": { "type": "string" },
                    "rationale": { "type": "string" },
                    "confidence": { "type": "number" },
                    "context_ids": { "type": "array", "items": { "type": "string" } },
                    "item_ids": { "type": "array", "items": { "type": "string" } },
                    "quotes": quotes } } }
        }
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireQuote {
    text: String,
    location: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireLabel {
    label: String,
    quotes: Vec<WireQuote>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireChange {
    kind: String,
    statement: String,
    quotes: Vec<WireQuote>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireWhy {
    text: String,
    // Informational: relevance is re-derived from verified context connections.
    #[allow(dead_code)]
    context_ids: Vec<String>,
    quotes: Vec<WireQuote>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireContextConnection {
    context_id: String,
    explanation: String,
    strength: f32,
    quotes: Vec<WireQuote>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRelated {
    item_id: String,
    relation: String,
    explanation: String,
    strength: f32,
    quotes: Vec<WireQuote>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireClaim {
    basis: String,
    statement: String,
    rationale: String,
    confidence: f32,
    context_ids: Vec<String>,
    item_ids: Vec<String>,
    quotes: Vec<WireQuote>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireInterpretation {
    topic: WireLabel,
    subject: WireLabel,
    change: WireChange,
    why_it_matters: Option<WireWhy>,
    context_connections: Vec<WireContextConnection>,
    related_items: Vec<WireRelated>,
    claims: Vec<WireClaim>,
}

fn excerpts(quotes: Vec<WireQuote>) -> Result<Vec<Excerpt>> {
    quotes
        .into_iter()
        .map(|quote| {
            let locator = match quote.location.as_str() {
                "title" => EvidenceLocator::Title,
                "content" => EvidenceLocator::Content,
                other => return Err(anyhow!("unknown quote location '{other}'")),
            };
            Ok(Excerpt { locator, text: quote.text })
        })
        .collect()
}

fn claim<T>(value: T, quotes: Vec<WireQuote>, rationale: &str) -> Result<Claim<T>> {
    Ok(Claim { value, excerpts: excerpts(quotes)?, confidence: 0.7, rationale: rationale.into() })
}

/// Parse a model's structured output into an advisory interpretation.
/// Malformed output is an error; it is never repaired by guessing.
pub fn parse_interpretation(raw: &str) -> Result<Interpretation> {
    let wire: WireInterpretation = serde_json::from_str(strip_code_fence(raw))
        .map_err(|error| anyhow!(ModelError::Malformed(error.to_string())))?;
    let kind = ChangeKind::parse(&wire.change.kind)
        .ok_or_else(|| anyhow!(ModelError::Malformed(format!("unknown change kind '{}'", wire.change.kind))))?;

    let context_matches = wire
        .context_connections
        .into_iter()
        .map(|connection| {
            Ok(ContextMatch {
                context_id: ContextId::new(connection.context_id),
                matched_terms: vec![],
                strength: connection.strength,
                excerpts: excerpts(connection.quotes)?,
                rationale: connection.explanation,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let item_links = wire
        .related_items
        .into_iter()
        .map(|related| {
            let (kind, stance) = match related.relation.as_str() {
                "same_change" => (LinkKind::SameChange, Stance::Supports),
                "contradicts" => (LinkKind::SameChange, Stance::Contradicts),
                "related" => (LinkKind::Related, Stance::Supports),
                other => return Err(anyhow!(ModelError::Malformed(format!("unknown relation '{other}'")))),
            };
            Ok(ItemLink {
                item_id: ItemId::new(related.item_id),
                kind,
                stance,
                strength: related.strength,
                shared_terms: vec![],
                excerpts: excerpts(related.quotes)?,
                rationale: related.explanation,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let claims = wire
        .claims
        .into_iter()
        .map(|claim| {
            let basis = ClaimBasis::parse(&claim.basis)
                .ok_or_else(|| anyhow!(ModelError::Malformed(format!("unknown claim basis '{}'", claim.basis))))?;
            Ok(ProposedClaim {
                basis,
                statement: claim.statement,
                excerpts: excerpts(claim.quotes)?,
                context_ids: claim.context_ids.into_iter().map(ContextId::new).collect(),
                item_ids: claim.item_ids.into_iter().map(ItemId::new).collect(),
                rationale: claim.rationale,
                confidence: claim.confidence,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let why_it_matters = match wire.why_it_matters {
        Some(why) if !why.text.trim().is_empty() => Some(claim(why.text, why.quotes, "model-proposed relevance")?),
        _ => None,
    };

    Ok(Interpretation {
        topic: claim(Topic { label: wire.topic.label.trim().to_owned() }, wire.topic.quotes, "model-proposed topic")?,
        subject: claim(Subject { label: wire.subject.label.trim().to_owned() }, wire.subject.quotes, "model-proposed subject")?,
        change: claim(
            Change { kind, statement: wire.change.statement.trim().to_owned() },
            wire.change.quotes,
            "model-proposed change",
        )?,
        context_matches,
        why_it_matters,
        item_links,
        claims,
    })
}

/// The most relevant earlier items for this item, by shared vocabulary.
fn relevant_prior<'a>(input: &'a InterpretationInput<'_>) -> Vec<&'a PriorItem> {
    let terms = key_terms(&input.item.title, &input.item.content_text, 20).into_iter().collect::<BTreeSet<_>>();
    let mut scored = input
        .prior
        .iter()
        .filter(|prior| prior.item_id != input.item.id)
        .map(|prior| {
            let other = key_terms(&prior.title, &prior.content_text, 20).into_iter().collect::<BTreeSet<_>>();
            (terms.intersection(&other).count(), prior)
        })
        .filter(|(shared, _)| *shared > 0)
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| b.0.cmp(&a.0));
    scored.into_iter().take(MAX_PRIOR_FOR_MODEL).map(|(_, prior)| prior).collect()
}

fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut cut = max;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    &text[..cut]
}

pub struct ModelInterpreter {
    provider: Arc<dyn ModelProvider>,
    id: String,
}

impl ModelInterpreter {
    pub fn new(provider: Arc<dyn ModelProvider>) -> Self {
        let id = format!("stream.model.v1+{}", provider.id());
        Self { provider, id }
    }

    pub fn request(input: &InterpretationInput<'_>) -> ModelRequest {
        let item = input.item;
        let contexts = input
            .contexts
            .iter()
            .map(|context| {
                let related = input
                    .contexts
                    .iter()
                    .filter(|other| context.related.contains(&other.id) || other.related.contains(&context.id))
                    .map(|other| other.name.clone())
                    .collect::<Vec<_>>();
                json!({
                    "id": context.id,
                    "name": context.name,
                    "kind": context.kind,
                    "description": context.description,
                    "aliases": context.aliases,
                    "related_to": related,
                })
            })
            .collect::<Vec<_>>();
        let prior = relevant_prior(input)
            .into_iter()
            .map(|prior| json!({ "id": prior.item_id, "title": prior.title, "subject": prior.subject }))
            .collect::<Vec<_>>();
        let user = json!({
            "source": {
                "kind": item.source_kind,
                "url": item.canonical_url,
                "author": item.author,
                "published_at": item.published_at,
            },
            "item": {
                "title": item.title,
                "content": truncate(&item.content_text, MAX_ITEM_CHARS),
            },
            "user_contexts": contexts,
            "existing_stream_items": prior,
        });
        ModelRequest {
            system: SYSTEM.into(),
            user: format!("Interpret this item.\n{}", serde_json::to_string_pretty(&user).unwrap_or_default()),
            schema_name: "stream_interpretation".into(),
            schema: interpretation_schema(),
            max_tokens: 2_000,
        }
    }
}

#[async_trait]
impl Interpreter for ModelInterpreter {
    fn id(&self) -> &str {
        &self.id
    }

    async fn interpret(&self, input: &InterpretationInput<'_>) -> Result<Interpretation> {
        let raw = self.provider.complete_json(&Self::request(input)).await?;
        parse_interpretation(&raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{verify, InterpretationInput};
    use chrono::Utc;
    use stream_model::{ContextEntry, ContextKind, Fingerprint, Item, SourceId, SourceKind};
    use url::Url;

    fn item() -> Item {
        Item {
            id: ItemId::new("item_a"),
            source_id: SourceId::new("source_a"),
            source_kind: SourceKind::Web,
            canonical_identity: "https://example.com/a".into(),
            canonical_url: Some(Url::parse("https://example.com/a").unwrap()),
            title: "Apple Container adds portable Linux VMs".into(),
            content_text: "Apple Container runs every Linux container inside its own lightweight virtual machine.".into(),
            content_html: None,
            author: None,
            published_at: None,
            fingerprint: Fingerprint::new("f"),
            provenance_summary: String::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn output(context_id: &str) -> String {
        json!({
            "topic": { "label": "Compute", "quotes": [{ "text": "lightweight virtual machine", "location": "content" }] },
            "subject": { "label": "Apple Container", "quotes": [{ "text": "Apple Container", "location": "title" }] },
            "change": { "kind": "adds", "statement": "Adds per-container Linux VMs",
                "quotes": [{ "text": "Apple Container adds portable Linux VMs", "location": "title" }] },
            "why_it_matters": { "text": "It may simplify portable runtimes you are building.", "context_ids": [context_id],
                "quotes": [{ "text": "portable Linux VMs", "location": "title" }] },
            "context_connections": [{ "context_id": context_id, "explanation": "Per-container VMs are a portable execution boundary.",
                "strength": 0.8, "quotes": [{ "text": "portable Linux VMs", "location": "title" }] }],
            "related_items": [],
            "claims": [
                { "basis": "observed", "statement": "Apple Container runs each container in its own VM.", "rationale": "stated",
                  "confidence": 0.9, "context_ids": [], "item_ids": [],
                  "quotes": [{ "text": "runs every Linux container inside its own lightweight virtual machine", "location": "content" }] },
                { "basis": "inferred", "statement": "This may reduce isolation work in portable runtimes.", "rationale": "VM per container",
                  "confidence": 0.6, "context_ids": [context_id], "item_ids": [],
                  "quotes": [{ "text": "lightweight virtual machine", "location": "content" }] },
                { "basis": "hypothesis", "statement": "The custom runtime layer could be removed entirely.", "rationale": "if isolation is solved upstream",
                  "confidence": 0.3, "context_ids": [context_id], "item_ids": [], "quotes": [] },
                { "basis": "observed", "statement": "Apple acquired Docker.", "rationale": "made up",
                  "confidence": 0.9, "context_ids": [], "item_ids": [],
                  "quotes": [{ "text": "Apple has acquired Docker", "location": "content" }] }
            ]
        })
        .to_string()
    }

    #[test]
    fn structured_output_parses_into_an_interpretation() {
        let parsed = parse_interpretation(&output("context_x")).unwrap();
        assert_eq!(parsed.subject.value.label, "Apple Container");
        assert_eq!(parsed.change.value.kind, ChangeKind::Adds);
        assert_eq!(parsed.claims.len(), 4);
        assert_eq!(parsed.claims[2].basis, ClaimBasis::Hypothesis);
        let fenced = format!("```json\n{}\n```", output("context_x"));
        assert!(parse_interpretation(&fenced).is_ok());
    }

    #[test]
    fn malformed_output_fails_safely() {
        for bad in [
            "Sure! The topic is compute.".to_string(),
            "{}".to_string(),
            output("c").replace("\"adds\"", "\"teleports\""),
            output("c").replace("\"hypothesis\"", "\"certainty\""),
            {
                let mut value: Value = serde_json::from_str(&output("c")).unwrap();
                value["confidence_score"] = json!(0.99);
                value.to_string()
            },
            {
                let mut value: Value = serde_json::from_str(&output("c")).unwrap();
                value.as_object_mut().unwrap().remove("claims");
                value.to_string()
            },
        ] {
            assert!(parse_interpretation(&bad).is_err(), "should reject: {bad}");
        }
    }

    #[test]
    fn the_gate_keeps_grounded_claims_and_drops_fabricated_ones() {
        let context = ContextEntry::new("Portable compute", ContextKind::Interest, "");
        let parsed = parse_interpretation(&output(context.id.as_str())).unwrap();
        let item = item();
        let verified = verify(parsed, &item, std::slice::from_ref(&context), &[]).unwrap();
        let bases = verified.interpretation.claims.iter().map(|c| c.basis).collect::<Vec<_>>();
        assert_eq!(bases, vec![ClaimBasis::Observed, ClaimBasis::Inferred, ClaimBasis::Hypothesis]);
        assert!(verified.rejections.iter().any(|r| r.claim == "claim:observed"));
        assert_eq!(verified.interpretation.context_matches.len(), 1);
        assert!(verified.interpretation.why_it_matters.is_some());
    }

    #[test]
    fn requests_carry_contexts_prior_knowledge_and_the_schema() {
        let context = ContextEntry::new("Portable compute", ContextKind::Interest, "Run anywhere");
        let prior = vec![PriorItem {
            item_id: ItemId::new("item_prior"),
            signal_id: None,
            title: "Linux VMs arrive in Apple Container".into(),
            content_text: "virtual machine per container".into(),
            subject: Some("Apple Container".into()),
        }];
        let item = item();
        let request = ModelInterpreter::request(&InterpretationInput { item: &item, contexts: &[context], prior: &prior });
        assert!(request.user.contains("Portable compute"));
        assert!(request.user.contains("item_prior"));
        assert!(request.system.contains("VERBATIM"));
        assert_eq!(request.schema["required"].as_array().unwrap().len(), 7);
    }
}
