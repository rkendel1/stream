//! FeltDB record mappings for Stream's intelligence collections.
//!
//! These follow the conventions of the rest of the runtime: flat records,
//! text for enums, RFC 3339 for datetimes, JSON-encoded text for lists, and
//! text for fractional numbers.

use super::{value_datetime, value_datetime_opt, value_string, value_string_opt, value_u64};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use stream_model::{
    ClaimBasis, ClaimId, Insight, InsightId, InsightKind, InsightStatus, IntelligenceEvent, IntelligenceEventId,
    SignalClaim, Synthesis, SynthesisPoint,
};
use stream_model::{
    slug, Change, ChangeKind, ClaimKind, Connection, ConnectionId, ConnectionRelation, ConnectionTargetKind,
    ContextEntry, ContextId, ContextKind, Evidence, EvidenceId, EvidenceLocator, FetchAttempt, FetchAttemptId,
    FetchStatus, ItemId, ItemRelation, ItemRelationId, ProvenanceId, RelationKind, Signal, SignalId, SignalStatus,
    SourceId, Subject, Topic,
};

fn parse_enum<T>(value: &Value, field: &str, parse: fn(&str) -> Option<T>) -> Result<T> {
    let raw = value_string(value, field)?;
    parse(&raw).ok_or_else(|| anyhow!("unsupported {field}: {raw}"))
}

fn float(value: &Value, field: &str) -> f32 {
    match value.get(field) {
        Some(Value::String(text)) => text.parse().unwrap_or(0.0),
        Some(Value::Number(number)) => number.as_f64().unwrap_or(0.0) as f32,
        _ => 0.0,
    }
}

fn json_list<T: serde::de::DeserializeOwned>(value: &Value, field: &str) -> Vec<T> {
    value_string_opt(value, field)
        .ok()
        .flatten()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

pub(crate) fn context_record(context: &ContextEntry) -> Value {
    json!({
        "__id": context.id.as_str(),
        "name": context.name,
        "name_key": slug(&context.name),
        "kind": context.kind.as_str(),
        "description": context.description,
        "aliases": serde_json::to_string(&context.aliases).unwrap_or_else(|_| "[]".into()),
        "related": serde_json::to_string(&context.related).unwrap_or_else(|_| "[]".into()),
        "created_at": context.created_at.to_rfc3339(),
        "updated_at": context.updated_at.to_rfc3339(),
    })
}

pub(crate) fn context_from_value(value: Value) -> Result<ContextEntry> {
    Ok(ContextEntry {
        id: ContextId::new(value_string(&value, "__id")?),
        name: value_string(&value, "name")?,
        kind: parse_enum(&value, "kind", ContextKind::parse)?,
        description: value_string_opt(&value, "description")?.unwrap_or_default(),
        aliases: json_list(&value, "aliases"),
        related: json_list(&value, "related"),
        created_at: value_datetime(&value, "created_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

pub(crate) fn signal_record(signal: &Signal) -> Value {
    json!({
        "__id": signal.id.as_str(),
        "item": signal.item_id.as_str(),
        "topic": signal.topic.label,
        "subject": signal.subject.label,
        "subject_key": slug(&signal.subject.label),
        "change_kind": signal.change.kind.as_str(),
        "change": signal.change.statement,
        "why_it_matters": signal.why_it_matters.clone().unwrap_or_default(),
        "status": signal.status.as_str(),
        "interpreter": signal.interpreter,
        "advisory_state": "advisory",
        "confidence": format!("{:.3}", signal.confidence),
        "created_at": signal.created_at.to_rfc3339(),
        "updated_at": signal.updated_at.to_rfc3339(),
    })
}

pub(crate) fn signal_from_value(value: Value) -> Result<Signal> {
    Ok(Signal {
        id: SignalId::new(value_string(&value, "__id")?),
        item_id: ItemId::new(value_string(&value, "item")?),
        topic: Topic { label: value_string(&value, "topic")? },
        subject: Subject { label: value_string(&value, "subject")? },
        change: Change {
            kind: parse_enum(&value, "change_kind", ChangeKind::parse)?,
            statement: value_string(&value, "change")?,
        },
        why_it_matters: value_string_opt(&value, "why_it_matters")?,
        status: parse_enum(&value, "status", SignalStatus::parse)?,
        interpreter: value_string_opt(&value, "interpreter")?.unwrap_or_default(),
        confidence: float(&value, "confidence"),
        created_at: value_datetime(&value, "created_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

pub(crate) fn evidence_record(evidence: &Evidence) -> Value {
    json!({
        "__id": evidence.id.as_str(),
        "signal": evidence.signal_id.as_str(),
        "item": evidence.item_id.as_str(),
        "source": evidence.source_id.as_str(),
        "provenance": evidence.provenance_id.as_ref().map(|value| value.as_str()).unwrap_or_default(),
        "url": evidence.url.as_str(),
        "claim": evidence.claim.as_str(),
        "locator": evidence.locator.as_str(),
        "excerpt": evidence.excerpt,
        "observed_at": evidence.observed_at.to_rfc3339(),
        "created_at": evidence.created_at.to_rfc3339(),
    })
}

pub(crate) fn evidence_from_value(value: Value) -> Result<Evidence> {
    Ok(Evidence {
        id: EvidenceId::new(value_string(&value, "__id")?),
        signal_id: SignalId::new(value_string(&value, "signal")?),
        item_id: ItemId::new(value_string(&value, "item")?),
        source_id: SourceId::new(value_string(&value, "source")?),
        provenance_id: value_string_opt(&value, "provenance")?.map(ProvenanceId::new),
        url: url::Url::parse(&value_string(&value, "url")?)?,
        claim: parse_enum(&value, "claim", ClaimKind::parse)?,
        locator: parse_enum(&value, "locator", EvidenceLocator::parse)?,
        excerpt: value_string(&value, "excerpt")?,
        observed_at: value_datetime(&value, "observed_at")?,
        created_at: value_datetime(&value, "created_at")?,
    })
}

pub(crate) fn connection_record(connection: &Connection) -> Value {
    json!({
        "__id": connection.id.as_str(),
        "signal": connection.signal_id.as_ref().map(|value| value.as_str()).unwrap_or_default(),
        "item": connection.item_id.as_str(),
        "target_kind": connection.target_kind.as_str(),
        "target_id": connection.target_id,
        "label": connection.label,
        "relation": connection.relation.as_str(),
        "strength": format!("{:.3}", connection.strength),
        "rationale": connection.rationale,
        "evidence": serde_json::to_string(&connection.evidence_ids).unwrap_or_else(|_| "[]".into()),
        "created_at": connection.created_at.to_rfc3339(),
    })
}

pub(crate) fn connection_from_value(value: Value) -> Result<Connection> {
    Ok(Connection {
        id: ConnectionId::new(value_string(&value, "__id")?),
        signal_id: value_string_opt(&value, "signal")?.map(SignalId::new),
        item_id: ItemId::new(value_string(&value, "item")?),
        target_kind: parse_enum(&value, "target_kind", ConnectionTargetKind::parse)?,
        target_id: value_string(&value, "target_id")?,
        label: value_string(&value, "label")?,
        relation: parse_enum(&value, "relation", ConnectionRelation::parse)?,
        strength: float(&value, "strength"),
        rationale: value_string_opt(&value, "rationale")?.unwrap_or_default(),
        evidence_ids: json_list(&value, "evidence"),
        created_at: value_datetime(&value, "created_at")?,
    })
}

pub(crate) fn fetch_attempt_from_value(value: Value) -> Result<FetchAttempt> {
    Ok(FetchAttempt {
        id: FetchAttemptId::new(value_string(&value, "__id")?),
        source_id: SourceId::new(value_string(&value, "source")?),
        attempted_at: value_datetime(&value, "attempted_at")?,
        completed_at: value_datetime_opt(&value, "completed_at")?,
        status: match value_string(&value, "status")?.as_str() {
            "started" => FetchStatus::Started,
            "succeeded" => FetchStatus::Succeeded,
            "failed" => FetchStatus::Failed,
            other => return Err(anyhow!("unsupported fetch status: {other}")),
        },
        failure_category: value_string_opt(&value, "failure_category")?
            .map(|value| super::failure_category(&value))
            .transpose()?,
        item_count: value_u64(&value, "item_count").unwrap_or(0),
        duplicate_count: value_u64(&value, "duplicate_count").unwrap_or(0),
        retained_count: value_u64(&value, "retained_count").unwrap_or(0),
        diagnostics: value_string_opt(&value, "diagnostics")?
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_else(|| json!({})),
    })
}

pub(crate) fn item_relation_from_value(value: Value) -> Result<ItemRelation> {
    Ok(ItemRelation {
        id: ItemRelationId::new(value_string(&value, "__id")?),
        from_item_id: ItemId::new(value_string(&value, "from_item")?),
        to_item_id: ItemId::new(value_string(&value, "to_item")?),
        relation: match value_string(&value, "relation")?.as_str() {
            "duplicate" => RelationKind::Duplicate,
            "same_story" => RelationKind::SameStory,
            "references" => RelationKind::References,
            "derived_from" => RelationKind::DerivedFrom,
            "related" => RelationKind::Related,
            other => return Err(anyhow!("unsupported item relation: {other}")),
        },
        evidence: value_string_opt(&value, "evidence")?.unwrap_or_default(),
        created_at: value_datetime(&value, "created_at")?,
    })
}

pub(crate) fn claim_record(claim: &SignalClaim) -> Value {
    json!({
        "__id": claim.id.as_str(),
        "signal": claim.signal_id.as_str(),
        "item": claim.item_id.as_str(),
        "basis": claim.basis.as_str(),
        "statement": claim.statement,
        "confidence": format!("{:.3}", claim.confidence),
        "rationale": claim.rationale,
        "evidence": serde_json::to_string(&claim.evidence_ids).unwrap_or_else(|_| "[]".into()),
        "contexts": serde_json::to_string(&claim.context_ids).unwrap_or_else(|_| "[]".into()),
        "created_at": claim.created_at.to_rfc3339(),
    })
}

pub(crate) fn claim_from_value(value: Value) -> Result<SignalClaim> {
    Ok(SignalClaim {
        id: ClaimId::new(value_string(&value, "__id")?),
        signal_id: SignalId::new(value_string(&value, "signal")?),
        item_id: ItemId::new(value_string(&value, "item")?),
        basis: parse_enum(&value, "basis", ClaimBasis::parse)?,
        statement: value_string(&value, "statement")?,
        confidence: float(&value, "confidence"),
        rationale: value_string_opt(&value, "rationale")?.unwrap_or_default(),
        evidence_ids: json_list(&value, "evidence"),
        context_ids: json_list(&value, "contexts"),
        created_at: value_datetime(&value, "created_at")?,
    })
}

fn points_text(points: &[SynthesisPoint]) -> String {
    serde_json::to_string(points).unwrap_or_else(|_| "[]".into())
}

pub(crate) fn synthesis_id(signal: &SignalId) -> String {
    format!("synthesis_{}", signal.as_str().trim_start_matches("signal_"))
}

pub(crate) fn synthesis_record(synthesis: &Synthesis) -> Value {
    json!({
        "__id": synthesis_id(&synthesis.signal_id),
        "signal": synthesis.signal_id.as_str(),
        "agreements": points_text(&synthesis.agreements),
        "new_information": points_text(&synthesis.new_information),
        "differences": points_text(&synthesis.differences),
        "uncertainties": points_text(&synthesis.uncertainties),
        "source_count": synthesis.source_count,
        "observation_count": synthesis.observation_count,
        "generated_by": synthesis.generated_by,
        "advisory_state": "advisory",
        "updated_at": synthesis.updated_at.to_rfc3339(),
    })
}

pub(crate) fn synthesis_from_value(value: Value) -> Result<Synthesis> {
    Ok(Synthesis {
        signal_id: SignalId::new(value_string(&value, "signal")?),
        agreements: json_list(&value, "agreements"),
        new_information: json_list(&value, "new_information"),
        differences: json_list(&value, "differences"),
        uncertainties: json_list(&value, "uncertainties"),
        source_count: value_u64(&value, "source_count").unwrap_or(0) as usize,
        observation_count: value_u64(&value, "observation_count").unwrap_or(0) as usize,
        generated_by: value_string_opt(&value, "generated_by")?.unwrap_or_default(),
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

pub(crate) fn insight_record(insight: &Insight) -> Value {
    json!({
        "__id": insight.id.as_str(),
        "kind": insight.kind.as_str(),
        "status": insight.status.as_str(),
        "statement": insight.statement,
        "basis": insight.basis.as_str(),
        "question": insight.question.clone().unwrap_or_default(),
        "evidence": serde_json::to_string(&insight.evidence_ids).unwrap_or_else(|_| "[]".into()),
        "signals": serde_json::to_string(&insight.signal_ids).unwrap_or_else(|_| "[]".into()),
        "contexts": serde_json::to_string(&insight.context_ids).unwrap_or_else(|_| "[]".into()),
        "confidence": insight.confidence.map(|c| format!("{c:.3}")).unwrap_or_default(),
        "uncertainty": insight.uncertainty.clone().unwrap_or_default(),
        "reasoner": insight.reasoner,
        "advisory_state": "advisory",
        "created_at": insight.created_at.to_rfc3339(),
        "updated_at": insight.updated_at.to_rfc3339(),
    })
}

pub(crate) fn insight_from_value(value: Value) -> Result<Insight> {
    Ok(Insight {
        id: InsightId::new(value_string(&value, "__id")?),
        kind: parse_enum(&value, "kind", InsightKind::parse)?,
        status: parse_enum(&value, "status", InsightStatus::parse)?,
        statement: value_string(&value, "statement")?,
        basis: parse_enum(&value, "basis", ClaimBasis::parse)?,
        question: value_string_opt(&value, "question")?,
        evidence_ids: json_list(&value, "evidence"),
        signal_ids: json_list(&value, "signals"),
        context_ids: json_list(&value, "contexts"),
        confidence: value_string_opt(&value, "confidence")?.and_then(|c| c.parse().ok()),
        uncertainty: value_string_opt(&value, "uncertainty")?,
        model_backed: value_string_opt(&value, "reasoner")?.map(|r| r.contains(".model.")).unwrap_or(false),
        reasoner: value_string_opt(&value, "reasoner")?.unwrap_or_default(),
        created_at: value_datetime(&value, "created_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

pub(crate) fn event_record(event: &IntelligenceEvent) -> Value {
    json!({
        "__id": event.id.as_str(),
        "operation": event.operation,
        "status": event.status,
        "subject_id": event.subject_id.clone().unwrap_or_default(),
        "detail": event.detail,
        "provider": event.provider,
        "created_at": event.created_at.to_rfc3339(),
    })
}

pub(crate) fn event_from_value(value: Value) -> Result<IntelligenceEvent> {
    Ok(IntelligenceEvent {
        id: IntelligenceEventId::new(value_string(&value, "__id")?),
        operation: value_string(&value, "operation")?,
        status: value_string(&value, "status")?,
        subject_id: value_string_opt(&value, "subject_id")?,
        detail: value_string_opt(&value, "detail")?.unwrap_or_default(),
        provider: value_string_opt(&value, "provider")?.unwrap_or_default(),
        created_at: value_datetime(&value, "created_at")?,
    })
}

fn opt_time(value: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    value.map(|v| v.to_rfc3339()).unwrap_or_default()
}

pub(crate) fn target_record(target: &stream_model::ObservationTarget) -> Value {
    json!({
        "__id": target.id.as_str(),
        "seed_url": target.seed_url.as_str(),
        "canonical_seed_url": target.canonical_seed_url.as_str(),
        "scope": target.scope.as_str(),
        "display_url": target.display_url(),
        "provider": target.identity.provider.as_str(),
        "identity_kind": target.identity.kind.as_str(),
        "identity_key": target.identity.key,
        "display_name": target.identity.display_name,
        "watching": target.identity.watching,
        "title": target.title.clone().unwrap_or_default(),
        "status": target.status.as_str(),
        "discovery_status": target.discovery_status.as_str(),
        "discovery_detail": target.discovery_detail.clone().unwrap_or_default(),
        "discovery_started_at": opt_time(&target.discovery_started_at),
        "last_discovered_at": opt_time(&target.last_discovered_at),
        "next_discovery_at": opt_time(&target.next_discovery_at),
        "last_observed_at": opt_time(&target.last_observed_at),
        "next_observation_at": opt_time(&target.next_observation_at),
        "consecutive_failures": target.consecutive_failures,
        "observe_every_minutes": target.discovery_policy.observe_every_minutes,
        "discover_every_hours": target.discovery_policy.discover_every_hours,
        "max_sources": target.discovery_policy.max_sources,
        "contexts": serde_json::to_string(&target.context_ids).unwrap_or_else(|_| "[]".into()),
        "sources": serde_json::to_string(&target.source_ids).unwrap_or_else(|_| "[]".into()),
        "provenance": target.provenance,
        "created_at": target.created_at.to_rfc3339(),
        "updated_at": target.updated_at.to_rfc3339(),
    })
}

pub(crate) fn target_from_value(value: Value) -> Result<stream_model::ObservationTarget> {
    use stream_model::{
        DiscoveryStatus, ObservationPolicy, ObservationScope, TargetId, TargetIdentity, TargetKind, TargetProvider, TargetStatus,
    };
    Ok(stream_model::ObservationTarget {
        id: TargetId::new(value_string(&value, "__id")?),
        seed_url: url::Url::parse(&value_string(&value, "seed_url")?)?,
        canonical_seed_url: url::Url::parse(&value_string(&value, "canonical_seed_url")?)?,
        scope: parse_enum(&value, "scope", ObservationScope::parse)?,
        identity: TargetIdentity {
            provider: parse_enum(&value, "provider", TargetProvider::parse)?,
            kind: parse_enum(&value, "identity_kind", TargetKind::parse)?,
            key: value_string_opt(&value, "identity_key")?.unwrap_or_default(),
            display_name: value_string_opt(&value, "display_name")?.unwrap_or_default(),
            watching: value_string_opt(&value, "watching")?.unwrap_or_default(),
        },
        title: value_string_opt(&value, "title")?,
        status: parse_enum(&value, "status", TargetStatus::parse)?,
        discovery_status: parse_enum(&value, "discovery_status", DiscoveryStatus::parse)?,
        discovery_detail: value_string_opt(&value, "discovery_detail")?,
        discovery_started_at: value_datetime_opt(&value, "discovery_started_at")?,
        last_discovered_at: value_datetime_opt(&value, "last_discovered_at")?,
        next_discovery_at: value_datetime_opt(&value, "next_discovery_at")?,
        last_observed_at: value_datetime_opt(&value, "last_observed_at")?,
        next_observation_at: value_datetime_opt(&value, "next_observation_at")?,
        consecutive_failures: value_u64(&value, "consecutive_failures").unwrap_or(0) as u32,
        discovery_policy: ObservationPolicy {
            observe_every_minutes: value_u64(&value, "observe_every_minutes").unwrap_or(60) as u32,
            discover_every_hours: value_u64(&value, "discover_every_hours").unwrap_or(24) as u32,
            max_sources: value_u64(&value, "max_sources").unwrap_or(30) as usize,
        },
        context_ids: json_list(&value, "contexts"),
        source_ids: json_list(&value, "sources"),
        provenance: value_string_opt(&value, "provenance")?.unwrap_or_default(),
        created_at: value_datetime(&value, "created_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

pub(crate) fn relation_record(relation: &stream_model::DiscoveryRelation) -> Value {
    json!({
        "__id": relation.id,
        "target": relation.target_id.as_str(),
        "source": relation.source_id.as_str(),
        "discovered_from": relation.discovered_from.as_str(),
        "from_source": relation.from_source_id.as_ref().map(|v| v.as_str()).unwrap_or_default(),
        "method": relation.method.as_str(),
        "confidence": relation.confidence.as_str(),
        "surface_kind": relation.surface_kind.as_str(),
        "reason": relation.reason,
        "discovered_at": relation.discovered_at.to_rfc3339(),
        "last_confirmed_at": relation.last_confirmed_at.to_rfc3339(),
    })
}

pub(crate) fn relation_from_value(value: Value) -> Result<stream_model::DiscoveryRelation> {
    use stream_model::{DiscoveryConfidence, DiscoveryMethod, SurfaceKind, TargetId};
    Ok(stream_model::DiscoveryRelation {
        id: value_string(&value, "__id")?,
        target_id: TargetId::new(value_string(&value, "target")?),
        source_id: SourceId::new(value_string(&value, "source")?),
        discovered_from: url::Url::parse(&value_string(&value, "discovered_from")?)?,
        from_source_id: value_string_opt(&value, "from_source")?.map(SourceId::new),
        method: parse_enum(&value, "method", DiscoveryMethod::parse)?,
        confidence: parse_enum(&value, "confidence", DiscoveryConfidence::parse)?,
        surface_kind: parse_enum(&value, "surface_kind", SurfaceKind::parse)?,
        reason: value_string_opt(&value, "reason")?.unwrap_or_default(),
        discovered_at: value_datetime(&value, "discovered_at")?,
        last_confirmed_at: value_datetime(&value, "last_confirmed_at")?,
    })
}

pub(crate) fn snapshot_record(snapshot: &stream_model::PageSnapshot) -> Value {
    json!({
        "__id": snapshot.id.as_str(),
        "source": snapshot.source_id.as_str(),
        "url": snapshot.url.as_str(),
        "title": snapshot.title,
        "observed_at": snapshot.observed_at.to_rfc3339(),
        "content_hash": snapshot.content_hash,
        "blocks": serde_json::to_string(&snapshot.blocks).unwrap_or_else(|_| "[]".into()),
        "links": serde_json::to_string(&snapshot.links).unwrap_or_else(|_| "[]".into()),
        "change": snapshot.change.as_str(),
        "added": serde_json::to_string(&snapshot.added).unwrap_or_else(|_| "[]".into()),
        "removed": serde_json::to_string(&snapshot.removed).unwrap_or_else(|_| "[]".into()),
        "summary": snapshot.summary,
        "item": snapshot.item_id.as_ref().map(|v| v.as_str()).unwrap_or_default(),
    })
}

pub(crate) fn snapshot_from_value(value: Value) -> Result<stream_model::PageSnapshot> {
    use stream_model::{PageChange, SnapshotId};
    Ok(stream_model::PageSnapshot {
        id: SnapshotId::new(value_string(&value, "__id")?),
        source_id: SourceId::new(value_string(&value, "source")?),
        url: url::Url::parse(&value_string(&value, "url")?)?,
        title: value_string_opt(&value, "title")?.unwrap_or_default(),
        observed_at: value_datetime(&value, "observed_at")?,
        content_hash: value_string(&value, "content_hash")?,
        blocks: json_list(&value, "blocks"),
        links: json_list(&value, "links"),
        change: parse_enum(&value, "change", PageChange::parse)?,
        added: json_list(&value, "added"),
        removed: json_list(&value, "removed"),
        summary: value_string_opt(&value, "summary")?.unwrap_or_default(),
        item_id: value_string_opt(&value, "item")?.map(ItemId::new),
    })
}

pub(crate) fn run_record(run: &stream_model::ObservationRun) -> Value {
    json!({
        "__id": run.id.as_str(),
        "trigger": run.trigger,
        "started_at": run.started_at.to_rfc3339(),
        "completed_at": opt_time(&run.completed_at),
        "targets_discovered": run.targets_discovered,
        "sources_observed": run.sources_observed,
        "sources_failed": run.sources_failed,
        "new_items": run.new_items,
        "page_changes": run.page_changes,
        "signals_created": run.signals_created,
        "signals_corroborated": run.signals_corroborated,
        "detail": run.detail,
    })
}

pub(crate) fn run_from_value(value: Value) -> Result<stream_model::ObservationRun> {
    let count = |field: &str| value_u64(&value, field).unwrap_or(0) as usize;
    Ok(stream_model::ObservationRun {
        id: stream_model::ObservationRunId::new(value_string(&value, "__id")?),
        trigger: value_string_opt(&value, "trigger")?.unwrap_or_default(),
        started_at: value_datetime(&value, "started_at")?,
        completed_at: value_datetime_opt(&value, "completed_at")?,
        targets_discovered: count("targets_discovered"),
        sources_observed: count("sources_observed"),
        sources_failed: count("sources_failed"),
        new_items: count("new_items"),
        page_changes: count("page_changes"),
        signals_created: count("signals_created"),
        signals_corroborated: count("signals_corroborated"),
        detail: value_string_opt(&value, "detail")?.unwrap_or_default(),
    })
}
