use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use url::Url;
use uuid::Uuid;

mod canonical;
mod intelligence;

pub use canonical::{canonicalize_url, classify_url, CanonicalUrlError};
pub use intelligence::{
    slug, Change, ChangeKind, ClaimKind, Connection, ConnectionRelation, ConnectionTargetKind, ContextEntry,
    ContextKind, Evidence, EvidenceLocator, ProcessingStage, Signal, SignalStatus, Subject, Topic,
};

macro_rules! typed_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn generate() -> Self {
                Self(format!("{}_{}", $prefix, Uuid::new_v4()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

typed_id!(SourceId, "source");
typed_id!(ItemId, "item");
typed_id!(RuleId, "rule");
typed_id!(AttentionEventId, "attention");
typed_id!(FetchAttemptId, "fetch");
typed_id!(SubscriptionId, "subscription");
typed_id!(SemanticDecisionId, "semantic");
typed_id!(RuleExecutionId, "rule_execution");
typed_id!(ProvenanceId, "provenance");
typed_id!(ItemRelationId, "relation");
typed_id!(ItemStateId, "item_state");
typed_id!(ContextId, "context");
typed_id!(SignalId, "signal");
typed_id!(EvidenceId, "evidence");
typed_id!(ConnectionId, "connection");

impl SourceId {
    /// A deterministic ID for a canonical identity, so that adding the same
    /// URL twice — even concurrently — resolves to the same durable source.
    pub fn for_identity(identity: &str) -> Self {
        let digest = Sha256::digest(identity.as_bytes());
        Self(format!("source_{}", &format!("{:x}", digest)[..32]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fingerprint(String);

impl Fingerprint {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn from_canonical_parts(parts: &CanonicalFingerprintParts) -> Self {
        let mut payload = BTreeMap::new();
        payload.insert("canonical_identity", Value::String(parts.canonical_identity.clone()));
        payload.insert(
            "canonical_url",
            parts.canonical_url
                .as_ref()
                .map(|value| Value::String(value.as_str().to_owned()))
                .unwrap_or(Value::Null),
        );
        payload.insert("title", Value::String(normalize_whitespace(&parts.title)));
        payload.insert("content_text", Value::String(normalize_whitespace(&parts.content_text)));
        payload.insert(
            "author",
            parts.author
                .clone()
                .map(|value| Value::String(normalize_whitespace(&value)))
                .unwrap_or(Value::Null),
        );
        payload.insert(
            "published_at",
            parts.published_at
                .map(|value| Value::String(value.to_rfc3339()))
                .unwrap_or(Value::Null),
        );

        let encoded = serde_json::to_vec(&payload).expect("fingerprint payload must serialize");
        let digest = Sha256::digest(encoded);
        Self(format!("{:x}", digest))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Fingerprint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone)]
pub struct CanonicalFingerprintParts {
    pub canonical_identity: String,
    pub canonical_url: Option<Url>,
    pub title: String,
    pub content_text: String,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Rss,
    Atom,
    JsonFeed,
    Web,
    Github,
    Youtube,
    Email,
    Webhook,
    Api,
    Appport,
    Documentation,
    Research,
}

impl SourceKind {
    pub const ALL: [SourceKind; 12] = [
        SourceKind::Rss,
        SourceKind::Atom,
        SourceKind::JsonFeed,
        SourceKind::Web,
        SourceKind::Github,
        SourceKind::Youtube,
        SourceKind::Email,
        SourceKind::Webhook,
        SourceKind::Api,
        SourceKind::Appport,
        SourceKind::Documentation,
        SourceKind::Research,
    ];

    pub fn parse(value: &str) -> Option<SourceKind> {
        Self::ALL.into_iter().find(|kind| kind.to_string() == value)
    }

    /// Feed formats are ingestion adapters, never the user-facing source model.
    pub fn is_feed_format(self) -> bool {
        matches!(self, SourceKind::Rss | SourceKind::Atom | SourceKind::JsonFeed)
    }
}

impl Display for SourceKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Rss => "rss",
            Self::Atom => "atom",
            Self::JsonFeed => "json_feed",
            Self::Web => "web",
            Self::Github => "github",
            Self::Youtube => "youtube",
            Self::Email => "email",
            Self::Webhook => "webhook",
            Self::Api => "api",
            Self::Appport => "appport",
            Self::Documentation => "documentation",
            Self::Research => "research",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    Discovered,
    Active,
    Paused,
    Failed,
    Disabled,
    Deleted,
}

impl Display for SourceStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Discovered => "discovered",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Failed => "failed",
            Self::Disabled => "disabled",
            Self::Deleted => "deleted",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    Network,
    Authentication,
    Authorization,
    Parsing,
    Normalization,
    Deduplication,
    Storage,
    Semantic,
    Provider,
    Delivery,
}

impl Display for FailureCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Network => "network",
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::Parsing => "parsing",
            Self::Normalization => "normalization",
            Self::Deduplication => "deduplication",
            Self::Storage => "storage",
            Self::Semantic => "semantic",
            Self::Provider => "provider",
            Self::Delivery => "delivery",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemState {
    Unseen,
    Seen,
    Read,
    Saved,
    Dismissed,
    Important,
    ActedOn,
    Archived,
}

impl Display for ItemState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Unseen => "unseen",
            Self::Seen => "seen",
            Self::Read => "read",
            Self::Saved => "saved",
            Self::Dismissed => "dismissed",
            Self::Important => "important",
            Self::ActedOn => "acted_on",
            Self::Archived => "archived",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    Duplicate,
    SameStory,
    References,
    DerivedFrom,
    Related,
}

impl Display for RelationKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Duplicate => "duplicate",
            Self::SameStory => "same_story",
            Self::References => "references",
            Self::DerivedFrom => "derived_from",
            Self::Related => "related",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleAction {
    Retain,
    Save,
    MarkImportant,
    CreateAttentionEvent,
}

impl Display for RuleAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Retain => "retain",
            Self::Save => "save",
            Self::MarkImportant => "mark_important",
            Self::CreateAttentionEvent => "create_attention_event",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchStatus {
    Started,
    Succeeded,
    Failed,
}

impl Display for FetchStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleExecutionResult {
    Matched,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    Active,
    Paused,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionStatus {
    Open,
    Resolved,
    Dismissed,
}

impl Display for AttentionStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
            Self::Dismissed => "dismissed",
        };
        f.write_str(value)
    }
}

/// A durable source of information.
///
/// `kind` is the user-facing classification (web page, GitHub, research, ...).
/// `adapter_kind` is the ingestion adapter used to observe `endpoint` — RSS,
/// Atom, and JSON Feed live here, underneath the generic source model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub id: SourceId,
    pub kind: SourceKind,
    pub adapter_kind: SourceKind,
    pub endpoint: Url,
    pub identity: String,
    pub canonical_url: Url,
    pub original_url: Url,
    pub title: Option<String>,
    pub stage: ProcessingStage,
    pub stage_detail: Option<String>,
    pub provenance: String,
    pub discovered_at: DateTime<Utc>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub configuration: Value,
    pub status: SourceStatus,
    pub refresh_minutes: u32,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_error_category: Option<FailureCategory>,
    pub last_error_message: Option<String>,
    pub consecutive_failures: u32,
    pub fetched_items_count: u64,
    pub duplicate_items_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Source {
    pub fn new(kind: SourceKind, endpoint: Url) -> Self {
        let now = Utc::now();
        Self {
            id: SourceId::generate(),
            kind,
            adapter_kind: kind,
            identity: endpoint.as_str().to_owned(),
            canonical_url: endpoint.clone(),
            original_url: endpoint.clone(),
            title: None,
            stage: ProcessingStage::Queued,
            stage_detail: None,
            provenance: "configured".into(),
            discovered_at: now,
            last_observed_at: None,
            endpoint,
            configuration: json!({}),
            status: SourceStatus::Active,
            refresh_minutes: 60,
            last_checked_at: None,
            last_success_at: None,
            last_failure_at: None,
            last_error_category: None,
            last_error_message: None,
            consecutive_failures: 0,
            fetched_items_count: 0,
            duplicate_items_count: 0,
            created_at: now,
            updated_at: now,
        }
    }

    /// A source established from a URL the user handed to Stream.
    ///
    /// The canonical URL is the durable identity; the original URL is kept
    /// verbatim as provenance and is what Stream fetches first.
    pub fn from_user_url(original_url: Url, canonical_url: Url, provenance: impl Into<String>) -> Self {
        let kind = classify_url(&canonical_url);
        let mut source = Self::new(kind, original_url.clone());
        source.id = SourceId::for_identity(canonical_url.as_str());
        source.adapter_kind = if kind.is_feed_format() { kind } else { SourceKind::Web };
        source.identity = canonical_url.as_str().to_owned();
        source.canonical_url = canonical_url;
        source.original_url = original_url;
        source.status = SourceStatus::Discovered;
        source.provenance = provenance.into();
        source
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchAttempt {
    pub id: FetchAttemptId,
    pub source_id: SourceId,
    pub attempted_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: FetchStatus,
    pub failure_category: Option<FailureCategory>,
    pub item_count: u64,
    pub duplicate_count: u64,
    pub retained_count: u64,
    pub diagnostics: Value,
}

impl FetchAttempt {
    pub fn started(source_id: SourceId) -> Self {
        Self {
            id: FetchAttemptId::generate(),
            source_id,
            attempted_at: Utc::now(),
            completed_at: None,
            status: FetchStatus::Started,
            failure_category: None,
            item_count: 0,
            duplicate_count: 0,
            retained_count: 0,
            diagnostics: json!({}),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResult {
    pub attempt: FetchAttempt,
    pub new_item_ids: Vec<ItemId>,
    pub duplicate_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: ItemId,
    pub source_id: SourceId,
    pub source_kind: SourceKind,
    pub canonical_identity: String,
    pub canonical_url: Option<Url>,
    pub title: String,
    pub content_text: String,
    pub content_html: Option<String>,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub fingerprint: Fingerprint,
    pub provenance_summary: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemRelation {
    pub id: ItemRelationId,
    pub from_item_id: ItemId,
    pub to_item_id: ItemId,
    pub relation: RelationKind,
    pub evidence: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemStateRecord {
    pub id: ItemStateId,
    pub item_id: ItemId,
    pub state: ItemState,
    pub seen_at: Option<DateTime<Utc>>,
    pub read_at: Option<DateTime<Utc>>,
    pub saved_at: Option<DateTime<Utc>>,
    pub dismissed_at: Option<DateTime<Utc>>,
    pub important_at: Option<DateTime<Utc>>,
    pub acted_on_at: Option<DateTime<Utc>>,
    pub archived_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl ItemStateRecord {
    pub fn unseen(item_id: ItemId) -> Self {
        Self {
            id: ItemStateId::generate(),
            item_id,
            state: ItemState::Unseen,
            seen_at: None,
            read_at: None,
            saved_at: None,
            dismissed_at: None,
            important_at: None,
            acted_on_at: None,
            archived_at: None,
            updated_at: Utc::now(),
        }
    }

    pub fn transition(&mut self, state: ItemState, at: DateTime<Utc>) {
        self.state = state;
        self.updated_at = at;
        match state {
            ItemState::Unseen => {}
            ItemState::Seen => self.seen_at = Some(at),
            ItemState::Read => self.read_at = Some(at),
            ItemState::Saved => self.saved_at = Some(at),
            ItemState::Dismissed => self.dismissed_at = Some(at),
            ItemState::Important => self.important_at = Some(at),
            ItemState::ActedOn => self.acted_on_at = Some(at),
            ItemState::Archived => self.archived_at = Some(at),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub id: RuleId,
    pub name: String,
    pub enabled: bool,
    pub source_filter: Option<SourceId>,
    pub source_kind_filter: Option<SourceKind>,
    pub title_pattern: Option<String>,
    pub content_pattern: Option<String>,
    pub url_pattern: Option<String>,
    pub author_pattern: Option<String>,
    pub published_after: Option<DateTime<Utc>>,
    pub action: RuleAction,
    pub created_by: Option<String>,
    pub updated_by: Option<String>,
    pub explanation: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleExecution {
    pub id: RuleExecutionId,
    pub rule_id: RuleId,
    pub item_id: ItemId,
    pub executed_at: DateTime<Utc>,
    pub result: RuleExecutionResult,
    pub failure: Option<String>,
    pub attention_event_id: Option<AttentionEventId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: SubscriptionId,
    pub source_id: SourceId,
    pub name: String,
    pub query: String,
    pub status: SubscriptionStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticDecision {
    pub id: SemanticDecisionId,
    pub item_id: ItemId,
    pub model: String,
    pub decision_type: String,
    pub decision: String,
    pub advisory_state: String,
    pub confidence: f32,
    pub explanation: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttentionEvent {
    pub id: AttentionEventId,
    pub item_id: ItemId,
    pub rule_id: Option<RuleId>,
    pub status: AttentionStatus,
    pub summary: String,
    pub rationale: String,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AttentionSummary {
    pub new_items: usize,
    pub unread_items: usize,
    pub saved_items: usize,
    pub important_items: usize,
    pub new_attention_events: usize,
    pub failed_sources: usize,
    pub stale_sources: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub id: ProvenanceId,
    pub item_id: ItemId,
    pub source_id: SourceId,
    pub source_url: Url,
    pub observed_at: DateTime<Utc>,
    pub parser: String,
    pub original_identifier: String,
    pub canonical_identifier: String,
    pub fingerprint: Fingerprint,
    pub transformations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedItem {
    pub source_kind: SourceKind,
    pub canonical_identity: String,
    pub canonical_url: Option<Url>,
    pub title: String,
    pub content_text: String,
    pub content_html: Option<String>,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub original_identifier: String,
    pub source_url: Url,
    pub parser: String,
    pub transformations: Vec<String>,
}

impl NormalizedItem {
    pub fn fingerprint(&self) -> Fingerprint {
        Fingerprint::from_canonical_parts(&CanonicalFingerprintParts {
            canonical_identity: self.canonical_identity.clone(),
            canonical_url: self.canonical_url.clone(),
            title: self.title.clone(),
            content_text: self.content_text.clone(),
            author: self.author.clone(),
            published_at: self.published_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedItem {
    pub item: Item,
    pub provenance: Provenance,
    pub state: ItemStateRecord,
}

pub fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}
