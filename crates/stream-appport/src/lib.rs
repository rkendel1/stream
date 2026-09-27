//! Stream's AppPort surface: the portable application API.
//!
//! Every Stream client — the `stream` CLI, the desktop app, any AppPort
//! host — reaches the runtime through [`StreamAppPort::invoke`]. There is no
//! client-specific data path: the desktop app is presentation over exactly
//! these capabilities, and FeltDB stays the only authority behind them.

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use stream_core::{AskRequest, FeltDbConfig, FeltDbStore, NewContext, NewInsight, ProviderConfig, SearchQuery, StreamRuntime};
use stream_ingest::AdapterRegistry;
use stream_model::{AttentionEventId, EvidenceId, InsightId, InsightStatus, ItemId, SignalId, SignalStatus, SourceId, SourceKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    Observation,
    Consequential,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityDefinition {
    pub name: String,
    pub version: u32,
    pub description: String,
    pub effect: Effect,
    pub operations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppPortManifest {
    pub application: ApplicationDescriptor,
    pub capabilities: Vec<CapabilityDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationDescriptor {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
}

fn capability(name: &str, effect: Effect, description: &str, operations: &[&str]) -> CapabilityDefinition {
    CapabilityDefinition {
        name: name.into(),
        version: 1,
        description: description.into(),
        effect,
        operations: operations.iter().map(|op| op.to_string()).collect(),
    }
}

impl AppPortManifest {
    pub fn stream() -> Self {
        use Effect::*;
        Self {
            application: ApplicationDescriptor {
                id: "com.rkendel.stream".into(),
                name: "Stream".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                description: "Stream turns URLs into durable, connected, evidence-backed signals about what is changing around you.".into(),
            },
            capabilities: vec![
                capability("appport.manifest", Observation, "Reserved AppPort manifest capability.", &["get"]),
                capability("appport.ping", Observation, "Reserved AppPort ping capability.", &["ping"]),
                // The Stream intelligence surface.
                capability("stream.source.add", Consequential, "Add a URL. Stream determines what it is, establishes a durable source, and (unless observe=false) understands it into signals.", &["invoke"]),
                capability("stream.source.observe", Consequential, "Observe a source again: fetch, normalize, understand, connect.", &["invoke"]),
                capability("stream.source.list", Observation, "List durable sources, newest first.", &["invoke"]),
                capability("stream.source.get", Observation, "A source with its observation history, items, and signals.", &["invoke"]),
                capability("stream.item.get", Observation, "A canonical item with its state and provenance.", &["invoke"]),
                capability("stream.item.list", Observation, "All canonical items.", &["invoke"]),
                capability("stream.signal.list", Observation, "Signals ranked by information density (Today), with ranking explanations.", &["invoke"]),
                capability("stream.signal.get", Observation, "A signal with every claim's evidence traced to item, source, and URL.", &["invoke"]),
                capability("stream.signal.resolve", Consequential, "Mark a signal resolved; it leaves Today but stays durable.", &["invoke"]),
                capability("stream.signal.dismiss", Consequential, "Dismiss a signal; it leaves Today but stays durable.", &["invoke"]),
                capability("stream.evidence.get", Observation, "One piece of evidence resolved to its item, source, provenance, and URL.", &["invoke"]),
                capability("stream.context.list", Observation, "Durable user context: what the user cares about.", &["invoke"]),
                capability("stream.context.add", Consequential, "Add or refine durable user context; existing signals are re-evaluated against it.", &["invoke"]),
                capability("stream.connection.list", Observation, "Connections touching a signal, item, or context (or all).", &["invoke"]),
                capability("stream.connection.graph", Observation, "The information graph: contexts, subjects, and related observations.", &["invoke"]),
                // Reasoning over Stream: grounded answers with provenance.
                capability("stream.chat.ask", Observation, "Ask Stream a question. Answers are grounded in retrieved Stream evidence, label observed/inferred/connected/hypothesis, and say when evidence is insufficient. Optional focus constrains retrieval.", &["invoke"]),
                capability("stream.reason.retrieve", Observation, "The evidence/context bundle Stream would reason over for a question.", &["invoke"]),
                capability("stream.insight.save", Consequential, "Save reasoning (insight, hypothesis, question, decision candidate, investigation) as durable, advisory Stream knowledge, citing evidence.", &["invoke"]),
                capability("stream.insight.list", Observation, "Saved reasoning, newest first; optionally for one signal.", &["invoke"]),
                capability("stream.insight.get", Observation, "A saved insight with its evidence traced to sources.", &["invoke"]),
                capability("stream.insight.resolve", Consequential, "Mark an insight resolved.", &["invoke"]),
                capability("stream.insight.drop", Consequential, "Drop an insight; it stays durable but no longer counts as open.", &["invoke"]),
                capability("stream.intelligence.status", Observation, "Whether a model backs Stream's intelligence, and recent failures, refusals, and fallbacks.", &["invoke"]),
                // Grouped capabilities from the original runtime surface.
                capability("stream.source", Consequential, "Manage Stream sources.", &["create", "list", "get", "refresh", "pause", "resume"]),
                capability("stream.item", Consequential, "Read and mutate Stream item state.", &["get", "state", "mark_read", "save", "dismiss", "mark_important", "archive"]),
                capability("stream.query", Observation, "Query normalized Stream items.", &["list", "unread", "saved", "important", "recent"]),
                capability("stream.search", Observation, "Search the durable Stream corpus.", &["search"]),
                capability("stream.attention", Consequential, "Summarize and resolve attention state.", &["summary", "list", "resolve"]),
            ],
        }
    }

    pub fn has_capability(&self, name: &str) -> bool {
        self.capabilities.iter().any(|capability| capability.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    UnknownCapability,
    InvalidInput,
    NotFound,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppPortError {
    pub code: ErrorCode,
    pub message: String,
}

impl std::fmt::Display for AppPortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for AppPortError {}

impl From<anyhow::Error> for AppPortError {
    fn from(error: anyhow::Error) -> Self {
        Self { code: ErrorCode::Internal, message: format!("{error:#}") }
    }
}

impl From<serde_json::Error> for AppPortError {
    fn from(error: serde_json::Error) -> Self {
        Self { code: ErrorCode::InvalidInput, message: error.to_string() }
    }
}

fn not_found(what: &str, id: &str) -> AppPortError {
    AppPortError { code: ErrorCode::NotFound, message: format!("{what} not found: {id}") }
}

fn invalid(message: impl Into<String>) -> AppPortError {
    AppPortError { code: ErrorCode::InvalidInput, message: message.into() }
}

pub type AppPortResult = Result<Value, AppPortError>;

/// The transport envelope used by every Stream AppPort host.
pub fn envelope(result: AppPortResult) -> Value {
    match result {
        Ok(result) => json!({ "ok": true, "result": result }),
        Err(error) => json!({ "ok": false, "error": error }),
    }
}

#[derive(Deserialize)]
struct IdInput {
    id: String,
}

#[derive(Deserialize)]
struct AddInput {
    url: String,
    #[serde(default = "default_true")]
    observe: bool,
    #[serde(default)]
    provenance: Option<String>,
}

#[derive(Deserialize, Default)]
struct SignalListInput {
    #[serde(default)]
    include_resolved: bool,
}

#[derive(Deserialize, Default)]
struct InsightListInput {
    #[serde(default)]
    signal_id: Option<String>,
}

#[derive(Deserialize, Default)]
struct ConnectionListInput {
    #[serde(default)]
    id: Option<String>,
}

#[derive(Deserialize)]
struct OperationInput {
    operation: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    query: Option<String>,
}

fn default_true() -> bool {
    true
}

fn parse<T: serde::de::DeserializeOwned>(input: Value) -> Result<T, AppPortError> {
    let input = if input.is_null() { json!({}) } else { input };
    serde_json::from_value(input).map_err(|error| invalid(error.to_string()))
}

fn to_value<T: Serialize>(value: T) -> AppPortResult {
    serde_json::to_value(value).map_err(|error| AppPortError { code: ErrorCode::Internal, message: error.to_string() })
}

/// Locate the Stream root: `STREAM_ROOT`, else the nearest ancestor of the
/// working directory containing `feltdb.flow`, else the source checkout this
/// binary was built from.
pub fn discover_root() -> PathBuf {
    if let Some(root) = std::env::var_os("STREAM_ROOT") {
        return PathBuf::from(root);
    }
    if let Ok(cwd) = std::env::current_dir() {
        if let Some(found) = cwd.ancestors().find(|dir| dir.join("feltdb.flow").is_file()) {
            return found.to_path_buf();
        }
    }
    let built = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."));
    if built.join("feltdb.flow").is_file() {
        return built.canonicalize().unwrap_or(built);
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Every ingestion adapter Stream ships: feeds and web pages.
pub fn default_adapters() -> AdapterRegistry {
    let mut adapters = stream_rss::default_adapters();
    adapters.extend(stream_web::web_adapters());
    AdapterRegistry::new(adapters)
}

/// Build the runtime for a Stream root. A model provider is used when one is
/// configured (`STREAM_MODEL_PROVIDER`, see `ProviderConfig`); Stream boots
/// and works without one.
pub fn runtime_for_root(root: impl AsRef<Path>) -> StreamRuntime {
    let runtime = StreamRuntime::new(FeltDbStore::new(FeltDbConfig::from_env(root)), default_adapters());
    match ProviderConfig::from_env().build() {
        Ok(Some(provider)) => runtime.with_model_provider(provider),
        Ok(None) => runtime,
        Err(problem) => {
            eprintln!("stream: model provider not used ({problem}); using local intelligence");
            runtime
        }
    }
}

pub struct StreamAppPort {
    runtime: StreamRuntime,
    default_provenance: String,
}

impl StreamAppPort {
    pub fn new(runtime: StreamRuntime) -> Self {
        Self { runtime, default_provenance: "appport".into() }
    }

    /// Record who added URLs through this port (e.g. "desktop", "cli").
    pub fn with_provenance(mut self, provenance: impl Into<String>) -> Self {
        self.default_provenance = provenance.into();
        self
    }

    pub fn from_env() -> Self {
        Self::new(runtime_for_root(discover_root()))
    }

    pub fn runtime(&self) -> &StreamRuntime {
        &self.runtime
    }

    pub fn manifest(&self) -> AppPortManifest {
        AppPortManifest::stream()
    }

    /// Invoke a capability with a JSON input and get its JSON result.
    pub async fn invoke(&self, capability: &str, input: Value) -> AppPortResult {
        let runtime = &self.runtime;
        match capability {
            "appport.manifest" => to_value(self.manifest()),
            "appport.ping" => Ok(json!({ "pong": true, "application": "com.rkendel.stream" })),

            "stream.source.add" => {
                let input: AddInput = parse(input)?;
                let provenance = input.provenance.unwrap_or_else(|| self.default_provenance.clone());
                let added = runtime.add_url(&input.url, &provenance).await.map_err(|error| invalid(format!("{error:#}")))?;
                if !input.observe {
                    return to_value(added);
                }
                let report = runtime.observe_source(&added.source.id, None).await?;
                to_value(json!({ "existing": added.existing, "report": report }))
            }
            "stream.source.observe" => {
                let input: IdInput = parse(input)?;
                let id = SourceId::new(input.id.clone());
                if runtime.get_source(&id).await?.is_none() {
                    return Err(not_found("source", &input.id));
                }
                to_value(runtime.observe_source(&id, None).await?)
            }
            "stream.source.list" => to_value(runtime.list_source_views().await?),
            "stream.source.get" => {
                let input: IdInput = parse(input)?;
                runtime
                    .source_detail(&SourceId::new(input.id.clone()))
                    .await?
                    .map(to_value)
                    .unwrap_or_else(|| Err(not_found("source", &input.id)))
            }
            "stream.item.get" => {
                let input: IdInput = parse(input)?;
                runtime
                    .get_item(&ItemId::new(input.id.clone()))
                    .await?
                    .map(to_value)
                    .unwrap_or_else(|| Err(not_found("item", &input.id)))
            }
            "stream.item.list" => to_value(runtime.list_items().await?),
            "stream.signal.list" => {
                let input: SignalListInput = parse(input)?;
                if input.include_resolved {
                    to_value(runtime.list_signals().await?)
                } else {
                    to_value(runtime.today().await?)
                }
            }
            "stream.signal.get" => {
                let input: IdInput = parse(input)?;
                runtime
                    .get_signal(&SignalId::new(input.id.clone()))
                    .await?
                    .map(to_value)
                    .unwrap_or_else(|| Err(not_found("signal", &input.id)))
            }
            "stream.signal.resolve" | "stream.signal.dismiss" => {
                let input: IdInput = parse(input)?;
                let status = if capability.ends_with("resolve") { SignalStatus::Resolved } else { SignalStatus::Dismissed };
                match runtime.set_signal_status(&SignalId::new(input.id.clone()), status).await {
                    Ok(view) => to_value(view),
                    Err(error) if error.to_string().starts_with("unknown signal") => Err(not_found("signal", &input.id)),
                    Err(error) => Err(error.into()),
                }
            }
            "stream.evidence.get" => {
                let input: IdInput = parse(input)?;
                runtime
                    .get_evidence(&EvidenceId::new(input.id.clone()))
                    .await?
                    .map(to_value)
                    .unwrap_or_else(|| Err(not_found("evidence", &input.id)))
            }
            "stream.context.list" => to_value(runtime.context_views().await?),
            "stream.context.add" => {
                let input: NewContext = parse(input)?;
                runtime
                    .add_context(input)
                    .await
                    .map_err(|error| invalid(format!("{error:#}")))
                    .and_then(to_value)
            }
            "stream.connection.list" => {
                let input: ConnectionListInput = parse(input)?;
                to_value(runtime.list_connections(input.id.as_deref()).await?)
            }
            "stream.connection.graph" => to_value(runtime.connection_graph().await?),

            "stream.chat.ask" => {
                let request: AskRequest = parse(input)?;
                if request.question.trim().is_empty() {
                    return Err(invalid("ask a question"));
                }
                to_value(runtime.ask(request).await?)
            }
            "stream.reason.retrieve" => {
                let request: AskRequest = parse(input)?;
                to_value(runtime.retrieve(&request).await?)
            }
            "stream.insight.save" => {
                let input: NewInsight = parse(input)?;
                runtime.save_insight(input).await.map_err(|error| invalid(format!("{error:#}"))).and_then(to_value)
            }
            "stream.insight.list" => {
                let input: InsightListInput = parse(input)?;
                to_value(runtime.list_insights(input.signal_id.map(SignalId::new).as_ref()).await?)
            }
            "stream.insight.get" => {
                let input: IdInput = parse(input)?;
                runtime
                    .get_insight(&InsightId::new(input.id.clone()))
                    .await?
                    .map(to_value)
                    .unwrap_or_else(|| Err(not_found("insight", &input.id)))
            }
            "stream.insight.resolve" | "stream.insight.drop" => {
                let input: IdInput = parse(input)?;
                let status = if capability.ends_with("resolve") { InsightStatus::Resolved } else { InsightStatus::Dropped };
                match runtime.set_insight_status(&InsightId::new(input.id.clone()), status).await {
                    Ok(insight) => to_value(insight),
                    Err(error) if error.to_string().starts_with("unknown insight") => Err(not_found("insight", &input.id)),
                    Err(error) => Err(error.into()),
                }
            }
            "stream.intelligence.status" => {
                // Provider identity stays behind Stream: events are reported
                // without it.
                let events = runtime
                    .intelligence_events()
                    .await?
                    .into_iter()
                    .take(50)
                    .map(|e| json!({ "operation": e.operation, "status": e.status, "subject_id": e.subject_id, "detail": e.detail, "at": e.created_at }))
                    .collect::<Vec<_>>();
                Ok(json!({ "model_backed": runtime.model_backed(), "recent_events": events }))
            }

            "stream.source" | "stream.item" | "stream.query" | "stream.search" | "stream.attention" => {
                self.invoke_grouped(capability, parse(input)?).await
            }
            other => Err(AppPortError {
                code: ErrorCode::UnknownCapability,
                message: format!("unknown capability: {other}"),
            }),
        }
    }

    async fn invoke_grouped(&self, capability: &str, input: OperationInput) -> AppPortResult {
        let runtime = &self.runtime;
        let id = || input.id.clone().ok_or_else(|| invalid("missing id"));
        let query = stream_query::StreamQueryService::new(runtime);
        match (capability, input.operation.as_str()) {
            ("stream.source", "create") => {
                let endpoint = input.endpoint.clone().ok_or_else(|| invalid("missing endpoint"))?;
                let kind = match input.kind.as_deref() {
                    Some(kind) => SourceKind::parse(kind).ok_or_else(|| invalid(format!("unknown source kind: {kind}")))?,
                    None => SourceKind::Rss,
                };
                to_value(runtime.add_source(kind, &endpoint).await?)
            }
            ("stream.source", "list") => to_value(runtime.list_sources().await?),
            ("stream.source", "get") => {
                let id = id()?;
                runtime.get_source(&SourceId::new(id.clone())).await?.map(to_value).unwrap_or_else(|| Err(not_found("source", &id)))
            }
            ("stream.source", "refresh") => to_value(runtime.refresh_source(&SourceId::new(id()?)).await?),
            ("stream.source", "pause") => to_value(runtime.pause_source(&SourceId::new(id()?)).await?),
            ("stream.source", "resume") => to_value(runtime.resume_source(&SourceId::new(id()?)).await?),
            ("stream.item", "get" | "state") => {
                let id = id()?;
                runtime.get_item(&ItemId::new(id.clone())).await?.map(to_value).unwrap_or_else(|| Err(not_found("item", &id)))
            }
            ("stream.item", "mark_read") => to_value(runtime.mark_read(&ItemId::new(id()?)).await?),
            ("stream.item", "save") => to_value(runtime.save_item(&ItemId::new(id()?)).await?),
            ("stream.item", "dismiss") => to_value(runtime.dismiss_item(&ItemId::new(id()?)).await?),
            ("stream.item", "mark_important") => to_value(runtime.mark_important(&ItemId::new(id()?)).await?),
            ("stream.item", "archive") => to_value(runtime.archive_item(&ItemId::new(id()?)).await?),
            ("stream.query", "list") => to_value(query.list().await?),
            ("stream.query", "unread") => to_value(query.unread().await?),
            ("stream.query", "saved") => to_value(query.saved().await?),
            ("stream.query", "important") => to_value(query.important().await?),
            ("stream.query", "recent") => to_value(query.recent(Utc::now() - Duration::days(7)).await?),
            ("stream.search", "search") => to_value(
                runtime
                    .search_items(SearchQuery { text: input.query.clone(), ..Default::default() })
                    .await?,
            ),
            ("stream.attention", "summary") => to_value(runtime.attention_summary().await?),
            ("stream.attention", "list") => to_value(runtime.list_attention().await?),
            ("stream.attention", "resolve") => to_value(runtime.resolve_attention(&AttentionEventId::new(id()?)).await?),
            (capability, operation) => Err(invalid(format!("unknown operation {operation} for {capability}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppPortManifest;

    #[test]
    fn manifest_contains_stream_capabilities() {
        let manifest = AppPortManifest::stream();
        for name in [
            "stream.source", "stream.item", "stream.query", "stream.search", "stream.attention",
            "stream.source.add", "stream.source.list", "stream.item.get", "stream.item.list",
            "stream.signal.get", "stream.signal.list", "stream.context.list", "stream.context.add",
            "stream.connection.list",
        ] {
            assert!(manifest.has_capability(name), "{name}");
        }
    }
}
