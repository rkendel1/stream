use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use tokio::process::Command;
use stream_ingest::{AdapterRegistry, HttpFetcher};
use stream_model::{
    AttentionEvent, AttentionEventId, AttentionStatus, AttentionSummary, FetchAttempt, FetchResult,
    FetchStatus, Fingerprint, Item, ItemId, ItemRelation, ItemState, ItemStateRecord, NormalizedItem,
    Provenance, ProvenanceId, RelationKind, Rule, RuleAction, RuleExecution, RuleExecutionId,
    ProcessingStage, RuleExecutionResult, RuleId, Source, SourceId, SourceKind, SourceStatus,
};
use stream_rules::rule_matches;
use stream_reason::{LocalReasoner, LocalSynthesizer, ModelReasoner, ModelSynthesizer, Reasoner, Synthesizer};
use stream_semantic::{HeuristicInterpreter, Interpreter, ModelInterpreter, ModelProvider};

mod intelligence;
mod reasoning;
mod records;

pub use intelligence::*;
pub use reasoning::*;
/// Provider types, re-exported so runtime hosts can configure a model
/// without depending on the semantic crate directly.
pub use stream_semantic::{ModelProvider as ModelProviderHandle, OpenAiCompatibleProvider, ProviderConfig};

/// The FeltDB bridge: one long-lived Node process speaking JSON lines.
///
/// FeltDB remains the only authority. The bridge holds no state of its own
/// beyond open FeltDB handles, and FeltDB file-runtime handles observe writes
/// made by other processes (the CLI and the desktop app can run side by side).
const FELTDB_BRIDGE: &str = r#"
import { createFeltDB } from '@feltdb/core';
import { createInterface } from 'node:readline';

const handles = new Map();
function database(namespace, path) {
  const key = `${namespace}\u0000${path}`;
  if (!handles.has(key)) handles.set(key, createFeltDB({ namespace, path }));
  return handles.get(key);
}

async function execute(input) {
  const collection = database(input.namespace, input.path).collection(input.collection);
  switch (input.op) {
    case 'insert':
      await collection.insert(input.record, input.id, input.requireAbsent ? { requireAbsent: true } : undefined);
      return input.record;
    case 'get':
      return (await collection.get(input.id)) ?? null;
    case 'find':
      return await collection.find(input.query ?? {});
    case 'update':
      await collection.update(input.id, input.record);
      return input.record;
    default:
      throw new Error(`unsupported op: ${input.op}`);
  }
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (!line.trim()) continue;
  let response;
  try {
    response = { ok: true, result: await execute(JSON.parse(line)) };
  } catch (error) {
    response = { ok: false, error: String(error?.stack ?? error) };
  }
  process.stdout.write(JSON.stringify(response) + '\n');
}
"#;

#[derive(Debug, Clone)]
pub struct FeltDbConfig {
    pub namespace: String,
    pub path: PathBuf,
    pub node_binary: String,
    /// Directory the bridge runs in; `@feltdb/core` is resolved from here.
    pub working_dir: PathBuf,
}

impl FeltDbConfig {
    pub fn from_env(repo_root: impl AsRef<Path>) -> Self {
        let repo_root = repo_root.as_ref();
        let namespace = env::var("VITE_FELTDB_NAMESPACE").unwrap_or_else(|_| "stream".into());
        let path = env::var("STREAM_FELTDB_PATH")
            .map(PathBuf::from)
            .map(|path| if path.is_relative() { repo_root.join(path) } else { path })
            .unwrap_or_else(|_| repo_root.join(".feltdb-data").join("stream"));
        let node_binary = env::var("STREAM_NODE_BINARY").unwrap_or_else(|_| "node".into());
        Self {
            namespace,
            path,
            node_binary,
            working_dir: repo_root.to_path_buf(),
        }
    }

    /// A config rooted at `working_dir` with an explicit namespace and data path.
    pub fn at(working_dir: impl Into<PathBuf>, namespace: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            namespace: namespace.into(),
            path: path.into(),
            node_binary: env::var("STREAM_NODE_BINARY").unwrap_or_else(|_| "node".into()),
            working_dir: working_dir.into(),
        }
    }
}

struct BridgeSession {
    _child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::io::BufReader<tokio::process::ChildStdout>,
    stderr: std::sync::Arc<std::sync::Mutex<String>>,
}

#[derive(Clone)]
pub struct FeltDbStore {
    config: FeltDbConfig,
    session: std::sync::Arc<tokio::sync::Mutex<Option<BridgeSession>>>,
}

impl std::fmt::Debug for FeltDbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeltDbStore").field("config", &self.config).finish()
    }
}

impl FeltDbStore {
    pub fn new(config: FeltDbConfig) -> Self {
        Self {
            config,
            session: Default::default(),
        }
    }

    pub fn config(&self) -> &FeltDbConfig {
        &self.config
    }

    fn spawn_session(&self) -> Result<BridgeSession> {
        use tokio::io::AsyncBufReadExt;
        let mut command = Command::new(&self.config.node_binary);
        command
            .arg("--input-type=module")
            .arg("-e")
            .arg(FELTDB_BRIDGE)
            .current_dir(&self.config.working_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // Stream is local-first: FeltDB must not phone home unless asked to.
        if env::var_os("FELTDB_TELEMETRY").is_none() {
            command.env("FELTDB_TELEMETRY", "0");
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn {} for FeltDB bridge", self.config.node_binary))?;
        let stdin = child.stdin.take().context("missing bridge stdin")?;
        let stdout = tokio::io::BufReader::new(child.stdout.take().context("missing bridge stdout")?);
        let stderr_pipe = child.stderr.take().context("missing bridge stderr")?;
        let stderr = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let sink = stderr.clone();
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stderr_pipe).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut buffer = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                buffer.push_str(&line);
                buffer.push('\n');
                if buffer.len() > 8192 {
                    let cut = buffer.len() - 4096;
                    let cut = (cut..buffer.len()).find(|index| buffer.is_char_boundary(*index)).unwrap_or(0);
                    buffer.drain(..cut);
                }
            }
        });
        Ok(BridgeSession {
            _child: child,
            stdin,
            stdout,
            stderr,
        })
    }

    async fn invoke(&self, collection: &str, op: &str, payload: Value) -> Result<Value> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let mut input = serde_json::Map::new();
        input.insert("namespace".into(), Value::String(self.config.namespace.clone()));
        input.insert("path".into(), Value::String(self.config.path.display().to_string()));
        input.insert("collection".into(), Value::String(collection.into()));
        input.insert("op".into(), Value::String(op.into()));
        if let Value::Object(map) = payload {
            input.extend(map);
        }
        let mut request = serde_json::to_vec(&Value::Object(input))?;
        request.push(b'\n');

        let mut guard = self.session.lock().await;
        if guard.is_none() {
            *guard = Some(self.spawn_session()?);
        }
        let session = guard.as_mut().expect("bridge session present");
        let mut line = String::new();
        let io = async {
            session.stdin.write_all(&request).await?;
            session.stdin.flush().await?;
            session.stdout.read_line(&mut line).await
        }
        .await;
        match io {
            Ok(read) if read > 0 => {}
            outcome => {
                let stderr = session.stderr.lock().map(|buffer| buffer.clone()).unwrap_or_default();
                *guard = None;
                let reason = match outcome {
                    Err(error) => error.to_string(),
                    _ => "bridge exited".into(),
                };
                return Err(anyhow!("FeltDB bridge failed ({reason}): {}", stderr.trim()));
            }
        }
        drop(guard);

        let response: BridgeResponse =
            serde_json::from_str(&line).context("failed to parse FeltDB bridge response")?;
        if !response.ok {
            return Err(anyhow!("FeltDB {op} on {collection} failed: {}", response.error.unwrap_or_default()));
        }
        Ok(response.result)
    }

    pub async fn insert(&self, collection: &str, id: &str, record: Value, require_absent: bool) -> Result<Value> {
        self.invoke(
            collection,
            "insert",
            json!({ "id": id, "record": record, "requireAbsent": require_absent }),
        )
        .await
    }

    pub async fn update(&self, collection: &str, id: &str, record: Value) -> Result<Value> {
        self.invoke(collection, "update", json!({ "id": id, "record": record })).await
    }

    pub async fn get(&self, collection: &str, id: &str) -> Result<Option<Value>> {
        let value = self.invoke(collection, "get", json!({ "id": id })).await?;
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    pub async fn find(&self, collection: &str, query: Value) -> Result<Vec<Value>> {
        let value = self.invoke(collection, "find", json!({ "query": query })).await?;
        serde_json::from_value(value).context("failed to decode FeltDB find response")
    }

    pub async fn all(&self, collection: &str) -> Result<Vec<Value>> {
        self.find(collection, json!({})).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemView {
    pub item: Item,
    pub state: ItemStateRecord,
    pub provenance: Vec<Provenance>,
}

#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub source_id: Option<SourceId>,
    pub source_kind: Option<SourceKind>,
    pub state: Option<ItemState>,
    pub after: Option<DateTime<Utc>>,
}

pub struct StreamRuntime {
    store: FeltDbStore,
    fetcher: HttpFetcher,
    adapters: AdapterRegistry,
    interpreter: std::sync::Arc<dyn Interpreter>,
    /// Used when the primary interpreter fails or its proposal is refused.
    fallback_interpreter: Option<std::sync::Arc<dyn Interpreter>>,
    synthesizer: std::sync::Arc<dyn Synthesizer>,
    fallback_synthesizer: Option<std::sync::Arc<dyn Synthesizer>>,
    reasoner: std::sync::Arc<dyn Reasoner>,
    fallback_reasoner: Option<std::sync::Arc<dyn Reasoner>>,
}

impl StreamRuntime {
    pub fn new(store: FeltDbStore, adapters: AdapterRegistry) -> Self {
        Self {
            store,
            fetcher: HttpFetcher::default(),
            adapters,
            interpreter: std::sync::Arc::new(HeuristicInterpreter),
            fallback_interpreter: None,
            synthesizer: std::sync::Arc::new(LocalSynthesizer),
            fallback_synthesizer: None,
            reasoner: std::sync::Arc::new(LocalReasoner),
            fallback_reasoner: None,
        }
    }

    /// Replace the semantic provider. Interpretation stays advisory whichever
    /// provider is used: every proposal passes the same evidence gate.
    pub fn with_interpreter(mut self, interpreter: std::sync::Arc<dyn Interpreter>) -> Self {
        self.interpreter = interpreter;
        self
    }

    /// Use a model for interpretation, synthesis, and reasoning. The local,
    /// deterministic implementations remain as fallbacks, so Stream keeps
    /// working — and says so durably — when the model is unavailable.
    pub fn with_model_provider(mut self, provider: std::sync::Arc<dyn ModelProvider>) -> Self {
        self.interpreter = std::sync::Arc::new(ModelInterpreter::new(provider.clone()));
        self.fallback_interpreter = Some(std::sync::Arc::new(HeuristicInterpreter));
        self.synthesizer = std::sync::Arc::new(ModelSynthesizer::new(provider.clone()));
        self.fallback_synthesizer = Some(std::sync::Arc::new(LocalSynthesizer));
        self.reasoner = std::sync::Arc::new(ModelReasoner::new(provider));
        self.fallback_reasoner = Some(std::sync::Arc::new(LocalReasoner));
        self
    }

    pub fn with_reasoner(mut self, reasoner: std::sync::Arc<dyn Reasoner>) -> Self {
        self.reasoner = reasoner;
        self
    }

    /// True when a model backs the intelligence layer (for diagnostics only).
    pub fn model_backed(&self) -> bool {
        self.fallback_interpreter.is_some()
    }

    pub fn interpreter_id(&self) -> &str {
        self.interpreter.id()
    }

    pub fn store(&self) -> &FeltDbStore {
        &self.store
    }

    pub async fn add_source(&self, kind: SourceKind, endpoint: &str) -> Result<Source> {
        let endpoint = url::Url::parse(endpoint)?;
        if let Some(existing) = self.find_source_by_endpoint(endpoint.as_str()).await? {
            return Ok(existing);
        }
        let source = Source::new(kind, endpoint);
        self.store
            .insert("Source", source.id.as_str(), source_record(&source), true)
            .await?;
        Ok(source)
    }

    pub async fn list_sources(&self) -> Result<Vec<Source>> {
        self.store
            .all("Source")
            .await?
            .into_iter()
            .map(source_from_value)
            .collect()
    }

    pub async fn get_source(&self, source_id: &SourceId) -> Result<Option<Source>> {
        Ok(self
            .store
            .get("Source", source_id.as_str())
            .await?
            .map(source_from_value)
            .transpose()?)
    }

    pub async fn pause_source(&self, source_id: &SourceId) -> Result<Source> {
        self.set_source_status(source_id, SourceStatus::Paused).await
    }

    pub async fn resume_source(&self, source_id: &SourceId) -> Result<Source> {
        self.set_source_status(source_id, SourceStatus::Active).await
    }

    pub async fn refresh_source(&self, source_id: &SourceId) -> Result<FetchResult> {
        let mut source = self
            .get_source(source_id)
            .await?
            .ok_or_else(|| anyhow!("unknown source: {}", source_id))?;
        source.last_checked_at = Some(Utc::now());
        source.updated_at = Utc::now();
        self.store.update("Source", source.id.as_str(), source_record(&source)).await?;

        let mut attempt = FetchAttempt::started(source.id.clone());
        self.store
            .insert("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(&attempt), true)
            .await?;

        let body = match self.fetcher.fetch(&source).await {
            Ok(body) => body,
            Err(error) => {
                source.status = SourceStatus::Failed;
                source.last_failure_at = Some(Utc::now());
                source.last_error_message = Some(error.to_string());
                source.updated_at = Utc::now();
                self.store.update("Source", source.id.as_str(), source_record(&source)).await?;
                attempt.status = FetchStatus::Failed;
                attempt.completed_at = Some(Utc::now());
                attempt.diagnostics = json!({ "message": error.to_string() });
                self.store.update("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(&attempt)).await?;
                return Err(error);
            }
        };

        let adapter = self.adapters.adapter_for(&source)?;
        let normalized_items = adapter.parse(&source, &body, Utc::now())?;
        self.persist_fetch_result(source, attempt, normalized_items).await
    }

    pub async fn ingest_items_for_source(
        &self,
        source_id: &SourceId,
        normalized_items: Vec<NormalizedItem>,
    ) -> Result<FetchResult> {
        let source = self
            .get_source(source_id)
            .await?
            .ok_or_else(|| anyhow!("unknown source: {}", source_id))?;
        let attempt = FetchAttempt::started(source.id.clone());
        self.store
            .insert("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(&attempt), true)
            .await?;
        self.persist_fetch_result(source, attempt, normalized_items).await
    }

    pub async fn list_items(&self) -> Result<Vec<ItemView>> {
        let items: Vec<Item> = self.store.all("Item").await?.into_iter().map(item_from_value).collect::<Result<_>>()?;
        self.hydrate_items(items).await
    }

    pub async fn get_item(&self, item_id: &ItemId) -> Result<Option<ItemView>> {
        let item = match self.store.get("Item", item_id.as_str()).await? {
            Some(value) => item_from_value(value)?,
            None => return Ok(None),
        };
        Ok(self.hydrate_items(vec![item]).await?.into_iter().next())
    }

    pub async fn search_items(&self, query: SearchQuery) -> Result<Vec<ItemView>> {
        let items = self.list_items().await?;
        let tokens = query
            .text
            .as_deref()
            .unwrap_or_default()
            .split_whitespace()
            .map(|token| token.to_ascii_lowercase())
            .collect::<Vec<_>>();

        Ok(items
            .into_iter()
            .filter(|view| {
                if let Some(source_id) = &query.source_id {
                    if &view.item.source_id != source_id {
                        return false;
                    }
                }
                if let Some(source_kind) = query.source_kind {
                    if view.item.source_kind != source_kind {
                        return false;
                    }
                }
                if let Some(state) = query.state {
                    if view.state.state != state {
                        return false;
                    }
                }
                if let Some(after) = query.after {
                    match view.item.published_at {
                        Some(published_at) if published_at > after => {}
                        Some(_) => return false,
                        None => return false,
                    }
                }
                let haystack = format!(
                    "{} {} {} {}",
                    view.item.title,
                    view.item.content_text,
                    view.item.author.clone().unwrap_or_default(),
                    view.item
                        .canonical_url
                        .as_ref()
                        .map(|url| url.as_str().to_owned())
                        .unwrap_or_default()
                )
                .to_ascii_lowercase();
                tokens.iter().all(|token| haystack.contains(token))
            })
            .collect())
    }

    pub async fn list_attention(&self) -> Result<Vec<AttentionEvent>> {
        self.store
            .all("AttentionEvent")
            .await?
            .into_iter()
            .map(attention_from_value)
            .collect()
    }

    pub async fn resolve_attention(&self, attention_id: &AttentionEventId) -> Result<AttentionEvent> {
        let mut event = self
            .store
            .get("AttentionEvent", attention_id.as_str())
            .await?
            .map(attention_from_value)
            .transpose()?
            .ok_or_else(|| anyhow!("unknown attention event: {}", attention_id))?;
        event.status = AttentionStatus::Resolved;
        event.resolved_at = Some(Utc::now());
        self.store
            .update("AttentionEvent", event.id.as_str(), attention_record(&event))
            .await?;
        Ok(event)
    }

    pub async fn mark_seen(&self, item_id: &ItemId) -> Result<ItemStateRecord> {
        self.transition_state(item_id, ItemState::Seen).await
    }

    pub async fn mark_read(&self, item_id: &ItemId) -> Result<ItemStateRecord> {
        self.transition_state(item_id, ItemState::Read).await
    }

    pub async fn save_item(&self, item_id: &ItemId) -> Result<ItemStateRecord> {
        self.transition_state(item_id, ItemState::Saved).await
    }

    pub async fn dismiss_item(&self, item_id: &ItemId) -> Result<ItemStateRecord> {
        self.transition_state(item_id, ItemState::Dismissed).await
    }

    pub async fn mark_important(&self, item_id: &ItemId) -> Result<ItemStateRecord> {
        self.transition_state(item_id, ItemState::Important).await
    }

    pub async fn archive_item(&self, item_id: &ItemId) -> Result<ItemStateRecord> {
        self.transition_state(item_id, ItemState::Archived).await
    }

    pub async fn attention_summary(&self) -> Result<AttentionSummary> {
        let items = self.list_items().await?;
        let attention_events = self.list_attention().await?;
        let sources = self.list_sources().await?;
        let now = Utc::now();
        Ok(AttentionSummary {
            new_items: items.iter().filter(|item| item.state.state == ItemState::Unseen).count(),
            unread_items: items
                .iter()
                .filter(|item| matches!(item.state.state, ItemState::Unseen | ItemState::Seen))
                .count(),
            saved_items: items.iter().filter(|item| item.state.state == ItemState::Saved).count(),
            important_items: items.iter().filter(|item| item.state.state == ItemState::Important).count(),
            new_attention_events: attention_events
                .iter()
                .filter(|event| event.status == AttentionStatus::Open)
                .count(),
            failed_sources: sources.iter().filter(|source| source.status == SourceStatus::Failed).count(),
            stale_sources: sources
                .iter()
                .filter(|source| {
                    source
                        .last_success_at
                        .or(source.last_checked_at)
                        .map(|timestamp| now - timestamp > Duration::hours(48))
                        .unwrap_or(false)
                })
                .count(),
        })
    }

    pub async fn add_rule(&self, rule: Rule) -> Result<Rule> {
        self.store.insert("Rule", rule.id.as_str(), rule_record(&rule), true).await?;
        Ok(rule)
    }

    pub async fn list_rules(&self) -> Result<Vec<Rule>> {
        self.store.all("Rule").await?.into_iter().map(rule_from_value).collect()
    }

    async fn apply_rules(&self, item: &Item) -> Result<Vec<RuleExecution>> {
        let rules = self.list_rules().await?;
        let mut executions = Vec::new();
        for rule in rules {
            let matched = rule_matches(&rule, item);
            let mut attention_event_id = None;
            let mut failure = None;
            let result = if matched {
                if let Err(error) = self.apply_rule_action(&rule, item, &mut attention_event_id).await {
                    failure = Some(error.to_string());
                    RuleExecutionResult::Failed
                } else {
                    RuleExecutionResult::Matched
                }
            } else {
                RuleExecutionResult::Skipped
            };

            let execution = RuleExecution {
                id: RuleExecutionId::generate(),
                rule_id: rule.id.clone(),
                item_id: item.id.clone(),
                executed_at: Utc::now(),
                result,
                failure,
                attention_event_id,
            };
            self.store
                .insert(
                    "RuleExecution",
                    execution.id.as_str(),
                    rule_execution_record(&execution),
                    true,
                )
                .await?;
            executions.push(execution);
        }
        Ok(executions)
    }

    async fn apply_rule_action(
        &self,
        rule: &Rule,
        item: &Item,
        attention_event_id: &mut Option<AttentionEventId>,
    ) -> Result<()> {
        match rule.action {
            RuleAction::Retain => Ok(()),
            RuleAction::Save => {
                self.save_item(&item.id).await?;
                Ok(())
            }
            RuleAction::MarkImportant => {
                self.mark_important(&item.id).await?;
                Ok(())
            }
            RuleAction::CreateAttentionEvent => {
                let event = AttentionEvent {
                    id: AttentionEventId::generate(),
                    item_id: item.id.clone(),
                    rule_id: Some(rule.id.clone()),
                    status: AttentionStatus::Open,
                    summary: format!("Rule '{}' matched", rule.name),
                    rationale: rule.explanation.clone().unwrap_or_else(|| "Stream rule matched".into()),
                    created_at: Utc::now(),
                    resolved_at: None,
                };
                self.store
                    .insert("AttentionEvent", event.id.as_str(), attention_record(&event), true)
                    .await?;
                *attention_event_id = Some(event.id);
                Ok(())
            }
        }
    }

    async fn persist_normalized_item(&self, source: &Source, normalized_item: &NormalizedItem) -> Result<PersistOutcome> {
        let fingerprint = normalized_item.fingerprint();
        if let Some(existing) = self.find_item_by_fingerprint(&fingerprint).await? {
            self.record_observation(source, &existing.id, normalized_item, &fingerprint).await?;
            return Ok(PersistOutcome { item: existing, is_new: false });
        }

        if let Some(existing) = self.find_item_by_identity(&normalized_item.canonical_identity).await? {
            self.record_observation(source, &existing.id, normalized_item, &fingerprint).await?;
            if existing.fingerprint != fingerprint {
                let relation = ItemRelation {
                    id: stream_model::ItemRelationId::generate(),
                    from_item_id: existing.id.clone(),
                    to_item_id: existing.id.clone(),
                    relation: RelationKind::SameStory,
                    evidence: "canonical_identity".into(),
                    created_at: Utc::now(),
                };
                self.store
                    .insert("ItemRelation", relation.id.as_str(), item_relation_record(&relation), false)
                    .await?;
            }
            return Ok(PersistOutcome { item: existing, is_new: false });
        }

        let item = Item {
            id: ItemId::generate(),
            source_id: source.id.clone(),
            source_kind: source.kind,
            canonical_identity: normalized_item.canonical_identity.clone(),
            canonical_url: normalized_item.canonical_url.clone(),
            title: normalized_item.title.clone(),
            content_text: normalized_item.content_text.clone(),
            content_html: normalized_item.content_html.clone(),
            author: normalized_item.author.clone(),
            published_at: normalized_item.published_at,
            fingerprint: fingerprint.clone(),
            provenance_summary: format!("observed via {}", source.endpoint),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        self.store.insert("Item", item.id.as_str(), item_record(&item), true).await?;

        let state = ItemStateRecord::unseen(item.id.clone());
        self.store
            .insert("ItemState", state.id.as_str(), item_state_record(&state), true)
            .await?;

        self.record_observation(source, &item.id, normalized_item, &fingerprint).await?;
        Ok(PersistOutcome { item, is_new: true })
    }

    async fn persist_fetch_result(
        &self,
        source: Source,
        attempt: FetchAttempt,
        normalized_items: Vec<NormalizedItem>,
    ) -> Result<FetchResult> {
        let (_, attempt, outcomes) = self.persist_observation(source, attempt, normalized_items).await?;
        let duplicate_count = outcomes.iter().filter(|outcome| !outcome.is_new).count() as u64;
        Ok(FetchResult {
            attempt,
            new_item_ids: outcomes
                .into_iter()
                .filter(|outcome| outcome.is_new)
                .map(|outcome| outcome.item.id)
                .collect(),
            duplicate_count,
        })
    }

    /// Persist one observation of a source: every normalized item goes through
    /// the existing fingerprint/identity dedup, and each outcome says whether
    /// the item is new or an additional observation of a known item.
    async fn persist_observation(
        &self,
        mut source: Source,
        mut attempt: FetchAttempt,
        normalized_items: Vec<NormalizedItem>,
    ) -> Result<(Source, FetchAttempt, Vec<PersistOutcome>)> {
        let mut outcomes = Vec::new();
        let mut duplicates = 0_u64;

        for normalized_item in normalized_items.iter() {
            let persisted = self.persist_normalized_item(&source, normalized_item).await?;
            if persisted.is_new {
                self.apply_rules(&persisted.item).await?;
            } else {
                duplicates += 1;
            }
            outcomes.push(persisted);
        }
        let retained = outcomes.iter().filter(|outcome| outcome.is_new).count() as u64;

        source.status = SourceStatus::Active;
        source.last_success_at = Some(Utc::now());
        source.last_error_message = None;
        source.last_error_category = None;
        source.consecutive_failures = 0;
        source.fetched_items_count += normalized_items.len() as u64;
        source.duplicate_items_count += duplicates;
        source.updated_at = Utc::now();
        self.store
            .update("Source", source.id.as_str(), source_record(&source))
            .await?;

        attempt.status = FetchStatus::Succeeded;
        attempt.completed_at = Some(Utc::now());
        attempt.item_count = normalized_items.len() as u64;
        attempt.duplicate_count = duplicates;
        attempt.retained_count = retained;
        self.store
            .update("FetchAttempt", attempt.id.as_str(), fetch_attempt_record(&attempt))
            .await?;

        Ok((source, attempt, outcomes))
    }

    async fn record_observation(
        &self,
        source: &Source,
        item_id: &ItemId,
        normalized_item: &NormalizedItem,
        fingerprint: &Fingerprint,
    ) -> Result<()> {
        let observation_id = format!("{}:{}", item_id, normalized_item.original_identifier);
        self.store
            .insert(
                "ItemSource",
                &observation_id,
                json!({
                    "item": item_id.as_str(),
                    "source": source.id.as_str(),
                    "original_identifier": normalized_item.original_identifier,
                    "source_url": normalized_item.source_url.as_str(),
                    "observed_at": Utc::now().to_rfc3339(),
                    "adapter_kind": normalized_item.parser,
                }),
                false,
            )
            .await?;

        let provenance = Provenance {
            id: ProvenanceId::generate(),
            item_id: item_id.clone(),
            source_id: source.id.clone(),
            source_url: normalized_item.source_url.clone(),
            observed_at: Utc::now(),
            parser: normalized_item.parser.clone(),
            original_identifier: normalized_item.original_identifier.clone(),
            canonical_identifier: normalized_item.canonical_identity.clone(),
            fingerprint: fingerprint.clone(),
            transformations: normalized_item.transformations.clone(),
        };
        self.store
            .insert("Provenance", provenance.id.as_str(), provenance_record(&provenance), true)
            .await?;
        Ok(())
    }

    async fn find_source_by_endpoint(&self, endpoint: &str) -> Result<Option<Source>> {
        Ok(self
            .store
            .find("Source", json!({ "endpoint": endpoint }))
            .await?
            .into_iter()
            .next()
            .map(source_from_value)
            .transpose()?)
    }

    async fn find_item_by_fingerprint(&self, fingerprint: &Fingerprint) -> Result<Option<Item>> {
        Ok(self
            .store
            .find("Item", json!({ "fingerprint": fingerprint.as_str() }))
            .await?
            .into_iter()
            .next()
            .map(item_from_value)
            .transpose()?)
    }

    async fn find_item_by_identity(&self, canonical_identity: &str) -> Result<Option<Item>> {
        Ok(self
            .store
            .find("Item", json!({ "canonical_identity": canonical_identity }))
            .await?
            .into_iter()
            .next()
            .map(item_from_value)
            .transpose()?)
    }

    async fn transition_state(&self, item_id: &ItemId, state: ItemState) -> Result<ItemStateRecord> {
        let _ = self.get_item(item_id).await?.ok_or_else(|| anyhow!("unknown item: {}", item_id))?;
        let existing = self
            .store
            .find("ItemState", json!({ "item": item_id.as_str() }))
            .await?
            .into_iter()
            .next()
            .map(item_state_from_value)
            .transpose()?;
        let mut record = existing.unwrap_or_else(|| ItemStateRecord::unseen(item_id.clone()));
        record.transition(state, Utc::now());
        self.store
            .update("ItemState", record.id.as_str(), item_state_record(&record))
            .await?;
        Ok(record)
    }

    async fn set_source_status(&self, source_id: &SourceId, status: SourceStatus) -> Result<Source> {
        let mut source = self
            .get_source(source_id)
            .await?
            .ok_or_else(|| anyhow!("unknown source: {}", source_id))?;
        source.status = status;
        source.updated_at = Utc::now();
        self.store.update("Source", source.id.as_str(), source_record(&source)).await?;
        Ok(source)
    }

    async fn hydrate_items(&self, items: Vec<Item>) -> Result<Vec<ItemView>> {
        let states = self
            .store
            .all("ItemState")
            .await?
            .into_iter()
            .map(item_state_from_value)
            .collect::<Result<Vec<_>>>()?;
        let provenances = self
            .store
            .all("Provenance")
            .await?
            .into_iter()
            .map(provenance_from_value)
            .collect::<Result<Vec<_>>>()?;
        let state_map = states
            .into_iter()
            .map(|state| (state.item_id.clone(), state))
            .collect::<HashMap<_, _>>();
        let mut provenance_map: HashMap<ItemId, Vec<Provenance>> = HashMap::new();
        for provenance in provenances {
            provenance_map
                .entry(provenance.item_id.clone())
                .or_default()
                .push(provenance);
        }

        Ok(items
            .into_iter()
            .map(|item| ItemView {
                state: state_map
                    .get(&item.id)
                    .cloned()
                    .unwrap_or_else(|| ItemStateRecord::unseen(item.id.clone())),
                provenance: provenance_map.remove(&item.id).unwrap_or_default(),
                item,
            })
            .collect())
    }
}

#[derive(Debug, Deserialize)]
struct BridgeResponse {
    ok: bool,
    #[serde(default)]
    result: Value,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug)]
struct PersistOutcome {
    item: Item,
    is_new: bool,
}

fn source_record(source: &Source) -> Value {
    json!({
        "__id": source.id.as_str(),
        "kind": source.kind.to_string(),
        "adapter_kind": source.adapter_kind.to_string(),
        "endpoint": source.endpoint.as_str(),
        "identity": source.identity,
        "canonical_url": source.canonical_url.as_str(),
        "original_url": source.original_url.as_str(),
        "title": source.title.clone().unwrap_or_default(),
        "stage": source.stage.as_str(),
        "stage_detail": source.stage_detail.clone().unwrap_or_default(),
        "provenance": source.provenance,
        "discovered_at": source.discovered_at.to_rfc3339(),
        "last_observed_at": source.last_observed_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "configuration": source.configuration.to_string(),
        "status": source.status.to_string(),
        "refresh_minutes": source.refresh_minutes,
        "last_checked_at": source.last_checked_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "last_success_at": source.last_success_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "last_failure_at": source.last_failure_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "last_error_category": source.last_error_category.map(|value| value.to_string()).unwrap_or_default(),
        "last_error_message": source.last_error_message.clone().unwrap_or_default(),
        "consecutive_failures": source.consecutive_failures,
        "fetched_items_count": source.fetched_items_count,
        "duplicate_items_count": source.duplicate_items_count,
        "created_at": source.created_at.to_rfc3339(),
        "updated_at": source.updated_at.to_rfc3339(),
    })
}

fn source_from_value(value: Value) -> Result<Source> {
    let kind = source_kind(&value_string(&value, "kind")?)?;
    let endpoint = url::Url::parse(&value_string(&value, "endpoint")?)?;
    let created_at = value_datetime(&value, "created_at")?;
    // Sources written before the generic source model default sensibly.
    let url_or = |field: &str, fallback: &url::Url| -> Result<url::Url> {
        Ok(match value_string_opt(&value, field)? {
            Some(raw) => url::Url::parse(&raw)?,
            None => fallback.clone(),
        })
    };
    Ok(Source {
        id: SourceId::new(value_string(&value, "__id")?),
        kind,
        adapter_kind: value_string_opt(&value, "adapter_kind")?
            .map(|value| source_kind(&value))
            .transpose()?
            .unwrap_or(kind),
        canonical_url: url_or("canonical_url", &endpoint)?,
        original_url: url_or("original_url", &endpoint)?,
        title: value_string_opt(&value, "title")?,
        stage: value_string_opt(&value, "stage")?
            .and_then(|value| ProcessingStage::parse(&value))
            .unwrap_or(ProcessingStage::Queued),
        stage_detail: value_string_opt(&value, "stage_detail")?,
        provenance: value_string_opt(&value, "provenance")?.unwrap_or_else(|| "configured".into()),
        discovered_at: value_datetime_opt(&value, "discovered_at")?.unwrap_or(created_at),
        last_observed_at: value_datetime_opt(&value, "last_observed_at")?,
        endpoint,
        identity: value_string(&value, "identity")?,
        configuration: serde_json::from_str(&value_string(&value, "configuration")?).unwrap_or_else(|_| json!({})),
        status: source_status(&value_string(&value, "status")?)?,
        refresh_minutes: value_u64(&value, "refresh_minutes")? as u32,
        last_checked_at: value_datetime_opt(&value, "last_checked_at")?,
        last_success_at: value_datetime_opt(&value, "last_success_at")?,
        last_failure_at: value_datetime_opt(&value, "last_failure_at")?,
        last_error_category: value_string_opt(&value, "last_error_category")?.map(|value| failure_category(&value)).transpose()?,
        last_error_message: value_string_opt(&value, "last_error_message")?,
        consecutive_failures: value_u64(&value, "consecutive_failures")? as u32,
        fetched_items_count: value_u64(&value, "fetched_items_count")?,
        duplicate_items_count: value_u64(&value, "duplicate_items_count")?,
        created_at,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

fn fetch_attempt_record(attempt: &FetchAttempt) -> Value {
    json!({
        "__id": attempt.id.as_str(),
        "source": attempt.source_id.as_str(),
        "attempted_at": attempt.attempted_at.to_rfc3339(),
        "completed_at": attempt.completed_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "status": attempt.status.to_string(),
        "failure_category": attempt.failure_category.map(|value| value.to_string()).unwrap_or_default(),
        "item_count": attempt.item_count,
        "duplicate_count": attempt.duplicate_count,
        "retained_count": attempt.retained_count,
        "diagnostics": attempt.diagnostics.to_string(),
    })
}

fn item_record(item: &Item) -> Value {
    json!({
        "__id": item.id.as_str(),
        "source": item.source_id.as_str(),
        "source_kind": item.source_kind.to_string(),
        "canonical_identity": item.canonical_identity,
        "canonical_url": item.canonical_url.as_ref().map(|value| value.as_str()).unwrap_or_default(),
        "title": item.title,
        "content_text": item.content_text,
        "content_html": item.content_html.clone().unwrap_or_default(),
        "author_name": item.author.clone().unwrap_or_default(),
        "published_at": item.published_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "fingerprint": item.fingerprint.as_str(),
        "provenance_summary": item.provenance_summary,
        "created_at": item.created_at.to_rfc3339(),
        "updated_at": item.updated_at.to_rfc3339(),
    })
}

fn item_from_value(value: Value) -> Result<Item> {
    Ok(Item {
        id: ItemId::new(value_string(&value, "__id")?),
        source_id: SourceId::new(value_string(&value, "source")?),
        source_kind: source_kind(&value_string(&value, "source_kind")?)?,
        canonical_identity: value_string(&value, "canonical_identity")?,
        canonical_url: value_string_opt(&value, "canonical_url")?
            .map(|value| url::Url::parse(&value))
            .transpose()?,
        title: value_string(&value, "title")?,
        content_text: value_string(&value, "content_text")?,
        content_html: value_string_opt(&value, "content_html")?,
        author: value_string_opt(&value, "author_name")?,
        published_at: value_datetime_opt(&value, "published_at")?,
        fingerprint: Fingerprint::new(value_string(&value, "fingerprint")?),
        provenance_summary: value_string(&value, "provenance_summary")?,
        created_at: value_datetime(&value, "created_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

fn item_state_record(record: &ItemStateRecord) -> Value {
    json!({
        "__id": record.id.as_str(),
        "item": record.item_id.as_str(),
        "state": record.state.to_string(),
        "seen_at": record.seen_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "read_at": record.read_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "saved_at": record.saved_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "dismissed_at": record.dismissed_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "important_at": record.important_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "acted_on_at": record.acted_on_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "archived_at": record.archived_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "updated_at": record.updated_at.to_rfc3339(),
    })
}

fn item_state_from_value(value: Value) -> Result<ItemStateRecord> {
    Ok(ItemStateRecord {
        id: stream_model::ItemStateId::new(value_string(&value, "__id")?),
        item_id: ItemId::new(value_string(&value, "item")?),
        state: item_state(&value_string(&value, "state")?)?,
        seen_at: value_datetime_opt(&value, "seen_at")?,
        read_at: value_datetime_opt(&value, "read_at")?,
        saved_at: value_datetime_opt(&value, "saved_at")?,
        dismissed_at: value_datetime_opt(&value, "dismissed_at")?,
        important_at: value_datetime_opt(&value, "important_at")?,
        acted_on_at: value_datetime_opt(&value, "acted_on_at")?,
        archived_at: value_datetime_opt(&value, "archived_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

fn provenance_record(provenance: &Provenance) -> Value {
    json!({
        "__id": provenance.id.as_str(),
        "item": provenance.item_id.as_str(),
        "source": provenance.source_id.as_str(),
        "source_url": provenance.source_url.as_str(),
        "observed_at": provenance.observed_at.to_rfc3339(),
        "parser": provenance.parser,
        "original_identifier": provenance.original_identifier,
        "canonical_identifier": provenance.canonical_identifier,
        "fingerprint": provenance.fingerprint.as_str(),
        "transformations": serde_json::to_string(&provenance.transformations).unwrap_or_else(|_| "[]".into()),
    })
}

fn provenance_from_value(value: Value) -> Result<Provenance> {
    Ok(Provenance {
        id: ProvenanceId::new(value_string(&value, "__id")?),
        item_id: ItemId::new(value_string(&value, "item")?),
        source_id: SourceId::new(value_string(&value, "source")?),
        source_url: url::Url::parse(&value_string(&value, "source_url")?)?,
        observed_at: value_datetime(&value, "observed_at")?,
        parser: value_string(&value, "parser")?,
        original_identifier: value_string(&value, "original_identifier")?,
        canonical_identifier: value_string(&value, "canonical_identifier")?,
        fingerprint: Fingerprint::new(value_string(&value, "fingerprint")?),
        transformations: serde_json::from_str(&value_string(&value, "transformations")?).unwrap_or_default(),
    })
}

fn attention_record(event: &AttentionEvent) -> Value {
    json!({
        "__id": event.id.as_str(),
        "item": event.item_id.as_str(),
        "rule": event.rule_id.as_ref().map(|value| value.as_str()).unwrap_or_default(),
        "status": attention_status_str(event.status),
        "summary": event.summary,
        "rationale": event.rationale,
        "created_at": event.created_at.to_rfc3339(),
        "resolved_at": event.resolved_at.map(|value| value.to_rfc3339()).unwrap_or_default(),
    })
}

fn attention_from_value(value: Value) -> Result<AttentionEvent> {
    Ok(AttentionEvent {
        id: AttentionEventId::new(value_string(&value, "__id")?),
        item_id: ItemId::new(value_string(&value, "item")?),
        rule_id: value_string_opt(&value, "rule")?.map(RuleId::new),
        status: attention_status(&value_string(&value, "status")?)?,
        summary: value_string(&value, "summary")?,
        rationale: value_string(&value, "rationale")?,
        created_at: value_datetime(&value, "created_at")?,
        resolved_at: value_datetime_opt(&value, "resolved_at")?,
    })
}

fn rule_record(rule: &Rule) -> Value {
    json!({
        "__id": rule.id.as_str(),
        "name": rule.name,
        "enabled": rule.enabled.to_string(),
        "source_filter": rule.source_filter.as_ref().map(|value| value.as_str()).unwrap_or_default(),
        "source_kind_filter": rule.source_kind_filter.map(|value| value.to_string()).unwrap_or_default(),
        "title_pattern": rule.title_pattern.clone().unwrap_or_default(),
        "content_pattern": rule.content_pattern.clone().unwrap_or_default(),
        "url_pattern": rule.url_pattern.clone().unwrap_or_default(),
        "author_pattern": rule.author_pattern.clone().unwrap_or_default(),
        "published_after": rule.published_after.map(|value| value.to_rfc3339()).unwrap_or_default(),
        "action": rule.action.to_string(),
        "created_by": rule.created_by.clone().unwrap_or_default(),
        "updated_by": rule.updated_by.clone().unwrap_or_default(),
        "explanation": rule.explanation.clone().unwrap_or_default(),
        "created_at": rule.created_at.to_rfc3339(),
        "updated_at": rule.updated_at.to_rfc3339(),
    })
}

fn rule_from_value(value: Value) -> Result<Rule> {
    Ok(Rule {
        id: RuleId::new(value_string(&value, "__id")?),
        name: value_string(&value, "name")?,
        enabled: value_string(&value, "enabled")? == "true",
        source_filter: value_string_opt(&value, "source_filter")?.map(SourceId::new),
        source_kind_filter: value_string_opt(&value, "source_kind_filter")?.map(|value| source_kind(&value)).transpose()?,
        title_pattern: value_string_opt(&value, "title_pattern")?,
        content_pattern: value_string_opt(&value, "content_pattern")?,
        url_pattern: value_string_opt(&value, "url_pattern")?,
        author_pattern: value_string_opt(&value, "author_pattern")?,
        published_after: value_datetime_opt(&value, "published_after")?,
        action: rule_action(&value_string(&value, "action")?)?,
        created_by: value_string_opt(&value, "created_by")?,
        updated_by: value_string_opt(&value, "updated_by")?,
        explanation: value_string_opt(&value, "explanation")?,
        created_at: value_datetime(&value, "created_at")?,
        updated_at: value_datetime(&value, "updated_at")?,
    })
}

fn rule_execution_record(execution: &RuleExecution) -> Value {
    json!({
        "__id": execution.id.as_str(),
        "rule": execution.rule_id.as_str(),
        "item": execution.item_id.as_str(),
        "executed_at": execution.executed_at.to_rfc3339(),
        "result": rule_execution_result_str(execution.result),
        "failure": execution.failure.clone().unwrap_or_default(),
        "attention_event_id": execution.attention_event_id.as_ref().map(|value| value.as_str()).unwrap_or_default(),
    })
}

fn item_relation_record(relation: &ItemRelation) -> Value {
    json!({
        "__id": relation.id.as_str(),
        "from_item": relation.from_item_id.as_str(),
        "to_item": relation.to_item_id.as_str(),
        "relation": relation.relation.to_string(),
        "evidence": relation.evidence,
        "created_at": relation.created_at.to_rfc3339(),
    })
}

fn value_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing string field '{}'", field))
}

fn value_string_opt(value: &Value, field: &str) -> Result<Option<String>> {
    Ok(match value.get(field).and_then(Value::as_str) {
        Some(text) if !text.trim().is_empty() => Some(text.to_owned()),
        _ => None,
    })
}

fn value_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing integer field '{}'", field))
}

fn value_datetime(value: &Value, field: &str) -> Result<DateTime<Utc>> {
    let raw = value_string(value, field)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("failed to parse datetime field '{}'", field))
}

fn value_datetime_opt(value: &Value, field: &str) -> Result<Option<DateTime<Utc>>> {
    match value_string_opt(value, field)? {
        Some(raw) => Ok(Some(
            DateTime::parse_from_rfc3339(&raw)
                .map(|value| value.with_timezone(&Utc))
                .with_context(|| format!("failed to parse datetime field '{}'", field))?,
        )),
        None => Ok(None),
    }
}

fn source_kind(value: &str) -> Result<SourceKind> {
    SourceKind::parse(value).ok_or_else(|| anyhow!("unsupported source kind: {value}"))
}

fn source_status(value: &str) -> Result<SourceStatus> {
    match value {
        "discovered" => Ok(SourceStatus::Discovered),
        "active" => Ok(SourceStatus::Active),
        "paused" => Ok(SourceStatus::Paused),
        "failed" => Ok(SourceStatus::Failed),
        "disabled" => Ok(SourceStatus::Disabled),
        "deleted" => Ok(SourceStatus::Deleted),
        other => Err(anyhow!("unsupported source status: {other}")),
    }
}

fn item_state(value: &str) -> Result<ItemState> {
    match value {
        "unseen" => Ok(ItemState::Unseen),
        "seen" => Ok(ItemState::Seen),
        "read" => Ok(ItemState::Read),
        "saved" => Ok(ItemState::Saved),
        "dismissed" => Ok(ItemState::Dismissed),
        "important" => Ok(ItemState::Important),
        "acted_on" => Ok(ItemState::ActedOn),
        "archived" => Ok(ItemState::Archived),
        other => Err(anyhow!("unsupported item state: {other}")),
    }
}

fn failure_category(value: &str) -> Result<stream_model::FailureCategory> {
    match value {
        "network" => Ok(stream_model::FailureCategory::Network),
        "authentication" => Ok(stream_model::FailureCategory::Authentication),
        "authorization" => Ok(stream_model::FailureCategory::Authorization),
        "parsing" => Ok(stream_model::FailureCategory::Parsing),
        "normalization" => Ok(stream_model::FailureCategory::Normalization),
        "deduplication" => Ok(stream_model::FailureCategory::Deduplication),
        "storage" => Ok(stream_model::FailureCategory::Storage),
        "semantic" => Ok(stream_model::FailureCategory::Semantic),
        "provider" => Ok(stream_model::FailureCategory::Provider),
        "delivery" => Ok(stream_model::FailureCategory::Delivery),
        other => Err(anyhow!("unsupported failure category: {other}")),
    }
}

fn attention_status_str(value: AttentionStatus) -> &'static str {
    match value {
        AttentionStatus::Open => "open",
        AttentionStatus::Resolved => "resolved",
        AttentionStatus::Dismissed => "dismissed",
    }
}

fn attention_status(value: &str) -> Result<AttentionStatus> {
    match value {
        "open" => Ok(AttentionStatus::Open),
        "resolved" => Ok(AttentionStatus::Resolved),
        "dismissed" => Ok(AttentionStatus::Dismissed),
        other => Err(anyhow!("unsupported attention status: {other}")),
    }
}

fn rule_action(value: &str) -> Result<RuleAction> {
    match value {
        "retain" => Ok(RuleAction::Retain),
        "save" => Ok(RuleAction::Save),
        "mark_important" => Ok(RuleAction::MarkImportant),
        "create_attention_event" => Ok(RuleAction::CreateAttentionEvent),
        other => Err(anyhow!("unsupported rule action: {other}")),
    }
}

fn rule_execution_result_str(value: RuleExecutionResult) -> &'static str {
    match value {
        RuleExecutionResult::Matched => "matched",
        RuleExecutionResult::Skipped => "skipped",
        RuleExecutionResult::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_ingest::AdapterRegistry;
    use stream_model::{CanonicalFingerprintParts, Fingerprint};
    use url::Url;

    #[test]
    fn search_filters_by_text_and_state() {
        let item = Item {
            id: ItemId::new("item_1"),
            source_id: SourceId::new("source_1"),
            source_kind: SourceKind::Rss,
            canonical_identity: "https://example.com/post".into(),
            canonical_url: Some(Url::parse("https://example.com/post").unwrap()),
            title: "Rust async news".into(),
            content_text: "Executor updates".into(),
            content_html: None,
            author: None,
            published_at: Some(Utc::now()),
            fingerprint: Fingerprint::from_canonical_parts(&CanonicalFingerprintParts {
                canonical_identity: "https://example.com/post".into(),
                canonical_url: Some(Url::parse("https://example.com/post").unwrap()),
                title: "Rust async news".into(),
                content_text: "Executor updates".into(),
                author: None,
                published_at: None,
            }),
            provenance_summary: "rss".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let view = ItemView {
            item,
            state: ItemStateRecord::unseen(ItemId::new("item_1")),
            provenance: vec![],
        };
        let query = SearchQuery {
            text: Some("rust async".into()),
            state: Some(ItemState::Unseen),
            ..Default::default()
        };
        let items = vec![view];
        let tokens = query
            .text
            .as_deref()
            .unwrap_or_default()
            .split_whitespace()
            .map(|token| token.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let filtered = items
            .into_iter()
            .filter(|view| {
                let haystack = format!("{} {}", view.item.title, view.item.content_text).to_ascii_lowercase();
                tokens.iter().all(|token| haystack.contains(token))
            })
            .collect::<Vec<_>>();
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn runtime_can_be_constructed() {
        let config = FeltDbConfig::at(".", "stream-test", PathBuf::from("/tmp/stream-test"));
        let runtime = StreamRuntime::new(FeltDbStore::new(config), AdapterRegistry::new(vec![]));
        assert_eq!(runtime.store().config.namespace, "stream-test");
    }
}
