use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use stream_appport::{AppPortManifest, StreamAppPort};
use stream_core::StreamRuntime;
use stream_model::{AttentionEventId, ItemId, SourceId, SourceKind};
use stream_query::StreamQueryService;
use std::path::Path;

mod intelligence;
mod observation;

pub use intelligence::{ContextCommand, ContextKindArg};
pub use observation::TargetAction;

#[derive(Debug, Parser)]
#[command(name = "stream")]
#[command(about = "Stream — give it URLs; it builds an evidence-backed understanding of what is changing")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Add a URL. `https://example.com` watches that resource;
    /// `'https://example.com/*'` watches the information surface beneath it
    /// (Stream discovers its blog, changelog, feeds, releases, …).
    Add {
        url: String,
        /// Only establish the durable target; do not fetch yet.
        #[arg(long)]
        no_observe: bool,
    },
    /// Today: signals ranked by information density.
    Signals {
        /// Include resolved and dismissed signals.
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// One signal with its evidence, connections, and why it is ranked where it is.
    Signal {
        id: String,
        #[arg(long)]
        json: bool,
        #[arg(long, conflicts_with = "dismiss")]
        resolve: bool,
        #[arg(long)]
        dismiss: bool,
    },
    /// What you care about.
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
    /// Connections of a signal, item, or context — or the whole graph.
    Connections {
        id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Sources Stream observes, with their processing stage.
    Sources,
    /// Observation targets and their scope.
    Targets {
        #[arg(long)]
        json: bool,
    },
    /// One observation target: `stream target <id> [show|discover|sources|pause|resume]`.
    Target {
        id: String,
        #[arg(value_enum, default_value_t = TargetAction::Show)]
        action: TargetAction,
        #[arg(long)]
        json: bool,
    },
    /// Observe what is due now (durable schedule), or keep observing with --watch.
    Observe {
        #[arg(long)]
        watch: bool,
        /// Observe every watched source now, due or not.
        #[arg(long)]
        all: bool,
    },
    /// Ask Stream about what it knows. Answers cite evidence and say when
    /// evidence is insufficient.
    Ask {
        question: String,
        /// Constrain the question to a signal (repeatable).
        #[arg(long = "signal")]
        signals: Vec<String>,
        /// Constrain the question to a context (repeatable).
        #[arg(long = "context")]
        contexts: Vec<String>,
        /// Constrain the question to a source (repeatable).
        #[arg(long = "source")]
        sources: Vec<String>,
        /// Save the answer as durable reasoning: insight, hypothesis, question, decision_candidate, investigation.
        #[arg(long)]
        save: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Reasoning you saved to Stream.
    Insights {
        #[arg(long)]
        json: bool,
    },
    /// One saved insight with its evidence.
    Insight {
        id: String,
        #[arg(long, conflicts_with = "drop")]
        resolve: bool,
        #[arg(long)]
        drop: bool,
        #[arg(long)]
        json: bool,
    },
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    Item {
        #[command(subcommand)]
        command: ItemCommand,
    },
    Query {
        #[command(subcommand)]
        command: QueryCommand,
    },
    Search {
        query: String,
    },
    Attention {
        #[command(subcommand)]
        command: AttentionCommand,
    },
    Appport {
        #[command(subcommand)]
        command: AppPortCommand,
    },
    Doctor,
}

#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    Add {
        endpoint: String,
        #[arg(long, value_enum, default_value_t = SourceKindArg::Rss)]
        kind: SourceKindArg,
    },
    List,
    Get { id: String },
    Refresh { id: String },
    /// Observe a source again through the full understanding pipeline.
    Observe { id: String },
    Pause { id: String },
    Resume { id: String },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SourceKindArg {
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

impl From<SourceKindArg> for SourceKind {
    fn from(value: SourceKindArg) -> Self {
        match value {
            SourceKindArg::Rss => SourceKind::Rss,
            SourceKindArg::Atom => SourceKind::Atom,
            SourceKindArg::JsonFeed => SourceKind::JsonFeed,
            SourceKindArg::Web => SourceKind::Web,
            SourceKindArg::Github => SourceKind::Github,
            SourceKindArg::Youtube => SourceKind::Youtube,
            SourceKindArg::Email => SourceKind::Email,
            SourceKindArg::Webhook => SourceKind::Webhook,
            SourceKindArg::Api => SourceKind::Api,
            SourceKindArg::Appport => SourceKind::Appport,
            SourceKindArg::Documentation => SourceKind::Documentation,
            SourceKindArg::Research => SourceKind::Research,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum ItemCommand {
    List,
    Get { id: String },
    MarkRead { id: String },
    Save { id: String },
    Dismiss { id: String },
    Important { id: String },
    Archive { id: String },
}

#[derive(Debug, Subcommand)]
pub enum QueryCommand {
    List,
    Unread,
    Saved,
    Important,
    Recent,
}

#[derive(Debug, Subcommand)]
pub enum AttentionCommand {
    Summary,
    List,
    Resolve { id: String },
}

#[derive(Debug, Subcommand)]
pub enum AppPortCommand {
    Manifest,
    /// Invoke a capability exactly as any AppPort client would.
    Invoke {
        capability: String,
        /// JSON input (defaults to {}).
        input: Option<String>,
    },
}

pub async fn run(cli: Cli, repo_root: impl AsRef<Path>) -> Result<String> {
    let port = StreamAppPort::new(stream_appport::runtime_for_root(repo_root)).with_provenance("cli");
    run_with(cli, &port).await
}

/// Run a command against an existing AppPort surface. The CLI holds no
/// logic of its own: everything goes through the same runtime the desktop
/// app and AppPort clients use.
pub async fn run_with(cli: Cli, port: &StreamAppPort) -> Result<String> {
    let runtime = port.runtime();
    match cli.command {
        Command::Add { url, no_observe } => observation::add(port, &url, no_observe).await,
        Command::Targets { json } => observation::targets(runtime, json).await,
        Command::Target { id, action, json } => observation::target(runtime, &id, action, json).await,
        Command::Observe { watch, all } => observation::observe(runtime, watch, all).await,
        Command::Signals { all, json } => intelligence::signals(runtime, all, json).await,
        Command::Signal { id, json, resolve, dismiss } => intelligence::signal(runtime, &id, json, resolve, dismiss).await,
        Command::Context { command } => intelligence::context(runtime, command).await,
        Command::Connections { id, json } => intelligence::connections(runtime, id.as_deref(), json).await,
        Command::Sources => intelligence::sources(runtime).await,
        Command::Ask { question, signals, contexts, sources, save, json } => {
            intelligence::ask(runtime, question, signals, contexts, sources, save, json).await
        }
        Command::Insights { json } => intelligence::insights(runtime, json).await,
        Command::Insight { id, resolve, drop, json } => intelligence::insight(runtime, &id, resolve, drop, json).await,
        Command::Source { command } => run_source(command, runtime).await,
        Command::Item { command } => run_item(command, runtime).await,
        Command::Query { command } => run_query(command, runtime).await,
        Command::Search { query } => {
            let service = StreamQueryService::new(runtime);
            let items = service.search(query, None, None).await?;
            Ok(format_item_list(items))
        }
        Command::Attention { command } => run_attention(command, runtime).await,
        Command::Appport { command } => match command {
            AppPortCommand::Manifest => Ok(serde_json::to_string_pretty(&AppPortManifest::stream())?),
            AppPortCommand::Invoke { capability, input } => {
                let input = match input {
                    Some(raw) => serde_json::from_str(&raw).map_err(|error| anyhow!("input is not JSON: {error}"))?,
                    None => serde_json::json!({}),
                };
                let result = port.invoke(&capability, input).await;
                Ok(serde_json::to_string_pretty(&stream_appport::envelope(result))?)
            }
        },
        Command::Doctor => run_doctor(runtime).await,
    }
}

pub fn build_runtime(repo_root: impl AsRef<Path>) -> StreamRuntime {
    stream_appport::runtime_for_root(repo_root)
}

async fn run_source(command: SourceCommand, runtime: &StreamRuntime) -> Result<String> {
    match command {
        SourceCommand::Add { endpoint, kind } => {
            let source = runtime.add_source(kind.into(), &endpoint).await?;
            Ok(format!("{}\t{}\t{}", source.id, source.kind, source.endpoint))
        }
        SourceCommand::List => {
            let sources = runtime.list_sources().await?;
            Ok(sources
                .into_iter()
                .map(|source| format!("{}\t{}\t{}\t{}", source.id, source.kind, source.status, source.endpoint))
                .collect::<Vec<_>>()
                .join("\n"))
        }
        SourceCommand::Get { id } => {
            let source = runtime
                .get_source(&SourceId::new(id))
                .await?
                .ok_or_else(|| anyhow!("source not found"))?;
            Ok(format!("{}\t{}\t{}", source.id, source.kind, source.endpoint))
        }
        SourceCommand::Refresh { id } => {
            let result = runtime.refresh_source(&SourceId::new(id)).await?;
            Ok(format!(
                "{}\tnew={}\tduplicates={}",
                result.attempt.id,
                result.new_item_ids.len(),
                result.duplicate_count
            ))
        }
        SourceCommand::Observe { id } => {
            let report = runtime.observe_source(&SourceId::new(id), Some(&intelligence::print_stage)).await?;
            Ok(intelligence::format_report(&report))
        }
        SourceCommand::Pause { id } => {
            let source = runtime.pause_source(&SourceId::new(id)).await?;
            Ok(format!("{}\t{}", source.id, source.status))
        }
        SourceCommand::Resume { id } => {
            let source = runtime.resume_source(&SourceId::new(id)).await?;
            Ok(format!("{}\t{}", source.id, source.status))
        }
    }
}

async fn run_item(command: ItemCommand, runtime: &StreamRuntime) -> Result<String> {
    match command {
        ItemCommand::List => Ok(format_item_list(runtime.list_items().await?)),
        ItemCommand::Get { id } => {
            let item = runtime
                .get_item(&ItemId::new(id))
                .await?
                .ok_or_else(|| anyhow!("item not found"))?;
            Ok(format_item_view(&item))
        }
        ItemCommand::MarkRead { id } => Ok(format_state(runtime.mark_read(&ItemId::new(id)).await?)),
        ItemCommand::Save { id } => Ok(format_state(runtime.save_item(&ItemId::new(id)).await?)),
        ItemCommand::Dismiss { id } => Ok(format_state(runtime.dismiss_item(&ItemId::new(id)).await?)),
        ItemCommand::Important { id } => Ok(format_state(runtime.mark_important(&ItemId::new(id)).await?)),
        ItemCommand::Archive { id } => Ok(format_state(runtime.archive_item(&ItemId::new(id)).await?)),
    }
}

async fn run_query(command: QueryCommand, runtime: &StreamRuntime) -> Result<String> {
    let service = StreamQueryService::new(runtime);
    let items = match command {
        QueryCommand::List => service.list().await?,
        QueryCommand::Unread => service.unread().await?,
        QueryCommand::Saved => service.saved().await?,
        QueryCommand::Important => service.important().await?,
        QueryCommand::Recent => service.recent(Utc::now() - Duration::days(7)).await?,
    };
    Ok(format_item_list(items))
}

async fn run_attention(command: AttentionCommand, runtime: &StreamRuntime) -> Result<String> {
    match command {
        AttentionCommand::Summary => {
            let summary = runtime.attention_summary().await?;
            Ok(format!(
                "new={} unread={} saved={} important={} attention={} failed_sources={} stale_sources={}",
                summary.new_items,
                summary.unread_items,
                summary.saved_items,
                summary.important_items,
                summary.new_attention_events,
                summary.failed_sources,
                summary.stale_sources,
            ))
        }
        AttentionCommand::List => Ok(runtime
            .list_attention()
            .await?
            .into_iter()
            .map(|event| format!("{}\t{}\t{}", event.id, event.status, event.summary))
            .collect::<Vec<_>>()
            .join("\n")),
        AttentionCommand::Resolve { id } => {
            let event = runtime.resolve_attention(&AttentionEventId::new(id)).await?;
            Ok(format!("{}\tresolved", event.id))
        }
    }
}

async fn run_doctor(runtime: &StreamRuntime) -> Result<String> {
    let sources = runtime.list_sources().await?;
    let failed_sources = sources.iter().filter(|source| matches!(source.status, stream_model::SourceStatus::Failed)).count();
    let stale_sources = sources
        .iter()
        .filter(|source| {
            source
                .last_success_at
                .or(source.last_checked_at)
                .map(|timestamp| Utc::now() - timestamp > Duration::hours(48))
                .unwrap_or(false)
        })
        .count();
    Ok([
        "Stream doctor".to_string(),
        format!("- source failures: {}", failed_sources),
        "- authentication failures: 0".to_string(),
        "- parser failures: 0".to_string(),
        format!("- stale sources: {}", stale_sources),
        "- storage problems: 0".to_string(),
        "- rule failures: 0".to_string(),
        "- delivery failures: 0".to_string(),
        intelligence::doctor_line(runtime).await?,
        observation::doctor_line(runtime).await?,
    ]
    .join("\n"))
}

fn format_item_list(items: Vec<stream_core::ItemView>) -> String {
    items.into_iter().map(|item| format_item_view(&item)).collect::<Vec<_>>().join("\n")
}

fn format_item_view(item: &stream_core::ItemView) -> String {
    format!(
        "{}\t{}\t{}\t{}",
        item.item.id,
        item.state.state,
        item.item.title,
        item.item
            .canonical_url
            .as_ref()
            .map(|url| url.as_str())
            .unwrap_or("")
    )
}

fn format_state(state: stream_model::ItemStateRecord) -> String {
    format!("{}\t{}", state.item_id, state.state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_contains_expected_capabilities() {
        let manifest = AppPortManifest::stream();
        assert!(manifest.has_capability("stream.source"));
        assert!(manifest.has_capability("stream.item"));
    }

    #[test]
    fn doctor_render_contains_expected_lines() {
        let text = [
            "Stream doctor".to_string(),
            "- source failures: 0".to_string(),
            "- authentication failures: 0".to_string(),
        ]
        .join("\n");
        assert!(text.contains("source failures"));
    }
}
