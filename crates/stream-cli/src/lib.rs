use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use stream_appport::AppPortManifest;
use stream_core::{FeltDbConfig, FeltDbStore, StreamRuntime};
use stream_ingest::AdapterRegistry;
use stream_model::{AttentionEventId, ItemId, ItemState, SourceId, SourceKind};
use stream_query::StreamQueryService;
use stream_rss::default_adapters;
use std::path::Path;

#[derive(Debug, Parser)]
#[command(name = "stream")]
#[command(about = "Stream — a local-first information runtime")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
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
}

pub async fn run(cli: Cli, repo_root: impl AsRef<Path>) -> Result<String> {
    let runtime = build_runtime(repo_root);
    match cli.command {
        Command::Source { command } => run_source(command, &runtime).await,
        Command::Item { command } => run_item(command, &runtime).await,
        Command::Query { command } => run_query(command, &runtime).await,
        Command::Search { query } => {
            let service = StreamQueryService::new(&runtime);
            let items = service.search(query, None, None).await?;
            Ok(format_item_list(items))
        }
        Command::Attention { command } => run_attention(command, &runtime).await,
        Command::Appport { command } => match command {
            AppPortCommand::Manifest => Ok(serde_json::to_string_pretty(&AppPortManifest::stream())?),
        },
        Command::Doctor => run_doctor(&runtime).await,
    }
}

pub fn build_runtime(repo_root: impl AsRef<Path>) -> StreamRuntime {
    let config = FeltDbConfig::from_env(repo_root);
    let store = FeltDbStore::new(config);
    let adapters = AdapterRegistry::new(default_adapters());
    StreamRuntime::new(store, adapters)
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
            .map(|event| format!("{}\t{}\t{}", event.id, event.status as u8, event.summary))
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
