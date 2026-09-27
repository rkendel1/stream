//! CLI rendering of Stream's intelligence surface. Presentation only: every
//! command is a thin call into the runtime behind the AppPort surface.

use anyhow::{anyhow, Result};
use clap::{Subcommand, ValueEnum};
use std::fmt::Write as _;
use stream_appport::StreamAppPort;
use stream_core::{NewContext, ObservationReport, SignalDetail, SignalSummary, StreamRuntime};
use stream_model::{ConnectionRelation, ContextKind, ProcessingStage, SignalId, SignalStatus};

#[derive(Debug, Subcommand)]
pub enum ContextCommand {
    List,
    /// Add (or refine) something you care about.
    Add {
        name: String,
        #[arg(long, short)]
        description: Option<String>,
        #[arg(long, value_enum)]
        kind: Option<ContextKindArg>,
        /// Other names Stream should recognize for this context.
        #[arg(long = "alias")]
        aliases: Vec<String>,
        /// Related context (name or ID); repeatable.
        #[arg(long)]
        related: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ContextKindArg {
    Interest,
    Project,
    Concern,
}

impl From<ContextKindArg> for ContextKind {
    fn from(value: ContextKindArg) -> Self {
        match value {
            ContextKindArg::Interest => ContextKind::Interest,
            ContextKindArg::Project => ContextKind::Project,
            ContextKindArg::Concern => ContextKind::Concern,
        }
    }
}

pub fn print_stage(stage: ProcessingStage) {
    if stage.is_terminal() {
        return;
    }
    eprintln!("  {}…", stage.label());
}

pub async fn add(port: &StreamAppPort, url: &str, no_observe: bool) -> Result<String> {
    let runtime = port.runtime();
    let added = runtime.add_url(url, "cli").await?;
    if added.existing {
        eprintln!("Already known as {} — observing again.", added.source.id);
    }
    if no_observe {
        return Ok(format!("{}\t{}\t{}", added.source.id, added.source.stage, added.source.canonical_url));
    }
    eprintln!("Analyzing…");
    let report = runtime.observe_source(&added.source.id, Some(&print_stage)).await?;
    let mut out = format_report(&report);
    if let Some(signal_id) = &report.primary_signal_id {
        if let Some(detail) = runtime.get_signal(signal_id).await? {
            out.push_str("\n\n");
            out.push_str(&format_card(&detail.summary));
        }
    }
    Ok(out)
}

pub fn format_report(report: &ObservationReport) -> String {
    let mut out = String::new();
    let source = &report.source;
    let _ = writeln!(out, "Source   {}  ({}, observed as {})", source.id, source.kind, source.adapter_kind);
    let _ = writeln!(out, "URL      {}", source.canonical_url);
    if let Some(title) = &source.title {
        let _ = writeln!(out, "Title    {title}");
    }
    let _ = write!(out, "Stage    {}", source.stage);
    if let Some(detail) = &source.stage_detail {
        let _ = write!(out, " — {detail}");
    }
    if let Some(failure) = &report.failure {
        let _ = write!(out, "\nFailed   {failure}\n         The URL is kept; run `stream source observe {}` to retry.", source.id);
    }
    for rejected in &report.rejected {
        let reasons = rejected.rejections.iter().map(|r| format!("{}: {}", r.claim, r.reason)).collect::<Vec<_>>();
        let _ = write!(out, "\nRefused  interpretation of {} ({})", rejected.item_id, reasons.join("; "));
    }
    out
}

fn connected(summary: &SignalSummary) -> String {
    summary
        .connected_to
        .iter()
        .map(|c| if c.relation == ConnectionRelation::Via { format!("{} (via)", c.label) } else { c.label.clone() })
        .collect::<Vec<_>>()
        .join(" · ")
}

pub fn format_card(summary: &SignalSummary) -> String {
    let signal = &summary.signal;
    let mut out = String::new();
    let rank = if summary.position > 0 { format!("#{}", summary.position) } else { signal.status.to_string() };
    let _ = writeln!(out, "{rank}  {} · {}    {}", signal.topic.label, signal.subject.label, signal.id);
    let _ = writeln!(out, "    {}", signal.change.statement);
    if let Some(why) = &signal.why_it_matters {
        let _ = writeln!(out, "    Why it matters: {why}");
    }
    if !summary.connected_to.is_empty() {
        let _ = writeln!(out, "    Connected to: {}", connected(summary));
    }
    let _ = write!(
        out,
        "    {} · {} evidence · {}",
        if summary.source_count == 1 { "1 source".to_owned() } else { format!("{} sources", summary.source_count) },
        summary.evidence_count,
        summary.ranking.summary
    );
    out
}

pub async fn signals(runtime: &StreamRuntime, all: bool, json: bool) -> Result<String> {
    let signals = if all { runtime.list_signals().await? } else { runtime.today().await? };
    if json {
        return Ok(serde_json::to_string_pretty(&signals)?);
    }
    if signals.is_empty() {
        return Ok("No signals yet. Add a URL with `stream add <url>`.".into());
    }
    Ok(signals.iter().map(format_card).collect::<Vec<_>>().join("\n\n"))
}

pub fn format_detail(detail: &SignalDetail) -> String {
    let summary = &detail.summary;
    let signal = &summary.signal;
    let mut out = String::new();
    let _ = writeln!(out, "Signal   {}  ({}, advisory interpretation)", signal.id, signal.status);
    let _ = writeln!(out, "Topic    {}", signal.topic.label);
    let _ = writeln!(out, "Subject  {}", signal.subject.label);
    let _ = writeln!(out, "Change   {}", signal.change.statement);
    let _ = writeln!(
        out,
        "\nWhy it matters\n  {}",
        signal.why_it_matters.as_deref().unwrap_or("Not yet connected to anything you've told Stream you care about.")
    );
    if !summary.connected_to.is_empty() {
        let _ = writeln!(out, "\nConnected to");
        for label in &summary.connected_to {
            let _ = writeln!(out, "  {}  ({}, {} {:.2})", label.label, label.kind, label.relation, label.strength);
        }
    }
    let _ = writeln!(
        out,
        "\nWhy is this here?  {}",
        if summary.position > 0 { format!("#{} in Today", summary.position) } else { "not in Today".into() }
    );
    for factor in &summary.ranking.factors {
        let _ = writeln!(out, "  {:+.2}  {} — {}", factor.contribution, factor.label, factor.explanation);
    }
    // Each distinct excerpt once, with every claim it supports.
    let mut groups: Vec<(Vec<String>, &stream_core::EvidenceTrace)> = Vec::new();
    for trace in &detail.evidence {
        let e = &trace.evidence;
        let claim = e.claim.to_string();
        match groups.iter_mut().find(|(_, t)| {
            t.evidence.item_id == e.item_id && t.evidence.locator == e.locator && t.evidence.excerpt == e.excerpt
        }) {
            Some((claims, _)) if !claims.contains(&claim) => claims.push(claim),
            Some(_) => {}
            None => groups.push((vec![claim], trace)),
        }
    }
    const ORDER: [&str; 6] = ["why_it_matters", "connection", "change", "subject", "topic", "corroboration"];
    let rank = |claim: &String| ORDER.iter().position(|c| c == claim).unwrap_or(ORDER.len());
    for (claims, _) in groups.iter_mut() {
        claims.sort_by_key(|claim| rank(claim));
    }
    groups.sort_by_key(|(claims, _)| rank(&claims[0]));
    let _ = writeln!(
        out,
        "\nEvidence ({} excerpts, {} evidence records, {} observations)",
        groups.len(),
        detail.evidence.len(),
        detail.observations.len()
    );
    for (claims, trace) in groups {
        let evidence = &trace.evidence;
        let _ = writeln!(out, "  [{}] “{}”", claims.join(", "), evidence.excerpt);
        let item = trace.item.as_ref().map(|i| format!("{} {}", i.id, i.title)).unwrap_or_else(|| evidence.item_id.to_string());
        let source = trace
            .source
            .as_ref()
            .map(|s| format!("{} ({}) {}", s.id, s.kind, s.canonical_url))
            .unwrap_or_else(|| evidence.source_id.to_string());
        let _ = writeln!(out, "      item    {item}");
        let _ = writeln!(out, "      source  {source}");
        let _ = writeln!(out, "      url     {}  ({} of the item)", evidence.url, evidence.locator);
    }
    out.trim_end().to_owned()
}

pub async fn signal(runtime: &StreamRuntime, id: &str, json: bool, resolve: bool, dismiss: bool) -> Result<String> {
    let id = SignalId::new(id);
    if resolve || dismiss {
        let status = if resolve { SignalStatus::Resolved } else { SignalStatus::Dismissed };
        let view = runtime.set_signal_status(&id, status).await?;
        return Ok(format!("{}\t{}", view.id, view.status));
    }
    let detail = runtime.get_signal(&id).await?.ok_or_else(|| anyhow!("signal not found: {id}"))?;
    if json {
        return Ok(serde_json::to_string_pretty(&detail)?);
    }
    Ok(format_detail(&detail))
}

pub async fn context(runtime: &StreamRuntime, command: ContextCommand) -> Result<String> {
    match command {
        ContextCommand::List => {
            let views = runtime.context_views().await?;
            if views.is_empty() {
                return Ok("No context yet. Tell Stream what you care about: stream context add \"Portable compute\"".into());
            }
            Ok(views
                .iter()
                .map(|view| {
                    let mut line = format!("{}\t{}\t{}", view.context.id, view.context.kind, view.context.name);
                    if !view.context.description.is_empty() {
                        line.push_str(&format!(" — {}", view.context.description));
                    }
                    if !view.related.is_empty() {
                        let names = view.related.iter().map(|r| r.label.as_str()).collect::<Vec<_>>();
                        line.push_str(&format!("  [related: {}]", names.join(", ")));
                    }
                    line.push_str(&format!("  ({} signals)", view.signal_ids.len()));
                    line
                })
                .collect::<Vec<_>>()
                .join("\n"))
        }
        ContextCommand::Add { name, description, kind, aliases, related } => {
            let entry = runtime
                .add_context(NewContext { name, kind: kind.map(Into::into), description, aliases, related })
                .await?;
            Ok(format!("{}\t{}\t{}", entry.id, entry.kind, entry.name))
        }
    }
}

pub async fn connections(runtime: &StreamRuntime, id: Option<&str>, json: bool) -> Result<String> {
    if let Some(id) = id {
        let views = runtime.list_connections(Some(id)).await?;
        if json {
            return Ok(serde_json::to_string_pretty(&views)?);
        }
        if views.is_empty() {
            return Ok(format!("No connections for {id}."));
        }
        return Ok(views
            .iter()
            .map(|view| {
                let c = &view.connection;
                format!(
                    "{} —{}→ {} {} ({:.2})  {}\n    signal: {}  item: {}",
                    c.item_id,
                    c.relation,
                    c.target_kind,
                    c.label,
                    c.strength,
                    c.rationale,
                    view.signal_subject.as_deref().unwrap_or("-"),
                    view.item_title.as_deref().unwrap_or("-"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n"));
    }
    let graph = runtime.connection_graph().await?;
    if json {
        return Ok(serde_json::to_string_pretty(&graph)?);
    }
    let mut out = String::new();
    for node in &graph.contexts {
        let _ = writeln!(out, "{} ({})", node.context.name, node.context.kind);
        for related in &node.related {
            let _ = writeln!(out, "  ├── {} (related)", related.label);
        }
        for signal in &node.signals {
            let via = if signal.relation == ConnectionRelation::Via { " (indirect)" } else { "" };
            let _ = writeln!(out, "  ├── {}: {}{via}  [{}]", signal.subject, signal.change, signal.id);
        }
    }
    for subject in graph.subjects.iter().filter(|s| s.observation_count > 1) {
        let _ = writeln!(out, "{} — {} observations", subject.label, subject.observation_count);
    }
    if out.is_empty() {
        out.push_str("No connections yet.");
    }
    Ok(out.trim_end().to_owned())
}

pub async fn sources(runtime: &StreamRuntime) -> Result<String> {
    let sources = runtime.list_source_views().await?;
    if sources.is_empty() {
        return Ok("No sources yet. Add a URL with `stream add <url>`.".into());
    }
    Ok(sources
        .iter()
        .map(|s| {
            format!(
                "{}\t{}\t{}\t{}\t{}",
                s.id,
                s.kind,
                s.stage,
                s.title.as_deref().unwrap_or("-"),
                s.canonical_url
            )
        })
        .collect::<Vec<_>>()
        .join("\n"))
}
