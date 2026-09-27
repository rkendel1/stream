//! CLI rendering of observation targets. Presentation only: parsing the
//! `/*` operator, resolving providers, discovery, and scheduling all happen
//! in the runtime behind the AppPort surface.

use anyhow::{anyhow, Result};
use clap::ValueEnum;
use std::fmt::Write as _;
use stream_appport::StreamAppPort;
use stream_core::{RunOptions, SourceHealth, StreamRuntime, TargetSummary, WatchOutcome};
use stream_model::{ObservationRun, ObservationScope};

use crate::intelligence::{format_card, format_report, print_stage};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TargetAction {
    Show,
    Discover,
    Sources,
    Pause,
    Resume,
}

/// The short id shown in tables; any unique prefix is accepted back.
pub fn short_id(id: &str) -> &str {
    &id[..id.len().min(14)]
}

fn health_mark(health: SourceHealth) -> &'static str {
    match health {
        SourceHealth::Healthy => "✓",
        SourceHealth::Pending => "…",
        SourceHealth::Retrying => "↻",
        SourceHealth::Unavailable => "✗",
        SourceHealth::Paused => "‖",
    }
}

fn heading(summary: &TargetSummary) -> String {
    let name = summary.title.clone().unwrap_or_else(|| summary.identity.display_name.clone());
    if summary.identity.provider == stream_model::TargetProvider::Web {
        name
    } else {
        format!("{} / {}", summary.identity.provider.label(), summary.identity.display_name)
    }
}

pub fn format_target(summary: &TargetSummary) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}", heading(summary));
    let _ = writeln!(out, "  URL: {}", summary.url);
    let _ = writeln!(out, "  Scope: {}", summary.scope);
    let _ = writeln!(out, "  Status: {}", summary.status);
    let _ = writeln!(out, "  ID: {}", summary.id);
    let _ = write!(out, "  {}", summary.watching);
    if let Some(detail) = &summary.discovery_detail {
        let routine = detail.starts_with("Found ") || detail == &summary.watching;
        if !routine {
            let _ = write!(out, "\n  {detail}");
        }
    }
    if !summary.surfaces.is_empty() {
        let _ = write!(out, "\n\nDiscovered surfaces:");
        for surface in &summary.surfaces {
            let _ = write!(
                out,
                "\n  {} {:<14} {}",
                health_mark(surface.health),
                surface.label,
                surface.title.clone().unwrap_or_else(|| surface.url.to_string())
            );
        }
    }
    out
}

fn format_run(run: &ObservationRun) -> String {
    format!("Observed {} source(s) — {}", run.sources_observed, run.detail)
}

/// `stream add <url>`: a resource is observed exactly as before; `<url>/*`
/// becomes an observation target that is discovered and observed.
pub async fn add(port: &StreamAppPort, url: &str, no_observe: bool) -> Result<String> {
    let runtime = port.runtime();
    let preview = runtime.preview_target(url)?;
    if preview.scope == ObservationScope::Resource {
        let added = runtime.add_target(url, "cli").await?;
        let source = added.seed_source.clone().ok_or_else(|| anyhow!("the resource has no source"))?;
        if added.existing {
            eprintln!("Already known as {} — observing again.", source.id);
        }
        if no_observe {
            return Ok(format!("{}\t{}\t{}", source.id, source.stage, source.canonical_url));
        }
        eprintln!("Analyzing…");
        let report = runtime.observe_source(&source.id, Some(&print_stage)).await?;
        let mut out = format_report(&report);
        if let Some(signal_id) = &report.primary_signal_id {
            if let Some(detail) = runtime.get_signal(signal_id).await? {
                out.push_str("\n\n");
                out.push_str(&format_card(&detail.summary));
            }
        }
        let _ = write!(out, "\n\nTarget   {}  (scope: resource)", added.target.id);
        return Ok(out);
    }

    if no_observe {
        let added = runtime.add_target(url, "cli").await?;
        let summary = runtime.target_summary(&added.target.id).await?.ok_or_else(|| anyhow!("target vanished"))?;
        return Ok(format!("Added observation target\n\n{}", format_target(&summary)));
    }
    eprintln!("Added observation target {}", preview.display_url);
    eprintln!("  {} · scope: descendants", preview.identity.display_name);
    eprintln!("  Discovering information surfaces…");
    let WatchOutcome { target, run, added, .. } = runtime.add_and_watch(url, "cli", None).await?;
    let summary = runtime.target_summary(&target.id).await?.ok_or_else(|| anyhow!("target vanished"))?;
    let mut out = String::from(if added.existing { "Observation target (already added)\n\n" } else { "Added observation target\n\n" });
    out.push_str(&format_target(&summary));
    if let Some(run) = &run {
        let _ = write!(out, "\n\n{}", format_run(run));
    }
    let signals = runtime.list_signals().await?;
    let mine = signals.iter().filter(|s| summary_signal(runtime, &summary, s)).collect::<Vec<_>>();
    for signal in mine.iter().take(3) {
        out.push_str("\n\n");
        out.push_str(&format_card(signal));
    }
    Ok(out)
}

fn summary_signal(_runtime: &StreamRuntime, summary: &TargetSummary, signal: &stream_core::SignalSummary) -> bool {
    signal
        .primary
        .as_ref()
        .and_then(|item| item.source.as_ref())
        .map(|source| summary.surfaces.iter().any(|s| s.source_id == source.id))
        .unwrap_or(false)
}

pub async fn targets(runtime: &StreamRuntime, json: bool) -> Result<String> {
    let summaries = runtime.target_summaries().await?;
    if json {
        return Ok(serde_json::to_string_pretty(&summaries)?);
    }
    if summaries.is_empty() {
        return Ok("No observation targets yet. Add one with `stream add <url>` or `stream add '<url>/*'`.".into());
    }
    let width = summaries.iter().map(|s| s.display_url.len()).max().unwrap_or(6).max(6);
    let mut out = format!("{:<14}  {:<width$}  {:<11}  {:<11}  WATCHING", "ID", "TARGET", "SCOPE", "STATUS");
    for summary in &summaries {
        let _ = write!(
            out,
            "\n{:<14}  {:<width$}  {:<11}  {:<11}  {}",
            short_id(summary.id.as_str()),
            summary.display_url,
            summary.scope.as_str(),
            summary.status.as_str(),
            summary.watching
        );
    }
    Ok(out)
}

pub async fn target(runtime: &StreamRuntime, id: &str, action: TargetAction, json: bool) -> Result<String> {
    let target = runtime.find_target(id).await?.ok_or_else(|| anyhow!("no observation target {id}"))?;
    match action {
        TargetAction::Show => {}
        TargetAction::Discover => {
            eprintln!("Discovering information surfaces…");
            let outcome = runtime.discover_target(&target.id).await?;
            if let Some(failure) = outcome.failure {
                eprintln!("{failure}");
            } else {
                eprintln!("{} new surface(s)", outcome.new_sources.len());
            }
        }
        TargetAction::Pause => {
            runtime.pause_target(&target.id).await?;
        }
        TargetAction::Resume => {
            runtime.resume_target(&target.id).await?;
        }
        TargetAction::Sources => {
            let sources = runtime.target_sources(&target.id).await?;
            if json {
                return Ok(serde_json::to_string_pretty(&sources)?);
            }
            let mut out = String::new();
            for watched in &sources {
                let source = &watched.source;
                let _ = writeln!(
                    out,
                    "{} {}\t{}\t{}\t{}",
                    health_mark(watched.health),
                    source.id,
                    source.surface_kind.map(|k| k.label()).unwrap_or("Page"),
                    source.discovery_method.map(|m| m.as_str()).unwrap_or("-"),
                    source.canonical_url
                );
                let _ = writeln!(out, "    Why: {}", watched.why);
                if let Some(change) = &watched.last_change {
                    let _ = writeln!(out, "    Last change ({}): {}", change.change, change.summary);
                }
                if let Some(error) = &source.last_error_message {
                    if source.consecutive_failures > 0 {
                        let _ = writeln!(out, "    Problem: {error}");
                    }
                }
            }
            return Ok(out.trim_end().to_owned());
        }
    }
    let summary = runtime.target_summary(&target.id).await?.ok_or_else(|| anyhow!("target vanished"))?;
    if json {
        return Ok(serde_json::to_string_pretty(&summary)?);
    }
    let body = format_target(&summary).lines().skip(1).collect::<Vec<_>>().join("\n");
    Ok(format!("Target — {}\n{body}", heading(&summary)))
}

/// `stream observe`: one scheduler pass (what is due), or `--watch` to keep
/// observing until interrupted.
pub async fn observe(runtime: &StreamRuntime, watch: bool, all: bool) -> Result<String> {
    if !watch {
        let run = runtime.run_observation(RunOptions { trigger: "cli".into(), target_id: None, force: all }).await?;
        return Ok(format_run(&run));
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = stop.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    eprintln!("Observing continuously; Ctrl-C to stop. Schedules are durable in FeltDB.");
    runtime.run_worker(stop, std::time::Duration::from_secs(60)).await;
    Ok("Stopped.".into())
}

pub async fn doctor_line(runtime: &StreamRuntime) -> Result<String> {
    let status = runtime.observation_status().await?;
    Ok(format!(
        "- observation: {} target(s), {} source(s) watched, {} due, {} failing{}",
        status.targets,
        status.sources,
        status.due_sources,
        status.failing_sources,
        status.next_due_at.map(|at| format!(", next due {}", at.format("%Y-%m-%d %H:%M UTC"))).unwrap_or_default()
    ))
}
