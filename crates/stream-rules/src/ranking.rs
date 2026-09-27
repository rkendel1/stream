//! Information-density ordering.
//!
//! This is not an importance score and it does not pretend semantic judgment
//! is authoritative. It is a deterministic sum of observable properties, and
//! every term carries its own explanation so Stream can always answer
//! "why did this appear here?".

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use stream_model::{ChangeKind, SignalStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingInput {
    /// (context name, match strength in [0, 1]) for each connected context.
    pub contexts: Vec<(String, f32)>,
    /// Distinct sources that observed this change.
    pub sources: usize,
    /// Distinct publishers (hosts) among those sources: independence.
    #[serde(default)]
    pub independent_publishers: usize,
    /// Open questions, hypotheses, and investigations the user saved about it.
    #[serde(default)]
    pub open_questions: usize,
    /// Observations that dispute the change.
    #[serde(default)]
    pub contradictions: usize,
    /// Strongest link to a previously observed item, in [0, 1].
    pub connection_strength: f32,
    pub change_kind: ChangeKind,
    /// Number of earlier signals about the same subject.
    pub prior_signals_on_subject: usize,
    /// Names of explicit user rules that marked the underlying items.
    pub matched_rules: Vec<String>,
    pub last_observed_at: DateTime<Utc>,
    pub now: DateTime<Utc>,
    pub status: SignalStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankingFactor {
    pub key: String,
    pub label: String,
    /// Observed value, normalized to [0, 1].
    pub value: f64,
    pub weight: f64,
    pub contribution: f64,
    pub explanation: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankingExplanation {
    pub factors: Vec<RankingFactor>,
    /// Sum of contributions; only meaningful relative to other signals.
    pub total: f64,
    /// Resolved and dismissed signals leave Today.
    pub eligible_for_today: bool,
    pub summary: String,
}

pub const WEIGHT_CONTEXT: f64 = 3.0;
pub const WEIGHT_RULES: f64 = 2.0;
pub const WEIGHT_CORROBORATION: f64 = 1.5;
pub const WEIGHT_CONNECTION: f64 = 1.0;
pub const WEIGHT_CHANGE: f64 = 1.0;
pub const WEIGHT_RECENCY: f64 = 1.0;
pub const WEIGHT_NOVELTY: f64 = 0.75;
pub const WEIGHT_OPEN_QUESTIONS: f64 = 1.0;
pub const WEIGHT_DISPUTED: f64 = 0.75;
/// Recency halves every three days.
pub const RECENCY_HALF_LIFE_HOURS: f64 = 72.0;

fn factor(key: &str, label: &str, value: f64, weight: f64, explanation: String) -> RankingFactor {
    let value = value.clamp(0.0, 1.0);
    RankingFactor {
        key: key.into(),
        label: label.into(),
        value,
        weight,
        contribution: value * weight,
        explanation,
    }
}

pub fn explain_rank(input: &RankingInput) -> RankingExplanation {
    let context_value = input.contexts.iter().map(|(_, strength)| *strength as f64).sum::<f64>();
    let context_names = input.contexts.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>();
    let hours = (input.now - input.last_observed_at).num_minutes().max(0) as f64 / 60.0;
    let recency = 0.5f64.powf(hours / RECENCY_HALF_LIFE_HOURS);
    let novelty = 1.0 / (1.0 + input.prior_signals_on_subject as f64);

    let factors = vec![
        factor(
            "context",
            "Relationship to your context",
            context_value,
            WEIGHT_CONTEXT,
            if context_names.is_empty() {
                "Not connected to anything you've told Stream you care about.".into()
            } else {
                format!("Connects to {}.", context_names.join(", "))
            },
        ),
        factor(
            "rules",
            "Your rules",
            if input.matched_rules.is_empty() { 0.0 } else { 1.0 },
            WEIGHT_RULES,
            if input.matched_rules.is_empty() {
                "No rule of yours matched.".into()
            } else {
                format!("Matched your rule{} {}.", if input.matched_rules.len() == 1 { "" } else { "s" }, input.matched_rules.join(", "))
            },
        ),
        factor(
            "corroboration",
            "Independent corroboration",
            input.independent_publishers.max(1).min(input.sources.max(1)).saturating_sub(1) as f64 / 3.0,
            WEIGHT_CORROBORATION,
            match (input.sources, input.independent_publishers) {
                (0 | 1, _) => "Observed by a single source.".into(),
                (n, 0 | 1) => format!("{n} sources, all from one publisher — not independent."),
                (n, p) if p >= n => format!("{n} independent sources describe this change."),
                (n, p) => format!("{n} sources from {p} independent publishers describe this change."),
            },
        ),
        factor(
            "open_questions",
            "Unresolved questions",
            input.open_questions as f64 / 2.0,
            WEIGHT_OPEN_QUESTIONS,
            match input.open_questions {
                0 => "No open questions of yours about this.".into(),
                1 => "You have 1 open question or investigation about this.".into(),
                n => format!("You have {n} open questions or investigations about this."),
            },
        ),
        factor(
            "disputed",
            "Disputed",
            if input.contradictions > 0 { 1.0 } else { 0.0 },
            WEIGHT_DISPUTED,
            match input.contradictions {
                0 => "No source disputes it.".into(),
                1 => "One source disputes it — unresolved.".into(),
                n => format!("{n} sources dispute it — unresolved."),
            },
        ),
        factor(
            "connection",
            "Connection to what Stream has seen",
            input.connection_strength as f64,
            WEIGHT_CONNECTION,
            if input.connection_strength > 0.0 {
                format!("Links to earlier observations (strength {:.2}).", input.connection_strength)
            } else {
                "No link to earlier observations yet.".into()
            },
        ),
        factor(
            "change",
            "Change magnitude",
            input.change_kind.magnitude(),
            WEIGHT_CHANGE,
            format!("Change type: {}.", input.change_kind.verb().to_lowercase()),
        ),
        factor(
            "novelty",
            "Novelty",
            novelty,
            WEIGHT_NOVELTY,
            match input.prior_signals_on_subject {
                0 => "The first signal about this subject.".into(),
                n => format!("{n} earlier signal{} about this subject.", if n == 1 { "" } else { "s" }),
            },
        ),
        factor(
            "recency",
            "Recency",
            recency,
            WEIGHT_RECENCY,
            if hours < 1.0 {
                "Observed within the last hour.".into()
            } else if hours < 48.0 {
                format!("Last observed {:.0} hours ago.", hours)
            } else {
                format!("Last observed {:.0} days ago.", hours / 24.0)
            },
        ),
    ];

    let eligible = input.status == SignalStatus::Open;
    let total = if eligible { factors.iter().map(|f| f.contribution).sum() } else { 0.0 };
    let mut leading = factors.iter().filter(|f| f.contribution > 0.0).collect::<Vec<_>>();
    leading.sort_by(|a, b| b.contribution.partial_cmp(&a.contribution).unwrap_or(std::cmp::Ordering::Equal));
    let summary = if !eligible {
        format!("Not shown in Today: this signal is {}.", input.status)
    } else {
        leading
            .iter()
            .take(2)
            .map(|f| f.explanation.trim_end_matches('.').to_owned())
            .collect::<Vec<_>>()
            .join("; ")
            + "."
    };
    RankingExplanation {
        factors,
        total,
        eligible_for_today: eligible,
        summary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn input() -> RankingInput {
        let now = Utc::now();
        RankingInput {
            contexts: vec![],
            sources: 1,
            independent_publishers: 1,
            open_questions: 0,
            contradictions: 0,
            connection_strength: 0.0,
            change_kind: ChangeKind::Describes,
            prior_signals_on_subject: 0,
            matched_rules: vec![],
            last_observed_at: now,
            now,
            status: SignalStatus::Open,
        }
    }

    #[test]
    fn context_and_corroboration_outrank_volume() {
        let plain = explain_rank(&input());
        let connected = explain_rank(&RankingInput {
            contexts: vec![("Portable compute".into(), 0.8)],
            sources: 3,
            independent_publishers: 3,
            ..input()
        });
        assert!(connected.total > plain.total);
        assert!(connected.summary.contains("Portable compute"), "{}", connected.summary);
    }

    #[test]
    fn every_factor_explains_itself() {
        let explanation = explain_rank(&input());
        assert_eq!(explanation.factors.len(), 9);
        assert!(explanation.factors.iter().all(|f| !f.explanation.is_empty()));
        let sum: f64 = explanation.factors.iter().map(|f| f.contribution).sum();
        assert!((sum - explanation.total).abs() < 1e-9);
    }

    #[test]
    fn ranking_is_deterministic_and_decays_with_age() {
        let fresh = explain_rank(&input());
        assert_eq!(fresh, explain_rank(&input()));
        let old = explain_rank(&RankingInput {
            last_observed_at: Utc::now() - Duration::days(9),
            ..input()
        });
        assert!(old.total < fresh.total);
    }

    #[test]
    fn unresolved_questions_and_disputes_raise_rank_but_one_publisher_is_not_corroboration() {
        let base = explain_rank(&input());
        let open = explain_rank(&RankingInput { open_questions: 1, contradictions: 1, ..input() });
        assert!(open.total > base.total);
        let same_publisher = explain_rank(&RankingInput { sources: 3, independent_publishers: 1, ..input() });
        let corroboration = same_publisher.factors.iter().find(|f| f.key == "corroboration").unwrap();
        assert_eq!(corroboration.value, 0.0);
        assert!(corroboration.explanation.contains("not independent"));
    }

    #[test]
    fn resolved_signals_leave_today() {
        let resolved = explain_rank(&RankingInput { status: SignalStatus::Resolved, ..input() });
        assert!(!resolved.eligible_for_today);
        assert_eq!(resolved.total, 0.0);
    }
}
