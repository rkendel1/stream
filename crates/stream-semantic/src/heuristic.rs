//! A deterministic, local interpreter. No network, no keys, no model: it
//! reads the item's own words and the user's own context. Its proposals are
//! modest by design and always quote the text they rely on.

use crate::text::{contains_phrase, is_stopword, key_terms, sentences, stem, stems, tokens};
use crate::{
    Claim, ContextMatch, Excerpt, Interpretation, InterpretationInput, Interpreter, ItemLink, LinkKind,
};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::BTreeSet;
use stream_model::{
    normalize_whitespace, slug, Change, ChangeKind, ContextEntry, ContextKind, EvidenceLocator, Item, SourceKind,
    Subject, Topic,
};

const TOPICS: &[(&str, &[&str])] = &[
    ("AI", &[
        "ai", "llm", "llms", "model", "models", "gpt", "claude", "openai", "anthropic", "gemini", "machine learning",
        "neural", "inference", "transformer", "embedding", "prompt", "multimodal", "fine-tuning", "reasoning",
    ]),
    ("Autonomous Software", &[
        "agent", "agents", "agentic", "autonomous", "autonomy", "copilot", "coding agent", "self-driving", "mcp",
        "tool use", "orchestration", "handoff",
    ]),
    ("Rust", &["rust", "cargo", "crate", "crates", "rustc", "rustacean", "borrow checker", "tokio"]),
    ("Compute & Infrastructure", &[
        "container", "containers", "kubernetes", "docker", "runtime", "virtual machine", "vm", "vms", "wasm",
        "webassembly", "serverless", "cloud", "compute", "edge", "oci", "linux", "hypervisor", "portable", "deploy",
        "infrastructure", "cluster",
    ]),
    ("Developer Tooling", &[
        "compiler", "ide", "editor", "debugger", "cli", "sdk", "tooling", "toolchain", "lint", "linter", "git",
        "github", "package manager", "api", "library", "framework", "developer", "developers", "build system",
    ]),
    ("Security", &[
        "vulnerability", "cve", "exploit", "security", "breach", "malware", "attack", "attacker", "ransomware",
        "zero-day", "authentication", "encryption",
    ]),
    ("Data & Databases", &["database", "databases", "sql", "postgres", "sqlite", "query", "storage", "index", "vector", "replication", "schema"]),
    ("Web Platform", &["browser", "javascript", "typescript", "css", "html", "chrome", "safari", "firefox", "node.js", "react", "web platform"]),
    ("Hardware", &["chip", "chips", "gpu", "gpus", "cpu", "silicon", "nvidia", "processor", "semiconductor", "m4", "m5"]),
    ("Healthcare", &["health", "patient", "patients", "clinical", "medical", "hospital", "fda", "diagnosis", "healthcare"]),
    ("Business & Markets", &[
        "funding", "acquisition", "acquires", "acquired", "revenue", "pricing", "startup", "ipo", "market",
        "customers", "customer", "valuation", "raises", "layoffs", "enterprise",
    ]),
    ("Research", &["paper", "study", "arxiv", "researchers", "dataset", "benchmark", "experiment", "findings", "university"]),
    ("Open Source", &["open source", "open-source", "license", "maintainer", "maintainers", "contributors", "fork", "foundation"]),
];

/// Verb forms that express a concrete change, in priority order.
const CHANGE_VERBS: &[(ChangeKind, &[&str])] = &[
    (ChangeKind::Deprecates, &["deprecates", "deprecated", "deprecating"]),
    (ChangeKind::Removes, &["removes", "removed", "removing", "drops", "dropped", "dropping", "discontinues", "discontinued", "sunsets", "retires", "retired", "kills"]),
    (ChangeKind::Releases, &["releases", "released", "ships", "shipped", "shipping", "is now available", "are now available", "now generally available", "generally available", "goes ga", "hits 1.0", "reaches 1.0"]),
    (ChangeKind::Introduces, &["introduces", "introduced", "introducing", "unveils", "unveiled", "announces", "announced", "announcing", "launches", "launched", "launching", "debuts", "debuted", "open-sources", "open sources", "open-sourced", "acquires", "acquired", "raises"]),
    (ChangeKind::Adds, &["adds", "added", "adding", "gains", "gets", "now supports", "adds support for", "brings", "enables", "expands", "extends"]),
    (ChangeKind::Changes, &["changes", "changed", "updates", "updated", "moves to", "switches to", "replaces", "replaced", "redesigns", "rewrites", "rewritten", "migrates", "shifts", "overhauls", "cuts", "raises prices", "renames"]),
    (ChangeKind::Fixes, &["fixes", "fixed", "patches", "patched", "resolves", "mitigates"]),
    (ChangeKind::SignalsMovement, &["plans to", "is planning", "will", "moving toward", "moves toward", "signals", "hints at", "considers", "explores", "previews", "experiments with", "roadmap", "is working on", "proposes", "proposal"]),
];

const TITLE_PREFIXES: &[(&str, ChangeKind)] = &[
    ("announcing", ChangeKind::Introduces),
    ("introducing", ChangeKind::Introduces),
    ("launching", ChangeKind::Introduces),
    ("meet", ChangeKind::Introduces),
    ("releasing", ChangeKind::Releases),
    ("released:", ChangeKind::Releases),
    ("release:", ChangeKind::Releases),
    ("show hn:", ChangeKind::Introduces),
    ("launch hn:", ChangeKind::Introduces),
];

#[derive(Debug, Default, Clone)]
pub struct HeuristicInterpreter;

impl HeuristicInterpreter {
    pub const ID: &'static str = "stream.heuristic.v1";
}

#[async_trait]
impl Interpreter for HeuristicInterpreter {
    fn id(&self) -> &str {
        Self::ID
    }

    async fn interpret(&self, input: &InterpretationInput<'_>) -> Result<Interpretation> {
        Ok(interpret(input))
    }
}

pub(crate) fn interpret(input: &InterpretationInput<'_>) -> Interpretation {
    let item = input.item;
    let title = clean_title(&item.title);
    let subject = subject_claim(item, &title);
    let change = change_claim(item, &title, &subject.value);
    let topic = topic_claim(item, input.contexts);
    let context_matches = context_matches(item, input.contexts);
    let why_it_matters = why_it_matters(&change.value, &context_matches, input.contexts);
    let item_links = item_links(item, &subject.value, input);
    Interpretation {
        topic,
        subject,
        change,
        context_matches,
        why_it_matters,
        item_links,
    }
}

fn excerpt(locator: EvidenceLocator, text: &str) -> Excerpt {
    Excerpt { locator, text: text.to_owned() }
}

/// Every sentence of the item with where it came from; the title first.
fn located_sentences(item: &Item) -> Vec<(EvidenceLocator, &str)> {
    let mut out = vec![(EvidenceLocator::Title, item.title.as_str())];
    out.extend(sentences(&item.content_text).into_iter().take(80).map(|s| (EvidenceLocator::Content, s)));
    out
}

/// Strip site suffixes ("Title | Site", "Title - Site") that describe the
/// publisher, not the information.
fn clean_title(title: &str) -> String {
    let title = normalize_whitespace(title);
    for separator in [" | ", " — ", " – ", " - ", " · ", " :: "] {
        if let Some(index) = title.rfind(separator) {
            let (head, tail) = title.split_at(index);
            if head.split_whitespace().count() >= 2 && tail.split_whitespace().count() <= 5 {
                return head.trim().to_owned();
            }
        }
    }
    title
}

fn find_verb(lower: &str, verb: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(offset) = lower[from..].find(verb) {
        let start = from + offset;
        let end = start + verb.len();
        let before_ok = start == 0 || !lower[..start].chars().next_back().map(char::is_alphanumeric).unwrap_or(false);
        let after_ok = end >= lower.len() || !lower[end..].chars().next().map(char::is_alphanumeric).unwrap_or(false);
        if before_ok && after_ok {
            return Some(start);
        }
        from = end;
    }
    None
}

/// Find the first change verb in a sentence: (kind, byte offset, verb length).
fn change_verb(sentence: &str) -> Option<(ChangeKind, usize, usize)> {
    let lower = sentence.to_lowercase();
    if lower.len() != sentence.len() {
        // Non-ASCII case folding changed byte offsets; stay conservative.
        return None;
    }
    let mut best: Option<(ChangeKind, usize, usize)> = None;
    for (kind, verbs) in CHANGE_VERBS {
        for verb in verbs.iter() {
            if let Some(position) = find_verb(&lower, verb) {
                if best.map(|(_, at, _)| position < at).unwrap_or(true) {
                    best = Some((*kind, position, verb.len()));
                }
            }
        }
    }
    best
}

fn trim_phrase(value: &str, max_words: usize) -> String {
    let words = value
        .split_whitespace()
        .take(max_words)
        .collect::<Vec<_>>()
        .join(" ");
    words
        .trim_matches(|c: char| c.is_whitespace() || matches!(c, ',' | ';' | ':' | '.' | '-' | '—' | '–' | '!' | '?' | '"' | '\''))
        .to_owned()
}

fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.len() >= prefix.len() && value.is_char_boundary(prefix.len()) && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
        let rest = &value[prefix.len()..];
        if rest.starts_with(|c: char| c.is_whitespace() || c == ':') || prefix.ends_with(':') {
            return Some(rest.trim_start_matches(|c: char| c.is_whitespace() || c == ':'));
        }
    }
    None
}

fn subject_claim(item: &Item, title: &str) -> Claim<Subject> {
    let evidence = vec![excerpt(EvidenceLocator::Title, &item.title)];
    let claim = |label: String, confidence: f32, rationale: &str| Claim {
        value: Subject { label },
        excerpts: evidence.clone(),
        confidence,
        rationale: rationale.to_owned(),
    };

    for (prefix, _) in TITLE_PREFIXES {
        if let Some(rest) = strip_prefix_ci(title, prefix) {
            let head = rest.split([':', ',', '(']).next().unwrap_or(rest);
            let head = head.split(" - ").next().unwrap_or(head);
            let label = trim_phrase(head, 6);
            if !label.is_empty() {
                return claim(label, 0.8, "named by an announcement title");
            }
        }
    }

    if let Some((_, position, _)) = change_verb(title) {
        let label = trim_phrase(&title[..position], 6);
        let label = label.rsplit(": ").next().unwrap_or(&label).trim().to_owned();
        if !label.is_empty() && label.split_whitespace().count() <= 6 {
            return claim(label, 0.75, "the actor of the change named in the title");
        }
    }

    if let Some(phrase) = proper_noun_phrase(title) {
        return claim(phrase, 0.6, "the most specific named thing in the title");
    }
    claim(trim_phrase(title, 6), 0.35, "no specific subject was named; using the title")
}

/// Longest run of Capitalized / ACRONYM / version words in the title.
fn proper_noun_phrase(title: &str) -> Option<String> {
    let mut best: Vec<&str> = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for (index, word) in title.split_whitespace().enumerate() {
        let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '+' && c != '#');
        let is_named = clean.chars().next().map(|c| c.is_uppercase()).unwrap_or(false)
            || (clean.chars().any(|c| c.is_ascii_digit()) && !current.is_empty());
        let common_first = index == 0 && is_stopword(&clean.to_lowercase());
        if is_named && !common_first && !clean.is_empty() {
            current.push(clean);
            if word.ends_with([',', ':', ';']) {
                if current.len() > best.len() {
                    best = std::mem::take(&mut current);
                }
                current.clear();
            }
        } else {
            if current.len() > best.len() {
                best = std::mem::take(&mut current);
            }
            current.clear();
        }
    }
    if current.len() > best.len() {
        best = current;
    }
    let phrase = best.into_iter().take(5).collect::<Vec<_>>().join(" ");
    (!phrase.is_empty()).then_some(phrase)
}

fn change_claim(item: &Item, title: &str, subject: &Subject) -> Claim<Change> {
    for (prefix, kind) in TITLE_PREFIXES {
        if strip_prefix_ci(title, prefix).is_some() {
            return Claim {
                value: Change { kind: *kind, statement: format!("{} {}", kind.verb(), subject.label) },
                excerpts: vec![excerpt(EvidenceLocator::Title, &item.title)],
                confidence: 0.75,
                rationale: "the title announces it".into(),
            };
        }
    }

    for (locator, sentence) in located_sentences(item).into_iter().take(25) {
        let source = if locator == EvidenceLocator::Title { title } else { sentence };
        let Some((kind, position, length)) = change_verb(source) else {
            continue;
        };
        let object = trim_phrase(&source[position + length..], 12);
        let object = object
            .strip_prefix("support for ")
            .map(|rest| format!("support for {rest}"))
            .unwrap_or(object);
        let statement = if object.split_whitespace().count() >= 1 && !object.eq_ignore_ascii_case(&subject.label) {
            format!("{} {}", kind.verb(), object)
        } else {
            format!("{} {}", kind.verb(), subject.label)
        };
        return Claim {
            value: Change { kind, statement },
            excerpts: vec![excerpt(locator, sentence)],
            confidence: if locator == EvidenceLocator::Title { 0.7 } else { 0.55 },
            rationale: format!("a '{}' change is stated in the {}", kind, locator),
        };
    }

    let first = sentences(&item.content_text).into_iter().next();
    let (locator, quote) = match first {
        Some(sentence) => (EvidenceLocator::Content, sentence),
        None => (EvidenceLocator::Title, item.title.as_str()),
    };
    Claim {
        value: Change {
            kind: ChangeKind::Describes,
            statement: format!("Describes {}: {}", subject.label, trim_phrase(quote, 16)),
        },
        excerpts: vec![excerpt(locator, quote)],
        confidence: 0.2,
        rationale: "no concrete change is stated; this describes the subject".into(),
    }
}

fn topic_claim(item: &Item, contexts: &[ContextEntry]) -> Claim<Topic> {
    let title_stems = stems(&item.title);
    let located = located_sentences(item);
    let sentence_stems = located.iter().map(|(_, s)| stems(s)).collect::<Vec<_>>();

    let mut best: Option<(&str, f32, Vec<usize>)> = None;
    for (label, terms) in TOPICS {
        let mut score = 0.0;
        let mut supporting = BTreeSet::new();
        for term in terms.iter() {
            if contains_phrase(&title_stems, term) {
                score += 3.0;
                supporting.insert(0);
            }
            for (index, stems) in sentence_stems.iter().enumerate().skip(1) {
                if contains_phrase(stems, term) {
                    score += 1.0;
                    if supporting.len() < 3 {
                        supporting.insert(index);
                    }
                }
            }
        }
        if score > best.as_ref().map(|(_, s, _)| *s).unwrap_or(0.0) {
            best = Some((label, score, supporting.into_iter().collect()));
        }
    }

    if let Some((label, score, supporting)) = best {
        return Claim {
            value: Topic { label: label.to_owned() },
            excerpts: supporting.into_iter().map(|index| excerpt(located[index].0, located[index].1)).collect(),
            confidence: (score / 8.0).min(1.0),
            rationale: format!("vocabulary of {label} appears in the item"),
        };
    }

    // No known area: fall back to the strongest context the item mentions,
    // then to what the source is.
    let all_stems = stems(&format!("{} {}", item.title, item.content_text));
    if let Some(context) = contexts.iter().find(|context| contains_phrase(&all_stems, &context.name)) {
        return Claim {
            value: Topic { label: context.name.clone() },
            excerpts: vec![excerpt(EvidenceLocator::Title, &item.title)],
            confidence: 0.3,
            rationale: "no broad area recognized; the item names one of your contexts".into(),
        };
    }
    let label = match item.source_kind {
        SourceKind::Research => "Research",
        SourceKind::Github => "Open Source",
        SourceKind::Youtube => "Video",
        SourceKind::Documentation => "Documentation",
        _ => "General",
    };
    Claim {
        value: Topic { label: label.into() },
        excerpts: vec![excerpt(EvidenceLocator::Title, &item.title)],
        confidence: 0.1,
        rationale: "no broad area recognized; using the kind of source".into(),
    }
}

fn describing_terms(context: &ContextEntry) -> Vec<String> {
    let mut terms = tokens(&context.description)
        .into_iter()
        .filter(|token| token.chars().count() >= 4 && !is_stopword(token))
        .map(|token| stem(&token))
        .collect::<Vec<_>>();
    terms.dedup();
    terms.truncate(16);
    terms
}

/// The word as the item actually wrote it, for each stem: users should read
/// “autonomous”, not the internal stem “autonomou”.
fn surface_forms(item: &Item) -> std::collections::HashMap<String, String> {
    let mut forms = std::collections::HashMap::new();
    for text in [item.title.as_str(), item.content_text.as_str()] {
        for word in text.split(|c: char| !(c.is_alphanumeric() || c == '-' || c == '\'')) {
            let word = word.trim_matches(|c: char| c == '-' || c == '\'');
            if !word.is_empty() {
                forms.entry(stem(word)).or_insert_with(|| word.to_owned());
            }
        }
    }
    forms
}

fn context_matches(item: &Item, contexts: &[ContextEntry]) -> Vec<ContextMatch> {
    let located = located_sentences(item);
    let sentence_stems = located.iter().map(|(_, s)| stems(s)).collect::<Vec<_>>();
    let surface = surface_forms(item);
    let mut out = Vec::new();

    for context in contexts {
        let mut score = 0.0f32;
        let mut matched: Vec<String> = Vec::new();
        let mut supporting: BTreeSet<usize> = BTreeSet::new();
        let mut direct = false;

        let phrases = std::iter::once(context.name.as_str())
            .chain(context.aliases.iter().map(String::as_str))
            .filter(|phrase| !phrase.trim().is_empty());
        for phrase in phrases {
            for (index, stems) in sentence_stems.iter().enumerate() {
                if contains_phrase(stems, phrase) {
                    score += if index == 0 { 4.0 } else { 3.0 };
                    direct = true;
                    supporting.insert(index);
                    if !matched.iter().any(|m| m.eq_ignore_ascii_case(phrase)) {
                        matched.push(phrase.to_owned());
                    }
                }
            }
        }

        let name_tokens = tokens(&context.name)
            .into_iter()
            .filter(|token| !is_stopword(token) && token.chars().count() >= 3)
            .collect::<Vec<_>>();
        if name_tokens.len() > 1 {
            for token in &name_tokens {
                let term = stem(token);
                if let Some(index) = sentence_stems.iter().position(|stems| stems.contains(&term)) {
                    score += if index == 0 { 1.5 } else { 1.0 };
                    supporting.insert(index);
                    if !matched.iter().any(|m| m.eq_ignore_ascii_case(token)) {
                        matched.push(token.clone());
                    }
                    direct = true;
                }
            }
        }

        let mut description_hits = 0;
        for term in describing_terms(context) {
            if let Some(index) = sentence_stems.iter().position(|stems| stems.contains(&term)) {
                description_hits += 1;
                if description_hits <= 6 {
                    score += 0.6;
                    if supporting.len() < 4 {
                        supporting.insert(index);
                    }
                    let shown = surface.get(&term).cloned().unwrap_or(term);
                    if !matched.iter().any(|m| m.eq_ignore_ascii_case(&shown)) {
                        matched.push(shown);
                    }
                }
            }
        }

        let qualifies = (direct && score >= 1.5) || description_hits >= 3;
        if !qualifies {
            continue;
        }
        let strength = (score / 6.0).min(1.0);
        out.push(ContextMatch {
            context_id: context.id.clone(),
            matched_terms: matched.clone(),
            strength,
            excerpts: supporting.into_iter().take(3).map(|index| excerpt(located[index].0, located[index].1)).collect(),
            rationale: if direct {
                format!("mentions {}", quote_list(&matched, 4))
            } else {
                format!("shares vocabulary with how you describe {}: {}", context.name, quote_list(&matched, 4))
            },
        });
    }
    out.sort_by(|a, b| b.strength.partial_cmp(&a.strength).unwrap_or(std::cmp::Ordering::Equal));
    out
}

fn quote_list(values: &[String], limit: usize) -> String {
    values
        .iter()
        .take(limit)
        .map(|value| format!("“{value}”"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn kind_phrase(kind: ContextKind) -> &'static str {
    match kind {
        ContextKind::Project => "a project you're working on",
        ContextKind::Concern => "something you're tracking",
        ContextKind::Interest => "something you care about",
    }
}

fn why_it_matters(change: &Change, matches: &[ContextMatch], contexts: &[ContextEntry]) -> Option<Claim<String>> {
    let lookup = |id| contexts.iter().find(|context| &context.id == id);
    let primary = matches.first()?;
    let context = lookup(&primary.context_id)?;
    let mut text = format!(
        "This may matter because it overlaps with {}, {}: it {}.",
        context.name,
        kind_phrase(context.kind),
        primary.rationale
    );
    if !context.description.is_empty() {
        text.push_str(&format!(" You describe {} as “{}”.", context.name, context.description.trim_end_matches('.')));
    }
    let others = matches
        .iter()
        .skip(1)
        .filter_map(|m| lookup(&m.context_id))
        .map(|context| context.name.clone())
        .collect::<Vec<_>>();
    if !others.is_empty() {
        text.push_str(&format!(" It also touches {}.", others.join(", ")));
    }
    match change.kind {
        ChangeKind::Removes | ChangeKind::Deprecates => text.push_str(" A removal or deprecation may require action."),
        ChangeKind::Introduces | ChangeKind::Releases | ChangeKind::Adds => {
            text.push_str(" It is a new capability, which may change what you build or buy.")
        }
        _ => {}
    }
    Some(Claim {
        value: text,
        excerpts: matches.iter().flat_map(|m| m.excerpts.clone()).take(3).collect(),
        confidence: primary.strength,
        rationale: format!("evaluated against {} context entries", contexts.len()),
    })
}

fn item_links(item: &Item, subject: &Subject, input: &InterpretationInput<'_>) -> Vec<ItemLink> {
    let terms = key_terms(&item.title, &item.content_text, 18).into_iter().collect::<BTreeSet<_>>();
    let subject_key = slug(subject.label.as_str());
    let located = located_sentences(item);
    let mut out = Vec::new();

    for prior in input.prior {
        if prior.item_id == item.id {
            continue;
        }
        let prior_terms = key_terms(&prior.title, &prior.content_text, 18).into_iter().collect::<BTreeSet<_>>();
        let shared = terms.intersection(&prior_terms).cloned().collect::<Vec<_>>();
        let denominator = terms.len().min(prior_terms.len()).max(1) as f32;
        let overlap = shared.len() as f32 / denominator;
        let same_subject = prior
            .subject
            .as_deref()
            .map(|label| {
                let key = slug(label);
                key == subject_key && key.split('-').count() <= 6 && !key.is_empty()
            })
            .unwrap_or(false);

        let kind = if (same_subject && shared.len() >= 2) || (overlap >= 0.45 && shared.len() >= 5) {
            LinkKind::SameChange
        } else if overlap >= 0.25 && shared.len() >= 3 || same_subject {
            LinkKind::Related
        } else {
            continue;
        };

        let supporting = located
            .iter()
            .filter(|(_, sentence)| {
                let stems = stems(sentence);
                shared.iter().filter(|term| stems.contains(term)).count() >= 2
            })
            .take(2)
            .map(|(locator, sentence)| excerpt(*locator, sentence))
            .collect::<Vec<_>>();
        let strength = if same_subject { overlap.max(0.7) } else { overlap }.min(1.0);
        out.push(ItemLink {
            item_id: prior.item_id.clone(),
            kind,
            strength,
            shared_terms: shared.clone(),
            excerpts: supporting,
            rationale: if same_subject {
                format!("both concern {} and share {}", subject.label, quote_list(&shared, 5))
            } else {
                format!("shares {} with “{}”", quote_list(&shared, 5), prior.title)
            },
        });
    }
    out.sort_by(|a, b| b.strength.partial_cmp(&a.strength).unwrap_or(std::cmp::Ordering::Equal));
    out.truncate(8);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{verify, PriorItem};
    use chrono::Utc;
    use stream_model::{ContextId, Fingerprint, ItemId, SourceId};
    use url::Url;

    fn item(title: &str, content: &str) -> Item {
        Item {
            id: ItemId::generate(),
            source_id: SourceId::new("source_test"),
            source_kind: SourceKind::Web,
            canonical_identity: "https://example.com/x".into(),
            canonical_url: Some(Url::parse("https://example.com/x").unwrap()),
            title: title.into(),
            content_text: content.into(),
            content_html: None,
            author: None,
            published_at: None,
            fingerprint: Fingerprint::new("f"),
            provenance_summary: String::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn context(name: &str, kind: ContextKind, description: &str) -> ContextEntry {
        ContextEntry::new(name, kind, description)
    }

    fn run(item: &Item, contexts: &[ContextEntry], prior: &[PriorItem]) -> Interpretation {
        interpret(&InterpretationInput { item, contexts, prior })
    }

    #[test]
    fn identifies_topic_subject_and_concrete_change() {
        let item = item(
            "Apple Container adds portable Linux VMs | Example News",
            "Apple Container now runs every container in its own lightweight virtual machine.\nThe runtime is portable across Macs.",
        );
        let result = run(&item, &[], &[]);
        assert_eq!(result.subject.value.label, "Apple Container");
        assert_eq!(result.change.value.kind, ChangeKind::Adds);
        assert_eq!(result.change.value.statement, "Adds portable Linux VMs");
        assert_eq!(result.topic.value.label, "Compute & Infrastructure");
    }

    #[test]
    fn announcement_titles_name_the_subject() {
        let item = item("Announcing Rust 1.99", "The Rust team is happy to announce a new version of Rust, 1.99.");
        let result = run(&item, &[], &[]);
        assert_eq!(result.subject.value.label, "Rust 1.99");
        assert_eq!(result.change.value.kind, ChangeKind::Introduces);
        assert_eq!(result.topic.value.label, "Rust");
    }

    #[test]
    fn falls_back_to_describes_without_inventing_a_change() {
        let item = item("Thoughts on compilers", "Compilers are fascinating programs.");
        let result = run(&item, &[], &[]);
        assert_eq!(result.change.value.kind, ChangeKind::Describes);
        assert!(result.change.confidence < 0.3);
    }

    #[test]
    fn evaluates_items_against_durable_context() {
        let compute = context("Portable compute", ContextKind::Interest, "Running workloads anywhere with containers");
        let attn = context("Attn", ContextKind::Project, "");
        let unrelated = context("Healthcare billing", ContextKind::Concern, "");
        let item = item(
            "Apple Container adds portable Linux VMs",
            "Apple Container runs workloads in lightweight VMs so compute becomes portable.",
        );
        let result = run(&item, &[compute.clone(), attn, unrelated], &[]);
        assert_eq!(result.context_matches.len(), 1);
        assert_eq!(result.context_matches[0].context_id, compute.id);
        let why = result.why_it_matters.expect("why it matters");
        assert!(why.value.contains("Portable compute"), "{}", why.value);
    }

    #[test]
    fn why_it_matters_quotes_words_as_written() {
        let attn = context("Attn", ContextKind::Project, "Handoff between autonomous software and people");
        let item = item("Attn ships handoff reports", "Autonomous agents hand work to people.");
        let why = run(&item, &[attn], &[]).why_it_matters.unwrap().value;
        assert!(why.contains("“Autonomous”"), "{why}");
        assert!(!why.contains("autonomou”"), "{why}");
    }

    #[test]
    fn why_it_matters_is_absent_without_context() {
        let item = item("Apple Container adds portable Linux VMs", "Details.");
        assert!(run(&item, &[], &[]).why_it_matters.is_none());
    }

    #[test]
    fn recognizes_the_same_underlying_change_across_items() {
        let first = item(
            "Apple Container adds portable Linux VMs",
            "Apple Container runs each Linux container in a lightweight virtual machine on macOS.",
        );
        let second = item(
            "Apple's Container tool brings Linux VMs to macOS",
            "The new Apple Container project runs each Linux container inside a lightweight virtual machine.",
        );
        let prior = vec![PriorItem {
            item_id: first.id.clone(),
            signal_id: None,
            title: first.title.clone(),
            content_text: first.content_text.clone(),
            subject: Some("Apple Container".into()),
        }];
        let result = run(&second, &[], &prior);
        assert_eq!(result.item_links.len(), 1, "{:?}", result.item_links);
        assert_eq!(result.item_links[0].kind, LinkKind::SameChange);
    }

    #[test]
    fn every_heuristic_claim_passes_the_evidence_gate() {
        let compute = context("Portable compute", ContextKind::Interest, "");
        let item = item(
            "Apple Container adds portable Linux VMs",
            "Apple Container runs workloads in lightweight VMs. Compute becomes portable.",
        );
        let contexts = vec![compute];
        let result = run(&item, &contexts, &[]);
        let verified = verify(result.clone(), &item, &contexts, &[]).expect("grounded");
        assert!(verified.rejections.is_empty(), "{:?}", verified.rejections);
        assert_eq!(verified.interpretation, result);
    }

    #[test]
    fn the_gate_refuses_ungrounded_claims() {
        let item = item("Apple Container adds portable Linux VMs", "Details.");
        let mut result = run(&item, &[], &[]);
        result.change.excerpts = vec![excerpt(EvidenceLocator::Content, "Apple acquires Docker for $10B")];
        let rejected = verify(result, &item, &[], &[]).unwrap_err();
        assert_eq!(rejected[0].claim, "change");
    }

    #[test]
    fn the_gate_drops_unknown_contexts_and_unsupported_why() {
        let item = item("Apple Container adds portable Linux VMs", "Details.");
        let mut result = run(&item, &[], &[]);
        result.context_matches.push(ContextMatch {
            context_id: ContextId::new("context_invented"),
            matched_terms: vec![],
            strength: 1.0,
            excerpts: vec![excerpt(EvidenceLocator::Title, "Apple Container")],
            rationale: String::new(),
        });
        result.why_it_matters = Some(Claim {
            value: "It will make you rich".into(),
            excerpts: vec![],
            confidence: 1.0,
            rationale: String::new(),
        });
        let verified = verify(result, &item, &[], &[]).unwrap();
        assert!(verified.interpretation.context_matches.is_empty());
        assert!(verified.interpretation.why_it_matters.is_none());
        assert_eq!(verified.rejections.len(), 2);
    }
}
