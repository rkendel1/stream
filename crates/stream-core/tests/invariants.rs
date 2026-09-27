//! Architectural invariants, enforced as tests:
//!
//! * FeltDB = authority
//! * Stream = information processing
//! * Semantic model = advisory
//! * RSS/Atom/JSON Feed = adapters
//! * Information density > information volume

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use stream_core::{NewContext, StreamRuntime};
use stream_model::{ContextKind, EvidenceLocator, ProcessingStage, SourceKind};
use stream_semantic::{Excerpt, HeuristicInterpreter, Interpretation, InterpretationInput, Interpreter};
use stream_testkit::{
    runtime_at, FixtureServer, Isolated, APPLE_CONTAINER_ARTICLE, APPLE_CONTAINER_SECOND_REPORT, NEWS_FEED,
};

/// A provider that proposes claims the source material does not support.
struct FabricatingInterpreter;

#[async_trait]
impl Interpreter for FabricatingInterpreter {
    fn id(&self) -> &str {
        "fabricating-provider"
    }

    async fn interpret(&self, input: &InterpretationInput<'_>) -> Result<Interpretation> {
        let mut proposal = HeuristicInterpreter.interpret(input).await?;
        proposal.change.value.statement = "Acquires Docker for $10B".into();
        proposal.change.excerpts = vec![Excerpt {
            locator: EvidenceLocator::Content,
            text: "Apple agreed to acquire Docker for $10 billion".into(),
        }];
        Ok(proposal)
    }
}

/// A different, well-behaved provider with a distinctive identity.
struct SecretProvider;

#[async_trait]
impl Interpreter for SecretProvider {
    fn id(&self) -> &str {
        "secret-provider-x9"
    }

    async fn interpret(&self, input: &InterpretationInput<'_>) -> Result<Interpretation> {
        HeuristicInterpreter.interpret(input).await
    }
}

fn web() -> FixtureServer {
    let server = FixtureServer::start();
    server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    server.html("/elsewhere/container-vms", APPLE_CONTAINER_SECOND_REPORT);
    server
}

fn with(isolated: &Isolated, interpreter: Arc<dyn Interpreter>) -> StreamRuntime {
    runtime_at(isolated).with_interpreter(interpreter)
}

#[tokio::test]
async fn semantic_model_is_advisory_ungrounded_claims_never_become_signals() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = with(&isolated, Arc::new(FabricatingInterpreter));

    let report = runtime
        .add_and_observe_url(&server.url("/news/apple-container"), "test", None)
        .await
        .unwrap();
    assert_eq!(report.stage, ProcessingStage::Observed, "observation itself succeeds");
    assert!(report.signals_created.is_empty(), "no signal from fabricated claims");
    assert!(!report.rejected.is_empty());
    assert_eq!(report.rejected[0].rejections[0].claim, "change");

    // Authority is untouched: the item is durable and the refusal is on record.
    assert!(!runtime.list_items().await.unwrap().is_empty());
    assert!(runtime.list_signals().await.unwrap().is_empty());
    let refusals = runtime
        .store()
        .find("SemanticDecision", json!({ "advisory_state": "rejected" }))
        .await
        .unwrap();
    assert!(!refusals.is_empty());
    assert_eq!(refusals[0]["model"], "fabricating-provider");
}

#[tokio::test]
async fn semantic_provider_is_replaceable_and_invisible_to_presentation() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = with(&isolated, Arc::new(SecretProvider));
    let report = runtime
        .add_and_observe_url(&server.url("/news/apple-container"), "test", None)
        .await
        .unwrap();
    let signal_id = report.primary_signal_id.unwrap();

    let presented = serde_json::to_string(&runtime.get_signal(&signal_id).await.unwrap()).unwrap()
        + &serde_json::to_string(&runtime.list_signals().await.unwrap()).unwrap();
    assert!(!presented.contains("secret-provider-x9"), "presentation must not know the provider");

    // ...while the durable record keeps it for audit.
    let record = runtime.store().get("Signal", signal_id.as_str()).await.unwrap().unwrap();
    assert_eq!(record["interpreter"], "secret-provider-x9");
    assert_eq!(record["advisory_state"], "advisory");
}

#[tokio::test]
async fn feltdb_is_the_only_authority() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    runtime
        .add_context(NewContext { name: "Portable compute".into(), kind: Some(ContextKind::Interest), ..Default::default() })
        .await
        .unwrap();
    runtime.add_and_observe_url(&server.url("/news/apple-container"), "test", None).await.unwrap();
    runtime.add_and_observe_url(&server.url("/elsewhere/container-vms"), "test", None).await.unwrap();

    // Every piece of product state is a FeltDB record, readable directly.
    for collection in ["Source", "FetchAttempt", "Item", "Provenance", "ContextEntry", "Signal", "Evidence", "Connection", "ItemRelation", "SemanticDecision"] {
        assert!(
            !runtime.store().all(collection).await.unwrap().is_empty(),
            "{collection} should hold durable state"
        );
    }

    // A second runtime over the same FeltDB (a different process, no shared
    // memory) sees exactly the same signals.
    let other = runtime_at(&isolated);
    let ids = |list: Vec<stream_core::SignalSummary>| list.into_iter().map(|s| s.signal.id).collect::<Vec<_>>();
    assert_eq!(ids(runtime.list_signals().await.unwrap()), ids(other.list_signals().await.unwrap()));
}

#[tokio::test]
async fn feed_formats_are_adapters_under_the_generic_source_model() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);

    // The user never names a format. A feed URL and a page URL are both just URLs.
    let feed = runtime
        .add_and_observe_url(&server.url("/news/feed.xml"), "test", None)
        .await
        .unwrap();
    assert_eq!(feed.stage, ProcessingStage::Observed);
    assert_eq!(feed.source.adapter_kind, SourceKind::Rss);
    let page = runtime
        .add_and_observe_url(&server.url("/elsewhere/container-vms"), "test", None)
        .await
        .unwrap();
    assert_eq!(page.source.adapter_kind, SourceKind::Web);
    for source in [&feed.source, &page.source] {
        assert!(source.canonical_url.as_str().starts_with("https://"));
        assert_eq!(source.provenance, "test");
    }
}

#[tokio::test]
async fn information_density_over_information_volume() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    runtime
        .add_context(NewContext { name: "Portable compute".into(), ..Default::default() })
        .await
        .unwrap();
    runtime.add_and_observe_url(&server.url("/news/apple-container"), "test", None).await.unwrap();
    runtime.add_and_observe_url(&server.url("/elsewhere/container-vms"), "test", None).await.unwrap();

    let items = runtime.list_items().await.unwrap().len();
    let today = runtime.today().await.unwrap();
    assert!(items >= 4, "Stream retains every observation ({items})");
    assert!(today.len() < items, "Today shows fewer, denser signals than raw items");
    let evidence: usize = today.iter().map(|s| s.evidence_count).sum();
    assert!(evidence > today.len(), "density: more evidence per signal than signals");
}
