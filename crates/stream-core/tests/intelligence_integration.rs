//! The Stream product loop, end to end against real FeltDB and a local web.

use std::collections::HashSet;
use stream_core::NewContext;
use stream_model::{
    ClaimKind, ConnectionRelation, ConnectionTargetKind, ContextKind, FetchStatus, ProcessingStage, SignalStatus,
    SourceKind, SourceStatus,
};
use stream_testkit::{
    runtime_at, FixtureServer, Isolated, APPLE_CONTAINER_ARTICLE, APPLE_CONTAINER_SECOND_REPORT, BAKERY_ARTICLE,
    NEWS_FEED,
};

fn web() -> FixtureServer {
    let server = FixtureServer::start();
    server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    server.html("/elsewhere/container-vms", APPLE_CONTAINER_SECOND_REPORT);
    server.html("/bakery", BAKERY_ARTICLE);
    server
}

fn portable_compute() -> NewContext {
    NewContext {
        name: "Portable compute".into(),
        kind: Some(ContextKind::Interest),
        description: Some("Running workloads anywhere with containers and lightweight VMs".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn url_creates_durable_source_and_duplicates_resolve_to_it() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);

    let url = server.url("/news/apple-container");
    let added = runtime.add_url(&url, "test").await.unwrap();
    assert!(!added.existing);
    assert_eq!(added.source.stage, ProcessingStage::Queued, "durable before anything is fetched");
    assert_eq!(added.source.original_url.as_str(), url);
    assert!(added.source.canonical_url.as_str().starts_with("https://127.0.0.1:"));
    assert_eq!(added.source.provenance, "test");

    for variant in [
        format!("{url}/"),
        format!("{url}?utm_source=newsletter"),
        format!("{url}#comments"),
        format!("  {url}?utm_campaign=x&fbclid=y  "),
    ] {
        let again = runtime.add_url(&variant, "test").await.unwrap();
        assert!(again.existing, "{variant} should resolve to the existing source");
        assert_eq!(again.source.id, added.source.id);
    }

    let restarted = runtime_at(&isolated);
    let sources = restarted.list_sources().await.unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].canonical_url, added.source.canonical_url);
}

#[tokio::test]
async fn a_url_becomes_a_durable_evidence_backed_signal_that_survives_restart() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);

    let attn = runtime
        .add_context(NewContext { name: "Attn".into(), kind: Some(ContextKind::Project), ..Default::default() })
        .await
        .unwrap();
    let compute = runtime
        .add_context(NewContext { related: vec!["Attn".into()], ..portable_compute() })
        .await
        .unwrap();
    assert_eq!(compute.related, vec![attn.id.clone()]);

    let stages = std::sync::Mutex::new(Vec::new());
    let record = |stage: ProcessingStage| stages.lock().unwrap().push(stage);
    let report = runtime
        .add_and_observe_url(&server.url("/news/apple-container"), "test", Some(&record))
        .await
        .unwrap();
    assert_eq!(report.stage, ProcessingStage::Observed, "{:?}", report.failure);
    assert_eq!(
        stages.into_inner().unwrap(),
        vec![
            ProcessingStage::Fetching,
            ProcessingStage::Understanding,
            ProcessingStage::Connecting,
            ProcessingStage::BuildingSignal,
            ProcessingStage::Observed
        ]
    );
    let signal_id = report.primary_signal_id.clone().expect("the user's URL produced a signal");

    let detail = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    let signal = &detail.summary.signal;
    assert_eq!(signal.topic.label, "Compute & Infrastructure");
    assert_eq!(signal.subject.label, "Apple Container");
    assert_eq!(signal.change.statement, "Adds portable Linux VMs");
    let why = signal.why_it_matters.as_deref().expect("evaluated against context");
    assert!(why.contains("Portable compute"), "{why}");
    assert!(signal.advisory);

    let connected = detail.summary.connected_to.iter().map(|c| (c.label.as_str(), c.relation)).collect::<Vec<_>>();
    assert!(connected.contains(&("Portable compute", ConnectionRelation::Matches)), "{connected:?}");
    assert!(connected.contains(&("Attn", ConnectionRelation::Via)), "{connected:?}");
    assert!(detail
        .connections
        .iter()
        .any(|c| c.target_kind == ConnectionTargetKind::Topic && c.relation == ConnectionRelation::About));
    assert!(detail.connections.iter().any(|c| c.target_kind == ConnectionTargetKind::Subject));

    // Every claim is traceable: Signal → Evidence → Item → Source → URL.
    let claims = detail.evidence.iter().map(|trace| trace.evidence.claim).collect::<HashSet<_>>();
    for claim in [ClaimKind::Topic, ClaimKind::Subject, ClaimKind::Change, ClaimKind::WhyItMatters, ClaimKind::Connection] {
        assert!(claims.contains(&claim), "missing evidence for {claim}");
    }
    for trace in &detail.evidence {
        assert!(runtime.evidence_is_grounded(&trace.evidence).await.unwrap(), "{:?}", trace.evidence);
        let item = trace.item.as_ref().expect("evidence resolves to an item");
        let source = trace.source.as_ref().expect("evidence resolves to a source");
        assert_eq!(source.id, report.source.id);
        assert_eq!(item.source.as_ref().unwrap().id, source.id);
        assert!(trace.provenance.is_some(), "evidence keeps provenance");
        assert!(trace.evidence.url.as_str().contains("/news/apple-container"));
    }
    for connection in detail.connections.iter().filter(|c| c.relation == ConnectionRelation::Matches) {
        assert!(!connection.evidence_ids.is_empty(), "context connections cite evidence");
    }

    // Restart: a new runtime (new bridge process, no caches) sees it all.
    drop(runtime);
    let restarted = runtime_at(&isolated);
    let again = restarted.get_signal(&signal_id).await.unwrap().expect("signal survives restart");
    assert_eq!(
        serde_json::to_value(&again.summary.signal).unwrap(),
        serde_json::to_value(&detail.summary.signal).unwrap()
    );
    assert_eq!(again.evidence.len(), detail.evidence.len());
    assert_eq!(again.connections.len(), detail.connections.len());
    let contexts = restarted.list_contexts().await.unwrap();
    assert_eq!(contexts.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["Attn", "Portable compute"]);
    let source = restarted.get_source(&report.source.id).await.unwrap().unwrap();
    assert_eq!(source.stage, ProcessingStage::Observed);
    assert!(source.last_observed_at.is_some());
}

#[tokio::test]
async fn feed_capable_urls_become_ongoing_sources_and_only_meaningful_items_become_signals() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    runtime
        .add_context(NewContext {
            name: "Attn".into(),
            kind: Some(ContextKind::Project),
            description: Some("Handoff between autonomous software and people".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let report = runtime
        .add_and_observe_url(&server.url("/news/apple-container"), "test", None)
        .await
        .unwrap();
    let source = &report.source;
    assert_eq!(source.kind, SourceKind::Web, "what the URL is, for the user");
    assert_eq!(source.adapter_kind, SourceKind::Rss, "how Stream keeps observing it");
    assert!(source.endpoint.as_str().ends_with("/news/feed.xml"));

    // The article page and its own feed entry are one item; the party photos
    // are kept as durable items but do not become signals.
    let items = runtime.list_items().await.unwrap();
    assert_eq!(items.len(), 3, "{:#?}", items.iter().map(|i| &i.item.title).collect::<Vec<_>>());
    let signals = runtime.list_signals().await.unwrap();
    let subjects = signals.iter().map(|s| s.signal.subject.label.as_str()).collect::<Vec<_>>();
    assert!(subjects.contains(&"Apple Container"), "{subjects:?}");
    assert!(subjects.contains(&"Attn"), "{subjects:?}");
    assert_eq!(signals.len(), 2, "{subjects:?}");
    assert_eq!(report.understood_without_signal.len(), 1);

    // Re-observing reads the feed and finds nothing new.
    let again = runtime.observe_source(&source.id, None).await.unwrap();
    assert_eq!(again.stage, ProcessingStage::Observed);
    assert!(again.new_item_ids.is_empty());
    assert_eq!(runtime.list_signals().await.unwrap().len(), 2);
    let detail = runtime.source_detail(&source.id).await.unwrap().unwrap();
    assert_eq!(detail.attempts.len(), 2, "observation history is durable");
}

#[tokio::test]
async fn multiple_observations_of_one_change_consolidate_but_keep_their_evidence() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    runtime.add_context(portable_compute()).await.unwrap();

    let first = runtime
        .add_and_observe_url(&server.url("/news/apple-container"), "test", None)
        .await
        .unwrap();
    let second = runtime
        .add_and_observe_url(&server.url("/elsewhere/container-vms"), "test", None)
        .await
        .unwrap();
    let signal_id = first.primary_signal_id.clone().unwrap();
    assert_ne!(first.source.id, second.source.id, "two sources");
    assert_eq!(second.signals_corroborated, vec![signal_id.clone()], "{second:#?}");
    assert!(second.signals_created.is_empty());
    assert_eq!(second.primary_signal_id, Some(signal_id.clone()));

    let today = runtime.today().await.unwrap();
    let apple = today.iter().filter(|s| s.signal.subject.label == "Apple Container").collect::<Vec<_>>();
    assert_eq!(apple.len(), 1, "one underlying change");
    assert_eq!(apple[0].source_count, 2, "two sources of evidence");
    assert_eq!(apple[0].observation_count, 2);

    let detail = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    let observed_items = detail.evidence.iter().map(|t| t.evidence.item_id.clone()).collect::<HashSet<_>>();
    assert_eq!(observed_items.len(), 2);
    assert!(detail.evidence.iter().any(|t| t.evidence.claim == ClaimKind::Corroboration));
    // Each observation stays individually addressable.
    for trace in &detail.evidence {
        let fetched = runtime.get_evidence(&trace.evidence.id).await.unwrap().expect("addressable evidence");
        assert_eq!(fetched.evidence, trace.evidence);
        assert_eq!(fetched.item.unwrap().id, trace.evidence.item_id);
    }
    assert!(detail
        .connections
        .iter()
        .any(|c| c.target_kind == ConnectionTargetKind::Item && c.relation == ConnectionRelation::SameChange));

    // The relation reuses the existing ItemRelation infrastructure.
    let graph = runtime.connection_graph().await.unwrap();
    assert!(graph
        .item_relations
        .iter()
        .any(|r| r.relation == stream_model::RelationKind::SameStory && r.to_item_id == detail.summary.signal.item_id));
    let subject = graph.subjects.iter().find(|s| s.label == "Apple Container").unwrap();
    assert_eq!(subject.observation_count, 2);
    let explanation = &apple[0].ranking;
    assert!(explanation.factors.iter().any(|f| f.key == "corroboration" && f.value > 0.0));
}

#[tokio::test]
async fn failures_are_durable_state_not_disappearing_urls() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let url = server.failing("/broken", 500);

    let report = runtime.add_and_observe_url(&url, "test", None).await.unwrap();
    assert_eq!(report.stage, ProcessingStage::Failed);
    let failure = report.failure.clone().unwrap();
    assert!(failure.contains("500"), "{failure}");

    let restarted = runtime_at(&isolated);
    let detail = restarted.source_detail(&report.source.id).await.unwrap().unwrap();
    assert_eq!(detail.source.status, SourceStatus::Failed);
    assert_eq!(detail.source.stage, ProcessingStage::Failed);
    assert_eq!(detail.source.original_url.as_str(), url, "the URL is never lost");
    assert_eq!(detail.attempts[0].status, FetchStatus::Failed);
    assert_eq!(detail.attempts[0].diagnostics["category"], "network");

    // Retrying after the page recovers reuses the same source.
    server.html("/broken", BAKERY_ARTICLE);
    let retry = restarted.add_and_observe_url(&url, "test", None).await.unwrap();
    assert_eq!(retry.source.id, report.source.id);
    assert_eq!(retry.stage, ProcessingStage::Observed);
}

#[tokio::test]
async fn adding_context_later_connects_what_stream_has_already_seen() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let report = runtime
        .add_and_observe_url(&server.url("/news/apple-container"), "test", None)
        .await
        .unwrap();
    let signal_id = report.primary_signal_id.unwrap();
    let before = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    assert!(before.summary.signal.why_it_matters.is_none(), "no context, no claim that it matters");
    assert!(before.summary.connected_to.is_empty());

    runtime.add_context(portable_compute()).await.unwrap();
    let after = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    assert!(after.summary.signal.why_it_matters.unwrap().contains("Portable compute"));
    assert_eq!(after.summary.connected_to[0].label, "Portable compute");
}

#[tokio::test]
async fn today_ranks_by_explainable_information_density() {
    let server = web();
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    runtime.add_context(portable_compute()).await.unwrap();
    runtime.add_and_observe_url(&server.url("/bakery"), "test", None).await.unwrap();
    runtime.add_and_observe_url(&server.url("/news/apple-container"), "test", None).await.unwrap();

    let today = runtime.today().await.unwrap();
    assert_eq!(today[0].signal.subject.label, "Apple Container", "context beats recency");
    assert_eq!(today[0].position, 1);
    assert!(today[0].ranking.summary.contains("Portable compute"), "{}", today[0].ranking.summary);
    let bakery = today.iter().find(|s| s.signal.topic.label != "Compute & Infrastructure").unwrap();
    assert!(bakery.ranking.factors.iter().any(|f| f.key == "context" && f.value == 0.0));

    let resolved = runtime.set_signal_status(&today[0].signal.id, SignalStatus::Resolved).await.unwrap();
    assert_eq!(resolved.status, SignalStatus::Resolved);
    let today = runtime.today().await.unwrap();
    assert!(today.iter().all(|s| s.signal.subject.label != "Apple Container"), "resolved leaves Today");
    assert_eq!(runtime.list_signals().await.unwrap().len(), 2, "but stays durable");
}
