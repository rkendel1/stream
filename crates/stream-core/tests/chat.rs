//! Thinking with Stream: retrieval, grounded answers, and saved insights.

use std::collections::HashSet;
use stream_core::{AskRequest, Focus, HistoryTurn, NewContext, NewInsight, StreamRuntime};
use stream_model::{ClaimBasis, ContextKind, InsightKind, InsightStatus};
use stream_reason::{Intent, Sufficiency};
use stream_testkit::{
    runtime_at, FixtureServer, Isolated, APPLE_CONTAINER_ARTICLE, APPLE_CONTAINER_SECOND_REPORT,
    APPLE_CONTAINER_THIRD_REPORT, BAKERY_ARTICLE, NEWS_FEED,
};

struct World {
    runtime: StreamRuntime,
    isolated: Isolated,
    _servers: Vec<FixtureServer>,
    urls: Vec<String>,
}

/// The acceptance journey's setup: context, then three URLs about one change
/// from three independent publishers, plus one unrelated URL.
async fn world() -> World {
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let a = FixtureServer::start();
    a.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    a.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    a.html("/bakery", BAKERY_ARTICLE);
    let b = FixtureServer::start();
    b.html("/report", APPLE_CONTAINER_SECOND_REPORT);
    let c = FixtureServer::start();
    c.html("/report", APPLE_CONTAINER_THIRD_REPORT);

    runtime
        .add_context(NewContext { name: "AppPort".into(), kind: Some(ContextKind::Project), description: Some("Portable application capability surface".into()), ..Default::default() })
        .await
        .unwrap();
    runtime
        .add_context(NewContext {
            name: "Portable compute".into(),
            kind: Some(ContextKind::Interest),
            description: Some("Running workloads anywhere with containers and lightweight VMs".into()),
            related: vec!["AppPort".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let urls = vec![a.url("/news/apple-container"), b.url("/report"), c.url("/report"), a.url("/bakery")];
    for url in &urls {
        runtime.add_and_observe_url(url, "test", None).await.unwrap();
    }
    World { runtime, isolated, _servers: vec![a, b, c], urls }
}

fn ask(question: &str) -> AskRequest {
    AskRequest { question: question.into(), ..Default::default() }
}

fn follow_up(question: &str, previous: &stream_core::Answer) -> AskRequest {
    AskRequest {
        question: question.into(),
        history: vec![HistoryTurn { question: previous.question.clone(), signal_ids: previous.signals.iter().map(|s| s.id.clone()).collect() }],
        ..Default::default()
    }
}

fn assert_provenance(answer: &stream_core::Answer) {
    let traced = answer.evidence.iter().map(|t| t.evidence.id.clone()).collect::<HashSet<_>>();
    for statement in &answer.statements {
        if statement.basis.requires_evidence() {
            assert!(!statement.evidence_ids.is_empty(), "unsupported statement: {statement:?}");
        }
        for id in &statement.evidence_ids {
            assert!(traced.contains(id), "cited evidence {id} is not traced for the user");
        }
    }
    for trace in &answer.evidence {
        assert!(trace.item.is_some() && trace.source.is_some(), "evidence must reach its item and source");
    }
    // Answers rest only on what Stream retrieved, which the answer discloses.
    let retrieved = answer.retrieved.signal_ids.iter().collect::<HashSet<_>>();
    assert!(answer.signals.iter().all(|s| retrieved.contains(&s.id)));
}

#[tokio::test]
async fn the_acceptance_journey_thinks_with_stream_and_keeps_what_is_saved() {
    let World { runtime, isolated, urls, .. } = world().await;

    // Three URLs, one underlying change.
    let today = runtime.today().await.unwrap();
    let apple = today.iter().find(|s| s.signal.subject.label == "Apple Container").expect("one Apple Container signal");
    assert_eq!(apple.source_count, 3);
    assert_eq!(today.iter().filter(|s| s.signal.subject.label == "Apple Container").count(), 1);

    // 5. Why does this matter to portable compute?
    let why = runtime.ask(ask("Why does this matter to portable compute?")).await.unwrap();
    assert_eq!(why.retrieved.intent, Intent::WhyMatters);
    assert_eq!(why.sufficiency, Sufficiency::Sufficient, "{why:#?}");
    assert_eq!(why.signals[0].subject, "Apple Container");
    let bases = why.statements.iter().map(|s| s.basis).collect::<HashSet<_>>();
    assert!(bases.contains(&ClaimBasis::Observed) && bases.contains(&ClaimBasis::Connected) && bases.contains(&ClaimBasis::Inferred), "{bases:?}");
    assert!(why.connected_to.iter().any(|c| c.label == "Portable compute"));
    assert!(!why.signals.iter().any(|s| s.subject.contains("bakery")), "unrelated signals are not retrieved");
    assert_provenance(&why);

    // 6. What evidence supports that?  ("that" = the previous answer)
    let evidence = runtime.ask(follow_up("What evidence supports that?", &why)).await.unwrap();
    assert_eq!(evidence.retrieved.intent, Intent::Evidence);
    assert!(evidence.retrieved.focused);
    assert!(evidence.statements.iter().all(|s| s.basis == ClaimBasis::Observed));
    assert!(evidence.sources.len() >= 3, "all three sources are shown: {:?}", evidence.sources);
    assert_provenance(&evidence);

    // 7. How does this connect to the other things I'm building?
    let connects = runtime.ask(follow_up("How does this connect to the other things I'm building?", &evidence)).await.unwrap();
    assert_eq!(connects.retrieved.intent, Intent::Connections);
    assert!(connects.summary.contains("AppPort"), "{}", connects.summary);
    assert!(connects.statements.iter().any(|s| s.text.contains("AppPort")), "{:#?}", connects.statements);
    assert!(connects.statements.iter().all(|s| s.basis == ClaimBasis::Connected));
    assert_provenance(&connects);

    // 8. What remains uncertain?
    let uncertain = runtime.ask(follow_up("What remains uncertain?", &connects)).await.unwrap();
    assert_eq!(uncertain.retrieved.intent, Intent::Uncertainty);
    assert!(uncertain.statements.iter().any(|s| s.text.contains("inference")), "{:#?}", uncertain.statements);
    assert!(uncertain.statements.iter().all(|s| s.basis != ClaimBasis::Observed), "uncertainties are not facts");
    assert_provenance(&uncertain);

    // 9. Save an insight from the answer.
    let statement = &why.statements.iter().find(|s| s.basis == ClaimBasis::Inferred).unwrap();
    let insight = runtime
        .save_insight(NewInsight {
            kind: Some(InsightKind::Insight),
            statement: statement.text.clone(),
            question: Some(why.question.clone()),
            evidence_ids: statement.evidence_ids.clone(),
            context_ids: why.connected_to.iter().filter(|c| c.label == "Portable compute").map(|c| stream_model::ContextId::new(c.id.clone())).collect(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(insight.basis, ClaimBasis::Inferred);
    assert_eq!(insight.signal_ids, vec![why.signals[0].id.clone()], "signals follow from the cited evidence");

    // 10. Restart: everything, including the insight, is still there.
    drop(runtime);
    let restarted = runtime_at(&isolated);
    let saved = restarted.list_insights(None).await.unwrap();
    assert_eq!(saved.len(), 1);
    let view = restarted.get_insight(&saved[0].id).await.unwrap().unwrap();
    assert_eq!(view.insight.question.as_deref(), Some("Why does this matter to portable compute?"));
    assert!(!view.evidence.is_empty() && view.evidence.iter().all(|t| t.source.is_some()));
    assert_eq!(view.contexts[0].label, "Portable compute");
    let detail = restarted.get_signal(&why.signals[0].id).await.unwrap().unwrap();
    assert_eq!(detail.insights.len(), 1, "the insight is part of the signal's knowledge");
    assert_eq!(restarted.list_sources().await.unwrap().len(), urls.len());
}

#[tokio::test]
async fn questions_without_evidence_say_so() {
    let World { runtime, .. } = world().await;
    let answer = runtime.ask(ask("What do we know about quantum biology?")).await.unwrap();
    assert_eq!(answer.sufficiency, Sufficiency::Insufficient);
    assert!(answer.statements.is_empty());
    assert!(answer.summary.contains("don't have enough evidence"));
    assert!(answer.retrieved.signal_ids.is_empty());

    let empty = runtime_at(&Isolated::new());
    let answer = empty.ask(ask("What changed recently?")).await.unwrap();
    assert_eq!(answer.sufficiency, Sufficiency::Insufficient);
}

#[tokio::test]
async fn asking_is_an_interface_not_memory() {
    let World { runtime, .. } = world().await;
    let collections = ["Source", "Item", "Signal", "Evidence", "Connection", "Claim", "Synthesis", "Insight", "ContextEntry", "SemanticDecision", "IntelligenceEvent"];
    let mut before = Vec::new();
    for collection in collections {
        before.push(runtime.store().all(collection).await.unwrap().len());
    }
    for question in ["Why does this matter to portable compute?", "What patterns are appearing across these sources?", "What should I investigate further?"] {
        runtime.ask(ask(question)).await.unwrap();
    }
    for (collection, count) in collections.iter().zip(before) {
        assert_eq!(runtime.store().all(collection).await.unwrap().len(), count, "asking wrote to {collection}");
    }
    // The same question over the same durable state gives the same answer:
    // nothing hidden (such as a conversation) shapes it.
    let first = runtime.ask(ask("Why does this matter to portable compute?")).await.unwrap();
    let second = runtime.ask(ask("Why does this matter to portable compute?")).await.unwrap();
    assert_eq!(serde_json::to_value(&first.statements).unwrap(), serde_json::to_value(&second.statements).unwrap());
}

#[tokio::test]
async fn temporal_questions_use_durable_history() {
    let World { runtime, .. } = world().await;
    let changed = runtime.ask(ask("What changed recently?")).await.unwrap();
    assert_eq!(changed.retrieved.intent, Intent::Changes);
    assert!(changed.retrieved.time_window.is_some());
    assert!(!changed.statements.is_empty());
    assert_provenance(&changed);

    let evolved = runtime.ask(ask("How has our understanding of portable compute evolved?")).await.unwrap();
    assert_eq!(evolved.retrieved.intent, Intent::Evolution);
    let texts = evolved.statements.iter().map(|s| s.text.clone()).collect::<Vec<_>>();
    let first_report = texts.iter().position(|t| t.contains("first reported")).expect("the first observation");
    let corroboration = texts.iter().position(|t| t.contains("corroborated it")).expect("later corroboration");
    assert!(first_report < corroboration, "chronological: {texts:#?}");
    assert!(texts.iter().any(|t| t.contains("You told Stream you care about Portable compute")));
    assert_provenance(&evolved);
}

#[tokio::test]
async fn ask_this_constrains_retrieval_to_what_the_user_pointed_at() {
    let World { runtime, .. } = world().await;
    let bakery = runtime.today().await.unwrap().into_iter().find(|s| s.signal.subject.label != "Apple Container" && s.signal.subject.label != "Attn").unwrap();
    let answer = runtime
        .ask(AskRequest { question: "What changed?".into(), focus: Focus { signal_ids: vec![bakery.signal.id.clone()], ..Default::default() }, ..Default::default() })
        .await
        .unwrap();
    assert!(answer.retrieved.focused);
    assert_eq!(answer.retrieved.signal_ids, vec![bakery.signal.id.clone()]);

    let why = runtime
        .ask(AskRequest { question: "Why does this matter?".into(), focus: Focus { signal_ids: vec![bakery.signal.id], ..Default::default() }, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(why.sufficiency, Sufficiency::Partial, "observed, but connected to nothing the user cares about");
    assert!(!why.uncertainties.is_empty());
}

#[tokio::test]
async fn insights_are_validated_advisory_objects_that_raise_open_questions() {
    let World { runtime, .. } = world().await;
    let apple = runtime.today().await.unwrap().into_iter().find(|s| s.signal.subject.label == "Apple Container").unwrap();

    let fabricated = runtime
        .save_insight(NewInsight { statement: "Apple bought Docker.".into(), evidence_ids: vec![stream_model::EvidenceId::new("evidence_nope")], ..Default::default() })
        .await;
    assert!(fabricated.is_err(), "insights cannot cite evidence that does not exist");
    let unsupported = runtime.save_insight(NewInsight { kind: Some(InsightKind::Insight), statement: "It's huge.".into(), ..Default::default() }).await;
    assert!(unsupported.is_err(), "an inferred insight needs evidence");

    let before = apple.ranking.total;
    let question = runtime
        .save_insight(NewInsight {
            kind: Some(InsightKind::Investigation),
            statement: "Does per-container VM isolation hold on Linux hosts too?".into(),
            signal_ids: vec![apple.signal.id.clone()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(question.basis, ClaimBasis::Hypothesis, "open work is never recorded as fact");
    let after = runtime.today().await.unwrap().into_iter().find(|s| s.signal.id == apple.signal.id).unwrap();
    let factor = after.ranking.factors.iter().find(|f| f.key == "open_questions").unwrap();
    assert!(factor.value > 0.0 && after.ranking.total > before, "{}", factor.explanation);

    let investigate = runtime.ask(ask("What should I investigate further?")).await.unwrap();
    assert!(investigate.statements.iter().any(|s| s.text.contains("per-container VM isolation")), "{:#?}", investigate.statements);

    runtime.set_insight_status(&question.id, InsightStatus::Resolved).await.unwrap();
    let resolved = runtime.today().await.unwrap().into_iter().find(|s| s.signal.id == apple.signal.id).unwrap();
    assert_eq!(resolved.ranking.factors.iter().find(|f| f.key == "open_questions").unwrap().value, 0.0);
}
