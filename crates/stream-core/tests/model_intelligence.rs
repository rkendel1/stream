//! Model-backed intelligence through the real provider path (a local
//! OpenAI-compatible server), and the boundaries around it.

use std::collections::HashSet;
use stream_core::{AskRequest, NewContext};
use stream_model::{ClaimBasis, ClaimKind, ContextKind, ProcessingStage};
use stream_reason::Sufficiency;
use stream_testkit::{
    model_runtime_at, runtime_at, FakeModelServer, FixtureServer, Isolated, ModelMode, APPLE_CONTAINER_ARTICLE,
    APPLE_CONTAINER_DENIAL, APPLE_CONTAINER_SECOND_REPORT, APPLE_CONTAINER_THIRD_REPORT, NEWS_FEED,
};

fn web() -> FixtureServer {
    let server = FixtureServer::start();
    server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    server
}

fn elsewhere(page: &str) -> (FixtureServer, String) {
    let server = FixtureServer::start();
    let url = server.html("/report", page);
    (server, url)
}

fn portable_compute() -> NewContext {
    NewContext {
        name: "Portable compute".into(),
        kind: Some(ContextKind::Interest),
        description: Some("Running workloads anywhere".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_model_interprets_through_the_provider_and_the_gate_keeps_only_grounded_claims() {
    let web = web();
    let model = FakeModelServer::start(ModelMode::Cooperative);
    let isolated = Isolated::new();
    let runtime = model_runtime_at(&isolated, &model);
    runtime.add_context(portable_compute()).await.unwrap();

    let report = runtime.add_and_observe_url(&web.url("/news/apple-container"), "test", None).await.unwrap();
    assert_eq!(report.stage, ProcessingStage::Observed);
    let signal_id = report.primary_signal_id.clone().expect("model interpretation produced a signal");

    // The request is structured and carries the user's context.
    let first = &model.requests()[0];
    assert_eq!(first["response_format"]["type"], "json_schema");
    assert_eq!(first["response_format"]["json_schema"]["name"], "stream_interpretation");
    assert!(first["messages"][1]["content"].as_str().unwrap().contains("Portable compute"));

    let detail = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    let bases = detail.claims.iter().map(|c| c.basis).collect::<Vec<_>>();
    assert!(bases.contains(&ClaimBasis::Observed) && bases.contains(&ClaimBasis::Inferred) && bases.contains(&ClaimBasis::Hypothesis), "{bases:?}");
    assert!(detail.claims.iter().all(|c| !c.statement.contains("acquired Docker")), "fabricated claim refused");
    assert!(detail.summary.signal.why_it_matters.as_deref().unwrap_or_default().contains("isolation layer"));
    assert_eq!(detail.summary.connected_to[0].label, "Portable compute");

    // Every factual claim has provenance, traceable to the original source.
    for claim in detail.claims.iter().filter(|c| c.basis.requires_evidence()) {
        assert!(!claim.evidence_ids.is_empty(), "{claim:?}");
        for id in &claim.evidence_ids {
            let trace = runtime.get_evidence(id).await.unwrap().expect("addressable evidence");
            assert!(runtime.evidence_is_grounded(&trace.evidence).await.unwrap());
            assert_eq!(trace.source.unwrap().id, report.source.id);
            assert!(trace.evidence.url.as_str().contains("/news/apple-container"));
        }
    }
    let hypothesis = detail.claims.iter().find(|c| c.basis == ClaimBasis::Hypothesis).unwrap();
    assert!(hypothesis.evidence_ids.is_empty(), "a hypothesis is not dressed up as evidence");

    // The refusal is observable and durable; the provider stays invisible to presentation.
    let events = runtime.intelligence_events().await.unwrap();
    assert!(events.iter().any(|e| e.operation == "interpret" && e.status == "rejected" && e.detail.contains("claim:observed")));
    let presented = serde_json::to_string(&detail).unwrap();
    assert!(!presented.contains("fake-model"), "presentation must not know the provider");
    let record = runtime.store().get("Signal", signal_id.as_str()).await.unwrap().unwrap();
    assert!(record["interpreter"].as_str().unwrap().contains("fake-model"), "audit keeps it");
}

#[tokio::test]
async fn provider_failures_are_durable_and_stream_falls_back_to_local_intelligence() {
    for mode in [ModelMode::Failing, ModelMode::Malformed] {
        let web = web();
        let model = FakeModelServer::start(mode);
        let isolated = Isolated::new();
        let runtime = model_runtime_at(&isolated, &model);
        runtime.add_context(portable_compute()).await.unwrap();

        let report = runtime.add_and_observe_url(&web.url("/news/apple-container"), "test", None).await.unwrap();
        assert_eq!(report.stage, ProcessingStage::Observed, "{mode:?}: the product loop survives the model");
        assert!(report.primary_signal_id.is_some(), "{mode:?}: the local interpreter took over");
        assert!(!report.notices.is_empty(), "{mode:?}: the degradation is visible");

        let answer = runtime.ask(AskRequest { question: "Why does this matter to portable compute?".into(), ..Default::default() }).await.unwrap();
        assert!(!answer.statements.is_empty(), "{mode:?}: local reasoning answered");
        assert!(!answer.notices.is_empty());

        // Restart: the failures are still on record.
        let restarted = runtime_at(&isolated);
        let events = restarted.intelligence_events().await.unwrap();
        let failed = |op: &str| events.iter().any(|e| e.operation == op && e.status == "failed");
        assert!(failed("interpret") && failed("reason"), "{mode:?}: {events:#?}");
        assert!(events.iter().any(|e| e.status == "fallback"));
        if mode == ModelMode::Malformed {
            assert!(events.iter().any(|e| e.detail.contains("structured output")), "{events:#?}");
        }
    }
}

#[tokio::test]
async fn several_sources_synthesize_into_one_richer_signal_with_every_source_kept() {
    let web = web();
    let (_b, second) = elsewhere(APPLE_CONTAINER_SECOND_REPORT);
    let (_c, third) = elsewhere(APPLE_CONTAINER_THIRD_REPORT);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    runtime.add_context(portable_compute()).await.unwrap();

    let first = runtime.add_and_observe_url(&web.url("/news/apple-container"), "test", None).await.unwrap();
    let signal_id = first.primary_signal_id.unwrap();
    for url in [&second, &third] {
        let report = runtime.add_and_observe_url(url, "test", None).await.unwrap();
        assert_eq!(report.primary_signal_id.as_ref(), Some(&signal_id), "{url} is the same change");
    }

    let detail = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    assert_eq!(detail.summary.source_count, 3);
    assert_eq!(detail.observations.len(), 3, "every underlying source is preserved");
    let synthesis = detail.synthesis.expect("a synthesis across sources");
    assert_eq!(synthesis.observation_count, 3);
    assert!(synthesis.agreements[0].statement.contains("3 observations from 3 publishers agree that Apple Container adds portable Linux VMs"), "{:?}", synthesis.agreements);
    assert!(synthesis.new_information.iter().any(|p| p.statement.contains("Apache 2.0")), "{:?}", synthesis.new_information);
    for point in synthesis.agreements.iter().chain(synthesis.new_information.iter()) {
        assert!(!point.evidence_ids.is_empty());
        for id in &point.evidence_ids {
            assert!(runtime.get_evidence(id).await.unwrap().is_some(), "synthesis cites real evidence");
        }
    }
    let ranking = &detail.summary.ranking;
    assert!(ranking.factors.iter().any(|f| f.key == "corroboration" && f.explanation.contains("3 independent sources")));
}

#[tokio::test]
async fn a_model_synthesis_is_gated_to_the_signals_own_evidence() {
    let web = web();
    let (_b, second) = elsewhere(APPLE_CONTAINER_SECOND_REPORT);
    let model = FakeModelServer::start(ModelMode::Cooperative);
    let isolated = Isolated::new();
    let runtime = model_runtime_at(&isolated, &model);
    let first = runtime.add_and_observe_url(&web.url("/news/apple-container"), "test", None).await.unwrap();
    let report = runtime.add_and_observe_url(&second, "test", None).await.unwrap();
    assert_eq!(report.primary_signal_id, first.primary_signal_id, "the model linked the same change");

    let detail = runtime.get_signal(first.primary_signal_id.as_ref().unwrap()).await.unwrap().unwrap();
    let synthesis = detail.synthesis.unwrap();
    assert_eq!(synthesis.agreements.len(), 1, "the agreement citing foreign evidence was dropped");
    assert_eq!(synthesis.uncertainties[0].basis, ClaimBasis::Hypothesis);
    let events = runtime.intelligence_events().await.unwrap();
    assert!(events.iter().any(|e| e.operation == "synthesize" && e.status == "rejected"));
}

#[tokio::test]
async fn contradictory_evidence_stays_visible() {
    let web = web();
    let (_d, denial) = elsewhere(APPLE_CONTAINER_DENIAL);
    let isolated = Isolated::new();
    let runtime = runtime_at(&isolated);
    let first = runtime.add_and_observe_url(&web.url("/news/apple-container"), "test", None).await.unwrap();
    let signal_id = first.primary_signal_id.unwrap();
    let report = runtime.add_and_observe_url(&denial, "test", None).await.unwrap();
    assert_eq!(report.primary_signal_id.as_ref(), Some(&signal_id), "a dispute of the same change joins its signal");

    let detail = runtime.get_signal(&signal_id).await.unwrap().unwrap();
    assert_eq!(detail.summary.contradiction_count, 1);
    let disputing = detail.evidence.iter().filter(|t| t.evidence.claim == ClaimKind::Contradiction).collect::<Vec<_>>();
    assert!(!disputing.is_empty());
    assert!(runtime.get_evidence(&disputing[0].evidence.id).await.unwrap().is_some());
    let synthesis = detail.synthesis.unwrap();
    assert!(synthesis.differences[0].statement.contains("disputes it"), "{:?}", synthesis.differences);
    assert!(synthesis.uncertainties.iter().any(|p| p.statement.contains("disagree")));
    assert!(detail.summary.ranking.factors.iter().any(|f| f.key == "disputed" && f.value > 0.0));

    let answer = runtime
        .ask(AskRequest { question: "What contradicts this?".into(), focus: stream_core::Focus { signal_ids: vec![signal_id], ..Default::default() }, ..Default::default() })
        .await
        .unwrap();
    assert!(answer.statements.iter().any(|s| s.text.contains("disput")), "{answer:#?}");
}

#[tokio::test]
async fn model_answers_pass_the_answer_gate() {
    let web = web();
    let model = FakeModelServer::start(ModelMode::Cooperative);
    let isolated = Isolated::new();
    let runtime = model_runtime_at(&isolated, &model);
    runtime.add_context(portable_compute()).await.unwrap();
    runtime.add_and_observe_url(&web.url("/news/apple-container"), "test", None).await.unwrap();

    let answer = runtime
        .ask(AskRequest { question: "Why does this matter to portable compute?".into(), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(answer.sufficiency, Sufficiency::Sufficient);
    let texts = answer.statements.iter().map(|s| s.text.as_str()).collect::<Vec<_>>();
    assert!(!texts.iter().any(|t| t.contains("open-source macOS")), "invented evidence refused: {texts:?}");
    assert!(answer.statements.iter().any(|s| s.basis == ClaimBasis::Hypothesis));
    let cited = answer.statements.iter().flat_map(|s| s.evidence_ids.iter()).collect::<HashSet<_>>();
    let traced = answer.evidence.iter().map(|t| &t.evidence.id).collect::<HashSet<_>>();
    assert_eq!(cited, traced, "every cited piece of evidence is traced for the user");
    assert!(answer.uncertainties.iter().any(|u| u.contains("withheld")));
    assert!(runtime.intelligence_events().await.unwrap().iter().any(|e| e.operation == "reason" && e.status == "rejected"));
    let request = model.requests().into_iter().find(|r| r["response_format"]["json_schema"]["name"] == "stream_answer").unwrap();
    assert!(request["messages"][1]["content"].as_str().unwrap().contains("evidence_"), "the model reasons over retrieved evidence ids");
}
