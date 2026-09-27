//! The AppPort surface drives the same runtime semantics every client uses.

use serde_json::{json, Value};
use stream_appport::{envelope, AppPortManifest, ErrorCode, StreamAppPort};
use stream_testkit::{runtime_at, FixtureServer, Isolated, APPLE_CONTAINER_ARTICLE, APPLE_CONTAINER_SECOND_REPORT, NEWS_FEED};

fn web() -> FixtureServer {
    let server = FixtureServer::start();
    server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    server.html("/elsewhere/container-vms", APPLE_CONTAINER_SECOND_REPORT);
    server
}

async fn ok(port: &StreamAppPort, capability: &str, input: Value) -> Value {
    match port.invoke(capability, input.clone()).await {
        Ok(value) => value,
        Err(error) => panic!("{capability} {input} failed: {error}"),
    }
}

#[tokio::test]
async fn context_source_signal_and_connection_apis_work_end_to_end() {
    let server = web();
    let isolated = Isolated::new();
    let port = StreamAppPort::new(runtime_at(&isolated)).with_provenance("appport-test");

    // Context
    let attn = ok(&port, "stream.context.add", json!({ "name": "Attn", "kind": "project" })).await;
    let compute = ok(
        &port,
        "stream.context.add",
        json!({ "name": "Portable compute", "description": "Running workloads anywhere", "related": ["Attn"] }),
    )
    .await;
    assert_eq!(compute["related"][0], attn["id"]);
    let contexts = ok(&port, "stream.context.list", json!({})).await;
    assert_eq!(contexts.as_array().unwrap().len(), 2);

    // Source
    let added = ok(&port, "stream.source.add", json!({ "url": server.url("/news/apple-container") })).await;
    assert_eq!(added["existing"], false);
    let report = &added["report"];
    assert_eq!(report["stage"], "observed");
    assert_eq!(report["source"]["provenance"], "appport-test");
    let source_id = report["source"]["id"].as_str().unwrap().to_owned();
    let signal_id = report["primary_signal_id"].as_str().unwrap().to_owned();

    let duplicate = ok(
        &port,
        "stream.source.add",
        json!({ "url": format!("{}?utm_source=x", server.url("/news/apple-container")), "observe": false }),
    )
    .await;
    assert_eq!(duplicate["existing"], true);
    assert_eq!(duplicate["source"]["id"], source_id.as_str());

    let sources = ok(&port, "stream.source.list", json!({})).await;
    assert_eq!(sources.as_array().unwrap().len(), 1);
    let source = ok(&port, "stream.source.get", json!({ "id": source_id })).await;
    assert_eq!(source["attempts"].as_array().unwrap().len(), 1);
    assert!(source["signal_ids"].as_array().unwrap().iter().any(|id| id == signal_id.as_str()));

    // Items
    let items = ok(&port, "stream.item.list", json!({})).await;
    let item_id = items[0]["item"]["id"].as_str().unwrap().to_owned();
    let item = ok(&port, "stream.item.get", json!({ "id": item_id })).await;
    assert!(!item["provenance"].as_array().unwrap().is_empty());

    // Signals
    let signals = ok(&port, "stream.signal.list", json!({})).await;
    let first = &signals[0];
    assert_eq!(first["position"], 1);
    assert!(first["ranking"]["factors"].as_array().unwrap().len() >= 5, "ranking explains itself");
    let signal = ok(&port, "stream.signal.get", json!({ "id": signal_id })).await;
    let view = &signal["summary"]["signal"];
    assert_eq!(view["subject"]["label"], "Apple Container");
    assert!(view["why_it_matters"].as_str().unwrap().contains("Portable compute"));
    assert!(view.get("interpreter").is_none(), "presentation never sees the semantic provider");
    let labels = signal["summary"]["connected_to"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["label"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert!(labels.contains(&"Portable compute".to_owned()) && labels.contains(&"Attn".to_owned()), "{labels:?}");

    // Evidence
    let evidence_id = signal["evidence"][0]["evidence"]["id"].as_str().unwrap().to_owned();
    let evidence = ok(&port, "stream.evidence.get", json!({ "id": evidence_id })).await;
    assert_eq!(evidence["source"]["id"], source_id.as_str());
    assert!(evidence["evidence"]["url"].as_str().unwrap().contains("/news/apple-container"));

    // Connections
    let by_signal = ok(&port, "stream.connection.list", json!({ "id": signal_id })).await;
    assert!(by_signal.as_array().unwrap().iter().any(|c| c["connection"]["target_kind"] == "context"));
    let by_context = ok(&port, "stream.connection.list", json!({ "id": compute["id"] })).await;
    assert!(by_context.as_array().unwrap().iter().all(|c| c["connection"]["target_id"] == compute["id"]));
    let graph = ok(&port, "stream.connection.graph", json!({})).await;
    assert!(graph["contexts"].as_array().unwrap().iter().any(|node| node["context"]["name"] == "Portable compute"
        && node["signals"].as_array().unwrap().iter().any(|s| s["id"] == signal_id.as_str())));

    // A second related URL corroborates rather than adding noise.
    let second = ok(&port, "stream.source.add", json!({ "url": server.url("/elsewhere/container-vms") })).await;
    assert_eq!(second["report"]["primary_signal_id"], signal_id.as_str());
    let detail = ok(&port, "stream.signal.get", json!({ "id": signal_id })).await;
    assert_eq!(detail["summary"]["source_count"], 2);

    // Resolve leaves Today but keeps the signal.
    ok(&port, "stream.signal.resolve", json!({ "id": signal_id })).await;
    let today = ok(&port, "stream.signal.list", json!({})).await;
    assert!(today.as_array().unwrap().iter().all(|s| s["signal"]["id"] != signal_id.as_str()));
    let everything = ok(&port, "stream.signal.list", json!({ "include_resolved": true })).await;
    assert!(everything.as_array().unwrap().iter().any(|s| s["signal"]["id"] == signal_id.as_str()));
}

#[tokio::test]
async fn errors_are_typed_and_enveloped() {
    let isolated = Isolated::new();
    let port = StreamAppPort::new(runtime_at(&isolated));
    let unknown = port.invoke("stream.nope", json!({})).await.unwrap_err();
    assert_eq!(unknown.code, ErrorCode::UnknownCapability);
    let missing = port.invoke("stream.signal.get", json!({ "id": "signal_missing" })).await.unwrap_err();
    assert_eq!(missing.code, ErrorCode::NotFound);
    let invalid = port.invoke("stream.source.add", json!({ "url": "ftp://example.com/x" })).await.unwrap_err();
    assert_eq!(invalid.code, ErrorCode::InvalidInput);
    let malformed = port.invoke("stream.context.add", json!({ "description": "no name" })).await.unwrap_err();
    assert_eq!(malformed.code, ErrorCode::InvalidInput);

    let wrapped = envelope(port.invoke("appport.ping", Value::Null).await);
    assert_eq!(wrapped["ok"], true);
    let wrapped = envelope(Err(missing));
    assert_eq!(wrapped["ok"], false);
    assert_eq!(wrapped["error"]["code"], "not_found");
}

#[tokio::test]
async fn every_manifest_capability_is_invokable() {
    let isolated = Isolated::new();
    let port = StreamAppPort::new(runtime_at(&isolated));
    for capability in AppPortManifest::stream().capabilities {
        let input = if capability.operations == ["invoke"] || capability.name.starts_with("appport.") {
            json!({})
        } else {
            json!({ "operation": capability.operations.iter().find(|op| ["list", "summary", "search"].contains(&op.as_str())).cloned().unwrap_or_else(|| capability.operations[0].clone()) })
        };
        if let Err(error) = port.invoke(&capability.name, input).await {
            assert_ne!(error.code, ErrorCode::UnknownCapability, "{} is advertised but not invokable", capability.name);
        }
    }
}

#[tokio::test]
async fn chat_reasoning_and_insights_work_through_appport_without_exposing_the_provider() {
    let server = web();
    let model = stream_testkit::FakeModelServer::start(stream_testkit::ModelMode::Cooperative);
    let isolated = Isolated::new();
    let port = StreamAppPort::new(stream_testkit::model_runtime_at(&isolated, &model));
    ok(&port, "stream.context.add", json!({ "name": "Portable compute", "description": "Running workloads anywhere" })).await;
    ok(&port, "stream.source.add", json!({ "url": server.url("/news/apple-container") })).await;

    let bundle = ok(&port, "stream.reason.retrieve", json!({ "question": "What connects to portable compute?" })).await;
    assert!(!bundle["signals"].as_array().unwrap().is_empty());
    assert!(!bundle["evidence"].as_array().unwrap().is_empty());

    let answer = ok(&port, "stream.chat.ask", json!({ "question": "What connects to portable compute?" })).await;
    let statements = answer["statements"].as_array().unwrap();
    assert!(!statements.is_empty());
    for statement in statements {
        if statement["basis"] != "hypothesis" {
            assert!(!statement["evidence_ids"].as_array().unwrap().is_empty(), "{statement}");
        }
    }
    assert!(!answer["evidence"].as_array().unwrap().is_empty(), "answers carry provenance");
    assert!(!answer["sources"].as_array().unwrap().is_empty());

    let evidence_id = answer["evidence"][0]["evidence"]["id"].clone();
    let insight = ok(
        &port,
        "stream.insight.save",
        json!({ "kind": "insight", "statement": "Per-container VMs overlap with portable compute.", "question": "What connects to portable compute?", "evidence_ids": [evidence_id] }),
    )
    .await;
    assert_eq!(insight["basis"], "inferred");
    let listed = ok(&port, "stream.insight.list", json!({})).await;
    assert_eq!(listed.as_array().unwrap().len(), 1);
    let fetched = ok(&port, "stream.insight.get", json!({ "id": insight["id"] })).await;
    assert!(!fetched["evidence"].as_array().unwrap().is_empty());
    ok(&port, "stream.insight.resolve", json!({ "id": insight["id"] })).await;

    let bad = port.invoke("stream.insight.save", json!({ "kind": "insight", "statement": "Unsupported.", "evidence_ids": [] })).await.unwrap_err();
    assert_eq!(bad.code, ErrorCode::InvalidInput);
    let empty = port.invoke("stream.chat.ask", json!({ "question": "  " })).await.unwrap_err();
    assert_eq!(empty.code, ErrorCode::InvalidInput);

    let status = ok(&port, "stream.intelligence.status", json!({})).await;
    assert_eq!(status["model_backed"], true);
    for payload in [answer.to_string(), bundle.to_string(), status.to_string(), fetched.to_string()] {
        assert!(!payload.contains("fake-model"), "AppPort exposes Stream's capability, not the provider");
    }
}
