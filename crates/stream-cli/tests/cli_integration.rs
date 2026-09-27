//! The CLI is the operational/debugging surface: the whole product loop is
//! testable without the desktop UI.

use clap::Parser;
use stream_appport::StreamAppPort;
use stream_cli::{run_with, Cli};
use stream_testkit::{runtime_at, FixtureServer, Isolated, APPLE_CONTAINER_ARTICLE, APPLE_CONTAINER_SECOND_REPORT, NEWS_FEED};

async fn stream(port: &StreamAppPort, args: &[&str]) -> String {
    let cli = Cli::try_parse_from(std::iter::once("stream").chain(args.iter().copied())).expect("valid arguments");
    run_with(cli, port).await.unwrap_or_else(|error| panic!("stream {args:?} failed: {error:#}"))
}

fn signal_id(text: &str) -> String {
    text.split_whitespace().find(|word| word.starts_with("signal_")).expect("a signal id").to_owned()
}

#[tokio::test]
async fn add_signals_signal_context_and_connections() {
    let server = FixtureServer::start();
    let article = server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    let second = server.html("/elsewhere/container-vms", APPLE_CONTAINER_SECOND_REPORT);
    let isolated = Isolated::new();
    let port = StreamAppPort::new(runtime_at(&isolated)).with_provenance("cli");

    // context add / list
    let attn = stream(&port, &["context", "add", "Attn", "--kind", "project"]).await;
    assert!(attn.contains("project\tAttn"), "{attn}");
    stream(&port, &["context", "add", "Portable compute", "-d", "Running workloads anywhere", "--related", "Attn"]).await;
    let contexts = stream(&port, &["context", "list"]).await;
    assert!(contexts.contains("Portable compute — Running workloads anywhere"), "{contexts}");
    assert!(contexts.contains("[related: Attn]"), "{contexts}");

    // add
    let added = stream(&port, &["add", &article]).await;
    assert!(added.contains("Stage    observed"), "{added}");
    assert!(added.contains("Compute & Infrastructure · Apple Container"), "{added}");
    assert!(added.contains("Adds portable Linux VMs"), "{added}");
    assert!(added.contains("Connected to: Portable compute · Attn (via)"), "{added}");
    let id = signal_id(&added);

    // signals
    // The article's discovered feed also carried an Attn release, which
    // connects to the Attn project and becomes its own signal.
    let signals = stream(&port, &["signals"]).await;
    assert!(signals.contains("Compute & Infrastructure · Apple Container"), "{signals}");
    assert!(signals.contains("Autonomous Software · Attn"), "{signals}");
    assert!(!signals.contains("office party"), "unconnected feed entries are not signals");
    let json: serde_json::Value = serde_json::from_str(&stream(&port, &["signals", "--json"]).await).unwrap();
    assert!(json.as_array().unwrap().iter().any(|s| s["signal"]["id"] == id.as_str()));

    // signal
    let detail = stream(&port, &["signal", &id]).await;
    for expected in ["Topic    Compute & Infrastructure", "Subject  Apple Container", "Change   Adds portable Linux VMs", "Why it matters", "Why is this here?  #", "Relationship to your context", "change, subject, topic, statement] “", "Claims (observed", "  observed    Apple Container: Adds portable Linux VMs.", "source  source_"] {
        assert!(detail.contains(expected), "missing {expected:?} in\n{detail}");
    }

    // A second URL about the same change strengthens the same signal.
    let again = stream(&port, &["add", &second]).await;
    assert_eq!(signal_id(&again), id, "{again}");
    assert!(stream(&port, &["signals"]).await.contains("2 sources"));

    // connections
    let by_signal = stream(&port, &["connections", &id]).await;
    assert!(by_signal.contains("matches→ context Portable compute"), "{by_signal}");
    assert!(by_signal.contains("same_change→ item"), "{by_signal}");
    let graph = stream(&port, &["connections"]).await;
    assert!(graph.contains("Portable compute (interest)\n  ├── Attn (related)\n  ├── Apple Container: Adds portable Linux VMs"), "{graph}");
    assert!(graph.contains("Apple Container — 2 observations"), "{graph}");

    // sources
    let sources = stream(&port, &["sources"]).await;
    assert_eq!(sources.lines().count(), 2, "{sources}");
    assert!(sources.lines().all(|line| line.contains("\tobserved\t")), "{sources}");

    // AppPort from the CLI shows the same state.
    let invoked: serde_json::Value =
        serde_json::from_str(&stream(&port, &["appport", "invoke", "stream.signal.get", &format!("{{\"id\":\"{id}\"}}")]).await)
            .unwrap();
    assert_eq!(invoked["ok"], true);
    assert_eq!(invoked["result"]["summary"]["signal"]["subject"]["label"], "Apple Container");

    // resolve
    assert!(stream(&port, &["signal", &id, "--resolve"]).await.ends_with("resolved"));
    assert!(!stream(&port, &["signals"]).await.contains(&id));
    assert!(stream(&port, &["signals", "--all"]).await.contains(&id));
}

#[tokio::test]
async fn failed_urls_stay_visible() {
    let server = FixtureServer::start();
    let broken = server.failing("/down", 503);
    let isolated = Isolated::new();
    let port = StreamAppPort::new(runtime_at(&isolated));
    let output = stream(&port, &["add", &broken]).await;
    assert!(output.contains("Stage    failed"), "{output}");
    assert!(output.contains("stream source observe"), "{output}");
    assert!(stream(&port, &["sources"]).await.contains("\tfailed\t"));
}

#[tokio::test]
async fn ask_insights_and_doctor() {
    let server = FixtureServer::start();
    let article = server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    let isolated = Isolated::new();
    let port = StreamAppPort::new(runtime_at(&isolated));
    stream(&port, &["context", "add", "Portable compute", "-d", "Running workloads anywhere"]).await;
    stream(&port, &["add", &article]).await;

    let answer = stream(&port, &["ask", "Why does this matter to portable compute?", "--save", "insight"]).await;
    for expected in ["observed", "connected", "inferred", "[1]", "Evidence\n  [1] “", "Connected to: Portable compute", "Saved insight insight_"] {
        assert!(answer.contains(expected), "missing {expected:?} in\n{answer}");
    }
    let id = answer.split_whitespace().find(|w| w.starts_with("insight_")).unwrap().to_owned();
    assert!(stream(&port, &["insights"]).await.contains(&id));
    let detail = stream(&port, &["insight", &id]).await;
    assert!(detail.contains("From the question: Why does this matter to portable compute?"), "{detail}");
    assert!(detail.contains("Evidence “"), "{detail}");
    assert!(stream(&port, &["insight", &id, "--resolve"]).await.ends_with("resolved"));

    let unknown = stream(&port, &["ask", "What do we know about quantum biology?"]).await;
    assert!(unknown.contains("don't have enough evidence") && unknown.contains("(Insufficient evidence.)"), "{unknown}");

    let doctor = stream(&port, &["doctor"]).await;
    assert!(doctor.contains("- intelligence: local"), "{doctor}");
}
