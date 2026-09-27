//! Desktop = presentation. These tests pin that down.

use serde_json::{json, Value};
use stream_appport::StreamAppPort;
use stream_desktop::{asset, serve, DesktopBridge};
use stream_testkit::{runtime_at, FixtureServer, Isolated, APPLE_CONTAINER_ARTICLE, NEWS_FEED};

async fn ask(bridge: &DesktopBridge, id: u64, capability: &str, input: Value) -> Value {
    let raw = bridge
        .handle(&json!({ "id": id, "capability": capability, "input": input }).to_string())
        .await;
    serde_json::from_str(&raw).unwrap()
}

#[tokio::test]
async fn the_desktop_bridge_is_the_appport_surface_not_a_second_data_path() {
    let server = FixtureServer::start();
    let url = server.html("/news/apple-container", APPLE_CONTAINER_ARTICLE);
    server.route("/news/feed.xml", "application/rss+xml", NEWS_FEED);
    let isolated = Isolated::new();
    let bridge = DesktopBridge::new(StreamAppPort::new(runtime_at(&isolated)).with_provenance("desktop"));

    ask(&bridge, 1, "stream.context.add", json!({ "name": "Portable compute" })).await;
    // The UI's Add URL flow: durable source first, then observation.
    let added = ask(&bridge, 2, "stream.source.add", json!({ "url": url, "observe": false })).await;
    assert_eq!(added["id"], 2, "responses are correlated to requests");
    assert_eq!(added["ok"], true);
    assert_eq!(added["result"]["source"]["stage"], "queued");
    assert_eq!(added["result"]["source"]["provenance"], "desktop");
    let source_id = added["result"]["source"]["id"].clone();
    let observed = ask(&bridge, 3, "stream.source.observe", json!({ "id": source_id })).await;
    assert_eq!(observed["result"]["stage"], "observed");

    // Whatever the desktop shows is exactly what AppPort (and so the CLI) returns.
    let direct = StreamAppPort::new(runtime_at(&isolated));
    for capability in ["stream.source.list", "stream.context.list", "stream.connection.graph"] {
        let via_desktop = ask(&bridge, 4, capability, json!({})).await;
        let via_appport = direct.invoke(capability, json!({})).await.unwrap();
        assert_eq!(via_desktop["result"], via_appport, "{capability}");
    }
    let ids = |list: &Value| list.as_array().unwrap().iter().map(|s| s["signal"]["id"].clone()).collect::<Vec<_>>();
    let desktop_signals = ask(&bridge, 5, "stream.signal.list", json!({})).await;
    assert_eq!(ids(&desktop_signals["result"]), ids(&direct.invoke("stream.signal.list", json!({})).await.unwrap()));

    // A restarted desktop app (new bridge, new runtime) sees the same state.
    let restarted = DesktopBridge::new(StreamAppPort::new(runtime_at(&isolated)));
    let after = ask(&restarted, 6, "stream.signal.list", json!({})).await;
    assert_eq!(ids(&after["result"]), ids(&desktop_signals["result"]));
}

#[tokio::test]
async fn bridge_errors_are_enveloped() {
    let isolated = Isolated::new();
    let bridge = DesktopBridge::new(StreamAppPort::new(runtime_at(&isolated)));
    let reply: Value = serde_json::from_str(&bridge.handle("not json").await).unwrap();
    assert_eq!(reply["ok"], false);
    assert_eq!(reply["error"]["code"], "invalid_input");
    let reply = ask(&bridge, 9, "stream.signal.get", json!({ "id": "signal_nope" })).await;
    assert_eq!(reply["id"], 9);
    assert_eq!(reply["error"]["code"], "not_found");
}

#[test]
fn the_ui_keeps_no_state_of_its_own() {
    for path in ["/", "/app.js", "/styles.css"] {
        let (_, body) = asset(path).expect("embedded asset");
        let text = std::str::from_utf8(body).unwrap();
        for forbidden in ["localStorage", "sessionStorage", "indexedDB", "document.cookie", "innerHTML", "http://", "https://fonts"] {
            assert!(!text.contains(forbidden), "{path} must not use {forbidden}");
        }
    }
    assert!(asset("/../Cargo.toml").is_none());
}

#[test]
fn serve_mode_requires_the_launch_token_and_a_loopback_host() {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let isolated = Isolated::new();
    let bridge = runtime.block_on(async { DesktopBridge::new(StreamAppPort::new(runtime_at(&isolated))) });
    let handle = serve::start(bridge, 0, runtime.handle().clone()).unwrap();
    let base = format!("http://{}", handle.addr);

    runtime.block_on(async {
        let client = reqwest::Client::new();
        let request = json!({ "id": 1, "capability": "appport.ping", "input": {} }).to_string();

        let index = client.get(handle.url()).send().await.unwrap();
        assert_eq!(index.status(), 200);
        assert!(index.headers()["content-security-policy"].to_str().unwrap().contains("default-src 'self'"));
        assert_eq!(client.get(format!("{base}/")).send().await.unwrap().status(), 403, "index needs the token");
        assert_eq!(client.get(format!("{base}/app.js")).send().await.unwrap().status(), 200);

        let without_token = client.post(format!("{base}/ipc")).body(request.clone()).send().await.unwrap();
        assert_eq!(without_token.status(), 403);
        let wrong_host = client
            .post(format!("{base}/ipc"))
            .header("Host", "evil.example")
            .header("X-Stream-Token", &handle.token)
            .body(request.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(wrong_host.status(), 403, "DNS rebinding is refused");

        let ok: Value = client
            .post(format!("{base}/ipc"))
            .header("X-Stream-Token", &handle.token)
            .body(request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(ok["ok"], true);
        assert_eq!(ok["result"]["pong"], true);
    });
    handle.stop();
}
