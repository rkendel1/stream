use chrono::Utc;
use std::path::PathBuf;
use stream_core::{FeltDbConfig, FeltDbStore, StreamRuntime};
use stream_ingest::AdapterRegistry;
use stream_model::{ItemState, NormalizedItem, SourceKind};
use stream_rss::default_adapters;
use url::Url;
use uuid::Uuid;

fn runtime_with_namespace(path: PathBuf, namespace: String) -> StreamRuntime {
    StreamRuntime::new(
        FeltDbStore::new(FeltDbConfig {
            namespace,
            path,
            node_binary: "node".into(),
        }),
        AdapterRegistry::new(default_adapters()),
    )
}

#[tokio::test]
async fn durable_state_persists_across_runtime_instances() {
    let path = PathBuf::from(format!("/tmp/stream-core-{}", Uuid::new_v4()));
    let namespace = format!("stream-test-{}", Uuid::new_v4());
    let runtime = runtime_with_namespace(path.clone(), namespace.clone());
    let source = runtime
        .add_source(SourceKind::Rss, "https://example.com/feed.xml")
        .await
        .unwrap();

    let result = runtime
        .ingest_items_for_source(
            &source.id,
            vec![NormalizedItem {
                source_kind: SourceKind::Rss,
                canonical_identity: "https://example.com/posts/rust-release".into(),
                canonical_url: Some(Url::parse("https://example.com/posts/rust-release").unwrap()),
                title: "Rust release".into(),
                content_text: "Rust 1.99 shipped.".into(),
                content_html: None,
                author: Some("Example News".into()),
                published_at: Some(Utc::now()),
                original_identifier: "item-1".into(),
                source_url: Url::parse("https://example.com/posts/rust-release").unwrap(),
                parser: "rss".into(),
                transformations: vec!["rss-normalized".into()],
            }],
        )
        .await
        .unwrap();

    let item_id = result.new_item_ids.first().unwrap().clone();
    runtime.mark_read(&item_id).await.unwrap();

    let runtime_again = runtime_with_namespace(path, namespace);
    let items = runtime_again.list_items().await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].state.state, ItemState::Read);
}
