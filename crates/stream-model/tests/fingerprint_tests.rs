use chrono::{TimeZone, Utc};
use stream_model::{CanonicalFingerprintParts, Fingerprint};
use url::Url;

#[test]
fn fingerprint_is_stable_for_equivalent_content() {
    let left = CanonicalFingerprintParts {
        canonical_identity: "https://example.com/posts/rust-release".into(),
        canonical_url: Some(Url::parse("https://example.com/posts/rust-release").unwrap()),
        title: "Rust   release".into(),
        content_text: "A new\nrelease is available.".into(),
        author: Some("Example News".into()),
        published_at: Some(Utc.with_ymd_and_hms(2026, 9, 27, 0, 0, 0).unwrap()),
    };
    let right = CanonicalFingerprintParts {
        canonical_identity: "https://example.com/posts/rust-release".into(),
        canonical_url: Some(Url::parse("https://example.com/posts/rust-release").unwrap()),
        title: "Rust release".into(),
        content_text: "A new release is available.".into(),
        author: Some("Example News".into()),
        published_at: Some(Utc.with_ymd_and_hms(2026, 9, 27, 0, 0, 0).unwrap()),
    };

    assert_eq!(
        Fingerprint::from_canonical_parts(&left).as_str(),
        Fingerprint::from_canonical_parts(&right).as_str()
    );
}
