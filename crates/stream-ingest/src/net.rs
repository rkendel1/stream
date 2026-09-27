//! Stream's network boundary. Every fetch Stream makes — discovery and
//! observation alike — goes through [`HttpFetcher`] and is checked against a
//! [`NetworkPolicy`]. Stream observes the web on the user's behalf; it must
//! never become a way to reach things the user did not mean it to reach.

use chrono::Utc;
use reqwest::{redirect, Client};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use url::{Host, Url};

use crate::FetchedDocument;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicy {
    /// Allow loopback, private, link-local, and other non-public addresses.
    /// Off by default; local development and tests turn it on explicitly.
    pub allow_private_network: bool,
    pub max_redirects: usize,
    pub max_bytes: usize,
    pub max_concurrency: usize,
    pub timeout: Duration,
}

impl Default for NetworkPolicy {
    fn default() -> Self {
        Self {
            allow_private_network: false,
            max_redirects: 5,
            max_bytes: 8 * 1024 * 1024,
            max_concurrency: 4,
            timeout: Duration::from_secs(30),
        }
    }
}

impl NetworkPolicy {
    /// `STREAM_ALLOW_PRIVATE_NETWORK=1` allows private/loopback targets.
    pub fn from_env() -> Self {
        let allow = std::env::var("STREAM_ALLOW_PRIVATE_NETWORK")
            .map(|value| matches!(value.trim(), "1" | "true" | "yes"))
            .unwrap_or(false);
        Self { allow_private_network: allow, ..Self::default() }
    }

    pub fn allowing_private_network(mut self) -> Self {
        self.allow_private_network = true;
        self
    }
}

/// Why a fetch did not produce a document. Durable failure state is built
/// from these categories.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    #[error("blocked by network policy: {0}")]
    Policy(String),
    #[error("could not resolve host: {0}")]
    Dns(String),
    #[error("timed out")]
    Timeout,
    #[error("HTTP {status}")]
    Http { status: u16, retry_after: Option<u64> },
    #[error("response exceeds {0} bytes")]
    TooLarge(usize),
    #[error("too many redirects")]
    Redirects,
    #[error("network error: {0}")]
    Network(String),
}

impl FetchError {
    /// Temporary failures are retried with backoff; permanent ones are
    /// retried too, but slowly — sources are never silently removed.
    pub fn is_temporary(&self) -> bool {
        match self {
            FetchError::Timeout | FetchError::Dns(_) | FetchError::Network(_) => true,
            FetchError::Http { status, .. } => *status == 429 || *status >= 500,
            _ => false,
        }
    }

    /// Seconds the server asked us to wait (HTTP 429/503 Retry-After).
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            FetchError::Http { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            FetchError::Http { status, .. } => Some(*status),
            _ => None,
        }
    }
}

fn private_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_documentation()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 198 && (b == 18 || b == 19))
        || a >= 240
}

fn private_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return private_v4(v4);
    }
    let first = ip.segments()[0];
    ip.is_loopback() || ip.is_unspecified() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
}

/// Is this address outside the public internet?
pub fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => private_v4(v4),
        IpAddr::V6(v6) => private_v6(v6),
    }
}

/// Static checks on a URL: scheme, credentials, and literal hosts.
pub fn check_url(url: &Url, policy: &NetworkPolicy) -> Result<(), FetchError> {
    match url.scheme() {
        "http" | "https" => {}
        other => return Err(FetchError::Policy(format!("unsupported scheme '{other}'"))),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(FetchError::Policy("URLs with embedded credentials are not fetched".into()));
    }
    let host = url.host().ok_or_else(|| FetchError::Policy("URL has no host".into()))?;
    if policy.allow_private_network {
        return Ok(());
    }
    let private = match host {
        Host::Ipv4(ip) => private_v4(ip),
        Host::Ipv6(ip) => private_v6(ip),
        Host::Domain(domain) => {
            let domain = domain.trim_end_matches('.').to_ascii_lowercase();
            domain == "localhost" || domain.ends_with(".localhost") || domain.ends_with(".local") || domain.ends_with(".internal")
        }
    };
    if private {
        return Err(FetchError::Policy(format!("{} is not a public address", url.host_str().unwrap_or_default())));
    }
    Ok(())
}

/// Resolve the host and refuse it if any address is private. This guards
/// against public names that point at private networks.
pub async fn check_resolved(url: &Url, policy: &NetworkPolicy) -> Result<(), FetchError> {
    check_url(url, policy)?;
    if policy.allow_private_network {
        return Ok(());
    }
    if let Some(Host::Domain(domain)) = url.host() {
        let port = url.port_or_known_default().unwrap_or(443);
        let addresses = tokio::net::lookup_host((domain, port))
            .await
            .map_err(|error| FetchError::Dns(format!("{domain}: {error}")))?
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(FetchError::Dns(domain.to_owned()));
        }
        if let Some(address) = addresses.iter().find(|address| is_private_ip(address.ip())) {
            return Err(FetchError::Policy(format!("{domain} resolves to non-public address {}", address.ip())));
        }
    }
    Ok(())
}

/// The only HTTP client Stream uses.
#[derive(Debug, Clone)]
pub struct HttpFetcher {
    client: Client,
    policy: Arc<NetworkPolicy>,
    permits: Arc<Semaphore>,
}

impl Default for HttpFetcher {
    fn default() -> Self {
        Self::new(NetworkPolicy::from_env())
    }
}

impl HttpFetcher {
    pub fn new(policy: NetworkPolicy) -> Self {
        let redirect_policy = {
            let policy = policy.clone();
            redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= policy.max_redirects {
                    return attempt.error(FetchError::Redirects);
                }
                match check_url(attempt.url(), &policy) {
                    Ok(()) => attempt.follow(),
                    Err(error) => attempt.error(error),
                }
            })
        };
        Self {
            client: Client::builder()
                .user_agent(concat!("stream-runtime/", env!("CARGO_PKG_VERSION"), " (+observation; bounded)"))
                .timeout(policy.timeout)
                .connect_timeout(Duration::from_secs(10))
                .redirect(redirect_policy)
                .build()
                .expect("reqwest client should build"),
            permits: Arc::new(Semaphore::new(policy.max_concurrency.max(1))),
            policy: Arc::new(policy),
        }
    }

    pub fn policy(&self) -> &NetworkPolicy {
        &self.policy
    }

    fn classify(error: reqwest::Error) -> FetchError {
        if error.is_timeout() {
            return FetchError::Timeout;
        }
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(current) = source {
            if let Some(fetch) = current.downcast_ref::<FetchError>() {
                return fetch.clone();
            }
            source = current.source();
        }
        let text = error.to_string();
        if error.is_connect() && (text.contains("dns") || text.contains("resolve")) {
            FetchError::Dns(text)
        } else {
            FetchError::Network(text)
        }
    }

    /// Fetch a document under the network policy, with bounded concurrency
    /// and a bounded body size.
    pub async fn fetch_document(&self, url: &Url) -> Result<FetchedDocument, FetchError> {
        check_resolved(url, &self.policy).await?;
        let _permit = self.permits.acquire().await.map_err(|_| FetchError::Network("fetcher closed".into()))?;
        let mut response = self.client.get(url.clone()).send().await.map_err(Self::classify)?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok());
            return Err(FetchError::Http { status: status.as_u16(), retry_after });
        }
        let final_url = response.url().clone();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_ascii_lowercase());
        if let Some(length) = response.content_length() {
            if length as usize > self.policy.max_bytes {
                return Err(FetchError::TooLarge(self.policy.max_bytes));
            }
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(Self::classify)? {
            if body.len() + chunk.len() > self.policy.max_bytes {
                return Err(FetchError::TooLarge(self.policy.max_bytes));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(FetchedDocument { requested_url: url.clone(), final_url, content_type, body, fetched_at: Utc::now() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deny() -> NetworkPolicy {
        NetworkPolicy::default()
    }

    #[test]
    fn rejects_unsupported_schemes_and_credentials() {
        for bad in ["file:///etc/passwd", "ftp://example.com/x", "gopher://example.com", "https://user:pass@example.com/"] {
            assert!(check_url(&Url::parse(bad).unwrap(), &deny()).is_err(), "{bad}");
        }
        assert!(check_url(&Url::parse("https://example.com/feed.xml").unwrap(), &deny()).is_ok());
    }

    #[test]
    fn rejects_private_and_local_targets_unless_allowed() {
        for bad in [
            "http://127.0.0.1:8080/", "http://localhost/", "http://10.0.0.5/", "http://192.168.1.1/", "http://169.254.169.254/latest/meta-data",
            "http://[::1]/", "http://[fd00::1]/", "http://printer.local/", "http://0.0.0.0/", "http://100.64.0.1/", "http://[::ffff:127.0.0.1]/",
        ] {
            let url = Url::parse(bad).unwrap();
            assert!(check_url(&url, &deny()).is_err(), "{bad}");
            assert!(check_url(&url, &deny().allowing_private_network()).is_ok(), "{bad} allowed when opted in");
        }
        assert!(check_url(&Url::parse("http://93.184.216.34/").unwrap(), &deny()).is_ok());
    }

    #[test]
    fn classifies_temporary_failures() {
        assert!(FetchError::Http { status: 429, retry_after: Some(30) }.is_temporary());
        assert!(FetchError::Http { status: 503, retry_after: None }.is_temporary());
        assert!(!FetchError::Http { status: 404, retry_after: None }.is_temporary());
        assert!(FetchError::Timeout.is_temporary());
        assert!(!FetchError::Policy("x".into()).is_temporary());
    }
}
