//! The Stream desktop shell: presentation only.
//!
//! The UI talks to Stream through [`DesktopBridge`], which forwards every
//! request to [`StreamAppPort::invoke`] — the same capability surface the CLI
//! and AppPort clients use. The bridge keeps no data of its own: no cache,
//! no local database, no browser storage. FeltDB stays the only authority.

use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use stream_appport::{envelope, AppPortError, ErrorCode, StreamAppPort};

pub mod serve;

/// A UI request: `{ "id": 1, "capability": "stream.signal.list", "input": {} }`.
#[derive(Debug, Deserialize)]
pub struct BridgeRequest {
    pub id: Value,
    pub capability: String,
    #[serde(default)]
    pub input: Value,
}

#[derive(Clone)]
pub struct DesktopBridge {
    port: Arc<StreamAppPort>,
}

impl DesktopBridge {
    pub fn new(port: StreamAppPort) -> Self {
        Self { port: Arc::new(port) }
    }

    /// A bridge over the user's Stream data (see `stream_appport::discover_root`).
    pub fn from_env() -> Self {
        Self::new(StreamAppPort::from_env().with_provenance("desktop"))
    }

    pub fn port(&self) -> &StreamAppPort {
        &self.port
    }

    /// Handle one serialized UI request and produce a serialized response.
    pub async fn handle(&self, raw: &str) -> String {
        let response = match serde_json::from_str::<BridgeRequest>(raw) {
            Ok(request) => {
                let mut response = envelope(self.dispatch(&request.capability, request.input).await);
                response["id"] = request.id;
                response
            }
            Err(error) => {
                let mut response = envelope(Err(AppPortError {
                    code: ErrorCode::InvalidInput,
                    message: format!("malformed request: {error}"),
                }));
                response["id"] = Value::Null;
                response
            }
        };
        response.to_string()
    }

    async fn dispatch(&self, capability: &str, input: Value) -> Result<Value, AppPortError> {
        if capability == "desktop.info" {
            return Ok(json!({
                "application": "Stream",
                "version": env!("CARGO_PKG_VERSION"),
            }));
        }
        self.port.invoke(capability, input).await
    }
}

/// UI assets compiled into the binary; the desktop app needs no web server
/// or network access to render.
pub fn asset(path: &str) -> Option<(&'static str, &'static [u8])> {
    let path = path.split(['?', '#']).next().unwrap_or(path);
    match path.trim_start_matches('/') {
        "" | "index.html" => Some(("text/html; charset=utf-8", include_bytes!("../ui/index.html"))),
        "app.js" => Some(("text/javascript; charset=utf-8", include_bytes!("../ui/app.js"))),
        "styles.css" => Some(("text/css; charset=utf-8", include_bytes!("../ui/styles.css"))),
        _ => None,
    }
}

/// Content-Security-Policy for the UI: everything local, nothing remote.
pub const CONTENT_SECURITY_POLICY: &str =
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'";
