//! Serve the desktop UI on localhost, for platforms without a native webview
//! and for driving the UI in automated tests.
//!
//! Only 127.0.0.1 is bound. Every request must carry the per-launch token and
//! a loopback `Host` header, so other local web pages cannot reach Stream
//! (CSRF) and DNS rebinding cannot either.

use crate::{asset, DesktopBridge, CONTENT_SECURITY_POLICY};
use std::io::Read;
use std::net::SocketAddr;
use tiny_http::{Header, Method, Request, Response, Server};

pub struct ServeHandle {
    pub addr: SocketAddr,
    pub token: String,
    server: std::sync::Arc<Server>,
}

impl ServeHandle {
    /// The URL to open, including the launch token.
    pub fn url(&self) -> String {
        format!("http://{}/?token={}", self.addr, self.token)
    }

    pub fn stop(&self) {
        self.server.unblock();
    }
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("static header")
}

fn header_value<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

fn respond(request: Request, status: u16, content_type: &str, body: Vec<u8>) {
    let response = Response::from_data(body)
        .with_status_code(status)
        .with_header(header("Content-Type", content_type))
        .with_header(header("Cache-Control", "no-store"))
        .with_header(header("X-Content-Type-Options", "nosniff"))
        .with_header(header("Content-Security-Policy", CONTENT_SECURITY_POLICY));
    let _ = request.respond(response);
}

/// Start serving on `127.0.0.1:port` (0 picks a free port). Requests are
/// handled on the given Tokio runtime.
pub fn start(bridge: DesktopBridge, port: u16, runtime: tokio::runtime::Handle) -> std::io::Result<ServeHandle> {
    let server = Server::http(("127.0.0.1", port)).map_err(|error| std::io::Error::other(error.to_string()))?;
    let addr = server
        .server_addr()
        .to_ip()
        .ok_or_else(|| std::io::Error::other("server has no IP address"))?;
    let token = uuid::Uuid::new_v4().simple().to_string();
    let server = std::sync::Arc::new(server);
    let handle = ServeHandle { addr, token: token.clone(), server: server.clone() };

    let allowed_hosts = [format!("127.0.0.1:{}", addr.port()), format!("localhost:{}", addr.port())];
    std::thread::spawn(move || {
        for mut request in server.incoming_requests() {
            let host_ok = header_value(&request, "Host").map(|host| allowed_hosts.iter().any(|h| h == host)).unwrap_or(false);
            if !host_ok {
                respond(request, 403, "text/plain", b"forbidden host".to_vec());
                continue;
            }
            let url = request.url().to_owned();
            let path = url.split('?').next().unwrap_or("/").to_owned();
            match (request.method(), path.as_str()) {
                (Method::Post, "/ipc") => {
                    if header_value(&request, "X-Stream-Token") != Some(token.as_str()) {
                        respond(request, 403, "text/plain", b"missing or wrong token".to_vec());
                        continue;
                    }
                    let mut body = String::new();
                    if request.as_reader().take(1024 * 1024).read_to_string(&mut body).is_err() {
                        respond(request, 400, "text/plain", b"unreadable body".to_vec());
                        continue;
                    }
                    let bridge = bridge.clone();
                    runtime.spawn(async move {
                        let reply = bridge.handle(&body).await;
                        respond(request, 200, "application/json", reply.into_bytes());
                    });
                }
                (Method::Get, "/") => {
                    let query_token = url
                        .split_once('?')
                        .and_then(|(_, query)| query.split('&').find_map(|pair| pair.strip_prefix("token=")));
                    if query_token != Some(token.as_str()) {
                        respond(request, 403, "text/plain", b"open the URL printed by stream-desktop".to_vec());
                        continue;
                    }
                    let (content_type, body) = asset("/").expect("index asset");
                    respond(request, 200, content_type, body.to_vec());
                }
                (Method::Get, _) => match asset(&path) {
                    Some((content_type, body)) => respond(request, 200, content_type, body.to_vec()),
                    None => respond(request, 404, "text/plain", b"not found".to_vec()),
                },
                _ => respond(request, 405, "text/plain", b"method not allowed".to_vec()),
            }
        }
    });
    Ok(handle)
}
