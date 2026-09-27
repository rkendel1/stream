//! Serve the desktop UI on localhost, for platforms without a native webview
//! and for driving the UI in automated tests.
//!
//! Only 127.0.0.1 is bound. Every request must carry the per-launch token and
//! a loopback `Host` header, so other local web pages cannot reach Stream
//! (CSRF) and DNS rebinding cannot either.
//!
//! The server is deliberately minimal: one thread per connection, one request
//! per connection, the response written by the thread that read the request.

use crate::{asset, DesktopBridge, CONTENT_SECURITY_POLICY};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Largest IPC request body accepted.
const MAX_BODY: usize = 1024 * 1024;

pub struct ServeHandle {
    pub addr: SocketAddr,
    pub token: String,
    stopped: Arc<AtomicBool>,
}

impl ServeHandle {
    /// The URL to open, including the launch token.
    pub fn url(&self) -> String {
        format!("http://{}/?token={}", self.addr, self.token)
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        // Wake the accept loop so it can observe the flag.
        let _ = TcpStream::connect(self.addr);
    }
}

struct Request {
    method: String,
    target: String,
    host: Option<String>,
    token: Option<String>,
    body: Vec<u8>,
}

fn read_request(stream: &TcpStream) -> std::io::Result<Request> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or("/").to_owned();
    let (mut host, mut token, mut length) = (None, None, 0usize);
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header == "\r\n" || header == "\n" {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            let value = value.trim().to_owned();
            match name.trim().to_ascii_lowercase().as_str() {
                "host" => host = Some(value),
                "x-stream-token" => token = Some(value),
                "content-length" => length = value.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    if length > MAX_BODY {
        return Err(std::io::Error::other("request body too large"));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    Ok(Request { method, target, host, token, body })
}

fn respond(mut stream: TcpStream, status: u16, content_type: &str, body: &[u8]) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\n\
         X-Content-Type-Options: nosniff\r\nContent-Security-Policy: {CONTENT_SECURITY_POLICY}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes()).and_then(|_| stream.write_all(body)).and_then(|_| stream.flush());
    let _ = stream.shutdown(std::net::Shutdown::Write);
}

fn serve_connection(stream: TcpStream, bridge: &DesktopBridge, token: &str, allowed_hosts: &[String], runtime: &tokio::runtime::Handle) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let request = match read_request(&stream) {
        Ok(request) => request,
        Err(_) => return respond(stream, 400, "text/plain", b"bad request"),
    };
    if !request.host.as_deref().map(|host| allowed_hosts.iter().any(|h| h == host)).unwrap_or(false) {
        return respond(stream, 403, "text/plain", b"forbidden host");
    }
    let path = request.target.split('?').next().unwrap_or("/");
    match (request.method.as_str(), path) {
        ("POST", "/ipc") => {
            if request.token.as_deref() != Some(token) {
                return respond(stream, 403, "text/plain", b"missing or wrong token");
            }
            let body = String::from_utf8_lossy(&request.body).into_owned();
            let reply = runtime.block_on(bridge.handle(&body));
            respond(stream, 200, "application/json", reply.as_bytes());
        }
        ("GET", "/") => {
            let query_token = request
                .target
                .split_once('?')
                .and_then(|(_, query)| query.split('&').find_map(|pair| pair.strip_prefix("token=")));
            if query_token != Some(token) {
                return respond(stream, 403, "text/plain", b"open the URL printed by stream-desktop");
            }
            let (content_type, body) = asset("/").expect("index asset");
            respond(stream, 200, content_type, body);
        }
        ("GET", _) => match asset(path) {
            Some((content_type, body)) => respond(stream, 200, content_type, body),
            None => respond(stream, 404, "text/plain", b"not found"),
        },
        _ => respond(stream, 405, "text/plain", b"method not allowed"),
    }
}

/// Start serving on `127.0.0.1:port` (0 picks a free port). IPC requests run
/// on the given Tokio runtime.
pub fn start(bridge: DesktopBridge, port: u16, runtime: tokio::runtime::Handle) -> std::io::Result<ServeHandle> {
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let addr = listener.local_addr()?;
    let token = uuid::Uuid::new_v4().simple().to_string();
    let stopped = Arc::new(AtomicBool::new(false));
    let handle = ServeHandle { addr, token: token.clone(), stopped: stopped.clone() };
    let allowed_hosts = Arc::new(vec![format!("127.0.0.1:{}", addr.port()), format!("localhost:{}", addr.port())]);
    let token = Arc::new(token);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            if stopped.load(Ordering::SeqCst) {
                break;
            }
            let Ok(stream) = stream else { continue };
            let (bridge, token, hosts, runtime) = (bridge.clone(), token.clone(), allowed_hosts.clone(), runtime.clone());
            std::thread::spawn(move || serve_connection(stream, &bridge, &token, &hosts, &runtime));
        }
    });
    Ok(handle)
}
