//! `stream-desktop` — launch Stream.
//!
//! By default opens a native window (system webview). `--serve` serves the
//! same UI on localhost instead, for platforms without a webview or for
//! driving the UI from a browser.

use anyhow::Result;
use stream_desktop::{serve, DesktopBridge};

const USAGE: &str = "Usage: stream-desktop [--serve [--port N] [--open]]

  (no flags)   open the Stream window
  --serve      serve the Stream UI on http://127.0.0.1 instead of opening a window
  --port N     port for --serve (default: any free port)
  --open       with --serve, open the UI in the default browser

Data lives in FeltDB under the Stream root (STREAM_ROOT, or the directory
containing feltdb.flow), shared with the `stream` CLI.";

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{USAGE}");
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let bridge = DesktopBridge::from_env();

    if args.iter().any(|arg| arg == "--serve") || !cfg!(feature = "native") {
        let port = args
            .windows(2)
            .find(|pair| pair[0] == "--port")
            .and_then(|pair| pair[1].parse().ok())
            .unwrap_or(0);
        let handle = serve::start(bridge, port, runtime.handle().clone())?;
        println!("Stream is running at {}", handle.url());
        if args.iter().any(|arg| arg == "--open") {
            open_in_browser(&handle.url());
        }
        loop {
            std::thread::park();
        }
    }

    #[cfg(feature = "native")]
    native::run(bridge, runtime)?;
    Ok(())
}

fn open_in_browser(url: &str) {
    #[cfg(feature = "native")]
    let _ = open::that(url);
    #[cfg(not(feature = "native"))]
    let _ = url;
}

#[cfg(feature = "native")]
mod native {
    use super::DesktopBridge;
    use anyhow::Result;
    use std::borrow::Cow;
    use stream_desktop::{asset, CONTENT_SECURITY_POLICY};
    use tao::dpi::LogicalSize;
    use tao::event::{Event, WindowEvent};
    use tao::event_loop::{ControlFlow, EventLoopBuilder};
    use tao::window::WindowBuilder;
    use wry::http::{header::CONTENT_TYPE, Request, Response};
    use wry::{NewWindowResponse, WebViewBuilder};

    #[cfg(any(target_os = "windows", target_os = "android"))]
    const APP_URL: &str = "http://stream.localhost/";
    #[cfg(not(any(target_os = "windows", target_os = "android")))]
    const APP_URL: &str = "stream://localhost/";

    enum UserEvent {
        Reply(String),
    }

    fn serve_asset(request: Request<Vec<u8>>) -> Response<Cow<'static, [u8]>> {
        match asset(request.uri().path()) {
            Some((content_type, body)) => Response::builder()
                .header(CONTENT_TYPE, content_type)
                .header("Content-Security-Policy", CONTENT_SECURITY_POLICY)
                .body(Cow::Borrowed(body))
                .expect("static response"),
            None => Response::builder()
                .status(404)
                .header(CONTENT_TYPE, "text/plain")
                .body(Cow::Borrowed(&b"not found"[..]))
                .expect("static response"),
        }
    }

    /// Links to the outside world open in the user's browser; the Stream
    /// window never navigates away from Stream.
    fn open_externally(url: &str) {
        if url.starts_with("http://") || url.starts_with("https://") {
            let _ = open::that(url);
        }
    }

    pub fn run(bridge: DesktopBridge, runtime: tokio::runtime::Runtime) -> Result<()> {
        let event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
        let proxy = event_loop.create_proxy();
        let window = WindowBuilder::new()
            .with_title("Stream")
            .with_inner_size(LogicalSize::new(1040.0, 820.0))
            .with_min_inner_size(LogicalSize::new(420.0, 480.0))
            .build(&event_loop)?;

        let handle = runtime.handle().clone();
        let builder = WebViewBuilder::new()
            .with_custom_protocol("stream".into(), |_id, request| serve_asset(request))
            .with_url(APP_URL)
            .with_devtools(cfg!(debug_assertions))
            .with_ipc_handler(move |request: Request<String>| {
                let body = request.into_body();
                let bridge = bridge.clone();
                let proxy = proxy.clone();
                handle.spawn(async move {
                    let reply = bridge.handle(&body).await;
                    let _ = proxy.send_event(UserEvent::Reply(reply));
                });
            })
            .with_navigation_handler(|url| {
                if url.starts_with(APP_URL) || url.starts_with("about:") {
                    true
                } else {
                    open_externally(&url);
                    false
                }
            })
            .with_new_window_req_handler(|url, _features| {
                open_externally(&url);
                NewWindowResponse::Deny
            });

        #[cfg(any(target_os = "windows", target_os = "macos", target_os = "ios", target_os = "android"))]
        let webview = builder.build(&window)?;
        #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "ios", target_os = "android")))]
        let webview = {
            use tao::platform::unix::WindowExtUnix;
            use wry::WebViewBuilderExtUnix;
            let vbox = window.default_vbox().ok_or_else(|| anyhow::anyhow!("window has no GTK container"))?;
            builder.build_gtk(vbox)?
        };

        event_loop.run(move |event, _, control_flow| {
            *control_flow = ControlFlow::Wait;
            // The Tokio runtime lives as long as the window.
            let _keep_alive = &runtime;
            match event {
                Event::UserEvent(UserEvent::Reply(reply)) => {
                    // `reply` is serialized JSON, which is a valid JS expression.
                    let _ = webview.evaluate_script(&format!("window.__streamReply({reply})"));
                }
                Event::WindowEvent { event: WindowEvent::CloseRequested, .. } => *control_flow = ControlFlow::Exit,
                _ => {}
            }
        });
    }
}
