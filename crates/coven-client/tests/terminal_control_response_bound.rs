#![cfg(unix)]

use std::{
    fs,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use coven_client::{
    terminal_control::{
        TerminalControlAction, TerminalControlClient, TerminalControlClientError,
        TerminalControlIdentity, TerminalControlRequest, MAX_TERMINAL_CONTROL_RESPONSE_BYTES,
    },
    ClientError, DaemonClient, DaemonEndpoint,
};

const HEALTH: &str = include_str!("../fixtures/health.json");

struct TestHome {
    path: PathBuf,
}

impl TestHome {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);

        let path = std::env::temp_dir().join(format!(
            "coven-terminal-control-response-bound-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create test Coven home");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("make test Coven home private");
        Self { path }
    }
}

impl Drop for TestHome {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn request() -> TerminalControlRequest {
    TerminalControlRequest::new(
        TerminalControlIdentity::new(
            "engine/session?literal",
            "00000000-0000-0000-0000-000000000001",
            3,
            7,
            11,
            "00000000-0000-0000-0000-000000000002",
        )
        .expect("valid terminal control identity"),
        TerminalControlAction::input(9, 12, vec![1, 2, 3]),
    )
}

fn read_request(stream: &mut std::os::unix::net::UnixStream) {
    let mut request = Vec::new();
    stream
        .read_to_end(&mut request)
        .expect("read complete client request");
    assert!(request.starts_with(b"GET /api/v1/health HTTP/1.1\r\n"));
}

fn response(status: u16, reason: &str, body: &[u8]) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body.iter().copied())
    .collect()
}

#[test]
fn terminal_control_uses_route_bound_before_typed_response_validation() {
    let home = TestHome::new();
    let socket = home.path.join("coven.sock");
    let listener = UnixListener::bind(&socket).expect("bind test daemon socket");
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))
        .expect("make test daemon socket owner-only");

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept health request");
        read_request(&mut stream);
        stream
            .write_all(&response(200, "OK", HEALTH.as_bytes()))
            .expect("write health response");

        let (mut stream, _) = listener.accept().expect("accept control request");
        let mut request = Vec::new();
        stream
            .read_to_end(&mut request)
            .expect("read complete control request");
        assert!(request.starts_with(b"POST /api/v1/terminal/control HTTP/1.1\r\n"));

        let body = vec![b'x'; MAX_TERMINAL_CONTROL_RESPONSE_BYTES + 1];
        let response = response(200, "OK", &body);
        stream
            .write_all(&response)
            .expect("write oversized response");
    });

    let endpoint = DaemonEndpoint::discover(&home.path).expect("discover owner-local socket");
    let mut client = DaemonClient::new(endpoint);
    let error = client
        .terminal_control(&request())
        .expect_err("response over the route cap must be rejected by transport framing");

    assert!(matches!(
        error,
        TerminalControlClientError::Transport(ClientError::ResponseTooLarge { max_bytes })
            if max_bytes == MAX_TERMINAL_CONTROL_RESPONSE_BYTES
    ));
    server.join().expect("server thread");
}
