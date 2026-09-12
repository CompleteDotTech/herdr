#![cfg(unix)]

use std::{
    fs,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use coven_client::{
    terminal_control::{
        TerminalControlAction, TerminalControlClient, TerminalControlClientError,
        TerminalControlIdentity, TerminalControlOutcome, TerminalControlReply,
        TerminalControlRequest, TerminalMutationStatus, MAX_TERMINAL_CONTROL_INPUT_BYTES,
    },
    ClientError, DaemonClient, DaemonEndpoint,
};

const HEALTH: &str = include_str!("../fixtures/health.json");
const STREAM: &str = "00000000-0000-0000-0000-000000000001";
const ATTACHMENT: &str = "00000000-0000-0000-0000-000000000002";

struct TestHome {
    path: PathBuf,
}

impl TestHome {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);

        let mut path = std::env::temp_dir();
        path.push(format!(
            "coven-terminal-control-{}-{}",
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

fn identity() -> TerminalControlIdentity {
    TerminalControlIdentity::new("engine/session?literal", STREAM, 3, 7, 11, ATTACHMENT)
        .expect("valid terminal control identity")
}

fn request() -> TerminalControlRequest {
    TerminalControlRequest::new(
        identity(),
        TerminalControlAction::input(9, 12, vec![1, 2, 3]),
    )
}

fn http_response(status: u16, reason: &str, body: &[u8]) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body.iter().copied())
    .collect()
}

fn read_request(stream: &mut std::os::unix::net::UnixStream) -> (String, Vec<u8>) {
    let mut request = Vec::new();
    stream
        .read_to_end(&mut request)
        .expect("read complete client request");
    let body_start = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP request header terminator")
        + 4;
    let header = std::str::from_utf8(&request[..body_start]).expect("UTF-8 request headers");
    let content_length = header
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .expect("Content-Length header")
        .parse::<usize>()
        .expect("numeric Content-Length header");
    assert_eq!(request.len() - body_start, content_length);
    (header.to_owned(), request[body_start..].to_vec())
}

fn serve_health_and_control<F>(home: &Path, make_reply: F) -> (JoinHandle<()>, Arc<AtomicUsize>)
where
    F: FnOnce(TerminalControlRequest) -> Vec<u8> + Send + 'static,
{
    let socket = home.join("coven.sock");
    let listener = UnixListener::bind(&socket).expect("bind test daemon socket");
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))
        .expect("make test daemon socket owner-only");
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_for_thread = Arc::clone(&accepted);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept health request");
        accepted_for_thread.fetch_add(1, Ordering::Relaxed);
        let (header, body) = read_request(&mut stream);
        assert!(header.starts_with("GET /api/v1/health HTTP/1.1\r\n"));
        assert!(body.is_empty());
        stream
            .write_all(&http_response(200, "OK", HEALTH.as_bytes()))
            .expect("write health response");

        let (mut stream, _) = listener.accept().expect("accept control request");
        accepted_for_thread.fetch_add(1, Ordering::Relaxed);
        let (header, body) = read_request(&mut stream);
        assert!(header.starts_with("POST /api/v1/terminal/control HTTP/1.1\r\n"));
        let request: TerminalControlRequest =
            serde_json::from_slice(&body).expect("decode terminal control request");
        let response = make_reply(request);
        stream
            .write_all(&http_response(200, "OK", &response))
            .expect("write control response");
    });
    (server, accepted)
}

#[test]
fn daemon_client_posts_one_bound_control_request_and_preserves_unknown() {
    let home = TestHome::new();
    let server = serve_health_and_control(&home.path, |request| {
        serde_json::to_vec(&TerminalControlReply {
            session_id: request.session_id.clone(),
            stream_id: request.stream_id.clone(),
            stream_generation: request.stream_generation,
            execution_generation: request.execution_generation,
            authority_epoch: request.authority_epoch,
            attachment_id: request.attachment_id.clone(),
            outcome: TerminalControlOutcome::Mutation {
                mutation_seq: 12,
                status: TerminalMutationStatus::Unknown,
                reason: None,
            },
        })
        .expect("encode control response")
    });
    let endpoint = DaemonEndpoint::discover(&home.path).expect("discover owner-local socket");
    let mut client = DaemonClient::new(endpoint);

    let reply = client
        .terminal_control(&request())
        .expect("unknown mutation receipt is a valid result");

    assert!(matches!(
        reply.outcome,
        TerminalControlOutcome::Mutation {
            mutation_seq: 12,
            status: TerminalMutationStatus::Unknown,
            ..
        }
    ));
    let (server, accepted) = server;
    server.join().expect("server thread");
    assert_eq!(accepted.load(Ordering::Relaxed), 2);
}

#[test]
fn daemon_client_rejects_a_control_reply_bound_to_another_stream() {
    let home = TestHome::new();
    let server = serve_health_and_control(&home.path, |request| {
        serde_json::to_vec(&TerminalControlReply {
            session_id: request.session_id.clone(),
            stream_id: request.stream_id.clone(),
            stream_generation: request.stream_generation + 1,
            execution_generation: request.execution_generation,
            authority_epoch: request.authority_epoch,
            attachment_id: request.attachment_id.clone(),
            outcome: TerminalControlOutcome::Mutation {
                mutation_seq: 12,
                status: TerminalMutationStatus::Completed,
                reason: None,
            },
        })
        .expect("encode mismatched control response")
    });
    let endpoint = DaemonEndpoint::discover(&home.path).expect("discover owner-local socket");
    let mut client = DaemonClient::new(endpoint);

    let error = client
        .terminal_control(&request())
        .expect_err("a replaced stream must not be accepted");
    assert!(matches!(
        error,
        TerminalControlClientError::InvalidResponse(_)
    ));
    let (server, accepted) = server;
    server.join().expect("server thread");
    assert_eq!(accepted.load(Ordering::Relaxed), 2);
}

#[test]
fn oversized_encoded_control_body_is_rejected_before_socket_io() {
    let home = TestHome::new();
    let socket = home.path.join("coven.sock");
    let listener = UnixListener::bind(&socket).expect("bind test daemon socket");
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))
        .expect("make test daemon socket owner-only");
    let endpoint = DaemonEndpoint::discover(&home.path).expect("discover owner-local socket");
    let mut client = DaemonClient::new(endpoint);
    let request = TerminalControlRequest::new(
        identity(),
        TerminalControlAction::input(9, 12, vec![0_u8; MAX_TERMINAL_CONTROL_INPUT_BYTES]),
    );

    let error = client
        .terminal_control(&request)
        .expect_err("encoded body must exceed the wire bound");
    assert!(matches!(
        error,
        TerminalControlClientError::RequestTooLarge { .. }
    ));

    listener
        .set_nonblocking(true)
        .expect("set test listener nonblocking");
    assert!(matches!(
        listener.accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}

#[test]
fn structured_control_errors_remain_transport_errors_without_retry() {
    let home = TestHome::new();
    let socket = home.path.join("coven.sock");
    let listener = UnixListener::bind(&socket).expect("bind test daemon socket");
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))
        .expect("make test daemon socket owner-only");
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_for_thread = Arc::clone(&accepted);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept health request");
        accepted_for_thread.fetch_add(1, Ordering::Relaxed);
        let (header, body) = read_request(&mut stream);
        assert!(header.starts_with("GET /api/v1/health HTTP/1.1\r\n"));
        assert!(body.is_empty());
        stream
            .write_all(&http_response(200, "OK", HEALTH.as_bytes()))
            .expect("write health response");

        let (mut stream, _) = listener.accept().expect("accept control request");
        accepted_for_thread.fetch_add(1, Ordering::Relaxed);
        let (header, body) = read_request(&mut stream);
        assert!(header.starts_with("POST /api/v1/terminal/control HTTP/1.1\r\n"));
        assert!(!body.is_empty());
        let error =
            br#"{"error":{"code":"terminal_authority_changed","message":"stale","details":null}}"#;
        stream
            .write_all(&http_response(409, "Conflict", error))
            .expect("write structured control error");
    });
    let endpoint = DaemonEndpoint::discover(&home.path).expect("discover owner-local socket");
    let mut client = DaemonClient::new(endpoint);

    let error = client
        .terminal_control(&request())
        .expect_err("daemon rejection must be returned");
    assert!(matches!(
        error,
        TerminalControlClientError::Transport(ClientError::Daemon { status: 409, error })
            if error.code == "terminal_authority_changed"
    ));
    server.join().expect("server thread");
    assert_eq!(accepted.load(Ordering::Relaxed), 2);
}
