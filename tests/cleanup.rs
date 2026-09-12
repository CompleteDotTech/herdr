//! Exercise real runtime admission and the JSON API with isolated configuration.
#![cfg(target_os = "linux")]

use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Runtime {
    base: PathBuf,
    socket: PathBuf,
    child: Child,
}
impl Runtime {
    fn start() -> Self {
        let temp_base = std::env::var_os("HERDR_TEST_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let base = temp_base.join(format!(
            "hc-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = base.join("config/herdr-dev");
        fs::create_dir_all(config.join("cleanup")).unwrap();
        fs::write(
            config.join("config.toml"),
            "onboarding = false\n[update]\nversion_check = false\nmanifest_check = false\n",
        )
        .unwrap();
        fs::write(
            config.join("cleanup/settings.json"),
            r#"{"policy":{"enabled":false},"repositories":[]}"#,
        )
        .unwrap();
        fs::create_dir_all(base.join("runtime")).unwrap();
        let socket = config.join("sessions/cleanup/herdr.sock");
        let log = fs::File::create(base.join("server.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["--session", "cleanup", "server"])
            .current_dir(&base)
            .env("XDG_CONFIG_HOME", base.join("config"))
            .env("XDG_STATE_HOME", base.join("state"))
            .env("XDG_RUNTIME_DIR", base.join("runtime"))
            .env("SHELL", "/bin/sh")
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_CLIENT_SOCKET_PATH")
            .env_remove("HERDR_ENV")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut runtime = Self {
            base,
            socket,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while !runtime.socket.exists() {
            assert!(
                runtime.child.try_wait().unwrap().is_none(),
                "server exited: {}",
                fs::read_to_string(runtime.base.join("server.log")).unwrap()
            );
            assert!(Instant::now() < deadline, "server socket unavailable");
            std::thread::sleep(Duration::from_millis(25));
        }
        runtime
    }
    fn request(&self, method: &str, params: serde_json::Value) -> serde_json::Value {
        let mut stream = UnixStream::connect(&self.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        writeln!(
            stream,
            "{}",
            serde_json::json!({"id":"test", "method":method,"params":params})
        )
        .unwrap();
        let mut response = String::new();
        BufReader::new(stream).read_line(&mut response).unwrap();
        serde_json::from_str(&response).unwrap()
    }
    fn cleanup(&self, params: serde_json::Value) -> serde_json::Value {
        let receipt = self.request("worktree.cleanup", params);
        assert!(receipt.get("error").is_none(), "{receipt}");
        let id = receipt
            .pointer("/result/operation_id")
            .and_then(serde_json::Value::as_u64)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let response =
                self.request("worktree.cleanup", serde_json::json!({"action":"inspect"}));
            if let Some(operation) = response
                .pointer("/result/snapshot/operations")
                .and_then(serde_json::Value::as_array)
                .and_then(|ops| {
                    ops.iter()
                        .find(|op| op["id"] == id && op["state"] != "running")
                })
            {
                assert_eq!(operation["state"], "completed", "{response}");
                return response;
            }
            assert!(
                Instant::now() < deadline,
                "cleanup operation did not settle: {response}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.base);
    }
}
fn git(path: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cleanup_runtime_admission_and_session_release_use_programmatic_service() {
    let runtime = Runtime::start();
    git(&runtime.base, &["init", "--bare", "remote.git"]);
    git(&runtime.base, &["init", "--initial-branch=main", "repo"]);
    let repo = runtime.base.join("repo");
    git(&repo, &["config", "user.name", "Cleanup Test"]);
    git(&repo, &["config", "user.email", "cleanup@example.invalid"]);
    git(&repo, &["commit", "--allow-empty", "-m", "base"]);
    git(
        &repo,
        &[
            "remote",
            "add",
            "origin",
            runtime.base.join("remote.git").to_str().unwrap(),
        ],
    );
    git(&repo, &["push", "origin", "main"]);
    git(
        &runtime.base.join("remote.git"),
        &["symbolic-ref", "HEAD", "refs/heads/main"],
    );
    let worktree = runtime.base.join("merged");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "merged",
            worktree.to_str().unwrap(),
            "main",
        ],
    );
    runtime.cleanup(serde_json::json!({"action":"register", "repository":repo}));

    // This lock uses the same namespace as the production admission code.
    let locks = runtime
        .base
        .join("config/herdr-worktree-ownership-v1/leases");
    fs::create_dir_all(&locks).unwrap();
    let key = format!(
        "{:x}",
        Sha256::digest(worktree.as_os_str().as_encoded_bytes())
    );
    let lease = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(locks.join(key))
        .unwrap();
    lease.try_lock().unwrap();
    let rejected = runtime.request(
        "workspace.create",
        serde_json::json!({"cwd":worktree,"focus":false}),
    );
    assert!(
        rejected.get("error").is_some(),
        "launch bypassed cleanup lease: {rejected}"
    );
    lease.unlock().unwrap();
    drop(lease);

    let workspace = runtime.request(
        "workspace.create",
        serde_json::json!({"cwd":worktree,"focus":false}),
    );
    assert!(workspace.get("error").is_none(), "{workspace}");
    let workspace_id = workspace
        .pointer("/result/workspace/workspace_id")
        .and_then(serde_json::Value::as_str)
        .unwrap();
    runtime.cleanup(serde_json::json!({"action":"configure", "policy":{"enabled":true,"github_merge_evidence":false}}));
    let owned = runtime.cleanup(serde_json::json!({"action":"reconcile","repository":repo}));
    assert!(worktree.exists(), "active worktree was removed: {owned}");
    let closed = runtime.request(
        "workspace.close",
        serde_json::json!({"workspace_id":workspace_id,"force":true}),
    );
    assert!(closed.get("error").is_none(), "{closed}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while worktree.exists() {
        let result = runtime.cleanup(serde_json::json!({"action":"reconcile","repository":repo}));
        assert!(
            Instant::now() < deadline,
            "released worktree remained: {result}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(repo.exists());
    let refs = Command::new("git")
        .current_dir(repo)
        .args(["branch", "--format=%(refname:short)"])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8(refs.stdout).unwrap().trim(), "main");
}
