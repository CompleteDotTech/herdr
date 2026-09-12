use crate::api::schema::{
    WorktreeCreateParams, WorktreeListParams, WorktreeOpenParams, WorktreeRemoveParams,
};

// Worktree output is always JSON. The parsers retain `--json` as a hidden compatibility no-op.
pub(super) fn run_worktree_command(args: &[String]) -> std::io::Result<i32> {
    let Some(subcommand) = args.first().map(|arg| arg.as_str()) else {
        print_worktree_help();
        return Ok(2);
    };

    match subcommand {
        "cleanup" => cleanup(&args[1..]),
        "list" => worktree_list(&args[1..]),
        "create" => worktree_create(&args[1..]),
        "open" => worktree_open(&args[1..]),
        "remove" => worktree_remove(&args[1..]),
        "help" | "--help" | "-h" => {
            print_worktree_help();
            Ok(0)
        }
        _ => {
            print_worktree_help();
            Ok(2)
        }
    }
}

fn cleanup(args: &[String]) -> std::io::Result<i32> {
    use crate::cleanup::service::Action;
    let command = args.first().map(String::as_str).unwrap_or("inspect");
    let action = match (command, args.get(1)) {
        ("inspect", None) => Action::Inspect,
        ("register", Some(path)) if args.len() == 2 => Action::Register {
            repository: std::path::PathBuf::from(normalize_path_arg(path)?),
        },
        ("preview", path) if args.len() <= 2 => Action::Preview {
            repository: path
                .map(|p| normalize_path_arg(p).map(Into::into))
                .transpose()?,
        },
        ("run", path) if args.len() <= 2 => Action::Reconcile {
            repository: path
                .map(|p| normalize_path_arg(p).map(Into::into))
                .transpose()?,
        },
        ("configure", Some(file)) if args.len() <= 3 => {
            let policy = serde_json::from_slice(&std::fs::read(file)?)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            Action::Configure {
                repository: args
                    .get(2)
                    .map(|p| normalize_path_arg(p).map(Into::into))
                    .transpose()?,
                policy,
            }
        }
        _ => {
            eprintln!("usage: herdr worktree cleanup inspect | register PATH | preview [PATH] | run [PATH] | configure POLICY.json [PATH]");
            return Ok(2);
        }
    };
    let response = super::send_request(&crate::api::schema::Request {
        id: "cli:worktree:cleanup".into(),
        method: crate::api::schema::Method::WorktreeCleanup(action),
    })?;
    let Some(operation_id) = response
        .pointer("/result/operation_id")
        .and_then(serde_json::Value::as_u64)
    else {
        return super::print_response(&response);
    };
    let boot = response.pointer("/result/snapshot/boot_id").cloned();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        let status = super::send_request(&crate::api::schema::Request {
            id: "cli:worktree:cleanup:inspect".into(),
            method: crate::api::schema::Method::WorktreeCleanup(Action::Inspect),
        })?;
        if status.get("error").is_some() {
            return super::print_response(&status);
        }
        if status.pointer("/result/snapshot/boot_id").cloned() != boot {
            return Err(std::io::Error::other(
                "cleanup runtime restarted; inspect persisted results before retrying",
            ));
        }
        let operation = status
            .pointer("/result/snapshot/operations")
            .and_then(serde_json::Value::as_array)
            .and_then(|ops| {
                ops.iter().find(|op| {
                    op.get("id").and_then(serde_json::Value::as_u64) == Some(operation_id)
                })
            });
        if let Some(operation) = operation {
            let state = operation.get("state").and_then(serde_json::Value::as_str);
            if state == Some("failed") {
                eprintln!("{operation}");
                return Ok(1);
            }
            if state == Some("completed") {
                return super::print_response(&status);
            }
        }
        if std::time::Instant::now() >= deadline {
            eprintln!("cleanup operation {operation_id} remains pending; use herdr worktree cleanup inspect");
            return Ok(2);
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

fn worktree_list(args: &[String]) -> std::io::Result<i32> {
    let mut workspace_id = None;
    let mut cwd = None;
    let mut trust_repository = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --workspace");
                    return Ok(2);
                };
                workspace_id = Some(super::normalize_workspace_id(value));
                index += 2;
            }
            "--cwd" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --cwd");
                    return Ok(2);
                };
                cwd = Some(normalize_path_arg(value)?);
                index += 2;
            }
            "--trust-repository" => {
                trust_repository = true;
                index += 1;
            }
            "--json" => index += 1,
            other => {
                eprintln!("unknown option: {other}");
                return Ok(2);
            }
        }
    }
    if workspace_id.is_some() && cwd.is_some() {
        eprintln!("usage: herdr worktree list [--workspace ID | --cwd PATH] [--trust-repository]");
        return Ok(2);
    }

    super::runtime::worktree_list(WorktreeListParams {
        workspace_id,
        cwd,
        trust_repository,
    })
}

fn worktree_create(args: &[String]) -> std::io::Result<i32> {
    let mut workspace_id = None;
    let mut cwd = None;
    let mut branch = None;
    let mut base = None;
    let mut path = None;
    let mut label = None;
    let mut focus = false;
    let mut trust_repository = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --workspace");
                    return Ok(2);
                };
                workspace_id = Some(super::normalize_workspace_id(value));
                index += 2;
            }
            "--cwd" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --cwd");
                    return Ok(2);
                };
                cwd = Some(normalize_path_arg(value)?);
                index += 2;
            }
            "--branch" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --branch");
                    return Ok(2);
                };
                branch = Some(value.clone());
                index += 2;
            }
            "--base" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --base");
                    return Ok(2);
                };
                base = Some(value.clone());
                index += 2;
            }
            "--path" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --path");
                    return Ok(2);
                };
                path = Some(normalize_path_arg(value)?);
                index += 2;
            }
            "--label" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --label");
                    return Ok(2);
                };
                label = Some(value.clone());
                index += 2;
            }
            "--focus" => {
                focus = true;
                index += 1;
            }
            "--no-focus" => {
                focus = false;
                index += 1;
            }
            "--trust-repository" => {
                trust_repository = true;
                index += 1;
            }
            "--json" => index += 1,
            other => {
                eprintln!("unknown option: {other}");
                return Ok(2);
            }
        }
    }
    if workspace_id.is_some() && cwd.is_some() {
        eprintln!(
            "usage: herdr worktree create [--workspace ID | --cwd PATH] [--branch NAME] [--base REF] [--path PATH] [--label TEXT] [--focus] [--no-focus] [--trust-repository]"
        );
        return Ok(2);
    }

    super::runtime::worktree_create(WorktreeCreateParams {
        workspace_id,
        cwd,
        branch,
        base,
        path,
        label,
        focus,
        trust_repository,
    })
}

fn worktree_open(args: &[String]) -> std::io::Result<i32> {
    let mut workspace_id = None;
    let mut cwd = None;
    let mut path = None;
    let mut branch = None;
    let mut label = None;
    let mut focus = false;
    let mut trust_repository = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --workspace");
                    return Ok(2);
                };
                workspace_id = Some(super::normalize_workspace_id(value));
                index += 2;
            }
            "--cwd" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --cwd");
                    return Ok(2);
                };
                cwd = Some(normalize_path_arg(value)?);
                index += 2;
            }
            "--path" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --path");
                    return Ok(2);
                };
                path = Some(normalize_path_arg(value)?);
                index += 2;
            }
            "--branch" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --branch");
                    return Ok(2);
                };
                branch = Some(value.clone());
                index += 2;
            }
            "--label" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --label");
                    return Ok(2);
                };
                label = Some(value.clone());
                index += 2;
            }
            "--focus" => {
                focus = true;
                index += 1;
            }
            "--no-focus" => {
                focus = false;
                index += 1;
            }
            "--trust-repository" => {
                trust_repository = true;
                index += 1;
            }
            "--json" => index += 1,
            other => {
                eprintln!("unknown option: {other}");
                return Ok(2);
            }
        }
    }
    if workspace_id.is_some() && cwd.is_some() {
        eprintln!(
            "usage: herdr worktree open [--workspace ID | --cwd PATH] (--path PATH | --branch NAME) [--label TEXT] [--focus] [--no-focus] [--trust-repository]"
        );
        return Ok(2);
    }
    if path.is_some() == branch.is_some() {
        eprintln!(
            "usage: herdr worktree open [--workspace ID | --cwd PATH] (--path PATH | --branch NAME) [--label TEXT] [--focus] [--no-focus] [--trust-repository]"
        );
        return Ok(2);
    }

    super::runtime::worktree_open(WorktreeOpenParams {
        workspace_id,
        cwd,
        path,
        branch,
        label,
        focus,
        trust_repository,
    })
}

fn worktree_remove(args: &[String]) -> std::io::Result<i32> {
    let mut workspace_id = None;
    let mut force = false;
    let mut trust_repository = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --workspace");
                    return Ok(2);
                };
                workspace_id = Some(super::normalize_workspace_id(value));
                index += 2;
            }
            "--force" => {
                force = true;
                index += 1;
            }
            "--trust-repository" => {
                trust_repository = true;
                index += 1;
            }
            "--json" => index += 1,
            other => {
                eprintln!("unknown option: {other}");
                return Ok(2);
            }
        }
    }

    let Some(workspace_id) = workspace_id else {
        eprintln!("usage: herdr worktree remove --workspace ID [--force] [--trust-repository]");
        return Ok(2);
    };

    super::runtime::worktree_remove(WorktreeRemoveParams {
        workspace_id,
        force,
        trust_repository,
    })
}

fn print_worktree_help() {
    eprintln!("herdr worktree commands:");
    eprintln!("  herdr worktree cleanup inspect | register PATH | preview [PATH] | run [PATH] | configure POLICY.json [PATH]");
    eprintln!("  herdr worktree list [--workspace ID | --cwd PATH] [--trust-repository]");
    eprintln!(
        "  herdr worktree create [--workspace ID | --cwd PATH] [--branch NAME] [--base REF] [--path PATH] [--label TEXT] [--focus] [--no-focus] [--trust-repository]"
    );
    eprintln!(
        "  herdr worktree open [--workspace ID | --cwd PATH] (--path PATH | --branch NAME) [--label TEXT] [--focus] [--no-focus] [--trust-repository]"
    );
    eprintln!("  herdr worktree remove --workspace ID [--force] [--trust-repository]");
}

fn normalize_path_arg(value: &str) -> std::io::Result<String> {
    let path = crate::worktree::expand_tilde_path(value);
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(absolute.display().to_string())
}
