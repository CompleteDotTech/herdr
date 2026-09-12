//! Argv-only access to the server-owned external execution projection.
use crate::api::schema::{
    EmptyParams, Method, Request, RuntimeProviderAttachParams, RuntimeProviderAttachmentTarget,
    RuntimeProviderTarget,
};

const USAGE: &str = "usage: herdr runtime-provider <list|get PROVIDER|attach PROVIDER SESSION GENERATION [--workspace ID] [--label LABEL] [--focus]|attachment TERMINAL|takeover TERMINAL|detach TERMINAL>";

pub(super) fn run(args: &[String]) -> std::io::Result<i32> {
    if matches!(args, [command] if matches!(command.as_str(), "help" | "--help" | "-h")) {
        println!("{USAGE}");
        crate::platform::end_cli_output();
        return Ok(0);
    }
    let Some(method) = parse(args) else {
        eprintln!("{USAGE}");
        return Ok(2);
    };
    let response = super::send_request(&Request {
        id: "cli:runtime-provider".into(),
        method,
    })?;
    super::print_response(&response)
}

fn parse(args: &[String]) -> Option<Method> {
    match args {
        [command] if command == "list" => Some(Method::RuntimeProviderList(EmptyParams::default())),
        [command, id] if command == "get" && valid_id(id) => {
            Some(Method::RuntimeProviderGet(RuntimeProviderTarget {
                provider_id: id.clone(),
            }))
        }
        [command, id] if command == "attachment" && valid_id(id) => Some(
            Method::RuntimeProviderAttachmentGet(RuntimeProviderAttachmentTarget {
                terminal_id: id.clone(),
            }),
        ),
        [command, id] if command == "detach" && valid_id(id) => Some(
            Method::RuntimeProviderDetach(RuntimeProviderAttachmentTarget {
                terminal_id: id.clone(),
            }),
        ),
        [command, id] if command == "takeover" && valid_id(id) => Some(
            Method::RuntimeProviderTakeover(RuntimeProviderAttachmentTarget {
                terminal_id: id.clone(),
            }),
        ),
        [command, provider, session, generation, rest @ ..]
            if command == "attach"
                && valid_id(provider)
                && coven_client::execution::ExecutionSessionId::new(session.clone()).is_ok() =>
        {
            let generation = generation
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && *value <= i64::MAX as u64)?;
            let mut request = RuntimeProviderAttachParams {
                provider_id: provider.clone(),
                session_id: session.clone(),
                generation,
                workspace_id: None,
                label: None,
                focus: false,
            };
            let mut rest = rest;
            while let Some((flag, tail)) = rest.split_first() {
                match flag.as_str() {
                    "--focus" if !request.focus => {
                        request.focus = true;
                        rest = tail;
                    }
                    "--workspace" if request.workspace_id.is_none() => {
                        let (value, tail) = tail.split_first()?;
                        if !valid_id(value) || value.starts_with("--") {
                            return None;
                        }
                        request.workspace_id = Some(value.clone());
                        rest = tail;
                    }
                    "--label" if request.label.is_none() => {
                        let (value, tail) = tail.split_first()?;
                        if value.starts_with("--")
                            || value.len() > 256
                            || value.chars().any(char::is_control)
                        {
                            return None;
                        }
                        request.label = Some(value.clone());
                        rest = tail;
                    }
                    _ => return None,
                }
            }
            Some(Method::RuntimeProviderAttach(request))
        }
        _ => None,
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn accepts_full_opaque_execution_session_contract() {
        for session in ["s".repeat(512), "engine/%2F42".into()] {
            assert!(parse(&args(&["attach", "p", &session, "1"])).is_some());
        }
        assert!(parse(&args(&["attach", "p", &"s".repeat(513), "1"])).is_none());
        assert_eq!(run(&args(&["help"])).unwrap(), 0);
    }

    #[test]
    fn preserves_literal_labels_as_typed_data() {
        let method = parse(&args(&[
            "attach",
            "provider",
            "session",
            "3",
            "--label",
            "$(literal); `data`",
            "--focus",
        ]))
        .unwrap();
        let Method::RuntimeProviderAttach(request) = method else {
            panic!("attach method expected")
        };
        assert_eq!(request.label.as_deref(), Some("$(literal); `data`"));
        assert_eq!(request.generation, 3);
        assert!(request.focus);
    }

    #[test]
    fn invalid_or_ambiguous_arguments_never_form_a_request() {
        for input in [
            vec!["attach", "p", "s", "0"],
            vec!["attach", "p", "s", "1", "--workspace"],
            vec!["attach", "p", "s", "1", "--workspace", "--focus"],
            vec!["attach", "p", "s", "1", "--label", "--focus"],
            vec!["attach", "p", "s", "1", "--focus", "--focus"],
            vec!["detach", ""],
            vec!["list", "extra"],
        ] {
            assert!(parse(&args(&input)).is_none(), "{input:?}");
        }
    }

    #[test]
    fn read_and_detach_commands_select_only_provider_methods() {
        assert!(matches!(
            parse(&args(&["list"])),
            Some(Method::RuntimeProviderList(_))
        ));
        assert!(matches!(
            parse(&args(&["get", "p"])),
            Some(Method::RuntimeProviderGet(_))
        ));
        assert!(matches!(
            parse(&args(&["attachment", "t"])),
            Some(Method::RuntimeProviderAttachmentGet(_))
        ));
        assert!(matches!(
            parse(&args(&["detach", "t"])),
            Some(Method::RuntimeProviderDetach(_))
        ));
        assert!(matches!(
            parse(&args(&["takeover", "t"])),
            Some(Method::RuntimeProviderTakeover(_))
        ));
    }
}
