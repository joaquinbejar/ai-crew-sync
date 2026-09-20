//! Turning a parsed subcommand into an MCP `tools/call`.
//!
//! Kept apart from transport and rendering because this is the layer with the
//! most invariants worth testing: every subcommand must map to a tool the
//! server actually serves, with the argument names its schema expects. A
//! rename on either side should fail a test here, not a user's command.

use anyhow::Context;
use serde_json::{Value, json};

use super::{ClientCmd, LockCmd, NoteCmd, TaskCmd};

pub(super) fn compact(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, compact(v)))
                .collect(),
        ),
        other => other,
    }
}

fn guess_content_type(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "txt" | "md" | "log" | "diff" | "patch" | "rs" | "toml" | "yml" | "yaml" | "sh" => {
            "text/plain"
        }
        "json" => "application/json",
        _ => "application/octet-stream",
    }
}

pub(super) fn attachment_json(
    path: &std::path::Path,
    content_type: Option<&str>,
) -> anyhow::Result<Value> {
    use base64::Engine;
    let data = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_owned();
    Ok(json!({
        "filename": filename,
        "content_type": content_type.unwrap_or_else(|| guess_content_type(path)),
        "data_base64": base64::engine::general_purpose::STANDARD.encode(&data),
    }))
}

/// The project's defaults, threaded into the calls that can use them. Only
/// what the resolver proved local and trusted: a repository names a channel,
/// it never names a credential.
#[derive(Clone, Debug, Default)]
pub struct Defaults {
    pub channel: Option<String>,
}

pub fn to_call(cmd: &ClientCmd) -> anyhow::Result<Option<(&'static str, Value)>> {
    to_call_with(cmd, &Defaults::default())
}

pub fn to_call_with(
    cmd: &ClientCmd,
    defaults: &Defaults,
) -> anyhow::Result<Option<(&'static str, Value)>> {
    let (tool, args): (&str, Value) = match cmd {
        ClientCmd::Whoami => ("whoami", json!({})),
        ClientCmd::Tools => return Ok(None),
        ClientCmd::Send {
            channel,
            to,
            body,
            announce,
            reply_to,
            file,
        } => {
            let attachments = file
                .iter()
                .map(|p| attachment_json(p, None))
                .collect::<anyhow::Result<Vec<_>>>()?;
            // A message with neither --channel nor --to falls back to the
            // project's channel, which is what .acs.toml is for. With --to
            // it stays a direct message, and an explicit --channel wins.
            let channel = match (channel, to) {
                (Some(c), _) => Some(c.clone()),
                (None, Some(_)) => None,
                (None, None) => defaults.channel.clone(),
            };
            let channel = &channel;
            (
                "post_message",
                json!({
                    "channel": channel, "to": to, "body": body,
                    "announce": announce, "reply_to": reply_to,
                    "attachments": if attachments.is_empty() { Value::Null } else { json!(attachments) }
                }),
            )
        }
        ClientCmd::Attach {
            task,
            file,
            content_type,
        } => {
            let mut att = attachment_json(file, content_type.as_deref())?;
            att["task"] = json!(task);
            ("attach_file", att)
        }
        ClientCmd::Download { id, .. } => ("get_attachment", json!({"id": id})),
        ClientCmd::Ask {
            to,
            question,
            timeout_seconds,
            resume_id,
        } => (
            "ask_agent",
            json!({
                "to": to, "question": question,
                "timeout_seconds": timeout_seconds, "resume_message_id": resume_id
            }),
        ),
        ClientCmd::Read {
            scope,
            history,
            limit,
            all_sessions,
        } => (
            "read_messages",
            json!({"scope": scope, "only_new": !history, "limit": limit,
                   "all_sessions": all_sessions}),
        ),
        ClientCmd::Search { query, limit } => {
            ("search_messages", json!({"query": query, "limit": limit}))
        }
        ClientCmd::Channels => ("list_channels", json!({})),
        ClientCmd::ChannelCreate { name, topic } => {
            ("create_channel", json!({"name": name, "topic": topic}))
        }
        ClientCmd::Agents { online } => ("list_agents", json!({"online_only": online})),
        ClientCmd::Beat {
            status,
            repo,
            branch,
            activity,
            ttl_seconds,
        } => (
            "heartbeat",
            json!({
                "status": status, "repo": repo, "branch": branch,
                "activity": activity, "ttl_seconds": ttl_seconds
            }),
        ),
        ClientCmd::Tasks { status, mine } => {
            ("list_tasks", json!({"status": status, "mine_only": mine}))
        }
        ClientCmd::Wait {
            timeout_seconds,
            kinds,
            all_channels,
        } => (
            "wait_for_updates",
            json!({
                "timeout_seconds": timeout_seconds,
                "kinds": if kinds.is_empty() { Value::Null } else { json!(kinds) },
                "all_channels": all_channels
            }),
        ),
        ClientCmd::Digest {
            hours,
            all_channels,
        } => (
            "team_digest",
            json!({"hours": hours, "all_channels": all_channels}),
        ),
        ClientCmd::Lock(lock) => match lock {
            LockCmd::Acquire {
                name,
                ttl_seconds,
                purpose,
            } => (
                "acquire_lock",
                json!({"name": name, "ttl_seconds": ttl_seconds, "purpose": purpose}),
            ),
            LockCmd::Release { name } => ("release_lock", json!({"name": name})),
            LockCmd::List => ("list_locks", json!({})),
        },
        ClientCmd::Task(task) => match task {
            TaskCmd::Create {
                key,
                title,
                description,
                depends_on,
            } => (
                "create_task",
                json!({
                    "key": key, "title": title, "description": description,
                    "depends_on": if depends_on.is_empty() { Value::Null } else { json!(depends_on) }
                }),
            ),
            TaskCmd::Show { key } => ("get_task", json!({"key": key})),
            TaskCmd::Claim { key, lease_seconds } => (
                "claim_task",
                json!({"key": key, "lease_seconds": lease_seconds}),
            ),
            TaskCmd::Next { lease_seconds } => {
                ("claim_next_task", json!({"lease_seconds": lease_seconds}))
            }
            TaskCmd::Renew { key, lease_seconds } => (
                "renew_task_lease",
                json!({"key": key, "lease_seconds": lease_seconds}),
            ),
            TaskCmd::Release { key } => ("release_task", json!({"key": key})),
            TaskCmd::Done { key, result } => {
                ("complete_task", json!({"key": key, "result": result}))
            }
        },
        ClientCmd::Notes { scope, tag } => ("list_notes", json!({"scope": scope, "tag": tag})),
        ClientCmd::Note(note) => match note {
            NoteCmd::Get { key, scope } => ("get_note", json!({"key": key, "scope": scope})),
            NoteCmd::Set {
                key,
                value,
                scope,
                tags,
            } => (
                "set_note",
                json!({
                    "key": key, "value": value, "scope": scope,
                    "tags": if tags.is_empty() { Value::Null } else { json!(tags) }
                }),
            ),
            NoteCmd::Rm { key, scope } => ("delete_note", json!({"key": key, "scope": scope})),
            NoteCmd::Search { query, scope } => {
                ("search_notes", json!({"query": query, "scope": scope}))
            }
        },
        ClientCmd::Call { .. } => unreachable!("handled by caller"),
    };
    Ok(Some((tool, compact(args))))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Map a command and assert the tool name and the arguments it carries.
    fn mapped(cmd: ClientCmd) -> (String, Value) {
        let (tool, args) = to_call(&cmd)
            .expect("mapping succeeds")
            .expect("this command is a tool call");
        (tool.to_string(), args)
    }

    #[test]
    fn every_subcommand_maps_to_a_tool() {
        // The escape hatch and `tools` are the only two that are not a
        // straight mapping; everything else must produce a tool name.
        let cases: Vec<ClientCmd> = vec![
            ClientCmd::Whoami,
            ClientCmd::Send {
                channel: Some("dev".into()),
                to: None,
                body: "hi".into(),
                announce: false,
                reply_to: None,
                file: vec![],
            },
            ClientCmd::Ask {
                to: "marta".into(),
                question: Some("q".into()),
                timeout_seconds: None,
                resume_id: None,
            },
            ClientCmd::Read {
                scope: "all".into(),
                history: false,
                limit: 50,
                all_sessions: false,
            },
            ClientCmd::Search {
                query: "x".into(),
                limit: 50,
            },
            ClientCmd::Channels,
            ClientCmd::ChannelCreate {
                name: "dev".into(),
                topic: None,
            },
            ClientCmd::Agents { online: false },
            ClientCmd::Beat {
                status: None,
                repo: None,
                branch: None,
                activity: None,
                ttl_seconds: None,
            },
            ClientCmd::Tasks {
                status: None,
                mine: false,
            },
            ClientCmd::Task(TaskCmd::Show { key: "k".into() }),
            ClientCmd::Notes {
                scope: None,
                tag: None,
            },
            ClientCmd::Note(NoteCmd::Get {
                key: "k".into(),
                scope: None,
            }),
            ClientCmd::Wait {
                timeout_seconds: None,
                kinds: vec![],
                all_channels: false,
            },
            ClientCmd::Lock(LockCmd::List),
            ClientCmd::Digest {
                hours: 24,
                all_channels: false,
            },
            ClientCmd::Download { id: 1, out: None },
        ];
        for cmd in cases {
            let mapped = to_call(&cmd).expect("mapping succeeds");
            assert!(mapped.is_some(), "a subcommand mapped to no tool");
        }
    }

    #[test]
    fn tools_is_not_a_tool_call() {
        assert!(
            to_call(&ClientCmd::Tools).expect("ok").is_none(),
            "`tools` lists the surface, it does not call into it"
        );
    }

    #[test]
    fn send_uses_the_schema_argument_names() {
        let (tool, args) = mapped(ClientCmd::Send {
            channel: None,
            to: Some("marta".into()),
            body: "hi".into(),
            announce: false,
            reply_to: Some(7),
            file: vec![],
        });
        assert_eq!(tool, "post_message");
        assert_eq!(args["to"], "marta");
        assert_eq!(args["body"], "hi");
        assert_eq!(args["reply_to"], 7);
        assert!(
            args.get("channel").is_none(),
            "an unset flag is absent, not null: the server treats null as a value"
        );
    }

    #[test]
    fn read_inverts_history_into_only_new() {
        let (_, args) = mapped(ClientCmd::Read {
            scope: "inbox".into(),
            history: true,
            limit: 10,
            all_sessions: false,
        });
        assert_eq!(
            args["only_new"], false,
            "--history means re-read, which is only_new = false"
        );
    }

    #[test]
    fn tasks_maps_mine_to_the_schemas_mine_only() {
        let (tool, args) = mapped(ClientCmd::Tasks {
            status: Some("open".into()),
            mine: true,
        });
        assert_eq!(tool, "list_tasks");
        assert_eq!(
            args["mine_only"], true,
            "the schema argument is mine_only; `mine` is the flag name"
        );
    }

    #[test]
    fn wait_omits_an_empty_kind_filter() {
        let (_, args) = mapped(ClientCmd::Wait {
            timeout_seconds: Some(30),
            kinds: vec![],
            all_channels: false,
        });
        assert_eq!(args["timeout_seconds"], 30);
        assert!(
            args.get("kinds").is_none(),
            "no --kinds means no filter, not an empty one"
        );

        let (_, args) = mapped(ClientCmd::Wait {
            timeout_seconds: None,
            kinds: vec!["message".into()],
            all_channels: false,
        });
        assert_eq!(args["kinds"][0], "message");
    }

    #[test]
    fn attachments_are_base64_with_a_guessed_type() {
        let dir = std::env::temp_dir().join("acs-mapping-test");
        std::fs::create_dir_all(&dir).expect("tmpdir");
        let path = dir.join("fix.diff");
        std::fs::write(&path, b"--- a\n+++ b\n").expect("write");

        let att = attachment_json(&path, None).expect("encodes");
        assert_eq!(att["filename"], "fix.diff");
        assert_eq!(att["content_type"], "text/plain", "guessed from .diff");
        assert_eq!(
            att["data_base64"], "LS0tIGEKKysrIGIK",
            "base64 of the file's bytes"
        );

        let att = attachment_json(&path, Some("application/x-custom")).expect("encodes");
        assert_eq!(att["content_type"], "application/x-custom", "explicit wins");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_missing_attachment_is_an_error_not_a_panic() {
        let err = attachment_json(std::path::Path::new("/nonexistent/nope.diff"), None);
        assert!(err.is_err(), "a missing file must surface as an error");
        assert!(
            format!("{:#}", err.unwrap_err()).contains("cannot read"),
            "the error names the problem"
        );
    }

    #[test]
    fn compact_drops_unset_flags_recursively() {
        let v = compact(json!({"a": 1, "b": null, "c": {"d": null, "e": 2}}));
        assert_eq!(v, json!({"a": 1, "c": {"e": 2}}));
    }
}
