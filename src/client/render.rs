//! Human-readable output. `--json` bypasses all of this and prints the raw
//! structured content, so scripts never depend on the shape of these lines.

use anyhow::Context;
use serde_json::Value;

use super::{ClientCmd, LockCmd, NoteCmd, TaskCmd};

pub(super) fn field<'v>(v: &'v Value, key: &str) -> &'v str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Where an agent (or one of its sessions) is working, as a leading fragment.
fn agent_place(v: &Value) -> String {
    match (v["repo"].as_str(), v["branch"].as_str()) {
        (Some(r), Some(b)) => format!(" {r}@{b}"),
        (Some(r), None) => format!(" {r}"),
        _ => String::new(),
    }
}

fn render_messages(value: &Value) {
    let messages = value["messages"].as_array().cloned().unwrap_or_default();
    if messages.is_empty() {
        println!("(no messages)");
        return;
    }
    for m in &messages {
        // Sessions shown on both ends: the sender's is the address to reply to,
        // and the recipient's says whether a message was for one window or for
        // the person.
        let target = m["channel"]
            .as_str()
            .map(|c| match m["announce"].as_bool() {
                Some(true) => format!("#{c} [announcement]"),
                _ => format!("#{c}"),
            })
            .or_else(|| {
                m["to"].as_str().map(|t| match m["to_session"].as_str() {
                    Some(s) => format!("@{t}/{s}"),
                    None => format!("@{t}"),
                })
            })
            .unwrap_or_default();
        let from = match m["from_session"].as_str() {
            Some(s) => format!("{}/{s}", field(m, "from")),
            None => field(m, "from").to_owned(),
        };
        println!(
            "[{}] {} {from} → {}: {}",
            m["id"],
            field(m, "created_at"),
            target,
            field(m, "body"),
        );
    }
    if value["truncated"].as_bool() == Some(true) {
        println!("(truncated at limit; raise --limit to see more)");
    }
}

fn render_task_line(t: &Value) {
    // The session is what tells two of your own windows apart, so it belongs
    // next to the name whenever there is one.
    let holder = t["claimed_by"]
        .as_str()
        .map(|h| match t["claimed_session"].as_str() {
            Some(s) => format!(" ({h}/{s})"),
            None => format!(" ({h})"),
        })
        .unwrap_or_default();
    let expired = if t["lease_expired"].as_bool() == Some(true) {
        " [lease expired]"
    } else {
        ""
    };
    println!(
        "{:<24} {:<8}{}{} {}",
        field(t, "key"),
        field(t, "status"),
        holder,
        expired,
        field(t, "title"),
    );
}

pub(super) fn render(cmd: &ClientCmd, value: &Value) -> anyhow::Result<()> {
    match cmd {
        ClientCmd::Whoami => {
            println!(
                "{} @ {} — {} unread DM(s), {} claimed task(s)",
                field(value, "agent"),
                field(value, "team"),
                value["unread_direct_messages"],
                value["open_claimed_tasks"],
            );
        }
        ClientCmd::Read { .. } | ClientCmd::Search { .. } => render_messages(value),
        ClientCmd::Send { .. } => {
            let m = &value["message"];
            println!("sent [{}] to {}", m["id"], value["delivered_to"]);
        }
        ClientCmd::Ask { .. } => {
            if value["answered"].as_bool() == Some(true) {
                let a = &value["answer"];
                println!("{}: {}", field(a, "from"), field(a, "body"));
            } else {
                println!("(no answer yet)");
                println!("{}", field(value, "suggestion"));
            }
        }
        ClientCmd::Attach { .. } => {
            println!(
                "attached [{}] {} ({} bytes)",
                value["id"],
                field(value, "filename"),
                value["size_bytes"]
            );
        }
        ClientCmd::Download { out, .. } => {
            use base64::Engine;
            let data = base64::engine::general_purpose::STANDARD
                .decode(field(value, "data_base64"))
                .context("server returned invalid base64")?;
            let path = out
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(field(value, "filename")));
            std::fs::write(&path, data)
                .with_context(|| format!("cannot write {}", path.display()))?;
            println!(
                "wrote {} ({} bytes, {})",
                path.display(),
                value["size_bytes"],
                field(value, "content_type")
            );
        }
        ClientCmd::ChannelCreate { .. } => {
            println!("channel #{} ready", field(value, "name"));
        }
        ClientCmd::Channels => {
            let channels = value["channels"].as_array().cloned().unwrap_or_default();
            if channels.is_empty() {
                println!("(no channels yet)");
            }
            for c in &channels {
                println!(
                    "#{:<20} {:>5} msg(s)  {}",
                    field(c, "name"),
                    c["message_count"],
                    field(c, "topic"),
                );
            }
        }
        ClientCmd::Sessions { .. } => {
            let sessions = value["sessions"].as_array().cloned().unwrap_or_default();
            if sessions.is_empty() {
                println!("(no sessions)");
            }
            for s in &sessions {
                let labels = match (s["project"].as_str(), s["role"].as_str()) {
                    (Some(p), Some(r)) => format!("{p}/{r}"),
                    (Some(p), None) => p.to_owned(),
                    (None, Some(r)) => format!("-/{r}"),
                    _ => "-".to_owned(),
                };
                println!(
                    "{:<36} {:<8} {:<28}{} {}",
                    field(s, "address"),
                    field(s, "status"),
                    labels,
                    agent_place(s),
                    field(s, "activity"),
                );
            }
            if value["count"] == value["limit"] {
                println!("(limit reached: narrow with --project or --role)");
            }
        }
        ClientCmd::Agents { .. } => {
            for a in value["agents"].as_array().cloned().unwrap_or_default() {
                let name = field(&a, "name");
                // `sessions` is only present when there is more than one, so
                // the common case prints exactly one line per teammate.
                match a["sessions"].as_array() {
                    Some(sessions) if !sessions.is_empty() => {
                        println!("{name}");
                        for s in sessions {
                            println!(
                                "  /{:<17} {:<8}{} {}",
                                s["session"].as_str().unwrap_or("(shared)"),
                                field(s, "status"),
                                agent_place(s),
                                field(s, "activity"),
                            );
                        }
                    }
                    _ => println!(
                        "{:<20} {:<8}{} {}",
                        name,
                        field(&a, "status"),
                        agent_place(&a),
                        field(&a, "activity"),
                    ),
                }
            }
            println!("({} online)", value["online_count"]);
        }
        ClientCmd::Tasks { .. } => {
            let tasks = value["tasks"].as_array().cloned().unwrap_or_default();
            if tasks.is_empty() {
                println!("(no tasks)");
            }
            for t in &tasks {
                render_task_line(t);
            }
            println!("({} open, {} claimed)", value["open"], value["claimed"]);
        }
        ClientCmd::Task(TaskCmd::Show { .. }) => {
            render_task_line(&value["task"]);
            if let Some(desc) = value["task"]["description"].as_str() {
                println!("  {desc}");
            }
            if let Some(result) = value["task"]["result"].as_str() {
                println!("  result: {result}");
            }
            for e in value["history"].as_array().cloned().unwrap_or_default() {
                println!(
                    "  {} {} {} {}",
                    field(&e, "created_at"),
                    field(&e, "event"),
                    field(&e, "agent"),
                    field(&e, "detail"),
                );
            }
        }
        ClientCmd::Task(TaskCmd::Claim { .. } | TaskCmd::Next { .. }) => {
            if value["claimed"].as_bool() == Some(true) {
                print!("claimed: ");
                render_task_line(&value["task"]);
            } else {
                println!(
                    "not claimed: {}",
                    value["reason"].as_str().unwrap_or("unknown reason")
                );
            }
        }
        ClientCmd::Task(_) => {
            render_task_line(value);
        }
        ClientCmd::Notes { .. } | ClientCmd::Note(NoteCmd::Search { .. }) => {
            let notes = value["notes"].as_array().cloned().unwrap_or_default();
            if notes.is_empty() {
                println!("(no notes)");
            }
            for n in &notes {
                let first_line = field(n, "value").lines().next().unwrap_or("").to_owned();
                println!("{}/{}: {}", field(n, "scope"), field(n, "key"), first_line);
            }
        }
        ClientCmd::Note(NoteCmd::Get { .. }) => {
            if value["found"].as_bool() == Some(true) {
                let n = &value["note"];
                println!(
                    "{}/{} (by {}, {})",
                    field(n, "scope"),
                    field(n, "key"),
                    field(n, "updated_by"),
                    field(n, "updated_at"),
                );
                println!("{}", field(n, "value"));
            } else {
                println!("not found");
            }
        }
        ClientCmd::Note(NoteCmd::Set { .. }) => {
            println!("saved {}/{}", field(value, "scope"), field(value, "key"));
        }
        ClientCmd::Note(NoteCmd::Rm { .. }) => {
            println!("{}", field(value, "detail"));
        }
        ClientCmd::Beat { .. } => {
            println!(
                "presence updated: {} {}",
                field(value, "status"),
                field(value, "activity"),
            );
        }
        ClientCmd::Wait { .. } => {
            if value["woke"].as_bool() == Some(true) {
                for e in value["events"].as_array().cloned().unwrap_or_default() {
                    println!("[{}] {}", field(&e, "kind"), field(&e, "summary"));
                }
            } else {
                println!("(nothing happened)");
            }
            println!("{}", field(value, "suggestion"));
        }
        ClientCmd::Lock(LockCmd::List) => {
            let locks = value["locks"].as_array().cloned().unwrap_or_default();
            if locks.is_empty() {
                println!("(no locks held)");
            }
            for l in &locks {
                let holder = match l["holder_session"].as_str() {
                    Some(s) => format!("{}/{s}", field(l, "holder")),
                    None => field(l, "holder").to_owned(),
                };
                println!(
                    "{:<28} {holder} until {}  {}",
                    field(l, "name"),
                    field(l, "expires_at"),
                    field(l, "purpose"),
                );
            }
        }
        ClientCmd::Lock(LockCmd::Acquire { .. }) => {
            if value["acquired"].as_bool() == Some(true) {
                let l = &value["lock"];
                println!(
                    "acquired {} until {}",
                    field(l, "name"),
                    field(l, "expires_at")
                );
            } else {
                println!("not acquired: {}", field(value, "reason"));
            }
        }
        ClientCmd::Lock(LockCmd::Release { .. }) => {
            println!("{}", field(value, "detail"));
        }
        ClientCmd::Digest { .. } => {
            println!("— last {}h —", value["hours"]);
            for c in value["channels"].as_array().cloned().unwrap_or_default() {
                println!("#{}: {} msg(s)", field(&c, "name"), c["message_count"]);
                for m in c["last_messages"].as_array().cloned().unwrap_or_default() {
                    println!("   {}: {}", field(&m, "from"), field(&m, "body"));
                }
            }
            let moved = value["tasks_moved"].as_array().cloned().unwrap_or_default();
            if !moved.is_empty() {
                println!("tasks:");
                for t in &moved {
                    println!(
                        "   {} → {} {}",
                        field(t, "key"),
                        field(t, "status"),
                        t["claimed_by"]
                            .as_str()
                            .map(|s| format!("({s})"))
                            .unwrap_or_default(),
                    );
                }
            }
            for n in value["notes_updated"]
                .as_array()
                .cloned()
                .unwrap_or_default()
            {
                println!(
                    "note {}/{} by {}",
                    field(&n, "scope"),
                    field(&n, "key"),
                    field(&n, "updated_by")
                );
            }
            println!(
                "({} open, {} claimed, {} lock(s))",
                value["open_tasks"],
                value["claimed_tasks"],
                value["active_locks"].as_array().map(Vec::len).unwrap_or(0),
            );
        }
        _ => {
            println!("{}", serde_json::to_string_pretty(value).unwrap());
        }
    }
    Ok(())
}
