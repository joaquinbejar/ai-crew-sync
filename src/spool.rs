//! The proxy's durable reference spool.
//!
//! `delivered` on this bus means a reference reached a process that can
//! still find it after a restart. That claim needs something on disk, and
//! this is it: an append-only JSON-lines file, 0600 inside the 0700 state
//! directory, fsynced **before** the bus is told anything was delivered.
//!
//! The order is the whole point. Append and fsync, then confirm. A crash
//! before the confirm costs a redelivery, which is idempotent; a confirm
//! before the append would cost the reference itself — the bus would record
//! a delivery that no process can prove.
//!
//! It holds references, never bodies: nothing private is written to a
//! developer's disk by this file, and a body is fetched with a current
//! access check at the moment it is read.

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// One spooled reference.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Entry {
    pub delivery_id: String,
    pub message_id: String,
    pub conversation_id: String,
    #[serde(default)]
    pub seq: i64,
    #[serde(default)]
    pub from_address: String,
    #[serde(default)]
    pub created_at: String,
    /// The bus has been told this one is held durably.
    #[serde(default)]
    pub confirmed: bool,
    pub spooled_at: String,
}

/// How long a confirmed entry is kept before the file is compacted. Long
/// enough to recognise a late redelivery, short enough that the file is a
/// spool and not an archive.
pub const KEEP_HOURS: i64 = 48;

pub fn spool_path(dir: &Path, session: &str) -> PathBuf {
    dir.join("inbox")
        .join(format!("{}.jsonl", crate::context::binding_key(session)))
}

fn ensure_dir(path: &Path) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    Ok(())
}

/// Append references and **fsync**. Returns the entries actually written:
/// one already spooled is not written twice, so a redelivery is recognised
/// rather than duplicated.
pub fn append(path: &Path, entries: &[Entry]) -> anyhow::Result<Vec<Entry>> {
    ensure_dir(path)?;
    let existing = read(path);
    let mut fresh = Vec::new();
    for entry in entries {
        if existing.iter().any(|e| e.delivery_id == entry.delivery_id)
            || fresh
                .iter()
                .any(|e: &Entry| e.delivery_id == entry.delivery_id)
        {
            continue;
        }
        fresh.push(entry.clone());
    }
    if fresh.is_empty() {
        return Ok(fresh);
    }
    // A process killed mid-write leaves a line with no newline. Appending
    // straight onto it would glue the next entry to that fragment, fsync
    // happily, and lose both on the next read — after the bus had already
    // been told they were held.
    repair_tail(path)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    for entry in &fresh {
        let line = serde_json::to_string(entry)?;
        writeln!(file, "{line}")?;
    }
    // The durability claim is this call. Without it, "delivered" is a hope.
    file.sync_all().context("fsync of the inbox spool")?;
    Ok(fresh)
}

/// Terminate an interrupted final line before anything is appended.
fn repair_tail(path: &Path) -> anyhow::Result<()> {
    let Ok(text) = std::fs::read(path) else {
        return Ok(());
    };
    if text.is_empty() || text.ends_with(b"\n") {
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

/// Every entry the spool still holds. A malformed line is skipped rather
/// than failing the read: a truncated write from a killed process must not
/// make the whole spool unreadable.
pub fn read(path: &Path) -> Vec<Entry> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<Entry>(l).ok())
        .collect()
}

/// Rewrite the spool with these entries, atomically, dropping confirmed
/// ones older than [`KEEP_HOURS`].
pub fn rewrite(path: &Path, entries: &[Entry]) -> anyhow::Result<()> {
    ensure_dir(path)?;
    let cutoff = chrono::Utc::now() - chrono::Duration::hours(KEEP_HOURS);
    let kept: Vec<String> = entries
        .iter()
        .filter(|e| {
            if !e.confirmed {
                return true;
            }
            match chrono::DateTime::parse_from_rfc3339(&e.spooled_at) {
                Ok(at) => at.with_timezone(&chrono::Utc) > cutoff,
                Err(_) => true,
            }
        })
        .filter_map(|e| serde_json::to_string(e).ok())
        .collect();
    crate::context::write_private(path, &format!("{}\n", kept.join("\n")))
}

/// Entries the bus has not been told about. These are what a reconnect
/// confirms before asking for anything new.
pub fn unconfirmed(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter(|e| !e.confirmed)
        .map(|e| e.delivery_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str) -> Entry {
        Entry {
            delivery_id: id.to_owned(),
            message_id: "m".into(),
            conversation_id: "c".into(),
            seq: 1,
            from_address: "dani/review".into(),
            created_at: "2026-09-20T00:00:00Z".into(),
            confirmed: false,
            spooled_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    #[test]
    fn a_redelivered_reference_is_not_spooled_twice() {
        let dir = std::env::temp_dir().join(format!("acs-spool-{}", uuid::Uuid::new_v4()));
        let path = dir.join("s.jsonl");
        assert_eq!(append(&path, &[entry("a"), entry("b")]).unwrap().len(), 2);
        assert_eq!(
            append(&path, &[entry("b"), entry("c")]).unwrap().len(),
            1,
            "'b' was already held; only 'c' is new"
        );
        assert_eq!(read(&path).len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_truncated_line_does_not_lose_the_rest() {
        let dir = std::env::temp_dir().join(format!("acs-spool-{}", uuid::Uuid::new_v4()));
        let path = dir.join("s.jsonl");
        append(&path, &[entry("a")]).unwrap();
        {
            // No newline: a process killed mid-write leaves exactly this.
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            write!(f, "{{\"delivery_id\": \"hal").unwrap();
        }
        append(&path, &[entry("b")]).unwrap();
        let held = read(&path);
        assert_eq!(held.len(), 2);
        assert!(held.iter().any(|e| e.delivery_id == "b"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_keeps_what_the_bus_has_not_been_told() {
        let dir = std::env::temp_dir().join(format!("acs-spool-{}", uuid::Uuid::new_v4()));
        let path = dir.join("s.jsonl");
        let mut old = entry("old");
        old.confirmed = true;
        old.spooled_at =
            (chrono::Utc::now() - chrono::Duration::hours(KEEP_HOURS + 1)).to_rfc3339();
        let mut pending = entry("pending");
        pending.spooled_at = old.spooled_at.clone();
        rewrite(&path, &[old, pending]).unwrap();
        let held = read(&path);
        assert_eq!(held.len(), 1);
        assert_eq!(
            held[0].delivery_id, "pending",
            "an unconfirmed entry is never dropped, however old"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
