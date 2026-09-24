//! Local connection context: which bus, as whom, for which project.
//!
//! Every client-side entry point — the console client, the stdio proxy, the
//! lifecycle hooks — answers the same question before it can talk to the
//! bus: *which endpoint, with which token, expected to be which agent of
//! which team, working on which project?* This module answers it once, from
//! three local sources and one explicit override, in a fixed order:
//!
//! 1. **Explicit credentials** — `--token` / `BUS_TOKEN` (with `--url` /
//!    `BUS_URL`). The operator said exactly what to use; nothing below may
//!    override it. A project file still contributes *metadata* (project
//!    name, channel), never credentials.
//! 2. **Explicit profile** — `--profile` / `BUS_PROFILE`. Must exist in the
//!    local profile store; a missing profile is an error, never a fallback.
//! 3. **Project defaults** — `.acs.toml` at the project root names an
//!    approved profile and a logical project. The repository is untrusted:
//!    it may only *name* a profile that the operator defined locally, and it
//!    may never carry an endpoint or a credential. A name that does not
//!    resolve locally is an error, not a different team.
//! 4. **User default** — `default = "…"` in the profile store.
//!
//! Profiles live in `<config dir>/profiles.toml` and carry the endpoint, the
//! expected team and agent, and a *reference* to a credential: the name of a
//! `tokens-<team>` file (the same `name=token` files `admin token issue
//! --save` writes) and, optionally, which entry. The secret itself is read
//! at resolve time and never stored twice.
//!
//! Nothing here logs; [`Resolved::redacted`] is what `context show` prints.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use crate::admin_cli::{config_dir, write_private};
use crate::auth::TOKEN_PREFIX;

pub const PROFILES_FILE: &str = "profiles.toml";
pub const PROJECT_FILE: &str = ".acs.toml";
/// Entry used when a project has no entry of its own in a tokens file.
pub const BASE_KEY: &str = "_base";
/// Default MCP endpoint, kept from the console client's original default.
pub const DEFAULT_MCP_URL: &str = "http://localhost:8787/mcp";

/// How far up a directory tree the project file is searched for. A source
/// tree is never this deep; a bound keeps a stray symlink loop finite.
const MAX_ASCENT: usize = 64;

// ---------------------------------------------------------------- profiles --

/// An operator-approved way to reach a bus: endpoint, expected identity and
/// where the credential lives. Never contains the secret.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Profile {
    /// Base URL of the bus (`https://bus.example.com:8443`); `/mcp` is
    /// appended. A URL pasted with `/mcp` already on it is accepted.
    pub url: String,
    /// Team the credential is expected to belong to. Verified against the
    /// server's `whoami`, never assumed.
    pub team: String,
    /// Agent the credential is expected to be.
    pub agent: String,
    /// Name of the token file inside the configuration directory, e.g.
    /// `tokens-acme`. A bare file name: it may not point outside that
    /// directory.
    pub tokens: String,
    /// Entry of the token file to use when the project names none. Defaults
    /// to the project name, then `_base`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Profiles {
    /// Profile used when nothing else selects one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

fn profiles_path(dir: &Path) -> PathBuf {
    dir.join(PROFILES_FILE)
}

/// A profile name is one safe word: it is a file key people type and a
/// value a repository may reference.
pub fn validate_name(what: &str, raw: &str) -> anyhow::Result<String> {
    let name = raw.trim();
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && !name.starts_with('.');
    if !ok {
        bail!(
            "{what} '{raw}' is not valid: use letters, digits, '-', '_' and '.' \
             (up to 64, not starting with '.')"
        );
    }
    Ok(name.to_owned())
}

/// A tokens file reference stays inside the configuration directory: a bare
/// name, no separators, no traversal.
pub fn validate_tokens_ref(raw: &str) -> anyhow::Result<String> {
    let name = raw.trim();
    if name.is_empty()
        || name.contains(['/', '\\'])
        || name == "."
        || name == ".."
        || name.starts_with('.')
    {
        bail!(
            "tokens file '{raw}' must be a bare file name inside the configuration \
             directory, such as tokens-acme"
        );
    }
    Ok(name.to_owned())
}

pub fn load_profiles(dir: &Path) -> anyhow::Result<Profiles> {
    let path = profiles_path(dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Profiles::default()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let parsed: Profiles =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    for (name, p) in &parsed.profiles {
        validate_name("profile name", name)?;
        validate_tokens_ref(&p.tokens)?;
    }
    Ok(parsed)
}

/// Serialise every write to the configuration directory through one lock,
/// so two `context profile add` or two `--save` running at once cannot
/// interleave a read-modify-write. The lock file itself is empty.
pub fn with_config_lock<T>(dir: &Path, f: impl FnOnce() -> anyhow::Result<T>) -> anyhow::Result<T> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let lock_path = dir.join(".lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening {}", lock_path.display()))?;
    lock.lock()
        .with_context(|| format!("locking {}", lock_path.display()))?;
    let out = f();
    let _ = lock.unlock();
    out
}

pub fn save_profiles(dir: &Path, profiles: &Profiles) -> anyhow::Result<PathBuf> {
    let path = profiles_path(dir);
    let text = toml::to_string_pretty(profiles).context("serialising profiles")?;
    let header = "# ai-crew-sync connection profiles — no secrets here; tokens live in the\n\
                  # tokens-<team> files this refers to. Edit with `ai-crew-sync context profile`.\n";
    write_private(&path, &format!("{header}{text}"))?;
    Ok(path)
}

/// Read-modify-write a profile store under the lock.
pub fn update_profiles(
    dir: &Path,
    f: impl FnOnce(&mut Profiles) -> anyhow::Result<()>,
) -> anyhow::Result<PathBuf> {
    with_config_lock(dir, || {
        let mut profiles = load_profiles(dir)?;
        f(&mut profiles)?;
        save_profiles(dir, &profiles)
    })
}

// ------------------------------------------------------------ project file --

/// What a repository may say about itself. Names only: it references an
/// approved profile, it never defines one.
/// `deny_unknown_fields` on purpose: this file comes from a repository, so
/// an unrecognised key is a claim we do not understand, not a comment. A
/// tolerated `token_file =` that silently did nothing would be indistinguishable
/// from one that worked.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    /// Locally approved profile to connect with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Logical project name; also the default token-file entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Channel this project's sessions post to by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Token-file entry to use instead of the project name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Keys a repository must never carry. Their presence is refused outright
/// rather than ignored: an operator who sees `url =` in a project file must
/// not believe it does something.
const FORBIDDEN_PROJECT_KEYS: [&str; 6] =
    ["url", "endpoint", "token", "tokens", "bearer", "secret"];

pub fn parse_project_file(text: &str, path: &Path) -> anyhow::Result<ProjectConfig> {
    let table: toml::Table =
        toml::from_str(text).with_context(|| format!("parsing {}", path.display()))?;
    for key in FORBIDDEN_PROJECT_KEYS {
        if table.contains_key(key) {
            bail!(
                "{} sets '{key}', which a repository may not do: endpoints and credentials \
                 come from your local profiles only (`ai-crew-sync context profile add`). \
                 Remove the key",
                path.display()
            );
        }
    }
    let cfg: ProjectConfig = table.try_into().with_context(|| {
        format!(
            "{} has a key this version does not accept. A project file may only set \
             profile, project, channel and key — never an endpoint or a credential",
            path.display()
        )
    })?;
    if let Some(p) = &cfg.profile {
        validate_name("profile name", p)?;
    }
    if let Some(p) = &cfg.project {
        validate_name("project name", p)?;
    }
    if let Some(k) = &cfg.key {
        validate_name("token key", k)?;
    }
    Ok(cfg)
}

/// The project a directory belongs to: the nearest ancestor holding
/// `.acs.toml`. A linked git worktree that has no file of its own inherits
/// the main worktree's, so one checked-in file covers every worktree of the
/// repository.
pub fn find_project(start: &Path) -> anyhow::Result<Option<(PathBuf, ProjectConfig)>> {
    let start = start
        .canonicalize()
        .with_context(|| format!("resolving {}", start.display()))?;
    let mut dir: Option<&Path> = Some(&start);
    let mut worktree_main: Option<PathBuf> = None;
    for _ in 0..MAX_ASCENT {
        let Some(d) = dir else { break };
        let candidate = d.join(PROJECT_FILE);
        if candidate.is_file() {
            let text = std::fs::read_to_string(&candidate)
                .with_context(|| format!("reading {}", candidate.display()))?;
            return Ok(Some((
                d.to_path_buf(),
                parse_project_file(&text, &candidate)?,
            )));
        }
        // A `.git` *file* marks a linked worktree; remember where the main
        // worktree is, and stop climbing past the repository root.
        let dot_git = d.join(".git");
        if dot_git.is_file() && worktree_main.is_none() {
            worktree_main = main_worktree_of(&dot_git);
        }
        if dot_git.is_dir() {
            break;
        }
        if dot_git.is_file() {
            break;
        }
        dir = d.parent();
    }
    if let Some(main) = worktree_main {
        let candidate = main.join(PROJECT_FILE);
        if candidate.is_file() {
            let text = std::fs::read_to_string(&candidate)
                .with_context(|| format!("reading {}", candidate.display()))?;
            return Ok(Some((main, parse_project_file(&text, &candidate)?)));
        }
    }
    Ok(None)
}

/// Resolve `gitdir: …/.git/worktrees/<name>` to the main worktree directory
/// through the `commondir` file git keeps next to it.
fn main_worktree_of(dot_git_file: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(dot_git_file).ok()?;
    let gitdir = text.trim().strip_prefix("gitdir:")?.trim();
    let gitdir = {
        let p = Path::new(gitdir);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            dot_git_file.parent()?.join(p)
        }
    };
    let common = std::fs::read_to_string(gitdir.join("commondir")).ok()?;
    let common_dir = gitdir.join(common.trim()).canonicalize().ok()?;
    // commondir is the main worktree's .git directory.
    common_dir.parent().map(Path::to_path_buf)
}

/// What [`keep_project_file_local`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum LocalOutcome {
    /// The entry was appended to this exclude file.
    Added(PathBuf),
    /// This exclude file already had it.
    AlreadyExcluded(PathBuf),
    /// Not inside a git repository: nothing to exclude.
    NotARepository,
}

/// The git directory shared by every worktree of the repository holding
/// `start`: `.git` itself, or the `commondir` a linked worktree points at.
fn git_common_dir(start: &Path) -> Option<PathBuf> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        let dot_git = d.join(".git");
        if dot_git.is_dir() {
            return Some(dot_git);
        }
        if dot_git.is_file() {
            let text = std::fs::read_to_string(&dot_git).ok()?;
            let gitdir = text.trim().strip_prefix("gitdir:")?.trim();
            let gitdir = if Path::new(gitdir).is_absolute() {
                PathBuf::from(gitdir)
            } else {
                d.join(gitdir)
            };
            return match std::fs::read_to_string(gitdir.join("commondir")) {
                Ok(common) => gitdir.join(common.trim()).canonicalize().ok(),
                // A submodule's gitdir is its own common directory.
                Err(_) => Some(gitdir),
            };
        }
        dir = d.parent();
    }
    None
}

/// Keep the project file out of git: list it in the repository's
/// `info/exclude` (shared by linked worktrees). It names profiles from each
/// person's own `profiles.toml`, so a committed copy only works for a team
/// whose members all use the same profile names; local is the default.
/// Idempotent. An exclude never hides a file git already tracks; see
/// [`project_file_is_tracked`].
pub fn keep_project_file_local(root: &Path) -> anyhow::Result<LocalOutcome> {
    let Some(common) = git_common_dir(root) else {
        return Ok(LocalOutcome::NotARepository);
    };
    let exclude = common.join("info").join("exclude");
    let text = match std::fs::read_to_string(&exclude) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading {}", exclude.display())),
    };
    let listed = text.lines().any(|l| {
        let l = l.trim();
        l == PROJECT_FILE || l == format!("/{PROJECT_FILE}") || l == format!("**/{PROJECT_FILE}")
    });
    if listed {
        return Ok(LocalOutcome::AlreadyExcluded(exclude));
    }
    if let Some(parent) = exclude.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut add = String::new();
    if !text.is_empty() && !text.ends_with('\n') {
        add.push('\n');
    }
    add.push_str(PROJECT_FILE);
    add.push('\n');
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&exclude)
        .and_then(|mut f| f.write_all(add.as_bytes()))
        .with_context(|| format!("writing {}", exclude.display()))?;
    Ok(LocalOutcome::Added(exclude))
}

/// Whether git already tracks the project file at `root`, which an exclude
/// cannot undo. `None` when git is not available to ask.
pub fn project_file_is_tracked(root: &Path) -> Option<bool> {
    std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "--error-unmatch", "--", PROJECT_FILE])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()
        .map(|s| s.success())
}

pub fn write_project_file(root: &Path, cfg: &ProjectConfig) -> anyhow::Result<PathBuf> {
    let path = root.join(PROJECT_FILE);
    let text = toml::to_string_pretty(cfg).context("serialising project defaults")?;
    let header = "# ai-crew-sync project defaults — names only, never a credential or an endpoint.\n\
                  # `profile` must exist in each teammate's local profiles.\n";
    // Atomic like every other write here; readable by the repository's
    // tooling, since there is nothing secret in it.
    let dir = path.parent().context("project root has no parent")?;
    let tmp = dir.join(format!(".{PROJECT_FILE}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, format!("{header}{text}"))
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(path)
}

// ----------------------------------------------------------------- resolve --

/// Everything a caller can say. Each field is `None` when not given; the
/// binary fills them from flags and environment, tests fill them directly.
/// Where an explicit value physically came from. Provenance for humans
/// debugging an upgrade, never an authorization signal: precedence is
/// decided by [`resolve`] exactly as before, whatever the origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Origin {
    /// Typed on the command line.
    Flag,
    /// Inherited from the process environment.
    Environment,
}

impl Origin {
    /// Best-effort attribution of a clap value that may come from a flag or
    /// an environment variable. clap does not say which one won, but it
    /// resolves flag-over-environment; so a value equal to the live
    /// environment variable is attributed to the environment. The one
    /// ambiguous case — a flag typed with exactly the environment's value —
    /// is attributed to the environment, which is harmless because both are
    /// the same value.
    pub fn of(env_name: &str, value: &str) -> Origin {
        match std::env::var(env_name) {
            Ok(v) if v == value => Origin::Environment,
            _ => Origin::Flag,
        }
    }

    /// How this origin reads next to the thing it qualifies, e.g.
    /// `BUS_TOKEN (environment)` or `--token (flag)`.
    pub fn describe(self, env_name: &str, flag: &str) -> String {
        match self {
            Origin::Environment => format!("{env_name} (environment)"),
            Origin::Flag => format!("{flag} (flag)"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Inputs {
    pub config_dir: PathBuf,
    /// `--url` / `BUS_URL`.
    pub explicit_url: Option<String>,
    /// Where `explicit_url` came from, when known.
    pub url_origin: Option<Origin>,
    /// `--token` / `BUS_TOKEN`.
    pub explicit_token: Option<String>,
    /// Where `explicit_token` came from, when known.
    pub token_origin: Option<Origin>,
    /// `--session` / `BUS_SESSION`.
    pub explicit_session: Option<String>,
    /// `--profile` / `BUS_PROFILE`.
    pub profile: Option<String>,
    /// `--project-dir` / `BUS_PROJECT_DIR`; the current directory when
    /// absent.
    pub project_dir: Option<PathBuf>,
    /// `--host-session` / `BUS_HOST_SESSION`: the id the host gives this
    /// conversation. Two processes of one conversation — the MCP proxy and a
    /// lifecycle hook — derive the same bus session from it without sharing
    /// state, which is what keeps a hook from draining a sibling window's
    /// messages.
    pub host_session: Option<String>,
}

/// The bus session a conversation id maps to. Pure and deterministic, so
/// every process of that conversation agrees without coordinating: this is
/// the handshake, not a file.
pub fn session_for_host(host_id: &str) -> String {
    let digest = Sha256::digest(host_id.trim().as_bytes());
    format!("s-{}", &hex::encode(digest)[..12])
}

/// Key of the binding record a proxy writes for its conversation.
pub fn binding_key(host_id: &str) -> String {
    hex::encode(Sha256::digest(host_id.trim().as_bytes()))
}

/// What the proxy of this conversation recorded: which profile, project and
/// role it settled on. Advisory — a hook works without it, just with less.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Binding {
    pub session: Option<String>,
    pub profile: Option<String>,
    pub project: Option<String>,
    pub role: Option<String>,
    pub agent: Option<String>,
    pub team: Option<String>,
    /// Endpoint the proxy of this conversation is connected to.
    pub mcp_url: Option<String>,
    /// The session credential. Present only while the window is open, and
    /// only ever read by `context hook`: never printed, logged or passed in
    /// argv.
    pub session_token: Option<String>,
    pub session_id: Option<String>,
    /// Epoch to send with it. A hook uses the proxy's epoch rather than
    /// registering, which would bump it and fence the proxy it belongs to.
    pub epoch: Option<i64>,
    pub expires_at: Option<String>,
    pub closed_at: Option<String>,
}

/// Directory holding one record per live conversation. Mode 0700: it is the
/// only place a session credential is written, and `context hook` is the
/// only thing that reads one.
pub const BINDINGS_DIR: &str = "sessions";

pub fn binding_path(dir: &Path, host_id: &str) -> PathBuf {
    dir.join(BINDINGS_DIR)
        .join(format!("{}.json", binding_key(host_id)))
}

/// Write a binding record: 0700 directory, 0600 file, atomic replace.
pub fn write_binding_file(path: &Path, content: &str) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Tightened every time: a directory created by an older version
            // (or by a careless umask) is corrected rather than trusted.
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    write_private(path, content)
}

pub fn read_binding(dir: &Path, host_id: &str) -> Option<Binding> {
    let text = std::fs::read_to_string(binding_path(dir, host_id)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Which rule produced the credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    /// `--token` / `BUS_TOKEN`.
    Explicit,
    /// `--profile` / `BUS_PROFILE`.
    ProfileFlag,
    /// `.acs.toml` at the project root.
    ProjectDefault,
    /// `default = "…"` in the profile store. Only `profile default` or
    /// `profile add --default` sets it.
    UserDefault,
}

/// The answer. `token` is the secret and the only field [`Self::redacted`]
/// hides.
#[derive(Clone, Debug)]
pub struct Resolved {
    pub mcp_url: String,
    pub token: String,
    pub source: Source,
    pub profile: Option<String>,
    /// `(team, agent)` the credential is expected to be. Only from a
    /// profile; explicit credentials promise nothing.
    pub expected: Option<(String, String)>,
    pub tokens_file: Option<PathBuf>,
    pub token_key: Option<String>,
    pub project: Option<String>,
    pub channel: Option<String>,
    pub project_root: Option<PathBuf>,
    pub session: Option<String>,
    /// Where the explicit token came from, when `source` is
    /// [`Source::Explicit`] and the caller said.
    pub token_origin: Option<Origin>,
    /// Where the URL came from when it was explicit.
    pub url_origin: Option<Origin>,
    /// Configuration the winning rule silently outranked — an installed
    /// default profile shadowed by leftover environment exports, say. Each
    /// entry is one printable sentence with no secret in it; every entry
    /// point shows them on stderr (or the log), never on MCP stdout.
    pub warnings: Vec<String>,
}

impl Resolved {
    /// Where the credential came from, in words a person debugging an
    /// upgrade can act on. Never contains the secret.
    pub fn credential_provenance(&self) -> String {
        match self.source {
            Source::Explicit => self
                .token_origin
                .unwrap_or(Origin::Flag)
                .describe("BUS_TOKEN", "--token"),
            _ => format!(
                "entry '{}' of {} (profile '{}', {})",
                self.token_key.as_deref().unwrap_or("?"),
                self.tokens_file
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                self.profile.as_deref().unwrap_or("?"),
                match self.source {
                    Source::ProfileFlag => "selected by --profile / BUS_PROFILE",
                    Source::ProjectDefault => "named by the project's .acs.toml",
                    _ => "the user default, which applies wherever no .acs.toml names a profile",
                }
            ),
        }
    }

    /// Where the endpoint came from. The URL itself is not a secret; what
    /// matters is whether the profile's endpoint or an override is in use.
    pub fn url_provenance(&self) -> String {
        match (self.url_origin, self.source) {
            (Some(o), _) => o.describe("BUS_URL", "--url"),
            (None, Source::Explicit) => "the built-in default".to_owned(),
            (None, _) => format!("profile '{}'", self.profile.as_deref().unwrap_or("?")),
        }
    }

    /// What `context show` prints: everything but the secret, which is
    /// replaced by its display prefix.
    pub fn redacted(&self) -> serde_json::Value {
        serde_json::json!({
            "mcp_url": self.mcp_url,
            "url_from": self.url_provenance(),
            "token_prefix": format!("{}…", crate::auth::token_prefix(&self.token)),
            "token_from": self.credential_provenance(),
            "source": self.source,
            "profile": self.profile,
            "expected_team": self.expected.as_ref().map(|e| e.0.clone()),
            "expected_agent": self.expected.as_ref().map(|e| e.1.clone()),
            "tokens_file": self.tokens_file.as_ref().map(|p| p.display().to_string()),
            "token_key": self.token_key,
            "project": self.project,
            "channel": self.channel,
            "project_root": self.project_root.as_ref().map(|p| p.display().to_string()),
            "session": self.session,
            "warnings": self.warnings,
        })
    }
}

fn mcp_url_of(base: &str) -> anyhow::Result<String> {
    let base = crate::admin_cli::normalize_base_url(base)?;
    Ok(format!("{base}/mcp"))
}

/// Read one `name=token` entry from a tokens file.
fn read_token_entry(path: &Path, key: &str) -> anyhow::Result<Option<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            let v = v.trim();
            if v.is_empty() {
                return Ok(None);
            }
            return Ok(Some(v.to_owned()));
        }
    }
    Ok(None)
}

fn none_if_blank(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty())
}

/// Resolve the connection context. See the module docs for the order.
pub fn resolve(inputs: &Inputs) -> anyhow::Result<Resolved> {
    let explicit_url = none_if_blank(inputs.explicit_url.clone());
    let explicit_token = none_if_blank(inputs.explicit_token.clone());
    let explicit_session = none_if_blank(inputs.explicit_session.clone());
    let mut profile_flag = none_if_blank(inputs.profile.clone());
    let host_session = none_if_blank(inputs.host_session.clone());

    // A conversation id fixes the session for every process of that
    // conversation, and the proxy may have recorded which profile it settled
    // on. The record never selects a profile over an explicit one, and never
    // carries a credential.
    let binding = host_session
        .as_deref()
        .and_then(|id| read_binding(&inputs.config_dir, id));
    let session = match (&explicit_session, &host_session) {
        (Some(s), _) => Some(s.clone()),
        (None, Some(id)) => Some(
            binding
                .as_ref()
                .and_then(|b| b.session.clone())
                .unwrap_or_else(|| session_for_host(id)),
        ),
        (None, None) => None,
    };
    if profile_flag.is_none()
        && inputs.explicit_token.is_none()
        && let Some(p) = binding.as_ref().and_then(|b| b.profile.clone())
    {
        profile_flag = Some(p);
    }

    // Project metadata is welcome whatever selects the credentials; a
    // broken project file is reported rather than silently ignored, since
    // it may be the very thing the operator is trying to use.
    let start = match &inputs.project_dir {
        Some(d) => d.clone(),
        None => std::env::current_dir().context("reading the current directory")?,
    };
    let project = find_project(&start)?;
    let (project_root, project_cfg) = match &project {
        Some((root, cfg)) => (Some(root.clone()), cfg.clone()),
        None => (None, ProjectConfig::default()),
    };

    // 1. Explicit credentials win, whole. Two explicit selections at once
    // are a contradiction to report, not a tie to break quietly.
    if let Some(token) = explicit_token {
        if let Some(p) = none_if_blank(inputs.profile.clone()).as_ref() {
            bail!(
                "both explicit credentials (--token / BUS_TOKEN) and a profile ('{p}', from \
                 --profile / BUS_PROFILE) were given; drop one so it is clear which identity \
                 this window uses"
            );
        }
        // The winner is decided; now name what it silently outranked.
        // Leftover exports from a previous release shadowing a freshly
        // configured profile is the normal state of an upgraded machine,
        // and invisible precedence is what made issue #187 cost hours.
        // Precedence itself does not move: this only reports it.
        let token_origin = inputs.token_origin;
        let mut warnings = Vec::new();
        if token_origin == Some(Origin::Environment) {
            let shadowed = match project_cfg.profile.as_deref() {
                Some(p) => Some((p.to_owned(), "the project's .acs.toml names")),
                // A broken profile store must not fail explicit credentials,
                // which never needed it; it just cannot be reported on.
                None => load_profiles(&inputs.config_dir)
                    .ok()
                    .and_then(|p| p.default)
                    .map(|p| (p, "the user default is")),
            };
            if let Some((name, how)) = shadowed {
                warnings.push(format!(
                    "BUS_TOKEN (environment) is overriding profile '{name}' ({how} it): this \
                     window authenticates with the environment token, not the profile. Unset \
                     BUS_TOKEN and BUS_URL to use the profile, or drop the profile if the \
                     override is intended. `ai-crew-sync context verify` shows who each one is."
                ));
            }
        }
        let mcp_url = match explicit_url {
            Some(u) => mcp_url_of(&u)?,
            None => DEFAULT_MCP_URL.to_owned(),
        };
        return Ok(Resolved {
            mcp_url,
            token,
            source: Source::Explicit,
            profile: None,
            expected: None,
            tokens_file: None,
            token_key: None,
            project: project_cfg.project,
            channel: project_cfg.channel,
            project_root,
            session,
            token_origin,
            url_origin: inputs.url_origin,
            warnings,
        });
    }

    let profiles = load_profiles(&inputs.config_dir)?;
    let (name, source) = if let Some(name) = profile_flag {
        (name, Source::ProfileFlag)
    } else if let Some(name) = project_cfg.profile.clone() {
        (name, Source::ProjectDefault)
    } else if let Some(name) = profiles.default.clone() {
        (name, Source::UserDefault)
    } else {
        bail!(
            "no credentials: pass --token / set BUS_TOKEN, select a profile with --profile / \
             BUS_PROFILE, add `profile = \"<name>\"` to {PROJECT_FILE} at the project root, or \
             set a default with `ai-crew-sync context profile default <name>` \
             (profiles: `ai-crew-sync context profile add`)"
        );
    };
    let name = validate_name("profile name", &name)?;
    let Some(profile) = profiles.profiles.get(&name) else {
        let where_from = match source {
            Source::ProfileFlag => "selected with --profile / BUS_PROFILE".to_owned(),
            Source::ProjectDefault => format!(
                "named by {} — a repository may only reference profiles you approved locally",
                project_root
                    .as_ref()
                    .map(|r| r.join(PROJECT_FILE).display().to_string())
                    .unwrap_or_else(|| PROJECT_FILE.to_owned())
            ),
            _ => "set as the user default".to_owned(),
        };
        let known: Vec<&String> = profiles.profiles.keys().collect();
        bail!(
            "profile '{name}' does not exist ({where_from}). Known profiles: {}. Create it with \
             `ai-crew-sync context profile add --name {name} --url <bus> --team <team> \
             --agent <agent> --tokens tokens-<team>`",
            if known.is_empty() {
                "none".to_owned()
            } else {
                known
                    .iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
    };

    // The endpoint comes from the explicit flag or the profile — never from
    // the project file, which cannot even express one.
    let mut warnings = Vec::new();
    let url_origin = explicit_url.is_some().then_some(()).and(inputs.url_origin);
    if let (Some(_), Some(Origin::Environment)) = (&explicit_url, inputs.url_origin) {
        warnings.push(format!(
            "BUS_URL (environment) is overriding profile '{name}'s endpoint: the profile's \
             token will be presented to a different bus. Unset BUS_URL to use the profile's \
             endpoint, or pass --url if the override is intended."
        ));
    }
    let mcp_url = match explicit_url {
        Some(u) => mcp_url_of(&u)?,
        None => mcp_url_of(&profile.url)?,
    };
    let tokens_file = inputs
        .config_dir
        .join(validate_tokens_ref(&profile.tokens)?);
    // Which entry: the project file's key, the project name, the profile's
    // own default, then the shared `_base` line — the same order the
    // per-directory shell wrapper used, so the files it reads keep working.
    let mut candidates: Vec<String> = Vec::new();
    for c in [
        project_cfg.key.clone(),
        project_cfg.project.clone(),
        profile.key.clone(),
    ]
    .into_iter()
    .flatten()
    {
        if !candidates.contains(&c) {
            candidates.push(c);
        }
    }
    candidates.push(BASE_KEY.to_owned());
    let mut found = None;
    for key in &candidates {
        if let Some(token) = read_token_entry(&tokens_file, key)? {
            found = Some((key.clone(), token));
            break;
        }
    }
    let Some((token_key, token)) = found else {
        bail!(
            "profile '{name}': no entry {} in {}. Issue one with `ai-crew-sync admin token issue \
             --team {} --agent {} --save --repo {}`",
            candidates
                .iter()
                .map(|c| format!("'{c}'"))
                .collect::<Vec<_>>()
                .join(" or "),
            tokens_file.display(),
            profile.team,
            profile.agent,
            candidates.first().map(String::as_str).unwrap_or(BASE_KEY)
        );
    };
    if !token.starts_with(TOKEN_PREFIX) {
        bail!(
            "profile '{name}': entry '{token_key}' in {} is not an agent token (expected the \
             {TOKEN_PREFIX} prefix)",
            tokens_file.display()
        );
    }

    Ok(Resolved {
        mcp_url,
        token,
        source,
        profile: Some(name),
        expected: Some((profile.team.clone(), profile.agent.clone())),
        tokens_file: Some(tokens_file),
        token_key: Some(token_key),
        project: project_cfg.project,
        channel: project_cfg.channel,
        project_root,
        session,
        token_origin: None,
        url_origin,
        warnings,
    })
}

/// What the server says the credential is.
#[derive(Clone, Debug, Serialize)]
pub struct Verified {
    pub agent: String,
    pub team: String,
}

/// Present the resolved credential to the bus and require it to be the
/// agent and team the profile expects. Explicit credentials, which promise
/// nothing, are simply reported.
pub async fn verify(resolved: &Resolved) -> anyhow::Result<Verified> {
    // On failure the reader gets what the symptom hides: which endpoint was
    // called and where each piece came from. The one thing never printed is
    // the credential itself.
    let (agent, team) = crate::admin_cli::whoami_on_mcp(&resolved.mcp_url, &resolved.token)
        .await
        .with_context(|| {
            let provenance = format!(
                "endpoint {} came from {}; the credential came from {}",
                resolved.mcp_url,
                resolved.url_provenance(),
                resolved.credential_provenance()
            );
            match &resolved.profile {
                Some(p) => format!(
                    "profile '{p}': the bus at {} did not accept the token. {provenance}. \
                     It may be revoked; issue a new one with `admin token issue --save`",
                    resolved.mcp_url
                ),
                None => format!(
                    "the bus at {} did not accept the token. {provenance}",
                    resolved.mcp_url
                ),
            }
        })?;
    if let Some((exp_team, exp_agent)) = &resolved.expected
        && (&agent != exp_agent || &team != exp_team)
    {
        bail!(
            "profile '{}' expects {exp_agent}@{exp_team} but the token authenticates as \
             {agent}@{team}. The entry '{}' of {} belongs to someone else; fix the profile \
             or replace the entry",
            resolved.profile.as_deref().unwrap_or("?"),
            resolved.token_key.as_deref().unwrap_or("?"),
            resolved
                .tokens_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        );
    }
    Ok(Verified { agent, team })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("acs-ctx-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn seed(dir: &Path) {
        save_profiles(
            dir,
            &Profiles {
                default: Some("acme".into()),
                profiles: BTreeMap::from([
                    (
                        "acme".into(),
                        Profile {
                            url: "https://acme.example:8443".into(),
                            team: "acme".into(),
                            agent: "joaquin".into(),
                            tokens: "tokens-acme".into(),
                            key: None,
                        },
                    ),
                    (
                        "other".into(),
                        Profile {
                            url: "https://other.example".into(),
                            team: "other".into(),
                            agent: "joaquin".into(),
                            tokens: "tokens-other".into(),
                            key: None,
                        },
                    ),
                ]),
            },
        )
        .unwrap();
        std::fs::write(
            dir.join("tokens-acme"),
            "_base=acs_base00000000\napi=acs_api000000000\n",
        )
        .unwrap();
        std::fs::write(dir.join("tokens-other"), "_base=acs_other0000000\n").unwrap();
    }

    fn inputs(dir: &Path, project: &Path) -> Inputs {
        Inputs {
            config_dir: dir.to_path_buf(),
            project_dir: Some(project.to_path_buf()),
            ..Default::default()
        }
    }

    #[test]
    fn explicit_credentials_win_and_keep_project_metadata() {
        let dir = tmp("explicit");
        seed(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(
            repo.join(PROJECT_FILE),
            "profile = \"other\"\nproject = \"api\"\nchannel = \"api\"\n",
        )
        .unwrap();
        let mut i = inputs(&dir, &repo);
        i.explicit_token = Some("acs_explicit".into());
        i.explicit_url = Some("https://x.example/mcp".into());
        let r = resolve(&i).unwrap();
        assert_eq!(r.source, Source::Explicit);
        assert_eq!(r.token, "acs_explicit");
        assert_eq!(r.mcp_url, "https://x.example/mcp");
        assert!(r.expected.is_none(), "explicit credentials promise nothing");
        assert_eq!(r.project.as_deref(), Some("api"));
        i.profile = Some("acme".into());
        let err = resolve(&i).unwrap_err().to_string();
        assert!(err.contains("drop one"), "two explicit selections: {err}");
        assert_eq!(r.channel.as_deref(), Some("api"));
        assert_eq!(
            r.project_root.as_deref(),
            Some(repo.canonicalize().unwrap().as_path())
        );
    }

    #[test]
    fn environment_shadowing_a_profile_is_warned_never_reordered() {
        let dir = tmp("shadow");
        seed(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();

        // An environment token over an installed user default: the token
        // still wins (precedence untouched) and the shadow is named.
        let mut i = inputs(&dir, &repo);
        i.explicit_token = Some("acs_leftover".into());
        i.token_origin = Some(Origin::Environment);
        let r = resolve(&i).unwrap();
        assert_eq!(r.source, Source::Explicit, "precedence must not move");
        assert_eq!(r.token, "acs_leftover");
        assert_eq!(r.warnings.len(), 1, "{:?}", r.warnings);
        assert!(r.warnings[0].contains("BUS_TOKEN (environment)"));
        assert!(
            r.warnings[0].contains("profile 'acme'"),
            "{}",
            r.warnings[0]
        );
        assert!(
            !r.warnings[0].contains("acs_leftover"),
            "a warning never carries the secret"
        );

        // The same token typed as a flag shadows nothing worth warning on:
        // the operator said it out loud.
        let mut i = inputs(&dir, &repo);
        i.explicit_token = Some("acs_leftover".into());
        i.token_origin = Some(Origin::Flag);
        assert!(resolve(&i).unwrap().warnings.is_empty());

        // A project file naming a profile is reported over the user default.
        std::fs::write(repo.join(PROJECT_FILE), "profile = \"other\"\n").unwrap();
        let mut i = inputs(&dir, &repo);
        i.explicit_token = Some("acs_leftover".into());
        i.token_origin = Some(Origin::Environment);
        let r = resolve(&i).unwrap();
        assert!(
            r.warnings[0].contains("profile 'other'"),
            "{}",
            r.warnings[0]
        );

        // No profile anywhere: an explicit token shadows nothing.
        let bare = tmp("shadow-bare");
        let mut i = inputs(&bare, &repo.join("..")); // no seed: empty config
        i.explicit_token = Some("acs_leftover".into());
        i.token_origin = Some(Origin::Environment);
        assert!(resolve(&i).unwrap().warnings.is_empty());
    }

    #[test]
    fn bus_url_from_the_environment_over_a_profile_is_warned() {
        let dir = tmp("shadow-url");
        seed(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();

        let mut i = inputs(&dir, &repo);
        i.explicit_url = Some("https://elsewhere.example".into());
        i.url_origin = Some(Origin::Environment);
        let r = resolve(&i).unwrap();
        assert_eq!(r.mcp_url, "https://elsewhere.example/mcp");
        assert_eq!(r.warnings.len(), 1, "{:?}", r.warnings);
        assert!(r.warnings[0].contains("BUS_URL (environment)"));
        assert!(
            r.warnings[0].contains("profile 'acme'"),
            "{}",
            r.warnings[0]
        );
        assert!(r.url_provenance().contains("BUS_URL (environment)"));

        // The same override typed as a flag is intentional: no warning.
        let mut i = inputs(&dir, &repo);
        i.explicit_url = Some("https://elsewhere.example".into());
        i.url_origin = Some(Origin::Flag);
        assert!(resolve(&i).unwrap().warnings.is_empty());
    }

    #[test]
    fn provenance_names_the_source_without_the_secret() {
        let dir = tmp("provenance");
        seed(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();

        // Profile path: entry, file and what selected the profile.
        let r = resolve(&inputs(&dir, &repo)).unwrap();
        let p = r.credential_provenance();
        assert!(p.contains("entry '_base'"), "{p}");
        assert!(p.contains("tokens-acme"), "{p}");
        assert!(p.contains("profile 'acme'"), "{p}");
        assert!(p.contains("user default"), "{p}");
        assert!(!p.contains("acs_base00000000"), "never the secret: {p}");
        assert!(r.url_provenance().contains("profile 'acme'"));

        // Explicit path: the origin, or the flag when unsaid. The fake
        // token is full-length so the 12-character display prefix does not
        // accidentally equal the whole secret.
        let secret = "acs_explicit0secret0secret0secret0secret";
        let mut i = inputs(&dir, &repo);
        i.explicit_token = Some(secret.into());
        i.token_origin = Some(Origin::Environment);
        let r = resolve(&i).unwrap();
        assert_eq!(r.credential_provenance(), "BUS_TOKEN (environment)");
        assert_eq!(r.url_provenance(), "the built-in default");

        // Serialized view carries the same, still without the secret.
        let view = r.redacted();
        assert_eq!(view["token_from"], "BUS_TOKEN (environment)");
        assert!(!view.to_string().contains(secret));
    }

    #[test]
    fn origin_of_an_unset_variable_is_the_flag() {
        assert_eq!(
            Origin::of("ACS_TEST_UNSET_VARIABLE_187", "acs_x"),
            Origin::Flag
        );
        assert_eq!(
            Origin::Environment.describe("BUS_TOKEN", "--token"),
            "BUS_TOKEN (environment)"
        );
        assert_eq!(Origin::Flag.describe("BUS_URL", "--url"), "--url (flag)");
    }

    #[test]
    fn profile_flag_beats_project_which_beats_user_default() {
        let dir = tmp("precedence");
        seed(&dir);
        let repo = dir.join("repo");
        let nested = repo.join("src").join("deep");
        std::fs::create_dir_all(&nested).unwrap();

        // No project file: the user default.
        let r = resolve(&inputs(&dir, &nested)).unwrap();
        assert_eq!(r.source, Source::UserDefault);
        assert_eq!(r.profile.as_deref(), Some("acme"));
        assert_eq!(r.token_key.as_deref(), Some(BASE_KEY));
        assert_eq!(r.mcp_url, "https://acme.example:8443/mcp");

        // Project file found from a nested directory; its project name is
        // the token key.
        std::fs::write(
            repo.join(PROJECT_FILE),
            "profile = \"acme\"\nproject = \"api\"\n",
        )
        .unwrap();
        let r = resolve(&inputs(&dir, &nested)).unwrap();
        assert_eq!(r.source, Source::ProjectDefault);
        assert_eq!(r.token_key.as_deref(), Some("api"));
        assert_eq!(r.token, "acs_api000000000");
        assert_eq!(r.project.as_deref(), Some("api"));

        // The flag overrides the project file without touching it.
        let mut i = inputs(&dir, &nested);
        i.profile = Some("other".into());
        let r = resolve(&i).unwrap();
        assert_eq!(r.source, Source::ProfileFlag);
        assert_eq!(r.profile.as_deref(), Some("other"));
        assert_eq!(r.token, "acs_other0000000");
        assert_eq!(
            std::fs::read_to_string(repo.join(PROJECT_FILE)).unwrap(),
            "profile = \"acme\"\nproject = \"api\"\n",
            "per-invocation selection never rewrites project defaults"
        );
    }

    #[test]
    fn a_missing_profile_is_an_error_never_another_team() {
        let dir = tmp("missing");
        seed(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join(PROJECT_FILE), "profile = \"stranger\"\n").unwrap();
        let err = resolve(&inputs(&dir, &repo)).unwrap_err().to_string();
        assert!(err.contains("'stranger' does not exist"), "{err}");
        assert!(err.contains("approved locally"), "{err}");
        assert!(err.contains("acme, other"), "{err}");

        let mut i = inputs(&dir, &repo);
        i.profile = Some("nope".into());
        let err = resolve(&i).unwrap_err().to_string();
        assert!(err.contains("--profile"), "{err}");

        // Profile exists, entry does not, no _base either.
        std::fs::write(dir.join("tokens-other"), "web=acs_web\n").unwrap();
        i.profile = Some("other".into());
        let err = resolve(&i).unwrap_err().to_string();
        assert!(err.contains("no entry '_base'"), "{err}");
        assert!(err.contains("admin token issue"), "{err}");
    }

    #[test]
    fn a_repository_may_not_carry_an_endpoint_or_a_credential() {
        let dir = tmp("malicious");
        seed(&dir);
        let repo = dir.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        for evil in [
            "profile = \"acme\"\nurl = \"https://evil.example\"\n",
            "profile = \"acme\"\ntoken = \"acs_stolen\"\n",
            "profile = \"acme\"\ntokens = \"../../etc/passwd\"\n",
            // Not on the forbidden list, and still refused: an unknown key
            // is a claim this version does not understand, and tolerating
            // it would make a credential-shaped one look accepted.
            "profile = \"acme\"\ntoken_file = \"~/.ssh/id_rsa\"\n",
            "profile = \"acme\"\nmcp_url = \"https://evil.example/mcp\"\n",
        ] {
            std::fs::write(repo.join(PROJECT_FILE), evil).unwrap();
            let err = format!("{:#}", resolve(&inputs(&dir, &repo)).unwrap_err());
            assert!(
                err.contains("may not do") || err.contains("does not accept"),
                "{evil}: {err}"
            );
        }
        // A profile whose tokens reference escapes the directory is refused
        // at load time.
        std::fs::write(
            dir.join(PROFILES_FILE),
            "[profiles.bad]\nurl = \"https://x\"\nteam = \"t\"\nagent = \"a\"\ntokens = \"../secrets\"\n",
        )
        .unwrap();
        let err = load_profiles(&dir).unwrap_err().to_string();
        assert!(err.contains("bare file name"), "{err}");
    }

    #[test]
    fn a_linked_worktree_inherits_the_main_worktrees_project_file() {
        let dir = tmp("worktree");
        seed(&dir);
        let main = dir.join("main");
        let wt = dir.join("wt");
        std::fs::create_dir_all(main.join(".git").join("worktrees").join("wt")).unwrap();
        std::fs::create_dir_all(wt.join("src")).unwrap();
        std::fs::write(
            main.join(PROJECT_FILE),
            "profile = \"acme\"\nproject = \"api\"\n",
        )
        .unwrap();
        std::fs::write(
            wt.join(".git"),
            format!("gitdir: {}\n", main.join(".git/worktrees/wt").display()),
        )
        .unwrap();
        std::fs::write(main.join(".git/worktrees/wt/commondir"), "../..\n").unwrap();
        let (root, cfg) = find_project(&wt.join("src"))
            .unwrap()
            .expect("found via worktree");
        assert_eq!(root, main.canonicalize().unwrap());
        assert_eq!(cfg.project.as_deref(), Some("api"));

        // A worktree with its own file uses that one.
        std::fs::write(
            wt.join(PROJECT_FILE),
            "profile = \"acme\"\nproject = \"wt\"\n",
        )
        .unwrap();
        let (root, cfg) = find_project(&wt.join("src")).unwrap().unwrap();
        assert_eq!(root, wt.canonicalize().unwrap());
        assert_eq!(cfg.project.as_deref(), Some("wt"));

        // The search stops at a repository root: a file above it is not ours.
        let other = dir.join("solo");
        std::fs::create_dir_all(other.join(".git")).unwrap();
        std::fs::write(dir.join(PROJECT_FILE), "profile = \"acme\"\n").unwrap();
        assert!(find_project(&other).unwrap().is_none());
    }

    #[test]
    fn concurrent_profile_updates_never_interleave() {
        let dir = tmp("concurrent");
        seed(&dir);
        let handles: Vec<_> = (0..16)
            .map(|i| {
                let dir = dir.clone();
                std::thread::spawn(move || {
                    update_profiles(&dir, |p| {
                        p.profiles.insert(
                            format!("p{i}"),
                            Profile {
                                url: "https://x.example".into(),
                                team: "t".into(),
                                agent: "a".into(),
                                tokens: "tokens-t".into(),
                                key: None,
                            },
                        );
                        Ok(())
                    })
                    .unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let p = load_profiles(&dir).unwrap();
        assert_eq!(
            p.profiles.len(),
            2 + 16,
            "every update landed, none was lost"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join(PROFILES_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
