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
#[derive(Clone, Debug, Default)]
pub struct Inputs {
    pub config_dir: PathBuf,
    /// `--url` / `BUS_URL`.
    pub explicit_url: Option<String>,
    /// `--token` / `BUS_TOKEN`.
    pub explicit_token: Option<String>,
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
fn binding_key(host_id: &str) -> String {
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
}

pub fn binding_path(dir: &Path, host_id: &str) -> PathBuf {
    dir.join("sessions")
        .join(format!("{}.json", binding_key(host_id)))
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
    /// `default = "…"` in the profile store.
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
}

impl Resolved {
    /// What `context show` prints: everything but the secret, which is
    /// replaced by its display prefix.
    pub fn redacted(&self) -> serde_json::Value {
        serde_json::json!({
            "mcp_url": self.mcp_url,
            "token_prefix": format!("{}…", crate::auth::token_prefix(&self.token)),
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
    let (agent, team) = crate::admin_cli::whoami_on_mcp(&resolved.mcp_url, &resolved.token)
        .await
        .with_context(|| match &resolved.profile {
            Some(p) => format!(
                "profile '{p}': the bus at {} did not accept the token (entry '{}' of {}). \
                 It may be revoked; issue a new one with `admin token issue --save`",
                resolved.mcp_url,
                resolved.token_key.as_deref().unwrap_or("?"),
                resolved
                    .tokens_file
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            ),
            None => format!("the bus at {} did not accept the token", resolved.mcp_url),
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
