//! `ai-crew-sync admin …` from the operator's own machine.
//!
//! Talks to `/admin/*` with an administrative credential stored by `admin
//! login`, verifies every token it mints by presenting it to `/mcp` and
//! checking `whoami` answers with the agent and team that were asked for,
//! and can write the result straight into the per-team token file.
//!
//! Every function takes the configuration directory explicitly so the
//! integration tests run the real flow against a temporary directory; the
//! binary resolves it once with [`config_dir`].
//!
//! Secrets: the credential is read from a hidden prompt or stdin, never from
//! an argument; a minted token is printed exactly once, or not at all when it
//! is saved to a file. Nothing here logs.

use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::auth::ADMIN_TOKEN_PREFIX;

/// Name of the file holding the endpoint and credential inside the
/// configuration directory.
pub const CONFIG_FILE: &str = "admin";

/// Where `admin login` keeps its state and `--save` writes token files.
/// `BUS_CONFIG_DIR`, else `$XDG_CONFIG_HOME/ai-crew-sync`, else
/// `$HOME/.config/ai-crew-sync`.
pub fn config_dir() -> anyhow::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("BUS_CONFIG_DIR").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(dir));
    }
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(xdg).join("ai-crew-sync"));
    }
    let home = std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .context("neither BUS_CONFIG_DIR, XDG_CONFIG_HOME nor HOME is set")?;
    Ok(PathBuf::from(home).join(".config").join("ai-crew-sync"))
}

/// Endpoint and credential of a logged-in administrator.
#[derive(Clone, Debug)]
pub struct AdminConfig {
    /// Base URL of the bus, without a path: `https://bus.example.com:8443`.
    pub url: String,
    pub token: String,
}

impl AdminConfig {
    pub fn mcp_url(&self) -> String {
        format!("{}/mcp", self.url)
    }
}

/// `https://host:8443`, `https://host:8443/`, `https://host:8443/mcp` and
/// `https://host:8443/admin` all mean the same bus.
pub fn normalize_base_url(raw: &str) -> anyhow::Result<String> {
    let mut url = raw.trim().trim_end_matches('/').to_owned();
    for suffix in ["/mcp", "/admin"] {
        if let Some(stripped) = url.strip_suffix(suffix) {
            url = stripped.trim_end_matches('/').to_owned();
        }
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("--url must start with http:// or https:// (got '{raw}')");
    }
    if url.len() <= "https://".len() {
        bail!("--url has no host (got '{raw}')");
    }
    Ok(url)
}

fn config_path(dir: &Path) -> PathBuf {
    dir.join(CONFIG_FILE)
}

/// Load the stored configuration. `BUS_ADMIN_URL` and `BUS_ADMIN_TOKEN`
/// override the file, for scripts and CI that never run `admin login`.
pub fn load_config(dir: &Path) -> anyhow::Result<AdminConfig> {
    let env_url = std::env::var("BUS_ADMIN_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let env_token = std::env::var("BUS_ADMIN_TOKEN")
        .ok()
        .filter(|v| !v.trim().is_empty());
    if let (Some(url), Some(token)) = (&env_url, &env_token) {
        return Ok(AdminConfig {
            url: normalize_base_url(url)?,
            token: token.trim().to_owned(),
        });
    }

    let path = config_path(dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => bail!(
            "not logged in: run `ai-crew-sync admin login --url <bus>` first \
             (or set BUS_ADMIN_URL and BUS_ADMIN_TOKEN)"
        ),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut url = None;
    let mut token = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.split_once('=') {
            Some(("url", v)) => url = Some(v.trim().to_owned()),
            Some(("token", v)) => token = Some(v.trim().to_owned()),
            _ => {}
        }
    }
    let url = env_url
        .or(url)
        .with_context(|| format!("{} has no url= line; log in again", path.display()))?;
    let token = env_token
        .or(token)
        .with_context(|| format!("{} has no token= line; log in again", path.display()))?;
    Ok(AdminConfig {
        url: normalize_base_url(&url)?,
        token,
    })
}

/// Write `content` to `path` atomically (temp file + rename) with mode 0600.
/// A crash mid-write leaves the previous file intact, never a truncated one.
pub fn write_private(path: &Path, content: &str) -> anyhow::Result<()> {
    let dir = path.parent().context("path has no parent directory")?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let tmp = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        std::process::id()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| -> anyhow::Result<()> {
        let mut f = options
            .open(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // `mode` only applies at creation; a pre-existing temp file from a
            // crashed run keeps its bits otherwise.
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

pub fn save_config(dir: &Path, cfg: &AdminConfig) -> anyhow::Result<PathBuf> {
    let path = config_path(dir);
    write_private(
        &path,
        &format!(
            "# ai-crew-sync administrative credential — written by `admin login`\nurl={}\ntoken={}\n",
            cfg.url, cfg.token
        ),
    )?;
    Ok(path)
}

pub fn remove_config(dir: &Path) -> anyhow::Result<bool> {
    match std::fs::remove_file(config_path(dir)) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

// ------------------------------------------------------------ HTTP client --

/// How many times a throttled (429) call is retried before giving up, and the
/// longest single wait honoured from `Retry-After`.
const MAX_THROTTLE_RETRIES: u32 = 5;
const MAX_THROTTLE_WAIT_SECS: u64 = 5;

/// A logged-in client for `/admin/*`.
pub struct Api {
    http: reqwest::Client,
    cfg: AdminConfig,
}

impl Api {
    pub fn new(cfg: AdminConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            cfg,
        }
    }

    pub fn config(&self) -> &AdminConfig {
        &self.cfg
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> anyhow::Result<Value> {
        let url = format!("{}/admin{path}", self.cfg.url);
        // A 429 is answered before the server does anything, so retrying it
        // is safe for every verb, minting included. Bounded: a script that
        // onboards twenty repositories waits a few seconds, a runaway loop
        // still fails.
        for attempt in 0..MAX_THROTTLE_RETRIES {
            let mut req = self
                .http
                .request(method.clone(), &url)
                .header("Authorization", format!("Bearer {}", self.cfg.token));
            if let Some(body) = &body {
                req = req.json(body);
            }
            let resp = req
                .send()
                .await
                .with_context(|| format!("could not reach {url}"))?;
            let status = resp.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                && attempt + 1 < MAX_THROTTLE_RETRIES
            {
                let wait = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(1)
                    .clamp(1, MAX_THROTTLE_WAIT_SECS);
                tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                continue;
            }
            let text = resp.text().await.unwrap_or_default();
            let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if status.is_success() {
                return Ok(value);
            }
            let msg = value["error"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| text.trim().to_owned());
            bail!("{method} {path} failed ({status}): {msg}");
        }
        unreachable!("the retry loop returns on its last attempt")
    }

    pub async fn whoami(&self) -> anyhow::Result<Value> {
        self.call(reqwest::Method::GET, "/whoami", None).await
    }
    pub async fn list_teams(&self) -> anyhow::Result<Value> {
        self.call(reqwest::Method::GET, "/teams", None).await
    }
    pub async fn create_team(&self, slug: &str, name: Option<&str>) -> anyhow::Result<Value> {
        self.call(
            reqwest::Method::POST,
            "/teams",
            Some(json!({ "slug": slug, "name": name })),
        )
        .await
    }
    pub async fn list_agents(&self, team: &str) -> anyhow::Result<Value> {
        self.call(reqwest::Method::GET, &format!("/teams/{team}/agents"), None)
            .await
    }
    pub async fn create_agent(
        &self,
        team: &str,
        name: &str,
        display_name: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.call(
            reqwest::Method::POST,
            &format!("/teams/{team}/agents"),
            Some(json!({ "name": name, "display_name": display_name })),
        )
        .await
    }
    pub async fn list_tokens(&self, team: &str) -> anyhow::Result<Value> {
        self.call(reqwest::Method::GET, &format!("/teams/{team}/tokens"), None)
            .await
    }
    pub async fn issue_token(
        &self,
        team: &str,
        agent: &str,
        label: Option<&str>,
    ) -> anyhow::Result<Issued> {
        let v = self
            .call(
                reqwest::Method::POST,
                &format!("/teams/{team}/tokens"),
                Some(json!({ "agent": agent, "label": label })),
            )
            .await?;
        let t = &v["token"];
        let field = |k: &str| {
            t[k].as_str()
                .map(str::to_owned)
                .with_context(|| format!("server response has no token.{k}"))
        };
        Ok(Issued {
            id: field("id")?.parse().context("token.id is not a UUID")?,
            token: field("token")?,
            agent: field("agent")?,
            team: field("team")?,
        })
    }
    pub async fn revoke_token(&self, team: &str, id: Uuid) -> anyhow::Result<Value> {
        self.call(
            reqwest::Method::DELETE,
            &format!("/teams/{team}/tokens/{id}"),
            None,
        )
        .await
    }
    pub async fn list_credentials(&self) -> anyhow::Result<Value> {
        self.call(reqwest::Method::GET, "/credentials", None).await
    }
    pub async fn grant_credential(
        &self,
        team: Option<&str>,
        label: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.call(
            reqwest::Method::POST,
            "/credentials",
            Some(json!({ "team": team, "label": label })),
        )
        .await
    }
    pub async fn revoke_credential(&self, id: Uuid) -> anyhow::Result<Value> {
        self.call(reqwest::Method::DELETE, &format!("/credentials/{id}"), None)
            .await
    }
}

/// What `/admin` minted: the secret plus the identity the server says it has.
#[derive(Clone, Debug)]
pub struct Issued {
    pub id: Uuid,
    pub token: String,
    pub agent: String,
    pub team: String,
}

// ----------------------------------------------------------------- login --

/// Read the credential without it touching argv or shell history: from stdin
/// when asked, else from a hidden prompt.
pub fn read_credential(from_stdin: bool) -> anyhow::Result<String> {
    let raw = if from_stdin {
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .context("reading the credential from stdin")?;
        s
    } else {
        rpassword::prompt_password("Administrative credential (acsa_…): ")
            .context("reading the credential from the terminal")?
    };
    let token = raw.trim().to_owned();
    if token.is_empty() {
        bail!("no credential given");
    }
    if !token.starts_with(ADMIN_TOKEN_PREFIX) {
        bail!(
            "that is not an administrative credential (expected the {ADMIN_TOKEN_PREFIX} \
             prefix). Agent tokens cannot administer the bus; mint a credential with \
             `ai-crew-sync admin bootstrap` next to Postgres, or ask a global administrator \
             for `admin grant`"
        );
    }
    Ok(token)
}

/// Verify the credential against the bus and, only then, persist it.
/// Returns the scope the server reported.
pub async fn login(dir: &Path, url: &str, token: String) -> anyhow::Result<(Value, PathBuf)> {
    let cfg = AdminConfig {
        url: normalize_base_url(url)?,
        token,
    };
    let me = Api::new(cfg.clone())
        .whoami()
        .await
        .context("the bus did not accept this credential; nothing was saved")?;
    let path = save_config(dir, &cfg)?;
    Ok((me, path))
}

// ------------------------------------------------------- verify and save --

/// Present a freshly minted token to `/mcp` and return the agent and team it
/// authenticates as. This is the server's word, not the request's.
pub async fn whoami_on_mcp(mcp_url: &str, token: &str) -> anyhow::Result<(String, String)> {
    let mut config = StreamableHttpClientTransportConfig::with_uri(mcp_url.to_owned());
    config.auth_header = Some(token.to_owned());
    config.allow_stateless = true;
    let transport = StreamableHttpClientTransport::from_config(config);
    let client = ClientConfig::default()
        .serve(transport)
        .await
        .context("the new token could not open an MCP session on the bus")?;
    let outcome = async {
        let result = client
            .call_tool(CallToolRequestParams::new("whoami"))
            .await
            .context("whoami failed with the new token")?;
        let v = result
            .structured_content
            .context("whoami returned no structured content")?;
        let agent = v["agent"]
            .as_str()
            .context("whoami has no agent")?
            .to_owned();
        let team = v["team"].as_str().context("whoami has no team")?.to_owned();
        Ok::<_, anyhow::Error>((agent, team))
    }
    .await;
    let _ = client.cancel().await;
    outcome
}

/// Where a saved token goes: `tokens-<team>` in the config directory, as the
/// line `<repo>=<token>`.
#[derive(Clone, Debug)]
pub struct SaveTarget {
    pub dir: PathBuf,
    pub repo: String,
}

/// `--repo` names a line in a file people edit by hand: one word, no `=`,
/// no whitespace, and never a path.
pub fn validate_repo_name(raw: &str) -> anyhow::Result<String> {
    let name = raw.trim();
    if name == "_base" {
        return Ok(name.to_owned());
    }
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !ok {
        bail!(
            "--repo '{raw}' is not a valid entry name: use letters, digits, '-', '_' and '.', \
             starting with a letter or digit (or exactly `_base` for the fallback entry)"
        );
    }
    Ok(name.to_owned())
}

pub fn tokens_file(dir: &Path, team: &str) -> PathBuf {
    dir.join(format!("tokens-{team}"))
}

/// Set `<name>=<token>` in the file, keeping every other line as it is:
/// comments, blank lines, order, and above all `_base`. A duplicate of
/// `name` left by a hand edit collapses to the one updated line. Atomic and
/// 0600, and nothing else in the file is touched — a previous token for the
/// same name is replaced in the file but never revoked on the bus.
pub fn upsert_token_entry(path: &Path, name: &str, token: &str) -> anyhow::Result<()> {
    let existing = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut out = Vec::new();
    let mut replaced = false;
    for line in existing.lines() {
        let is_entry = line.split_once('=').is_some_and(|(k, _)| k.trim() == name);
        if is_entry {
            if !replaced {
                out.push(format!("{name}={token}"));
                replaced = true;
            }
            continue;
        }
        out.push(line.to_owned());
    }
    if !replaced {
        out.push(format!("{name}={token}"));
    }
    let mut content = out.join("\n");
    content.push('\n');
    write_private(path, &content)
}

/// Everything after the mint: verify the token on `/mcp` as exactly the
/// requested agent and team, then either save it (and say nothing of the
/// secret) or print it once. On a mismatch the token is revoked, nothing is
/// written, and the error says what the server answered.
pub async fn finish_issue(
    api: &Api,
    issued: &Issued,
    expected_agent: &str,
    expected_team: &str,
    save: Option<&SaveTarget>,
) -> anyhow::Result<Option<PathBuf>> {
    let expected_agent = expected_agent.trim().to_lowercase();
    let expected_team = expected_team.trim().to_lowercase();
    // Best effort: the token must not stay usable if we cannot vouch for it.
    // A failed revoke is reported alongside, never hidden. Revoked through
    // the team that was asked for: that is the scope the credential has.
    let mismatch = |what: String| {
        let revoke_in = expected_team.clone();
        async move {
            let cleanup = match api.revoke_token(&revoke_in, issued.id).await {
                Ok(_) => "the token has been revoked".to_owned(),
                Err(e) => format!(
                    "and revoking it FAILED ({e}); revoke token {} by hand",
                    issued.id
                ),
            };
            anyhow::anyhow!("{what}; {cleanup}. Nothing was saved or printed.")
        }
    };

    let (agent, team) = match whoami_on_mcp(&api.config().mcp_url(), &issued.token).await {
        Ok(identity) => identity,
        Err(e) => return Err(mismatch(format!("could not verify the new token: {e:#}")).await),
    };
    if agent != expected_agent || team != expected_team {
        return Err(mismatch(format!(
            "the new token authenticates as {agent}@{team}, not {expected_agent}@{expected_team}"
        ))
        .await);
    }
    if issued.agent != agent || issued.team != team {
        return Err(mismatch(format!(
            "the server reported the token as {}@{} but it authenticates as {agent}@{team}",
            issued.agent, issued.team
        ))
        .await);
    }

    match save {
        Some(target) => {
            let path = tokens_file(&target.dir, &team);
            upsert_token_entry(&path, &target.repo, &issued.token)?;
            Ok(Some(path))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_is_normalised_whatever_path_was_pasted() {
        for raw in [
            "https://crew.example.com:8443",
            "https://crew.example.com:8443/",
            "https://crew.example.com:8443/mcp",
            "https://crew.example.com:8443/admin/",
            "  https://crew.example.com:8443/mcp/ ",
        ] {
            assert_eq!(
                normalize_base_url(raw).unwrap(),
                "https://crew.example.com:8443",
                "{raw}"
            );
        }
        assert!(normalize_base_url("crew.example.com").is_err());
        assert!(normalize_base_url("https://").is_err());
    }

    #[test]
    fn repo_names_are_one_safe_word() {
        assert_eq!(validate_repo_name(" backend ").unwrap(), "backend");
        assert_eq!(validate_repo_name("_base").unwrap(), "_base");
        assert_eq!(validate_repo_name("0dte-api.v2").unwrap(), "0dte-api.v2");
        for bad in ["", "_other", "-x", "a b", "a=b", "../etc", "a/b", "ünicode"] {
            assert!(validate_repo_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn upsert_keeps_every_other_line_and_collapses_duplicates() {
        let dir = std::env::temp_dir().join(format!("acs-upsert-{}", Uuid::new_v4()));
        let path = dir.join("tokens-acme");
        upsert_token_entry(&path, "backend", "acs_1").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "backend=acs_1\n");

        std::fs::write(
            &path,
            "# hand-written\n_base=acs_base\nbackend=acs_old\n\nweb=acs_web\nbackend=acs_dup\n",
        )
        .unwrap();
        upsert_token_entry(&path, "backend", "acs_new").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "# hand-written\n_base=acs_base\nbackend=acs_new\n\nweb=acs_web\n"
        );
        upsert_token_entry(&path, "docs", "acs_docs").unwrap();
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .ends_with("web=acs_web\ndocs=acs_docs\n")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
            assert!(
                std::fs::read_dir(&dir).unwrap().count() == 1,
                "no temp file left"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
