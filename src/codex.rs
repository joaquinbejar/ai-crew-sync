//! A minimal client for the Codex app-server (#222).
//!
//! Codex runs a local app-server daemon (`codex app-server daemon`) that owns
//! the user's threads and exposes them on a Unix socket. The protocol is
//! JSON-RPC 2.0, one message per WebSocket text frame. This module speaks
//! exactly what the wake adapter needs: `initialize`, `thread/read`,
//! `thread/resume`, `turn/start`, and the notifications that follow. It is
//! a client only, and only over a local Unix socket the user owns: no TCP,
//! no TLS, no authentication of its own.
//!
//! The WebSocket framing is written here rather than pulled in as a crate:
//! the adapter needs text frames, ping, pong and close over one local
//! socket, which is a few dozen lines, and a new dependency needs approval
//! the feature does not justify.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, bail};
use base64::Engine;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

/// Largest frame accepted from the app-server. Thread reads can be large
/// when a caller asks for turns; the adapter never does, so this is a bound
/// against a misbehaving peer, not a limit the protocol imposes.
const MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;
/// How long one request may take before the adapter gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Connecting and upgrading are local and quick; a daemon that accepts and
/// then stalls must not hang the adapter.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Where the Codex app-server daemon listens by default: under
/// `$CODEX_HOME` (else `~/.codex`), as `codex app-server daemon version`
/// reports in `socketPath`.
pub fn default_socket() -> anyhow::Result<PathBuf> {
    let home = match std::env::var_os("CODEX_HOME") {
        Some(h) if !h.is_empty() => PathBuf::from(h),
        _ => crate::admin_cli::home_dir()
            .context("neither CODEX_HOME nor HOME is set")?
            .join(".codex"),
    };
    Ok(home
        .join("app-server-control")
        .join("app-server-control.sock"))
}

/// A connected, initialised app-server client.
pub struct AppServer {
    stream: UnixStream,
    /// Bytes read past the end of the last frame.
    buf: Vec<u8>,
    /// Fragments of a message whose final frame has not arrived yet. Kept
    /// here rather than on the stack so a read cancelled by a timeout loses
    /// nothing: the next read continues the same message.
    partial: Vec<u8>,
    next_id: i64,
    /// Notifications that arrived while waiting for a response, oldest first.
    pending: VecDeque<Value>,
}

/// A JSON-RPC error the app-server returned, kept apart from transport
/// failures so the adapter can tell "Codex refused" from "Codex is gone".
#[derive(Debug, thiserror::Error)]
#[error("{method}: {message}")]
pub struct RpcError {
    pub method: String,
    pub code: i64,
    pub message: String,
}

impl AppServer {
    /// Connect to the daemon's socket, upgrade to WebSocket, and initialise
    /// as `client_name`.
    pub async fn connect(socket: &Path, client_name: &str) -> anyhow::Result<Self> {
        tokio::time::timeout(CONNECT_TIMEOUT, Self::open(socket, client_name))
            .await
            .with_context(|| {
                format!(
                    "the Codex app-server at {} did not answer within {}s",
                    socket.display(),
                    CONNECT_TIMEOUT.as_secs()
                )
            })?
    }

    async fn open(socket: &Path, client_name: &str) -> anyhow::Result<Self> {
        let stream = UnixStream::connect(socket).await.with_context(|| {
            format!(
                "could not reach the Codex app-server at {}; is it running? \
                 (`codex app-server daemon start`)",
                socket.display()
            )
        })?;
        let mut client = Self {
            stream,
            buf: Vec::new(),
            partial: Vec::new(),
            next_id: 1,
            pending: VecDeque::new(),
        };
        client.handshake().await?;
        client
            .request(
                "initialize",
                json!({"clientInfo": {"name": client_name, "version": env!("CARGO_PKG_VERSION")}}),
            )
            .await?;
        client
            .send_json(&json!({"jsonrpc": "2.0", "method": "initialized"}))
            .await?;
        Ok(client)
    }

    async fn handshake(&mut self) -> anyhow::Result<()> {
        let key = base64::engine::general_purpose::STANDARD.encode(rand::random::<[u8; 16]>());
        let request = format!(
            "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        self.stream.write_all(request.as_bytes()).await?;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        // The response head is short; read it byte by byte so no frame byte
        // after it is consumed by mistake.
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() > 16 * 1024 {
                bail!("the Codex app-server sent an oversized handshake response");
            }
            let n = self.stream.read(&mut byte).await?;
            if n == 0 {
                bail!("the Codex app-server closed the connection during the handshake");
            }
            head.push(byte[0]);
        }
        let status = String::from_utf8_lossy(&head);
        if !status.starts_with("HTTP/1.1 101") {
            bail!(
                "the Codex app-server refused the WebSocket upgrade: {}",
                status.lines().next().unwrap_or("")
            );
        }
        Ok(())
    }

    /// Send one JSON-RPC message as a masked text frame (clients mask).
    async fn send_json(&mut self, value: &Value) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(value)?;
        self.send_frame(0x1, &payload).await
    }

    async fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> anyhow::Result<()> {
        let mut frame = Vec::with_capacity(payload.len() + 14);
        frame.push(0x80 | opcode);
        let len = payload.len();
        if len < 126 {
            frame.push(0x80 | len as u8);
        } else if len <= u16::MAX as usize {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        let mask: [u8; 4] = rand::random();
        frame.extend_from_slice(&mask);
        frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    async fn fill(&mut self, want: usize) -> anyhow::Result<()> {
        let mut chunk = [0u8; 16 * 1024];
        while self.buf.len() < want {
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                bail!("the Codex app-server closed the connection");
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
        Ok(())
    }

    /// Read the next complete message: text frames (reassembled across
    /// continuations) are parsed as JSON; pings are answered; a close ends
    /// the connection with an error.
    async fn read_message(&mut self) -> anyhow::Result<Value> {
        loop {
            self.fill(2).await?;
            let fin = self.buf[0] & 0x80 != 0;
            let opcode = self.buf[0] & 0x0f;
            let masked = self.buf[1] & 0x80 != 0;
            let mut len = u64::from(self.buf[1] & 0x7f);
            let mut offset = 2usize;
            if len == 126 {
                self.fill(4).await?;
                len = u64::from(u16::from_be_bytes([self.buf[2], self.buf[3]]));
                offset = 4;
            } else if len == 127 {
                self.fill(10).await?;
                let mut b = [0u8; 8];
                b.copy_from_slice(&self.buf[2..10]);
                len = u64::from_be_bytes(b);
                offset = 10;
            }
            if len > MAX_FRAME_BYTES {
                bail!("the Codex app-server sent a {len}-byte frame; refusing it");
            }
            let mask_len = if masked { 4 } else { 0 };
            let total = offset + mask_len + len as usize;
            self.fill(total).await?;
            let mut payload = self.buf[offset + mask_len..total].to_vec();
            if masked {
                let mask = [
                    self.buf[offset],
                    self.buf[offset + 1],
                    self.buf[offset + 2],
                    self.buf[offset + 3],
                ];
                for (i, b) in payload.iter_mut().enumerate() {
                    *b ^= mask[i % 4];
                }
            }
            self.buf.drain(..total);
            match opcode {
                0x0 | 0x1 => {
                    if self.partial.len() as u64 + payload.len() as u64 > MAX_FRAME_BYTES {
                        bail!("the Codex app-server sent an oversized message; refusing it");
                    }
                    self.partial.extend_from_slice(&payload);
                    if fin {
                        let message = std::mem::take(&mut self.partial);
                        return serde_json::from_slice(&message)
                            .context("the Codex app-server sent a message that is not JSON");
                    }
                }
                0x8 => bail!("the Codex app-server closed the connection"),
                0x9 => self.send_frame(0xA, &payload).await?,
                // Pong, binary or reserved: nothing the adapter uses.
                _ => {}
            }
        }
    }

    /// Send a request and wait for its response. Notifications received in
    /// the meantime are queued for [`Self::next_notification`]. A request
    /// the server sends us (an approval prompt) is answered with an error:
    /// the adapter has no authority to approve anything.
    pub async fn request(&mut self, method: &str, params: Value) -> anyhow::Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.send_json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        let deadline = tokio::time::Instant::now() + REQUEST_TIMEOUT;
        loop {
            let message = tokio::time::timeout_at(deadline, self.read_message())
                .await
                .with_context(|| format!("{method}: no answer from the Codex app-server"))??;
            if message.get("method").is_some() {
                self.route_incoming(message).await?;
                continue;
            }
            if message.get("id").and_then(Value::as_i64) != Some(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(RpcError {
                    method: method.to_owned(),
                    code: error.get("code").and_then(Value::as_i64).unwrap_or(0),
                    message: error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("error")
                        .to_owned(),
                }
                .into());
            }
            return Ok(message.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    /// A notification is queued; a server request is declined.
    async fn route_incoming(&mut self, message: Value) -> anyhow::Result<()> {
        match message.get("id") {
            Some(id) => {
                let id = id.clone();
                self.send_json(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32601, "message": "the ai-crew-sync wake adapter approves nothing"},
                }))
                .await
            }
            None => {
                self.pending.push_back(message);
                Ok(())
            }
        }
    }

    /// The next notification, waiting up to `timeout`. `None` on timeout.
    pub async fn next_notification(&mut self, timeout: Duration) -> anyhow::Result<Option<Value>> {
        if let Some(n) = self.pending.pop_front() {
            return Ok(Some(n));
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let message = match tokio::time::timeout_at(deadline, self.read_message()).await {
                Ok(m) => m?,
                Err(_) => return Ok(None),
            };
            if message.get("method").is_none() {
                continue;
            }
            if message.get("id").is_some() {
                self.route_incoming(message).await?;
                continue;
            }
            return Ok(Some(message));
        }
    }
}

/// A thread's status as `thread/read` and `thread/status/changed` report it:
/// `notLoaded`, `idle`, `active` or `systemError`.
pub fn thread_status(thread_or_notification: &Value) -> Option<&str> {
    thread_or_notification
        .get("status")
        .and_then(|s| s.get("type"))
        .and_then(Value::as_str)
}
