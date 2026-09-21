//! Small file attachments on messages and tasks, stored as bytea in Postgres.
//!
//! Deliberately capped: the bus shares diffs, logs and small artifacts, not
//! large binaries. One binary, one database — no external object storage.

use base64::Engine;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::AuthCtx,
    error::{BusError, BusResult},
    model::{AttachmentContent, AttachmentMeta, ts},
};

pub const MAX_ATTACHMENT_BYTES: usize = 256 * 1024;
pub const MAX_ATTACHMENTS_PER_TARGET: i64 = 8;

/// A validated, decoded attachment ready to insert.
pub struct NewAttachment {
    pub filename: String,
    pub content_type: String,
    pub data: Vec<u8>,
}

/// Decode and validate user-supplied attachment input before touching the
/// database, so a bad file never leaves a half-written message behind.
pub fn decode_input(
    filename: &str,
    content_type: Option<String>,
    data_base64: &str,
) -> BusResult<NewAttachment> {
    let filename = filename.trim();
    if filename.is_empty() {
        return Err(BusError::invalid("attachment filename cannot be empty"));
    }
    if filename.len() > 255 {
        return Err(BusError::invalid(
            "attachment filename is limited to 255 characters",
        ));
    }
    let data = base64::engine::general_purpose::STANDARD
        .decode(data_base64.trim())
        .map_err(|_| {
            BusError::invalid(format!(
                "attachment '{filename}': data_base64 is not valid base64"
            ))
        })?;
    if data.is_empty() {
        return Err(BusError::invalid(format!(
            "attachment '{filename}' is empty"
        )));
    }
    if data.len() > MAX_ATTACHMENT_BYTES {
        return Err(BusError::invalid(format!(
            "attachment '{filename}' is {} bytes; the limit is {MAX_ATTACHMENT_BYTES} \
             (share a link or trim the file instead)",
            data.len()
        )));
    }
    let content_type = content_type
        .map(|c| c.trim().to_owned())
        .filter(|c| !c.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_owned());
    Ok(NewAttachment {
        filename: filename.to_owned(),
        content_type,
        data,
    })
}

/// Insert one attachment row for a message. Runs inside the caller's
/// transaction so message + attachments commit (and notify) together.
pub async fn insert_for_message(
    conn: &mut sqlx::PgConnection,
    team_id: Uuid,
    message_id: i64,
    uploader: Uuid,
    att: &NewAttachment,
) -> BusResult<AttachmentMeta> {
    super::quota::check_attachment_quota(conn, team_id, att.data.len() as i64).await?;

    let (id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO attachments
            (team_id, message_id, uploader_agent_id, filename, content_type, size_bytes, data)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        "#,
    )
    .bind(team_id)
    .bind(message_id)
    .bind(uploader)
    .bind(&att.filename)
    .bind(&att.content_type)
    .bind(att.data.len() as i64)
    .bind(&att.data)
    .fetch_one(conn)
    .await?;
    Ok(AttachmentMeta {
        id,
        filename: att.filename.clone(),
        content_type: att.content_type.clone(),
        size_bytes: att.data.len() as i64,
    })
}

/// Attach a file to a task (any teammate may: reviewers add logs and diffs).
pub async fn attach_to_task(
    pool: &PgPool,
    auth: &AuthCtx,
    task_key: &str,
    att: &NewAttachment,
) -> BusResult<AttachmentMeta> {
    let task_key = task_key.trim();
    let task: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM tasks WHERE team_id = $1 AND key = $2")
            .bind(auth.team_id)
            .bind(task_key)
            .fetch_optional(pool)
            .await?;
    let (task_id,) = task.ok_or_else(|| BusError::not_found(format!("task '{task_key}'")))?;

    // The cap, the quota and the insert share one transaction, under the
    // task's row lock: two uploads racing for the last slot cannot both see
    // room and both commit, any more than two racing for the last megabyte.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT id FROM tasks WHERE id = $1 FOR UPDATE")
        .bind(task_id)
        .fetch_one(&mut *tx)
        .await?;
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM attachments WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(&mut *tx)
        .await?;
    if count >= MAX_ATTACHMENTS_PER_TARGET {
        return Err(BusError::invalid(format!(
            "task '{task_key}' already has {MAX_ATTACHMENTS_PER_TARGET} attachments; \
             delete-and-recreate is not supported, start a new task or link externally"
        )));
    }
    super::quota::check_attachment_quota(&mut tx, auth.team_id, att.data.len() as i64).await?;

    let (id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO attachments
            (team_id, task_id, uploader_agent_id, filename, content_type, size_bytes, data)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        "#,
    )
    .bind(auth.team_id)
    .bind(task_id)
    .bind(auth.agent_id)
    .bind(&att.filename)
    .bind(&att.content_type)
    .bind(att.data.len() as i64)
    .bind(&att.data)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(AttachmentMeta {
        id,
        filename: att.filename.clone(),
        content_type: att.content_type.clone(),
        size_bytes: att.data.len() as i64,
    })
}

/// Attach a file to a message you sent earlier. Only the sender may add to
/// their own message; prefer attaching at post_message time so readers never
/// see the message without its files.
pub async fn attach_to_message(
    pool: &PgPool,
    auth: &AuthCtx,
    message_id: i64,
    att: &NewAttachment,
) -> BusResult<AttachmentMeta> {
    let row: Option<(Uuid,)> =
        sqlx::query_as("SELECT sender_agent_id FROM messages WHERE id = $1 AND team_id = $2")
            .bind(message_id)
            .bind(auth.team_id)
            .fetch_optional(pool)
            .await?;
    let (sender,) = row.ok_or_else(|| BusError::not_found(format!("message {message_id}")))?;
    if sender != auth.agent_id {
        return Err(BusError::invalid(
            "you can only attach files to your own messages",
        ));
    }

    // Same rule as a task: the cap is decided under the message's row lock,
    // in the transaction that inserts.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT id FROM messages WHERE id = $1 FOR UPDATE")
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM attachments WHERE message_id = $1")
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
    if count >= MAX_ATTACHMENTS_PER_TARGET {
        return Err(BusError::invalid(format!(
            "message {message_id} already has {MAX_ATTACHMENTS_PER_TARGET} attachments"
        )));
    }
    let meta = insert_for_message(&mut tx, auth.team_id, message_id, auth.agent_id, att).await?;
    tx.commit().await?;
    Ok(meta)
}

/// Fetch an attachment's content. Team-scoped; attachments on direct messages
/// are only visible to the DM's sender and recipient.
pub async fn get_attachment(
    pool: &PgPool,
    auth: &AuthCtx,
    id: i64,
) -> BusResult<AttachmentContent> {
    let row: Option<(
        String,
        String,
        i64,
        Vec<u8>,
        String,
        chrono::DateTime<chrono::Utc>,
        Option<Uuid>,
        Option<Uuid>,
    )> = sqlx::query_as(
        r#"
        SELECT a.filename, a.content_type, a.size_bytes, a.data, u.name, a.created_at,
               m.recipient_agent_id, m.sender_agent_id
        FROM attachments a
        JOIN agents u ON u.id = a.uploader_agent_id
        LEFT JOIN messages m ON m.id = a.message_id
        WHERE a.id = $1 AND a.team_id = $2
        "#,
    )
    .bind(id)
    .bind(auth.team_id)
    .fetch_optional(pool)
    .await?;

    let (filename, content_type, size_bytes, data, uploaded_by, created_at, dm_recipient, sender) =
        row.ok_or_else(|| BusError::not_found(format!("attachment {id}")))?;

    // DMs never leave their two parties, and neither do their files.
    if let Some(recipient) = dm_recipient
        && recipient != auth.agent_id
        && sender != Some(auth.agent_id)
    {
        return Err(BusError::not_found(format!("attachment {id}")));
    }

    Ok(AttachmentContent {
        id,
        filename,
        content_type,
        size_bytes,
        uploaded_by,
        created_at: ts(created_at),
        data_base64: base64::engine::general_purpose::STANDARD.encode(data),
    })
}
