//! Durable per-recipient inbox delivery, for conversations routed to a
//! broker.
//!
//! The shape, and why each step exists:
//!
//! * **References, never bodies.** What a recipient is notified of is the
//!   existence of a message. The body is fetched separately, with a current
//!   ACL check at the moment it is read, so a membership that ended between
//!   the notification and the read stops reading.
//! * **One exact subject and one durable consumer per recipient.** Two
//!   recipients cannot compete for each other's references, and one
//!   acknowledgement cannot drain another's inbox.
//! * **Handing over and committing are two steps.** A client is given
//!   references and says, separately, that it holds them durably; only then
//!   is `delivered_at` written and only then is the broker acknowledged. A
//!   crash in between redelivers, and redelivery is idempotent.
//! * **Postgres is the truth.** The broker is how a recipient finds out
//!   quickly. Everything it holds can be rebuilt from `message_receipts`,
//!   which is exactly what happens after an expiry, a deleted consumer, or a
//!   reference that exceeded its redeliveries.
//!
//! What this does **not** do is wake an idle window. Nothing pushes into a
//! model that is not in a turn, on any host we support, and no broker
//! changes that. Delivery here means the reference reached the process, and
//! it is deliberately a different fact from presented, acknowledged and
//! resolved.

use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::AuthCtx,
    error::{BusError, BusResult},
    model::{InboxBatch, InboxReference, InboxState, ts},
    store::{jetstream::JetStreamBackend, routing::AnyBackend},
};

/// References handed out in one call. Bounded so a recipient that has been
/// away cannot pull an unbounded batch into one turn.
pub const MAX_BATCH: i64 = 50;
pub const DEFAULT_BATCH: i64 = 20;

/// Who a reference is for, as a subject-safe, deterministic key.
///
/// Derived from the **logical** identity — the agent and its window's label
/// — rather than from a session row's id, so revoking and re-registering a
/// window (a reinstall, a reconnect) keeps its inbox instead of orphaning
/// it. The label is hex-encoded because a label may legally contain `.` and
/// `:`, which are subject separators.
pub fn recipient_key(agent_id: Uuid, session: &str) -> String {
    if session.is_empty() {
        return format!("a{}", agent_id.simple());
    }
    format!("a{}_{}", agent_id.simple(), hex::encode(session))
}

/// The key for the caller of a tool.
fn caller_key(auth: &AuthCtx) -> String {
    recipient_key(auth.agent_id, &auth.session)
}

/// Queue one reference per recipient of a message whose body is now
/// canonical.
///
/// Called inside the transaction that records the publication, so a stored
/// message cannot exist without its references and a reference cannot exist
/// for a message nobody stored. Idempotent on (kind, message, recipient):
/// a retried publication is the same notification, not a second one.
pub async fn enqueue_message_references(
    tx: &mut sqlx::PgConnection,
    message_id: Uuid,
) -> BusResult<u64> {
    let done = sqlx::query(
        "INSERT INTO inbox_events
            (team_id, recipient_key, kind, message_id, membership_id, payload)
         SELECT c.team_id,
                CASE WHEN m.session = '' THEN 'a' || replace(ag.id::text, '-', '')
                     ELSE 'a' || replace(ag.id::text, '-', '') || '_' || encode(convert_to(m.session, 'UTF8'), 'hex')
                END,
                'message', msg.id, m.id,
                jsonb_build_object(
                    'kind', 'message',
                    'dedup', '',
                    'message_id', msg.id,
                    'conversation_id', c.id,
                    'seq', msg.seq,
                    'from', sender.name,
                    'created_at', msg.created_at)
           FROM message_recipients r
           JOIN conversation_memberships m ON m.id = r.membership_id
           JOIN agents ag ON ag.id = m.agent_id
           JOIN conversation_messages msg ON msg.id = r.message_id
           JOIN conversations c ON c.id = msg.conversation_id
           JOIN agents sender ON sender.id = msg.sender_agent
          WHERE r.message_id = $1
         ON CONFLICT (kind, message_id, recipient_key, dedup) DO NOTHING",
    )
    .bind(message_id)
    .execute(tx)
    .await?;
    Ok(done.rows_affected())
}

/// Queue the sender's notification that a receipt changed, on the same
/// transaction as the receipt itself.
///
/// One row per (message, sender): a recipient that acknowledges and then
/// resolves does not send two notifications, because the sender reads the
/// receipts, not the notification. The notification only says *look again*.
pub async fn enqueue_receipt_reference(
    tx: &mut sqlx::PgConnection,
    message_id: Uuid,
    membership_id: Uuid,
    observation: &str,
) -> BusResult<u64> {
    // The observation is part of the key. Coalescing every receipt change
    // for a message into one row means the sender is told once and never
    // again: a later resolution, or a second recipient answering, conflicts
    // with the row already there and queues nothing.
    let dedup = format!("{membership_id}:{observation}");
    let done = sqlx::query(
        "INSERT INTO inbox_events
            (team_id, recipient_key, kind, message_id, membership_id, payload, dedup)
         SELECT c.team_id,
                CASE WHEN msg.sender_session = '' THEN 'a' || replace(ag.id::text, '-', '')
                     ELSE 'a' || replace(ag.id::text, '-', '') || '_'
                          || encode(convert_to(msg.sender_session, 'UTF8'), 'hex')
                END,
                'receipt', msg.id, $2,
                jsonb_build_object('kind', 'receipt', 'message_id', msg.id,
                                   'conversation_id', c.id, 'seq', msg.seq,
                                   'observation', $3::text, 'dedup', $4::text),
                $4
           FROM conversation_messages msg
           JOIN conversations c ON c.id = msg.conversation_id
           JOIN agents ag ON ag.id = msg.sender_agent
          WHERE msg.id = $1 AND c.backend <> 'postgres'
         ON CONFLICT (kind, message_id, recipient_key, dedup) DO NOTHING",
    )
    .bind(message_id)
    .bind(membership_id)
    .bind(observation)
    .bind(&dedup)
    .execute(tx)
    .await?;
    Ok(done.rows_affected())
}

/// Publish the references a team has queued. Bounded per pass, like every
/// other drainer here.
pub async fn publish_pending(
    pool: &PgPool,
    backend: &JetStreamBackend,
    team_id: Uuid,
    limit: i64,
) -> BusResult<u64> {
    let rows: Vec<(Uuid, String, String, i32)> = sqlx::query_as(
        "SELECT id, recipient_key, payload::text, attempts
           FROM inbox_events
          WHERE team_id = $1 AND state = 'pending' AND next_attempt_at <= now()
          ORDER BY created_at
          LIMIT $2",
    )
    .bind(team_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut published = 0;
    for (id, key, payload, attempts) in rows {
        match backend.publish_reference(&key, id, &payload).await {
            crate::store::backend::Published::Confirmed(_) => {
                sqlx::query("UPDATE inbox_events SET state = 'published' WHERE id = $1")
                    .bind(id)
                    .execute(pool)
                    .await?;
                published += 1;
            }
            crate::store::backend::Published::Retryable(why) => {
                let give_up = attempts + 1 >= crate::store::outbox::MAX_ATTEMPTS;
                sqlx::query(
                    "UPDATE inbox_events
                        SET attempts = attempts + 1,
                            state = CASE WHEN $3 THEN 'failed' ELSE 'pending' END,
                            next_attempt_at = now() + make_interval(secs => $2),
                            last_error = $4
                      WHERE id = $1",
                )
                .bind(id)
                .bind((1_i64 << attempts.min(6)) as f64)
                .bind(give_up)
                .bind(&why)
                .execute(pool)
                .await?;
            }
            crate::store::backend::Published::Fatal(why) => {
                sqlx::query(
                    "UPDATE inbox_events SET state = 'failed', last_error = $2 WHERE id = $1",
                )
                .bind(id)
                .bind(&why)
                .execute(pool)
                .await?;
            }
        }
    }
    Ok(published)
}

/// Hand the caller up to `limit` references. Nothing is acknowledged and no
/// receipt is written here: that is [`confirm`], after the caller says it
/// holds them durably.
pub async fn fetch(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    auth: &AuthCtx,
    limit: Option<i64>,
) -> BusResult<InboxBatch> {
    crate::store::conversations::require_capability(pool, auth).await?;
    // An inbox belongs to a window, and a label in a header is not proof of
    // being one. Without this, the parent agent token could take its own
    // window's references and record them as delivered on its behalf.
    crate::store::sessions::require_window(pool, auth).await?;
    let limit = limit.unwrap_or(DEFAULT_BATCH).clamp(1, MAX_BATCH);
    let key = caller_key(auth);
    let mut references = Vec::new();
    let mut from_broker = 0usize;
    let mut note = None;

    // The broker first, when this team is routed to one and it is reachable.
    match backends.for_team(pool, auth.team_id).await {
        Ok(AnyBackend::JetStream(backend)) => {
            match backend.fetch_references(&key, limit as usize).await {
                Ok(refs) => {
                    for r in refs {
                        match record_broker_reference(pool, auth, &key, &r).await? {
                            Some(reference) => {
                                from_broker += 1;
                                references.push(reference);
                            }
                            // Already confirmed: acknowledge it now rather
                            // than handing the same thing over twice.
                            None => backend.ack_reference(&r.ack_subject).await?,
                        }
                    }
                }
                Err(e) => {
                    // A missing stream or consumer is not an empty inbox.
                    // Say so, and fall through to Postgres, which has it all.
                    note = Some(format!(
                        "the broker could not be read ({e}); these references come from the \
                         bus's own records, which are the authority"
                    ));
                }
            }
        }
        Ok(AnyBackend::Postgres(_)) => {
            note = Some(
                "this team's conversations are on Postgres, where readers are woken by \
                 wait_for_conversation_updates. These references come from the bus's own \
                 records."
                    .to_owned(),
            );
        }
        Err(e) => note = Some(e.to_string()),
    }

    // Top up from Postgres: an expired reference, a deleted consumer or one
    // that ran out of redeliveries is still an undelivered message, and a
    // recipient must not be told its inbox is empty because a cache lost it.
    if (references.len() as i64) < limit {
        let rest = limit - references.len() as i64;
        references.extend(reconcile_from_postgres(pool, auth, &key, rest).await?);
    }
    // Receipt notifications are not in message_receipts — the sender is not
    // a recipient of its own message — so they are rebuilt from the events
    // themselves, or an inbox loss would cost the sender every "look again"
    // it had not yet collected.
    if (references.len() as i64) < limit {
        let rest = limit - references.len() as i64;
        references.extend(reconcile_receipt_events(pool, auth, &key, rest).await?);
    }
    let more = references.len() as i64 >= limit;
    Ok(InboxBatch {
        references,
        from_broker: from_broker as i64,
        more,
        note,
    })
}

/// Record that a broker reference was handed over. `None` means it was
/// already confirmed and needs acknowledging rather than delivering again.
async fn record_broker_reference(
    pool: &PgPool,
    auth: &AuthCtx,
    key: &str,
    r: &crate::store::jetstream::InboxRef,
) -> BusResult<Option<InboxReference>> {
    let payload: serde_json::Value = serde_json::from_str(&r.payload)
        .map_err(|_| BusError::invalid("the broker held a reference this build cannot read"))?;
    let message_id: Uuid = payload
        .get("message_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| BusError::invalid("a reference without a message id"))?;

    let dedup = payload
        .get("dedup")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    // Already confirmed means this is a redelivery of something the holder
    // has, so the answer is an acknowledgement rather than another copy.
    let (confirmed,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM inbox_deliveries
          WHERE recipient_key = $1 AND message_id = $2 AND event_dedup = $3
            AND confirmed_at IS NOT NULL",
    )
    .bind(key)
    .bind(message_id)
    .bind(&dedup)
    .fetch_one(pool)
    .await?;
    if confirmed > 0 {
        return Ok(None);
    }

    let row: Option<(
        Uuid,
        Uuid,
        i64,
        String,
        String,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT m.id, m.conversation_id, m.seq, ag.name, m.sender_session, m.created_at
               FROM conversation_messages m
               JOIN agents ag ON ag.id = m.sender_agent
              WHERE m.id = $1 AND m.deleted_at IS NULL",
    )
    .bind(message_id)
    .fetch_optional(pool)
    .await?;
    // The broker held a reference to something Postgres no longer has: a
    // deleted message. Acknowledging it is the right answer.
    let Some((_, conversation_id, seq, from, from_session, created_at)) = row else {
        return Ok(None);
    };

    let membership: Option<(Uuid,)> = sqlx::query_as(
        "SELECT r.membership_id FROM message_recipients r
           JOIN conversation_memberships cm ON cm.id = r.membership_id
          WHERE r.message_id = $1 AND cm.agent_id = $2 AND cm.session = $3
            AND cm.state = 'active'",
    )
    .bind(message_id)
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_optional(pool)
    .await?;
    // A message reference is for a recipient, and a membership that has
    // ended is not one any more. The reference names a private thread, its
    // sender and when it was sent, so it is not harmless to hand over.
    // A receipt notification is for the sender, who is not a recipient.
    if membership.is_none() && dedup.is_empty() {
        return Ok(None);
    }

    // One open hand-out per reference. A redelivery, or two fetches racing,
    // finds the row that is already out and returns its id rather than
    // making a second one under a different delivery id.
    let (delivery_id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO inbox_deliveries
            (team_id, recipient_key, session_id, epoch, message_id, membership_id,
             ack_subject, stream_seq, event_dedup)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (recipient_key, message_id, event_dedup) WHERE confirmed_at IS NULL
         DO UPDATE SET ack_subject = EXCLUDED.ack_subject,
                       stream_seq = EXCLUDED.stream_seq,
                       session_id = EXCLUDED.session_id,
                       epoch = EXCLUDED.epoch,
                       handed_at = now()
         RETURNING id",
    )
    .bind(auth.team_id)
    .bind(key)
    .bind(auth.session_id)
    .bind(auth.session_epoch)
    .bind(message_id)
    .bind(membership.map(|m| m.0))
    .bind(&r.ack_subject)
    .bind(r.stream_seq as i64)
    .bind(&dedup)
    .fetch_one(pool)
    .await?;

    Ok(Some(InboxReference {
        delivery_id: delivery_id.to_string(),
        message_id: message_id.to_string(),
        conversation_id: conversation_id.to_string(),
        seq,
        from_address: if from_session.is_empty() {
            from.clone()
        } else {
            format!("{from}/{from_session}")
        },
        from,
        created_at: ts(created_at),
        redelivered: r.deliveries > 1,
        source: "broker".to_owned(),
        kind: payload
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_owned(),
    }))
}

/// Everything this recipient has not been delivered, straight from the
/// bus's own records.
async fn reconcile_from_postgres(
    pool: &PgPool,
    auth: &AuthCtx,
    key: &str,
    limit: i64,
) -> BusResult<Vec<InboxReference>> {
    let rows: Vec<(
        Uuid,
        Uuid,
        Uuid,
        i64,
        String,
        String,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT r.membership_id, m.id, m.conversation_id, m.seq, ag.name,
                    m.sender_session, m.created_at
               FROM message_receipts r
               JOIN conversation_memberships cm ON cm.id = r.membership_id
               JOIN conversation_messages m ON m.id = r.message_id
               JOIN agents ag ON ag.id = m.sender_agent
              WHERE cm.agent_id = $1 AND cm.session = $2 AND cm.state = 'active'
                AND r.delivered_at IS NULL
                AND m.deleted_at IS NULL
                AND m.publication_state = 'stored'
                AND NOT EXISTS (
                    SELECT 1 FROM inbox_deliveries d
                     WHERE d.recipient_key = $3 AND d.message_id = m.id
                       AND d.confirmed_at IS NULL
                       AND d.handed_at > now() - interval '5 minutes')
              ORDER BY m.created_at
              LIMIT $4",
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(key)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for (membership_id, message_id, conversation_id, seq, from, from_session, created_at) in rows {
        let (delivery_id,): (Uuid,) = sqlx::query_as(
            "INSERT INTO inbox_deliveries
                (team_id, recipient_key, session_id, epoch, message_id, membership_id)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (recipient_key, message_id, event_dedup) WHERE confirmed_at IS NULL
             DO UPDATE SET handed_at = now()
             RETURNING id",
        )
        .bind(auth.team_id)
        .bind(key)
        .bind(auth.session_id)
        .bind(auth.session_epoch)
        .bind(message_id)
        .bind(membership_id)
        .fetch_one(pool)
        .await?;
        out.push(InboxReference {
            delivery_id: delivery_id.to_string(),
            message_id: message_id.to_string(),
            conversation_id: conversation_id.to_string(),
            seq,
            from_address: if from_session.is_empty() {
                from.clone()
            } else {
                format!("{from}/{from_session}")
            },
            from,
            created_at: ts(created_at),
            redelivered: false,
            source: "bus".to_owned(),
            kind: "message".to_owned(),
        });
    }
    Ok(out)
}

/// Receipt notifications this window has not confirmed, from the events
/// themselves.
async fn reconcile_receipt_events(
    pool: &PgPool,
    auth: &AuthCtx,
    key: &str,
    limit: i64,
) -> BusResult<Vec<InboxReference>> {
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        Uuid,
        String,
        Uuid,
        i64,
        String,
        String,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT e.message_id, e.dedup, m.conversation_id, m.seq, ag.name,
                    m.sender_session, m.created_at
               FROM inbox_events e
               JOIN conversation_messages m ON m.id = e.message_id
               JOIN agents ag ON ag.id = m.sender_agent
              WHERE e.recipient_key = $1 AND e.kind = 'receipt' AND e.state = 'published'
                AND m.deleted_at IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM inbox_deliveries d
                     WHERE d.recipient_key = $1 AND d.message_id = e.message_id
                       AND d.event_dedup = e.dedup
                       AND (d.confirmed_at IS NOT NULL
                            OR d.handed_at > now() - interval '5 minutes'))
              ORDER BY e.created_at
              LIMIT $2",
    )
    .bind(key)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for (message_id, dedup, conversation_id, seq, from, from_session, created_at) in rows {
        let (delivery_id,): (Uuid,) = sqlx::query_as(
            "INSERT INTO inbox_deliveries
                (team_id, recipient_key, session_id, epoch, message_id, event_dedup)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (recipient_key, message_id, event_dedup) WHERE confirmed_at IS NULL
             DO UPDATE SET handed_at = now()
             RETURNING id",
        )
        .bind(auth.team_id)
        .bind(key)
        .bind(auth.session_id)
        .bind(auth.session_epoch)
        .bind(message_id)
        .bind(&dedup)
        .fetch_one(pool)
        .await?;
        out.push(InboxReference {
            delivery_id: delivery_id.to_string(),
            message_id: message_id.to_string(),
            conversation_id: conversation_id.to_string(),
            seq,
            from_address: if from_session.is_empty() {
                from.clone()
            } else {
                format!("{from}/{from_session}")
            },
            from,
            created_at: ts(created_at),
            redelivered: false,
            source: "bus".to_owned(),
            kind: "receipt".to_owned(),
        });
    }
    Ok(out)
}

/// The caller says it holds these references durably. Only now is
/// `delivered_at` written, and only after that is the broker acknowledged.
///
/// Idempotent in both directions: confirming twice changes nothing, and a
/// reference redelivered because an acknowledgement was lost is recognised
/// and acknowledged rather than delivered again.
pub async fn confirm(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    auth: &AuthCtx,
    delivery_ids: &[String],
) -> BusResult<i64> {
    crate::store::conversations::require_capability(pool, auth).await?;
    crate::store::sessions::require_window(pool, auth).await?;
    if delivery_ids.is_empty() {
        return Err(BusError::invalid(
            "nothing to confirm: pass the delivery_id of each reference you hold",
        ));
    }
    let ids: Vec<Uuid> = delivery_ids
        .iter()
        .map(|s| {
            s.trim()
                .parse::<Uuid>()
                .map_err(|_| BusError::invalid("delivery_id must be an id the bus returned"))
        })
        .collect::<BusResult<_>>()?;
    let key = caller_key(auth);

    let mut tx = pool.begin().await?;
    // The window that pulled these is the window that may confirm them, at
    // the epoch it pulled them at: a process that has been resumed away
    // writes nothing.
    crate::store::sessions::guard(&mut tx, auth).await?;
    let rows: Vec<(Uuid, String, Option<Uuid>)> = sqlx::query_as(
        "UPDATE inbox_deliveries
            SET confirmed_at = now()
          WHERE id = ANY($1) AND recipient_key = $2 AND team_id = $3
            AND confirmed_at IS NULL
            AND (epoch IS NULL OR $4::bigint IS NULL OR epoch = $4)
          RETURNING id, ack_subject, membership_id",
    )
    .bind(&ids)
    .bind(&key)
    .bind(auth.team_id)
    .bind(auth.session_epoch)
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        tx.commit().await?;
        return Ok(0);
    }
    let confirmed: Vec<Uuid> = rows.iter().map(|r| r.0).collect();
    // Delivered means the reference reached the process, and nothing more.
    // It never overwrites a stronger observation, and never invents one.
    sqlx::query(
        "UPDATE message_receipts r
            SET delivered_at = COALESCE(r.delivered_at, now())
           FROM inbox_deliveries d
          WHERE d.id = ANY($1)
            AND r.membership_id = d.membership_id
            AND r.message_id = d.message_id",
    )
    .bind(&confirmed)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    // Only now the broker. An acknowledgement lost here costs a redelivery,
    // which the next fetch recognises; acknowledging before the commit would
    // cost the reference itself.
    // The acknowledgement goes to the broker this deployment has, not to
    // whatever the team's *default route* is now: a team routed back to
    // Postgres still has references outstanding on the broker, and they
    // would redeliver for ever.
    let has_ack = rows.iter().any(|(_, subject, _)| !subject.is_empty());
    if has_ack
        && let Ok(AnyBackend::JetStream(backend)) = backends
            .named(
                crate::store::jetstream::JetStreamBackend::NAME,
                auth.team_id,
            )
            .await
    {
        for (_, ack_subject, _) in &rows {
            if let Err(e) = backend.ack_reference(ack_subject).await {
                tracing::warn!(error = %e, "a delivery was committed but not acknowledged");
            }
        }
    }
    Ok(rows.len() as i64)
}

/// What this window's inbox holds, from both sides, with the difference
/// stated rather than averaged.
pub async fn state(
    pool: &PgPool,
    backends: &crate::store::routing::Backends,
    auth: &AuthCtx,
) -> BusResult<InboxState> {
    crate::store::conversations::require_capability(pool, auth).await?;
    crate::store::sessions::require_window(pool, auth).await?;
    let key = caller_key(auth);
    let (undelivered,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM message_receipts r
           JOIN conversation_memberships cm ON cm.id = r.membership_id
           JOIN conversation_messages m ON m.id = r.message_id
          WHERE cm.agent_id = $1 AND cm.session = $2 AND cm.state = 'active'
            AND r.delivered_at IS NULL AND m.deleted_at IS NULL",
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .fetch_one(pool)
    .await?;
    let (unconfirmed,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM inbox_deliveries
          WHERE recipient_key = $1 AND confirmed_at IS NULL",
    )
    .bind(&key)
    .fetch_one(pool)
    .await?;
    let broker = match backends.for_team(pool, auth.team_id).await {
        Ok(AnyBackend::JetStream(backend)) => backend.inbox_status(&key).await.ok(),
        _ => None,
    };
    Ok(InboxState {
        address: if auth.session.is_empty() {
            auth.agent_name.clone()
        } else {
            format!("{}/{}", auth.agent_name, auth.session)
        },
        undelivered,
        handed_out_unconfirmed: unconfirmed,
        broker_pending: broker.as_ref().map(|b| b.pending as i64),
        broker_awaiting_ack: broker.as_ref().map(|b| b.awaiting_ack as i64),
        broker_consumer_present: broker.as_ref().map(|b| b.present),
    })
}
