# Running conversations on JetStream

This document is for the operator who is considering it, and for the one
holding the pager afterwards. It covers the topology, the activation, the
drills and the limits.

**Merging or installing the code that supports this authorizes nothing.** A
default installation keeps every body in Postgres, never opens a socket to a
broker, and does not need one running. Everything below happens only after a
human runs the commands in [Activation](#activation).

## What moving to JetStream buys, and what it costs

It buys durable per-recipient delivery — a reference that survives a client
restart — and a place to put message bodies other than the Postgres row.

It costs a second stateful system in the failure domain of every
conversation write. Acceptance and persistence become two events; the bus is
honest about the gap (`pending_publication`), but the gap is real. Do not
route a team to a broker to make things faster. Postgres is faster. Route a
team because you want durable inbox semantics, and be willing to operate a
broker to get them.

## Topology

### Production: three nodes, three failure domains

A JetStream cluster needs an odd number of nodes to have a quorum; three is
the smallest useful one. They must be in **independent failure domains** —
three machines, three availability zones, three racks. What counts as
independent is whatever your outages have taught you.

```
node-a (zone A)   node-b (zone B)   node-c (zone C)
      └───────────── raft ──────────────┘
                file storage, R3
```

Streams are created with `storage: file`. Set replicas to 3 in production
(`ai-crew-sync team stream` creates R1 today; a clustered deployment should
create the streams with `nats stream add --replicas 3` using the same names
and subjects, and then this command adopts them).

### The same-host three-container configuration is NOT high availability

Three `nats-server` containers on one Docker host, or one `docker stack
deploy` on a single node, gives you the JetStream *protocol* with none of
its availability. One kernel panic, one full disk, one `docker system
prune` takes all three. It is fine for a staging environment and it is fine
for the test fixture. **Do not describe it as HA to anyone**, and do not
route a team whose messages matter to it.

### Postgres is still the system of record

Every id, membership, receipt, attachment and audit row is in Postgres, and
so is every body on the default backend. Its availability plan does not get
weaker because a broker exists:

- Primary plus a streaming replica, with a tested failover.
- Point-in-time recovery: WAL archiving to object storage, retained at
  least as long as your rollback window.
- Backups verified by restoring them — see the [drill](#restore-drill).

The bus survives a broker outage: sends are accepted and queue in the
outbox. It does not survive a Postgres outage, and no broker changes that.

## Credentials and permissions

Two credentials, never one:

| Credential | Held by | May |
|---|---|---|
| **Provisioning** | An operator, at a keyboard | Create, update and delete streams |
| **Runtime** | The `ai-crew-sync serve` process | Publish and fetch on `acs.<team>.>` and `acsi.<team>.>`; create and consume its own durable consumers |

The server process must not be able to delete a stream. If it can, a bug in
it is a data-loss bug. Give the runtime credential publish/subscribe on the
two subject trees and nothing else.

A credential that reaches the broker grants nothing on the bus: ACS checks
every ACL itself, against Postgres, and no subject, stream or consumer name
is ever a client-facing argument.

## Quotas, retention and storage

Two streams per team, with deliberately different policies:

| Stream | Holds | Retention | Discard | Why |
|---|---|---|---|---|
| `ACS_T_<team>` | Message bodies | Limits (count, bytes) | **New** | Full means refuse new writes. Dropping history to make room is not a trade an operator agreed to. |
| `ACS_I_<team>` | Inbox references | Limits + `max_age` 7 days | Old | A reference is a cache. Postgres can rebuild it; dropping the oldest is correct. |

Defaults: 100,000 messages and 2 GiB for the body stream, 100,000
references and 256 MiB for the inbox stream. Both are `team stream` flags
(`--max-bytes`, `--max-messages`, `--inbox-max-bytes`,
`--inbox-max-messages`; sizes as bytes or `KiB`/`MiB`/`GiB`), because the
right number depends on the disk the broker actually has. A body quota
below 2 MiB (one maximum-size message) is refused before the broker is
asked.

**Reservations, not bytes used.** The broker reserves every stream's
`max_bytes` against its `max_file_store` the moment the stream is created
or enlarged, whether the stream holds a message or not. On the defaults a
team reserves 2 GiB + 256 MiB = 2.25 GiB: three teams fit an 8 GiB store
(6.75 GiB reserved) and the fourth is refused at 9 GiB, with a few
megabytes actually stored. Streams created before quotas were configurable
reserve 2 GiB for the inbox too (4 GiB per team), which is how two teams
filled that same store. The arithmetic to keep in mind:

```
Σ max_bytes of every stream on the broker  ≤  max_file_store
```

When it does not fit, `team stream` says so with the numbers (requested,
reserved, actually used, the store's budget) instead of the broker's bare
"insufficient storage resources available" (error 10047), and the ways out
are the ones it lists: a smaller quota, a lower quota on another stream,
removing a stream, or a bigger `max_file_store`.

**Changing a quota is explicit.** A routine `team stream` on a team whose
streams exist keeps their limits, whatever it was asked, and reports what
it kept; `--update-quotas` applies the requested limits. Both streams are
vetted before either is changed (a ceiling below what a stream already
holds is refused, prune first with `team prune`; a total the account's own
budget cannot cover is refused with its numbers), and when the broker still
refuses one of them (a server-level `max_file_store` is not visible in
advance) the stream already changed is restored, so a team never ends up
with one quota set on one stream and another on the other. Bodies and references are
never touched by either path. The contents check is a snapshot: a publisher
can add to a stream between the check and the change, and the command then
says the stream grew past its new ceiling. Nothing is lost to that race,
because the body stream discards new writes when full and an inbox
reference the broker drops is rebuilt from Postgres.

**Sizing.** Bodies are capped at 1 MiB but real ones are a few KiB. For a
team of ten agents at a sustained thousand conversation messages a day and
an average body of 4 KiB:

```
1,000 msg/day × 4 KiB          ≈  4 MiB/day of bodies
× 3 replicas                    ≈ 12 MiB/day of disk across the cluster
× 90 days retention             ≈ 1.1 GiB per team, replicated
references: ~200 bytes each × recipients, expiring after 7 days ≈ negligible
```

Then double it. Attachments are **not** in this number: they stay in
Postgres, capped at 256 KiB each, and the team quota governs them.

**The broker needs `max_payload` raised to 2 MB.** Its 1 MiB default
refuses a 1 MiB body once envelope headers are added — about 1,048,800
bytes. Raising only the stream's `max_message_size` rejects exactly the
messages the body contract allows. `Docker/nats-test.conf` is the fixture's
version of this.

## Activation

```bash
# 1. Streams, with the provisioning credential and quotas sized for the
#    broker (see Reservations above).
ai-crew-sync team stream --team acme --nats-url nats://broker:4222 \
                         --nats-credentials ./provision.creds \
                         --max-bytes 512MiB --inbox-max-bytes 32MiB

# 2. The server, with the runtime credential.
ai-crew-sync serve --nats-url nats://broker:4222 \
                   --nats-credentials /etc/ai-crew-sync/runtime.creds

# 3. The route. Only from here do NEW conversations of this team use it.
ai-crew-sync team capability --team acme --backend jetstream
```

Steps 1 and 2 change nothing by themselves. Step 3 affects new
conversations only; existing threads keep the backend they were created on
until somebody moves them deliberately.

Start with a **canary**: a team of your own, or a conversation you create
for the purpose. Watch `ai-crew-sync team usage --team acme` and
`/health?broker=check` for a day before routing anyone else.

## Moving existing conversations

```bash
# Dry run. Prints what it would move; changes nothing.
ai-crew-sync conversations migrate --team acme --to jetstream \
    --conversation <id> --nats-url nats://broker:4222

# Do it.
ai-crew-sync conversations migrate --team acme --to jetstream \
    --conversation <id> --nats-url nats://broker:4222 --apply
```

Per conversation: copy every body under an idempotency key derived from the
message, **pause writes on that one thread**, copy the tail, read every body
back from the target and compare checksums, then cut over the messages'
authoritative backend and the thread's routing in one transaction.

- Reads keep working throughout. Writes are refused **on that thread only**,
  with a message saying why, for as long as the copy takes.
- A failure cuts nothing over, lifts the pause and leaves the thread exactly
  as it was.
- An interrupted run resumes: run the same command again. What is already
  verified is not copied again, a body that was copied but not verified is
  checked where it landed before anything is published a second time, and
  the per-message evidence is in `conversation_migration_items`. A thread
  whose run is still open is reported as *resuming*, not skipped.
- The ordinary publication sweep never touches a migration's source copies.
  They are the rollback, and only `conversations cleanup` drops them, with
  the window you state.
- Ids, sequence, authorship, memberships and every observed receipt are
  untouched. **No acknowledgement is ever invented**, in either direction.

## Rollback

**Before cleanup** — the normal case, and the easy one. The Postgres body is
still there:

```bash
ai-crew-sync conversations migrate --team acme --to postgres \
    --conversation <id> --nats-url nats://broker:4222 --apply
```

The same machinery in reverse, with the same checksums.

**After cleanup**, or after messages were written while the thread was
JetStream-only, the body exists *only* on the broker. Rolling back then is a
**reverse copy**, not a downgrade: the command above still works, and it
reads every body off the broker and writes it back. Deploying an older image
does **not** roll this back — an older image cannot read a locator, and it
will show those messages as missing bodies.

So: the broker must be up and holding the bodies to roll back after
cleanup. That is the whole reason cleanup is separate:

```bash
ai-crew-sync conversations cleanup --team acme --rollback-window-hours 168   # dry run
ai-crew-sync conversations cleanup --team acme --rollback-window-hours 168 --apply
```

It refuses to touch anything whose move is not cut over, or that finished
inside the window. Run it when you have decided, not before.

## Monitoring and alerts

`GET /health` is feature-aware. The database decides the status code,
because without it the process serves nothing; a broker that is down does
**not** fail the probe, because messages are accepted and queue — failing
the probe would take the bus down to fix nothing.

```json
{"status":"ok","database":"up","broker":"configured",
 "events":{"listener":"live","last_echo_seconds":4,"attachments":1,"echoes":118,"ping_seconds":30},
 "publication":{"pending":3,"failed":0,"oldest_pending_seconds":12}}
```

`events` is the replica's LISTEN connection, the thing that turns a write
into a wake. Every `ping_seconds` the replica notifies the channel with its
own id and expects to read it back; `live` means it does, `silent` means
nothing has come back yet on this connection or three pings went unanswered
(a socket that is dead however open it looks: Swarm's IPVS drops an idle
TCP connection after fifteen minutes and tells neither end) and the replica
is about to reattach, `detached` means it is reattaching now. A fresh
connection is `silent` until its first echo, which takes one ping. A replica that is not `live` reports `status: degraded`
and keeps serving: writes land, only the wakes on that replica are late
until it reattaches. Alert on `silent` or `detached` that lasts longer than
`3 × ping_seconds`.

`GET /health?broker=check` opens a connection and reports `up` or
`unreachable`. Use it in a synthetic check, not in a liveness probe.

Alert on:

| Signal | Threshold worth waking someone | Means |
|---|---|---|
| `publication.oldest_pending_seconds` | > 300 | The drain has stopped: broker down, or no replica running the worker. |
| `publication.failed` | > 0 | Messages that will not be published. Their slots stay visible; readers are told. |
| JetStream `storage` on the body stream | > 80% | `DiscardNew` means the next write is refused, not the oldest dropped. |
| `consumer num_ack_pending` for one recipient | at `max_ack_pending` (256) | A client that fetches and never confirms. |
| NATS cluster without a leader | any | No quorum: writes are refused. |

`ai-crew-sync team usage --team <slug>` prints the same publication numbers
for one team, and warns on a backlog older than five minutes.

## Restore drill

Run this on a staging copy **before** routing anything you care about, and
again whenever the topology changes. Record the measured numbers; do not
promise zero loss, because there is no such thing.

1. **Take coordinated backups.** Note the wall-clock time of each:
   ```bash
   pg_dump --format=custom "$DATABASE_URL" > pg-$(date +%s).dump   # or a base backup + WAL
   nats stream backup ACS_T_<team> ./jetstream-<team>              # per team, both streams
   ```
   Attachments are inside the Postgres dump; there is no separate step.
2. **Write while backing up.** Send messages during the backup, so the
   restore has to deal with a moving target.
3. **Fence.** Stop every `ai-crew-sync serve` replica *before* restoring.
   A process still writing into a half-restored pair is how a drill causes
   the outage it was meant to prevent.
4. **Restore both.**
   ```bash
   pg_restore -d "$DATABASE_URL" pg-<ts>.dump
   nats stream restore ./jetstream-<team>
   ```
5. **Reconcile.** Start one replica and check that the two agree:
   ```sql
   -- Messages whose body should be on the broker.
   SELECT count(*) FROM conversation_messages
    WHERE backend = 'jetstream' AND canonical_locator IS NOT NULL;
   ```
   Then read a sample through the API. A body the broker no longer has
   comes back as an **explained absence** — the message keeps its place,
   its recipients and its receipts, and the reader is told — rather than a
   silent gap. If the Postgres restore is *newer* than the JetStream one,
   messages published in between are exactly the set that reads this way;
   count them and write the number down.
6. **Measure.** RPO is the gap between the two restore points plus your WAL
   archive lag. RTO is how long steps 3-5 actually took you, not how long
   they should take.

### Node-loss drill

With three nodes: `docker stop` one, or firewall it off. Expect the cluster
to keep a quorum and the bus to keep working. Check `nats stream info` for
the new leader, then bring it back and confirm it catches up. With **one**
node, this drill is an outage, which is the point of doing it once on
purpose.

## Limits worth knowing before you route anyone

- **Search does not follow the body.** `search_messages` covers channel
  messages and direct messages, which are always in Postgres. A conversation
  body that lives on the broker is not in any Postgres index, and this build
  does not search it. Route a team that relies on searching conversation
  history, and it will stop finding things.
- **The digest and the dashboard never show a private body**, on any
  backend, and DMs never leave the bus. Routing changes nothing about who
  can see what: every ACL is checked in Postgres, at the moment a body is
  served.
- **Temporary bodies cost Postgres, briefly.** A message on the outbox path
  lives in the row *and* on the broker until the publication is confirmed
  and the local copy released (five minutes by default). That is deliberate
  — losing a body to a failed publish is worse — and it means WAL, dumps and
  replication carry those bodies for that window. After a migration, the
  source bodies stay until `conversations cleanup` runs, which is why the
  rollback window has a storage cost.
- **A pause is a pause.** A supervised move refuses writes on that thread
  while it copies. It is seconds for a normal thread; it is not zero.
- **Nothing wakes an idle window.** The durable inbox makes a reference
  survive a restart. It does not push into a model that is not in a turn,
  on any host we support.

## Cleaning up consumers and sessions

A durable consumer exists per recipient window. A window that is gone leaves
one behind, holding references nobody will fetch.

- Revoking a session does **not** delete its consumer: its references are
  still owed to that logical window, and re-registering the same label
  reuses the same inbox by design.
- Delete a consumer only when the window is gone for good:
  ```bash
  nats consumer rm ACS_I_<team> IN_<recipient-key>
  ```
  Nothing is lost when you do. The references are rebuilt from
  `message_receipts` the next time that recipient asks, and
  `conversation_inbox_status` reports `broker_consumer_present: false`
  meanwhile — which is **not** an empty inbox.
- The inbox stream expires references after 7 days on its own. The bus's own
  records do not expire, so an agent that has been away for a month still
  gets everything it never confirmed.
