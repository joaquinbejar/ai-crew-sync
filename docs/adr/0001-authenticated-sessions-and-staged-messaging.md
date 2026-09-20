# ADR 0001: authenticated sessions and staged messaging storage

Status: accepted for implementation planning by the owner in the 2026-09-20 design discussion. No runtime implementation or production activation is implied.

> **Editorial note (2026-09-20), not part of the accepted decision.** The text
> below is published verbatim as accepted, and it was written against a
> working checkout that already held unmerged migrations. At the time this
> document lands on `main`, the last applied migration is
> `0011_admin_credentials.sql`; `0012_session_discovery.sql` and
> `0013_authenticated_sessions.sql` arrive with the phase 1 work. Take every
> migration number in this ADR as illustrative and read `migrations/` for the
> next free one when you implement.

Public decision record: [issue #93](https://github.com/joaquinbejar/ai-crew-sync/issues/93). Phase issues: [#94](https://github.com/joaquinbejar/ai-crew-sync/issues/94), [#95](https://github.com/joaquinbejar/ai-crew-sync/issues/95), [#96](https://github.com/joaquinbejar/ai-crew-sync/issues/96), [#97](https://github.com/joaquinbejar/ai-crew-sync/issues/97), [#98](https://github.com/joaquinbejar/ai-crew-sync/issues/98), [#99](https://github.com/joaquinbejar/ai-crew-sync/issues/99) and [#100](https://github.com/joaquinbejar/ai-crew-sync/issues/100).

## Context

Several Claude and Codex conversations share a repository and sometimes an agent token. X-Crew-Session currently partitions work using a caller-controlled label; it does not prove ownership of a private session. Operators also need conversations, participant-specific acknowledgment and continuity when windows close.

The initial NATS design made useful behavior depend on a complete infrastructure migration. The expected workload does not establish a capacity requirement for a broker. The owner selected seven independently deployable phases, with the complete conversation experience delivered on Postgres before introducing NATS.

## Decisions

### Identity and session lifecycle

Add an opaque session credential derived from an existing agent token. Store only its hash, parent token, expiry/revocation and connection epoch in the sessions table. Default access lifetime is 24 hours. Registration derives agent and team from the parent token; clients cannot assert them. A session credential cannot create administrative or agent credentials. Renewal/resume requires the existing proof and an active parent credential. A revoked/disabled parent invalidates child access.

Server-issued sessions and epoch fencing bind reconnects to the right host conversation. Forks receive new sessions. A header label alone never authorizes private conversation access. Legacy headers and direct HTTP MCP retain their existing behavior for existing tools. Application session state does not introduce MCP transport state.

### Hooks and local secrets

Authenticated hook mode requires the ACS binary. Hooks call `ai-crew-sync context hook` using an opaque host binding. The binary reads the private binding/credential files and performs the authenticated operation, returning only the hook response. It does not print credentials or pass them through argv.

This is an explicit exception to the existing curl/python3-only hook rule. Preserve that dependency set for legacy mode. Ship the binary-backed path, startup ordering, concurrent epoch coordination and missing-binding behavior in phase 1, not in the final host-integration phase. Do not silently fall back to another session or team.

Use 0700 state directories and 0600 files. Credentials stay out of model-facing results, logs and repository configuration. These permissions do not isolate hostile processes running as the same OS user with unrestricted shell access; stronger isolation needs separate sandboxes/credentials.

### Conversation access and continuity

Conversations are project-visible or private within one team. Project access is an explicit grant, not a repository path or descriptive role. Private membership is normally session-specific, with immutable visibility and bounded history grants.

Support explicit, audited membership transfer to an authenticated session of the same agent/team/project. Target acceptance atomically supersedes the old membership. Preserve authorship, historical recipients and receipts; use a linked successor obligation for unfinished work rather than acknowledging as the old session.

Accept a read-only agent recovery exception when all of that agent's sessions in the team are server-confirmed closed, revoked or expired. Offline presence alone is insufficient. Authenticate the owner with an active agent credential and restrict history to its prior non-revoked membership and current project access. Explicit removal/bans remain effective. Recovery does not grant broader membership, publish messages or forge receipts, and every access is audited.

Serialize recovery authorization with registration/resume. A bounded, one-use same-agent handoff grant may bridge recovery to a newly opened session. An audited moderator/operator handoff can resolve other orphan cases without exposing message bodies to administrative credentials. Document that privacy is not isolation from the owning agent's explicit recovery rights.

### Storage and delivery

Phase 2 commits conversation body, sequence, recipient snapshot, audit and receipt state together in Postgres. Existing EventHub/LISTEN/NOTIFY wakes readers; durable queries reconcile missed wakeups. No NATS dependency or publication outbox is needed for that release.

Phase 3 adds a backend trait with a Postgres implementation and a gated asynchronous outbox route for reliability testing. Synchronous Postgres remains supported. Phase 4 adds a pinned NATS runtime and Rust client for synthetic test teams. Phase 5 enables explicit per-conversation JetStream body storage and phase 6 enables inbox fanout. No automatic production cutover occurs merely by installing a release.

For the JetStream route, keep authorization, recipients, receipts, metadata, leases, notes and audit in Postgres. Store published bodies in JetStream, allowing temporary bodies in a transactional outbox. Acknowledgment, canonical IDs and reconciliation cover failures between the systems. This explicitly relaxes the current all-state-in-Postgres architecture only for opted-in conversation bodies and inbox events. NATS is internal infrastructure, never a client credential requirement.

The addition of the NATS runtime and client is authorized in scope for phase 4. Exact versions and limits must be pinned and validated in the implementation PR. Production topology, costs, rollout and data migration remain separate operator actions.

### Receipts and compatibility

Keep stored, delivered, presented, acknowledged and resolved as distinct observations. Stored means the selected authoritative backend confirmed persistence. Transport receipt or cursor advancement never implies model recognition. Presented is unknown when a host cannot reliably confirm injection. Resolution does not complete a task or merge a PR automatically.

Introduce the 14 explicitly listed conversation tools behind a server-enforced per-team capability flag. Preserve existing tool schemas and legacy history; any later compatibility retirement requires a separate migration/version decision. Phase 2 must include complete Claude/Codex acceptance and orphan recovery rather than deferring them until NATS ships.

## Phase gates

1. Authenticated sessions, proxy and binary-backed hooks against existing messaging.
2. Complete conversations, receipts, transfer/recovery and five-window acceptance on Postgres.
3. Postgres backend abstraction and gated outbox reliability tests.
4. NATS runtime, adapter and required integration fixture using synthetic teams.
5. Opt-in JetStream body publication and history.
6. Opt-in durable inbox fanout and local proxy spool.
7. Production migration tooling, reverse-copy rollback, restore drill and independent failure domains.

Native issue dependencies and explicit release criteria determine readiness. Numbers such as 0012 are not reserved by this ADR; session discovery already occupies that filename in the current checkout. Choose the next free additive migration when implementing.

## Consequences

Users receive the complete coordination workflow after phase 2, and can remain on it. Later phases are deployable with their optional routes disabled. The operational cost and dual-system recovery complexity of JetStream are deliberate, not a prerequisite disguised as a messaging feature.

Session credentials introduce a narrow additional capability class without replacing existing tokens with JWTs. Agent recovery improves continuity but means private-session access has a documented parent-agent exception. Local credential storage and binary-backed hooks require explicit updates to the legacy configuration/engineering guidance.

Rollback is bounded by actual schema and tool compatibility in every phase. Even with a single database, an older binary may not expose newly written conversation data. After JetStream-only writes exist, switching to an old image alone is not a sufficient rollback plan.

## Verification

Implementation PRs must pass `make check` and `make test`. Test session impersonation, parent revocation, epoch races, cross-team access, recovery versus registration, transfer without forged receipts, receipt semantics and legacy behavior. From phase 4, required NATS tests must fail visibly rather than skip when the fixture is unavailable. Production readiness additionally requires migration, reverse-copy and restore evidence.

The detailed protocol is in `docs/design/nats-conversations.md`. This ADR is published in the planning issue as well because the local design directory is excluded from git. The initial documentation PR must make the accepted decision available to repository readers without committing local-only agent scaffolding.
