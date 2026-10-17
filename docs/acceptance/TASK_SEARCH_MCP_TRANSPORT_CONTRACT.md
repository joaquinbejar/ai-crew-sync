# Task Search MCP Transport Contract — E2E Acceptance Gate

**Task key:** `crewsync:task-search-mcp-runtime-docs-r1`
**Project:** `crewsync`
**Attempt:** `attempt-e8e8913e-4066-4b6d-a190-e23611c183db`
**Session:** `manual-mcp-runtime-docs-r1-task-search-mcp-runtime-docs-r1`
**Producer role:** `implementer`
**Producer thread:** `null`
**Review status:** `CANDIDATE / UNACCEPTED`
**Review date:** 2026-10-09
**Changed files:** `docs/acceptance/TASK_SEARCH_MCP_TRANSPORT_CONTRACT.md` only

This is the bounded MCP transport acceptance contract for the existing task inventory. It is an evidence contract, not task administration. It does not claim `DONE`, `ACCEPTED`, `ACCEPTED_GREEN`, deployment, or release readiness. No source, migration, test, task state, lock, Ruflo state, or Native state is modified.

## Scope and authority

The implementation source of truth is the existing CrewSync MCP service and task inventory:

- `src/serve.rs`: canonical Streamable HTTP service at `POST /mcp`, stateless mode, host/origin controls, bearer middleware, request-body limit, health, and broker publication behavior.
- `src/tools/tasks.rs`: one additive `list_tasks` tool with pagination and search parameters. There must not be a competing `search_tasks` tool or second inventory.
- `src/store/tasks.rs`: PostgreSQL-backed, team-scoped search, filters, stable continuation ordering, page-plus-one limit, totals, and lease semantics.
- `src/client/mod.rs`: the console client uses the same Streamable HTTP transport, bearer identity, `tools/list`, and `tools/call` path as an MCP agent.
- `tests/task_pagination.rs`, `tests/task_search.rs`, and `tests/task_search_public_api.rs`: disposable fixture and compatibility evidence, including the 2,000-row traversal, ACL, cursor binding, filters, all search modes, old/new client calls, and pathological regex coverage.
- `_bmad-output/implementation-artifacts/spec-task-search.md`: frozen V1 contract and mandatory acceptance matrix.
- Draft PR #202 (`f213f1e`, branch `n0namer:feat/task-list-cursor-20261008`): upstream context only. It is not an acceptance receipt and must not be promoted from status text alone.

Acceptance requires an actual MCP client transport against a disposable PostgreSQL-backed service and an independently run Rust compile/runtime check. Static source review alone is insufficient for E2E rows marked **RUNTIME-PENDING**.

## Invocation contract

Use the real service binary and a real MCP client transport. The canonical endpoint is `POST /mcp`; do not test a private router helper as a substitute for the binary canary.

### Binary canary

Build and start the actual binary with a disposable `DATABASE_URL`, a loopback bind, and a test-only bearer token provisioned through the existing admin/token flow. Do not place a token or database credential in this document, command history captured as evidence, or logs.

```text
cargo build --bin ai-crew-sync
ai-crew-sync serve --bind 127.0.0.1:<free-port>
ai-crew-sync client --url http://127.0.0.1:<port>/mcp --token <redacted-agent-token> --json tools
ai-crew-sync client --url http://127.0.0.1:<port>/mcp --token <redacted-agent-token> --json tasks --limit 50 --search deployment --search-mode contains --search-fields both --search-language simple
```

The binary canary passes only when the process is the built workspace binary, the request reaches `POST /mcp`, the bearer identity resolves to the fixture team, and the response is produced through the same rmcp Streamable HTTP client path used by the console client. A direct SQL call, an in-process `build_router` call, or a fabricated JSON response is not a canary.

### Direct JSON-RPC transport probe

When a raw wire receipt is needed, use an MCP-aware HTTP client or a request equivalent to these exact method/parameter shapes. The client must preserve the response headers and any transport metadata required by the installed rmcp revision; do not add a legacy session header because the service explicitly disables legacy session mode.

Initialize:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "<client-supported-version>",
    "capabilities": {},
    "clientInfo": {"name": "crewsync-acceptance", "version": "1"}
  }
}
```

The request is `POST /mcp` with `Authorization: Bearer <redacted-agent-token>` and `Content-Type: application/json`. The client records the negotiated protocol version from the response; the test must not hard-code a version different from the installed rmcp client.

List tools:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/list",
  "params": {}
}
```

Call the legacy-compatible list path with no search:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "list_tasks",
    "arguments": {
      "status": "open",
      "mine_only": false,
      "limit": 50,
      "project_prefix": "search-fixture-",
      "cursor": null
    }
  }
}
```

Call the additive search path through the same tool:

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "list_tasks",
    "arguments": {
      "status": "open",
      "mine_only": false,
      "limit": 25,
      "project_prefix": "search-fixture-",
      "cursor": null,
      "search": "deployment",
      "search_mode": "contains",
      "search_fields": "both",
      "search_language": "simple"
    }
  }
}
```

The exact client invocation is authoritative for transport behavior; the JSON-RPC shapes above are the wire assertions. The fixture must use a generated token and an isolated team, never a repository or operator credential.

## `tools/list` schema gate

The `tools/list` response must contain `list_tasks` and must not contain `search_tasks`. The `list_tasks.inputSchema` must remain an object and expose the legacy properties plus these additive search properties:

| Property | Required shape | Frozen behavior |
|---|---|---|
| `status` | string, optional | `open`, `claimed`, `done`, `cancelled`, or `any`; omitted preserves legacy default |
| `mine_only` | boolean, optional | Defaults false; scopes live claims to the authenticated session |
| `limit` | integer, optional | Defaults 50; effective range 1–10,000 |
| `project_prefix` | string or null, optional | Optional key-prefix inventory scope |
| `cursor` | string or null, optional | Opaque signed continuation token |
| `search` | string or null, optional | Title/description query; omitted or whitespace means no-search compatibility path |
| `search_mode` | string, optional | `keywords`, `contains`, or `regex`; default `keywords` when search is nonempty |
| `search_fields` | string, optional | `title`, `description`, or `both`; default `both` |
| `search_language` | string, optional | `simple`, `english`, or `russian`; default `simple` |

The generated schema and the runtime must agree. In particular, the search parameters are not a second tool, are not required for an old request, and are not silently accepted with arbitrary enum values. The acceptance receipt must preserve the complete `tools/list` JSON so an independent reviewer can compare property names, types, defaults, and required fields against the actual generated schema.

## `tools/call` acceptance matrix

Every row is a real `tools/call` over `POST /mcp` with a valid bearer token, unless the row explicitly tests authentication. Record HTTP status, JSON-RPC error or MCP `isError`, returned structured content, task keys, `has_more`, `next_cursor`, and `open`/`claimed` totals.

| Case | Invocation variant | Required result |
|---|---|---|
| Legacy absent search | Omit `search`, `search_mode`, `search_fields`, `search_language` | Same ordering, filters, totals, and cursor semantics as the pre-search list path |
| Legacy whitespace search | `search: "   "` with arbitrary search options | Same result as omitted search; no broadening and no changed totals |
| Title-only | `search_fields: "title"` | Matches title only; body-only sentinel is absent |
| Description-only | `search_fields: "description"` | Matches description only; title-only sentinel is absent |
| Both fields | `search_fields: "both"` | Matches either title or description |
| Keywords | `search_mode: "keywords"`, all three whitelisted languages | Uses the selected language; multiple terms and quoted phrase are verified |
| Literal contains | `search_mode: "contains"` with `%`, `_`, and `\` in the query | Wildcards are literal; no SQL interpolation or accidental broad match |
| Regex | `search_mode: "regex"` | Case-insensitive PostgreSQL regex semantics; bounded page and statement time |
| Filters combined | Search plus `status`, `mine_only`, `project_prefix`, and tenant | All predicates apply together; no cross-team rows; totals remain team-wide status totals, not hit counts |
| Cursor continuation | Reuse returned `next_cursor` with identical arguments | No duplicates on the static fixture; terminal page has `has_more: false` and `next_cursor: null` |
| Cursor replay rejection | Alter query, mode, fields, language, status, mine flag, prefix, or team | Signed cursor is rejected; it must not be reinterpreted under new filters |
| Bounded limit | `limit` 1, 200, 2,000, 10,000, 0, negative, and above 10,000 | Valid limits are clamped/bounded as implemented; no zero/negative page, unbounded scan, or oversized response |
| Invalid search options | Unknown mode, field, or language with nonempty search | Invalid-params error; never silently falls back to keywords/both/simple |
| Empty/oversized input | Empty, whitespace, over-limit query, NUL-containing regex | Empty preserves compatibility; unsafe oversized/NUL input is rejected as safe input error |
| Invalid regex | Malformed pattern and pathological/non-indexable pattern | Safe input/timeout behavior; no process crash, SQL injection, or unbounded request |

Search is only over task `title` and `description`. It must not match `result`, attachments, metadata, notes, events, or another team's rows. Ordering remains stable rank ASC, `updated_at DESC`, key ASC; relevance must not disturb the signed continuation order.

## Authentication, ACL, and error map

All `/mcp` requests require an agent bearer token. The middleware resolves the token once, injects the authenticated context, and charges the per-token limiter. The `X-Crew-Session` header partitions an agent's own work; it is not a substitute for identity. A session credential proves its own label and may not be mixed with a conflicting header.

| Boundary case | Expected HTTP result | Expected body/header |
|---|---:|---|
| No `Authorization` header | 401 | JSON error `missing bearer token`; `WWW-Authenticate: Bearer` |
| Wrong, revoked, or wrong-prefix token | 401 | JSON error `invalid or revoked token`; `WWW-Authenticate: Bearer` |
| Disabled agent | 403 | JSON error `agent is disabled` |
| Session/header mismatch | 403 | JSON error names the mismatch without exposing a secret |
| Bad session header | 400 | JSON error explains invalid ASCII/length/control/template/slash input |
| Stale session epoch | 409 | JSON error identifies stale connection fencing; no write is accepted |
| Expired session credential | 401 | JSON error directs registration of a new session; no `WWW-Authenticate` requirement beyond implementation receipt |
| Per-token rate limit | 429 | JSON error includes retry seconds and `Retry-After` |
| Body over `max_request_bytes` | 413 | JSON error names the byte limit and suggests splitting attachments |
| Unknown route | 404 | Route error names `POST /mcp`, `/health`, `/dashboard`, and `/admin/*`; it must not masquerade as an auth failure |
| Valid token, foreign team task | 200 with zero foreign rows | Team predicate excludes the row; no cross-tenant leak |
| Tool argument validation failure | MCP invalid-params / tool error | Error is structured and safe; server remains available |

`/admin/*` uses the separate administrative credential class and is not an alternate MCP route. A dashboard or admin credential must not authenticate `/mcp`. Query strings and bearer values must not appear in trace spans or acceptance output.

## Status and transport health

`GET /health` is an operational probe, not an MCP call. With PostgreSQL reachable it returns HTTP 200 and `status: "ok"`, `database: "up"`, plus event-listener state. If the event listener is not live, status becomes `degraded` while writes may still be served. If PostgreSQL is unavailable it returns HTTP 503 with `status: "degraded"` and `database: "down"`.

When JetStream is configured, health reports `broker: "configured"`; `?broker=check` distinguishes `up` from `unreachable` and may include publication backlog. A broker outage alone does not turn a working Postgres-backed request path into HTTP failure: accepted routed messages remain in the outbox for reconciliation. The receipt must distinguish:

- HTTP/MCP request acceptance;
- Postgres persistence;
- outbox publication state;
- broker reachability;
- eventual publication/failover completion.

Do not call a broker outage success, and do not call an accepted Postgres write lost merely because publication is pending.

## Compatibility and failover guarantees

The contract must prove all of the following with the old and new clients:

1. An old `list_tasks` request with no search fields still deserializes and returns the old shape plus only implementation-defined additive pagination fields; no old filter is dropped.
2. The console command `ai-crew-sync client ... tasks` and the MCP `tools/call` reach the same server tool and store path. `--all-pages` follows opaque cursors, detects repeated/missing cursors, and reports `incomplete`/`exhausted` truthfully.
3. The service is stateless (`legacy_session_mode(false)`), so two healthy binary replicas behind a load balancer can serve alternating requests without sticky sessions. The acceptance client must alternate replicas for initialize/tools/list/tools/call and confirm identical authenticated team behavior.
4. A cursor is signed and bound to team, status, mine flag, project prefix, search text, mode, fields, language, and continuation order. Replaying it against another replica is valid only when those bindings match.
5. A broker configured for routed teams may be unreachable without dropping the Postgres outbox row. Recovery must reconcile uncertain publication before retrying, preserve at-most-once publication intent, and expose pending/failed backlog in health. This contract does not claim broker delivery without a raw receipt.
6. Concurrent lease/claim operations remain isolated from read-only search: a task claimed by the fixture agent appears under `status=claimed,mine_only=true`, a lapsed lease returns to effective `open`, and search does not mutate claims.

## 2,000-row PostgreSQL fixture gate

Run the existing disposable fixture, not a synthetic in-memory replacement. The evidence must identify PostgreSQL version, migration state, binary revision, command, fixture size exactly 2,000, elapsed time, and whether `CREWSYNC_ENABLE_LARGE_FIXTURE=1` and a disposable `TEST_DATABASE_URL` were available.

Required checks:

- Traverse all 2,000 rows with a bounded page size and search before truncation; record every page size, `has_more`, terminal `next_cursor`, and unique-key count. Expected static traversal: 10 pages at `limit=200`, 2,000 unique keys, zero duplicates.
- Prove title-only, description-only, both-field, contains escaping, quoted/multiple keywords, English, Russian/Cyrillic, case-insensitive regex, invalid regex, pathological regex, empty/oversized input, and non-indexable regex behavior.
- Combine search with status, mine-only, project prefix, and team ACL. The foreign-team sentinel must never appear. Record team-wide `open`/`claimed` totals separately from search hit count.
- Replay the initial cursor, altered query, altered mode, altered field, altered language, altered status, altered prefix, and tampered token. Record rejection class without leaking SQL or secrets.
- Run the old client request and the new search request through the real binary. Preserve the actual `tools/list` and `tools/call` receipts.
- Run a read-only database canary and concurrent lease operations. A read-only failure is a blocker; do not infer it from source inspection.
- Capture `EXPLAIN (ANALYZE, BUFFERS)` and timings for supported keyword field/language branches, contains, regex, and the pathological/non-indexable regex. This document records the required evidence; it does not certify index quality without the receipt.

The ignored Rust tests in the repository are the fixture authority. Their being present, or a status note saying `2/2 PASS`, is not by itself an independent runtime receipt.

## Focused checks and evidence state

| Check | Owner of execution | Required evidence | Current state |
|---|---|---|---|
| Generated schema and wire calls | Independent QA/reducer | Raw `tools/list`, initialize, legacy/search `tools/call` receipts | **PENDING** |
| Bearer/ACL/error map | Independent QA/reducer | HTTP status, JSON body, headers, no secret leakage | **PENDING** |
| Cargo compile and Rust runtime tests | Coordinator/independent QA | `cargo check`, focused tests, full affected test result | **PENDING** |
| Disposable PostgreSQL 2,000-row E2E | E2E owner/reducer | Version, fixture, timings, pages, unique keys, cursor/error matrix | **PENDING** |
| Real binary canary | Independent QA | Process/binary identity and actual transport receipts | **PENDING** |
| Replica/broker failover | Release/reducer | Alternating-replica and outbox reconciliation receipt | **PENDING** |
| Node-only worker validation | This worker | `run_node_tests` attempt and tool result | **BLOCKED_INFRA** |

This worker cannot convert Rust acceptance into a Node result. The project is Rust/Cargo-only in `Cargo.toml`, and the mandated `run_node_tests` runner is not a Rust test runner. The coordinator must preserve the exact runner output and use the appropriate independent Cargo/PostgreSQL environment for runtime acceptance.

## Decision rule

The artifact is **CANDIDATE / UNACCEPTED** until every runtime-pending row has an independent receipt and all required checks are green. A complete report must retain the exact task binding above, list only this document as changed, and distinguish source evidence, runtime evidence, independent QA, and coordinator acceptance. `turn.completed`, a successful report publication, a textual PR checkpoint, or a prior status note is not equivalent to runtime success or acceptance.
