# Task Search MCP Release Matrix

**Gate status:** `UNACCEPTED` / `PENDING`
**As of:** 2026-10-09
**Project:** `crewsync`
**Task key:** `crewsync:task-search-mcp-release-matrix-r1`
**Attempt:** `attempt-741ec760-e296-4338-8882-73623575fa5a`
**Session:** `manual-mcp-release-matrix-r1-task-search-mcp-release-matrix-r1`
**Scope:** acceptance evidence only; no product, test, migration, deployment, or scheduler changes.

This is an independent release gate for task-search over the real MCP transport. It does not convert a worker receipt, `turn.completed`, CrewSync `DONE`, or a passing direct-store test into release acceptance. A gate is `PASS` only when the named evidence exists, is tied to the same Git source and disposable environment, and has an independent QA readback where specified. A missing, stale, indirect, or coordinator-only assertion is `PENDING`, not pass.

## Release decision

**Current decision: `DO NOT RELEASE`.** The matrix is intentionally unaccepted until all mandatory rows below are independently evidenced. No production rollout, merge, or release promotion is authorized by this document.

| Decision rule | Exact result |
|---|---|
| Any mandatory row `FAIL` | Release `FAIL`; affected-only repair required |
| Any mandatory row `PENDING` or missing independent QA | Release `PENDING`; do not promote |
| Every mandatory row `PASS`, same-source receipts verified, independent QA `PASS`, no known blocker | Eligible for coordinator-owned acceptance review; this document still does not self-accept |

## Evidence contract

Every receipt must identify the exact task key, commit/source revision, test command, environment, result, and artifact path. MCP evidence must include the actual request/response boundary for `tools/list` and `tools/call`; a direct Rust function call, SQL-only test, or CLI-only invocation is supporting evidence, not transport proof. Secrets, bearer tokens, database URLs, and credentials must never be copied into evidence.

Required baseline before any `PASS`:

- Disposable PostgreSQL instance with migrations applied, including task-search migrations; capture version and fixture size without secrets.
- Real server bootstrap on the tested binary/source, with health/readiness and clean shutdown or restart evidence.
- Authenticated MCP client session against the server's configured transport; capture status/error class and redacted JSON shape.
- Stable source revision shared by all eight owners and the independent reviewer.
- Independent QA readback of the wire evidence, negative cases, and release reducer.

## Eight-owner evidence matrix

These are the exact Wave A owners and their disjoint evidence responsibilities from the task-search contract. Each row is `PENDING` until the corresponding typed receipt and artifact are independently read back.

| Owner / scope | Required proof | Exact pass condition | Current status |
|---|---|---|---|
| `crewsync:task-search-mcp-wire-e2e-r1` / `tests/task_search_mcp_transport.rs` | Real MCP `tools/list` and `tools/call` against `list_tasks`; keywords, literal contains, and regex calls reach PostgreSQL-backed server | Wire calls return the documented shape and expected hits; no direct-store substitute; clean server/session teardown | `PENDING` |
| `crewsync:task-search-mcp-security-e2e-r1` / `tests/task_search_mcp_security.rs` | Auth failures, tenant ACL isolation, cursor tamper rejection, malformed regex, bounded/pathological regex behavior | Unauthorized or cross-tenant requests fail safely; invalid regex is an input error; pathological input stays bounded; tampered cursor is rejected | `PENDING` |
| `crewsync:task-search-mcp-pagination-e2e-r1` / `tests/task_search_mcp_pagination.rs` | 2,000-task traversal over MCP with cursor, filters, totals, ties, and repeated cursor | Zero duplicates or omissions, correct `has_more`/terminal cursor, stable timestamp/key ordering, totals remain team-wide | `PENDING` |
| `crewsync:task-search-mcp-schema-compat-r1` / `tests/task_search_mcp_schema.rs` | `tools/list` schema plus old request payloads omitting new search fields | Schema exposes only additive supported fields; legacy `list_tasks` request remains valid and preserves no-search semantics | `PENDING` |
| `crewsync:task-search-mcp-client-cli-e2e-r1` / `tests/task_search_cli_transport.rs` | Existing CLI/client path reaches MCP server with omitted and populated search arguments | Old client behavior remains compatible; search options and cursor metadata survive the transport without local-only behavior | `PENDING` |
| `crewsync:task-search-mcp-runtime-docs-r1` / `docs/acceptance/TASK_SEARCH_MCP_TRANSPORT_CONTRACT.md` | Runtime contract, lifecycle, auth, transport assumptions, and operator evidence requirements | Contract matches actual server behavior and names all non-claims; no production assertion is implied | `PENDING` |
| `crewsync:task-search-mcp-server-canary-r1` / `tests/task_search_mcp_server_canary.rs` | Bootstrap, health, authenticated readiness, restart/reconnect, and clean shutdown | Server becomes ready only after required dependencies; health/auth behavior is deterministic across restart; no leaked session/worker state | `PENDING` |
| `crewsync:task-search-mcp-release-matrix-r1` / this document | Independent reducer across transport, search, security, compatibility, CI, cursor, auth, and QA | Matrix has exact evidence refs, no false acceptance, and remains `UNACCEPTED` until all prerequisite gates pass | `CANDIDATE / PENDING` |

## Mandatory gate checklist

The following checklist is the reducer. `PASS` requires the named evidence, not merely the expected behavior or a source inspection.

| ID | Gate | Evidence required | Pass / fail rule | Status |
|---|---|---|---|---|
| MCP-01 | `tools/list` exposure | Wire transcript from schema owner, redacted | `list_tasks` is present once; schema documents additive `search`, `search_mode`, `search_fields`, `search_language`, cursor, and existing filters | `PENDING` |
| MCP-02 | `tools/call` search behavior | Wire transcript plus PostgreSQL fixture receipt | Keywords, contains, and regex return correct title/description results through MCP; no second competing search tool | `PENDING` |
| MCP-03 | Legacy request | Old JSON payload and response assertion | Omitted search fields retain legacy pagination/filter semantics and numeric totals | `PENDING` |
| MCP-04 | Auth and tenant boundary | Positive authenticated call plus unauthenticated/invalid-token and foreign-team cases | Valid auth succeeds; invalid auth is rejected; no task from another team is observable | `PENDING` |
| MCP-05 | Cursor binding | First page, replay, altered query/mode/field/language, tampered token | Valid continuation is accepted; altered or tampered binding is rejected; no duplicates in static traversal | `PENDING` |
| MCP-06 | Filter composition | MCP calls combining search with status, `mine_only`, `project_prefix`, and tenant | Every filter remains effective together; claims/leases are not changed by read-only search | `PENDING` |
| MCP-07 | Regex safety | Invalid regex, oversized query, pathological pattern, no-trigram pattern, timeout evidence | Safe input error or bounded empty/result response; no SQL injection, unbounded scan, or server instability | `PENDING` |
| MCP-08 | Pagination scale | 2,000-task disposable PostgreSQL fixture over actual MCP transport | Page limit + 1 behavior is correct; all expected tasks appear once; terminal page has no cursor | `PENDING` |
| MCP-09 | Ordering and ties | Identical `updated_at` fixture plus repeated page traversal | Stable rank/time/key ordering is maintained across pages and repeated runs | `PENDING` |
| MCP-10 | Runtime lifecycle | Bootstrap, `/health`/readiness, reconnect or restart, clean shutdown receipts | Dependency/auth readiness is observable and restart does not corrupt or duplicate search sessions | `PENDING` |
| MCP-11 | CLI/client compatibility | Existing client/CLI transport test with old and new arguments | Existing commands remain valid; all-pages and cursor metadata are unchanged; no CLI-only pass is substituted for MCP proof | `PENDING` |
| MCP-12 | Rust/Postgres regression | Focused search/pagination tests, migration application, `cargo check --tests`, affected full tests | Search, pagination, auth, claims, leases, and no-search behavior pass on same source; unrelated failures are separated, not hidden | `PENDING` |
| MCP-13 | CI and formatting | CI-equivalent checks and `cargo fmt --check` receipt | Required CI checks pass on the tested revision; no generated-recipe or compatibility blocker remains | `PENDING` |
| MCP-14 | Independent QA | Reviewer readback of all wire artifacts and negative cases | Reviewer confirms actual transport, source identity, redaction, and no false acceptance | `PENDING` |
| MCP-15 | Release reducer | Final accepted readbacks for all eight owners plus known-blocker review | Zero mandatory blockers and every prerequisite `PASS`; coordinator may then decide acceptance | `PENDING` |

## Direct supporting evidence already visible

The scoped implementation and test sources support the contract but do not, by themselves, pass the MCP transport gates:

- `src/tools/tasks.rs` exposes additive search arguments with defaults and validates mode, fields, and language only for non-empty search.
- `tests/task_search.rs` covers direct PostgreSQL/store behavior for title/description selection, keywords, contains wildcard literals, regex, invalid/pathological input, pagination, cursor filter binding, ACL, and the real client path; it is marked ignored and requires a disposable database.
- `tests/task_pagination.rs` covers legacy pagination, filters, ACL, cursor traversal, and a real client path; it is also environment-dependent.
- `src/serve.rs` shows the server uses rmcp streamable HTTP and authenticated runtime state, but source inspection is not a wire receipt.
- `Cargo.toml` enables rmcp server/client transport features and PostgreSQL dependencies; dependency declarations are not CI evidence.
- `_bmad-output/implementation-artifacts/spec-task-search.md` explicitly requires real MCP `tools/list`/`tools/call`, eight-owner evidence, independent QA, and a no-release decision until the barrier is satisfied.

These are **supporting references only**. They do not change any matrix row from `PENDING` to `PASS`.

## Known blockers and non-claims

1. No wire-level receipt from any of the eight Wave A owners is included in this document; all transport rows remain `PENDING`.
2. The direct E2E tests require `CREWSYNC_ENABLE_LARGE_FIXTURE=1` and a disposable `TEST_DATABASE_URL`; no production database may be used.
3. Independent QA/reducer evidence is required after the owners finish. Worker completion or a typed `VERIFIED` report is not independent acceptance.
4. CI, formatting, migration, runtime restart, auth-negative, and old-client evidence must be captured on the same source revision; source inspection cannot substitute for those checks.
5. The referenced Draft PR remains unmerged; this matrix makes no merge, deployment, or production-readiness claim.
6. This worker has no authority to claim, lock, complete, accept, deploy, or modify any task outside the exact document scope.

## QA handoff and acceptance boundary

The independent reviewer should replay the exact artifacts, verify source identity and redaction, and mark each row `PASS` or `FAIL` with a durable evidence reference. Any failure routes to the original affected Goal Owner for a bounded repair; do not broaden this document's write scope or silently waive a negative case. The coordinator owns CrewSync terminal acceptance and any release decision. Until then, the only valid outcome of this matrix is `UNACCEPTED`.

**Evidence disposition:** `CANDIDATE` only; no `ACCEPTED_GREEN`, no production rollout, no merge authorization.
