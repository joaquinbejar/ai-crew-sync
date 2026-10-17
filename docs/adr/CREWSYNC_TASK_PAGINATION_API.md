---
title: CrewSync task listing pagination API
type: adr
status: approved
created: 2026-10-08
scope: list_tasks
---

# CrewSync task listing pagination API

## Status and decision

Approved after separate coordinator review of the implementation contract against current Rust types and legacy SQL ordering. This approval covers the ADR interfaces only; it is not product implementation acceptance.

Extend `list_tasks` with bounded keyset pagination while preserving the
backward-compatible `TaskList` fields and their legacy meanings. This ADR is
one normative contract. The request, response, ordering, cursor, count, and
concurrency rules below are the only V1 definitions.

## Frozen logical signatures

The public request is:

```json
{
  "project_prefix": "core-manager",
  "status": "open",
  "mine_only": true,
  "limit": 200,
  "cursor": "opaque-token-or-null"
}
```

All fields are optional except that `mine_only` defaults to `false` and `limit`
defaults to the server's documented page size. `limit` is a positive integer
capped by the server maximum. The V1 default is 50 and the current server cap is 10,000; requests exceeding the cap are clamped to 10,000 while responses remain explicitly paginated.

- `project_prefix` is an optional parameterized, case-sensitive task-key prefix.
  Omission means all task keys visible in the authenticated team.
- `status` is an optional existing task-status filter. Omission means all
  statuses visible to the caller.
- `mine_only` restricts results to tasks currently claimed by the
  server-authenticated caller. Caller-supplied identity fields are ignored.
- `cursor` is an opaque continuation token. Omission or null starts a traversal;
  a non-null value continues the same effective filters and team.

The implementation uses one typed query object rather than diverging positional
parameters. The logical Rust signatures are:

```rust
pub struct ListTasksArgs {
    pub status: Option<String>,
    pub mine_only: bool,
    pub limit: i64,
    pub project_prefix: Option<String>,
    pub cursor: Option<String>,
}

pub struct TaskList {
    pub tasks: Vec<TaskInfo>,
    pub open: i64,
    pub claimed: i64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

pub struct TaskPageQuery {
    pub status: Option<String>,
    pub mine_only: bool,
    pub limit: i64,
    pub project_prefix: Option<String>,
    pub cursor: Option<String>,
}

pub async fn list_tasks(
    pool: &PgPool,
    auth: &AuthCtx,
    query: TaskPageQuery,
) -> BusResult<TaskList>;
```

The exact language type names may follow the repository's conventions, but the
field names, meanings, and scalar types above are frozen for V1.

## Frozen response contract and count semantics

The response is:

```json
{
  "tasks": [],
  "open": 42,
  "claimed": 7,
  "next_cursor": "opaque-token-or-null",
  "has_more": false
}
```

- `tasks` contains only the current page, in the frozen ordering below.
- `open` is an `i64` total of all visible open tasks in the authenticated team
  satisfying the request's team and authorization scope. It is not the page
  length and not an array.
- `claimed` is an `i64` total of all visible claimed tasks in the authenticated
  team satisfying the request's team and authorization scope. It is not the page
  length and not an array.
- `open` and `claimed` retain their legacy total-count semantics. They are not
  page projections, and they are not filtered by `limit` or `cursor`; a future
  page-level count would require a new explicitly named field.
- `next_cursor` is null exactly when `has_more` is false; otherwise it is an
  opaque continuation token for the last returned row.
- `has_more` is true exactly when at least one additional row exists after the
  page boundary under the same visibility and request filters. Implementations
  should fetch `limit + 1`, expose at most `limit`, and never expose the
  sentinel row.

A request omitting all new fields remains backward compatible: it returns the
legacy `tasks`, `open`, and `claimed` fields with the same meanings, plus
additive `next_cursor` and `has_more`. The default limit still applies; the
legacy path is not unbounded.

There is intentionally no total result count. The two existing totals are
status totals with their historic meanings; `has_more` is only a continuation
existence signal and is not a count estimate. Counts and page rows are allowed
to reflect concurrent writes independently according to the existing database
read policy.

## Frozen ordering and strict keyset boundary

The single V1 ordering is:

```sql
ORDER BY legacy_status_rank ASC, updated_at DESC, key ASC
```

`legacy_status_rank` is the existing server-owned status-priority rank used by
the legacy task listing. It is stable, not caller-configurable, and must be
implemented in one shared function/query expression used by both ordering and
cursor validation. The rank is evaluated after the status filter; omitting the
status filter does not remove the rank from ordering.

`key` is the task's unique stable key within the team and is the final
 tie-breaker. The order is total and deterministic even when status rank and
`updated_at` tie. No alternative `updated_at DESC, key ASC`-only ordering is
permitted for V1, and no offset pagination is permitted.

The cursor stores the last returned tuple:

```text
(last_status_rank, last_updated_at, last_key)
```

The strict continuation predicate is the lexicographic equivalent of the
frozen ordering:

```sql
legacy_status_rank > :last_status_rank
OR (
  legacy_status_rank = :last_status_rank
  AND updated_at < :last_updated_at
)
OR (
  legacy_status_rank = :last_status_rank
  AND updated_at = :last_updated_at
  AND key > :last_key
)
```

Apply authorization, project prefix, status, mine-only filtering, and this
strict boundary before ordering and limiting. The last row of page N is never
returned on page N+1. Equal timestamps are safe because `key` is mandatory.

A live keyset traversal is not a repeatable snapshot. A task updated to an
already traversed rank/time position can move before the boundary and be
skipped; a deleted task disappears; a newly qualifying task can appear after
the boundary. These are documented concurrency semantics and do not alter
claims, leases, locks, or task state.

## Cursor integrity, filter binding, and authorization

The cursor is opaque and authenticated. V1 uses a versioned, base64url payload
with a server-side MAC, or an equivalent backend token that provides the same
integrity and binding guarantees. Its authenticated claims include:

```json
{
  "v": 1,
  "team_id": "server-derived-team",
  "status_rank": 0,
  "updated_at": "database-precision-timestamp",
  "key": "last-returned-key",
  "filter_hash": "hash(canonical-effective-filters)",
  "issued_at": "server-time",
  "expires_at": "server-time"
}
```

The canonical filter hash covers authenticated team identity, normalized
`project_prefix`, normalized `status`, and `mine_only`. It does not cover
`limit`, so a caller may change page size while continuing a valid traversal.
The server recomputes authorization and all effective filters from the request;
it never trusts team, status, identity, or filters decoded from a cursor.

Reject before querying task rows when the cursor is malformed, truncated,
expired, unknown-version, missing claims, MAC-invalid, from another team, bound
to different filters, or encoded with an incompatible timestamp precision.
Cursor failure never falls back to page one. A cursor is a continuation
capability, not an access grant.

## Concurrency and side-effect boundaries

`list_tasks` is read-only. It must not acquire, renew, release, or alter any
claim, lease, lock, dependency, task status, or completion record. Team ACL and
effective caller identity come from the authenticated request context.

Each page is one database statement or transactionally consistent read under the
existing store policy. `open` and `claimed` totals are computed with the same
team visibility rules but are not required to be a repeatable snapshot of the
page rows. A stronger repeatable export requires a separate snapshot API with
explicit retention and resource limits.

## Given / When / Then acceptance criteria

### Contract and compatibility

- Given an old request with no new fields, when `list_tasks` runs, then the
  response contains `tasks`, `open: i64`, and `claimed: i64` with legacy
  meanings plus additive `next_cursor` and `has_more`.
- Given `limit: 200` and 2,000 visible tasks, when pages are followed until
  `has_more` is false, then no page exposes more than 200 tasks and every task
  key returned by the traversal is distinct.
- Given an omitted limit, when the request runs, then the documented default and
  configured maximum apply and no unbounded result is allocated.

### Ordering and continuation

- Given rows with equal statuses and timestamps, when pages are traversed, then
  keys are ordered ascending and no key repeats across the strict boundary.
- Given rows in different legacy status ranks, when a page is returned, then
  rank is the primary order, `updated_at DESC` is the secondary order, and
  `key ASC` is the final tie-breaker.
- Given the last tuple of page N, when page N+1 is requested, then every row
  satisfies the three-branch strict predicate and the boundary row is absent.
- Given concurrent insert, update, completion, claim, or delete activity, when
  valid cursors are followed, then authorization is never bypassed and the
  documented live-keyset behavior is observed.

### Filters, totals, and security

- Given any combination of project prefix, status, and mine-only, when a page is
  returned, then every row satisfies all effective filters and the two totals
  retain their team-wide legacy status-count meanings.
- Given a cursor issued for one filter set or team, when it is used with another,
  then the server rejects it before reading task rows.
- Given a modified, truncated, expired, or unknown-version cursor, when it is
  used, then the server returns a client error and does not restart at page one.
- Given no matching rows, when the first page is requested, then `tasks` is
  empty, `has_more` is false, and `next_cursor` is null.
- Given exactly a multiple of the page size, when the final full page is read,
  then `has_more` is false and `next_cursor` is null.
- Given Unicode, delimiter, or wildcard characters in a prefix, when filtering
  occurs, then matching is parameterized and cursor canonicalization cannot
  alter the query.

## Test, rollout, and review gate

Focused implementation tests must cover cursor round trips, MAC tampering,
expiry, version and filter mismatch, timestamp normalization, page-size changes,
legacy status-rank ordering, tied timestamps, 2,000-task traversal, limits 1
and 200, the configured maximum, all filters, team isolation, totals, sentinel
suppression, and concurrent writes. Compatibility tests must assert that
`open` and `claimed` are i64 totals rather than arrays or page projections.

Roll out behind the existing capability/version gate if a client cannot tolerate
additive fields. Canary old no-cursor requests and cursor validation while
measuring page size, rejected cursors, exhausted pages, and latency. Rollback
stops issuing new cursors and may retire a cursor version or signing key using
the documented client error; it does not alter claims or task state.

Independent review must explicitly approve this ADR before dependent
implementation tasks treat it as frozen. That independent contract review has been completed; the front matter is
`status: approved`. This does not approve C03/C05+ runtime implementation.

## Evidence receipt

- Exact write scope: `docs/adr/CREWSYNC_TASK_PAGINATION_API.md`.
- QA1 repair: removed contradictory array/page-projection definitions; made
  `open` and `claimed` normative `i64` team totals; froze additive cursor fields;
  froze legacy status-rank ASC, `updated_at DESC`, `key ASC` ordering and its
  three-branch strict boundary; retained `status: proposed` pending review.
- No task claims, locks, task administration, Ruflo state, or Native state are
  changed by this ADR.
