# `semloom_pg` capability spike

`semloom_pg` is the first `REL_18_3` capability slice from
`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`. It proves that a
fail-closed SQL marker can be lowered to a planner-visible `CustomPath`/`CustomScan` with an ordinary
PostgreSQL child plan. It is not the final semantic plan/task/result protocol and does not call an
HTTP, Ray, vLLM, or SemLoom scheduling backend.

The current supported query shape is deliberately narrow:

- one top-level `ai_semantic.map(text)` in a single-table `SELECT` target list;
- direct single-table `INSERT ... SELECT` without `RETURNING`, `ON CONFLICT`, or `OVERRIDING`;
- ordinary child filters and projections;
- forward execution with child order preserved;
- `LIMIT`, including `LIMIT 0` and early stop after one row;
- an in-process recording transform that returns `recorded:<input>`; PostgreSQL applies
  `PROPAGATE_NULL` locally without opening a provider session;
- an optional external UDS recording provider with the same SQL-visible output.

The planner rejects joins, inheritance, subqueries, CTEs, aggregates, grouping, windows, `DISTINCT`,
sorting, set operations, row locks, set-returning targets, nested marker use, and marker use outside
the target list. The executor rejects backward scan, mark/restore, rescan, and EPQ. Parallel execution
is disabled. The version-2 UDS protocol is deliberately synchronous with one in-flight task, a 1 MiB
frame limit, and a conservative 174,080-byte input limit applied before JSON encoding. Three separate digests
bind SQL-visible semantic spec, database-selected physical algorithm, and concrete provider execution profile;
PostgreSQL's physical mapped-column number is not part of any wire identity. Accepted-prefix
backpressure, multiple in-flight tasks, out-of-order/missing completion handling, automatic retries, real model
calls, and the full model/prompt/result schema remain pending; this slice must not
be described as a complete database AI operator.

`sem_scan.c` is a thin CustomScan adapter. The PostgreSQL-private `SemloomExecPump` owns child tuple pulls,
NULL propagation, task sequence, completion copying into per-tuple memory, EXPLAIN counters, and provider
lifecycle/error mapping. It calls the provider-neutral `AiOpenSpec → AiPreparedTask → AiCompletion`
`open/drive/close` contract in `ai_provider_port.h`; that header contains only fixed-width values, byte slices,
caller-owned errors, and opaque provider/session handles, with no PostgreSQL headers or types. The in-process
recording adapter and UDS adapter implement the same contract, while socket, JSON, digest, and framing details
remain in the UDS/wire-private modules.

Provider selection and its opaque configuration snapshot are query-fixed, but no session or FD is acquired
until the first non-NULL task. A cleanup callback is registered in `estate->es_query_cxt` before lazy open can
obtain a resource. Returned provider errors make the session terminal and close it before PostgreSQL raises the
preserved SQL error; PostgreSQL interrupts, out-of-memory errors, and other direct longjmps remain cleanup-safe.
Normal close and the callback share an idempotent local close path that invalidates the FD before releasing it
and performs no protocol I/O, wait, allocation, or error reporting.

The PGXS regression covers EXPLAIN identity, ordinary filters/projections, duplicate payloads, expression
inputs, `NULL`, `LIMIT 0/1`, early-stop counters, direct insert rollback/commit, error recovery, and fail-closed
unsupported shapes. TAP starts isolated PostgreSQL nodes and covers missing-preload failure, a prepared
statement, repeatable-read snapshot visibility, child-plan cancellation, insert variants, and successful
execution after cancellation. It also verifies that plain `EXPLAIN`, `LIMIT 0`, zero-row children, and
NULL-only input do not connect; runs the same SQL rows, sequence, NULL, EXPLAIN, and error-lifecycle checks
against both recording adapters; and covers malformed JSON, invalid encoding, integer overflow, evidence
mismatch, disconnect, cancellation during response and saturated-connect waits, input bounds, and socket
cleanup. The final exact-18.3 qualification passed 129/129 TAP checks; the local neutral-boundary and protocol
suite passed 16/16 checks.

The in-process provider remains the default. To exercise the external recording boundary, start the gateway
with an absolute socket path and set the superuser-only GUC for the SQL session:

```bash
python3 gateway/recording_gateway.py --socket /absolute/path/semloom-recording.sock
```

```sql
SET semloom_pg.gateway_socket = '/absolute/path/semloom-recording.sock';
SELECT ai_semantic.map(payload) FROM semloom_documents;
```

The gateway refuses to replace an existing filesystem entry and removes only the socket it created. The UDS
adapter requires a UTF8 database and makes the socket nonblocking before `connect()`. Protocol errors are
fail-closed and do not persist or report task payload text.

The planner hook must be loaded before a statement containing the marker is planned. The regression
script loads the library in its session. A persistent deployment must put `semloom_pg` in
`session_preload_libraries` or `shared_preload_libraries`; merely installing the SQL function is not
sufficient.

Build and test against an exact `REL_18_3` installation. The Makefile rejects a `pg_config` whose
reported version is not 18.3:

```bash
make PG_CONFIG=/path/to/postgresql-18.3/bin/pg_config
make PG_CONFIG=/path/to/postgresql-18.3/bin/pg_config install
make PG_CONFIG=/path/to/postgresql-18.3/bin/pg_config installcheck
```

`installcheck` uses the standard PGXS regression harness and expects an already running PostgreSQL
18.3 server. Set `PGHOST`, `PGPORT`, and `PGUSER` for that server as needed. Existing PostgreSQL 18.4
deployments are compatibility/rehearsal environments and do not substitute for this qualification. The
PostgreSQL build must include `--enable-tap-tests`; run the test target as a non-root user because TAP creates
an additional temporary cluster.
