# `semloom_pg` capability spike

`semloom_pg` is the current `REL_18_3` reference capability slice from
`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`. It proves that a
fail-closed SQL marker can be lowered to a planner-visible `CustomPath`/`CustomScan` with an ordinary
PostgreSQL child plan. It now includes a deterministic exact-SemFilter semantic contract, but it does not
call a real model, HTTP, Ray, vLLM, or a SemLoom scheduling backend.

The current supported query shape is deliberately narrow:

- one top-level `ai_semantic.map(text)` in a single-table `SELECT` target list;
- one top-level `ai_semantic.filter(text)` base-relation predicate in `WHERE`; exact `true` emits the
  tuple, while `false`, `unknown`, and SQL `NULL` drop it without letting the provider create rows;
- one top-level `ai_semantic.filter(text,text,jsonb)` exact-reference predicate. The planner requires a
  non-NULL constant instruction and exactly `model`, numeric-zero `temperature`, and integer `max_tokens=8`;
  the external golden adapter returns fixture-bound raw output and PostgreSQL alone parses exact uppercase
  `TRUE`, `FALSE`, or `UNKNOWN`;
- direct single-table `INSERT ... SELECT` for either reference operator, without `RETURNING`,
  `ON CONFLICT`, or `OVERRIDING`;
- ordinary child filters and projections;
- forward execution with child order preserved;
- `LIMIT`, including `LIMIT 0` and early stop; SemFilter is placed below `LIMIT`, so keep/drop is
  evaluated before the limit consumes a row;
- an in-process recording transform that returns `recorded:<input>`; PostgreSQL applies
  `PROPAGATE_NULL` locally without opening a provider session; the recording SemFilter echoes the
  deterministic `true`/`false`/`unknown` decision fixture;
- an optional external UDS recording provider with the same SQL-visible output.
- an external UDS golden provider for the three-argument path; unknown payload digests fail closed and the
  adapter never interprets the instruction or decides tuple cardinality.

The planner rejects joins, inheritance, subqueries, CTEs, aggregates, grouping, windows, `DISTINCT`,
set operations, row locks, set-returning targets, nested/multiple marker use, and combined SemMap/SemFilter.
SemMap remains target-list-only and rejects sorting; SemFilter remains one top-level `AND` predicate and
allows ordinary predicates, `ORDER BY`, and `LIMIT`. The executor rejects backward scan, mark/restore,
rescan, and EPQ. Parallel execution is disabled. The version-2 UDS protocol is deliberately synchronous
with one in-flight task, a 1 MiB frame limit, and a conservative 174,080-byte input limit applied before
JSON encoding. Three separate digests
bind SQL-visible semantic spec, database-selected physical algorithm, and concrete provider execution profile;
PostgreSQL's physical column number is not part of any wire identity. The strict version-3 contract has its
own schema and 163,840-byte input limit while version 2 remains frozen. Accepted-prefix backpressure,
multiple in-flight tasks, out-of-order completion handling, automatic retries, real model calls, and a second
physical path remain pending; this slice must not be described as a complete optimized database AI operator.

The planner serializes two strict named-field schemas. Schema 1 preserves the recording compatibility paths.
Schema 2 owns the exact Filter instruction, prompt/parser identities, model and fixed generation constraints,
NULL/error/order policy, physical algorithm/role, and semantic/physical digests. The executor rejects missing,
duplicate, unknown, mistyped, oversized, or unsupported fields before provider I/O; the input column remains
a separate binding and is not hashed. `PgSemanticRuntime` is the only PG-private plan-to-provider conversion
point. Exact EXPLAIN exposes the semantic spec, prompt/parser IDs, model, physical algorithm and role without
printing the instruction, input, raw output, socket path, or credentials. Quality, cost, fallback and second-path
fields remain pending because no current consumer uses them.

`sem_scan.c` is a thin CustomScan adapter, and `sem_pump.c` owns child-slot/value binding and flow. The shared
PostgreSQL-private `PgSemanticRuntime` fixes and lazily opens the provider, owns task sequence, copies
session-owned completions into per-tuple memory, registers query cleanup, maps neutral errors, and reports
common EXPLAIN counters. `SemMapMachine` and `FilterMachine` use a PostgreSQL-independent header and return
emit/drop/error after canonical task construction or raw completion parsing; they know no slot, Datum,
MemoryContext, provider session, or cleanup type. The runtime calls the provider-neutral
`AiOpenSpec → AiPreparedTask → AiCompletion` `open/drive/close` contract in `ai_provider_port.h`; that header
contains only fixed-width values, byte slices, caller-owned errors, and opaque provider/session handles, with
no PostgreSQL headers or types. The in-process recording adapter and UDS adapter implement the same contract,
while socket, JSON, digest, and framing details remain in the UDS/wire-private modules. Wire v3 reuses the
bounded framing/JSON primitives but has independent strict open/task/completion fields and digest domains.
`AiProviderError` exposes stable neutral categories, `errno`, a byte-limit parameter, and bounded local redacted
detail. It no longer exposes socket/JSON/frame/response-field operations; adapters generate safe detail and the
runtime maps only neutral categories to PostgreSQL SQLSTATEs, never treating detail as a format string.

Provider selection and its opaque configuration snapshot are query-fixed, but no session or FD is acquired
until the first non-NULL task. A cleanup callback is registered in `estate->es_query_cxt` before lazy open can
obtain a resource. Returned provider errors make the session terminal and close it before PostgreSQL raises the
preserved SQL error; PostgreSQL interrupts, out-of-memory errors, and other direct longjmps remain cleanup-safe.
Normal close and the callback share an idempotent local close path that invalidates the FD before releasing it
and performs no protocol I/O, wait, allocation, or error reporting.

The PGXS regression covers both operators' exact rows, EXPLAIN identity, ordinary predicates/projections,
duplicate payloads, expression inputs, three-valued Filter decisions, `NULL`, `LIMIT`, direct insert
rollback/commit, error recovery, and fail-closed unsupported combinations. TAP starts isolated PostgreSQL
nodes and covers missing preload, prepared/generic plans and invalidation, repeatable-read snapshots,
transactions/savepoints, RLS/permissions, two backend sessions, child/provider cancellation, insert variants,
and recovery. It also verifies that plain `EXPLAIN`, `LIMIT 0`, zero-row children, and NULL-only input do not
connect. Both recording adapters are compared for Map and Filter SQL rows and normalized EXPLAIN output.
UDS fault tests cover malformed JSON, invalid encoding, integer validation, evidence mismatch, disconnect,
cancellation during response and saturated-connect waits, recovery, input bounds, and socket cleanup. They
explicitly reject escaped JSON NUL, raw NUL inside a length-delimited frame, and fractional integer fields
while preserving the redacted `08P01` boundary.

The exact-18.3 qualification for commit `e89060a7` passed 193/193 TAP checks, PGXS regression 1/1, an
`-Werror` build, and 20/20 Python/static checks. Its clean-build `semloom_pg.so` SHA-256 is
`a2fc37c372ff0bd892e1e75e3a404d7688d85291e6ad13151e786ad7cdeb4ec0`. A repository-external resource smoke
consumed 2,000 100,000-byte Map inputs (200,018,000 output bytes) with backend RSS
21,340/22,172/22,172 KiB and FD 43/43/41, then evaluated 20,000 Filter rows (15,000 non-NULL tasks, 5,000
emitted rows) with RSS 22,172/22,204/22,204 KiB and FD 43/43/41. These are start/peak/end observations that
support absence of cumulative-payload memory growth and FD leakage; they are not performance results.

The behavior-preserving gateway migration at commit `868430f9` passed the same exact-18.3 PGXS regression
1/1 and TAP 193/193 checks, plus 25/25 Python migration/protocol/static checks, an `-Werror` build, and the
C11-neutral-header compile. Its unchanged `semloom_pg.so` SHA-256 is
`a2fc37c372ff0bd892e1e75e3a404d7688d85291e6ad13151e786ad7cdeb4ec0`. The repository-external resource smoke
produced 200,018,000 Map output bytes with RSS 21,368/22,248/22,248 KiB and FD 42/42/41, then 5,000 Filter
rows with RSS 22,248/22,248/22,248 KiB and FD 43/43/41. The migration changes module ownership only; it does
not add wire v3, model execution, or performance evidence.

Work package 4A is implemented by commit `3b2077e1`. Its exact PostgreSQL 18.3 qualification passed an
`-Werror` build, regression 1/1, TAP 268/268, 32/32 gateway/v2/v3/static checks, and the neutral C11 header
compile. The same run first preserved the recording compatibility paths, then covered three-argument planner
constants/options, prompt and digest vectors, TRUE/FALSE/UNKNOWN/NULL cardinality, duplicate inputs, model/
usage/evidence mismatches, prepared generic plans, no-task execution, cancellation and recovery. Repository-
external resource smokes observed Map RSS 20,932/21,792/21,792 KiB and FD 27/27/25; recording Filter RSS
21,792/21,792/21,792 and FD 27/27/25; and 20,000-row exact Filter RSS 17,636/17,636/17,636 and FD 25/25/25.
These start/peak/end observations are resource-lifecycle evidence, not model correctness or performance data.

The in-process provider remains the default. To exercise the external recording boundary, start the canonical
gateway from the repository root with an absolute socket path and set the superuser-only GUC for the SQL session:

```bash
python3 code/scripts/services/run_execution_provider_gateway.py \
  --socket /absolute/path/semloom-recording.sock
```

The historical `code/postgres/semloom_pg/gateway/recording_gateway.py` path remains a bootstrap-only
compatibility CLI for TAP and existing callers; it contains no protocol or server logic.

```sql
SET semloom_pg.gateway_socket = '/absolute/path/semloom-recording.sock';
SELECT ai_semantic.map(payload) FROM semloom_documents;
SELECT doc_id FROM semantic_decisions WHERE ai_semantic.filter(decision);
```

For the deterministic three-argument path, pass a test-owned JSON object mapping semantic payload SHA-256
values to raw `TRUE`, `FALSE`, or `UNKNOWN` outputs:

```bash
python3 code/scripts/services/run_execution_provider_gateway.py \
  --socket /absolute/path/semloom-golden.sock \
  --golden-fixture /absolute/path/golden-fixture.json
```

```sql
SET semloom_pg.gateway_socket = '/absolute/path/semloom-golden.sock';
SELECT doc_id
FROM documents
WHERE ai_semantic.filter(
  payload,
  'The input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb
);
```

The fixture is a deterministic contract test input. It is not a model endpoint or a quality oracle.

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
