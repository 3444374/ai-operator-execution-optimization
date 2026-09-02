# `semloom_pg` capability spike

`semloom_pg` is the current `REL_18_3` reference capability slice from
`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`. It proves that a
fail-closed SQL marker can be lowered to a planner-visible `CustomPath`/`CustomScan` with an ordinary
PostgreSQL child plan. It includes a deterministic exact-SemFilter semantic contract and a gateway-side fixed
OpenAI-compatible model adapter. It does not put HTTP in the PostgreSQL backend or implement Ray/SemLoom
scheduling, asynchronous execution, or a second physical path.

The [opt-in choice generation profile](../../../experiments/plans/completed/postgresql_choice_profile_engineering.md)
now has PostgreSQL plan support (`00cc6bbf`). Adding the fourth option
`"generation_profile":"semloom.generation.choice.tristate.v1"` saves a complete ordered profile in schema 3.
Ordinary EXPLAIN displays its ID, version, choices, digest and unqualified status; prepared/generic plans retain
the identity. Old three-field options still use schema 2, unchanged digests and wire v3.
The C codec, `AiOpenSpec` mapping and gateway wire v4 now execute choice SELECT through the same runtime.
The temporary plan-only branch has been removed; no-task queries still avoid provider connections.
It never silently uses v3, and the original result parser remains strict.
The bounded C encoder in `src/generation_profile.{h,c}` is now linked into PGXS. Its canonical bytes match
the Python value and an independently hashed vector; the complete bytes enter the new semantic digest.
Old calibration artifacts cannot match it. This planning support does not qualify model quality or resume calibration.
The [PG connection qualification](../../../experiments/results/postgresql/choice_pg_wire_20260902/README.md)
at `80bb7fc5` passes 83/83 Python contracts, PG18.3 warning-free build, regression 1/1 and TAP 748/748.
The external config must explicitly opt in with `"choice_format":"vllm_structured_outputs"` before v4 can
send a constrained HTTP request. Missing support or HTTP rejection never triggers an unconstrained retry.
These fixture results prove the supported PostgreSQL choice SELECT execution, not a real endpoint's constrained
decoding or RSS/FD growth limits. The later [controlled resource run](../../../experiments/results/postgresql/choice_resources_20260902/README.md)
passes at the declared fixture scales, including cancellation and blocked DNS recovery. The subsequent
[real-service check](../../../experiments/results/postgresql/choice_service_20260902/README.md) completes
14 old/choice requests and two NULL controls on PG18.3 with Qwen2.5-1.5B/vLLM 0.25.1. All reported usage and
SQL dispositions match; 15/100 cumulative attempts include one failed token-count audit before its correction.
This completes choice engineering qualification on the development branch, not model quality or calibration.
The later [Filter INSERT qualification](../../../experiments/results/postgresql/semfilter_insert_20260902/README.md)
at `39007150` passes PG18.3 regression 1/1, TAP 919/919 and 83/83 Python checks. The planner now handles a
pulled-up INSERT source while keeping the ordinary PostgreSQL write node. New tests cover recording/exact/choice
write results, rollback, savepoint recovery, target constraints, permissions and cancellation using fixtures.
After the full engineering comparison and completed choice checks, real generative SemMap drives the necessary shared
task/result changes. Composable execution and bounded sessions follow, including two Filter conjuncts and Filter → Map;
recording Map remains unchanged. The independent SemLoom
core may be developed with fixtures before Filter qualification, but its PG integration needs separate validation.

This extension remains the project's own frontend; the company demo is an engineering reference, not its
replacement or a limit on this system's capabilities. Future transfer includes operator semantics, processing
and optimization as well as SemLoom execution/scheduling; a company adapter is only one part of that work.
That transfer is not implemented. Ownership and early mapping checks belong to the
[frontend-adapter design](../../../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#frontend-adapter-strategy).

The current supported query shape is deliberately narrow:

- one top-level `ai_semantic.map(text)` in a single-table `SELECT` target list;
- one top-level `ai_semantic.filter(text)` base-relation predicate in `WHERE`; exact `true` emits the
  tuple, while `false`, `unknown`, and SQL `NULL` drop it without letting the provider create rows;
- one top-level `ai_semantic.filter(text,text,jsonb)` exact-reference predicate. The planner requires a
  non-NULL constant instruction and exactly `model`, numeric-zero `temperature`, and integer `max_tokens=8`;
  the external golden adapter returns fixture-bound raw output and PostgreSQL alone parses exact uppercase
  `TRUE`, `FALSE`, or `UNKNOWN`;
- direct single-table `INSERT ... SELECT` for recording SemMap and recording/exact SemFilter, without `RETURNING`,
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
- an external UDS fixed-model provider for the same three-argument path; the gateway sends one non-streaming
  OpenAI-compatible request per task without retry, while PostgreSQL still validates evidence and parses the
  raw result.

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
multiple in-flight tasks, out-of-order completion handling, automatic retries, and a second
physical path remain pending; this slice must not be described as a complete optimized database AI operator.

The planner serializes three strict named-field semantic schemas. Schema 1 preserves the recording compatibility paths.
Schema 2 owns the exact Filter instruction, prompt/parser identities, model and fixed generation constraints,
NULL/error/order policy, physical algorithm/role, and semantic/physical digests. Schema 3 adds the self-contained
choice profile and uses wire v4. The executor rejects missing,
duplicate, unknown, mistyped, oversized, or unsupported fields before provider I/O; the input column remains
a separate binding and is not hashed. `PgSemanticRuntime` is the only PG-private plan-to-provider conversion
point. Exact EXPLAIN exposes the semantic spec, prompt/parser IDs, model, physical algorithm and role without
printing the instruction, input, raw output, socket path, or credentials. Exact reference plans carry a separate,
strict third `custom_private` element for cost model ID, model role, semantic input rows, output selectivity,
estimated model calls, prompt/output tokens, calibration identity/applicability, predicted service milliseconds,
held-out error, and AI work cost; these fields do not enter either semantic digest. A planner-only loader can replace
the visible uncalibrated heuristic with a strictly matched static reference artifact. Quality policy, fallback, and
second-path fields remain pending because no current consumer uses them.

`sem_scan.c` is a thin CustomScan adapter, and `sem_pump.c` owns child-slot/value binding and flow while forwarding
planner-computed cost metadata to EXPLAIN; it does not calculate cost during execution. The shared
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
while preserving the redacted protocol-violation SQLSTATE `08P01`.

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

The behavior-preserving 4A.1 hardening is commit `359ffdf3`. A real `wire_common.c` now owns bounded
framing, interruptible socket/connect waits, and PostgreSQL JSON primitives; v2 and v3 retain only their
versioned schemas, identities, digests, and error interpretation. Wire v3 validates the exact four-field error
object, protocol version, nullable/open or decimal/task sequence, and a versioned redacted code allowlist.
The selected provider publishes a query-fixed neutral input limit, so the runtime rejects oversized input
before the machine scans or allocates canonical-message JSON while the UDS adapter keeps its defensive check.
Exact PostgreSQL 18.3 passed the warning-free `-Werror` build, regression 1/1, TAP 320/320, 33/33 PostgreSQL
protocol/static checks, 5/5 gateway migration checks, and neutral/machine C11 compilation. The added TAP paths
cover C-to-Python Unicode instruction/input, empty non-NULL input, exact savepoint recovery, valid open/task
errors, and malformed/missing/extra/mismatched v3 error frames. The earlier resource observations remain bound
to `3b2077e1`; this hardening run does not add model-quality, performance, or new RSS/FD claims.
The repository-external artifact bundle `postgresql_semfilter_4a1_hardening_359ffdf3_20260831` preserves the
final TAP and PostgreSQL logs, byte-identical regression actual/expected files, commit identities, and SHA-256
manifest. It also contains a clean detached-`359ffdf3` build log, exit status 0, and resulting extension binary
from `COPT='-O2 -Werror'` with an explicitly selected PostgreSQL 18.3 `pg_config`. The bundle still verifies
after removal of the temporary worktrees. The slice-specific stale resource-test gateway and socket were also
stopped and removed; this statement does not cover unrelated server workloads.

Work package 4B is implemented by commit `53cf3da8`. Golden and fixed-model completions share one strict
wire-v3 session runner, and PostgreSQL snapshots `semloom_pg.provider_execution_profile` at query start to
select `golden` or `openai-compatible-fixed`. The profiles use distinct provider execution digests and safe
EXPLAIN names; endpoint URL, model, timeout and optional bearer-token environment name stay in a strict
repository-external JSON file. Model-unavailable/timeout/request-rejected/invalid-response/internal failures
map to stable redacted `08006`/`38000`/`08P01`/`XX000` errors.

Exact PostgreSQL 18.3 passed a warning-free `-O2 -Werror` build, regression 1/1, TAP 404/404, 45/45
Python/static contracts, and neutral/machine C11 compilation. Fixed-profile TAP covers valid keep/drop,
returned-model identity, invalid raw output, HTTP 4xx/5xx, invalid JSON, timeout, savepoint recovery,
PostgreSQL statement cancellation, fresh sessions, EXPLAIN and `LIMIT 0`. The repository-external bundle
`postgresql_semfilter_4b_fixed_model_53cf3da8_20260831` preserves these outputs and the failed setup attempts.
A small Qwen2.5-1.5B-Instruct/vLLM 0.25.1 run returned only the `yes` row from `yes/no/NULL`, with a separately
saved raw `TRUE` completion and usage. This is capability evidence only, not a model-quality or performance
claim; earlier 4A RSS/FD observations remain bound to `3b2077e1`.

The final 4B.1 boundary hardening is commit `ef314618`, including `a4319655`. Fixed-model requests reject
301/302/303/307/308 without following Location or forwarding bearer credentials. A single monotonic deadline
bounds DNS resolution, connect/TLS, request send, response headers, and response body; timeout remains a terminal
`MODEL_TIMEOUT` and does not retry. The equivalent server source tree passes 48/48 Python/static contracts,
PostgreSQL 18.3 warning-free `-O2 -Werror`, regression 1/1, TAP 404/404, and neutral/machine C11 compilation.
The repository-external bundle `postgresql_semfilter_4b1_http_hardening_ef314618_20260831` preserves source and
tracked-diff identities, raw logs, byte-identical regression outputs, the extension binary, and a verified
SHA-256 manifest. This run does not add model-quality, performance, or RSS/FD evidence.

Commit `47407751` adds exact-reference cost/cardinality observability. The planner rebuilds semantic input
rows from table cardinality and ordinary restrictions after excluding the semantic marker, estimates NULL-adjusted
model calls, and uses PostgreSQL average input width plus the fixed prompt contract for an explicit prompt-token
heuristic. Output work uses the plan's eight-token cap, and `cpu_operator_cost` converts calls plus prompt/output
tokens to a provisional PostgreSQL path-cost term. Plain `EXPLAIN` reports `AI Cost Model`, `Model Role`, `Semantic Input
Rows`, `Output Selectivity`, estimated calls/tokens, and `AI Work Cost`; `EXPLAIN ANALYZE` additionally reports actual
`Model Calls`, `Prompt Tokens`, and `Output Tokens` from validated provider completions. This engineering estimate is
not matched reference calibration and must not be used to compare a second path; the planner still creates only the
reference path.
Exact PostgreSQL 18.3 passes warning-free `-O2 -Werror`, regression 1/1, TAP 414/414, 49/49
Python/static+migration contracts, and neutral/machine C11 compilation. The repository-external bundle
`postgresql_semfilter_cost_cardinality_47407751_20260831` preserves source hashes, raw qualification logs,
byte-identical regression outputs, the extension binary, statuses, and a verified SHA-256 manifest.

Commit `71a8ef7d` makes that limit machine-visible: `AI Cost Model` is
`semloom.exact_filter.uncalibrated.v1` and `AI Cost Calibration` is `unavailable`. It also rejects an explicit
endpoint port zero and shares at most one in-flight DNS resolver attempt per fixed adapter, so repeated timeout
failures cannot accumulate one worker each. A blocked system resolver is not cancellable by Python; the caller still
returns on its deadline and resolver work remains bounded. Exact PostgreSQL 18.3 passes warning-free `-O2 -Werror`,
regression 1/1, TAP 415/415, 49/49 Python/static+migration contracts, and neutral/machine C11 compilation. The
repository-external bundle is `postgresql_semfilter_gap_hardening_71a8ef7d_20260901`.

Commit `dcde2be5` implements the planner-side matched-reference calibration mechanism without changing runtime,
provider, wire, semantic digest, or SQL behavior. A pure-Python offline builder accepts strict training/held-out
reference observations and separately derives output selectivity, calls per input, prompt/output tokens per call,
and nonnegative fixed/call/prompt/output service-time coefficients. It rejects duplicate/unknown fields,
non-canonical values, underidentified service data, negative coefficients, identity tampering, or held-out error above
the registered limit. PostgreSQL reads only an explicitly selected absolute artifact path during planning, caps it at
64 KiB, independently verifies the 29-field schema and cross-language artifact identity, and matches semantic/
physical digest, model, reference role, and query-fixed provider profile. Missing, malformed, escaped-NUL, duplicate,
or mismatched artifacts retain the executable uncalibrated exact reference path with a stable redacted reason.
Matched plans save artifact, workload, and service identities and use predicted service milliseconds as the explicit
AI path-cost unit; prepared plans retain the copied values and execution never reopens the artifact.

The exact PostgreSQL 18.3 qualification for `dcde2be5` passed a clean warning-free `-O2 -Werror` build, regression
1/1, TAP 437/437, 55/55 Python/static/gateway contracts, Python compilation, and neutral/machine C11 compilation.
The repository-external bundle `postgresql_semfilter_reference_calibration_dcde2be5_20260901` preserves raw logs,
byte-identical regression actual/expected output, source and commit identities, the extension binary, status files,
and a verified SHA-256 manifest. Its deterministic calibration fixture proves builder/loader/control-flow parity,
not empirical accuracy for a real model, workload distribution, service, or hardware. A real matched artifact must
still be collected and held-out qualified before implementing a second physical path.

The [first real collection on 2026-09-01](../../../experiments/results/postgresql/semfilter_reference_calibration_20260901/README.md)
completed 64 warm-up inputs, then stopped on the 23rd response of the first training query: the fixed model returned
an invalid tristate value and PostgreSQL 18.3 raised `22000`. No complete training observation, held-out measurement,
fit, or artifact resulted. Production code and the predeclared 20% error limit were unchanged. The next collection
is paused for three independent checks: reference output qualification (including a separately versioned constrained
decoding candidate), ordinary PostgreSQL multicolumn statistics, and builder identifiability. The builder now scales
each design column by its maximum magnitude, forms and inverts the Gram matrix with exact rational arithmetic,
and rejects singularity or an infinity-norm Gram condition of at least `1e16`. Checking only individual pivots
missed a chained near-dependence fixture. This engineering limit is not a held-out error limit or an SVD condition
number. Ten local calibration tests cover exact/near/joint dependence and valid unit/row-order changes.
Production SQL, the strict C parser, and wire v3 do not yet support the constrained-decoding candidate.

The [independent qualification slice](../../../experiments/results/postgresql/semfilter_qualification_20260901/README.md)
at `6c111b24` passes PostgreSQL 18.3 `-Werror`, regression 1/1, TAP 437/437 and 59/59 Python contracts.
Ordinary multicolumn statistics correct the fixture's input estimate from 8 to 64 (actual 64). Native choice decoding
improves format acceptance from 27/30 to 30/30, but both profiles match only 12/27 predeclared semantic expectations.
The reference is therefore not qualified; full calibration remains paused, and the experimental choice manifest
must not be confused with a production PostgreSQL plan or a validated calibration artifact.
Final numerical hardening `44f6632c` additionally rejects chained near dependence using the full Gram condition.
It independently passes PostgreSQL 18.3 `-Werror`, regression 1/1, TAP 437/437 and 60/60 Python contracts
(10 calibration tests). It does not rerun the model or change the failed semantic qualification.

The subsequent [single-prompt comparison](../../../experiments/results/postgresql/semfilter_prompt_qualification_20260901/README.md)
verifies actual HTTP messages against the service and model chat-template token IDs. The new prompt passes only
5/9 old and 5/9 new independent cases on 1.5B, and 7/9 old and 6/9 new cases on matched 7B, each repeated three
times. No configuration qualifies; production SQL/plan/wire and the strict parser remain unchanged. Full collection
stays paused. Python contracts pass 60/60 again; PostgreSQL regression/TAP were not rerun in that diagnostic.

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

For a fixed OpenAI-compatible endpoint, create a repository-external configuration such as:

```json
{
  "endpoint_url": "http://127.0.0.1:8000/v1/chat/completions",
  "model_id": "<fixed-model-id>",
  "timeout_ms": 60000,
  "bearer_token_env": "SEMLOOM_MODEL_TOKEN"
}
```

Start the same canonical gateway with `--fixed-model-config /absolute/path/fixed-model.json`, then set both
query-scoped values before planning the exact Filter:

```sql
SET semloom_pg.gateway_socket = '/absolute/path/semloom-model.sock';
SET semloom_pg.provider_execution_profile = 'openai-compatible-fixed';
```

After building a repository-external reference artifact for the same semantic plan, model, provider profile,
workload distribution, and service signature, select it before planning:

```sql
SET semloom_pg.reference_calibration_file =
  '/absolute/path/reference-calibration.json';
```

Plain `EXPLAIN` then reports `AI Cost Calibration=matched`, the artifact/workload/service identities, predicted
service milliseconds, and held-out error. An unreadable, invalid, or mismatched artifact reports `rejected` plus a
stable reason and continues with `semloom.exact_filter.uncalibrated.v1`; it never silently calibrates a different
plan or provider profile. PostgreSQL does not infer live workload/hardware applicability from the opaque signatures:
the external deployment must select an artifact produced for the active conditions.

If authentication is not required, omit `bearer_token_env`; credentials never belong in SQL, argv, wire
messages, EXPLAIN, or the repository.

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
