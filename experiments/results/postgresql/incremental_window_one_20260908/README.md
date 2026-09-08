# PostgreSQL generated Map through the incremental core, window one

Status: real-model run and final-source checks passed; owned services stopped.
This is the [window-one PG connection](../../../plans/semloom_incremental_session_design.md#pg-map-window-one),
not PG multi-in-flight execution or a performance experiment.

## Implementation and ownership

PG keeps its existing generated Map semantics, input evaluation, NULL/error handling, result binding
and synchronous v5 port. The opt-in `incremental-map-window-one` GUC selects the distinct execution ID
`semloom.provider.incremental-map-window-one.uds.v5`; its digest differs from the old fixed adapter.
The gateway flag of the same name routes each validated request through SessionEngine,
WorkWindowOrganizer and BoundedAsyncBackend. One gateway control thread owns one engine and one active
connection. There is no per-query engine replica or second RequestAdmission ledger.

The model adapter performs actual asynchronous HTTP with a total configured deadline, bounded raw
response reads, redirects disabled, no environment proxy inheritance, and no HTTP retry. It shares the
strict completion decoder with the old synchronous adapter. The PG port waits for one result; while it
waits, the gateway watches for peer closure and service stop. Cancelled work stops producing PG output,
but its reservation survives until the authoritative response. Unknown transport outcomes quarantine
the engine and stop new admissions; socket closure is not treated as model cancellation acknowledgement.

The code contains short comments for those ownership transitions. Planner/input binding code was not
changed in the final candidate. Recording paths retain their previous behavior; the new generated-Map
profile does not implement semantic Filter or composition through the incremental core.

## Source and controlled checks

The candidate extends `1f06b97e` on `codex/incremental-session`. The remote working tree was populated
on the older base `2d2dee35`; that Git value alone is not its source identity. All
[628 non-Markdown source/test files](raw/prepare-source-hashes.json) match the final local candidate.
The built and installed extension binaries match the [recorded SHA-256](raw/source-identity.json).
PostgreSQL is an isolated relocated 18.3 installation; existing installations and services were not replaced.

| Check | Result |
|---|---|
| PostgreSQL message, adapter and C contracts | [115 pass](raw/python-postgres-final.log.txt) |
| Provider contracts, including incremental bridge | [23 pass](raw/python-provider-final.log.txt); six bridge tests also [rechecked](raw/python-incremental-final.log.txt) |
| HTTP budget and session observation contracts | [9 pass](raw/python-observer-final.log.txt) |
| Extension build | [-O2 -Werror succeeds](raw/build-final.log.txt) |
| PGXS SQL regression and all 11 TAP files | [1 regression and 1926 TAP checks pass](raw/pg-final.log.txt) |
| Changed Python files | Ruff formatting and lint pass |

The new TAP test covers top-level generated Map SELECT/INSERT, NULL and LIMIT zero controls,
non-inlined strict input evaluation under LIMIT, child row/result association, independent writeback
inspection, and explicit rejection of semantic Filter with the new profile. Existing tests continue to
cover supported SQL shapes, permissions, snapshots, cancellation and old wire behavior. The bridge's
controlled backend additionally checks late-response draining, next-query isolation, unknown-outcome
quarantine, invalid completion release, distinct identities and initialization-failure thread cleanup.

Commands use the configured driver interpreter from the repository root:

```sh
python -m unittest discover -s code/tests/postgres -t code -p 'test_*.py'
python -m unittest discover -s code/tests/execution_provider -t code -p 'test_*.py'
PYTHONPATH=code python -m unittest tests.experiments.test_choice_http_observer tests.experiments.test_gateway_observation tests.experiments.test_gateway_session_isolation
make -C code/postgres/semloom_pg PG_CONFIG=<pg-prefix>/bin/pg_config PG_CFLAGS='-O2 -Werror'
```

PGXS `installcheck` runs as the cluster owner against an isolated socket-only cluster. The raw test
log records the regression and TAP invocations. Core/text [preflight](raw/preflight-summary.json) is
read-only; private environment reports remain outside Git.

## Retained diagnostic failures and corrected interpretation

No failed real-model run occurred. Before model execution, the new controlled tests required corrections:

- The NULL assertion wrapped Map in `IS NULL`, then a query used `ORDER BY`; both are outside the
  existing supported generated-Map shape. The [NULL](raw/failure-null.log.txt) and
  [sort](raw/failure-sort.log.txt) failures were retained. Tests now use a top-level Map projection.
- The reused HTTP fixture accepted only Filter's eight-token/newline-stop configuration. Its
  [rejection](raw/failure-filter-fixture.log.txt) led to a separate strict Map mode; old Filter checks
  were not relaxed. One test revision also had a retained [Perl quoting error](raw/failure-test-syntax.log.txt).
- A [cumulative sequence value of two](raw/failure-cumulative-counter.log.txt) was initially mistaken
  for duplicate LIMIT evaluation. The [diagnostic plan](raw/diagnostic-input-plan.log.txt) shows the
  SQL function had been inlined and the earlier NULL control had already advanced the sequence.
  Temporary planner changes were withdrawn. A non-inlined PL/pgSQL STRICT helper now isolates the
  intended checks: zero controls do not evaluate it and LIMIT 1 evaluates it once.
- Passing a new observer keyword to legacy callback stubs caused a
  [compatibility failure](raw/failure-observer.log.txt). The argument is now passed only in incremental
  mode, and the existing observation suite passes. The async POST observer separately verifies durable
  reservation before network send, including refusal when persistence fails.

TAP cleanup now also stops its owned HTTP/gateway processes after an unexpected test exit. This record
does not present the withdrawn planner hypothesis as a fixed database defect.

## Real model: nine requests, no retry

A fresh localhost Qwen2.5-7B-Instruct service uses revision
`a09a35458c702b33eeacc393d103063234e8bc28`. Its nine model files were rehashed; see
[model identity](raw/prepare-model-verification.json) and [service identity](raw/prepare-vllm-identity.json).
The single-GPU BF16/eager/FCFS configuration is unchanged from the preceding checks, with model context
4096 and separate driver/model environments. Normal generations allow 128 output tokens; cancellation
uses 256. Temperature is zero. HTTP and PG statement deadlines are 120 seconds.

| Phase | Actual requests | Check |
|---|---:|---|
| Old synchronous reference SELECT | 2 | Two nonnull rows; NULL preserved; raw completion equals SQL value |
| Incremental SELECT | 2 | Same inputs, prompt configuration and output values as reference |
| Incremental INSERT | 2 | Separate connection verifies written original/result columns |
| Target constraint error | 1 | SQL transaction fails and leaves no partial rows |
| Cancel long generation | 1 | Client receives SQLSTATE 57014 after dispatch |
| Query after cancellation | 1 | Same gateway drains old work and returns the new row |

Both profiles also pass NULL, LIMIT 0 and EXPLAIN controls without model calls. The strict input helper
runs twice for the two nonnull SELECT rows, not for the NULL row. Original child row order and NULLs are
preserved; this does not claim support for an explicit ORDER BY around generated Map.

The [summary](raw/run-summary.json), [reference HTTP observations](raw/run-reference-events.jsonl),
[incremental HTTP/core observations](raw/run-incremental-events.jsonl) and
[durable ledger](raw/prepare-budget.jsonl) agree on nine requests. Seven pass through the incremental
core. Five incremental sessions drain to zero for all resource counters. The cancelled response only
settles its old task; no completion is returned to that cancelled consumer. The final transport-close
event reports a joined I/O thread and zero resources. Actual prompt/output token usage is checked against
the tokenizer and request limit for returned completions; cancelled output is not a semantic-quality sample.

The [controller](raw/prepare-controller-summary.json) confirms PG/driver success, model process exit,
closed model port and both GPUs back at 1 MiB. The driver verifies PG and gateway socket cleanup; [postflight](raw/postflight.json) finds no surviving owned test processes.
The [archived driver](raw/prepare-real_check.py.txt) and [controller](raw/prepare-launch.py.txt) use
private settings; credentials, runtime files and headers are not exported. Artifact hashes are in the
[index](raw/artifact-index.json).

## What remains

The PG port is still synchronous window one. Expanding it requires versioned partial intake and result
correlation/reordering, PG-owned buffering, and separate checks of LIMIT, expression evaluation, errors,
snapshots and cancellation. Multiple active sessions, shared query groups, multi-member physical
requests, preparation-memory accounting, optimized methods and performance/quality experiments are
not completed here. A scalar work estimate remains a request quota, not measured GPU memory.


<a id="async-readiness"></a>

## Follow-up: can PG submit a second task? (2026-09-08)

Source review at `27729f34` found that the PG port still has only blocking `drive`, v5
advertises one in-flight task, and the bridge rejects incoming bytes while awaiting a result.
A new controlled test sends the beginning of another frame while the first backend request is
pending. The bridge rejects it, retains the active request credit, drains the late response,
and serves the next connection using the same engine. This confirms the current restriction;
it does not establish a production defect or multi-in-flight support.

The bridge suite passes 7 tests locally and 7 on Linux; the new test reuses the disconnect
lifecycle check. Linux core preflight passed. [Test output and test source identity](raw/readiness-tests.log.txt)
are separate from the earlier 628-file real-model manifest; production files are unchanged.
The initial local invocation used the wrong Python module prefix and failed before executing tests;
the corrected invocation is `PYTHONPATH=code python3 -m unittest tests.execution_provider.test_incremental_map -v`.
A standalone `ruff` executable was unavailable on the server, so that check was not completed.
No PG or model service was started in this follow-up, and there were zero new model requests.
The earlier nine requests prove window one only. The required PG work is recorded in
[the binding design](../../../plans/postgresql_call_binding_design.md#pg-async-readiness).
