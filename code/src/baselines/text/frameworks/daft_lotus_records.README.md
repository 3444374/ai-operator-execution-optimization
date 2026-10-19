# Daft Native LOTUS text Filter records

The independent [finite-chain adapter](daft_lotus_chain.py) uses the shared fused
LOTUS batch kernel in one native process class. Its FME/MF/MMF/FFM, error and
cleanup CPU qualification is recorded in the [slice report](../../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#共享批内核及-daftcore-有限链-cpu-增量).
The single-Filter lifecycle and frozen model results below retain their own source.

`daft_lotus_records.evaluate()` builds one lazy Daft 0.7.21 graph: typed CSV,
an optional source-declared equality, projection, a process-based LOTUS preparation class, a separate
process-based inference class, and one final compact result iterator. Each
occurrence remains an independent model request. Duplicate text and review IDs
are preserved. It uses no Ray runtime, SemLoom pool, HTTP request replay or
custom submission queue.

The source owns task binding through `preflight(method)` and returns a validated
`RecordScan` from `scan_spec(method)`. Its `csv_schema` fixes string/int64 types,
`payload_columns` selects text inputs, and `predicate` is one equality or `None`.
The backend passes those selected columns in a native struct batch to the
original LOTUS formatter. Column names and instructions belong to the source
and method. `MovieRecordSource` retains its Q3 binding; the backend also accepts
other ordinary text Filter sources. The example below uses the Movie wrapper,
whose query/evaluator contract remains Q3.

The driver binds the original LOTUS input-column order before worker allocation.
Workers validate the instruction's reference set and retain that bound order;
they do not recalculate its order from a process-local Python hash seed.

The preparation class uses the existing `LotusFilterMethod`; the private model
interpreter constructs the existing `VllmEngine`. The driver retains the
existing `CallLedger`, LOTUS accounting/parser and SemBench evaluator through
`run_movie_query()`. Results carry the entire engine row as JSON inside a native
struct, preserving additional engine fields without maintaining a second usage
schema. Existing preparation observations and native inference batch sizes are
carried separately from method statistics.

```python
from src.baselines.text.frameworks.daft_lotus_records import evaluate
from src.experiments.lotus_records_movie import run_movie_query

report = run_movie_query(
    source, checkout, method, generation,
    lambda: evaluate(source, method, engine, generation, resources,
                     query_id=query_id, runner_threads=runner_threads),
    query_id=query_id,
)
```

Run this query in a dedicated process under the existing
`lotus_records_supervisor.supervise()` for a finite wall deadline and observed
process cleanup. The shared CLI selector is left to the integration owner.

## Effective settings

`RecordsResources.cpu_workers` controls preparation actor processes;
`gpu_workers` remains one for this ordinary single-model profile. The preparation and
model batch sizes remain independent. Each model class requests one CPU and
one GPU from Daft. `max_active_batches` is not applied: native buffering belongs
to Daft. No `results_buffer_size` or unsupported `memory_bytes` setting is used
as a claimed memory limit.

The optional `runner_threads` controls the process-owned native runner and is
independent of those instance/batch settings. `None` uses the existing runner
selection. Conflicting later thread settings are rejected by the shared
`configure_daft_runner()` implementation.

`max_prepared_batch_bytes` limits UTF-8 `messages_json` bytes in the actual
inference batch, after `to_pylist()`. Row count is checked against the requested
model batch size. Both checks precede first model construction. These are
logical input checks, not limits on allocation, RSS or GPU memory. The private
JSON IPC additionally rejects frames larger than 64 MiB. The byte allowance
does not include JSON framing overhead, so that transport check remains
independent.

## Lifecycle and observations

Each inference instance owns one private process in `model_python`, inheriting
Daft's visible-device environment. It reuses that engine across batches of one
query. This adapter explicitly closes the engine after the query's expected
completion count; this is its workload policy, not a required Daft lifecycle or
a limit on native cross-query reuse. The worker records engine identity, PIDs, startup,
close, JSON encode/decode time and pipe bytes. `ipc_wait_seconds` includes engine
execution and must not be reported as isolated IPC overhead. These combination
costs incurred before the `query_seconds` boundary are included there; final
owner cleanup is reported at its actual time.

Cancellation interrupts private IPC waits and also reaps idle engines. Missing
responses remain `unknown` with unavailable usage. Cleanup errors remain
separate from query errors and completed rows. OS cleanup uses the existing
supervisor's process identity/group helpers. Reports confirm only observed OS
processes; GPU exit and unobserved detached descendants remain unconfirmed.

Private-process close uses a 15-second default total budget after acquiring its
operation lock: up to 11 seconds for the existing ten-second engine API and its
reply, with four seconds reserved for exit observation and TERM/KILL cleanup.
An explicit shorter positive finite timeout proportionally reduces the API
wait and cleanup reserve; an unconfirmed API remains `unknown`, even when OS
exit is confirmed. The close wire frame is unchanged. Completed or failed rows
retain their known usage independently of the cleanup result. The dedicated
outer supervisor remains responsible for a hard wall deadline, including lock
waits and process-table operations.

Malformed result batches retain uniquely associated actual terminals and partial
usage. Missing, duplicate or contradictory calls remain unknown; the original
IPC reply stays in `result_failure.raw_response_json`. The reserved
`engine_protocol_error` field prevents semantic success, including when its value
is empty. Duplicate nonempty native request IDs also enter this failure path.
The Daft close report exposes only original-query receipts that the iterator has
not delivered, through `unconsumed_call_results`; it never upgrades a delivered
unknown or compares a delivered row against engine-only preparation metadata.
If EOF cleanup fails before interpretation, the query ledger retains known
usage while unexecuted LOTUS statistics remain unchanged. See the source-bound
[CPU fault checks](../../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#engine-result-receipts-cpu-20261019).

A synchronous native scan, formatter or worker initialization can block before
its next cooperative check. Therefore `query_timeout_s` alone is not a hard
wall deadline for the whole graph. The caller must supervise the dedicated
query process. CPU formatter cancellation is checked again before inference
can allocate a model. Closing a driver-side Daft wrapper is never used as a
worker broadcast.

## Sources, checks and handoff

The implementation starts at `integration/data-execution@957e91eb` and reuses
its method/source/ledger/evaluator interfaces. Engineering decisions follow
the existing [architecture references](../../../../../experiments/plans/系统架构.md#company-engineering-reference):
one source for effective parameters, separate model computation and execution,
reuse expensive instances, retain query identity and cleanup outcomes. No
company source was read or copied.

Daft's installed 0.7.21 `udf/__init__.py`, `udf_v2.py`, `execution/udf.py` and
`native_executor.py` were inspected for lazy class invocation, process pools,
resource settings and iterator cleanup. The supplied candidate files were read
as reference text only; their code/tests were not executed or imported. The
implementation uses `.get()` for struct access, and does not adopt their
finalizer-only cleanup or driver-side close assumptions.

The CPU checks use the original Movie fixture, real LOTUS 1.2.4 preparation and
parser, the unchanged fixed SemBench evaluator and explicit fixed responses.
They cover actual process-mode placement, repeated queries, a 17-row split/tail
case, empty/null-looking/number-looking strings, association, bytes before
allocation, cancellation, startup failure, crash and cleanup failures. See
[Daft evidence](../../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/daft_verification.json).

The opt-in [generic CPU composition check](../../../../tests/baselines/text/test_lotus_records_generic_libraries.py)
uses two other text columns and an instruction shared across Ray Data, Daft,
the static reference and Core. It checks typed equality and no-predicate scans,
original LOTUS messages/parser/accounting, occurrence identity and existing
cleanup responsibilities with explicit fixed responses and synthetic usage.
Its worker seed deliberately produces a different original parser order from
the driver, so the check also exercises the serialized column-order binding.

Actual vLLM/model interpreter compatibility, single-GPU placement and physical
exit, throughput, overlap, fused layout and cascade selection remain pending.
This slice does not qualify performance, continuous admission or image inputs.
Shared runner and project status/report navigation changes are assigned to
the integration owner to avoid concurrent edits.
