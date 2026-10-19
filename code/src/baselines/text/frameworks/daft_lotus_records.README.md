# Daft Native Movie Q3 records

`daft_lotus_records.evaluate()` builds one lazy Daft 0.7.21 graph: typed CSV,
`id == taken_3`, projection, a process-based LOTUS preparation class, a separate
process-based inference class, and one final compact result iterator. Each
occurrence remains an independent model request. Duplicate text and review IDs
are preserved. It uses no Ray runtime, SemLoom pool, HTTP request replay or
custom submission queue.

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
`gpu_workers` remains one for this ordinary Q3 profile. The preparation and
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

Actual vLLM/model interpreter compatibility, single-GPU placement and physical
exit, throughput, overlap, fused layout and cascade selection remain pending.
This slice does not qualify performance, continuous admission or image inputs.
Shared runner and project status/report navigation changes are assigned to
the integration owner to avoid concurrent edits.
