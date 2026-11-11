# Native engines and private IPC

`vllm_protocol.py` owns engine/generation values, `AsyncEngineLimits` and result
conversion. Internal callers import shared values from this module; the native
async engine's `AsyncEngineLimits` import refers to the same class. Sync and
async engines construct their own native runtime. `async_engine_process.py` owns
the parent sockets, submitted inputs, result consumption and its original child;
`async_engine_worker.py` owns the child event loop. `async_engine_wire.py` encodes
frames and compact terminal records without transport or scheduling.

`EngineSpec` can explicitly forward native `enable_prefix_caching`,
`enable_chunked_prefill`, `disable_cascade_attn`, `cudagraph_mode` and
`cudagraph_capture_sizes`. Omitted values remain native defaults; graph options
enter the native `compilation_config`. These controls are experimental inputs,
not an adaptive policy. [Candidate evidence](../../../../experiments/results/diagnostics/semloom_execution_candidates_20261110/README.md)
separates constructor checks, CPU metadata and actual device runs.

## Native text prefill/decode handoff

`prefill_decode_process.PrefillDecodeProcess` is an opt-in synchronous owner of
two native vLLM 0.25.1 processes. `NixlPlacement` assigns each process one visible
GPU and a distinct localhost side-channel port. Both stages use the same
`EngineSpec`; `NixlConnector` owns KV memory, reads and native block release.
The model environment needs `nixl==1.3.0` separately from the driver.

P generates one disposable token and returns an opaque native transfer record.
D receives the **original full conversation** and the original `GenerationSpec`,
plus that record through native sampling `extra_args`. Its complete response is
the logical output. Neither prompts nor output budgets are split. Every result
retains `stage_receipts` for physical-call and usage accounting; do not count
only D when computing physical cost. Missing transfer or mismatched association
stops the owner, and native KV-load errors use `fail`, without automatic retries.

```python
from src.serving.backends.prefill_decode_process import NixlPlacement
from src.serving.backends.pipelined_prefill_decode import PipelinedPrefillDecodeProcess
from src.serving.backends.vllm_protocol import AsyncEngineLimits

owner = PipelinedPrefillDecodeProcess(
    engine, generation, private_control_dir, deadline_monotonic, startup_timeout_s=240,
    prefill=NixlPlacement("0", 5600), decode=NixlPlacement("0", 5601),
    limits=AsyncEngineLimits(max_inflight=32), batch_size=16, prefill_window=16,
)
try:
    results = owner.run_prepared(prepared_rows, generation=generation)
finally:
    close_report = owner.close(timeout=30)
```

The caller owns GPU allocation, memory fractions and the total deadline. CPU
staging is also explicit via `kv_buffer_device="cpu"`. `cancel()` wakes an active
pipeline and prevents further admission; `close()` stops and reports each process separately and does not claim
GPU exit. An unconsumed P handoff remains native-owned until its send expiry or
process shutdown. The serial implementation remains a functional control.
Dynamic routing, live decode migration, vision-encoder splitting and query-adapter
integration remain pending. Colocated execution remains available.
The `EngineSpec` memory fraction applies to **each** stage; a total fraction of
0.6 uses 0.3 per stage. Example capacities are illustrative and require calibration.

`pipelined_prefill_decode.PipelinedPrefillDecodeProcess` adds cross-request
concurrency over two `AsyncEngineProcess` owners. Pass explicit `limits`,
`batch_size` and `prefill_window`. One logical capacity covers P, waiting handoffs
and D; each P terminal can advance immediately, independent of its IPC batch peers.
Native vLLM owns physical continuous batching. A shared progress event wakes the
coordinator; there is no fixed polling sleep. Results restore input order and keep
both physical receipts. Failure stops admission, drains retained receipts and closes
both owners. This object accepts one call at a time and supports resident reuse.
`batch_size` is a member ceiling. If an encoded batch cannot fit the input-byte
allowance, the coordinator shortens its prefix before any member is accepted;
single requests remain complete and an oversized single item still fails.
This is frame packing, not model retry; other encoding errors are not retried.
Command-queue capacity release also wakes the coordinator, so a held request cannot
hide newly available submission space. The caller deadline is checked before and
after environment validation; process creation and the native READY wait honor
that deadline. Environment metadata probes retain their existing finite limits.
If the API close is unknown, a dead IPC parent still triggers the existing registered
process-group cleanup; its observed exit does not turn the unknown API result into
success. Final process reports retain both the direct-child and group scopes.
Registered-process cleanup uses the same absolute close deadline, with no extra
fixed wait after it. Unknown results keep their observed process identities for
a later close; physical process exit does not confirm the model API outcome.

The [resident worker](../../experiments/native_stage_pipeline.py) compares against
native `LLM.chat` on the complete request set, retains first outputs and every timed
repeat, and separates warmup and startup. Its caller supplies matched total resources
and the same external safeguards as the functional worker. The async native logger
captures NIXL updates even in iterations without output; absent or unsupported
metrics remain explicit. CPU IPC checks establish pipeline behavior, not GPU benefit.
The [single-GPU comparison](../../../../experiments/results/diagnostics/native_stage_handoff_20261110/README.md#async-pipeline-20261111)
also verifies native batching, real transfer totals and complete output parity.
Both tested eager request replays were slower than matched native colocated execution;
full queries, multiple GPUs and real GPU failure injection remain unqualified.

The [worker](../../experiments/native_stage_handoff.py) compares exact prompt IDs,
output IDs, text and finish reasons. Its caller supplies preflight, host lease,
persistent attempt budget, process supervision and final GPU observation.
Set `VLLM_LOGGING_LEVEL=DEBUG` for this diagnostic worker. It checks native
receive completion through the returned producer request ID and the decoder's
own finished receipt; it does not infer identity from internal ID suffixes.
Native cumulative NIXL metrics are also retained at close. In the observed sync
path those counters remained zero beside completed transfer logs, so they cannot
currently provide transfer bytes or duration. This does not affect the preserved
output and receive-completion checks.
See the [plan](../../../../experiments/plans/数据组织.md#native-stage-handoff)
and [evidence](../../../../experiments/results/diagnostics/native_stage_handoff_20261110/README.md).

Explicit result and control frame allowances may be below 4096 bytes. Frames
still have to fit their actual encoded bytes, and each submitted identity must
fit its compact terminal receipt. Defaults remain 2 MiB per result and 64 KiB
per control frame. After valid opening limits are parsed, error reports use the
same control allowance; an oversized error report does not confirm work or close.

## Async close timing

`AsyncEngineProcess.close(timeout=...)` keeps its existing total caller allowance
and direct-child cleanup reserve. Its `close` control frame now carries the actual
API-wait deadline, using the same local monotonic clock as request deadlines.
The worker supplies the remaining time to the native engine, rather than replacing
it with a fixed ten seconds. The private parent and child migrate together; the
old internal close frame without a deadline is not a separate supported format.
A drain still waiting for dispatch is not sent once closing starts; accepted
abort commands still go ahead of close.

A normal close response is emitted once, including an unconfirmed response.
An expired deadline reports unknown without claiming API shutdown. If a pipe or
worker error occurs before a normal response, finalization retains bounded
best-effort cleanup: the original one second when no caller close was received,
or only the remaining caller time otherwise. That emergency return does not emit
a successful close receipt or replace an earlier result.

Request outcomes, retained unknown reservations, and the separate engine/producer,
original-Popen and parent-I/O evidence required for retirement are unchanged.
Physical GPU and descendant exit are reported by their actual external owners.

Closing creates unknown outcomes only for requests still missing a terminal;
an empty owner or confirmed unconsumed results do not invent a transport error.
Existing transport errors and result reservations remain. `wait` can still await
results during close, but does not wait after parent I/O has ended.

## Sync close timing

`vllm_sync_process.EngineProcess` sends its existing API-wait deadline to both
the ordinary and `staged_generation_process` workers. Each passes only the
remaining caller time to the native engine. The total caller allowance and its
original observed-process cleanup reserve remain separate.

An already expired request reports unknown without claiming API shutdown.
If a worker error follows a received close request, emergency cleanup can use
only the remaining caller time. A pipe failure without a close request keeps the
native engine's original bounded cleanup call. That emergency return emits no
successful close receipt. The private peers migrate together; sync result
association, observed-process evidence and known usage are unchanged.
