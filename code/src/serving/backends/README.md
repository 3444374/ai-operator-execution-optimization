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
