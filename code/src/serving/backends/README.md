# Native engines and private IPC

`vllm_protocol.py` owns engine/generation values and result conversion. Sync and
async engines construct their own native runtime. `async_engine_process.py` owns
the parent sockets, submitted inputs, result consumption and its original child;
`async_engine_worker.py` owns the child event loop. `async_engine_wire.py` encodes
frames and compact terminal records without transport or scheduling.

## Async close timing

`AsyncEngineProcess.close(timeout=...)` keeps its existing total caller allowance
and direct-child cleanup reserve. Its `close` control frame now carries the actual
API-wait deadline, using the same local monotonic clock as request deadlines.
The worker supplies the remaining time to the native engine, rather than replacing
it with a fixed ten seconds. The private parent and child migrate together; the
old internal close frame without a deadline is not a separate supported format.

A normal close response is emitted once, including an unconfirmed response.
An expired deadline reports unknown without claiming API shutdown. If a pipe or
worker error occurs before a normal response, finalization retains bounded
best-effort cleanup: the original one second when no caller close was received,
or only the remaining caller time otherwise. That emergency return does not emit
a successful close receipt or replace an earlier result.

Request outcomes, retained unknown reservations, and the separate engine/producer,
original-Popen and parent-I/O evidence required for retirement are unchanged.
Physical GPU and descendant exit are reported by their actual external owners.
