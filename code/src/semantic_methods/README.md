# Semantic method continuations

This module lets an authorized semantic method produce several sequential requests for one row.
It contains no optimization algorithm, model client, SQL binding, or independent scheduler.
`continuation.py` provides `Method`, `Continue`, `Final`, and the bounded `MethodRun` consumer.
The original direct-task session path remains available without a method wrapper.

A method receives input bytes in `start`, then its previous state and a successful stage result in
`resume`. Each callback returns either the next `Request` with immutable state or a final row value.
A method object can be shared between rows only if callbacks keep per-row state in the continuation.
Callbacks must do bounded local work; model calls become requests. Size checks bound retained values,
not arbitrary temporary allocations inside trusted method code.

`SessionSpec.task_profiles` declares the allowed capability/operator combinations before execution.
A request names one of those profiles; an undeclared name rejects the whole offer batch before any
ownership transfer. Existing callers omit the profile and keep the original session capability.
Profiles share one normalized `work_unit`. The model adapter performs unit conversion and resolves
capabilities; unsupported capabilities must fail explicitly. Declaring a profile does not install a
backend, authorize an approximate method, or implement capability-aware endpoint selection.

The driver follows this sequence:

1. Keep a bounded mapping from the database's row/call identity to `MethodRun`.
2. Convert `run.pending` with `offered(sequence)` and offer it to the existing session. Bind
   `run.accepted(TaskKey(...))` only for an accepted request. Backpressure leaves it pending.
3. Associate each successful delivery with that run, call `run.complete(key, result)`, and release
   its session lease in `finally`. Handle failed terminal status under the operator's error policy;
   do not feed failure bytes to `resume`. Cancel/close the session when that policy requires it.
4. Return `run.final.value` for the original row, or offer the next stage after releasing the lease.
   Use a fresh monotonically increasing task sequence for every accepted stage in the session.
5. Seal only when source input has ended **and all live methods can produce no more requests**.
   Source EOF alone is insufficient. Ordinary unsealed idle remains a valid waiting state.

The driver owns aggregate method-state/final-result memory, limits live rows, and discards cancelled
runs. `MethodLimits` bounds each run's retained input/state/result and number of stages; it is not a
query-wide memory budget. `TaskKey` is execution identity, not a replacement for row/call identity.
Invalid continuations fail the run; an unrelated completion leaves the active stage unchanged.
The contract currently supports zero, one, or several sequential requests. The session can now organize
accepted tasks using `WorkWindowOrganizer`; its current backend expands each group into independent
single-member requests. Parallel stage fan-out, calibration over sampled rows, cross-row joins, a
single physical request with multiple member results, and PG bridging need their own bounded integration. They are not hidden behind this interface or claimed implemented.

For future LOTUS reuse, keep scoring, calibration artifacts, threshold decisions, and parsers in a
versioned method adapter. Translate model calls to `Request`; retain SemLoom's existing admission,
routing, transport, and resource ownership. A DataFrame accessor or global LM client is not a drop-in
method. Pin the upstream revision, retain required license/NOTICE and modification notices, and test
behavior against that revision. No LOTUS source has been copied in this slice.

The [two-stage controlled test](../../tests/scheduling/test_method_continuation.py) demonstrates the
same session with one held-task slot and two declared capabilities. The design and remaining work are
recorded in [the incremental design](../../../experiments/plans/semloom_incremental_session_design.md).


To enter the organized path, the producer adds `TaskInfo` to each offered request after the semantic
method has chosen the stage. Keep `row_sequence` and `call_id` stable across stages; task sequence
continues to increase. The typed `WorkDescriptor` must match session work units and the admitted work
estimate. Its locality key and calibration identity are explicit inputs, never parsed from request
bytes. Typed and opaque metadata together must fit the per-task metadata limit.

`BatchMember` identifies an organization batch and the task's member index. It does not change which
row owns a result, promise atomic group acceptance, or imply one HTTP request for the whole group.
The result consumer uses `Delivery.info` for row/stage association and decides when to emit SQL rows;
completion order alone is not SQL output order. Partial failure currently fails the session under the
existing policy, cancels siblings, and waits for their terminal events before returning their credits.

The producer must offer only work whose evaluation and submission are already permitted. A finite
window limits speculative volume; it does not establish correct LIMIT, volatile-expression or error
ordering behavior. Those are verified by the future PG carrier before enlarging its input window.


When `SessionPolicies.organize` is configured, it selects batches in place of `choose_task`.
The default path keeps `choose_task` and leaves membership unset. `WorkWindowOrganizer` uses the oldest
accepted task's compatibility group, optionally sorts that group by work, and invokes the existing
complete-task work slicer. An oversized task remains whole. This static policy does not promise fairness
under continuous arrival. The core validates returned membership before dispatch and preserves an
unfinished group across capacity or backend rejection; the organizer stores no payload history.
