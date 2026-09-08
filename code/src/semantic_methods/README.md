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
The contract currently supports zero, one, or several sequential requests. Parallel stage fan-out,
calibration over sampled rows, cross-row joins, multi-member batches, and PG bridging need their own
bounded integration. They are not hidden behind this interface or claimed implemented.

For future LOTUS reuse, keep scoring, calibration artifacts, threshold decisions, and parsers in a
versioned method adapter. Translate model calls to `Request`; retain SemLoom's existing admission,
routing, transport, and resource ownership. A DataFrame accessor or global LM client is not a drop-in
method. Pin the upstream revision, retain required license/NOTICE and modification notices, and test
behavior against that revision. No LOTUS source has been copied in this slice.

The [two-stage controlled test](../../tests/scheduling/test_method_continuation.py) demonstrates the
same session with one held-task slot and two declared capabilities. The design and remaining work are
recorded in [the incremental design](../../../experiments/plans/semloom_incremental_session_design.md).
