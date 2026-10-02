# Adaptive Flush Window Design

**Date:** 2026-07-25
**Status:** Approved direction; implementation pending
**Scope:** Queue-adaptive flush only, inside the existing
PostgreSQL → Daft → Arrow → Ray → vLLM execution path

## 1. Problem and evidence

The current queue-adaptive flush policy does not form upstream batches:

- in the 512-row formal experiment, its mean batch size was 1.0 in all five
  repeats;
- in the 1024-row scale probe, it again produced 1,024 submissions for 1,024
  rows;
- 92.28% of adjacent arrivals in the first 1,024 rows are no more than 50 ms
  apart after applying arrival scale `0.0005`, so the workload contains
  sufficient coalescing opportunities.

The failure is therefore not explained by row count alone. Code and trace
inspection identified four causes:

1. `low_load_running=64` is disconnected from experimental `K_max=8`.
   With `waiting=0`, the policy classifies almost every snapshot as
   underloaded and flushes immediately.
2. The 250 ms metrics polling interval is longer than the 25–50 ms flush
   horizon. Metrics normally do not change during one pending batch.
3. The current adaptive policy is not a superset of fixed timeout. Under low
   load it chooses zero wait, so it deliberately gives up fixed-timeout
   coalescing opportunities.
4. When the lazy replay generator resumes after downstream backpressure, it
   checks the timeout before consuming rows whose event-time arrival was
   before that timeout. Eligible rows are therefore pushed into the next
   batch.

The objective approved for the next version is throughput-first:
maximize observed vLLM tokens/s while applying an explicit batch-service P99
guardrail.

## 2. Considered approaches

### 2.1 Threshold-only patch

Replace the constant 64 with `K_max` and stop flushing immediately under low
load.

This is small but does not repair event-time catch-up or establish a clear
fixed-timeout fallback. It is rejected as an incomplete symptom fix.

### 2.2 Two-window adaptive policy

Choose one immutable wait window when a pending batch opens:

- 25 ms under low load or when metrics are missing/stale;
- 50 ms under service pressure;
- immediate close on token-budget or row-budget membership limits.

Consume all rows whose event-time arrival is no later than the selected
deadline before closing the batch. This approach is selected.

### 2.3 Continuous EWMA/PID window

Continuously adjust a 0–100 ms wait window using smoothed queue/KV signals.

This has a larger action space but lacks supporting data and adds sensitivity
to sampling noise. It remains a later comparison only after the two-window
policy establishes a positive signal.

## 3. Selected behavior

### 3.1 Window selection

`QueueAdaptiveFlush` receives:

- `min_wait_s`, default 0.025;
- `max_wait_s`, default 0.050;
- `pressure_running`, supplied from the run's `max_inflight`;
- `congestion_kv_usage`, default 0.85.

At the opening of each nonempty pending batch it selects exactly one window:

| Observation | Selected window | Reason |
|---|---:|---|
| token/row membership limit already reached | 0 | `budget_reached` |
| metrics missing or stale | `min_wait_s` | `fixed_fallback` |
| `waiting > 0` | `max_wait_s` | `queue_pressure` |
| `kv_usage >= congestion_kv_usage` | `max_wait_s` | `kv_pressure` |
| `running >= pressure_running` | `max_wait_s` | `running_pressure` |
| otherwise | `min_wait_s` | `underloaded_base_window` |

The selection remains fixed for that batch. The policy adapts between batches,
not repeatedly inside a 25–50 ms window. This matches the slower metrics
sampling cadence and prevents one batch from oscillating between deadlines.

The minimum window equals the fixed-timeout baseline. Thus stale metrics and
underload no longer collapse to immediate flush. The adaptive policy is a
structural superset of the 25 ms fixed policy in available coalescing time,
but this does not guarantee higher observed performance: pressure
misclassification and longer waiting can still hurt tail latency.

### 3.2 Event-time catch-up

For a selected deadline `D`, the replay batcher processes rows in this order:

1. add the first row and select `D`;
2. consume every subsequent row with event-time replay deadline `<= D`,
   stopping before any membership limit would be exceeded;
3. if wall-clock time is before both the next arrival and `D`, wait for the
   earlier event;
4. at `D`, close the batch;
5. rows with event-time deadline `> D` start a later batch even if wall-clock
   execution has fallen behind.

This preserves the configured event-time window when Ray submission or
collection temporarily blocks the lazy source. Immediate flush retains its
existing singleton semantics; rows with the same timestamp are not silently
merged into the immediate baseline.

### 3.3 Metrics and fallback

Metrics sampling remains off the flush hot path. A sampler error, missing
sample, or stale sample selects the 25 ms fixed fallback and never blocks
arrival replay.

The queue policy must not use the hard-coded value 64. The profiler passes the
actual `max_inflight` value as `pressure_running`. Multi-endpoint expansion
may later replace this scalar with endpoint-local capacity, but that is
outside this change.

## 4. Interfaces and traceability

The implementation should make the selected window explicit rather than infer
it through reflection on `timeout_s` or `max_wait_s`.

Each pending batch records:

- selected wait seconds;
- selection reason;
- selected event-time deadline;
- flush reason;
- pending rows/tokens at close.

The flush trace schema is bumped because it gains `selected_wait_s` and
`window_reason`. Every trace row continues to include `server_version` and
`pgvector_version`.

No new Daft-, Arrow-, Ray-, or vLLM-specific import enters the policy module.
The policy consumes typed observations; the replay runtime owns clocks and
event ordering; the profiler owns engine metrics and CLI wiring.

## 5. Correctness and failure handling

Required invariants:

- each input document appears in exactly one output batch;
- batch row and token membership limits are never exceeded;
- rows after the selected event-time deadline are never pulled backward;
- budget limits override time windows;
- missing/stale metrics select the fixed fallback;
- immediate and fixed-timeout baselines preserve their documented behavior;
- metrics I/O never blocks the replay deadline path.

Invalid configuration is rejected:

- `min_wait_s <= 0`;
- `max_wait_s < min_wait_s`;
- `pressure_running <= 0`;
- KV threshold outside `[0, 1]`.

## 6. Test strategy

Implementation follows test-first development.

### Unit tests

- low load selects 25 ms rather than immediate flush;
- missing/stale metrics select 25 ms;
- waiting, KV, or running pressure selects 50 ms;
- budget membership flushes immediately;
- invalid window and threshold values fail clearly.

### Deterministic replay tests

- a row arriving before the selected deadline joins the batch even when the
  runtime resumes after the deadline;
- a row arriving after the deadline does not join;
- pressure extends the window from 25 to 50 ms;
- the window cannot change after batch opening;
- exactly-once and membership limits hold;
- immediate and fixed-timeout regression tests remain green.

### Real single-GPU gates

1. **64-row gate:** real PostgreSQL, Daft, Ray, and vLLM; no fake. Adaptive
   submissions must not exceed fixed-timeout submissions, document coverage
   must be exactly once, and all required traces must be nonempty.
2. **1024-row probe:** adaptive mean batch rows must be at least the
   fixed-timeout value; observed tokens/s must be at least 95% of immediate;
   batch-service P99 must be at most 110% of fixed timeout.
3. **Formal scale:** only after both gates pass, run randomized policy order at
   512, 1024, and 2048 rows with warm-up and repeated measurements.

Failure of a gate stops scale expansion. It does not trigger hidden threshold
tuning on the formal workload.

## 7. Claim boundary

Passing these gates would show that the adaptive mechanism changes upstream
batch formation without unacceptable local tail-latency loss. It would not
prove universal dominance over fixed timeout, multi-GPU effectiveness, or
production-rate behavior.

The implementation remains on `feat/runtime-scheduling-foundation` until
tests and real experiments pass. It is not merged into `main` as part of this
design step.
