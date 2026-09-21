# Adaptive Flush Window Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make queue-adaptive flush a two-window superset of the 25 ms fixed-timeout baseline, preserve event-time batching under downstream backpressure, and verify the change on the real single-GPU Daft/Ray/vLLM path.

**Architecture:** Flush policies explicitly select an immutable `FlushWindow` when a pending batch opens. `ArrivalReplayBatcher` uses that event-time deadline to consume every eligible row before closing the batch; the profiler wires the actual `max_inflight` into pressure detection and records the selected window in a versioned trace.

**Tech Stack:** Python 3.10, standard-library dataclasses and unittest, Daft 0.7.20, PyArrow 24.0.0, Ray 2.56.0, PostgreSQL 18.4, pgvector 0.8.2, vLLM 0.25.1, Qwen2.5-1.5B.

## Global Constraints

- Keep the formal path `PostgreSQL -> Daft -> Arrow -> Ray task -> compatible HTTP vLLM`.
- Do not use fake results for any experiment claim.
- Keep complete rows as independent model requests; batching merges rows but never splits prompt content.
- Use 25 ms as the fixed fallback and 50 ms as the pressure window.
- Select a window once per pending batch; do not resample or change it inside that batch.
- Every CSV row includes `server_version` and `pgvector_version`.
- Do not merge `main`; work only on `feat/runtime-scheduling-foundation`.
- Follow RED → GREEN for every behavior change.

---

### Task 1: Explicit flush-window selection

**Files:**
- Modify: `code/src/scheduling/flush.py`
- Modify: `code/src/scheduling/__init__.py`
- Test: `code/tests/scheduling/test_flush_policies.py`

**Interfaces:**
- Produces: `FlushWindow(wait_s: float, reason: str)`.
- Produces: `ImmediateFlush.select_window(observation) -> FlushWindow`.
- Produces: `FixedTimeoutFlush.select_window(observation) -> FlushWindow`.
- Produces: `QueueAdaptiveFlush(min_wait_s, max_wait_s, pressure_running, congestion_kv_usage)`.
- Produces: `QueueAdaptiveFlush.select_window(observation) -> FlushWindow`.

- [ ] **Step 1: Write failing window-selection tests**

Add tests that require low load and missing/stale metrics to select 25 ms,
while each pressure signal selects 50 ms:

```python
def test_queue_adaptive_selects_fixed_fallback_for_low_or_unknown_load():
    policy = QueueAdaptiveFlush(
        min_wait_s=0.025,
        max_wait_s=0.050,
        pressure_running=8,
    )
    assert policy.select_window(observation(running=2)).wait_s == 0.025
    assert policy.select_window(observation(fresh=False)).reason == "fixed_fallback"
    assert policy.select_window(observation(waiting=None)).reason == "fixed_fallback"


def test_queue_adaptive_selects_max_window_for_each_pressure_signal():
    policy = QueueAdaptiveFlush(
        min_wait_s=0.025,
        max_wait_s=0.050,
        pressure_running=8,
    )
    assert policy.select_window(observation(waiting=1)).reason == "queue_pressure"
    assert policy.select_window(observation(running=8)).reason == "running_pressure"
    assert policy.select_window(observation(kv_usage=0.9)).reason == "kv_pressure"
```

Add validation cases for `min_wait_s <= 0`, `max_wait_s < min_wait_s`,
`pressure_running <= 0`, and invalid KV threshold.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_flush_policies.py
```

Expected: failures because `FlushWindow`, `select_window`, and the new
constructor parameters do not exist.

- [ ] **Step 3: Implement the minimal policy API**

Add:

```python
@dataclass(frozen=True)
class FlushWindow:
    wait_s: float
    reason: str

    def __post_init__(self) -> None:
        if self.wait_s < 0:
            raise ValueError("wait_s must be non-negative")
        if not self.reason:
            raise ValueError("reason must be non-empty")
```

`ImmediateFlush` returns `FlushWindow(0.0, "immediate")`.
`FixedTimeoutFlush` returns its configured timeout and
`"fixed_timeout"`. `QueueAdaptiveFlush.select_window` checks missing/stale
metrics first, then queue, KV, and running pressure, otherwise selecting
`FlushWindow(min_wait_s, "underloaded_base_window")`.

Keep `decide()` as a small compatibility wrapper: budget closes immediately;
otherwise it flushes only when `age_s >= selected_window.wait_s`.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the same test command. Expected: all flush policy tests pass.

- [ ] **Step 5: Commit Task 1**

```powershell
git add code/src/scheduling/flush.py code/src/scheduling/__init__.py code/tests/scheduling/test_flush_policies.py
git commit -m "feat: select adaptive flush windows"
```

---

### Task 2: Preserve event-time windows during replay

**Files:**
- Modify: `code/src/scheduling/batching.py`
- Test: `code/tests/scheduling/test_runtime_batching.py`

**Interfaces:**
- Consumes: `FlushWindow` and each policy's `select_window`.
- Produces: `FlushTraceEvent.selected_wait_s: float`.
- Produces: `FlushTraceEvent.window_reason: str`.
- Preserves: `ArrivalReplayBatcher` public constructor and iterator result type.

- [ ] **Step 1: Write failing event-time tests**

Add a deterministic regression proving that a row before the selected
deadline joins even if downstream work advances the wall clock:

```python
def test_due_before_selected_deadline_joins_after_downstream_delay():
    clock = FakeReplayClock()
    closed = 0

    def delayed_close(batch):
        nonlocal closed
        closed += 1
        if closed == 1:
            clock.advance(0.2)
        return tuple(item.row_id for item in batch.rows)

    batcher = replay(
        [
            row("first", arrival_s=0.0),
            row("second", arrival_s=1.0),
            row("third", arrival_s=1.02),
            row("after", arrival_s=1.06),
        ],
        clock,
        flush_policy=FixedTimeoutFlush(0.05),
        close_batch=delayed_close,
    )
    assert list(batcher) == [("first",), ("second", "third"), ("after",)]
```

Add tests that:

- pressure selects 50 ms and includes a 40 ms row;
- low load selects 25 ms and excludes a 30 ms row;
- the selected window does not change after opening;
- immediate remains singleton, including equal-timestamp rows;
- token membership prevents an over-budget row from joining;
- all output row IDs are exactly once.

- [ ] **Step 2: Run runtime batching tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_runtime_batching.py
```

Expected: the delayed catch-up test splits `second` and `third`, and trace
field assertions fail.

- [ ] **Step 3: Replace reflection-based deadlines with a selected window**

Refactor only `ArrivalReplayBatcher.__iter__` and its trace helper:

```python
window = self._flush_policy.select_window(observation)
selected_deadline_s = pending_oldest_deadline_s + window.wait_s
```

For nonzero windows, consume rows in event-time order while
`next_deadline_s <= selected_deadline_s`. Wait only when wall clock is before
the next eligible arrival. If a row would exceed membership limits, close the
current batch without adding it. Close at the selected deadline before
consuming any later row.

For a zero window, close immediately before examining the next row, preserving
the immediate baseline. Remove `_policy_deadline` reflection after no caller
uses it.

Record `selected_wait_s` and `window_reason` on both wait/selection and close
trace events.

- [ ] **Step 4: Run runtime and policy tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_runtime_batching.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_flush_policies.py
```

Expected: both modules pass; immediate/fixed regression behavior matches the
new event-time specification.

- [ ] **Step 5: Commit Task 2**

```powershell
git add code/src/scheduling/batching.py code/tests/scheduling/test_runtime_batching.py
git commit -m "fix: preserve adaptive event-time windows"
```

---

### Task 3: Wire capacity and versioned trace fields

**Files:**
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`
- Modify: `code/scripts/README.md`
- Test: `code/tests/observability/test_postgres_profile_scheduling.py`
- Test: `code/tests/scheduling/test_scheduling_daft_ray_contract.py`

**Interfaces:**
- Consumes: `QueueAdaptiveFlush(min_wait_s, max_wait_s, pressure_running)`.
- Produces trace columns: `selected_wait_s`, `window_reason`.
- Preserves trace identity columns and database version columns.

- [ ] **Step 1: Write failing profiler tests**

Require `_arrival_replay_envelopes` to construct queue-adaptive flush with:

```python
min_wait_s = args.flush_timeout_ms / 1000.0
max_wait_s = args.flush_max_wait_ms / 1000.0
pressure_running = args.max_inflight
```

Extend the trace-writer assertion to require:

```python
{
    "schema_version",
    "server_version",
    "pgvector_version",
    "selected_wait_s",
    "window_reason",
}
```

Add CLI validation asserting queue-adaptive rejects
`flush_max_wait_ms < flush_timeout_ms`.

- [ ] **Step 2: Run focused profiler tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py
```

Expected: missing trace fields and missing min/max validation failures.

- [ ] **Step 3: Implement minimal profiler wiring**

Construct:

```python
QueueAdaptiveFlush(
    min_wait_s=args.flush_timeout_ms / 1000.0,
    max_wait_s=args.flush_max_wait_ms / 1000.0,
    pressure_running=args.max_inflight,
)
```

Write both new trace fields, bump flush trace `schema_version` to 2, and keep
`server_version`/`pgvector_version` in every row. Add the exact CLI validation
message:

```text
queue-adaptive flush requires --flush-max-wait-ms >= --flush-timeout-ms
```

- [ ] **Step 4: Run profiler and real framework contract tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py
```

Expected: both modules pass, including real local Daft → Arrow → Ray
task/actor contracts.

- [ ] **Step 5: Commit Task 3**

```powershell
git add code/scripts/profiling/postgres_ai_operator_profile.py code/scripts/README.md code/tests/observability/test_postgres_profile_scheduling.py code/tests/scheduling/test_scheduling_daft_ray_contract.py
git commit -m "feat: wire adaptive flush window traces"
```

---

### Task 4: Full verification and real single-GPU gates

**Files:**
- Create: `experiments/results/adaptive_flush_window_20260725/manifest.json`
- Create: `experiments/results/adaptive_flush_window_20260725/README.md`
- Create: `experiments/results/adaptive_flush_window_20260725/*.csv`
- Modify: `experiments/results/README.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`
- Modify if conclusions change: `PROJECT_OUTLINE.md`

**Interfaces:**
- Consumes: profiler CLI and trace schema from Task 3.
- Produces: gate, probe, and optional formal result artifacts.

- [ ] **Step 1: Run full local verification**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py"
$env:PYTHONPYCACHEPREFIX="tmp\adaptive_flush_pycache"
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts/profiling/postgres_ai_operator_profile.py code/tests
git diff --check
```

Expected: all tests pass, compilation succeeds, and diff check is clean.

- [ ] **Step 2: Run the 64-row real gate**

Use the existing real components and common parameters:

```text
PostgreSQL 18.4 / pgvector 0.8.2
Daft source
Ray task executor
compatible_http vLLM Qwen2.5-1.5B
arrival scale 0.0001
token budget 6144
K_max 8
completion max tokens 16
fixed timeout 25 ms
adaptive max wait 50 ms
```

Run fixed-timeout and queue-adaptive once, writing separate flush,
submission, and resource traces.

Gate conditions:

- both status rows are `ok`;
- both have 64 successful vLLM requests;
- every document ID appears exactly once;
- adaptive submissions do not exceed fixed-timeout submissions;
- all traces are nonempty and contain database versions.

- [ ] **Step 3: Run the 1024-row real probe**

If Step 2 passes, run immediate, fixed-timeout, and queue-adaptive once at
arrival scale `0.0005`.

Probe conditions:

- adaptive mean batch rows >= fixed-timeout mean batch rows;
- adaptive tokens/s >= 95% of immediate tokens/s;
- adaptive batch-service P99 <= 110% of fixed-timeout P99.

If any condition fails, stop. Do not run 2048 rows.

- [ ] **Step 4: Decide and run formal scale only if gated**

If the 1024-row probe passes, run randomized policy order with one warm-up and
five formal repeats at the approved scales. Add 2048 only after the repeated
512/1024 results remain within the same guardrails.

- [ ] **Step 5: Audit and document results**

Check every CSV for `server_version` and `pgvector_version`, verify exactly-once
coverage from submission traces, calculate observed tokens/s, sample standard
deviation, and 95% t confidence intervals. Separate facts, inferences,
limitations, and claims that remain unsupported.

- [ ] **Step 6: Commit verified implementation and results**

Stage only implementation, tests, documentation, and validated result
artifacts. Exclude `.superpowers/`. Commit without AI attribution and keep the
branch unmerged.
