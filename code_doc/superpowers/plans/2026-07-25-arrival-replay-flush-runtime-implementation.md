# Arrival Replay and Flush Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replay workload arrivals on monotonic time, assemble pending batches,
apply the independent flush policies, and feed the resulting lazy batch stream
into the existing Daft-to-Arrow-to-Ray scheduler.

**Architecture:** Keep arrival and batching decisions engine-independent.
`ArrivalReplayBatcher` consumes typed row metadata, advances through an
injectable clock, and asks an existing `FlushPolicy` when to close the pending
batch. A profile-layer Arrow adapter slices complete rows from the Daft-emitted
Arrow table and assembles closed rows into `PayloadEnvelope` objects. The
existing scheduler consumes this iterator lazily, so Ray execution overlaps
with replay without duplicating admission or routing logic.

**Tech Stack:** Python 3.11, dataclasses, monotonic time, PyArrow, Daft, Ray,
`unittest`.

## Global Constraints

- Formal execution remains PostgreSQL -> Daft -> Arrow -> Ray task/actor ->
  endpoint.
- Every row remains one complete model request; never split prompt contents.
- Scheduling policy modules do not import Daft, PyArrow, Ray, HTTP, or
  PostgreSQL.
- Policy decisions never sleep. Only the replay event loop may wait through an
  injected clock.
- Arrival values are offsets: normalize the first valid arrival to zero and
  preserve all subsequent gaps.
- Missing arrival values are rejected when replay is enabled; they are not
  silently converted to zero.
- `max_wait_s` is a hard pending-row bound even when service metrics are
  missing, stale, or congested.
- Existing offline throughput paths remain unchanged unless
  `--arrival-replay` is explicitly enabled.
- Do not claim a GPU performance improvement from unit or contract tests.
- Do not merge to `main` until formal single-GPU results and artifacts exist.

---

### Task 1: Engine-Independent Pending Batch Builder

**Files:**
- Create: `code/src/scheduling/batching.py`
- Modify: `code/src/scheduling/__init__.py`
- Create: `code/tests/test_runtime_batching.py`

**Interfaces:**
- `RowArrival(row_id, arrival_s, prompt_tokens, estimated_output_tokens,
  prefix_key, payload_ref)`
- `PendingBatch(rows, prompt_tokens, estimated_output_tokens,
  oldest_arrival_s)`
- `PendingBatchBuilder(max_rows, token_budget)`
- `PendingBatchBuilder.add(row) -> bool`, returning whether capacity is reached.
- `PendingBatchBuilder.close() -> PendingBatch`

- [ ] **Step 1: Write failing builder tests**

Add `unittest` cases equivalent to:

```python
builder = PendingBatchBuilder(max_rows=2, token_budget=0)
self.assertFalse(builder.add(row("r1", arrival_s=2.0, prompt_tokens=10)))
self.assertTrue(builder.add(row("r2", arrival_s=3.0, prompt_tokens=20)))
closed = builder.close()
self.assertEqual([item.row_id for item in closed.rows], ["r1", "r2"])
self.assertEqual(closed.prompt_tokens, 30)
self.assertEqual(closed.oldest_arrival_s, 2.0)
```

Also prove that one oversized complete row forms a one-row batch, invalid
metadata is rejected, closing an empty builder fails, and adding after capacity
requires closing first.

- [ ] **Step 2: Verify RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_runtime_batching.py
```

Expected: import failure because `src.scheduling.batching` does not exist.

- [ ] **Step 3: Implement the minimal typed builder**

Preserve row order, use prompt plus estimated output tokens for capacity, expose
row and token totals, reset on close, and contain no engine imports.

- [ ] **Step 4: Verify GREEN and engine independence**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_runtime_batching.py
rg -n "import (daft|pyarrow|ray|psycopg)|from (daft|pyarrow|ray|psycopg)" code/src/scheduling
```

- [ ] **Step 5: Commit Task 1**

```powershell
git add code/src/scheduling/batching.py code/src/scheduling/__init__.py code/tests/test_runtime_batching.py
git commit -m "feat: add pending batch builder"
```

### Task 2: Monotonic Arrival Replay and Flush Composition

**Files:**
- Modify: `code/src/scheduling/batching.py`
- Modify: `code/tests/test_runtime_batching.py`

**Interfaces:**
- `ReplayClock.now() -> float`
- `ReplayClock.wait_until(deadline_s) -> None`
- `SystemReplayClock`
- `ReplayServiceObservation(fresh, running, waiting, kv_usage)`
- `FlushTraceEvent(elapsed_s, pending_rows, pending_tokens, oldest_age_s,
  action, reason)`
- `ArrivalReplayBatcher(rows, builder_factory, flush_policy, close_batch,
  service_observation, clock)`

- [ ] **Step 1: Write failing replay tests**

With a deterministic fake clock, prove separately that:

- the first arrival is normalized to zero and later gaps are preserved;
- fixed timeout closes a partial batch before a later arrival;
- queue congestion still closes at the hard maximum;
- rows arriving while downstream is blocked are consumed immediately on resume;
- equal timestamps preserve source order;
- decreasing, missing, negative, or non-finite arrivals are rejected;
- every row ID appears in exactly one emitted envelope.

Assert emitted groups and fake-clock deadlines, not only policy calls.

- [ ] **Step 2: Verify RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_runtime_batching.py
```

- [ ] **Step 3: Implement the lazy replay iterator**

Set the replay epoch from `clock.now()`, normalize against the first arrival,
consume all currently due rows, build a `FlushObservation`, emit on flush, and
otherwise wait until the earlier of the next arrival and the hard/fixed
deadline. Flush the final pending rows without dropping them. Only
`SystemReplayClock.wait_until()` may call `time.sleep`.

- [ ] **Step 4: Verify GREEN and regressions**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_runtime_batching.py
.conda\pg-ai-profile\python.exe code\tests\test_flush_policies.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduler.py
```

- [ ] **Step 5: Commit Task 2**

```powershell
git add code/src/scheduling/batching.py code/tests/test_runtime_batching.py
git commit -m "feat: replay arrivals through flush policies"
```

### Task 3: Arrow Adapter and Profiler CLI Wiring

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- CLI: `--arrival-replay`, `--flush-policy
  {immediate,fixed_timeout,queue_adaptive}`, `--flush-timeout-ms` (25),
  `--flush-max-wait-ms` (50), and `--flush-trace-output`.
- `_row_arrivals(table, completion_max_tokens) -> list[RowArrival]`
- `_arrow_envelope(pending, batch_index, job_id, operator) -> PayloadEnvelope`
- `_arrival_replay_envelopes(table, args, ...) -> ArrivalReplayBatcher`

- [ ] **Step 1: Write failing profile and CLI tests**

Prove that Arrow rows and metadata round-trip exactly once; dry-run records the
configuration; replay requires arrival ordering, a Ray executor, non-null
arrivals, and positive bounds; offline defaults retain `_batch_envelopes`; and
missing queue metrics still use the hard maximum.

- [ ] **Step 2: Verify RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

- [ ] **Step 3: Wire the explicit replay path**

Fetch through the existing source, retain the Daft-to-Arrow boundary, create a
lazy replay iterator from complete Arrow rows, and feed it to the existing
typed Ray scheduler. Reuse the cached 250 ms service observation. Write flush
trace records separately from admission traces. Leave the replay-disabled path
unchanged.

- [ ] **Step 4: Verify GREEN, help, and dry-run**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py --help
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py --dry-run --executor ray_task --source-order arrival_time --arrival-replay --flush-policy fixed_timeout --output tmp\arrival_replay_dry_run.csv
```

- [ ] **Step 5: Commit Task 3**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: wire arrival replay into profiler"
```

### Task 4: Real Daft-to-Ray Replay Contract

**Files:**
- Modify: `code/tests/test_scheduling_daft_ray_contract.py`
- Modify: `code/src/scheduling/ray_adapter.py` and
  `code/tests/test_ray_adapter.py` only if timeout-aware collection is required.

- [ ] **Step 1: Write the failing contract test**

Use four rows at offsets `0.0, 0.0, 0.020, 0.100`. On a real local Ray
instance, verify fixed-timeout boundaries, task and actor exactly-once
completion, deterministic normalized results, and elapsed replay time with a
generous Windows tolerance.

- [ ] **Step 2: Verify RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
```

- [ ] **Step 3: Add only minimal adapter support**

Do not add another scheduler. If polling is required, add optional timeout
support while retaining the blocking default.

- [ ] **Step 4: Verify GREEN and full suite**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -p "test_*.py"
```

- [ ] **Step 5: Commit Task 4**

```powershell
git add code/tests/test_scheduling_daft_ray_contract.py code/src/scheduling/ray_adapter.py code/tests/test_ray_adapter.py
git commit -m "test: validate arrival replay through daft and ray"
```

### Task 5: Documentation and Experiment Gate

**Files:**
- Modify: `code/README.md`
- Modify: `code/scripts/README.md`
- Modify: relevant `learning/` walkthrough
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`

- [ ] **Step 1: Document semantics and claim boundary**

State that sorting is not replay, `--arrival-replay` is required for online
flush experiments, batching/flush/admission are distinct, and local contracts
are not performance evidence.

- [ ] **Step 2: Define the single-GPU smoke gate**

```text
flush_policy in {immediate, fixed_timeout, queue_adaptive}
admission    = static K_max=8
batching     = token_budget=6144
repeats      = 1 warm-up + 1 smoke
```

Require non-empty run, request/submission, flush/control, resource, and manifest
artifacts before formal repeats.

- [ ] **Step 3: Verify syntax, CLI, and full tests**

```powershell
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -p "test_*.py"
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py --help
```

- [ ] **Step 4: Inspect the complete diff**

```powershell
git status --short
git diff --check
git diff --stat main...HEAD
```

- [ ] **Step 5: Commit Task 5**

```powershell
git add code/README.md code/scripts/README.md learning PROJECT_INDEX.md PROJECT_LOG.md experiments/plans/experiment_status_and_gaps.md
git commit -m "docs: define arrival replay experiment gate"
```

## Plan Self-Review

- Covers the missing runtime half of batch-building and flush implementation.
- Task 1 types are consumed unchanged by Tasks 2 and 3.
- Daft remains the engine adapter, Arrow the payload boundary, and Ray the
  execution path.
- No flush result is formal unless actual replay and trace artifacts prove it.
- Statistical aggregation, joint search, and formal GPU repetitions begin only
  after this plan's smoke gate passes.
