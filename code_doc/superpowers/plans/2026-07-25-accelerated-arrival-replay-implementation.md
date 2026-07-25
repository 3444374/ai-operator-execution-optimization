# Accelerated Arrival Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit recorded arrival-time scale, verify it through the
real replay path, and run the approved short gate followed by the formal
single-GPU flush comparison.

**Architecture:** `ArrivalReplayBatcher` owns the engine-independent clock
mapping and receives a positive finite scale. The profiler exposes and records
the scale without changing offline execution. Experiment commands use the
existing PostgreSQL -> Daft -> Arrow -> Ray -> vLLM path and save configuration
and result artifacts beside the CSV.

**Tech Stack:** Python 3.11, argparse, PyArrow, Daft, Ray, PostgreSQL, vLLM,
`unittest`.

## Global Constraints

- Database timestamps remain unchanged.
- `scaled_offset = (raw_arrival_s - first_raw_arrival_s) * scale`.
- Scale defaults to `1.0` and must be finite and greater than zero.
- Flush timeout and hard maximum wait are not scaled.
- Offline paths remain unchanged.
- No fake backend may contribute to gate or formal results.
- Do not merge to `main` before formal artifacts and analysis are complete.

---

### Task 1: Replay Clock Scaling

**Files:**
- Modify: `code/src/scheduling/batching.py`
- Modify: `code/tests/test_runtime_batching.py`

**Interfaces:**
- Produces: `ArrivalReplayBatcher(..., arrival_time_scale: float = 1.0)`

- [ ] **Step 1: Write failing scale tests**

Add tests that construct rows at raw offsets `0` and `100`, pass scale
`0.001`, and assert the deterministic clock waits at replay time `0.1`.
Add subtests rejecting `0`, `-1`, `nan`, and `inf`. Retain an assertion that
the default scale preserves existing waits.

- [ ] **Step 2: Verify RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_runtime_batching.py
```

Expected: `ArrivalReplayBatcher` rejects the unknown
`arrival_time_scale` argument.

- [ ] **Step 3: Implement the minimal mapping**

Validate with:

```python
if (
    not isinstance(arrival_time_scale, (int, float))
    or isinstance(arrival_time_scale, bool)
    or not math.isfinite(arrival_time_scale)
    or arrival_time_scale <= 0
):
    raise ValueError("arrival_time_scale must be finite and positive")
```

Store the float and change only the next-row deadline:

```python
next_deadline_s = replay_start_s + (
    following.arrival_s - origin_arrival_s
) * self._arrival_time_scale
```

- [ ] **Step 4: Verify GREEN**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_runtime_batching.py
.conda\pg-ai-profile\python.exe code\tests\test_flush_policies.py
```

- [ ] **Step 5: Commit**

```powershell
git add code/src/scheduling/batching.py code/tests/test_runtime_batching.py
git commit -m "feat: scale arrival replay time"
```

### Task 2: Profiler CLI and Artifact Recording

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`
- Modify: `code/tests/test_scheduling_daft_ray_contract.py`
- Modify: `code/scripts/README.md`

**Interfaces:**
- Consumes: `ArrivalReplayBatcher(..., arrival_time_scale: float = 1.0)`
- Produces: CLI `--arrival-time-scale`; result field
  `arrival_time_scale`; flush-trace column `arrival_time_scale`

- [ ] **Step 1: Write failing CLI and trace tests**

Assert that default dry-run records `1.0`, replay dry-run records `0.0005`,
invalid values fail before execution, and `_write_flush_trace` writes the same
scale on every event. Update the real Daft/Ray contract to pass `0.001` and
assert its raw `100` gap completes near `0.1` seconds.

- [ ] **Step 2: Verify RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
```

- [ ] **Step 3: Add the explicit profiler wiring**

Add:

```python
parser.add_argument(
    "--arrival-time-scale",
    type=float,
    default=1.0,
    help="Positive multiplier applied to normalized arrival replay offsets.",
)
```

Validate it with the same positive-finite rule, pass it into the batcher, add
it to dry-run/formal result dictionaries, and include it in flush trace rows.
Do not read it in the replay-disabled organization path.

- [ ] **Step 4: Verify the complete code path**

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -p "test_*.py"
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts
.conda\pg-ai-profile\python.exe code/scripts/postgres_ai_operator_profile.py --help
```

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py code/tests/test_scheduling_daft_ray_contract.py code/scripts/README.md
git commit -m "feat: expose accelerated arrival replay"
```

### Task 3: Real Gate and Formal Experiment

**Files:**
- Create: `experiments/results/accelerated_arrival_flush_20260725/manifest.json`
- Create: `experiments/results/accelerated_arrival_flush_20260725/gate_runs.csv`
- Create: `experiments/results/accelerated_arrival_flush_20260725/formal_runs.csv`
- Create: `experiments/results/accelerated_arrival_flush_20260725/*_flush_trace.csv`
- Create: `experiments/results/accelerated_arrival_flush_20260725/report.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Consumes: profiler CLI from Task 2
- Produces: source-typed single-GPU gate/formal artifacts and statistical
  comparison

- [ ] **Step 1: Run the fast real-component gate**

For each policy in `immediate`, `fixed_timeout`, and `queue_adaptive`, run 64
rows with scale `0.0001`, token budget 6144, static K_max 8, real
`daft_postgres`, Ray task execution, Qwen vLLM completions, metrics endpoint,
no writeback, and a policy-specific flush trace.

- [ ] **Step 2: Audit gate artifacts**

Require successful run status, 64 completed rows, non-empty main CSV and flush
trace, real PostgreSQL/pgvector versions, real endpoint/model metadata, and no
fake backend. Stop before formal repetitions if any condition fails.

- [ ] **Step 3: Run the formal matrix**

For every policy, run 512 rows with scale `0.0005`, one warm-up and five formal
repeats. Keep all non-policy parameters identical:

```text
batching_policy=token_budget
token_budget=6144
scheduling_policy=static
max_inflight=8
completion_max_tokens=16
writeback_mode=none
```

- [ ] **Step 4: Compute and explain results**

From CSV and traces, report per policy: successful repeats, rows/s, tokens/s
when available, E2E mean/p50/p95/p99, invocation count, batch-size/token
distribution, flush-reason counts, queue/running/KV observations, mean,
standard deviation, and 95% confidence interval. Separate facts, inferences,
unavailable metrics, and claims that cannot be made.

- [ ] **Step 5: Update project records and verify**

Register every new artifact in `PROJECT_INDEX.md`, record the measured
conclusion in `PROJECT_LOG.md`, run `git diff --check`, and commit only after
artifact completeness passes. Do not merge `main`.

## Plan Self-Review

- Every design requirement maps to Tasks 1-3.
- Scaling occurs only in the replay clock mapping.
- Stored workload data and timeout semantics are unchanged.
- Formal work cannot start until the real-component gate passes.
- The plan contains no fake performance path or multi-GPU claim.
