# Row-Cap-Aware Packing and Non-Blocking Observation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove metrics network I/O from typed adaptive admission decisions and add a BFD-inspired packing candidate that prioritizes the enforced row cap before residual token capacity.

**Architecture:** The observation change reuses the existing background sampler and adds sample age to the engine-independent observation/trace schema. The packing change adds one pure deterministic sibling to classic BFD and wires it through the existing shared Arrow/Daft organizer path; sequential remains the default and experiment data selects which mechanisms survive.

**Tech Stack:** Python 3.10, `unittest`, PyArrow, Daft, Ray, PostgreSQL 18.4, vLLM 0.25.1, Prometheus metrics, RTX 5070.

## Global Constraints

- Work only on `feat/runtime-scheduling-foundation`; do not merge `main`.
- Follow RED→GREEN for every production behavior.
- Do not add a dependency or a second sampling framework.
- Do not modify vLLM internals.
- Do not use fake backends for performance evidence.
- Classic BFD is a baseline; useful mechanisms may survive independently.
- Keep sequential token-budget as the default.
- Use fixed-output cost for the primary comparison; unpaired BurstGPT output metadata is sensitivity-only.
- Every formal run must record PostgreSQL/pgvector versions, request and submission traces, GPU/power/energy, non-zero vLLM FLOP delta, and `mfu_status=ok`.
- Stop after a failed real gate; do not continue into a larger experiment.

---

## File Structure

- `code/src/scheduling/models.py`: add sample-age semantics to an admission observation.
- `code/src/scheduling/observations.py`: compute sample age in cached and background providers and expose it in trace rows.
- `code/src/scheduling/admission.py`: depend on a small observation-provider protocol rather than one concrete provider.
- `code/scripts/profiling/postgres_ai_operator_profile.py`: construct, retain, and close the non-blocking provider; emit sample age.
- `code/src/packing.py`: implement the pure BFD-inspired row-cap-first algorithm.
- `code/src/organizers.py`: expose the candidate through the existing shared Arrow/Daft policy path.
- `code/tests/scheduling/test_dynamic_admission.py`: observation freshness, latency, and lifecycle tests.
- `code/tests/observability/test_postgres_profile_scheduling.py`: production construction/cleanup and profiler metric tests.
- `code/tests/planning/test_packing.py`: canonical membership and packing invariants.
- `code/tests/planning/test_organizers.py`: Arrow/Daft shared-policy contract.
- `code/scripts/README.md`, `code/README.md`, `learning/experiment_walkthrough.md`: verified behavior and command documentation.
- `experiments/results/row_cap_aware_packing_gate_20260726/`: real correctness gate.
- `experiments/results/row_cap_aware_packing_512_20260726/`: screening and repeated candidate comparison.
- `experiments/results/row_cap_aware_packing_1024_20260726/`: held-out confirmation only when a 512-row candidate survives.

---

### Task 1: Sample-Age Schema and Provider Protocol

**Files:**
- Modify: `code/src/scheduling/models.py`
- Modify: `code/src/scheduling/observations.py`
- Modify: `code/src/scheduling/admission.py`
- Test: `code/tests/scheduling/test_dynamic_admission.py`

**Interfaces:**
- Produces: `AdmissionObservation.sample_age_s: float | None`
- Produces: `AdmissionTraceEvent.sample_age_s: float | None`
- Produces: `ObservationProvider.latest(inflight: int) -> AdmissionObservation`
- Consumes: the existing `CachedMetricsObservationProvider` and `NonBlockingMetricsObservationProvider`

- [ ] **Step 1: Write failing sample-age tests**

Add tests that use a controlled clock:

```python
def test_nonblocking_provider_reports_sample_age(self) -> None:
    now = [10.0]
    provider = NonBlockingMetricsObservationProvider(
        lambda: ServiceMetricsSnapshot(4, 0, 0.25),
        poll_interval_s=60.0,
        stale_after_s=0.5,
        clock=lambda: now[0],
    )
    try:
        self.assertTrue(provider.wait_until_sampled(1.0))
        now[0] = 10.25
        observation = provider.latest(inflight=2)
        self.assertTrue(observation.fresh)
        self.assertEqual(observation.sample_age_s, 0.25)
        now[0] = 10.75
        stale = provider.latest(inflight=2)
        self.assertFalse(stale.fresh)
        self.assertEqual(stale.sample_age_s, 0.75)
    finally:
        provider.close()
```

Add validation coverage:

```python
with self.assertRaisesRegex(ValueError, "sample_age_s"):
    AdmissionObservation(
        observed_at_s=1.0,
        fresh=True,
        inflight=0,
        running=0,
        waiting=0,
        kv_usage=0.0,
        sample_age_s=-0.1,
    )
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_dynamic_admission.py -v
```

Expected: failure because `AdmissionObservation` has no `sample_age_s`.

- [ ] **Step 3: Add the minimal schema and protocol**

Append a defaulted field to `AdmissionObservation`:

```python
sample_age_s: float | None = None
```

Validate it is finite and non-negative when present. Add the same defaulted
field to `AdmissionTraceEvent`.

In `admission.py`, replace the concrete provider annotation with:

```python
class ObservationProvider(Protocol):
    def latest(self, inflight: int) -> AdmissionObservation:
        ...
```

Compute age in providers:

```python
sample_age_s = (
    now - sampled_at_s
    if sampled_at_s is not None
    else None
)
```

For the synchronous cached provider, age is `0.0` on a new sample and elapsed
time since `_last_sample_s` on a cached sample. Pass the observation age into
`AdmissionTraceEvent` in `DynamicAdmissionGate`.

- [ ] **Step 4: Run focused admission tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_dynamic_admission.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```powershell
git add code/src/scheduling/models.py code/src/scheduling/observations.py code/src/scheduling/admission.py code/tests/scheduling/test_dynamic_admission.py
git commit -m "feat: trace adaptive observation age"
```

---

### Task 2: Production Non-Blocking Adaptive Observation Lifecycle

**Files:**
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`
- Test: `code/tests/observability/test_postgres_profile_scheduling.py`

**Interfaces:**
- Consumes: `NonBlockingMetricsObservationProvider`
- Changes: `_build_adaptive_config(...) -> dict` includes `observation_provider`
- Produces: control trace column `sample_age_s`

- [ ] **Step 1: Write failing construction and latency tests**

Add:

```python
def test_typed_adaptive_config_uses_nonblocking_provider(self) -> None:
    config = profile._build_adaptive_config(
        scheduling_policy="aimd",
        metrics_url="http://metrics",
        trace_events=[],
        min_window=4,
        max_window=16,
        initial_window=4,
        sample_interval_s=0.25,
        ewma_alpha=0.5,
        pid_proportional_gain=1.0,
        pid_integral_gain=0.0,
        pid_derivative_gain=0.0,
    )
    try:
        self.assertIsInstance(
            config["observation_provider"],
            NonBlockingMetricsObservationProvider,
        )
    finally:
        config["observation_provider"].close()
```

Patch `_service_metrics_snapshot` with a blocking event, then assert
`config["admission_gate"].decide(0)` returns in less than `0.05s`; release and
close the provider in `finally`. The decision must hold its initial window
while no sample exists.

- [ ] **Step 2: Run the focused profiler test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: failure because `_build_adaptive_config` still constructs
`CachedMetricsObservationProvider` and returns no provider.

- [ ] **Step 3: Wire the existing background provider**

Replace the cached provider construction with:

```python
provider = NonBlockingMetricsObservationProvider(
    lambda: _service_metrics_snapshot(metrics_url),
    poll_interval_s=sample_interval_s,
    stale_after_s=max(0.5, sample_interval_s * 2),
    close_timeout_s=2.0,
)
```

Return it as `observation_provider`. Keep it out of the controller and Ray
adapter APIs.

Initialize `adaptive_observation_provider = None` before the profiler's outer
`try`. After `_build_adaptive_config`, assign the returned provider. In the
outer `finally`, close it before closing the database:

```python
if adaptive_observation_provider is not None:
    adaptive_observation_provider.close()
```

Do not wait for an initial sample in the measured path. Missing metrics already
produce a hold decision.

Add `"sample_age_s"` to `_write_control_trace`; write an empty value when the
age is unavailable.

- [ ] **Step 4: Add cleanup-on-error coverage**

Patch provider `close`, make the submitted operator raise, and assert `close`
is called once. Also assert static scheduling never creates the provider.

- [ ] **Step 5: Run focused profiler and admission tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_dynamic_admission.py -v
```

Expected: all tests pass; the slow-sampler latency assertion is below `0.05s`.

- [ ] **Step 6: Commit**

```powershell
git add code/scripts/profiling/postgres_ai_operator_profile.py code/tests/observability/test_postgres_profile_scheduling.py
git commit -m "fix: keep adaptive metric scrapes off submission path"
```

---

### Task 3: Pure BFD-Inspired Row-Cap-First Packing

**Files:**
- Modify: `code/src/packing.py`
- Modify: `code/tests/planning/test_packing.py`

**Interfaces:**
- Produces:

```python
def row_cap_aware_best_fit_decreasing(
    items: Sequence[PackItem],
    *,
    capacity: int,
    max_rows: int,
) -> tuple[tuple[int, ...], ...]:
```

- Consumes: existing `PackItem` and `_OpenBatch`

- [ ] **Step 1: Write the canonical failing membership test**

Import the new function and add:

```python
def test_row_cap_first_avoids_classic_bfd_fragmentation(self) -> None:
    source = items([1, 1, 2, 3, 3, 8])
    self.assertEqual(
        best_fit_decreasing(source, capacity=10, max_rows=3),
        ((5, 2), (3, 4, 0), (1,)),
    )
    self.assertEqual(
        row_cap_aware_best_fit_decreasing(
            source,
            capacity=10,
            max_rows=3,
        ),
        ((5, 0, 1), (3, 4, 2)),
    )
```

Add tests that reuse the classic BFD input sets for deterministic ties,
oversized singleton rows, duplicate row indexes, empty input, invalid capacity,
and invalid row cap.

- [ ] **Step 2: Run packing tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_packing.py -v
```

Expected: import failure for `row_cap_aware_best_fit_decreasing`.

- [ ] **Step 3: Implement the minimal deterministic algorithm**

Reuse the same decreasing order:

```python
ordered = sorted(
    items,
    key=lambda item: (
        -item.cost_units,
        item.stable_id,
        item.row_index,
    ),
)
```

Among feasible batches, select:

```python
selected = min(
    eligible,
    key=lambda batch: (
        resolved_max_rows - len(batch.row_indexes) - 1,
        resolved_capacity - batch.total_cost_units - item.cost_units,
        batch.creation_index,
    ),
)
```

Do not introduce weights, generalized resources, or a strategy class.

- [ ] **Step 4: Run packing tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_packing.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```powershell
git add code/src/packing.py code/tests/planning/test_packing.py
git commit -m "feat: add row-cap-aware best-fit packing"
```

---

### Task 4: Shared Arrow/Daft Organizer and CLI Wiring

**Files:**
- Modify: `code/src/organizers.py`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`
- Modify: `code/tests/planning/test_organizers.py`
- Modify: `code/tests/observability/test_postgres_profile_scheduling.py`

**Interfaces:**
- Adds batching policy: `row_cap_aware_token_budget`
- Adds metric value: `packing_algorithm=row_cap_aware_best_fit_decreasing`
- Consumes: `row_cap_aware_best_fit_decreasing`

- [ ] **Step 1: Write failing organizer tests**

Create a six-row Arrow table with prompt-token costs
`[1, 1, 2, 3, 3, 8]`, `completion_max_tokens=0`, token budget `10`, and
batch size `3`. Assert:

```python
config = OrganizerConfig(
    batch_size=3,
    batching_policy="row_cap_aware_token_budget",
    token_budget=10,
    output_cost_mode="prompt_only",
)
organized = ArrowOrganizer(config).organize(table)
self.assertEqual(
    [batch.column("doc_id").to_pylist() for batch in organized.batches],
    [[5, 0, 1], [3, 4, 2]],
)
self.assertEqual(
    organized.metrics["packing_algorithm"],
    "row_cap_aware_best_fit_decreasing",
)
```

Run the same contract through `DaftOrganizer` and assert identical doc
membership and packing metrics.

- [ ] **Step 2: Run organizer tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_organizers.py -v
```

Expected: `unknown batching policy: row_cap_aware_token_budget`.

- [ ] **Step 3: Wire one shared implementation**

Extend `BatchingPolicy`, `_validate_batching_policy`, and
`_uses_token_budget`. Rename `_best_fit_batches` to accept an algorithm
callable or add a focused sibling `_row_cap_aware_batches`; both must construct
`PackItem` exactly once through a shared helper.

Dispatch in `organize_arrow_table`:

```python
if config.batching_policy == "row_cap_aware_token_budget":
    return _packing_batches(
        table,
        config,
        row_cap_aware_best_fit_decreasing,
    )
```

Treat both global packing policies identically in `DaftOrganizer`: collect the
complete organizer input once, concatenate, and call `organize_arrow_table`.
Do not duplicate packing logic in the Daft branch.

Add the policy to the profiler CLI choices and dry-run metadata.

- [ ] **Step 4: Run organizer and profiler tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_organizers.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: all tests pass and dry-run reports the new algorithm.

- [ ] **Step 5: Commit**

```powershell
git add code/src/organizers.py code/scripts/profiling/postgres_ai_operator_profile.py code/tests/planning/test_organizers.py code/tests/observability/test_postgres_profile_scheduling.py
git commit -m "feat: expose row-cap-aware organizer policy"
```

---

### Task 5: Full Regression and Real Daft→Ray Contract

**Files:**
- No planned file changes; regressions return to the originating task before
  this verification task is considered complete.

**Interfaces:**
- Verifies all earlier interfaces without adding behavior.

- [ ] **Step 1: Run the full unit/integration suite**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py" -v
```

Expected: all tests pass with no error or warning introduced by this phase.

- [ ] **Step 2: Run syntax and whitespace checks**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts code/tests
git diff --check
```

Expected: exit code `0`.

- [ ] **Step 3: Run the existing real Daft→Ray contract three times**

Run this command three times with real Daft and Ray installed:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py -v
```

The test covers both task and actor paths.

Expected: 12/12 contract checks pass; no fake performance result is recorded.

---

### Task 6: Real 64-Row Gate

**Files:**
- Create: `experiments/results/row_cap_aware_packing_gate_20260726/scenario_config.json`
- Create: generated gate CSV/traces/logs/manifest under the same directory
- Create: `experiments/results/row_cap_aware_packing_gate_20260726/README.md`
- Modify: `experiments/results/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Consumes: `code/scripts/experiments/run_ai_operator_scenarios.py`
- Produces scenarios: `seq_fixed`, `bfd_fixed`, `row_cap_fixed`

- [ ] **Step 1: Check the real environment**

Verify PostgreSQL, vLLM health, `/metrics`, GPU identity, and that
`vllm:estimated_flops_per_gpu_total` exists. Do not start the gate if the MFU
counter is absent.

- [ ] **Step 2: Write the three-scenario gate config**

Use common arguments matching the corrected 512/1024 studies except:

```json
{
  "schema_version": 1,
  "experiment_id": "row_cap_aware_packing_gate_20260726",
  "seed": 20260726,
  "warmup_runs_per_scenario": 1,
  "formal_repeats": 1,
  "common_args": [
    "--database-url", "postgresql://postgres:postgres@localhost:5432/ai_operator",
    "--total-rows", "64",
    "--db-fetch-rows", "64",
    "--ray-batch-rows", "16",
    "--operator", "ai_complete",
    "--executor", "ray_task",
    "--model-backend", "compatible_http",
    "--completion-endpoint-url", "http://localhost:8000/v1/completions",
    "--completion-model", "qwen2.5-1.5b",
    "--completion-request-timeout-s", "180",
    "--model-metrics-url", "http://localhost:8000/metrics",
    "--source-workload-name", "sharegpt_burstgpt",
    "--source-order", "doc_id",
    "--data-source", "daft_postgres",
    "--organizer", "daft",
    "--organizer-partition-mode", "none",
    "--daft-runner", "native",
    "--token-budget", "6144",
    "--cost-model-id", "qwen2.5-1.5b",
    "--cost-tokenizer-id", "qwen2.5-1.5b",
    "--output-cost-mode", "fixed_output_cap",
    "--scheduling-policy", "static",
    "--max-inflight", "8",
    "--model-workers", "2",
    "--flush-policy", "immediate",
    "--completion-max-tokens", "16",
    "--writeback-mode", "none",
    "--request-slo-ms", "10000",
    "--resource-sample-interval-s", "0.25",
    "--gpu-peak-tflops", "61.7",
    "--mfu-precision", "bf16_dense_fp32_accumulate"
  ],
  "scenarios": [
    {
      "scenario_id": "seq_fixed",
      "args": ["--batching-policy", "token_budget"]
    },
    {
      "scenario_id": "bfd_fixed",
      "args": ["--batching-policy", "best_fit_token_budget"]
    },
    {
      "scenario_id": "row_cap_fixed",
      "args": ["--batching-policy", "row_cap_aware_token_budget"]
    }
  ]
}
```

- [ ] **Step 3: Run the gate through the seeded scenario runner**

Expected: six successful runs, zero incidents.

- [ ] **Step 4: Audit gate invariants**

For every scenario assert:

- 64 input rows, 64 successes, 64 unique request/doc IDs;
- request→submission foreign keys valid;
- `batch_rows_max <= 16`;
- non-oversized batch cost `<= 6144`;
- resource trace non-empty;
- vLLM FLOP delta `> 0`;
- `mfu_status=ok`;
- final vLLM running/waiting gauges are zero.

If any assertion fails, stop before Task 7.

- [ ] **Step 5: Write the seven-part gate report and commit**

Document setup, design, audit, raw data, fact/inference boundaries, project
meaning, and next step. State that 64 rows are correctness evidence only.

```powershell
git add experiments/results/row_cap_aware_packing_gate_20260726 experiments/results/README.md PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "results: validate row-cap-aware packing gate"
```

---

### Task 7: 512 Screening, Repeats, and 1024 Held-Out Confirmation

**Files:**
- Create: `experiments/results/row_cap_aware_packing_512_20260726/`
- Create conditionally after a 512-row winner:
  `experiments/results/row_cap_aware_packing_1024_20260726/`
- Modify: `experiments/results/README.md`
- Modify: `overview/current_direction_and_plan.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Produces an evidence-based mechanism decision.

- [ ] **Step 1: Run one-repeat 512 screening**

Screen:

```text
row cap ∈ {16, 32, 64}
token budget ∈ {4096, 6144, 8192}
algorithm ∈ {sequential, classic BFD, BFD-inspired row-cap-aware}
```

Use one warm-up per algorithm/config and one formal repeat. Keep the same 512
doc IDs, source order, model, output cap, K_max, endpoint, and measurement
configuration.

- [ ] **Step 2: Apply the documented pruning rule**

Discard a candidate when it is more than 10% below the matching sequential
tokens/s baseline and improves neither request P95 nor energy per 1k tokens.
Also discard any correctness/MFU-invalid run.

- [ ] **Step 3: Run three repeats for survivors**

Always include the matching sequential baseline. Include classic BFD only as
the component baseline needed to interpret the hybrid. Interleave scenario
order with the seeded runner.

- [ ] **Step 4: Decide whether 1024 confirmation is warranted**

Advance a row-cap-aware candidate only if its 512 mean is not worse than
sequential on tokens/s and it improves at least one of request P95, SLO
goodput, energy/1k tokens, or MFU without a material regression in the others.

If no candidate advances, report sequential as the recommendation and stop.

- [ ] **Step 5: Run held-out 1024 confirmation**

Use the winning 512 configuration unchanged. Compare only the winner,
sequential, and classic BFD when needed for mechanism attribution. Run one
warm-up and three formal repeats.

- [ ] **Step 6: Summarize comprehensive metrics**

Report means and standard deviations for:

- rows/s and tokens/s;
- E2E, request P50/P95/P99, SLO violation and goodput;
- submission count and packing utilization;
- GPU utilization/memory/power/energy;
- energy per 1k observed tokens;
- vLLM running/waiting/KV;
- FLOP delta and MFU.

Recompute summaries from `runs.csv`; do not manually transcribe values.

- [ ] **Step 7: Update project conclusions and commit**

State exactly which mechanisms survive:

- complete classic BFD;
- decreasing order only;
- row-cap-first placement;
- shared constraints/diagnostics only;
- or sequential alone.

```powershell
git add experiments/results overview/current_direction_and_plan.md PROJECT_OUTLINE.md PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "results: compare row-cap-aware packing mechanisms"
```
