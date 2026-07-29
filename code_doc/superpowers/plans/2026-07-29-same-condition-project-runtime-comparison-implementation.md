# Same-Condition Project Runtime Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the project Daft+Ray scheduler and the official/direct baselines
against the same frozen Chat Completions requests, then compare saturation
speed, steady-state capacity, queue pressure, JCT, and tail latency.

**Architecture:** The existing profiler remains the project-runtime executor.
A small profiling manifest guard validates and annotates PostgreSQL rows before
organization, while an offline request expansion path turns every row into one
complete Chat request without arrival replay. A pinned endpoint router provides
the causal framework-cost arm; the existing least-work router remains the
optimized arm. Existing scenario and official-baseline runners execute separate
calibration schedules and a common 2,048-row held-out schedule.

**Tech Stack:** Python 3.12, PyArrow, Daft, Ray actors, PostgreSQL, vLLM Chat
Completions, JSONL/SHA-256 manifests, unittest, AutoDL dual RTX 4090.

## Global Constraints

- Use `code_doc/superpowers/plans/2026-07-29-same-condition-official-baselines-design.md`
  as the approved specification.
- Every production behavior starts with a failing test and a witnessed RED run.
- The 512-row calibration manifest and 2,048-row held-out manifest are disjoint.
- All comparable arms use one HTTP Chat Completions request per manifest row,
  temperature `0`, max output tokens `256`, no arrival replay, the same model,
  the same two endpoints, and the same endpoint service configuration.
- The pinned project arm obeys the manifest endpoint assignment. Dynamic routing
  is reported only as a separate optimization arm.
- Formal validity requires exactly-once, zero failed rows, both endpoints used,
  positive service token deltas, and empty final vLLM queues.
- Official systems are calibrated only through their documented knobs. Do not
  rewrite their scheduling implementation to improve a baseline.
- OceanBase is a numeric arm only if the Community Edition capability gate
  exposes `AI_COMPLETE`, permits the same local vLLM endpoints, and preserves
  auditable request semantics.
- Do not modify vLLM, Ray scheduler internals, or Daft internals.
- Do not touch opening-slide files and do not sync Wiki.
- Commit messages contain no AI attribution.

---

### Task 1: Support Offline Request-Level Continuous Replenishment

**Files:**
- Modify: `code/src/profiling/replay.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Consumes: `_offline_batch_envelopes(..., submission_granularity="request")`.
- Produces: one `PayloadEnvelope` and one request-lifecycle seed per Arrow row.

- [ ] **Step 1: Write the failing offline-request expansion test**

```python
def test_offline_request_granularity_emits_one_envelope_per_row(self) -> None:
    batch = pa.table(
        {
            "doc_id": [11, 12],
            "text": ["a", "b"],
            "prompt_tokens": [3, 5],
            "target_output_tokens": [7, 9],
            "prefix_key": ["", ""],
        }
    )
    envelopes, seeds = profile._offline_batch_envelopes(
        [batch],
        job_id="job",
        operator="ai_complete",
        completion_max_tokens=256,
        output_cost_mode="target_output_tokens",
        batch_index_start=0,
        job_start_epoch_s=10.0,
        ready_epoch_s=10.1,
        submission_granularity="request",
    )
    self.assertEqual([item.request.row_count for item in envelopes], [1, 1])
    self.assertEqual(
        [item.request.request_id for item in envelopes],
        ["job:request:11", "job:request:12"],
    )
    self.assertEqual(
        [seed.submission_id for seed in seeds],
        ["job:request:11", "job:request:12"],
    )
```

- [ ] **Step 2: Run the test and witness RED**

Run:

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

Expected: FAIL with `offline envelope expansion supports batch or
service_quantum`.

- [ ] **Step 3: Implement the minimal row expansion**

Add an offline helper that slices each row, computes its prompt/output work,
uses `f"{job_id}:request:{doc_id}"`, and preserves the parent
`planning_batch_id`. Select it in `_offline_batch_envelopes()` when
`submission_granularity == "request"`.

- [ ] **Step 4: Remove the obsolete replay-only validation**

Delete only the validation that rejects request granularity without
`--arrival-replay`. Ensure the non-replay execution constructs offline
envelopes whenever granularity is `request`, even if request tracing is off.
Report request-level latency granularity for this path.

- [ ] **Step 5: Run focused and full tests**

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests
```

Expected: PASS.

---

### Task 2: Lock the Project Profiler to the Frozen Request Manifest

**Files:**
- Create: `code/src/profiling/manifest_guard.py`
- Modify: `code/src/profiling/cli.py`
- Modify: `code/src/profiling/schema.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: `code/tests/test_profile_manifest_guard.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Produces:
  - `ProfileManifestGuard.from_path(path, endpoint_ids)`
  - `guard.validate_and_annotate(table) -> pa.Table`
  - `guard.finish() -> ProfileManifestEvidence`
  - CLI `--request-manifest PATH`

- [ ] **Step 1: Write failing manifest-equivalence tests**

```python
def test_guard_validates_rows_and_adds_pinned_endpoint_id(self) -> None:
    guard = ProfileManifestGuard(
        requests=(
            sample_request(1, prompt="one", prompt_tokens=3, endpoint_index=1),
            sample_request(2, prompt="two", prompt_tokens=4, endpoint_index=0),
        ),
        manifest_sha256="a" * 64,
        endpoint_ids=("endpoint-0", "endpoint-1"),
    )
    annotated = guard.validate_and_annotate(
        pa.table(
            {
                "doc_id": [1, 2],
                "text": ["one", "two"],
                "prompt_tokens": [3, 4],
                "target_output_tokens": [256, 256],
            }
        )
    )
    self.assertEqual(
        annotated["preferred_endpoint_id"].to_pylist(),
        ["endpoint-1", "endpoint-0"],
    )
    self.assertEqual(guard.finish().validated_rows, 2)
```

Add separate tests for prompt mismatch, prompt-token mismatch, target-output
mismatch, duplicate row, missing row, unknown endpoint index, and manifest
row-count versus `--total-rows` mismatch.

- [ ] **Step 2: Run and witness RED**

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_profile_manifest_guard.py
```

Expected: import failure because `manifest_guard` does not exist.

- [ ] **Step 3: Implement a fail-closed guard**

Reuse `src.baselines.manifests.read_manifest()` and SHA-256 metadata. Match by
`doc_id`; compare exact prompt text and integer token fields; append one Arrow
string column named `preferred_endpoint_id`. `finish()` rejects unseen or
duplicate manifest rows.

- [ ] **Step 4: Wire the CLI and result evidence**

Add `--request-manifest`. Require:

```text
--operator ai_complete
--completion-protocol chat_completions
--source-order doc_id
--output-cost-mode trace_target_output
--completion-max-tokens == every manifest max_output_tokens
--total-rows == manifest row count
```

Record the resolved path, SHA-256, row count, validated count, and validation
status in the formal CSV row.

- [ ] **Step 5: Run focused and full tests**

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_profile_manifest_guard.py
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests
```

Expected: PASS.

---

### Task 3: Add Manifest-Pinned Endpoint Routing

**Files:**
- Modify: `code/src/scheduling/models.py`
- Modify: `code/src/scheduling/endpoint_routing/policies.py`
- Modify: `code/src/scheduling/routing.py`
- Modify: `code/src/scheduling/__init__.py`
- Modify: `code/src/profiling/replay.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: `code/tests/test_scheduling_models.py`
- Test: `code/tests/test_scheduling_policies.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Adds `BatchRequest.preferred_endpoint_id: str = ""`.
- Produces `PinnedEndpointRouter.route(request, topology, pool_id)`.
- Adds profiler choice `--endpoint-routing manifest_pinned`.

- [ ] **Step 1: Write failing routing tests**

```python
def test_pinned_router_selects_the_requested_healthy_endpoint(self) -> None:
    request = request_fixture(preferred_endpoint_id="endpoint-1")
    decision = PinnedEndpointRouter().route(
        request,
        topology_fixture(),
        "default",
    )
    self.assertEqual(decision.endpoint_id, "endpoint-1")
    self.assertEqual(decision.reason, "manifest_pinned")
```

Add tests rejecting a missing pin, an unknown pin, an unhealthy pin, and a pin
outside the selected pool.

- [ ] **Step 2: Run and witness RED**

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_scheduling_policies.py
```

Expected: import failure for `PinnedEndpointRouter`.

- [ ] **Step 3: Implement the router and propagate row pins**

Read `preferred_endpoint_id` from a one-row Arrow payload when building offline
request envelopes. Batch and service-quantum behavior remain unchanged and
leave the field empty. Add the router to `_build_routing_config()` without
changing existing defaults.

- [ ] **Step 4: Run focused and full tests**

Run the three focused files, then full unittest discovery. Expected: PASS.

---

### Task 4: Add Same-Condition Project Calibration and Held-Out Templates

**Files:**
- Create: `deploy/autodl/dual_gpu_same_condition_project_calibration.example.json`
- Create: `deploy/autodl/dual_gpu_same_condition_project_formal.example.json`
- Modify: `deploy/autodl/README.md`
- Modify: `code/scripts/README.md`
- Modify: `code/INFRA_STATUS.md`
- Modify: `experiments/plans/database_ai_operator_baseline_matrix_20260729.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`
- Test: `code/tests/test_postgres_profile_scheduling.py`
- Test: `code/tests/test_experiment_scenario_runner.py`

**Interfaces:**
- Calibration scenarios:
  - pinned static request K `{32,64,128,256}` per endpoint;
  - token-work `{16384,32768,65536,98304,131072}` per endpoint;
  - least-work routing only after the pinned causal arm passes.
- Formal scenarios freeze the smallest 97%-ceiling point for bounded HTTP,
  project static, and project token-work.

- [ ] **Step 1: Add failing template contract tests**

Load both JSON files and assert Chat protocol, no `--arrival-replay`, request
granularity, manifest path, fixed 512/2,048 row counts, 1 warmup, 3 formal
repeats, and an explicit single Ray address.

- [ ] **Step 2: Witness RED, then add the minimal templates**

The calibration template uses the disjoint 512-row manifest and no formal
repeats beyond the pre-registered single-run curve. The held-out template uses
2,048 rows, seeded interleaving, and only frozen configurations.

- [ ] **Step 3: Document the exact startup and stop conditions**

Document preflight, manifest export/hash, endpoint health, Ray address, runner
lease, fresh output directory, monitoring, evidence preservation, and cleanup.
State that no 2,048 formal may start until the 64-row project gate and 512-row
calibration both pass.

- [ ] **Step 4: Run tests and diff checks**

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe code\tests\test_experiment_scenario_runner.py
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\Scripts\ruff.exe check code
git diff --check
```

Expected: PASS.

---

### Task 5: Publish, Run the Real Gate, and Calibrate Every Arm

**Files:**
- Create remote artifacts under fresh directories only.
- Modify local code/tests only if a preserved remote failure has a regression
  test.

- [ ] **Step 1: Verify and publish the isolated branch**

Run full tests, ruff, compileall, and `git diff --check`. Review the exact diff,
commit without AI attribution, and push the isolated branch. Do not publish the
other conversation's slide changes.

- [ ] **Step 2: Perform the documented remote preflight**

Verify no runner/lease, endpoints idle, Ray idle, GPU idle, and git/untracked
results safe. Use a clean remote worktree at the pushed commit.

- [ ] **Step 3: Run a 64-row project profiler gate**

Use Chat/no-replay/request granularity and the frozen manifest. Require 64/64
exactly-once, manifest validation, both pinned endpoints, positive service
counter deltas, request trace coverage, zero worker failures, and final empty
queues.

- [ ] **Step 4: Run the 512-row calibration**

Run direct vLLM/bounded C128 and C256, independently calibrate official Daft
Native/Ray and Ray Data documented knobs, then run project static K and
token-work curves. Stop an arm after the first valid adjacent point below 3%
gain when the lower point is at least 97% of that arm's maximum safe
throughput.

- [ ] **Step 5: Apply the OceanBase capability gate**

If unavailable or semantically non-equivalent, preserve evidence and mark it
unsupported; do not replace it with a custom HTTP loop.

- [ ] **Step 6: Freeze formal configurations**

Write chosen parameters, manifest hashes, service metadata, excluded arms and
reasons into the formal manifest before any held-out run.

---

### Task 6: Run the 2,048-Row Formal Matrix and Produce the Comparison

**Files:**
- Create: `experiments/results/dual_gpu_same_condition_baseline_formal_<id>/`
- Modify: `experiments/plans/database_ai_operator_baseline_matrix_20260729.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `code/INFRA_STATUS.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `overview/current_direction_and_plan.md`
- Modify: `PROJECT_LOG.md`

- [ ] **Step 1: Run one warmup and three seeded, interleaved formal repeats**

Required numeric arms are direct vLLM, bounded HTTP, valid official Daft
Native/Ray, valid Ray Data, project pinned-static, and project token-work. Add
OceanBase only after its gate passes.

- [ ] **Step 2: Validate every repeat before aggregation**

Reject any repeat with manifest mismatch, output-work drift above 1%, failure,
duplicate/missing row, endpoint omission, service-counter contamination,
non-empty final queues, or missing trace evidence.

- [ ] **Step 3: Compute the pre-registered metrics**

Report service-counter total/generation tokens/s, JCT, comparable request
P50/P95/P99, capacity efficiency, time-to-95%-ceiling, ramp regret, minimum
saturating request/work pressure, GPU/MFU, vLLM/upstream queue peaks, endpoint
work skew, and writeback boundaries.

- [ ] **Step 4: Apply the claim thresholds**

Call throughput/JCT acceleration only for at least 5% improvement, 2/3 repeats
in the same direction, worst-repeat regression no more than 3%, and no P99 or
correctness regression. If throughput is within ±3%, call only pressure
efficiency/transient improvement when active work drops at least 20% or
P99/time-to-ceiling/ramp regret improves at least 10%.

- [ ] **Step 5: Write the seven-step result report**

Separate facts, supported inferences, unresolved questions, and claims the data
cannot support. Explicitly answer whether project scheduling turns an
underfilled approximately 15-second job into a near-ceiling shorter job, and
whether any benefit remains after bounded HTTP is independently saturated.

- [ ] **Step 6: Verify, commit, and push only experiment-related files**

Run result-schema checks and `git diff --check`; inspect the staged file list;
commit without AI attribution and push the experiment branch. Keep slide files
and Wiki out of the commit.
