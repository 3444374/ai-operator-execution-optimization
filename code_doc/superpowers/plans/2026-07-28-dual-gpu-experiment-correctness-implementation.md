# Dual-GPU Experiment Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent invalid dual-GPU capacity and shared-scheduling experiments by enforcing complete service metadata, preserving pre-expansion organization metrics, requiring one Ray cluster for shared credit, and revising the staged AutoDL templates.

**Architecture:** Keep policy code independent of Ray and Daft. Add validation at the scenario/profiler boundaries, compute organization metrics from the existing pre-submission packing observations, and make Ray cluster ownership explicit only when cross-job coordination is requested. Preserve existing summary fields for compatibility.

**Local command convention:** Run named `unittest` modules from the repository's
`code/` directory as `tests.*`, using
`..\.conda\pg-ai-profile\python.exe`. Running `code.tests.*` from the repository
root collides with Python's standard-library `code` module on Windows.

**Tech Stack:** Python 3.12, unittest, PyArrow, Ray 2.56.1, Daft 0.7.21, JSON scenario templates, Ruff.

## Global Constraints

- Do not modify vLLM or Ray's internal scheduler.
- Every production behavior change starts with a failing test.
- Formal CSV rows continue to record `server_version` and `pgvector_version`.
- Existing `batch_rows_*` fields retain submission-payload semantics.
- Formal GPU matrices are not launched during development verification.
- Commit messages contain no `Co-Authored-By` or AI attribution.

---

### Task 1: Reject incomplete formal service metadata

**Files:**
- Modify: `code/src/experiment_scenarios.py`
- Modify: `code/tests/test_experiment_scenarios.py`

**Interfaces:**
- Consumes: `validate_service_metadata(metadata: Mapping[str, object]) -> None`
- Produces: the same interface with strict rejection of empty strings and the `unknown` capacity sentinel.

- [ ] **Step 1: Replace the permissive test with failing strict tests**

```python
def test_service_metadata_rejects_unknown_capacity(self) -> None:
    for key in ("max_num_batched_tokens", "max_num_seqs"):
        metadata = self._complete_service_metadata()
        metadata[key] = "unknown"
        with self.assertRaisesRegex(ValueError, key):
            validate_service_metadata(metadata)

def test_service_metadata_rejects_empty_required_strings(self) -> None:
    for key in ("vllm_version", "compilation_mode"):
        metadata = self._complete_service_metadata()
        metadata[key] = " "
        with self.assertRaisesRegex(ValueError, key):
            validate_service_metadata(metadata)
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_experiment_scenarios.ExperimentScenarioTests.test_service_metadata_rejects_unknown_capacity `
  tests.test_experiment_scenarios.ExperimentScenarioTests.test_service_metadata_rejects_empty_required_strings -v
```

Expected: both tests fail because `unknown` and blank required strings are currently accepted.

- [ ] **Step 3: Implement strict metadata validation**

```python
for key in ("vllm_version", "compilation_mode"):
    value = metadata[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")

for key in ("max_num_batched_tokens", "max_num_seqs"):
    capacity = metadata[key]
    if (
        isinstance(capacity, bool)
        or not isinstance(capacity, int)
        or capacity <= 0
    ):
        raise ValueError(f"{key} must be a positive integer")
```

- [ ] **Step 4: Run the scenario tests and verify GREEN**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_experiment_scenarios -v
```

Expected: all scenario tests pass.

- [ ] **Step 5: Commit the metadata gate**

```powershell
git add code/src/experiment_scenarios.py code/tests/test_experiment_scenarios.py
git commit -m "test: require concrete vllm capacity metadata"
```

---

### Task 2: Preserve organization metrics across request expansion

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/src/profiling/schema.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Consumes: `_packing_run_metrics(batch_cost_units, batch_row_counts, *, capacity, row_cap, packing_scope, packing_algorithm) -> dict`
- Produces: six explicit `organization_*` fields plus existing packing fields.

- [ ] **Step 1: Add failing metric assertions**

Extend `test_packing_run_metrics_aggregate_exact_batch_costs`:

```python
metrics = profile._packing_run_metrics(
    batch_cost_units=[8, 12],
    batch_row_counts=[2, 1],
    capacity=10,
    row_cap=2,
    packing_scope="fetch_chunk_local",
    packing_algorithm="best_fit_decreasing",
)
self.assertEqual(metrics["organization_batch_count"], 2)
self.assertEqual(metrics["organization_batch_rows_mean"], 1.5)
self.assertEqual(metrics["organization_batch_rows_max"], 2)
self.assertEqual(metrics["organization_batch_cost_units_mean"], 10.0)
self.assertEqual(metrics["organization_batch_cost_units_p95"], 12.0)
self.assertEqual(metrics["organization_row_cap_hit_ratio"], 0.5)
```

Add a request-expansion regression assertion to
`test_request_granularity_expands_closed_batch_into_complete_requests`:

```python
self.assertEqual(packing, [(30, 2)])
metrics = profile._packing_run_metrics(
    [cost for cost, _ in packing],
    [rows for _, rows in packing],
    capacity=0,
    row_cap=8,
    packing_scope="arrival_order",
    packing_algorithm="sequential_pending",
)
self.assertEqual(metrics["organization_batch_rows_mean"], 2.0)
self.assertEqual([item.request.row_count for item in envelopes], [1, 1])
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_postgres_profile_scheduling.SchedulingProfileHelperTests.test_packing_run_metrics_aggregate_exact_batch_costs `
  tests.test_postgres_profile_scheduling.SchedulingProfileHelperTests.test_request_granularity_expands_closed_batch_into_complete_requests -v
```

Expected: failure because `_packing_run_metrics` has no `row_cap` parameter or organization fields.

- [ ] **Step 3: Implement the minimal organization summary**

Add a small percentile helper only if the existing packing summary does not
already expose the required mean values:

```python
organization_batch_count = len(batch_row_counts)
organization_batch_rows_mean = (
    sum(batch_row_counts) / organization_batch_count
    if organization_batch_count
    else 0.0
)
organization_batch_cost_units_mean = (
    sum(batch_cost_units) / organization_batch_count
    if organization_batch_count
    else 0.0
)
row_cap_hits = (
    sum(rows >= row_cap for rows in batch_row_counts)
    if row_cap > 0
    else 0
)
```

Return:

```python
"organization_batch_count": organization_batch_count,
"organization_batch_rows_mean": round(organization_batch_rows_mean, 6),
"organization_batch_rows_max": max(batch_row_counts, default=0),
"organization_batch_cost_units_mean": round(
    organization_batch_cost_units_mean,
    6,
),
"organization_batch_cost_units_p95": summary.cost_units_p95,
"organization_row_cap_hit_ratio": round(
    row_cap_hits / organization_batch_count,
    6,
) if organization_batch_count else 0.0,
```

Pass `row_cap=args.ray_batch_rows` from dry-run and real-run call sites. Insert
the six fields next to the existing packing fields in
`FORMAL_RESULT_FIELDS`, and merge them into both dry-run and formal result
rows through the existing `**dry_packing_metrics` / `**packing_metrics`
paths.

- [ ] **Step 4: Run profiler tests and verify GREEN**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_postgres_profile_scheduling -v
```

Expected: all profiler scheduling tests pass and formal-row schema validation
accepts the new ordered fields.

- [ ] **Step 5: Commit organization metric semantics**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/src/profiling/schema.py code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: separate organization and submission metrics"
```

---

### Task 3: Require an explicit shared Ray cluster

**Files:**
- Modify: `code/src/profiling/cli.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/scripts/run_kmax_interference_experiment.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`
- Modify: `code/tests/test_kmax_interference_script.py`

**Interfaces:**
- Produces: `--ray-address`, defaulting to `RAY_ADDRESS` and otherwise empty.
- Produces: shared-credit validation requiring a non-empty Ray address.
- Produces: the multi-job runner forwarding the same address to foreground and background profiler processes.

- [ ] **Step 1: Add failing profiler validation and initialization tests**

```python
def test_shared_credit_requires_explicit_ray_address(self) -> None:
    args = profile.parse_args([
        "--dry-run",
        "--executor", "ray_task",
        "--shared-credit-coordinator-name", "credits",
        "--shared-credit-request-limit", "64",
        "--shared-credit-work-limit", "32768",
        "--shared-credit-quantum", "2048",
    ])
    with self.assertRaisesRegex(SystemExit, "ray-address"):
        profile.run_once(args, "formal", 1)

def test_ray_address_is_forwarded_to_ray_init(self) -> None:
    args = profile.parse_args([
        "--executor", "ray_task",
        "--ray-address", "auto",
    ])
    self.assertEqual(args.ray_address, "auto")
```

Update the existing shared-credit dry-run test to include:

```python
"--ray-address", "auto",
```

- [ ] **Step 2: Add a failing multi-job command propagation test**

In `test_kmax_interference_script.py`, construct parser arguments with
`--ray-address auto`, call `profile_command`, and assert:

```python
self.assertIn("--ray-address", command)
self.assertEqual(command[command.index("--ray-address") + 1], "auto")
```

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_postgres_profile_scheduling `
  tests.test_kmax_interference_script -v
```

Expected: failures because neither CLI currently exposes `--ray-address`.

- [ ] **Step 4: Implement the explicit address contract**

Add to the profiler CLI:

```python
parser.add_argument(
    "--ray-address",
    default=os.environ.get("RAY_ADDRESS", ""),
)
```

Before building `shared_credit_config`:

```python
if args.shared_credit_coordinator_name and not args.ray_address:
    raise SystemExit(
        "shared credit requires --ray-address or RAY_ADDRESS so all jobs "
        "connect to one Ray cluster"
    )
```

Initialize Ray with:

```python
ray_init_kwargs = {
    "ignore_reinit_error": True,
    "runtime_env": ray_runtime_env(),
}
if args.ray_address:
    ray_init_kwargs["address"] = args.ray_address
ray_module.init(**ray_init_kwargs)
```

Add `--ray-address` to the multi-job runner parser and append it to every
profiler command when non-empty:

```python
if args.ray_address:
    command.extend(["--ray-address", args.ray_address])
```

- [ ] **Step 5: Run both test modules and verify GREEN**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_postgres_profile_scheduling `
  tests.test_kmax_interference_script -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit the shared-cluster guard**

```powershell
git add code/src/profiling/cli.py code/scripts/postgres_ai_operator_profile.py code/scripts/run_kmax_interference_experiment.py code/tests/test_postgres_profile_scheduling.py code/tests/test_kmax_interference_script.py
git commit -m "fix: require one ray cluster for shared credit"
```

---

### Task 4: Revise the AutoDL templates and documentation

**Files:**
- Modify: `deploy/autodl/dual_gpu_active_work_curve.example.json`
- Modify: `deploy/autodl/dual_gpu_token_budget_curve.example.json`
- Modify: `deploy/autodl/dual_gpu_data_organization.example.json`
- Modify: `deploy/autodl/autodl.env.example`
- Modify: `deploy/autodl/README.md`
- Modify: `code/tests/test_experiment_scenarios.py`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `experiments/plans/data_organization_batching.md`
- Modify: `experiments/plans/service_scheduling_backpressure.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- First capacity variable: request-level
  `max_active_work_per_endpoint`, with all organization settings fixed.
- Work-normalized budget scenarios: `tb8192`, `tb16384`, `tb24576`,
  `tb32768`, `tb49152`, `tb65536`.
- Required environment: `ACTIVE_WORK_PER_ENDPOINT`,
  `VLLM_MAX_NUM_BATCHED_TOKENS`, `VLLM_MAX_NUM_SEQS`, `REQUEST_SLO_MS`.
- Data-organization input scale: 1024 rows with `ray_batch_rows=256`.

- [ ] **Step 1: Update the template-expansion test first**

Extend the test environment:

```python
"VLLM_MAX_NUM_BATCHED_TOKENS": "8192",
"VLLM_MAX_NUM_SEQS": "256",
"REQUEST_SLO_MS": "30000",
"ACTIVE_WORK_PER_ENDPOINT": "65536",
```

Assert the corrected capacity config:

```python
capacity = _load_config(
    CODE_ROOT.parent / "deploy" / "autodl"
    / "dual_gpu_token_budget_curve.example.json"
)
self.assertEqual(
    [scenario.scenario_id for scenario in capacity.scenarios],
    ["tb8192", "tb16384", "tb24576", "tb32768", "tb49152", "tb65536"],
)
self.assertIn("256", capacity.common_args)
self.assertIn("65536", capacity.common_args)
self.assertEqual(capacity.service_metadata["max_num_seqs"], 256)

active_work = _load_config(
    CODE_ROOT.parent / "deploy" / "autodl"
    / "dual_gpu_active_work_curve.example.json"
)
self.assertIn("request", active_work.common_args)
```

- [ ] **Step 2: Run the template test and verify RED**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_experiment_scenarios.ExperimentScenarioTests.test_committed_dual_gpu_templates_expand_and_validate -v
```

Expected: failure because the committed token-budget template still changes
offered request concurrency with batch size and uses a 64-row cap.

- [ ] **Step 3: Apply the minimal JSON changes**

For all three templates:

```json
"max_num_batched_tokens": "${VLLM_MAX_NUM_BATCHED_TOKENS}",
"max_num_seqs": "${VLLM_MAX_NUM_SEQS}"
```

The active-work capacity template must use request-level submission and vary
only `--max-active-work-per-endpoint`. Its organization budget is a fixed,
predeclared non-treatment setting.

For the token-budget and data-organization templates use:

```json
"--ray-batch-rows", "256",
"--max-active-work-per-endpoint", "${ACTIVE_WORK_PER_ENDPOINT}",
"--request-slo-ms", "${REQUEST_SLO_MS}"
```

Set the data-organization template's `--total-rows` and `--db-fetch-rows` to
`1024`. Replace the token-budget scenarios with the six IDs and budgets
defined by this task. Set request/batch admission high enough that the
active-work limit, not K, is binding.

- [ ] **Step 4: Run the template test and verify GREEN**

Run:

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_experiment_scenarios -v
```

Expected: all scenario tests pass.

- [ ] **Step 5: Update operator-facing documentation**

Document these exact boundaries:

- the completed 32768 point is `BEST_TESTED_TOKEN_BUDGET`, not a sweet point;
- the completed budget sweep changed effective request concurrency from about
  9 to 256 per endpoint and is therefore an offered-load diagnostic;
- formal ordering is active-work capacity first, then token budget at fixed
  active work, then membership at fixed budget and work;
- the corrected budget curve must pass the row-cap/fill audit before the
  membership comparison;
- the membership comparison reports endpoint imbalance and submission count
  as mediators;
- shared multi-job runs start a Ray head and export `RAY_ADDRESS=auto`;
- the existing request replay is a low-fill replenishment test and may
  complete independently.

Update `PROJECT_INDEX.md` descriptions to distinguish the request-level
active-work capacity curve from the `8192–65536` work-normalized token-budget
curve.

- [ ] **Step 6: Commit templates and docs**

```powershell
git add deploy/autodl code/tests/test_experiment_scenarios.py PROJECT_INDEX.md PROJECT_OUTLINE.md experiments/plans PROJECT_LOG.md
git commit -m "docs: correct dual gpu experiment gates"
```

---

### Task 5: Verify locally, push, and test an independent remote worktree

**Files:**
- Modify: `code_doc/superpowers/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Produces: a pushed development branch usable by the remote experiment agent.
- Produces: remote-worktree evidence for tests, template expansion, and shared Ray smoke only.

- [ ] **Step 1: Run focused tests**

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_experiment_scenarios `
  tests.test_postgres_profile_scheduling `
  tests.test_kmax_interference_script -v
```

Expected: PASS.

- [ ] **Step 2: Run full local verification**

```powershell
..\.conda\pg-ai-profile\python.exe -m unittest discover -s tests -p "test_*.py"
..\.conda\pg-ai-profile\python.exe -m ruff check .
git diff --check
```

Expected: all tests pass, Ruff exits 0, and `git diff --check` reports no
errors.

- [ ] **Step 3: Update planning indexes and log**

Set the current plan in `code_doc/superpowers/README.md` to this file. Register
the design and implementation plan in `PROJECT_INDEX.md`. Add a
`PROJECT_LOG.md` entry containing local test counts and the remote smoke
boundary.

- [ ] **Step 4: Commit and push the development branch**

```powershell
git add code_doc/superpowers PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "docs: record dual gpu correctness implementation"
git push origin codex/architecture-deployment-audit
```

Expected: push succeeds with no AI attribution in commit metadata.

- [ ] **Step 5: Create an independent remote worktree**

On the remote host:

```bash
cd /root/autodl-tmp/ai-operator
source /etc/network_turbo >/dev/null 2>&1
git fetch origin codex/architecture-deployment-audit
git worktree add --detach \
  /root/autodl-tmp/worktrees/dual-gpu-correctness \
  origin/codex/architecture-deployment-audit
```

Expected: the worktree points to the pushed development commit and does not
alter the main experiment checkout.

- [ ] **Step 6: Run remote tests and template expansion**

```bash
cd /root/autodl-tmp/worktrees/dual-gpu-correctness/code
source /root/miniconda3/etc/profile.d/conda.sh
conda activate base
python -m unittest \
  tests.test_experiment_scenarios \
  tests.test_postgres_profile_scheduling \
  tests.test_kmax_interference_script -v
python -m ruff check .
```

Expected: PASS without launching PostgreSQL/vLLM formal profiles.

- [ ] **Step 7: Run a shared-Ray contract smoke**

First check for active profiler, scenario-runner, or experiment processes. If
any formal run is active, skip this step and record the reason. Never stop or
replace an experiment-owned Ray cluster.

When the host is idle, start an isolated Ray head with a non-default port and
temporary directory, export its concrete address, and run:

```bash
python -m unittest tests.test_shared_credit_ray -v
```

Expected: the named actor contract passes inside one explicit isolated Ray
cluster. Stop only that isolated cluster afterward. This smoke does not
collect performance data.

- [ ] **Step 8: Record verified handoff**

Add the pushed commit SHA, remote worktree path, remote test results, and exact
formal-run prohibition to `PROJECT_LOG.md`, then commit and push that final
documentation update.
