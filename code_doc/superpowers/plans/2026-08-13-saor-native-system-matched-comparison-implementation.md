# SAOR Native-System Matched Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fail-closed local execution and analysis path for the two-Job Daft Native/Daft Ray/Ray Data/Project comparison plus the same-regime Project bounded-ready FIFO/DRR/VTC-style/SAOR selector sanity block.

**Architecture:** Preserve the existing native and Project scheduler owners. Extend each path only enough to expose a common PostgreSQL-source-to-validated-gather cell contract, then use a thin matrix module to validate eight unique arms, generate a balanced schedule, invoke one cell at a time, and retain atomic evidence. A separate offline summarizer emits the system and Project-internal tables and never declares a winner or authorizes formal.

**Tech Stack:** Python 3.12+, dataclasses, JSON/CSV, `unittest`, existing PostgreSQL/Daft/Ray/vLLM adapters and shared-vLLM runner.

## Global Constraints

- Required unique arms are exactly `daft_native`, `daft_ray`, `ray_data_http`, `project_frozen_static`, `project_bounded_ready_fifo`, `project_bounded_ready_drr`, `project_bounded_ready_vtc_style`, and `project_bounded_ready_saor_0125we`.
- The system table contains the three native arms, Project frozen-static, and Project bounded-ready SAOR; the selector-sanity table contains Project bounded-ready FIFO/DRR/VTC-style/SAOR. The same physical SAOR cell feeds both tables.
- FIFO/DRR/VTC-style are Project controls, never native baselines. `bounded-ready FIFO` means `Project bounded-ready + global FIFO matched-control`; bounded-ready is not part of the FIFO algorithm.
- All Project selector-sanity arms share the same visible ready-set contract, maximum request/work envelope, ready bytes, actor topology, source, organizer, and non-selector arguments.
- Native arms keep framework-owned batching/backpressure/scheduling and must reject Project K/W, credit, coordinator, router, or bounded-ready flags.
- Arrival is exactly Job-level `bulk@0s -> foreground@5s`, eager within each Job. Per-request manifest arrival replay is disabled for this matrix.
- Native rankable cells load and verify PostgreSQL rows inside the measured Job lifecycle. Pre-timed JSONL materialization remains diagnostic and is not rankable.
- The performance matrix uses `writeback_mode=none`; a sink correctness gate is separate.
- Unsupported request P99/SLO is the literal state `unavailable` with a reason, never zero or replicated Job/shard completion time.
- Local implementation only in this batch: do not connect to the powered-off server and do not run long GPU formal experiments.
- Do not implement reservation, dynamic K, four-Job expansion, new debt caps, strict-priority reruns, or selector parameter search.
- Do not sync Wiki or cloud documents.

---

## Task 1: Common PostgreSQL Source and Eager-Arrival Foundations

**Files:**
- Modify: `code/src/baselines/text/orchestration/postgres_manifest.py`
- Modify: `code/src/baselines/text/orchestration/cli.py`
- Modify: `code/src/baselines/text/orchestration/native_multijob.py`
- Modify: `code/src/experiments/shared_vllm/config.py`
- Test: `code/tests/baselines/text/test_postgres_baseline_manifest.py`
- Test: `code/tests/baselines/text/test_native_multijob.py`
- Test: `code/tests/experiments/test_shared_vllm_experiment.py`

**Interfaces:**
- Produces: `load_manifest_postgres_requests(connection, *, workload_name: str, manifest: Sequence[ChatRequest]) -> tuple[ChatRequest, ...]`.
- Produces: native `run-shard` flags `--database-url`, `--source-workload-name`, and `--timed-postgres-source`.
- Produces: native shard summary fields `source_kind`, `source_timing_boundary`, `source_read_s`, and `source_validation_status`.
- Produces: `SharedVllmConfig.job_internal_arrival_contract: Literal["manifest_timed", "eager"]`.

- [ ] **Step 1: Write failing exact-manifest PostgreSQL tests**

Add tests that query by workload plus manifest doc IDs, restore manifest order,
retain endpoint assignment, and reject missing rows, duplicate rows, prompt/hash
drift, or a workload mismatch:

```python
requests = load_manifest_postgres_requests(
    connection,
    workload_name="sharegpt",
    manifest=(expected_endpoint_1, expected_endpoint_0),
)
self.assertEqual([item.doc_id for item in requests], [11, 10])
self.assertEqual([item.endpoint_index for item in requests], [1, 0])
```

- [ ] **Step 2: Run the source tests and verify red**

Run:

```powershell
python -m unittest discover -s code/tests/baselines/text -t code -p 'test_postgres_baseline_manifest.py'
```

Expected: failure because `load_manifest_postgres_requests` does not exist.

- [ ] **Step 3: Implement exact manifest-backed PostgreSQL loading**

Implement one query ordered independently of the manifest, construct canonical
source hashes with `source_row_hash`, compare every immutable field, then return
new `ChatRequest` objects in manifest order with the manifest endpoint index.
Reject any missing, extra, or duplicated database row before execution.

- [ ] **Step 4: Write failing native timed-source command and evidence tests**

Extend the native multijob fixture with:

```json
"source": {
  "kind": "timed_postgres_manifest",
  "database_url": "postgresql://postgres:postgres@localhost:5432/ai_operator",
  "workload_name": "sharegpt"
},
"job_internal_arrival_contract": "eager"
```

Assert that native shard commands include the three new source flags, keep
Project scheduling flags absent, and reject `manifest_timed` or an un-timed
JSONL source when `comparison_eligible=true`.

- [ ] **Step 5: Implement timed native source plumbing**

In `_run_shard`, start `time.perf_counter()` before opening PostgreSQL, use the
new exact loader, and end source timing after the verified request tuple exists.
The official Daft/Ray Data adapters continue to receive complete rows and own
their graph. Add the four source fields to `summary.json`. Update native job
validation to require both endpoint shard summaries to report
`timed_postgres_manifest`, `inside_job_barrier`, and `ok` for rankable cells.

- [ ] **Step 6: Write failing shared-vLLM eager-arrival tests**

Add tests for:

```python
self.assertEqual(config.job_internal_arrival_contract, "eager")
```

and the two inverse validation rules:

- `manifest_timed` requires `--arrival-replay`;
- `eager` rejects `--arrival-replay`.

Existing configs without the new field must retain `manifest_timed` behavior.

- [ ] **Step 7: Implement the explicit eager-arrival contract**

Add the config field with backward-compatible default `manifest_timed`. Do not
change profiler semantics: eager simply means the existing job process is
released at `arrival_offsets_s[j]` and its common args omit
`--arrival-replay`. Preserve the existing global Job clock and request-manifest
guard.

- [ ] **Step 8: Run Task 1 tests**

Run:

```powershell
python -m unittest discover -s code/tests/baselines/text -t code -p 'test_postgres_baseline_manifest.py'
python -m unittest discover -s code/tests/baselines/text -t code -p 'test_native_multijob.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_shared_vllm_experiment.py'
```

Expected: all selected tests pass.

- [ ] **Step 9: Commit Task 1**

```powershell
git add code/src/baselines/text/orchestration/postgres_manifest.py code/src/baselines/text/orchestration/cli.py code/src/baselines/text/orchestration/native_multijob.py code/src/experiments/shared_vllm/config.py code/tests/baselines/text/test_postgres_baseline_manifest.py code/tests/baselines/text/test_native_multijob.py code/tests/experiments/test_shared_vllm_experiment.py
git commit -m "Add matched PostgreSQL source timing contract"
```

## Task 2: Eight-Arm Static Contract and Readiness Audit

**Files:**
- Create: `code/src/experiments/saor/native_system_matched.py`
- Create: `code/scripts/analysis/audit_saor_native_system_matched.py`
- Create: `code/tests/experiments/test_saor_native_system_matched.py`
- Create: `deploy/autodl/saor_native_system_matched.example.json`
- Modify: `deploy/autodl/README.md`

**Interfaces:**
- Produces: `SYSTEM_ARM_IDS`, `SELECTOR_SANITY_ARM_IDS`, `REQUIRED_ARM_IDS`.
- Produces: `MatchedSystemConfig`, `MatchedArm`, and `ScheduledMatchedCell` frozen dataclasses.
- Produces: `load_matched_system_config(path: Path) -> MatchedSystemConfig`.
- Produces: `balanced_matched_schedule(config, *, phase: str, repeat: int) -> tuple[ScheduledMatchedCell, ...]`.
- Produces: `audit_matched_system_config(config: MatchedSystemConfig) -> dict[str, object]`.

- [ ] **Step 1: Write failing arm-identity and two-table tests**

Pin the constants exactly:

```python
SYSTEM_ARM_IDS = (
    "daft_native", "daft_ray", "ray_data_http",
    "project_frozen_static", "project_bounded_ready_saor_0125we",
)
SELECTOR_SANITY_ARM_IDS = (
    "project_bounded_ready_fifo", "project_bounded_ready_drr",
    "project_bounded_ready_vtc_style",
    "project_bounded_ready_saor_0125we",
)
```

Assert eight unique arms and that SAOR is shared by both blocks.

- [ ] **Step 2: Write failing contract-rejection tests**

Use a complete temporary config and mutate one field per subtest. Reject:

- missing/duplicate/extra arms;
- native arm with Project K/W/credit/bounded-ready fields;
- native scheduler owner other than `daft` or `ray_data`;
- FIFO/DRR/VTC labelled native;
- offsets other than `[0, 5]` or arrival contract other than `eager`;
- a non-`none` performance writeback mode;
- manifest/SHA/endpoint/service/protocol/output-cap drift;
- Project K/W, ready bytes, actor topology, organizer, or source drift;
- SAOR policy other than `saor_bounded_ready`, non-bounded observation, or
  debt caps other than `[0.125, null]`;
- a reused output root.

- [ ] **Step 3: Run the new contract tests and verify red**

Run:

```powershell
python -m unittest discover -s code/tests/experiments -t code -p 'test_saor_native_system_matched.py'
```

Expected: import failure for the new module.

- [ ] **Step 4: Implement the pure config and schedule module**

Use `expand_structure` for environment expansion. Keep parsing/auditing pure
except for file existence/SHA checks. The schedule uses a seeded shuffle followed
by cyclic rotation so every repeat rotates arm positions. A physical SAOR arm
appears once per phase/repeat even though its evidence belongs to both report
blocks.

- [ ] **Step 5: Implement the read-only readiness CLI**

The CLI accepts:

```text
--config PATH --output PATH
```

It writes `schema_version`, `status`, `errors`, resolved arm identities, report
blocks, immutable manifest hashes, service signature, calibration paths, and the
planned schedule. It sends no model request and starts no Ray process. Exit 0 on
`passed`, 2 on `failed`.

- [ ] **Step 6: Add the environment-only example config**

The template must use `${DATABASE_URL}`, model/service variables, committed
manifest paths, native calibration variables, and frozen Project K/W variables.
It must contain no real host, credential, remote username, or runtime output.
Set `warmup_repeats=1`, `formal_repeats=3`, and
`selector_sanity_development_repeats=2`; readiness records that GPU formal is
not locally authorized.

- [ ] **Step 7: Run Task 2 tests**

Run:

```powershell
python -m unittest discover -s code/tests/experiments -t code -p 'test_saor_native_system_matched.py'
```

Expected: all tests pass, including missing-field and scheduler-owner cases.

- [ ] **Step 8: Commit Task 2**

```powershell
git add code/src/experiments/saor/native_system_matched.py code/scripts/analysis/audit_saor_native_system_matched.py code/tests/experiments/test_saor_native_system_matched.py deploy/autodl/saor_native_system_matched.example.json deploy/autodl/README.md
git commit -m "Add SAOR matched-system readiness contract"
```

## Task 3: Thin Balanced Single-Cell Orchestration

**Files:**
- Modify: `code/src/baselines/text/orchestration/native_multijob.py`
- Modify: `code/src/experiments/shared_vllm/runner.py`
- Modify: `code/src/experiments/saor/native_system_matched.py`
- Create: `code/scripts/experiments/run_saor_native_system_matched.py`
- Modify: `code/tests/baselines/text/test_native_multijob.py`
- Modify: `code/tests/experiments/test_saor_native_system_matched.py`

**Interfaces:**
- Produces: `run_native_multijob_cell(config, arm, identity, output_dir, ...) -> dict[str, object]`.
- Produces: `run_shared_vllm_group_cell(options, config, scenario, identity, ...) -> dict[str, object]`.
- Produces: `run_matched_system(config_path: Path, *, native_executor, project_executor, idle_gate, instrumenter) -> dict[str, object]`.

- [ ] **Step 1: Write failing single-cell extraction tests**

Assert one native arm/cell can run without its old internal schedule and retains
Job evidence, timed-source status, counters, resources, and provenance. Assert
one shared-vLLM scenario can run with an explicit `GroupRunIdentity` without
creating its own matrix schedule.

- [ ] **Step 2: Refactor public single-cell functions without changing old runners**

Extract the existing per-arm body from `run_native_multijob` and wrap the
existing `_run_group` body with `run_shared_vllm_group_cell`. Old entry points
must call these public functions and retain their current tests/output schema.
The single-cell functions do not acquire a host-wide lease; the outer matrix
owns it.

- [ ] **Step 3: Write failing global-order and failure-retention tests**

Inject fake native/Project executors. Assert:

- every scheduled cell is invoked exactly once in deterministic balanced order;
- SAOR is invoked once and tagged with both report blocks;
- idle gates run before and after each cell;
- output root existence is rejected;
- commands/evidence are atomically appended to `matrix_index.json`;
- the first exception writes a failed cell and stops later execution;
- host runner lease releases after success or failure;
- no native dispatch contains Project flags.

- [ ] **Step 4: Implement the thin matrix orchestrator**

The module owns only configuration, schedule, one outer lease, idle barriers,
dispatch, and evidence validation. Each single-cell executor retains its existing
resource/vLLM instrumentation; the outer matrix validates and indexes those
artifacts instead of starting a second sampler. Dispatch by `execution_owner` to
one of the two public single-cell APIs. Each completed record must include:

```text
arm_id, report_blocks, scheduler_owner, implementation_source,
phase, repeat, order_index, repository_commit,
start_epoch_s, end_epoch_s, database_operator_e2e_s,
jobs, service_metrics, resource_metrics, exactly_once,
request_tail_status, output_paths, status
```

Reject missing common-boundary, provenance, source, counter, resource, overlap,
or exactly-once evidence before marking a cell complete.

- [ ] **Step 5: Add the CLI wrapper**

The CLI accepts the existing config plus runner/profiler paths, health/metrics
URLs, Ray address, `--rehearsal`, `--resume`, and stale-lease recovery. In this
local batch, tests use injected fakes; do not execute the example against a
remote host.

- [ ] **Step 6: Run Task 3 tests**

Run:

```powershell
python -m unittest discover -s code/tests/baselines/text -t code -p 'test_native_multijob.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_saor_native_system_matched.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_shared_vllm_experiment.py'
```

Expected: existing runner tests and new cell-orchestrator tests pass.

- [ ] **Step 7: Commit Task 3**

```powershell
git add code/src/baselines/text/orchestration/native_multijob.py code/src/experiments/shared_vllm/runner.py code/src/experiments/saor/native_system_matched.py code/scripts/experiments/run_saor_native_system_matched.py code/tests/baselines/text/test_native_multijob.py code/tests/experiments/test_saor_native_system_matched.py
git commit -m "Add balanced SAOR system matrix orchestration"
```

## Task 4: Two-Layer Fail-Closed Summary, Documentation, and Verification

**Files:**
- Create: `code/scripts/analysis/summarize_saor_native_system_matched.py`
- Modify: `code/tests/experiments/test_saor_native_system_matched.py`
- Modify: `code/scripts/README.md`
- Modify: `code/README.md`
- Modify: `code/INFRA_STATUS.md`
- Modify: `learning/experiment_walkthrough.md`
- Modify: `experiments/plans/state_aware_work_unit_evaluation_20260808.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Produces: `summarize_matched_system(matrix_root: Path, output_dir: Path) -> bool`.
- Produces: `all_runs.csv`, `system_summary.csv`, `project_selector_sanity.csv`, `job_summary.csv`, `resource_summary.csv`, and `validation.json`.

- [ ] **Step 1: Write failing summary tests with synthetic evidence**

Build one complete small matrix fixture and assert:

- system table has exactly five arms;
- selector-sanity table has exactly four arms;
- the SAOR repeat values are identical in both outputs and originate from the
  same physical run IDs;
- service throughput is `(prompt_tokens_delta + generation_tokens_delta) /
  database_operator_e2e_s`;
- bulk/foreground JCT uses Job release-to-completion and overlap is positive;
- request tails for native arms are `unavailable` with a non-empty reason;
- repeats, sample CV, all single values, scheduler owner, and report role are
  preserved;
- no output column is named `winner` or `formal_authorized=true`.

- [ ] **Step 2: Write failing corruption tests**

Reject missing arm/repeat, duplicated run ID, failed or non-exact cell, empty
final queue, counter attribution failure, source timing outside the cell,
non-positive overlap, missing resource trace, Project K/W drift, native Project
flag contamination, and native request P99 populated from a Job barrier.

- [ ] **Step 3: Implement the offline summarizer**

Read only committed matrix evidence. Use `statistics.fmean` and `statistics.stdev`
for mean/sample CV when `n>=2`; write every repeat list as JSON. Set validation
fields exactly:

```json
{
  "status": "passed|failed",
  "comparison_scope": "complete_system_empirical_plus_project_internal_sanity",
  "selector_victory_decided": false,
  "formal_authorized": false,
  "native_baseline_count": 3,
  "project_control_count": 5
}
```

Do not calculate a cross-system request-tail rank if any required arm is
unavailable.

- [ ] **Step 4: Run summary and affected tests**

Run:

```powershell
python -m unittest discover -s code/tests/experiments -t code -p 'test_saor_native_system_matched.py'
python -m unittest discover -s code/tests/baselines/text -t code -p 'test_native_multijob.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_shared_vllm_experiment.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_saor_formal_tools.py'
python -m compileall -q code/src code/scripts code/tests
```

Expected: all selected tests pass and compileall exits 0.

- [ ] **Step 5: Update code, learning, plan, index, and log documentation**

Document both evidence layers, the full FIFO matched-control name, common eager
arrival, PostgreSQL timing, unsupported request-tail handling, local-only test
status, and the exact future run order: runtime preflight -> static readiness ->
small correctness/rehearsal -> review -> separately authorized formal. Do not
mark GPU evidence complete.

- [ ] **Step 6: Run full local discovery and record environmental exclusions**

Run:

```powershell
python -m unittest discover -s code/tests -t code -p 'test_*.py'
```

Expected: either full pass, or an exact list of dependency/platform failures.
Never report selected suites as a full pass.

- [ ] **Step 7: Verify format, secrets, and tracked status**

Run:

```powershell
git diff --check
python code/scripts/environment/scan_git_secrets.py --all
git status --short
```

Expected: no diff-format error, no secret violation, and only intended files.

- [ ] **Step 8: Commit and push the completed local infrastructure**

```powershell
git add code code_doc deploy experiments learning PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "Implement SAOR native-system matched comparison"
git push origin codex/saor-reservation
```

Before commit, confirm no external server host, username, password, runtime env,
raw workload, or unredacted command output is tracked.
