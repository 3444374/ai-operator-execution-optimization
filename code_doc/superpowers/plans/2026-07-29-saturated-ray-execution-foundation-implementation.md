# Saturated Ray Execution Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. The user selected inline execution
> without subagents.

**Goal:** Make formal runs single-writer safe, make Ray completion failures
release credit exactly once, calibrate a defensible per-endpoint saturation
point, and add fixed service quanta plus a bounded observable Ray HTTP actor
pool for the first saturated strategy comparison.

**Architecture:** Preserve planning batches as the data-organization unit, then
optionally slice them into complete-row service quanta before the existing typed
scheduler. Keep the first actor-pool implementation driver-owned and explicitly
bounded so it can be tested against the current path before adding the
endpoint-local async dispatcher. Use one atomic output-directory lease around
the scenario runner and convert Ray `get` exceptions into typed failed
completions so the existing scheduler cleanup path always runs.

**Local command convention:** Run tests from the repository root with
`.conda\pg-ai-profile\python.exe`. On the managed Windows workspace, the full
suite needs an unsandboxed invocation because `TemporaryDirectory` cannot create
writable children under the sandbox DACL. Normal developer and remote Linux
environments do not need this exception.

**Tech Stack:** Python 3.10/3.12, unittest/pytest, PyArrow, Ray 2.56.1, Daft
0.7.21, JSON AutoDL scenario templates, Ruff.

## Global Constraints

- Do not modify vLLM, Ray's cluster scheduler, or GPU kernels.
- Ray HTTP worker actors keep `num_gpus=0`; vLLM endpoint processes own GPUs.
- A planning or service policy must never split one prompt row.
- Every production behavior change starts with a focused failing test.
- Automatic retry remains disabled for ambiguous completion requests.
- Existing `batch_rows_*` fields retain submission-payload semantics;
  organization and service-quantum metrics remain separate.
- Formal comparisons hold per-endpoint active work and total actor slots fixed.
- No strategy claim is made until the extended active-work curve selects
  saturation or explicitly reports that saturation was not reached.
- Commit messages contain no `Co-Authored-By` or AI attribution.

## Stage Boundary

This plan intentionally stops at the bounded driver-owned actor pool. The
approved design requires that version to pass correctness, queue visibility,
and remote performance gates before replacing driver replenishment with an
endpoint-local async dispatcher. A second implementation plan will cover that
dispatcher only after the first remote results establish the remaining
driver/Ray gap.

---

### Task 1: Add an exclusive scenario-runner lease

**Files:**
- Create: `code/src/runner_lease.py`
- Create: `code/tests/test_runner_lease.py`
- Modify: `code/scripts/run_ai_operator_scenarios.py`
- Modify: `code/tests/test_experiment_scenarios.py`

**Interfaces:**
- Produces:
  `acquire_runner_lease(output_dir: Path, *, config_fingerprint: str,
  repository_commit: str, recover_stale: bool = False,
  owner: RunnerOwner | None = None,
  process_alive: Callable[[int], bool] = is_process_alive) -> RunnerLease`.
- Produces: `RunnerLease.recovered_owner: dict[str, object] | None`.
- Produces: CLI flag `--recover-stale-lease`, valid only with `--resume`.
- Consumes: a SHA-256 fingerprint of experiment ID, seed, redacted config, and
  schedule; config fingerprint mismatches are never recovered in place.

- [ ] **Step 1: Write lease collision and stale-recovery tests**

Create `code/tests/test_runner_lease.py` with:

```python
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.runner_lease import RunnerOwner, acquire_runner_lease


class RunnerLeaseTests(unittest.TestCase):
    def test_live_owner_rejects_second_runner(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            lease = acquire_runner_lease(
                output,
                config_fingerprint="cfg-a",
                repository_commit="abc123",
                owner=RunnerOwner("host-a", 11, "start-a", "owner-a"),
                process_alive=lambda pid: pid == 11,
            )
            self.addCleanup(lease.release)
            with self.assertRaisesRegex(RuntimeError, "active runner"):
                acquire_runner_lease(
                    output,
                    config_fingerprint="cfg-a",
                    repository_commit="abc123",
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda pid: pid == 11,
                )

    def test_stale_owner_requires_explicit_recovery(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            stale = {
                "hostname": "host-a",
                "pid": 11,
                "process_start_id": "start-a",
                "owner_token": "owner-a",
                "config_fingerprint": "cfg-a",
                "repository_commit": "abc123",
                "started_epoch_s": 1.0,
            }
            (output / ".runner-lease.json").write_text(
                json.dumps(stale),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "stale runner lease"):
                acquire_runner_lease(
                    output,
                    config_fingerprint="cfg-a",
                    repository_commit="abc123",
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda _pid: False,
                )
            recovered = acquire_runner_lease(
                output,
                config_fingerprint="cfg-a",
                repository_commit="abc123",
                recover_stale=True,
                owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                process_alive=lambda _pid: False,
            )
            self.addCleanup(recovered.release)
            self.assertEqual(recovered.recovered_owner["owner_token"], "owner-a")

    def test_stale_recovery_rejects_config_mismatch(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            stale = {
                "hostname": "host-a",
                "pid": 11,
                "process_start_id": "start-a",
                "owner_token": "owner-a",
                "config_fingerprint": "cfg-a",
                "repository_commit": "abc123",
                "started_epoch_s": 1.0,
            }
            (output / ".runner-lease.json").write_text(
                json.dumps(stale),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "fingerprint"):
                acquire_runner_lease(
                    output,
                    config_fingerprint="cfg-b",
                    repository_commit="def456",
                    recover_stale=True,
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda _pid: False,
                )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the lease tests and verify RED**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_runner_lease -v
```

Expected: import failure because `src.runner_lease` does not exist.

- [ ] **Step 3: Implement the atomic lease**

Create `code/src/runner_lease.py` with a frozen `RunnerOwner`, a `RunnerLease`
context manager, and these behaviors:

```python
LEASE_NAME = ".runner-lease.json"


@dataclass(frozen=True)
class RunnerOwner:
    hostname: str
    pid: int
    process_start_id: str
    owner_token: str


class RunnerLease:
    def __init__(self, path, owner, recovered_owner=None):
        self.path = path
        self.owner = owner
        self.recovered_owner = recovered_owner
        self._released = False

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        self.release()

    def release(self):
        if self._released or not self.path.exists():
            return
        current = json.loads(self.path.read_text(encoding="utf-8"))
        if current.get("owner_token") != self.owner.owner_token:
            raise RuntimeError("runner lease ownership changed before release")
        self.path.unlink()
        self._released = True
```

`acquire_runner_lease` creates the directory, serializes
host/PID/start identity/token/fingerprint/commit/start time, and performs the
first write with:

```python
flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
descriptor = os.open(lease_path, flags, 0o600)
```

On collision it parses strictly, rejects a fingerprint mismatch, treats another
host conservatively as active, checks same-host PID liveness, and requires
`recover_stale=True` before atomically replacing a stale lease. Use
`/proc/<pid>/stat` field 22 as the process start identity on Linux and
`os.kill(pid, 0)` for the dependency-free liveness check.

- [ ] **Step 4: Run the lease tests and verify GREEN**

Run the command from Step 2. Expected: all three tests pass.

- [ ] **Step 5: Add failing runner integration tests**

In `test_experiment_scenarios.py` add:

```python
def test_parse_recover_stale_lease_requires_resume(self) -> None:
    with self.assertRaises(SystemExit):
        parse_args([
            "--config", "config.json",
            "--profiler", "profile.py",
            "--python-executable", sys.executable,
            "--output-dir", "output",
            "--health-url", "http://health",
            "--metrics-urls", "http://metrics",
            "--recover-stale-lease",
        ])

def test_runner_releases_lease_after_success(self) -> None:
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        exit_code = run_experiment(
            RunnerOptions(
                config_path=self._write_config(
                    root, scenario_ids=["fixed"], formal_repeats=1, seed=7
                ),
                profiler_path=self._write_fake_profiler(root),
                python_executable=Path(sys.executable),
                output_dir=root / "output",
                health_url="http://health",
                metrics_urls=("http://metrics",),
                idle_timeout_s=1.0,
            ),
            idle_gate=lambda _health, _metrics, _timeout: None,
        )
        self.assertEqual(exit_code, 0)
        self.assertFalse((root / "output" / ".runner-lease.json").exists())
```

- [ ] **Step 6: Integrate the lease around manifest and CSV writes**

Add `recover_stale_lease: bool = False` to `RunnerOptions`, the CLI flag, and
the `--resume` validation. Compute the fingerprint:

```python
fingerprint_payload = {
    "experiment_id": config.experiment_id,
    "seed": config.seed,
    "redacted_config": _redacted_config(config),
    "schedule": [asdict(item) for item in schedule],
}
fingerprint = hashlib.sha256(
    json.dumps(
        fingerprint_payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()
```

Acquire before any manifest/CSV read or write. Extract the current execution
loop into `_run_experiment_locked(...)` so `run_experiment` owns one concise
`with lease:` boundary. If `recovered_owner` is present, append a manifest
incident with reason `stale_runner_lease_recovered`, the old owner, and
`recovered=True`.

Resolve the repository commit without invoking a shell:

```python
repository_commit = subprocess.run(
    ["git", "rev-parse", "HEAD"],
    cwd=CODE_ROOT.parent,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()
```

Fail before acquiring the lease if this command does not produce a non-empty
commit, so the formal owner record can never silently contain `unknown`.

- [ ] **Step 7: Run runner tests and verify GREEN**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_runner_lease tests.test_experiment_scenarios -v
```

Expected: all tests pass, including existing resume behavior.

- [ ] **Step 8: Commit**

```powershell
git add code/src/runner_lease.py code/tests/test_runner_lease.py `
  code/scripts/run_ai_operator_scenarios.py `
  code/tests/test_experiment_scenarios.py
git commit -m "fix: enforce one scenario runner per output"
```

---

### Task 2: Convert Ray result failures into typed completions

**Files:**
- Modify: `code/src/scheduling/runtime/ray_adapter.py`
- Modify: `code/tests/test_ray_adapter.py`
- Modify: `code/tests/test_scheduler.py`

**Interfaces:**
- Preserves:
  `RaySubmissionAdapter.wait_one(pending) -> CollectedSubmission`.
- Produces: a failed `SubmissionCompletion` for `ray.get` errors, retaining the
  canonical pending handle and request ID.
- Consumes: the scheduler's existing failed-completion cleanup path; no retry.

- [ ] **Step 1: Write a failing Ray adapter exception test**

```python
def test_collect_converts_ray_get_error_to_failed_completion(self) -> None:
    class FailingRay(FakeRay):
        @staticmethod
        def get(_ref):
            raise RuntimeError("worker crashed")

    item = envelope()
    handle = FakeRef(None)
    collected = RaySubmissionAdapter(FailingRay, {}).wait_one([(handle, item)])
    self.assertIs(collected.handle, handle)
    self.assertEqual(collected.completion.request_id, "r1")
    self.assertEqual(collected.completion.status, "failed")
    self.assertIn("RuntimeError: worker crashed", collected.completion.error)
```

- [ ] **Step 2: Run the test and verify RED**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_ray_adapter.RaySubmissionAdapterTests.test_collect_converts_ray_get_error_to_failed_completion -v
```

Expected: the runtime error escapes `wait_one`.

- [ ] **Step 3: Implement exception conversion**

Wrap only `ray_module.get`:

```python
result_start = time.perf_counter()
try:
    result = self.ray_module.get(ready_handle)
except Exception as exc:
    completion = SubmissionCompletion(
        matched_envelope.request.request_id,
        "failed",
        error=f"{type(exc).__name__}: {exc}",
    )
else:
    completion = SubmissionCompletion(
        matched_envelope.request.request_id,
        "completed",
        result=result,
    )
result_s = time.perf_counter() - result_start
return CollectedSubmission(handle, completion, wait_s, result_s)
```

- [ ] **Step 4: Prove failed completions release shared credit**

Add a scheduler test using `FailingAdapter` and a recording shared-credit
implementation. Assert one acquire, one release, one failed completion, and one
failed lifecycle event. Do not change scheduler production code unless this
test exposes a separate cleanup defect.

- [ ] **Step 5: Run and commit**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_ray_adapter tests.test_scheduler -v
cd ..
git add code/src/scheduling/runtime/ray_adapter.py `
  code/tests/test_ray_adapter.py code/tests/test_scheduler.py
git commit -m "fix: release ray credit after worker failure"
```

Expected: all adapter and scheduler tests pass.

---

### Task 3: Extend and pre-register the saturation curve

**Files:**
- Modify: `deploy/autodl/dual_gpu_active_work_curve.example.json`
- Modify: `code/tests/test_experiment_scenarios.py`
- Modify: `deploy/autodl/README.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Produces eight work arms:
  `16384, 24576, 32768, 49152, 65536, 81920, 98304, 131072`.
- Preserves request submission, fixed planning policy, fixed pool shape, fixed
  seeds, one warm-up, and three formal repeats.
- Uses the pre-registered 97%/3% saturation selection rule.

- [ ] **Step 1: Make the template test expect eight arms**

Change the active-work template count from five to eight and assert:

```python
self.assertEqual(
    [item.scenario_id for item in active_work_curve.scenarios],
    [
        "work16384", "work24576", "work32768", "work49152",
        "work65536", "work81920", "work98304", "work131072",
    ],
)
```

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_experiment_scenarios.ExperimentScenarioTests.test_committed_dual_gpu_templates_expand_and_validate -v
```

Expected: five committed arms do not match eight expected arms.

- [ ] **Step 3: Add the higher-work scenarios**

Append scenarios with exact work values `81920`, `98304`, and `131072`. Do not
change common arguments.

- [ ] **Step 4: Update runbook and log**

Record lease/process inspection, explicit stale recovery, OOM/failure/timeout
stop conditions, the 97%/3% rule, and the required
`saturation_not_reached` outcome when no point qualifies.

- [ ] **Step 5: Run and commit**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_experiment_scenarios -v
cd ..
git add deploy/autodl/dual_gpu_active_work_curve.example.json `
  deploy/autodl/README.md code/tests/test_experiment_scenarios.py PROJECT_LOG.md
git commit -m "test: extend active work saturation curve"
```

Expected: scenario tests pass.

- [ ] **Step 6: Remote checkpoint A**

After Tasks 1–3 pass full local verification:

1. push local `main`;
2. inspect the exact runner lease/process and do not touch the contaminated
   fixed-work output;
3. pull remote `main`;
4. recover PostgreSQL and both vLLM endpoints with the AutoDL runbook;
5. run one 64-row smoke into a new output directory;
6. launch the eight-arm curve into a new dated directory;
7. record commit, config fingerprint, lease owner, endpoint flags, and logs.

This checkpoint may run while Tasks 4–6 are implemented locally. The remote
checkout must not pull later commits until its runner reaches a terminal state.

---

### Task 4: Add a pure complete-row service-quantum slicer

**Files:**
- Create: `code/src/scheduling/organization/service_quantum.py`
- Create: `code/tests/test_service_quantum.py`
- Modify: `code/src/scheduling/__init__.py`

**Interfaces:**
- Produces:
  `slice_service_quanta(row_costs: Sequence[int], target_tokens: int)
  -> tuple[ServiceQuantumSlice, ...]`.
- Does not import PyArrow, Daft, Ray, or HTTP.

- [ ] **Step 1: Write deterministic slicing tests**

```python
class ServiceQuantumTests(unittest.TestCase):
    def test_slices_rows_without_splitting_oversized_row(self) -> None:
        self.assertEqual(
            slice_service_quanta([6, 4, 7, 20, 3], 10),
            (
                ServiceQuantumSlice(0, 2, 10, False),
                ServiceQuantumSlice(2, 3, 7, False),
                ServiceQuantumSlice(3, 4, 20, True),
                ServiceQuantumSlice(4, 5, 3, False),
            ),
        )

    def test_empty_costs_produce_no_quanta(self) -> None:
        self.assertEqual(slice_service_quanta([], 10), ())

    def test_rejects_invalid_cost_or_target(self) -> None:
        with self.assertRaisesRegex(ValueError, "target_tokens"):
            slice_service_quanta([1], 0)
        with self.assertRaisesRegex(ValueError, "row costs"):
            slice_service_quanta([1, -1], 10)
```

- [ ] **Step 2: Run tests and verify RED**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_service_quantum -v
```

Expected: module import failure.

- [ ] **Step 3: Implement ordered accumulation**

```python
@dataclass(frozen=True)
class ServiceQuantumSlice:
    start: int
    stop: int
    estimated_work: int
    oversized: bool

    @property
    def row_count(self) -> int:
        return self.stop - self.start


def slice_service_quanta(row_costs, target_tokens):
    if (
        isinstance(target_tokens, bool)
        or not isinstance(target_tokens, int)
        or target_tokens <= 0
    ):
        raise ValueError("target_tokens must be a positive integer")
    costs = tuple(row_costs)
    if any(
        isinstance(cost, bool) or not isinstance(cost, int) or cost < 0
        for cost in costs
    ):
        raise ValueError("row costs must be non-negative integers")
    slices = []
    start = 0
    work = 0
    for index, cost in enumerate(costs):
        if index > start and work + cost > target_tokens:
            slices.append(ServiceQuantumSlice(start, index, work, False))
            start = index
            work = 0
        if cost > target_tokens:
            if work:
                slices.append(ServiceQuantumSlice(start, index, work, False))
            slices.append(ServiceQuantumSlice(index, index + 1, cost, True))
            start = index + 1
            work = 0
        else:
            work += cost
    if start < len(costs):
        slices.append(ServiceQuantumSlice(start, len(costs), work, False))
    return tuple(slices)
```

Export both names through `scheduling.__init__`.

- [ ] **Step 4: Run and commit**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_service_quantum -v
cd ..
git add code/src/scheduling/organization/service_quantum.py `
  code/src/scheduling/__init__.py code/tests/test_service_quantum.py
git commit -m "feat: slice planning batches into service quanta"
```

Expected: all slicer tests pass.

---

### Task 5: Wire fixed service quanta into offline and replay execution

**Files:**
- Modify: `code/src/scheduling/models.py`
- Modify: `code/src/profiling/cli.py`
- Modify: `code/src/profiling/replay.py`
- Modify: `code/src/profile_replay.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/src/profiling/schema.py`
- Modify: `code/src/profiling/traces.py`
- Modify: `code/tests/test_scheduling_models.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`
- Modify: `code/tests/test_request_lifecycle.py`

**Interfaces:**
- Adds backwards-compatible default `planning_batch_id`,
  `service_quantum_index`, and `service_quantum_oversized` fields to
  `BatchRequest`.
- Adds `--submission-granularity service_quantum` and positive
  `--service-quantum-tokens`.
- Produces separate quantum count/rows/work/oversized summary fields.
- Submission trace schema 4 records planning-batch and quantum identity.

- [ ] **Step 1: Add failing CLI and model tests**

Construct a `BatchRequest` with explicit planning/quantum fields and assert
them. Parse:

```python
args = profile.parse_args([
    "--dry-run",
    "--submission-granularity", "service_quantum",
    "--service-quantum-tokens", "4096",
])
self.assertEqual(args.service_quantum_tokens, 4096)
```

Also test zero target rejection and non-zero targets under batch/request modes.

- [ ] **Step 2: Add failing offline and replay membership tests**

For predicted row costs `[6, 4, 7]` and target 10, assert two submission IDs,
row counts `[2, 1]`, lifecycle membership `[q0, q0, q1]`, unchanged planning
metrics `[(17, 3)]`, and quantum metrics
`[(10, 2, False), (7, 1, False)]`.

- [ ] **Step 3: Run focused tests and verify RED**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_scheduling_models `
  tests.test_postgres_profile_scheduling `
  tests.test_request_lifecycle -v
```

Expected: new CLI choice, model fields, and expansion are absent.

- [ ] **Step 4: Implement one shared Arrow expansion helper**

Add `_service_quantum_envelopes(...)` in `profiling/replay.py`. It computes each
row's prompt plus estimated-output work, calls `slice_service_quanta`, uses
`batch.slice(start, row_count)`, and assigns IDs:

```python
quantum_id = f"{planning_batch_id}:quantum:{quantum_index}"
```

Refactor `_batch_envelopes` only enough to accept explicit request IDs and
default planning metadata. Both offline and arrival replay use this helper and
map lifecycle seeds to the corresponding quantum. Increment planning-batch
indexes by original organized batches, not expanded quanta.

- [ ] **Step 5: Wire profiler validation and summary**

Enforce:

```python
if args.submission_granularity == "service_quantum":
    if args.service_quantum_tokens <= 0:
        raise SystemExit(
            "--service-quantum-tokens must be positive for service_quantum"
        )
elif args.service_quantum_tokens != 0:
    raise SystemExit(
        "--service-quantum-tokens requires service_quantum granularity"
    )
```

Use a separate quantum metric sink. Add ordered fields
`service_quantum_tokens`, `service_quantum_count`,
`service_quantum_rows_mean`, `service_quantum_work_mean`,
`service_quantum_work_p95`, and `service_quantum_oversized_rows` to dry-run and
formal rows.

- [ ] **Step 6: Upgrade submission traces**

Submission trace schema 4 writes planning batch ID, quantum index, oversized
marker, credit-held duration, and:

```python
"ray_to_service_s": max(
    0.0,
    float(result.get("service_start_epoch_s", 0.0))
    - event.submit_epoch_s,
),
```

Request lifecycle keeps `latency_granularity="submission"` for multi-row
quanta and `"request"` only for request mode.

- [ ] **Step 7: Run and commit**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_scheduling_models `
  tests.test_postgres_profile_scheduling `
  tests.test_request_lifecycle -v
cd ..
git add code/src/scheduling/models.py code/src/profiling `
  code/src/profile_replay.py code/scripts/postgres_ai_operator_profile.py `
  code/tests/test_scheduling_models.py `
  code/tests/test_postgres_profile_scheduling.py `
  code/tests/test_request_lifecycle.py
git commit -m "feat: execute fixed service quanta"
```

Expected: focused tests pass and batch/request defaults stay unchanged.

---

### Task 6: Bound and observe the driver-owned Ray actor pool

**Files:**
- Modify: `code/src/scheduling/models.py`
- Modify: `code/src/scheduling/runtime/ray_adapter.py`
- Modify: `code/src/profiling/ray.py`
- Modify: `code/src/profiling/cli.py`
- Modify: `code/src/profiling/schema.py`
- Modify: `code/src/profiling/traces.py`
- Modify: `code/src/model_backends.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/tests/test_ray_adapter.py`
- Modify: `code/tests/test_model_backends.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- `ActorWorkerPoolSubmitter(..., max_concurrency_per_worker: int,
  routing_policy: Literal["round_robin", "least_active_work"],
  endpoint_id: str)`.
- Adds `submit(payload, *, estimated_work)`, `complete(handle, *, failed)`, and
  immutable worker snapshots.
- Adds CLI `--actor-worker-routing`, default `round_robin`.
- Effective per-endpoint submission limit cannot exceed pool slots.

- [ ] **Step 1: Write failing capacity and routing tests**

Test that a one-worker/two-slot pool rejects a third submission until one
canonical handle completes, round-robin fills two one-slot workers, and
least-active-work reuses the worker with lower active work. Example:

```python
submitter = ActorWorkerPoolSubmitter(
    [RecordingActor(), RecordingActor()],
    "complete",
    endpoint_id="endpoint-0",
    max_concurrency_per_worker=1,
    routing_policy="least_active_work",
)
first = submitter.submit("large", estimated_work=100)
second = submitter.submit("small", estimated_work=10)
submitter.complete(second, failed=False)
third = submitter.submit("next", estimated_work=5)
self.assertEqual(submitter.assignment(third).worker_index, 1)
```

Also prove failed completion releases one slot and equal-but-distinct ready refs
release the canonical handle's assignment.

- [ ] **Step 2: Run adapter tests and verify RED**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest tests.test_ray_adapter -v
```

Expected: constructor and state interfaces are absent.

- [ ] **Step 3: Implement explicit worker assignments**

Add frozen `ActorWorkerAssignment` and `ActorWorkerSnapshot` models. The
submitter chooses only workers below `max_concurrency_per_worker`, records
assignments by canonical handle identity, and updates running, active work,
submitted, completed, failed, maxima, and slot-held time. Round-robin scans for
a free worker. Least-active-work minimizes:

```python
(active_work / max_concurrency_per_worker, running, worker_index)
```

- [ ] **Step 4: Make the adapter own completion callbacks**

`RaySubmissionAdapter.submit` calls the stateful submitter with
`estimated_work`. `wait_one` gets assignment metadata before collection and
calls `complete(canonical_handle, failed=...)` in `finally`. Extend
`CollectedSubmission` and `SubmissionLifecycleEvent` with default worker
ID/index and copy them into the event, preserving generic task submitters.

- [ ] **Step 5: Add worker PID without changing transport**

Add `"actor_worker_pid": os.getpid()` to result dictionaries returned by the
five actor classes. Do not add persistent HTTP connections in this task.

- [ ] **Step 6: Wire capacity, metrics, and traces**

Set:

```python
pool_slots = actor_workers_per_endpoint * args.ray_actor_max_concurrency
effective_endpoint_limit = (
    min(per_endpoint_inflight_limit, pool_slots)
    if per_endpoint_inflight_limit is not None
    else pool_slots
)
```

Add formal fields for worker routing, per-endpoint slot capacity, per-worker max
running/work, failures, and slot-held utilization. Submission trace schema 4
also writes worker ID/index/PID. Name the utilization “slot-held” because it
includes Ray/HTTP time, not GPU compute utilization.

- [ ] **Step 7: Run and commit**

```powershell
cd code
..\.conda\pg-ai-profile\python.exe -m unittest `
  tests.test_ray_adapter tests.test_model_backends `
  tests.test_postgres_profile_scheduling -v
cd ..
git add code/src/scheduling code/src/profiling code/src/model_backends.py `
  code/scripts/postgres_ai_operator_profile.py `
  code/tests/test_ray_adapter.py code/tests/test_model_backends.py `
  code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: bound and observe ray actor workers"
```

Expected: focused tests pass and legacy callable submitters remain compatible.

---

### Task 7: Verify, document, push, and run remote strategy gates

**Files:**
- Modify: `code/README.md`
- Modify: `code/INFRA_STATUS.md`
- Modify: `code/scripts/README.md`
- Modify: `deploy/autodl/README.md`
- Create: `deploy/autodl/dual_gpu_actor_pool_shape.example.json`
- Create: `deploy/autodl/dual_gpu_service_quantum.example.json`
- Modify: `code/tests/test_experiment_scenarios.py`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `PROJECT_LOG.md`
- Modify: the scheduling walkthrough selected by `learning/README.md`

**Interfaces:**
- Pool-shape arms: `1x256`, `2x128`, `4x64`, fixed 256 slots per endpoint.
- Quantum arms: whole planning batch, fixed candidates, one-row diagnostic.
- Uses new output directories; never resumes the contaminated fixed-work curve.

- [ ] **Step 1: Add failing committed-template tests**

Load both new JSON files. Assert
`workers * actor_max_concurrency == 256` for every pool arm. Assert quantum arms
keep the same active-work reference and change only submission
granularity/target.

- [ ] **Step 2: Run the template test and verify RED**

Expected: both template files are absent.

- [ ] **Step 3: Create isolated templates**

The pool template uses the saturated request baseline, round-robin, and fixed
slots. The original 16-slot draft was rejected before execution: current
evidence has about 332 work units per request and 1337 per organization batch,
so 16 slots would cap visible work at roughly 5.3K or 21K per endpoint, below
the active-work saturation range. Keeping 256 slots preserves the Checkpoint A
load envelope while changing only actor topology.

The quantum template uses the selected pool/work/planning budget and arms:

```text
batch
service_quantum_512
service_quantum_1024
service_quantum_2048
service_quantum_4096
request_diagnostic
```

The 8192 draft arm is removed because Checkpoint A reports organization-batch
cost p95 about 3366 and maximum 5892: 8192 would not split any observed batch
and would silently duplicate the batch control. Remove any remaining quantum
larger than the selected planning budget or observed maximum before formal
launch and record why; never silently convert it into the control.

- [ ] **Step 4: Run full local verification**

```powershell
.\.conda\pg-ai-profile\python.exe -m pytest code/tests -q -p no:cacheprovider
.\.conda\pg-ai-profile\python.exe -m ruff check code
git diff --check
```

Expected: all tests pass, Ruff exits zero, and no whitespace errors.

- [ ] **Step 5: Synchronize layered documentation**

Document planning batch versus service quantum versus vLLM batching, actor
worker versus endpoint/GPU ownership, lease recovery, new metrics, local test
count, remote checkpoint A, and exact remote commands. Update learning material
as required by `code/AGENTS.md` and register new files in the project index.

- [ ] **Step 6: Commit and push `main`**

```powershell
git add code deploy/autodl PROJECT_INDEX.md PROJECT_OUTLINE.md `
  PROJECT_LOG.md learning
git commit -m "docs: register saturated ray execution gates"
git push origin main
```

Expected: push succeeds; a running checkpoint A checkout is not updated mid-run.

- [ ] **Step 7: Remote checkpoint B correctness gate**

After checkpoint A reaches a terminal manifest, pull `main`, run focused tests
and Ruff, recover services, and run one 64-row batch, one quantum, and every
pool-shape smoke. Require zero failures, exact row coverage, complete traces,
actor identities, correct slot counts, and lease cleanup.

- [ ] **Step 8: Run formal matrices sequentially**

Select saturation; verify total visible slots; compare pool shape; compare
whole batch/fixed quanta/request diagnostic; run least-active-work only when
imbalance is visible. Analyze repeat distributions for tokens/s, P99, SLO
goodput, MFU, Ray-to-service delay, credit-held time, slot utilization, and
failures.

- [ ] **Step 9: Close the evidence loop**

Download summaries and compact trace evidence. Update the seven-step result
report and infra strategy ledger. If driver replenishment remains material,
write the endpoint-local dispatcher plan. If no stable benefit remains after
saturation, retain the simple baseline and report the negative result.
