# Ray Static Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the typed scheduling core into the existing static Ray task and Ray actor submission paths while preserving current result and timing metric semantics.

**Architecture:** Add an engine-specific `RaySubmissionAdapter` whose endpoint submitters hide task/actor call signatures from the policy core. Extend typed scheduler collection results with wait/get timings, then make the existing profiler functions delegate only their static path to the scheduler. The existing adaptive branch remains unchanged until the adaptive-controller plan.

**Tech Stack:** Python 3.11+, Ray, Daft/Arrow payloads, standard-library dataclasses/protocols/unittest, existing `postgres_ai_operator_profile.py`.

## Global Constraints

- Formal path stays `PostgreSQL -> Daft -> Arrow -> Ray task/actor -> endpoint`.
- Strategy modules do not import Daft, Arrow, Ray, or HTTP.
- `ray_adapter.py` may depend on the injected Ray module but must not contain scheduling policy.
- Preserve current function signatures and CSV metric keys.
- Static behavior uses typed scheduler; legacy adaptive behavior remains isolated and unchanged.
- Tests precede production changes and must demonstrate RED for each new behavior.
- No queue-adaptive flush, AIMD, EWMA, PID, UCB, pool routing, or joint search in this plan.
- No AI attribution in commits.

---

## File Map

- Modify `code/src/scheduling/models.py`: typed collected-submission timing.
- Modify `code/src/scheduling/scheduler.py`: aggregate static submission metrics.
- Modify `code/tests/test_scheduler.py`: metric and completion-identity tests.
- Create `code/src/scheduling/ray_adapter.py`: generic Ray ObjectRef adapter.
- Create `code/tests/test_ray_adapter.py`: fake-Ray adapter unit tests.
- Modify `code/tests/test_scheduling_daft_ray_contract.py`: use production adapter.
- Modify `code/scripts/postgres_ai_operator_profile.py`: static task/actor delegation.
- Create `code/tests/test_postgres_profile_scheduling.py`: static path parity tests.
- Modify `code/README.md`, `code/scripts/README.md`,
  `learning/local_vllm_ray_baseline_walkthrough.md`, `PROJECT_INDEX.md`, and
  `PROJECT_LOG.md`.

### Task 1: Typed Collection Timings

**Files:**
- Modify: `code/src/scheduling/models.py`
- Modify: `code/src/scheduling/scheduler.py`
- Modify: `code/src/scheduling/__init__.py`
- Modify: `code/tests/test_scheduler.py`

**Interfaces:**
- Produce `CollectedSubmission(handle, completion, wait_s, result_s)`.
- Extend `SchedulerResult` with `operator_invocations`, `bounded_wait_s`,
  `avg_bounded_wait_s`, `fanin_s`, `submit_s`, and `applied_limit`.
- Change `SubmissionAdapter.wait_one(...) -> CollectedSubmission`.

- [ ] **Step 1: Add failing scheduler metric tests**

Update the fake adapter:

```python
from src.scheduling.models import CollectedSubmission

class FakeSubmissionAdapter:
    ...
    def wait_one(self, pending):
        handle, pending_envelope = pending[0]
        return CollectedSubmission(
            handle=handle,
            completion=SubmissionCompletion(
                request_id=pending_envelope.request.request_id,
                status="completed",
                result=pending_envelope.payload,
            ),
            wait_s=0.1,
            result_s=0.05,
        )
```

Add assertions to the five-request, limit-two test:

```python
self.assertEqual(result.operator_invocations, 5)
self.assertAlmostEqual(result.bounded_wait_s, 0.3)
self.assertAlmostEqual(result.avg_bounded_wait_s, 0.1)
self.assertAlmostEqual(result.fanin_s, 0.25)
self.assertGreaterEqual(result.submit_s, 0.0)
```

Add a completion-identity test:

```python
def test_scheduler_rejects_completion_for_different_request(self) -> None:
    class WrongCompletionAdapter(FakeSubmissionAdapter):
        def wait_one(self, pending):
            collected = super().wait_one(pending)
            return CollectedSubmission(
                collected.handle,
                SubmissionCompletion("wrong", "completed"),
                collected.wait_s,
                collected.result_s,
            )

    scheduler = SynchronousScheduler(
        StaticAdmissionController(1),
        RoundRobinEndpointRouter(),
        WrongCompletionAdapter(),
        "default",
    )

    with self.assertRaisesRegex(RuntimeError, "completion request_id"):
        scheduler.run([envelope(0)], topology())
```

- [ ] **Step 2: Run RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_scheduler.py
```

Expected: import failure for `CollectedSubmission`.

- [ ] **Step 3: Add collection types and aggregation**

Add to `models.py`:

```python
@dataclass(frozen=True)
class CollectedSubmission:
    handle: object
    completion: SubmissionCompletion
    wait_s: float
    result_s: float

    def __post_init__(self) -> None:
        if self.wait_s < 0 or self.result_s < 0:
            raise ValueError("collection timings must be non-negative")
```

Extend `SchedulerResult`:

```python
@dataclass(frozen=True)
class SchedulerResult:
    completions: tuple[SubmissionCompletion, ...]
    operator_invocations: int
    max_inflight_seen: int
    applied_limit: int
    bounded_wait_s: float
    avg_bounded_wait_s: float
    fanin_s: float
    submit_s: float
```

In `run`, measure `adapter.submit` with `time.perf_counter()`. Make
`_collect_one` return `CollectedSubmission`; count its `wait_s` only when the
collection was triggered by admission blocking, and count every `result_s` in
`fanin_s`. Validate:

```python
if collected.completion.request_id != pending_envelope.request.request_id:
    raise RuntimeError("completion request_id does not match pending request")
```

- [ ] **Step 4: Run GREEN**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_scheduler.py
```

Expected: `Ran 3 tests ... OK`.

- [ ] **Step 5: Commit**

```powershell
git add code/src/scheduling code/tests/test_scheduler.py
git commit -m "feat: collect typed scheduler metrics"
```

### Task 2: Generic Ray Submission Adapter

**Files:**
- Create: `code/src/scheduling/ray_adapter.py`
- Create: `code/tests/test_ray_adapter.py`
- Modify: `code/src/scheduling/__init__.py`
- Modify: `code/tests/test_scheduling_daft_ray_contract.py`

**Interfaces:**
- `RaySubmissionAdapter(ray_module, submitters)`.
- `submitters: Mapping[str, Callable[[object], object]]`.
- `submit(envelope, endpoint_id) -> ObjectRef-like handle`.
- `wait_one(pending) -> CollectedSubmission`.

- [ ] **Step 1: Write failing adapter tests**

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.models import BatchRequest, PayloadEnvelope
from src.scheduling.ray_adapter import RaySubmissionAdapter


class FakeRef:
    def __init__(self, value):
        self.value = value


class FakeRay:
    @staticmethod
    def wait(refs, num_returns):
        return refs[:num_returns], refs[num_returns:]

    @staticmethod
    def get(ref):
        return ref.value


def envelope() -> PayloadEnvelope:
    return PayloadEnvelope(
        BatchRequest("r1", "j1", "ai_complete", 1, 2, 3, "", 0.0, 0.0, "p1"),
        "payload",
    )


class RaySubmissionAdapterTests(unittest.TestCase):
    def test_submit_and_collect_preserve_request_identity(self) -> None:
        adapter = RaySubmissionAdapter(
            FakeRay,
            {"e1": lambda payload: FakeRef({"payload": payload})},
        )
        item = envelope()

        handle = adapter.submit(item, "e1")
        collected = adapter.wait_one([(handle, item)])

        self.assertEqual(collected.completion.request_id, "r1")
        self.assertEqual(collected.completion.result, {"payload": "payload"})
        self.assertGreaterEqual(collected.wait_s, 0.0)
        self.assertGreaterEqual(collected.result_s, 0.0)

    def test_submit_rejects_unknown_endpoint(self) -> None:
        adapter = RaySubmissionAdapter(FakeRay, {})

        with self.assertRaisesRegex(RuntimeError, "no Ray submitter"):
            adapter.submit(envelope(), "missing")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_ray_adapter.py
```

Expected: import failure for `src.scheduling.ray_adapter`.

- [ ] **Step 3: Implement adapter**

```python
"""Ray-specific submission adapter for the typed scheduling core."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping

from .models import CollectedSubmission, PayloadEnvelope, SubmissionCompletion


class RaySubmissionAdapter:
    def __init__(
        self,
        ray_module,
        submitters: Mapping[str, Callable[[object], object]],
    ):
        self.ray_module = ray_module
        self.submitters = dict(submitters)

    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        submitter = self.submitters.get(endpoint_id)
        if submitter is None:
            raise RuntimeError(f"no Ray submitter for endpoint {endpoint_id}")
        return submitter(envelope.payload)

    def wait_one(self, pending) -> CollectedSubmission:
        wait_start = time.perf_counter()
        ready, _ = self.ray_module.wait(
            [handle for handle, _ in pending],
            num_returns=1,
        )
        wait_s = time.perf_counter() - wait_start
        handle = ready[0]
        matches = [envelope for item, envelope in pending if item == handle]
        if len(matches) != 1:
            raise RuntimeError("Ray returned an unknown or duplicate pending handle")
        result_start = time.perf_counter()
        result = self.ray_module.get(handle)
        result_s = time.perf_counter() - result_start
        return CollectedSubmission(
            handle,
            SubmissionCompletion(matches[0].request.request_id, "completed", result=result),
            wait_s,
            result_s,
        )
```

Export `RaySubmissionAdapter`. Replace the test-local Ray adapter in
`test_scheduling_daft_ray_contract.py` with this production adapter.

- [ ] **Step 4: Run GREEN and real contract**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_ray_adapter.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
```

Expected: both modules end in `OK`.

- [ ] **Step 5: Commit**

```powershell
git add code/src/scheduling code/tests/test_ray_adapter.py code/tests/test_scheduling_daft_ray_contract.py
git commit -m "feat: add Ray scheduling adapter"
```

### Task 3: Profiler Scheduling Helpers

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Create: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- `_batch_envelopes(batches, job_id, operator, completion_max_tokens)`.
- `_endpoint_topology(endpoint_ids, endpoint_urls)`.
- `_scheduler_metrics(result, adaptive=False)`.

- [ ] **Step 1: Write failing helper tests**

Create tests that build a two-row Arrow table with prompt tokens `[10, 20]`
and assert:

```python
envelopes = profile._batch_envelopes(
    [table],
    job_id="job-1",
    operator="ai_complete",
    completion_max_tokens=8,
)
self.assertEqual(envelopes[0].request.prompt_tokens, 30)
self.assertEqual(envelopes[0].request.estimated_output_tokens, 16)
self.assertEqual(envelopes[0].request.payload_id, "job-1:batch:0")
self.assertIs(envelopes[0].payload, table)
```

Assert topology keeps endpoint ID/URL pairs and uses pool `default`. Assert
`_scheduler_metrics` returns the exact existing keys:

```python
{
    "operator_invocations",
    "max_inflight",
    "bounded_wait_s",
    "avg_bounded_wait_s",
    "fanin_s",
    "submit_s",
    "adaptive_downshifts",
    "adaptive_upshifts",
    "adaptive_limit_mean",
}
```

- [ ] **Step 2: Run RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

Expected: attribute failure for `_batch_envelopes`.

- [ ] **Step 3: Implement helpers**

Import scheduling types at the existing project-import boundary. Build request
IDs deterministically from job ID and batch index. For missing
`prompt_tokens`, use zero. Estimated output is
`completion_max_tokens * batch.num_rows` only for `ai_complete`.

Map `SchedulerResult` back to the exact existing metric dictionary; static
adaptive counts are zero and `adaptive_limit_mean` equals the configured
static limit from the result.

- [ ] **Step 4: Run GREEN**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

Expected: all helper tests pass.

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: add profiler scheduling helpers"
```

### Task 4: Static Ray Task Delegation

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Preserve `submit_ray_tasks(...) -> tuple[list[dict], dict]`.
- Delegate only when `adaptive_config is None`.
- Keep current body in `_submit_ray_tasks_legacy_adaptive`.

- [ ] **Step 1: Add failing static task parity test**

Use `FakeRay`, `FakeRemote`, and two Arrow batches. Assert static submission:

- returns both backend result dictionaries;
- records `operator_invocations == 2`;
- records `max_inflight == min(2, max_inflight)`;
- routes compatible HTTP calls across endpoint URLs in deterministic order;
- leaves all adaptive counters at zero.

Add a spy test proving a non-`None` adaptive config calls the isolated legacy
function rather than the typed scheduler.

Add an explicit static-delegation test:

```python
with mock.patch.object(
    profile,
    "_run_static_scheduler",
    return_value=([], expected_metrics),
) as run:
    profile.submit_ray_tasks(..., adaptive_config=None)
run.assert_called_once()
```

This must fail on the old loop even when old and new output values happen to
be equal.

- [ ] **Step 2: Run RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

Expected: `_run_static_scheduler` spy reports that it was not called.

- [ ] **Step 3: Delegate static task path**

At the top of `submit_ray_tasks`:

```python
if adaptive_config is not None:
    return _submit_ray_tasks_legacy_adaptive(...)
```

For static mode:

- create one logical endpoint for fake backend;
- create one endpoint/submitter per compatible endpoint URL;
- build typed envelopes and topology;
- run `SynchronousScheduler(StaticAdmissionController(max_inflight), ...)`;
- return completion results and `_scheduler_metrics`.

Do not change the legacy adaptive body beyond moving it to its named helper.

- [ ] **Step 4: Run GREEN and profiler dry-run**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py --dry-run --executor ray_task --data-source daft_postgres --organizer daft --writeback-mode none --output tmp\ray_static_wiring_dry_run.csv
```

Expected: tests pass; dry-run reports `status=dry_run`.

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "refactor: wire static Ray task scheduler"
```

### Task 5: Static Ray Actor Delegation

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Preserve `submit_with_backpressure(...) -> tuple[list[dict], dict]`.
- Delegate only when `adaptive_config is None`.
- Keep current body in `_submit_with_backpressure_legacy_adaptive`.

- [ ] **Step 1: Add failing actor parity test**

Use two fake actors with `.complete.remote(batch)` call logs. Assert:

- four batches route actor0, actor1, actor0, actor1;
- each result appears exactly once;
- `max_inflight` obeys the configured static limit;
- metric keys match the existing schema.

Add a spy test proving non-`None` adaptive config uses the isolated legacy
function.

Add the same `_run_static_scheduler` spy assertion for the static actor path,
so RED does not depend on incidental output differences.

- [ ] **Step 2: Run RED**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

Expected: actor delegation assertion fails before implementation.

- [ ] **Step 3: Delegate static actor path**

Create one endpoint ID and submitter per actor. Submitters call only
`getattr(actor, method_name).remote(payload)`. Run the same
`SynchronousScheduler` and map results/metrics identically to task mode.

Move the previous loop unchanged into
`_submit_with_backpressure_legacy_adaptive`.

- [ ] **Step 4: Run GREEN and real contract**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
```

Expected: both modules end in `OK`.

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "refactor: wire static Ray actor scheduler"
```

### Task 6: Full Verification and Documentation

**Files:**
- Modify: `code/README.md`
- Modify: `code/scripts/README.md`
- Modify: `learning/local_vllm_ray_baseline_walkthrough.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

- [ ] **Step 1: Document production static wiring**

State that static Ray task/actor now use the typed scheduler, while the
existing adaptive mode remains legacy until the next plan. Document focused
test and dry-run commands. Do not claim performance improvement.

- [ ] **Step 2: Run compile/import/dependency checks**

```powershell
.conda\pg-ai-profile\python.exe -m compileall -q code\src\scheduling code\scripts\postgres_ai_operator_profile.py code\tests
rg -n "import (daft|pyarrow|ray)|from (daft|pyarrow|ray)" code\src\scheduling\admission.py code\src\scheduling\models.py code\src\scheduling\routing.py code\src\scheduling\scheduler.py code\src\scheduling\topology.py
```

Expected: compile exits 0; policy scan has no matches.

- [ ] **Step 3: Run all tests**

```powershell
Get-ChildItem code\tests\test_*.py | ForEach-Object {
  .conda\pg-ai-profile\python.exe $_.FullName
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
```

Expected: every module ends in `OK`.

- [ ] **Step 4: Run real Daft→Ray smoke**

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_scheduling_daft_ray_contract.py
```

Expected: `Ran 1 test ... OK`.

- [ ] **Step 5: Inspect diff and commit docs**

```powershell
git diff --check
git status --short
```

Commit:

```powershell
git add code/README.md code/scripts/README.md learning/local_vllm_ray_baseline_walkthrough.md PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "docs: document static Ray scheduler wiring"
```

## Plan Self-Review

- This plan implements only production static task/actor wiring and timing
  parity.
- The old adaptive algorithm remains callable and behaviorally isolated.
- All public profiler function signatures and metric keys remain unchanged.
- Real Daft and Ray appear only in adapter/integration layers.
- Dynamic policies and performance experiments remain separate, preventing
  refactor effects from being confused with method effects.
