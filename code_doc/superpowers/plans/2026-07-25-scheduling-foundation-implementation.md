# Scheduling Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the typed scheduling schemas, validated endpoint topology, static admission/routing policies, and a deterministic synchronous scheduler that preserves every submitted batch exactly once.

**Architecture:** Add a focused `src/scheduling/` policy core independent of Daft, Arrow, Ray, HTTP, and vLLM, while keeping the only formal runtime path as `PostgreSQL -> Daft -> Arrow payload -> Ray -> endpoint`. Policies consume immutable metadata and topology snapshots; the scheduler owns orchestration through a small submission-adapter protocol. A deterministic adapter supports unit tests, and a Daft-to-Ray contract smoke proves that the boundary fits the project framework before adaptive work begins.

**Tech Stack:** Python 3.11+, standard-library `dataclasses`, `typing.Protocol`, `unittest`; existing project test entrypoints.

## Global Constraints

- Do not modify vLLM internals, the Ray scheduler, model kernels, or database query operators.
- Strategy code must not import `daft`, `pyarrow`, `ray`, or HTTP clients.
- Production payloads come from Daft and production submission uses Ray;
  synchronous/fake adapters are test-only and cannot produce formal results.
- Every input request must be completed or failed exactly once.
- Static scheduling must not depend on adaptive metrics.
- Use typed dataclasses and protocols; do not expose unconstrained dictionaries as public policy APIs.
- Use red-green-refactor and run each focused test in the project `.conda/pg-ai-profile` environment.
- Do not add dynamic flush, AIMD, EWMA, PID, UCB, actor pools, or search in this plan.
- Do not add `Co-Authored-By` or AI attribution to commits.

---

## File Map

- Create `code/src/scheduling/__init__.py`: public scheduling-foundation exports.
- Create `code/src/scheduling/models.py`: immutable request, payload-envelope, endpoint, topology, decision, and completion types.
- Create `code/src/scheduling/topology.py`: topology validation and healthy-endpoint lookup.
- Create `code/src/scheduling/admission.py`: fixed in-flight admission baseline.
- Create `code/src/scheduling/routing.py`: deterministic round-robin endpoint baseline.
- Create `code/src/scheduling/scheduler.py`: synchronous scheduler and submission-adapter protocol.
- Create `code/tests/scheduling/test_scheduling_models.py`: schema and topology behavior.
- Create `code/tests/scheduling/test_scheduling_policies.py`: static admission and routing behavior.
- Create `code/tests/scheduling/test_scheduler.py`: deterministic scheduler and invariants.
- Create `code/tests/scheduling/test_scheduling_daft_ray_contract.py`: real Daft payload
  and Ray task contract smoke.
- Modify `code/README.md`: scheduling package entry.
- Modify `PROJECT_INDEX.md`: new source/test entries.
- Modify `PROJECT_LOG.md`: implementation and verification record.

### Task 1: Immutable Request and Endpoint Schemas

**Files:**
- Create: `code/src/scheduling/__init__.py`
- Create: `code/src/scheduling/models.py`
- Test: `code/tests/scheduling/test_scheduling_models.py`

**Interfaces:**
- Produces: `BatchRequest`, `PayloadEnvelope`, `EndpointSnapshot`, `TopologySnapshot`, `AdmissionDecision`, `RoutingDecision`, `SubmissionCompletion`.
- Consumes: standard-library types only.

- [ ] **Step 1: Write failing schema tests**

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    TopologySnapshot,
)


class SchedulingModelTests(unittest.TestCase):
    def test_batch_request_rejects_non_positive_row_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "row_count must be positive"):
            BatchRequest(
                request_id="r1",
                job_id="j1",
                operator="ai_complete",
                row_count=0,
                prompt_tokens=10,
                estimated_output_tokens=5,
                prefix_key="",
                first_arrival_s=1.0,
                oldest_arrival_s=1.0,
                payload_id="p1",
            )

    def test_payload_envelope_keeps_payload_out_of_request_metadata(self) -> None:
        request = BatchRequest(
            request_id="r1",
            job_id="j1",
            operator="ai_complete",
            row_count=2,
            prompt_tokens=10,
            estimated_output_tokens=6,
            prefix_key="prefix",
            first_arrival_s=2.0,
            oldest_arrival_s=1.0,
            payload_id="p1",
        )
        payload = object()

        envelope = PayloadEnvelope(request=request, payload=payload)

        self.assertEqual(request.estimated_total_tokens, 16)
        self.assertIs(envelope.payload, payload)

    def test_topology_snapshot_rejects_duplicate_endpoint_ids(self) -> None:
        endpoint = EndpointSnapshot(
            endpoint_id="e1",
            url="http://localhost:8000/v1/completions",
            pool_id="default",
            gpu_id="0",
            healthy=True,
            running=0,
            waiting=0,
            kv_usage=0.0,
            observed_at_s=1.0,
        )

        with self.assertRaisesRegex(ValueError, "endpoint_id values must be unique"):
            TopologySnapshot(endpoints=(endpoint, endpoint), observed_at_s=1.0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py
```

Expected: import failure because `src.scheduling.models` does not exist.

- [ ] **Step 3: Implement the minimal immutable schemas**

Create `code/src/scheduling/models.py`:

```python
"""Typed, engine-independent scheduling data models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


OperatorName = Literal["ai_complete", "ai_embed", "ai_classify"]


@dataclass(frozen=True)
class BatchRequest:
    request_id: str
    job_id: str
    operator: OperatorName
    row_count: int
    prompt_tokens: int
    estimated_output_tokens: int
    prefix_key: str
    first_arrival_s: float
    oldest_arrival_s: float
    payload_id: str

    def __post_init__(self) -> None:
        if self.row_count <= 0:
            raise ValueError("row_count must be positive")
        if self.prompt_tokens < 0 or self.estimated_output_tokens < 0:
            raise ValueError("token counts must be non-negative")
        if not self.request_id or not self.job_id or not self.payload_id:
            raise ValueError("request_id, job_id, and payload_id must be non-empty")

    @property
    def estimated_total_tokens(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens


@dataclass(frozen=True)
class PayloadEnvelope:
    request: BatchRequest
    payload: object


@dataclass(frozen=True)
class EndpointSnapshot:
    endpoint_id: str
    url: str
    pool_id: str
    gpu_id: str
    healthy: bool
    running: int
    waiting: int
    kv_usage: float | None
    observed_at_s: float

    def __post_init__(self) -> None:
        if not self.endpoint_id or not self.url or not self.pool_id:
            raise ValueError("endpoint_id, url, and pool_id must be non-empty")
        if self.running < 0 or self.waiting < 0:
            raise ValueError("running and waiting must be non-negative")
        if self.kv_usage is not None and not 0.0 <= self.kv_usage <= 1.0:
            raise ValueError("kv_usage must be between 0 and 1")


@dataclass(frozen=True)
class TopologySnapshot:
    endpoints: tuple[EndpointSnapshot, ...]
    observed_at_s: float

    def __post_init__(self) -> None:
        endpoint_ids = [endpoint.endpoint_id for endpoint in self.endpoints]
        if len(endpoint_ids) != len(set(endpoint_ids)):
            raise ValueError("endpoint_id values must be unique")


@dataclass(frozen=True)
class AdmissionDecision:
    allowed: bool
    limit: int
    action: str
    reason: str


@dataclass(frozen=True)
class RoutingDecision:
    endpoint_id: str
    pool_id: str
    reason: str


@dataclass(frozen=True)
class SubmissionCompletion:
    request_id: str
    status: Literal["completed", "failed"]
    result: object | None = None
    error: str = ""
```

Create `code/src/scheduling/__init__.py`:

```python
"""Composable scheduling policies for database AI operator execution."""

from .models import (
    AdmissionDecision,
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    RoutingDecision,
    SubmissionCompletion,
    TopologySnapshot,
)

__all__ = [
    "AdmissionDecision",
    "BatchRequest",
    "EndpointSnapshot",
    "PayloadEnvelope",
    "RoutingDecision",
    "SubmissionCompletion",
    "TopologySnapshot",
]
```

- [ ] **Step 4: Run the focused test and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py
```

Expected: `Ran 3 tests ... OK`.

- [ ] **Step 5: Commit Task 1**

```powershell
git add code/src/scheduling/__init__.py code/src/scheduling/models.py code/tests/scheduling/test_scheduling_models.py
git commit -m "feat: add typed scheduling models"
```

### Task 2: Validated Topology and Round-Robin Routing

**Files:**
- Create: `code/src/scheduling/topology.py`
- Create: `code/src/scheduling/routing.py`
- Test: `code/tests/scheduling/test_scheduling_policies.py`
- Modify: `code/src/scheduling/__init__.py`

**Interfaces:**
- Consumes: `BatchRequest`, `EndpointSnapshot`, `TopologySnapshot`, `RoutingDecision`.
- Produces: `healthy_endpoints(topology, pool_id)`, `RoundRobinEndpointRouter.route(request, topology, pool_id)`.

- [ ] **Step 1: Write failing topology and routing tests**

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.models import BatchRequest, EndpointSnapshot, TopologySnapshot  # noqa: E402
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.topology import healthy_endpoints  # noqa: E402


def request() -> BatchRequest:
    return BatchRequest("r1", "j1", "ai_complete", 1, 10, 5, "", 1.0, 1.0, "p1")


def endpoint(endpoint_id: str, *, healthy: bool = True, pool_id: str = "default") -> EndpointSnapshot:
    return EndpointSnapshot(
        endpoint_id,
        f"http://localhost/{endpoint_id}",
        pool_id,
        "0",
        healthy,
        0,
        0,
        0.0,
        1.0,
    )


class SchedulingPolicyTests(unittest.TestCase):
    def test_healthy_endpoints_filters_pool_and_health(self) -> None:
        topology = TopologySnapshot(
            (endpoint("e1"), endpoint("e2", healthy=False), endpoint("e3", pool_id="long")),
            1.0,
        )

        self.assertEqual(
            [item.endpoint_id for item in healthy_endpoints(topology, "default")],
            ["e1"],
        )

    def test_round_robin_skips_unhealthy_endpoints(self) -> None:
        topology = TopologySnapshot(
            (endpoint("e1"), endpoint("e2", healthy=False), endpoint("e3")),
            1.0,
        )
        router = RoundRobinEndpointRouter()

        selected = [router.route(request(), topology, "default").endpoint_id for _ in range(3)]

        self.assertEqual(selected, ["e1", "e3", "e1"])

    def test_round_robin_fails_when_pool_has_no_healthy_endpoint(self) -> None:
        topology = TopologySnapshot((endpoint("e1", healthy=False),), 1.0)

        with self.assertRaisesRegex(RuntimeError, "no healthy endpoint"):
            RoundRobinEndpointRouter().route(request(), topology, "default")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
```

Expected: import failure for `src.scheduling.routing`.

- [ ] **Step 3: Implement topology lookup and deterministic routing**

Create `code/src/scheduling/topology.py`:

```python
"""Endpoint topology queries."""

from __future__ import annotations

from .models import EndpointSnapshot, TopologySnapshot


def healthy_endpoints(topology: TopologySnapshot, pool_id: str) -> tuple[EndpointSnapshot, ...]:
    return tuple(
        endpoint
        for endpoint in topology.endpoints
        if endpoint.pool_id == pool_id and endpoint.healthy
    )
```

Create `code/src/scheduling/routing.py`:

```python
"""Endpoint routing policies."""

from __future__ import annotations

from .models import BatchRequest, RoutingDecision, TopologySnapshot
from .topology import healthy_endpoints


class RoundRobinEndpointRouter:
    def __init__(self) -> None:
        self._next_index_by_pool: dict[str, int] = {}

    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        del request
        candidates = healthy_endpoints(topology, pool_id)
        if not candidates:
            raise RuntimeError(f"no healthy endpoint in pool {pool_id}")
        index = self._next_index_by_pool.get(pool_id, 0)
        endpoint = candidates[index % len(candidates)]
        self._next_index_by_pool[pool_id] = (index + 1) % len(candidates)
        return RoutingDecision(endpoint.endpoint_id, pool_id, "round_robin")
```

Add public exports to `code/src/scheduling/__init__.py`:

```python
from .routing import RoundRobinEndpointRouter
from .topology import healthy_endpoints
```

Add both names to `__all__`.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
```

Expected: `Ran 3 tests ... OK`.

- [ ] **Step 5: Commit Task 2**

```powershell
git add code/src/scheduling/__init__.py code/src/scheduling/topology.py code/src/scheduling/routing.py code/tests/scheduling/test_scheduling_policies.py
git commit -m "feat: add endpoint topology and routing baseline"
```

### Task 3: Static Admission Baseline

**Files:**
- Create: `code/src/scheduling/admission.py`
- Modify: `code/tests/scheduling/test_scheduling_policies.py`
- Modify: `code/src/scheduling/__init__.py`

**Interfaces:**
- Consumes: current integer in-flight count.
- Produces: `StaticAdmissionController(limit).decide(inflight) -> AdmissionDecision`.

- [ ] **Step 1: Add failing static-admission tests**

Append to `SchedulingPolicyTests`:

```python
    def test_static_admission_allows_below_limit(self) -> None:
        controller = StaticAdmissionController(limit=2)

        decision = controller.decide(inflight=1)

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.limit, 2)
        self.assertEqual(decision.reason, "below_static_limit")

    def test_static_admission_blocks_at_limit(self) -> None:
        decision = StaticAdmissionController(limit=2).decide(inflight=2)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "at_static_limit")

    def test_static_admission_rejects_non_positive_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit must be positive"):
            StaticAdmissionController(limit=0)
```

Add the import:

```python
from src.scheduling.admission import StaticAdmissionController  # noqa: E402
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
```

Expected: import failure for `src.scheduling.admission`.

- [ ] **Step 3: Implement the fixed admission controller**

Create `code/src/scheduling/admission.py`:

```python
"""Admission-control policies."""

from __future__ import annotations

from .models import AdmissionDecision


class StaticAdmissionController:
    def __init__(self, limit: int):
        if limit <= 0:
            raise ValueError("limit must be positive")
        self.limit = limit

    def decide(self, inflight: int) -> AdmissionDecision:
        if inflight < 0:
            raise ValueError("inflight must be non-negative")
        allowed = inflight < self.limit
        return AdmissionDecision(
            allowed=allowed,
            limit=self.limit,
            action="admit" if allowed else "wait",
            reason="below_static_limit" if allowed else "at_static_limit",
        )
```

Export `StaticAdmissionController` from `code/src/scheduling/__init__.py`.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
```

Expected: `Ran 6 tests ... OK`.

- [ ] **Step 5: Commit Task 3**

```powershell
git add code/src/scheduling/__init__.py code/src/scheduling/admission.py code/tests/scheduling/test_scheduling_policies.py
git commit -m "feat: add static admission baseline"
```

### Task 4: Deterministic Scheduler and Exactly-Once Invariants

**Files:**
- Create: `code/src/scheduling/scheduler.py`
- Create: `code/tests/scheduling/test_scheduler.py`
- Modify: `code/src/scheduling/__init__.py`

**Interfaces:**
- Consumes: iterable of `PayloadEnvelope`, `TopologySnapshot`, `StaticAdmissionController`, `RoundRobinEndpointRouter`, and `SubmissionAdapter`.
- Produces: `SchedulerResult(completions, max_inflight_seen)` and adapter calls containing endpoint decisions.

- [ ] **Step 1: Write failing scheduler tests**

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionCompletion,
    TopologySnapshot,
)
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.scheduler import SynchronousScheduler  # noqa: E402


def envelope(index: int) -> PayloadEnvelope:
    request = BatchRequest(
        f"r{index}",
        "j1",
        "ai_complete",
        1,
        10,
        5,
        "",
        float(index),
        float(index),
        f"p{index}",
    )
    return PayloadEnvelope(request, f"payload-{index}")


def topology() -> TopologySnapshot:
    endpoints = tuple(
        EndpointSnapshot(
            endpoint_id,
            f"http://localhost/{endpoint_id}",
            "default",
            "0",
            True,
            0,
            0,
            0.0,
            1.0,
        )
        for endpoint_id in ("e1", "e2")
    )
    return TopologySnapshot(endpoints, 1.0)


class FakeSubmissionAdapter:
    def __init__(self) -> None:
        self.submitted: list[tuple[str, str]] = []

    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        handle = (envelope.request.request_id, endpoint_id)
        self.submitted.append(handle)
        return handle

    def wait_one(self, pending: list[tuple[object, PayloadEnvelope]]) -> tuple[object, SubmissionCompletion]:
        handle, pending_envelope = pending[0]
        completion = SubmissionCompletion(
            request_id=pending_envelope.request.request_id,
            status="completed",
            result=pending_envelope.payload,
        )
        return handle, completion


class SchedulerTests(unittest.TestCase):
    def test_scheduler_completes_each_request_once_with_bounded_inflight(self) -> None:
        adapter = FakeSubmissionAdapter()
        scheduler = SynchronousScheduler(
            admission=StaticAdmissionController(limit=2),
            router=RoundRobinEndpointRouter(),
            adapter=adapter,
            pool_id="default",
        )

        result = scheduler.run([envelope(index) for index in range(5)], topology())

        self.assertEqual([item.request_id for item in result.completions], ["r0", "r1", "r2", "r3", "r4"])
        self.assertEqual(len(set(item.request_id for item in result.completions)), 5)
        self.assertEqual(result.max_inflight_seen, 2)
        self.assertEqual(
            adapter.submitted,
            [("r0", "e1"), ("r1", "e2"), ("r2", "e1"), ("r3", "e2"), ("r4", "e1")],
        )

    def test_scheduler_preserves_failed_completion_without_retry(self) -> None:
        class FailingAdapter(FakeSubmissionAdapter):
            def wait_one(self, pending):
                handle, pending_envelope = pending[0]
                return handle, SubmissionCompletion(
                    request_id=pending_envelope.request.request_id,
                    status="failed",
                    error="synthetic failure",
                )

        scheduler = SynchronousScheduler(
            StaticAdmissionController(1),
            RoundRobinEndpointRouter(),
            FailingAdapter(),
            "default",
        )

        result = scheduler.run([envelope(0)], topology())

        self.assertEqual(result.completions[0].status, "failed")
        self.assertEqual(result.completions[0].error, "synthetic failure")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py
```

Expected: import failure for `src.scheduling.scheduler`.

- [ ] **Step 3: Implement the minimal synchronous scheduler**

Create `code/src/scheduling/scheduler.py`:

```python
"""Synchronous policy-composition scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .admission import StaticAdmissionController
from .models import PayloadEnvelope, SubmissionCompletion, TopologySnapshot
from .routing import RoundRobinEndpointRouter


class SubmissionAdapter(Protocol):
    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        ...

    def wait_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> tuple[object, SubmissionCompletion]:
        ...


@dataclass(frozen=True)
class SchedulerResult:
    completions: tuple[SubmissionCompletion, ...]
    max_inflight_seen: int


class SynchronousScheduler:
    def __init__(
        self,
        admission: StaticAdmissionController,
        router: RoundRobinEndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
    ):
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        pending: list[tuple[object, PayloadEnvelope]] = []
        completions: list[SubmissionCompletion] = []
        max_inflight_seen = 0

        for envelope in envelopes:
            while not self.admission.decide(len(pending)).allowed:
                self._collect_one(pending, completions)
            route = self.router.route(envelope.request, topology, self.pool_id)
            handle = self.adapter.submit(envelope, route.endpoint_id)
            pending.append((handle, envelope))
            max_inflight_seen = max(max_inflight_seen, len(pending))

        while pending:
            self._collect_one(pending, completions)

        return SchedulerResult(tuple(completions), max_inflight_seen)

    def _collect_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
    ) -> None:
        handle, completion = self.adapter.wait_one(pending)
        matching = [index for index, (item, _) in enumerate(pending) if item == handle]
        if len(matching) != 1:
            raise RuntimeError("adapter returned an unknown or duplicate pending handle")
        pending.pop(matching[0])
        completions.append(completion)
```

Export `SchedulerResult`, `SubmissionAdapter`, and `SynchronousScheduler` from
`code/src/scheduling/__init__.py`.

- [ ] **Step 4: Run focused scheduling tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py
```

Expected: all three commands end in `OK`.

- [ ] **Step 5: Run the entire current code test suite**

Run:

```powershell
Get-ChildItem code\tests\test_*.py | ForEach-Object {
  .conda\pg-ai-profile\python.exe $_.FullName
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
```

Expected: every test module ends in `OK`, with no traceback.

- [ ] **Step 6: Commit Task 4**

```powershell
git add code/src/scheduling/__init__.py code/src/scheduling/scheduler.py code/tests/scheduling/test_scheduler.py
git commit -m "feat: add deterministic static scheduler"
```

### Task 5: Daft-to-Ray Framework Contract Smoke

**Files:**
- Create: `code/tests/scheduling/test_scheduling_daft_ray_contract.py`

**Interfaces:**
- Consumes: `DaftOrganizer`, `OrganizerConfig`, `PayloadEnvelope`,
  `SynchronousScheduler`, and a test-local Ray adapter.
- Proves: a Daft-produced Arrow batch can cross the typed scheduling boundary
  and execute as a Ray task without policy modules importing Daft or Ray.

- [ ] **Step 1: Write the framework contract test**

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.organizers import DaftOrganizer, OrganizerConfig  # noqa: E402
from src.scheduling.admission import StaticAdmissionController  # noqa: E402
from src.scheduling.models import (  # noqa: E402
    BatchRequest,
    EndpointSnapshot,
    PayloadEnvelope,
    SubmissionCompletion,
    TopologySnapshot,
)
from src.scheduling.routing import RoundRobinEndpointRouter  # noqa: E402
from src.scheduling.scheduler import SynchronousScheduler  # noqa: E402


class DaftRayContractTests(unittest.TestCase):
    def test_daft_arrow_batches_execute_through_ray_adapter(self) -> None:
        import ray

        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "tenant_id": [1, 1, 1, 1],
                "category": ["a", "a", "b", "b"],
                "text": ["one", "two", "three", "four"],
                "prompt_tokens": [1, 1, 1, 1],
                "target_output_tokens": [1, 1, 1, 1],
                "arrival_time_s": [0.0, 0.1, 0.2, 0.3],
                "session_id": ["s1", "s1", "s2", "s2"],
                "prefix_key": ["", "", "", ""],
            }
        )
        organized = DaftOrganizer(
            OrganizerConfig(batch_size=2, runner="native")
        ).organize(table)

        @ray.remote
        def execute(payload, endpoint_id):
            return {
                "rows": payload.num_rows,
                "endpoint_id": endpoint_id,
            }

        class RayTaskAdapter:
            def submit(self, envelope, endpoint_id):
                return execute.remote(envelope.payload, endpoint_id)

            def wait_one(self, pending):
                refs = [handle for handle, _ in pending]
                ready, _ = ray.wait(refs, num_returns=1)
                handle = ready[0]
                envelope = next(item for item_handle, item in pending if item_handle == handle)
                return handle, SubmissionCompletion(
                    request_id=envelope.request.request_id,
                    status="completed",
                    result=ray.get(handle),
                )

        envelopes = [
            PayloadEnvelope(
                BatchRequest(
                    request_id=f"r{index}",
                    job_id="j1",
                    operator="ai_complete",
                    row_count=batch.num_rows,
                    prompt_tokens=batch.num_rows,
                    estimated_output_tokens=batch.num_rows,
                    prefix_key="",
                    first_arrival_s=0.0,
                    oldest_arrival_s=0.0,
                    payload_id=f"p{index}",
                ),
                batch,
            )
            for index, batch in enumerate(organized.batches)
        ]
        topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "e1",
                    "http://localhost:8000/v1/completions",
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
            ),
            1.0,
        )

        ray.init(ignore_reinit_error=True, num_cpus=1)
        try:
            result = SynchronousScheduler(
                StaticAdmissionController(2),
                RoundRobinEndpointRouter(),
                RayTaskAdapter(),
                "default",
            ).run(envelopes, topology)
        finally:
            ray.shutdown()

        self.assertEqual([item.result["rows"] for item in result.completions], [2, 2])
        self.assertEqual(
            [item.result["endpoint_id"] for item in result.completions],
            ["e1", "e1"],
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the Daft-to-Ray contract smoke**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py
```

Expected: `Ran 1 test ... OK`. A dependency/import failure is an environment
failure and must be resolved before adaptive-policy work starts.

- [ ] **Step 3: Verify policy modules remain engine independent**

Run:

```powershell
rg -n "import (daft|pyarrow|ray)|from (daft|pyarrow|ray)" code\src\scheduling
```

Expected: no matches.

- [ ] **Step 4: Commit Task 5**

```powershell
git add code/tests/scheduling/test_scheduling_daft_ray_contract.py
git commit -m "test: verify Daft Ray scheduling contract"
```

### Task 6: Documentation and Verification Closeout

**Files:**
- Modify: `code/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Documents: the package boundary, supported static behavior, test command, and explicit exclusions.
- Produces: no new runtime API.

- [ ] **Step 1: Document the scheduling foundation**

Add to `code/README.md`:

````markdown
## Scheduling foundation

`code/src/scheduling/` contains engine-independent request metadata, endpoint
topology, static admission, round-robin routing, and a deterministic
synchronous scheduler. Policies do not import Daft, Arrow, Ray, or HTTP.

Run the focused tests with:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py
```

This foundation is designed only for the Daft-to-Ray production path. The
deterministic synchronous adapter is test-only. This slice does not yet
replace the existing production Ray submission loop and does not yet implement
flush or adaptive policies; the next implementation plan must wire the typed
core into the Ray task baseline before new method policies are enabled.
````

- [ ] **Step 2: Update project index and log**

Add exact entries for:

- `code/src/scheduling/`;
- the four new test modules;
- the focused test result and full-suite result;
- the boundary that production Ray wiring is the required next subproject.

- [ ] **Step 3: Run syntax and import verification**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m compileall -q code\src\scheduling code\tests\scheduling\test_scheduling_models.py code\tests\scheduling\test_scheduling_policies.py code\tests\scheduling\test_scheduler.py code\tests\scheduling\test_scheduling_daft_ray_contract.py
.conda\pg-ai-profile\python.exe -c "import sys; sys.path.insert(0, 'code'); from src.scheduling import BatchRequest, StaticAdmissionController, SynchronousScheduler; print('scheduling imports ok')"
```

Expected:

```text
scheduling imports ok
```

- [ ] **Step 4: Re-run the entire code test suite**

Run:

```powershell
Get-ChildItem code\tests\test_*.py | ForEach-Object {
  .conda\pg-ai-profile\python.exe $_.FullName
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
```

Expected: every module ends in `OK`.

- [ ] **Step 5: Inspect the final diff**

Run:

```powershell
git diff --check
git status --short
```

Expected: no whitespace errors; only scheduling-foundation source, tests, and
required documentation are modified.

- [ ] **Step 6: Commit Task 6**

```powershell
git add code/README.md PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "docs: document scheduling foundation"
```

## Plan Self-Review

- Spec coverage: this plan covers implementation-order items 1 and the
  engine-independent portion of item 2 from the approved suite design.
- Framework constraint: the only formal target is Daft input plus Ray
  execution; deterministic adapters are test-only.
- Deliberate exclusions: production Ray wiring, flush, adaptive admission,
  pool routing, metrics artifacts, search, and GPU experiments each require a
  separate focused plan after this foundation passes.
- Type consistency: all later tasks consume the exact dataclasses created in
  Task 1.
- Test discipline: every production behavior has a failing focused test before
  implementation.
- Scope discipline: no existing profiler behavior changes in this plan.
