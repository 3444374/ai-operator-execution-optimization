# Ray Execution Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate vLLM service endpoints from endpoint-local Ray actor workers and expose explicit, tested Ray CPU and actor-concurrency settings.

**Architecture:** Endpoint routing continues to select one `EndpointSnapshot`. For actor execution, that endpoint owns a small round-robin submitter over one or more Ray actors. Ray resource options are represented by one immutable scheduling-layer value and applied when remote task/actor definitions are created.

**Tech Stack:** Python 3.11, Ray 2.56, PyArrow, unittest, Daft, PostgreSQL profiler CLI.

## Global Constraints

- Preserve `PostgreSQL -> Daft -> Arrow -> Ray -> external vLLM`; do not modify Ray or vLLM internals.
- HTTP Ray workers always request `num_gpus=0`.
- Formal completion calls use `max_retries=0`, `max_restarts=0`, and `max_task_retries=0`.
- Keep existing single-endpoint behavior compatible through `--model-workers`; new experiment configs use `--actor-workers-per-endpoint`.
- Multi-endpoint actor runs require the explicit per-endpoint option.
- Follow strict RED -> GREEN -> REFACTOR; do not write production behavior before its failing test.
- Do not touch the unrelated untracked `.superpowers/` directory or the inaccessible historical `code/.test-tmp/tmpgjluud5o`.

---

### Task 1: Endpoint-local actor worker submitter

**Files:**
- Modify: `code/src/scheduling/ray_adapter.py`
- Modify: `code/src/scheduling/__init__.py`
- Test: `code/tests/test_ray_adapter.py`

**Interfaces:**
- Consumes: actor handles whose named method exposes `.remote(payload)`.
- Produces: `ActorWorkerPoolSubmitter(actors: Sequence[object], method_name: str)` with `__call__(payload: object) -> object`, `worker_count: int`, and `submission_counts: tuple[int, ...]`.

- [ ] **Step 1: Write failing constructor and rotation tests**

```python
from src.scheduling.ray_adapter import ActorWorkerPoolSubmitter


def test_actor_worker_pool_rotates_inside_one_endpoint() -> None:
    actors = [_RecordingActor(), _RecordingActor()]
    submitter = ActorWorkerPoolSubmitter(actors, "complete")

    submitter("a")
    submitter("b")
    submitter("c")

    assert actors[0].complete.payloads == ["a", "c"]
    assert actors[1].complete.payloads == ["b"]
    assert submitter.submission_counts == (2, 1)


def test_actor_worker_pool_rejects_empty_workers() -> None:
    with pytest.raises(ValueError, match="actors must not be empty"):
        ActorWorkerPoolSubmitter([], "complete")
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_ray_adapter.py -q
```

Expected: FAIL because `ActorWorkerPoolSubmitter` is not defined.

- [ ] **Step 3: Implement the minimal submitter**

```python
class ActorWorkerPoolSubmitter:
    def __init__(self, actors: Sequence[object], method_name: str):
        if not actors:
            raise ValueError("actors must not be empty")
        if not method_name:
            raise ValueError("method_name must not be empty")
        self._actors = tuple(actors)
        self._method_name = method_name
        self._next_index = 0
        self._submission_counts = [0] * len(self._actors)

    @property
    def worker_count(self) -> int:
        return len(self._actors)

    @property
    def submission_counts(self) -> tuple[int, ...]:
        return tuple(self._submission_counts)

    def __call__(self, payload: object) -> object:
        index = self._next_index
        self._next_index = (index + 1) % len(self._actors)
        self._submission_counts[index] += 1
        return getattr(self._actors[index], self._method_name).remote(payload)
```

Export the class from `code/src/scheduling/__init__.py`.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_ray_adapter.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add code/src/scheduling/ray_adapter.py code/src/scheduling/__init__.py code/tests/test_ray_adapter.py
git commit -m "feat: add endpoint-local actor submitter"
```

---

### Task 2: Typed Ray worker options

**Files:**
- Create: `code/src/scheduling/ray_runtime.py`
- Modify: `code/src/scheduling/__init__.py`
- Create: `code/tests/test_ray_runtime.py`

**Interfaces:**
- Produces: `RayWorkerOptions(num_cpus: float, actor_max_concurrency: int)`.
- Produces: `task_options() -> dict[str, object]` and `actor_options() -> dict[str, object]`.

- [ ] **Step 1: Write failing validation and option tests**

```python
def test_http_worker_options_never_reserve_gpu_or_retry() -> None:
    options = RayWorkerOptions(num_cpus=0.25, actor_max_concurrency=4)

    assert options.task_options() == {
        "num_cpus": 0.25,
        "num_gpus": 0,
        "max_retries": 0,
    }
    assert options.actor_options() == {
        "num_cpus": 0.25,
        "num_gpus": 0,
        "max_concurrency": 4,
        "max_restarts": 0,
        "max_task_retries": 0,
    }


@pytest.mark.parametrize("num_cpus", [0, -0.1])
def test_worker_options_require_positive_cpu(num_cpus: float) -> None:
    with pytest.raises(ValueError, match="num_cpus must be positive"):
        RayWorkerOptions(num_cpus=num_cpus, actor_max_concurrency=1)
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_ray_runtime.py -q
```

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the immutable options value**

```python
@dataclass(frozen=True)
class RayWorkerOptions:
    num_cpus: float
    actor_max_concurrency: int = 1

    def __post_init__(self) -> None:
        if not math.isfinite(self.num_cpus) or self.num_cpus <= 0:
            raise ValueError("num_cpus must be positive and finite")
        if self.actor_max_concurrency <= 0:
            raise ValueError("actor_max_concurrency must be positive")

    def task_options(self) -> dict[str, object]:
        return {"num_cpus": self.num_cpus, "num_gpus": 0, "max_retries": 0}

    def actor_options(self) -> dict[str, object]:
        return {
            "num_cpus": self.num_cpus,
            "num_gpus": 0,
            "max_concurrency": self.actor_max_concurrency,
            "max_restarts": 0,
            "max_task_retries": 0,
        }
```

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_ray_runtime.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add code/src/scheduling/ray_runtime.py code/src/scheduling/__init__.py code/tests/test_ray_runtime.py
git commit -m "feat: define Ray HTTP worker resources"
```

---

### Task 3: CLI resolution and validation

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Adds CLI: `--actor-workers-per-endpoint` (integer, default `0`).
- Adds CLI: `--ray-actor-max-concurrency` (integer, default `1`).
- Adds CLI: `--ray-worker-num-cpus` (float, default `0.25`).
- Produces: `_resolve_actor_workers_per_endpoint(args, endpoint_count: int) -> int`.

- [ ] **Step 1: Write failing CLI and validation tests**

```python
def test_ray_worker_cli_defaults_are_explicit() -> None:
    args = profile.parse_args([])
    assert args.actor_workers_per_endpoint == 0
    assert args.ray_actor_max_concurrency == 1
    assert args.ray_worker_num_cpus == 0.25


def test_single_endpoint_legacy_workers_remain_compatible() -> None:
    args = profile.parse_args(["--model-workers", "4"])
    assert profile._resolve_actor_workers_per_endpoint(args, 1) == 4


def test_multi_endpoint_actor_requires_explicit_workers() -> None:
    args = profile.parse_args(["--executor", "ray_actor"])
    with pytest.raises(SystemExit, match="actor-workers-per-endpoint"):
        profile._resolve_actor_workers_per_endpoint(args, 2)
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py -q
```

Expected: FAIL because the new parsed attributes and resolver do not exist.

- [ ] **Step 3: Add arguments and resolver**

```python
parser.add_argument("--actor-workers-per-endpoint", type=int, default=0)
parser.add_argument("--ray-actor-max-concurrency", type=int, default=1)
parser.add_argument("--ray-worker-num-cpus", type=float, default=0.25)


def _resolve_actor_workers_per_endpoint(args, endpoint_count: int) -> int:
    if endpoint_count <= 0:
        raise SystemExit("endpoint_count must be positive")
    if args.actor_workers_per_endpoint < 0:
        raise SystemExit("--actor-workers-per-endpoint must be non-negative")
    if args.actor_workers_per_endpoint:
        return args.actor_workers_per_endpoint
    if endpoint_count > 1:
        raise SystemExit(
            "multi-endpoint ray_actor requires --actor-workers-per-endpoint"
        )
    if args.model_workers <= 0:
        raise SystemExit("--model-workers must be positive")
    return args.model_workers
```

Validate positive actor concurrency and finite positive CPU values before
database or Ray initialization.

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: expose Ray actor worker settings"
```

---

### Task 4: Build endpoint-local actor pools

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Changes `submit_with_backpressure` to consume
  `actor_pools: Mapping[str, Sequence[object]]` and
  `endpoint_urls: Mapping[str, str]`.
- Uses `ActorWorkerPoolSubmitter` per endpoint.
- Keeps endpoint topology cardinality equal to service endpoint cardinality.

- [ ] **Step 1: Replace the old actor-count topology test with a failing endpoint-local pool test**

```python
def test_two_actor_workers_remain_one_service_endpoint() -> None:
    actors = [_RecordingActor(), _RecordingActor()]
    actor_pools = {"endpoint-0": actors}
    endpoint_urls = {"endpoint-0": "http://localhost:8000/v1/completions"}

    results, metrics = profile.submit_with_backpressure(
        ray_module=_ImmediateRay,
        actor_pools=actor_pools,
        endpoint_urls=endpoint_urls,
        batches=self.batches,
        max_inflight=2,
        method_name="execute_batch",
    )

    assert len(results) == 3
    assert [call[0] for call in actors[0].execute_batch.calls] == [
        self.batches[0],
        self.batches[2],
    ]
    assert [call[0] for call in actors[1].execute_batch.calls] == [
        self.batches[1],
    ]
    assert metrics["endpoint_count"] == 1
    assert metrics["actor_worker_count"] == 2
```

- [ ] **Step 2: Run the actor scheduling class and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py -q
```

Expected: FAIL because `submit_with_backpressure` still consumes a flat actor list.

- [ ] **Step 3: Refactor topology and submitter construction**

```python
endpoint_ids = list(actor_pools)
topology = _endpoint_topology(
    endpoint_ids,
    [endpoint_urls[item] for item in endpoint_ids],
    pool_ids=routing_config.get("pool_ids") if routing_config else None,
    gpu_ids=routing_config.get("gpu_ids") if routing_config else None,
)
pool_submitters = {
    endpoint_id: ActorWorkerPoolSubmitter(actors, method_name)
    for endpoint_id, actors in actor_pools.items()
}
submitters = {
    endpoint_id: submitter
    for endpoint_id, submitter in pool_submitters.items()
}
```

Add `endpoint_count`, `actor_worker_count`, and semicolon-separated
`actor_worker_submission_counts` to scheduler metrics.

- [ ] **Step 4: Run focused scheduling tests and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py code\tests\test_scheduler.py code\tests\test_ray_adapter.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "refactor: separate endpoints from Ray actors"
```

---

### Task 5: Apply Ray options during task and actor construction

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Consumes: `RayWorkerOptions`.
- Produces actor pools with exactly `workers_per_endpoint` handles.
- Applies task options once to each remote function definition.

- [ ] **Step 1: Write failing remote-option tests**

```python
def test_http_actor_definition_receives_safe_ray_options() -> None:
    ray = _RecordingRay()
    options = RayWorkerOptions(0.25, actor_max_concurrency=4)

    remote_cls = profile._remote_actor_class(
        ray, CompatibleHTTPCompletionActor, options
    )

    assert remote_cls.options_calls == [{
        "num_cpus": 0.25,
        "num_gpus": 0,
        "max_concurrency": 4,
        "max_restarts": 0,
        "max_task_retries": 0,
    }]


def test_http_task_definition_disables_retry() -> None:
    ray = _RecordingRay()
    options = RayWorkerOptions(0.25)
    remote_fn = profile._remote_task(
        ray, compatible_http_complete_batch, options
    )
    assert remote_fn.options_calls[0]["max_retries"] == 0
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py -q
```

Expected: FAIL because `_remote_actor_class` and `_remote_task` do not exist.

- [ ] **Step 3: Implement the two small construction helpers**

```python
def _remote_actor_class(ray_module, actor_cls, worker_options):
    return ray_module.remote(actor_cls).options(
        **worker_options.actor_options()
    )


def _remote_task(ray_module, task_fn, worker_options):
    return ray_module.remote(task_fn).options(
        **worker_options.task_options()
    )
```

Build one completion pool for each URL:

```python
actor_pools = {
    f"endpoint-{endpoint_index}": [
        RayCompletionActor.remote(endpoint_url, *constructor_args)
        for _ in range(workers_per_endpoint)
    ]
    for endpoint_index, endpoint_url in enumerate(endpoint_urls)
}
```

Construct the other HTTP actor definitions through the same tested helper,
without duplicating Ray option dictionaries:

```python
RayEmbeddingActor = _remote_actor_class(
    ray_module,
    CompatibleHTTPEmbeddingActor,
    worker_options,
)
RayOllamaCompletionActor = _remote_actor_class(
    ray_module,
    OllamaCompletionActor,
    worker_options,
)
```

- [ ] **Step 4: Run focused and CLI tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py code\tests\test_model_backends.py -q
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py --help
```

Expected: tests PASS; help lists all three new options.

- [ ] **Step 5: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: apply Ray worker resource options"
```

---

### Task 6: Result schema, documentation, and full verification

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/README.md`
- Modify: `code/scripts/README.md`
- Modify: `code/INFRA_STATUS.md`
- Modify: `learning/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Result fields:
  `ray_version`, `actor_workers_per_endpoint`,
  `ray_actor_max_concurrency`, `ray_worker_num_cpus`,
  `ray_worker_num_gpus`, `endpoint_count`, `actor_worker_count`,
  and `actor_worker_submission_counts`.

- [ ] **Step 1: Write a failing dry-run schema assertion**

```python
def test_dry_run_records_ray_execution_contract() -> None:
    row = profile.dry_run_row(
        profile.parse_args([
            "--executor", "ray_actor",
            "--actor-workers-per-endpoint", "4",
            "--ray-actor-max-concurrency", "2",
            "--ray-worker-num-cpus", "0.25",
        ])
    )
    assert row["actor_workers_per_endpoint"] == 4
    assert row["ray_actor_max_concurrency"] == 2
    assert row["ray_worker_num_cpus"] == 0.25
    assert row["ray_worker_num_gpus"] == 0
```

- [ ] **Step 2: Run the schema test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\test_postgres_profile_scheduling.py -q
```

Expected: FAIL because the fields are absent.

- [ ] **Step 3: Add fields without changing unrelated metric names**

Populate dry-run and real rows from resolved configuration. Read
`ray.__version__` only after Ray is available; use an empty string for Python
executor rows.

- [ ] **Step 4: Update documentation**

Document:

```text
service endpoint != Ray actor worker
effective actor concurrency =
  endpoints * workers per endpoint * actor max concurrency
HTTP workers reserve 0 Ray GPUs
formal completion retries remain disabled
```

Mark multi-GPU performance testing as pending independent GPU endpoints.

- [ ] **Step 5: Run full verification**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m compileall -q code
.conda\pg-ai-profile\python.exe -m pytest code\tests -q
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py --dry-run --executor ray_actor --actor-workers-per-endpoint 4 --ray-actor-max-concurrency 2 --ray-worker-num-cpus 0.25 --output .test-tmp\ray_actor_dry_run.csv
```

Expected:

- compileall succeeds;
- all tests pass;
- dry-run CSV has one row and the Ray GPU field is `0`;
- no files under the inaccessible historical temp directory are modified.

- [ ] **Step 6: Commit**

```powershell
git add code/scripts/postgres_ai_operator_profile.py code/README.md code/scripts/README.md code/INFRA_STATUS.md learning/README.md PROJECT_INDEX.md PROJECT_LOG.md code/tests/test_postgres_profile_scheduling.py
git commit -m "docs: record Ray execution contract"
```
