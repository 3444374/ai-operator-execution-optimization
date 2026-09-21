# Request Lifecycle and Scenario Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add exactly-once per-prompt lifecycle traces, client-observed E2E/SLO metrics, and a seeded interleaved scenario runner to the existing PostgreSQL → Daft → Arrow → Ray → vLLM execution infra.

**Architecture:** The typed scheduler records one immutable lifecycle event per Ray submission. Arrival replay records one immutable seed per complete input row when its pending batch closes. The profiler joins row seeds, submission lifecycle events, and backend results into `requests.csv`; a separate pure scheduling module produces deterministic warm-up/formal scenario order for a thin subprocess runner.

**Tech Stack:** Python 3.10, dataclasses, PyArrow, Daft 0.7.20, Ray 2.56, PostgreSQL 18.4, pgvector 0.8.2, vLLM 0.25.1, `unittest`.

## Global Constraints

- Formal execution remains `PostgreSQL -> Daft -> Arrow -> Ray task/actor -> real vLLM`.
- Do not modify vLLM, Ray, or Daft internal schedulers.
- Every input row remains one complete model request; never split prompt contents.
- Request tracing initially requires `--arrival-replay` and the typed static/AIMD/EWMA/PID scheduler path.
- Legacy `scheduling_policy=queue_adaptive` must fail validation when request tracing is enabled; do not fabricate missing lifecycle events.
- Batch response timing is persisted with `latency_granularity=submission`; it is client-observed per-row E2E, not internal per-sequence completion timing.
- All formal CSV rows include actual `server_version` and `pgvector_version`.
- Every production behavior change follows RED → GREEN; fake backends are test-only.
- Do not change batching membership, flush-window selection, controller laws, routing policy, or writeback behavior in this plan.
- Keep `.superpowers/` untracked and do not merge `main`.

---

### Task 1: Typed Submission Lifecycle Events

**Files:**
- Modify: `code/src/scheduling/models.py`
- Modify: `code/src/scheduling/scheduler.py`
- Modify: `code/src/scheduling/__init__.py`
- Test: `code/tests/scheduling/test_scheduler.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class SubmissionLifecycleEvent:
    submission_id: str
    pool_id: str
    endpoint_id: str
    gpu_id: str
    submit_epoch_s: float
    completion_epoch_s: float
    status: Literal["completed", "failed"]
    error: str = ""
```

- `SchedulerResult.submission_events` is ordered by original submission order, exactly like `SchedulerResult.completions`.
- `SynchronousScheduler(..., epoch_clock: Callable[[], float] = time.time)` permits deterministic lifecycle tests.
- Consumes existing `PayloadEnvelope`, routing decisions, topology snapshots, and adapter completions.

- [ ] **Step 1: Write failing scheduler lifecycle tests**

Add a deterministic epoch clock and assertions to `code/tests/scheduling/test_scheduler.py`:

```python
class SequenceClock:
    def __init__(self, values: list[float]) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


def test_scheduler_records_submission_lifecycle_in_source_order(self) -> None:
    adapter = FakeSubmissionAdapter()
    scheduler = SynchronousScheduler(
        admission=StaticAdmissionController(limit=2),
        router=RoundRobinEndpointRouter(),
        adapter=adapter,
        pool_id="default",
        epoch_clock=SequenceClock([10.0, 11.0, 20.0, 21.0]),
    )

    result = scheduler.run([envelope(0), envelope(1)], topology())

    self.assertEqual(
        [event.submission_id for event in result.submission_events],
        ["r0", "r1"],
    )
    self.assertEqual(
        [
            (
                event.pool_id,
                event.endpoint_id,
                event.gpu_id,
                event.submit_epoch_s,
                event.completion_epoch_s,
                event.status,
            )
            for event in result.submission_events
        ],
        [
            ("default", "e1", "0", 10.0, 20.0, "completed"),
            ("default", "e2", "0", 11.0, 21.0, "completed"),
        ],
    )


def test_scheduler_records_failed_submission_without_retry(self) -> None:
    scheduler = SynchronousScheduler(
        StaticAdmissionController(1),
        RoundRobinEndpointRouter(),
        FailingAdapter(),
        "default",
        epoch_clock=SequenceClock([10.0, 20.0]),
    )

    result = scheduler.run([envelope(0)], topology())

    self.assertEqual(result.submission_events[0].status, "failed")
    self.assertEqual(
        result.submission_events[0].error,
        "synthetic failure",
    )
```

The failing adapter used by the second test must be a module-level test helper so both the existing failure test and lifecycle test use the same behavior.

- [ ] **Step 2: Run tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py -v
```

Expected: the new tests fail because `epoch_clock` and `submission_events` do not exist.

- [ ] **Step 3: Implement immutable lifecycle events**

Add to `code/src/scheduling/models.py`:

```python
@dataclass(frozen=True)
class SubmissionLifecycleEvent:
    submission_id: str
    pool_id: str
    endpoint_id: str
    gpu_id: str
    submit_epoch_s: float
    completion_epoch_s: float
    status: Literal["completed", "failed"]
    error: str = ""

    def __post_init__(self) -> None:
        if not self.submission_id or not self.pool_id or not self.endpoint_id:
            raise ValueError(
                "submission_id, pool_id, and endpoint_id must be non-empty"
            )
        if (
            not math.isfinite(self.submit_epoch_s)
            or not math.isfinite(self.completion_epoch_s)
            or self.submit_epoch_s < 0
            or self.completion_epoch_s < self.submit_epoch_s
        ):
            raise ValueError("submission lifecycle timestamps are invalid")
```

Extend `SchedulerResult`:

```python
submission_events: tuple[SubmissionLifecycleEvent, ...]
```

Update `SynchronousScheduler`:

```python
def __init__(
    self,
    admission: AdmissionPolicy,
    router: EndpointRouter,
    adapter: SubmissionAdapter,
    pool_id: str,
    *,
    pool_router: PoolRouter | None = None,
    epoch_clock: Callable[[], float] = time.time,
):
    ...
    self.epoch_clock = epoch_clock
```

During submission, save `(pool_id, endpoint_id, gpu_id, submit_epoch_s)` by
`request_id`. Resolve `gpu_id` from the selected endpoint in the supplied
`TopologySnapshot`. Immediately after `adapter.wait_one()` returns, call the
epoch clock once and create the lifecycle event. Sort lifecycle events by the
same `submission_order` mapping used for completions.

Do not call the epoch clock for timing `submit_s`, `wait_s`, or `fanin_s`;
those existing durations continue to use `perf_counter`.

- [ ] **Step 4: Run focused and scheduling regression tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py -v
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: all tests pass and existing completion ordering remains unchanged.

- [ ] **Step 5: Commit Task 1**

```powershell
git add code/src/scheduling/models.py code/src/scheduling/scheduler.py code/src/scheduling/__init__.py code/tests/scheduling/test_scheduler.py
git commit -m "feat: record submission lifecycle events"
```

---

### Task 2: Row Lifecycle Seeds and Request Trace Assembly

**Files:**
- Create: `code/src/scheduling/lifecycle.py`
- Modify: `code/src/scheduling/__init__.py`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`
- Create: `code/tests/scheduling/test_request_lifecycle.py`
- Modify: `code/tests/observability/test_postgres_profile_scheduling.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class RequestLifecycleSeed:
    request_id: str
    submission_id: str
    doc_id: str
    prompt_tokens: int
    estimated_output_tokens: int
    prefix_key: str
    arrival_epoch_s: float
    flush_epoch_s: float


@dataclass(frozen=True)
class RequestTraceRow:
    request_id: str
    submission_id: str
    doc_id: str
    pool_id: str
    endpoint_id: str
    gpu_id: str
    prompt_tokens: int
    estimated_output_tokens: int
    client_estimated_output_tokens: int | None
    actual_output_tokens: int | None
    output_token_source: Literal[
        "submission_aggregate_unavailable",
        "endpoint_request",
    ]
    total_tokens: int | None
    prefix_key: str
    status: str
    error_type: str
    arrival_epoch_s: float
    flush_epoch_s: float
    submit_epoch_s: float
    service_start_epoch_s: float | None
    completion_epoch_s: float
    buffer_s: float
    submit_to_service_s: float | None
    service_s: float | None
    service_clock_domain: Literal["backend"]
    e2e_s: float
    latency_granularity: Literal["submission", "request"]
    slo_target_s: float | None
    slo_met: bool | None
```

- `build_request_trace_rows(seeds, submission_events, results, slo_target_s)`
  validates exactly-once joins and returns rows sorted by seed order.
- `_arrival_replay_envelopes(..., lifecycle_seed_sink, epoch_clock=time.time)`
  appends one seed for every complete row.

- [ ] **Step 1: Write failing lifecycle model and join tests**

Create `code/tests/scheduling/test_request_lifecycle.py` with tests covering:

```python
def test_build_request_trace_rows_preserves_row_arrival_and_shared_batch_timing():
    seeds = [
        RequestLifecycleSeed(
            "job:row:1", "job:batch:0", "1", 10, 4, "p",
            100.000, 100.025,
        ),
        RequestLifecycleSeed(
            "job:row:2", "job:batch:0", "2", 20, 4, "p",
            100.010, 100.025,
        ),
    ]
    events = [
        SubmissionLifecycleEvent(
            "job:batch:0", "default", "task-0", "0",
            100.030, 100.300, "completed",
        )
    ]
    rows = build_request_trace_rows(
        seeds,
        events,
        service_by_submission_id={
            "job:batch:0": SubmissionServiceTiming(
                "job:batch:0", 100.040, 100.290
            ),
        },
        client_estimated_output_tokens_by_doc_id={"1": 2, "2": 1},
        actual_output_tokens_by_doc_id={},
        slo_target_s=0.250,
    )

    assert [row.doc_id for row in rows] == ["1", "2"]
    assert [row.client_estimated_output_tokens for row in rows] == [2, 1]
    assert [row.actual_output_tokens for row in rows] == [None, None]
    assert {
        row.output_token_source for row in rows
    } == {"submission_aggregate_unavailable"}
    assert [row.buffer_s for row in rows] == [0.025, 0.015]
    assert [row.e2e_s for row in rows] == [0.300, 0.290]
    assert [row.slo_met for row in rows] == [False, False]
    assert all(row.latency_granularity == "submission" for row in rows)
```

Also add independent tests that reject:

- duplicate seed `request_id`;
- duplicate or missing document IDs in backend results;
- missing submission lifecycle events;
- negative timestamp ordering;
- backend output count different from `doc_id` count;
- failed submission producing a trace row with status/error and blank service
  timing rather than a fabricated successful timing.

- [ ] **Step 2: Verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_request_lifecycle.py -v
```

Expected: import failure because `src.scheduling.lifecycle` does not exist.

- [ ] **Step 3: Implement lifecycle dataclasses and deterministic join**

Create `code/src/scheduling/lifecycle.py`.

Use the existing `text_token_count()` function from `src.model_backends` only
at the profiler boundary to derive explicitly named client estimates. The
current compatible endpoint exposes only aggregate submission usage, so it
must not be divided into fabricated per-request actual token counts. Keep
`scheduling/lifecycle.py` engine-independent by accepting:

```python
client_estimated_output_tokens_by_doc_id: Mapping[str, int]
actual_output_tokens_by_doc_id: Mapping[str, int]
```

Therefore implement the pure join as:

```python
def build_request_trace_rows(
    seeds: Sequence[RequestLifecycleSeed],
    submission_events: Sequence[SubmissionLifecycleEvent],
    service_by_submission_id: Mapping[str, SubmissionServiceTiming],
    client_estimated_output_tokens_by_doc_id: Mapping[str, int],
    actual_output_tokens_by_doc_id: Mapping[str, int],
    *,
    slo_target_s: float | None,
) -> tuple[RequestTraceRow, ...]:
    ...
```

Add:

```python
@dataclass(frozen=True)
class SubmissionServiceTiming:
    submission_id: str
    service_start_epoch_s: float | None
    service_end_epoch_s: float | None
```

For successful submissions, require finite ordered service timestamps. For
failed submissions, require both service timestamps to be `None`.
`actual_output_tokens_by_doc_id` may be empty; when it is unavailable,
`actual_output_tokens` and `total_tokens` are `None` and
`output_token_source` is `submission_aggregate_unavailable`. Only a genuine
per-request endpoint usage value may set `output_token_source=endpoint_request`.

Use a small tolerance of `1e-6` seconds for epoch ordering comparisons. Clamp
only differences in `[-1e-6, 0)` to zero; reject larger negative differences.
Do not silently repair arbitrary timing errors.

- [ ] **Step 4: Write the failing replay seed test**

Add to `code/tests/observability/test_postgres_profile_scheduling.py`:

```python
def test_arrival_replay_emits_one_lifecycle_seed_per_complete_row(self):
    args = SimpleNamespace(
        ray_batch_rows=8,
        batching_policy="fixed_rows",
        token_budget=0,
        flush_policy="fixed_timeout",
        flush_timeout_ms=25.0,
        flush_max_wait_ms=50.0,
        max_inflight=8,
        arrival_time_scale=0.001,
        completion_max_tokens=4,
        _replay_clock=_DeterministicReplayClock(),
        _replay_epoch_clock=iter([1_000.0, 1_000.025]).__next__,
    )
    table = pa.table(
        {
            "doc_id": [1, 2],
            "prompt_tokens": [10, 20],
            "arrival_time_s": [5.0, 15.0],
            "prefix_key": ["p", "p"],
        }
    )
    seeds = []

    envelopes = list(
        profile._arrival_replay_envelopes(
            [table],
            args,
            job_id="job",
            operator="ai_complete",
            service_observation=lambda: ReplayServiceObservation(
                fresh=False,
                running=None,
                waiting=None,
                kv_usage=None,
            ),
            trace_sink=[],
            lifecycle_seed_sink=seeds,
        )
    )

    self.assertEqual(len(envelopes), 1)
    self.assertEqual([seed.request_id for seed in seeds], [
        "job:row:1",
        "job:row:2",
    ])
    self.assertEqual({seed.submission_id for seed in seeds}, {"job:batch:0"})
    self.assertEqual(
        [seed.arrival_epoch_s for seed in seeds],
        [1_000.0, 1_000.01],
    )
    self.assertEqual(
        [seed.flush_epoch_s for seed in seeds],
        [1_000.025, 1_000.025],
    )
```

- [ ] **Step 5: Verify replay seed test RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: failure because `lifecycle_seed_sink` is not accepted.

- [ ] **Step 6: Emit row lifecycle seeds from replay**

Modify `_arrival_replay_envelopes`:

```python
def _arrival_replay_envelopes(
    tables,
    args,
    job_id,
    operator,
    service_observation,
    trace_sink,
    lifecycle_seed_sink=None,
):
```

Inside the returned `replay()` generator:

1. capture `replay_start_epoch_s` from
   `getattr(args, "_replay_epoch_clock", None) or time.time`;
2. derive the first source arrival as the origin;
3. when `close_batch` creates the `PayloadEnvelope`, call the epoch clock once
   for `flush_epoch_s`;
4. append one seed per `pending.rows`;
5. compute row arrival epoch using the same `arrival_time_scale` as the
   monotonic replay deadline.

No seed is emitted before a batch closes. If envelope construction fails,
emit no partial seeds.

- [ ] **Step 7: Verify Task 2 GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_request_lifecycle.py -v
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_runtime_batching.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: all tests pass.

- [ ] **Step 8: Commit Task 2**

```powershell
git add code/src/scheduling/lifecycle.py code/src/scheduling/__init__.py code/scripts/profiling/postgres_ai_operator_profile.py code/tests/scheduling/test_request_lifecycle.py code/tests/observability/test_postgres_profile_scheduling.py
git commit -m "feat: assemble per-request lifecycle traces"
```

---

### Task 3: Profiler Request CSV, SLO, and Run Metrics

**Files:**
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`
- Modify: `code/scripts/README.md`
- Modify: `code/tests/observability/test_postgres_profile_scheduling.py`
- Modify: `code/tests/scheduling/test_scheduling_daft_ray_contract.py`

**Interfaces:**
- New CLI:

```text
--request-trace-output PATH
--request-slo-ms FLOAT
```

- New run fields:

```text
request_trace_path
request_trace_events
request_e2e_s_p50
request_e2e_s_p95
request_e2e_s_p99
request_slo_target_ms
request_slo_violation_ratio
request_slo_goodput_per_s
latency_granularity
```

- `_write_request_trace()` writes schema version 1.
- Existing submit functions accept an optional `submission_lifecycle_sink` and
  do not change their two-value return contract.

- [ ] **Step 1: Write failing CLI validation tests**

Add cases to the existing profiler validation table:

```python
(
    ["--dry-run", "--request-trace-output", "tmp/requests.csv"],
    "request tracing requires --arrival-replay",
),
(
    [
        "--dry-run",
        "--arrival-replay",
        "--data-source",
        "daft_postgres",
        "--source-order",
        "arrival_time",
        "--request-trace-output",
        "tmp/requests.csv",
        "--scheduling-policy",
        "queue_adaptive",
    ],
    "request tracing requires the typed scheduler",
),
(
    [
        "--dry-run",
        "--arrival-replay",
        "--data-source",
        "daft_postgres",
        "--source-order",
        "arrival_time",
        "--request-slo-ms",
        "-1",
    ],
    "request-slo-ms must be non-negative",
),
```

Also assert dry-run output records explicit request trace and SLO settings.

- [ ] **Step 2: Write failing request trace writer test**

Add a writer test with two `RequestTraceRow` instances. Assert exact columns:

```text
schema_version, experiment_id, phase, repeat_index, scenario_id, random_seed,
job_id, server_version, pgvector_version, request_index, request_id,
submission_id, doc_id, pool_id, endpoint_id, gpu_id, prompt_tokens,
estimated_output_tokens, client_estimated_output_tokens, actual_output_tokens,
output_token_source, total_tokens, prefix_key, status, error_type,
arrival_epoch_s, flush_epoch_s, submit_epoch_s,
service_start_epoch_s, completion_epoch_s, buffer_s, submit_to_service_s,
service_s, service_clock_domain, e2e_s, latency_granularity, slo_target_s,
slo_met
```

Assert versions, scenario identity, and numeric values survive CSV round-trip.

- [ ] **Step 3: Verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: parser/writer tests fail for missing CLI and writer.

- [ ] **Step 4: Add profiler CLI, validation, and writer**

Add:

```python
parser.add_argument("--request-trace-output", default=None)
parser.add_argument("--request-slo-ms", type=float, default=0.0)
parser.add_argument("--scenario-id", default="manual")
parser.add_argument("--random-seed", type=int, default=0)
```

Validation:

- request trace requires arrival replay;
- request trace rejects legacy `scheduling_policy=queue_adaptive`;
- SLO must be finite and non-negative;
- `scenario_id` must be non-empty.

Implement `_write_request_trace()` using `append_metrics()`. Use empty strings
for unavailable service timestamps/SLO fields on failed requests.

- [ ] **Step 5: Propagate scheduler lifecycle without changing result tuples**

Add optional `submission_lifecycle_sink` parameters through:

```text
submit_with_backpressure
submit_ray_tasks
_run_static_scheduler
_run_dynamic_scheduler
_run_scheduler
```

Immediately after `scheduler.run()`, append `result.submission_events` to the
provided sink. Legacy paths reject a non-`None` sink. Existing callers that do
not request tracing remain unchanged.

In `run_once()`:

1. create `request_lifecycle_seeds` and `submission_lifecycle_events`;
2. pass both sinks through replay and scheduler;
3. map each ordered operator result to its ordered submission event;
4. compute explicitly labelled client-estimated per-doc output tokens from
   `output_text` using `text_token_count()`; leave per-request actual endpoint
   usage empty until a backend exposes it;
5. build and write request rows;
6. derive run metrics from successful request rows.

Goodput:

```python
request_slo_goodput_per_s = (
    sum(row.slo_met is True for row in request_rows) / e2e_s
    if request_slo_ms > 0
    else 0.0
)
```

Use the existing percentile helper behavior from `batch_result_stats`; do not
introduce a second percentile convention.

- [ ] **Step 6: Extend the real Daft-Ray contract**

Update `test_scheduling_daft_ray_contract.py` to run four arrival-replayed rows
through both Ray task and actor paths with request tracing enabled. Assert:

- four request rows;
- four unique `doc_id` and `request_id` values;
- one or more submission IDs;
- all request rows map to existing submission IDs;
- all timings are non-negative and ordered;
- `latency_granularity == "submission"`.

The contract may use the local deterministic backend because it validates
Daft/Arrow/Ray behavior, not GPU performance.

- [ ] **Step 7: Verify Task 3 GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py -v
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_request_lifecycle.py -v
```

Expected: all tests pass.

- [ ] **Step 8: Document timing semantics and commit**

Document:

- request vs submission identity;
- arrival epoch reconstruction;
- submission-granularity limitation;
- SLO goodput formula;
- request tracing supported paths.

Commit:

```powershell
git add code/scripts/profiling/postgres_ai_operator_profile.py code/scripts/README.md code/tests/observability/test_postgres_profile_scheduling.py code/tests/scheduling/test_scheduling_daft_ray_contract.py
git commit -m "feat: write request lifecycle metrics"
```

---

### Task 4: Seeded Interleaved Scenario Runner

**Files:**
- Create: `code/src/experiment_scenarios.py`
- Create: `code/scripts/experiments/run_ai_operator_scenarios.py`
- Create: `code/tests/experiments/test_experiment_scenarios.py`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`
- Modify: `code/scripts/README.md`

**Interfaces:**
- Scenario config JSON:

```json
{
  "schema_version": 1,
  "experiment_id": "adaptive_flush_randomized_512",
  "seed": 20260725,
  "warmup_runs_per_scenario": 1,
  "formal_repeats": 5,
  "common_args": [
    "--database-url", "postgresql://postgres:postgres@localhost:5432/ai_operator",
    "--data-source", "daft_postgres",
    "--source-workload-name", "sharegpt_burstgpt"
  ],
  "scenarios": [
    {
      "scenario_id": "fixed_timeout",
      "args": ["--flush-policy", "fixed_timeout"]
    },
    {
      "scenario_id": "queue_adaptive",
      "args": ["--flush-policy", "queue_adaptive"]
    }
  ]
}
```

- Pure API:

```python
@dataclass(frozen=True)
class ScheduledScenarioRun:
    scenario_id: str
    phase: Literal["warmup", "formal"]
    repeat_index: int
    order_index: int
    random_seed: int


def build_scenario_schedule(
    scenario_ids: Sequence[str],
    warmup_runs_per_scenario: int,
    formal_repeats: int,
    seed: int,
) -> tuple[ScheduledScenarioRun, ...]:
    ...
```

- Profiler single-run CLI:

```text
--run-phase {warmup,formal}
--run-repeat-index INT
```

Both arguments must be supplied together. When present, `main()` runs exactly
one phase/repeat and ignores `--warmup-runs/--repeats`.

- [ ] **Step 1: Write failing deterministic schedule tests**

Create `code/tests/experiments/test_experiment_scenarios.py`:

```python
def test_schedule_is_reproducible_and_interleaves_formal_scenarios():
    first = build_scenario_schedule(
        ["immediate", "fixed", "adaptive"],
        warmup_runs_per_scenario=1,
        formal_repeats=3,
        seed=7,
    )
    second = build_scenario_schedule(
        ["immediate", "fixed", "adaptive"],
        warmup_runs_per_scenario=1,
        formal_repeats=3,
        seed=7,
    )

    assert first == second
    assert len(first) == 12
    assert [item.phase for item in first[:3]] == ["warmup"] * 3
    for repeat_index in (1, 2, 3):
        group = [
            item for item in first
            if item.phase == "formal"
            and item.repeat_index == repeat_index
        ]
        assert sorted(item.scenario_id for item in group) == [
            "adaptive", "fixed", "immediate"
        ]
```

Also test:

- duplicate/empty scenario IDs rejected;
- negative warm-up or formal repeat counts rejected;
- same seed same order;
- a different seed changes at least one formal ordering for the three-scenario
  test vector;
- `order_index` is contiguous.

- [ ] **Step 2: Verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\experiments\test_experiment_scenarios.py -v
```

Expected: import failure because the module does not exist.

- [ ] **Step 3: Implement the pure schedule**

Use `random.Random(seed)`. Warm-ups preserve config order. For each formal
repeat, copy and shuffle scenario IDs once. Store the original experiment seed
in every scheduled row; do not derive hidden process-global randomness.

- [ ] **Step 4: Write failing single-run profiler tests**

Add parser/main tests:

```python
args = profile.parse_args([
    "--dry-run",
    "--run-phase", "formal",
    "--run-repeat-index", "4",
])
assert list(profile.iter_requested_runs(args)) == [("formal", 4)]
```

Reject supplying only one single-run argument and reject repeat index `< 1`.

- [ ] **Step 5: Add explicit single-run profiler mode**

Introduce:

```python
def iter_requested_runs(args: argparse.Namespace):
    if args.run_phase is not None:
        yield args.run_phase, args.run_repeat_index
        return
    yield from iter_run_phases(args.warmup_runs, args.repeats)
```

`main()` uses only this function. Existing behavior remains byte-for-byte
equivalent when single-run arguments are absent.

- [ ] **Step 6: Implement the subprocess runner**

`run_ai_operator_scenarios.py` must:

1. parse and validate the JSON config;
2. call `build_scenario_schedule`;
3. before each run, GET `/health` and scrape metrics until health=200 and
   running=waiting=0, with a finite CLI timeout;
4. build a subprocess argument list without `shell=True`;
5. pass scenario ID, seed, explicit phase/repeat, output paths, common args,
   and scenario args;
6. write stdout/stderr per scheduled run;
7. stop on nonzero subprocess result or missing expected CSV row;
8. append an incident entry containing run identity and exit code;
9. write manifest atomically after every completed scheduled run.

Before persisting commands or configuration, redact values associated with
credential-bearing flags whose normalized names contain `api-key`,
`auth-token`, `secret`, or `password`. Do not redact experimental controls such
as `--token-budget` or `--completion-max-tokens`, because the manifest must
remain reproducible. Database URLs are stored only after their password
component is replaced with `***`.

CLI:

```text
--config PATH
--profiler PATH
--python-executable PATH
--output-dir PATH
--health-url URL
--metrics-url URL
--idle-timeout-s FLOAT
```

The runner never restarts containers. A non-idle service at timeout is a
failed run requiring external recovery.

- [ ] **Step 7: Test runner argument and failure behavior**

Use a temporary fake executable script in the test workspace that records its
arguments and returns a configured exit code. Do not call a real model in unit
tests. Assert:

- exact phase/repeat/scenario/seed arguments;
- no shell invocation;
- stop after first failure;
- manifest contains completed and failed run identities;
- same config and seed produce the same invocation order.

- [ ] **Step 8: Verify Task 4 GREEN and commit**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\experiments\test_experiment_scenarios.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Commit:

```powershell
git add code/src/experiment_scenarios.py code/scripts/experiments/run_ai_operator_scenarios.py code/tests/experiments/test_experiment_scenarios.py code/scripts/profiling/postgres_ai_operator_profile.py code/scripts/README.md
git commit -m "feat: run seeded scheduling scenarios"
```

---

### Task 5: Full Verification and Real Infra Gate

**Files:**
- Create: `experiments/results/request_lifecycle_gate_20260725/README.md`
- Create: `experiments/results/request_lifecycle_gate_20260725/manifest.json`
- Create: `experiments/results/request_lifecycle_gate_20260725/*.csv`
- Modify: `code/README.md`
- Modify: `experiments/results/README.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Consumes the completed profiler and scenario runner.
- Produces a 64-row real-component gate; it is infrastructure validation, not
  a new policy performance conclusion.

- [ ] **Step 1: Run fresh full verification**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py" -v
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts
git diff --check
```

Expected: all tests pass, including real local Daft→Arrow→Ray task/actor
contracts; compile and diff checks succeed.

- [ ] **Step 2: Run the 64-row real request lifecycle gate**

Use:

```text
PostgreSQL 18.4 / pgvector 0.8.2
Daft source
Ray task
compatible_http vLLM Qwen2.5-1.5B
ShareGPT/BurstGPT arrival order
arrival scale 0.0001
token budget 6144
static K_max 8
fixed timeout 25 ms
completion max tokens 16
writeback none
64 rows
```

Run fixed-timeout and queue-adaptive flush once through the new scenario
runner. Write run, request, submission, flush, resource, stdout, stderr, and
manifest artifacts.

- [ ] **Step 3: Audit gate invariants**

Fail the gate unless:

- both run rows are `ok`;
- vLLM success delta is 64 for each scenario;
- request trace has 64 rows and 64 unique request/doc IDs per scenario;
- every request submission ID exists in submission trace;
- every lifecycle timestamp is finite and phase ordering is valid;
- all E2E values are non-negative;
- request E2E P50/P95/P99 in the run CSV exactly match recomputation from
  request rows within `1e-6`;
- every CSV row contains server and pgvector versions;
- seeded schedule and manifest agree;
- vLLM ends with running=waiting=0.

- [ ] **Step 4: Document claim boundary**

The report follows the seven-step experiment explanation structure. State:

- request E2E is client-observed;
- batch endpoint rows share submission completion;
- gate scale is not performance evidence;
- no fake formal data;
- no 2048 run in this infrastructure gate.

- [ ] **Step 5: Update project records**

Update the listed READMEs, experiment status, index, and log. Because
`experiments/plans/experiment_status_and_gaps.md` is a knowledge file, follow
the current user instruction regarding Wiki synchronization at session
close; do not silently sync.

- [ ] **Step 6: Final verification and commit**

Re-run:

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py" -q
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts
git diff --check
git status --short
```

Stage only validated code, tests, docs, and gate artifacts. Exclude
`.superpowers/`.

Commit:

```powershell
git commit -m "experiment: validate request lifecycle infra"
```

Keep `feat/runtime-scheduling-foundation` unmerged.

## Plan Self-Review

- **Design coverage:** The plan covers request/submission identity, replay
  arrival mapping, submission/client completion timing, request CSV, SLO
  metrics, seeded ordering, failure audit, real Daft/Ray contract, and a real
  vLLM gate.
- **Scope isolation:** It does not change batch membership, flush decisions,
  controller laws, routing decisions, output-cost estimation, or actor
  architecture.
- **Type consistency:** `submission_id` is the existing batch
  `BatchRequest.request_id`; row `request_id` is generated as
  `{job_id}:row:{doc_id}`. `SubmissionLifecycleEvent` and
  `RequestLifecycleSeed` use those names consistently.
- **Claim discipline:** Submission-granularity timing is never described as
  an internal per-sequence completion timestamp.
- **No placeholders:** Every task has explicit files, interfaces, tests,
  commands, failure conditions, and commit boundaries.
