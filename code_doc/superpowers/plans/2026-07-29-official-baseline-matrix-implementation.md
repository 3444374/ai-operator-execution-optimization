# Official Baseline Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a testable, same-request Chat Completions baseline harness for
direct vLLM, strong bounded HTTP, OceanBase `AI_COMPLETE`, Daft `prompt()`
Native/Ray, Ray Data HTTP Processor, and the existing project runtime.

**Architecture:** A small `src/baselines` package owns the immutable request
manifest, per-request result contract, deterministic endpoint assignment and
adapter interfaces. Each external system remains behind a focused adapter with
lazy optional imports. A thin CLI emits raw artifacts plus one normalized
summary; the existing project profiler is extended only enough to use the same
Chat Completions semantics.

**Tech Stack:** Python 3.12, dataclasses, JSONL/SHA-256, `httpx`, `psycopg`,
`PyMySQL`, Daft 0.7.20+, Ray 2.56.0+, vLLM 0.25.1 CLI, unittest, AutoDL dual
RTX 4090.

## Global Constraints

- Every production behavior starts with a failing test and a witnessed RED run.
- Do not modify vLLM, Ray scheduler internals, or Daft internals.
- Every manifest row is one complete request; never split one prompt into
  multiple model requests.
- All formal arms use `/v1/chat/completions`, one user message, temperature
  `0.0`, identical output cap and identical endpoint service configuration.
- Fixed endpoint assignment is computed once and stored in the manifest.
- Baselines are independently calibrated; defaults are never compared directly
  with an already tuned project arm.
- Raw artifacts are immutable. Normalization creates new files and never
  rewrites official-tool output.
- Formal validity requires exactly-once, zero failed rows, both endpoints used,
  endpoint predicted-work skew at most 2%, and empty vLLM queues after a run.
- No AI attribution in commit messages.
- Do not sync Wiki.

---

### Task 1: Add Chat Completions to the Existing Project Runtime

**Files:**
- Modify: `code/src/model_backends.py`
- Modify: `code/src/profiling/cli.py`
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Modify: `code/src/profiling/schema.py`
- Test: `code/tests/test_model_backends.py`
- Test: `code/tests/test_postgres_profile_scheduling.py`

**Interfaces:**
- Consumes: existing `CompletionEndpointResult`,
  `CompatibleHTTPCompletionActor`, and profiler CLI.
- Produces: `CompletionProtocol = Literal["completions", "chat_completions"]`
  and `--completion-protocol`.

- [ ] **Step 1: Write the failing backend request-body test**

```python
def _json_response(payload: dict):
    encoded = json.dumps(payload).encode("utf-8")

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self) -> bytes:
            return encoded

    return Response()


def test_chat_completion_endpoint_sends_one_message_per_prompt(self) -> None:
    response = _json_response(
        {
            "choices": [
                {
                    "index": 0,
                    "message": {"content": "answer"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 4,
                "completion_tokens": 2,
                "total_tokens": 6,
            },
        }
    )
    with patch("src.model_backends.request.urlopen", return_value=response) as urlopen:
        result = call_compatible_completion_endpoint(
            "http://localhost/v1/chat/completions",
            "model",
            ["question"],
            None,
            1.0,
            8,
            protocol="chat_completions",
            temperature=0.0,
        )
    sent = json.loads(urlopen.call_args.args[0].data.decode("utf-8"))
    self.assertEqual(
        sent["messages"],
        [{"role": "user", "content": "question"}],
    )
    self.assertNotIn("prompt", sent)
    self.assertEqual(result.outputs, ["answer"])
```

- [ ] **Step 2: Run the test and witness RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_model_backends.py
```

Expected: FAIL because `call_compatible_completion_endpoint()` does not accept
`protocol`.

- [ ] **Step 3: Implement the minimal protocol switch**

```python
CompletionProtocol = Literal["completions", "chat_completions"]

def _completion_request_body(
    model_name: str,
    prompts: list[str],
    max_tokens: int,
    protocol: CompletionProtocol,
) -> dict:
    if protocol == "completions":
        return {"model": model_name, "prompt": prompts, "max_tokens": max_tokens}
    if protocol == "chat_completions":
        if len(prompts) != 1:
            raise ValueError(
                "Chat Completions requires one complete prompt per HTTP request"
            )
        return {
            "model": model_name,
            "messages": [{"role": "user", "content": prompts[0]}],
            "max_tokens": max_tokens,
        }
    raise ValueError(f"Unknown completion protocol: {protocol}")
```

Thread `protocol` through the completion actor and task helper without changing
the default `completions` behavior.

- [ ] **Step 4: Write and witness the profiler CLI RED test**

```python
def test_chat_protocol_is_recorded_in_dry_run_summary(self) -> None:
    row = profile.run_once(
        profile.parse_args(
            [
                "--dry-run",
                "--operator",
                "ai_complete",
                "--completion-protocol",
                "chat_completions",
            ]
        ),
        "formal",
        0,
    )
    self.assertEqual(row["completion_protocol"], "chat_completions")
```

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
```

Expected: FAIL because the CLI and summary schema do not expose the field.

- [ ] **Step 5: Add the CLI field and propagate it**

```python
parser.add_argument(
    "--completion-protocol",
    choices=["completions", "chat_completions"],
    default="completions",
)
```

Pass `args.completion_protocol` to Python, Ray task, and Ray actor completion
paths, and add `completion_protocol` to the formal ordered summary schema.

- [ ] **Step 6: Run focused and full tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_model_backends.py
.conda\pg-ai-profile\python.exe code\tests\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests -p "test_*.py"
```

Expected: PASS.

- [ ] **Step 7: Commit**

```powershell
git add code/src/model_backends.py code/src/profiling/cli.py code/src/profiling/schema.py code/scripts/postgres_ai_operator_profile.py code/tests/test_model_backends.py code/tests/test_postgres_profile_scheduling.py
git commit -m "feat: support chat completion profiling"
```

---

### Task 2: Freeze the Request and Result Contracts

**Files:**
- Create: `code/src/baselines/__init__.py`
- Create: `code/src/baselines/contracts.py`
- Create: `code/src/baselines/manifests.py`
- Create: `code/src/baselines/results.py`
- Create: `code/tests/test_baseline_contracts.py`

**Interfaces:**
- Produces:
  - `ChatRequest`
  - `BaselineRequestResult`
  - `ManifestMetadata`
  - `read_manifest(path)`
  - `write_manifest(path, requests)`
  - `assign_endpoint_shards(requests, endpoint_count)`
  - `validate_results(requests, results)`
  - `summarize_results(requests, results)`

- [ ] **Step 0: Add explicit test fixtures**

```python
def sample_request(
    doc_id: int,
    *,
    prompt_tokens: int = 4,
    endpoint_index: int = 0,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=float(doc_id) / 10,
        prompt_tokens=prompt_tokens,
        max_output_tokens=8,
        estimated_output_tokens=8,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


def sample_result(
    doc_id: int,
    *,
    status: str = "completed",
    endpoint_index: int = 0,
) -> BaselineRequestResult:
    return BaselineRequestResult(
        doc_id=doc_id,
        endpoint_index=endpoint_index,
        status=status,
        error=None if status == "completed" else "model call failed",
        submitted_at_s=1.0,
        started_at_s=1.1,
        completed_at_s=1.2,
        input_tokens=4,
        output_tokens=1,
        output_text="ok" if status == "completed" else None,
        finish_reason="stop" if status == "completed" else None,
    )
```

- [ ] **Step 1: Write failing immutable-manifest tests**

```python
def test_manifest_round_trip_preserves_order_and_hash(self) -> None:
    requests = (
        ChatRequest(
            doc_id=1,
            prompt="first",
            arrival_time_s=0.0,
            prompt_tokens=3,
            max_output_tokens=8,
            estimated_output_tokens=8,
            source_row_hash="row-1",
            endpoint_index=0,
        ),
        ChatRequest(
            doc_id=2,
            prompt="second",
            arrival_time_s=0.1,
            prompt_tokens=5,
            max_output_tokens=8,
            estimated_output_tokens=8,
            source_row_hash="row-2",
            endpoint_index=1,
        ),
    )
    metadata = write_manifest(self.path, requests)
    self.assertEqual(read_manifest(self.path), requests)
    self.assertEqual(metadata.row_count, 2)
    self.assertEqual(len(metadata.sha256), 64)
```

```python
def test_manifest_rejects_duplicate_doc_id(self) -> None:
    request = sample_request(doc_id=1)
    with self.assertRaisesRegex(ValueError, "duplicate doc_id"):
        write_manifest(self.path, (request, request))
```

- [ ] **Step 2: Run and witness RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_baseline_contracts.py
```

Expected: import failure because `src.baselines` does not exist.

- [ ] **Step 3: Implement immutable dataclasses and canonical JSONL**

```python
@dataclass(frozen=True)
class ChatRequest:
    doc_id: int
    prompt: str
    arrival_time_s: float
    prompt_tokens: int
    max_output_tokens: int
    estimated_output_tokens: int
    source_row_hash: str
    endpoint_index: int

    @property
    def estimated_work(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens

    @property
    def messages(self) -> tuple[dict[str, str], ...]:
        return ({"role": "user", "content": self.prompt},)
```

Canonical JSON uses UTF-8, one object per line, `sort_keys=True`, compact
separators and a final newline. The SHA-256 covers the exact written bytes.

- [ ] **Step 4: Write failing deterministic-shard tests**

```python
def test_endpoint_assignment_is_deterministic_and_balances_work(self) -> None:
    requests = tuple(
        sample_request(doc_id=i, prompt_tokens=cost, endpoint_index=-1)
        for i, cost in enumerate([20, 18, 7, 6], start=1)
    )
    first = assign_endpoint_shards(requests, endpoint_count=2)
    second = assign_endpoint_shards(requests, endpoint_count=2)
    self.assertEqual(first, second)
    work = [0, 0]
    for request in first:
        work[request.endpoint_index] += request.estimated_work
    self.assertLessEqual(abs(work[0] - work[1]) / max(work), 0.02)
```

- [ ] **Step 5: Implement stable largest-work-first assignment**

Sort only for assignment by `(-estimated_work, doc_id)`, assign to the endpoint
with the smallest `(current_work, endpoint_index)`, then return requests in
their original manifest order with the computed `endpoint_index`.

- [ ] **Step 6: Write failing exactly-once tests**

```python
def test_result_validation_rejects_missing_duplicate_and_failed_rows(self) -> None:
    requests = (sample_request(1), sample_request(2))
    with self.assertRaisesRegex(ValueError, "exactly-once"):
        validate_results(
            requests,
            (
                sample_result(1, status="completed"),
                sample_result(1, status="completed"),
            ),
        )
```

- [ ] **Step 7: Implement result validation and normalized summary**

`BaselineRequestResult` contains doc/endpoint/status/error, submit/start/end
epochs, input/output tokens, output text and finish reason. Summary reports
row counts, failures, token totals, JCT, tokens/s, P50/P95/P99, endpoint counts,
endpoint predicted-work skew and exactly-once status.

- [ ] **Step 8: Run tests and commit**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_baseline_contracts.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests -p "test_*.py"
```

Expected: PASS.

Commit:

```powershell
git add code/src/baselines code/tests/test_baseline_contracts.py
git commit -m "feat: add immutable baseline contracts"
```

---

### Task 3: Add Strong Bounded HTTP and vLLM Bench Adapters

**Files:**
- Create: `code/src/baselines/async_http.py`
- Create: `code/src/baselines/vllm_bench.py`
- Create: `code/tests/test_baseline_async_http.py`
- Create: `code/tests/test_vllm_bench_adapter.py`
- Modify: `code/requirements.txt`

**Interfaces:**
- Consumes: Task 2 contracts.
- Produces:
  - `BoundedHttpConfig`
  - `run_bounded_http(requests, config, transport=None)`
  - `write_vllm_custom_dataset(path, requests)`
  - `build_vllm_bench_command(config)`

- [ ] **Step 1: Write the failing bounded-concurrency test**

```python
def sample_request(
    doc_id: int,
    *,
    endpoint_index: int,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=4,
        max_output_tokens=8,
        estimated_output_tokens=8,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


async def fake_transport(url: str, payload: dict) -> dict:
    nonlocal active, peak
    active += 1
    peak = max(peak, active)
    await asyncio.sleep(0.01)
    active -= 1
    return {
        "choices": [
            {
                "message": {"content": "ok"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 4,
            "completion_tokens": 1,
        },
    }

results = asyncio.run(
    run_bounded_http(
        tuple(sample_request(i, endpoint_index=0) for i in range(8)),
        BoundedHttpConfig(
            endpoint_urls=("http://ep0/v1/chat/completions",),
            model="model",
            concurrency_per_endpoint=2,
            timeout_s=30,
            api_key=None,
        ),
        transport=fake_transport,
    )
)
self.assertEqual(peak, 2)
self.assertEqual({row.status for row in results}, {"completed"})
```

- [ ] **Step 2: Run and witness RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_baseline_async_http.py
```

Expected: import failure because the adapter does not exist.

- [ ] **Step 3: Implement one-request-per-row bounded HTTP**

Use one `asyncio.Semaphore` per endpoint and one shared `httpx.AsyncClient`.
The payload is exactly:

```python
{
    "model": config.model,
    "messages": list(request.messages),
    "temperature": 0.0,
    "max_tokens": request.max_output_tokens,
}
```

Do not retry formal requests. Convert HTTP/JSON failures into failed result
rows so the validity gate can reject the run without losing evidence.

- [ ] **Step 4: Write failing vLLM command tests**

```python
def test_vllm_bench_uses_custom_chat_dataset_without_shuffle(self) -> None:
    command = build_vllm_bench_command(
        VllmBenchConfig(
            python_executable="/venv/bin/python",
            base_url="http://127.0.0.1:8000",
            model="qwen",
            dataset_path=Path("/tmp/shard0.jsonl"),
            result_dir=Path("/tmp/results"),
            result_filename="ep0.json",
            num_prompts=64,
            max_concurrency=32,
        )
    )
    self.assertIn("openai-chat", command)
    self.assertIn("--dataset-name", command)
    self.assertIn("custom", command)
    self.assertIn("--disable-shuffle", command)
    self.assertIn("--custom-output-len", command)
    self.assertIn("-1", command)
    self.assertIn("--save-detailed", command)
```

- [ ] **Step 5: Implement the vLLM custom dataset and command builder**

Each JSONL line is:

```python
{"prompt": request.prompt, "output_tokens": request.max_output_tokens}
```

The command uses:

```text
python -m vllm.benchmarks.serve
--backend openai-chat
--endpoint /v1/chat/completions
--dataset-name custom
--dataset-path <shard.jsonl>
--custom-output-len -1
--disable-shuffle
--request-rate inf
--max-concurrency <calibrated>
--save-result --save-detailed
```

- [ ] **Step 6: Declare direct dependencies**

Add:

```text
httpx>=0.27
```

to `code/requirements.txt`.

- [ ] **Step 7: Run tests and commit**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_baseline_async_http.py
.conda\pg-ai-profile\python.exe code\tests\test_vllm_bench_adapter.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests -p "test_*.py"
```

Expected: PASS.

Commit:

```powershell
git add code/src/baselines/async_http.py code/src/baselines/vllm_bench.py code/tests/test_baseline_async_http.py code/tests/test_vllm_bench_adapter.py code/requirements.txt
git commit -m "feat: add direct baseline adapters"
```

---

### Task 4: Add Daft Prompt and Ray Data Official Adapters

**Files:**
- Create: `code/src/baselines/official_runtime.py`
- Create: `code/tests/test_official_runtime_adapters.py`
- Modify: `code/requirements.txt`

**Interfaces:**
- Consumes: one endpoint shard at a time.
- Produces:
  - `DaftPromptConfig`
  - `RayDataHttpConfig`
  - `run_daft_prompt(requests, config, modules=None)`
  - `run_ray_data_http(requests, config, modules=None)`

- [ ] **Step 1: Write failing Daft plan-construction tests**

```python
def test_daft_prompt_options_are_same_request_chat_semantics(self) -> None:
    options = daft_prompt_options(
        model="qwen",
        max_tokens=128,
    )
    self.assertEqual(
        options,
        {
            "model": "qwen",
            "use_chat_completions": True,
            "temperature": 0.0,
            "max_tokens": 128,
            "max_retries": 0,
            "on_error": "raise",
        },
    )
```

- [ ] **Step 2: Run and witness RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_official_runtime_adapters.py
```

Expected: import failure because the module does not exist.

- [ ] **Step 3: Implement the Daft adapter with lazy imports**

For Native:

```python
daft.set_runner_native()
```

For Ray:

```python
daft.set_runner_ray(noop_if_initialized=True)
```

Build:

```python
provider = OpenAIProvider(
    base_url=config.base_url,
    api_key=config.api_key or "not-needed",
)
frame = daft.from_pydict(
    {
        "doc_id": [request.doc_id for request in requests],
        "prompt": [request.prompt for request in requests],
    }
)
frame = frame.with_column(
    "output_text",
    prompt(
        daft.col("prompt"),
        provider=provider,
        model=config.model,
        use_chat_completions=True,
        temperature=0.0,
        max_tokens=config.max_tokens,
        max_retries=0,
        on_error="raise",
    ),
)
rows = frame.collect().to_pylist()
```

Reject mixed per-row output caps in one adapter invocation.

- [ ] **Step 4: Write the failing Ray Data payload and semantics test**

```python
def test_ray_data_preprocess_emits_one_chat_request_per_row(self) -> None:
    row = ray_data_preprocess(
        {"doc_id": 7, "prompt": "question"},
        model="qwen",
        max_tokens=128,
    )
    self.assertEqual(
        row["payload"]["messages"],
        [{"role": "user", "content": "question"}],
    )
    self.assertEqual(row["payload"]["temperature"], 0.0)
    self.assertNotIn("prompt", row["payload"])
```

- [ ] **Step 5: Implement the Ray Data adapter**

Use `HttpRequestProcessorConfig` with `max_retries=0`. `batch_size` is the Ray
stage batch: source inspection shows `HttpRequestUDF` creates one
`session.post()` per row and then awaits all responses. Record this as
`http_requests == rows`, not as multi-prompt HTTP.

```python
config = HttpRequestProcessorConfig(
    batch_size=config.batch_size,
    url=config.endpoint_url,
    headers={"Authorization": f"Bearer {config.api_key}"} if config.api_key else None,
    concurrency=config.concurrency,
    max_retries=0,
)
processor = build_processor(
    config,
    preprocess=lambda row: ray_data_preprocess(
        row,
        model=config.model,
        max_tokens=config.max_tokens,
    ),
    postprocess=ray_data_postprocess,
)
rows = processor(ray.data.from_items(items)).take_all()
```

- [ ] **Step 6: Declare official adapter dependencies**

Add:

```text
openai>=1.0
pandas>=2.0
aiohttp>=3.9
```

to `code/requirements.txt`. Keep all imports inside adapter functions so the
base unit suite still reports a clear optional-dependency error.

- [ ] **Step 7: Run tests and commit**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_official_runtime_adapters.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests -p "test_*.py"
```

Expected: PASS without starting Ray or contacting a model endpoint.

Commit:

```powershell
git add code/src/baselines/official_runtime.py code/tests/test_official_runtime_adapters.py code/requirements.txt
git commit -m "feat: add official runtime baselines"
```

---

### Task 5: Add the OceanBase AI_COMPLETE Adapter

**Files:**
- Create: `code/src/baselines/oceanbase.py`
- Create: `code/tests/test_oceanbase_baseline.py`
- Create: `deploy/autodl/oceanbase_ai_complete_gate.sql`
- Modify: `code/requirements.txt`
- Modify: `deploy/autodl/README.md`

**Interfaces:**
- Produces:
  - `OceanBaseConfig`
  - `build_register_model_sql(config)`
  - `build_ai_complete_sql(table_name, result_table, parallel_degree)`
  - `run_oceanbase_ai_complete(requests, config, connection_factory=None)`

- [ ] **Step 1: Write failing endpoint-registration SQL tests**

```python
def test_oceanbase_registration_targets_same_vllm_chat_endpoint(self) -> None:
    statements = build_register_model_sql(
        OceanBaseConfig(
            host="127.0.0.1",
            port=2881,
            user="root@test",
            password="",
            database="test",
            model_key="baseline_qwen",
            model_name="qwen2.5-1.5b",
            endpoint_url="http://127.0.0.1:8000/v1/chat/completions",
            access_key="not-needed",
            parallel_degree=1,
        )
    )
    joined = "\n".join(statements)
    self.assertIn("CREATE_AI_MODEL", joined)
    self.assertIn("CREATE_AI_MODEL_ENDPOINT", joined)
    self.assertIn("/v1/chat/completions", joined)
    self.assertIn('"provider": "openai"', joined)
```

- [ ] **Step 2: Run and witness RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_oceanbase_baseline.py
```

Expected: import failure because the adapter does not exist.

- [ ] **Step 3: Implement identifier validation and SQL builders**

Accept identifiers matching:

```python
r"[A-Za-z_][A-Za-z0-9_]*"
```

Bind all values through PyMySQL parameters. Only validated identifiers and the
integer parallel degree may be interpolated.

The execution statement is:

```sql
INSERT INTO baseline_results
    (doc_id, output_text, completed_at)
SELECT /*+ PARALLEL(1) */
    doc_id,
    AI_COMPLETE(
        'baseline_qwen',
        prompt,
        JSON_OBJECT('temperature', 0.0, 'max_tokens', 128)
    ),
    NOW(6)
FROM baseline_requests
ORDER BY doc_id
```

- [ ] **Step 4: Write failing exactly-once transaction tests**

Add concrete `RecordingConnection` and `RecordingCursor` test doubles. The
cursor appends `(sql, params)` to `connection.executed`; `executemany()`
also records the row batch; `fetchall()` returns the configured result rows.
The connection exposes boolean `committed` and `rolled_back` flags through
`commit()` and `rollback()`. Use these doubles to assert:

- source rows are inserted once;
- result table is cleared before the gate;
- one `INSERT ... SELECT AI_COMPLETE` is executed;
- result rows are read ordered by `doc_id`;
- commit happens only after successful execution;
- rollback occurs on model-call failure.

- [ ] **Step 5: Implement the minimal PyMySQL adapter**

Keep one connection per endpoint shard. The outer dual-endpoint runner starts
two shard processes concurrently, each with its own OceanBase model key,
source/result tables and vLLM URL. No Ray or Daft import is allowed.

- [ ] **Step 6: Add the read-only-compatible gate SQL**

`deploy/autodl/oceanbase_ai_complete_gate.sql` must:

1. query `SELECT VERSION()`;
2. query whether `DBMS_AI_SERVICE` exists;
3. query whether `AI_COMPLETE` resolves;
4. display registered model endpoint metadata;
5. execute one deterministic prompt only after explicit endpoint setup.

It must not drop databases, tenants or existing model registrations.

- [ ] **Step 7: Add dependency and AutoDL instructions**

Add:

```text
PyMySQL>=1.1
```

Document that OceanBase formal is forbidden until the local CE image/version
gate proves AI Function availability and the endpoint points to
`127.0.0.1:{8000,8001}`.

- [ ] **Step 8: Run tests and commit**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_oceanbase_baseline.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests -p "test_*.py"
```

Expected: PASS.

Commit:

```powershell
git add code/src/baselines/oceanbase.py code/tests/test_oceanbase_baseline.py deploy/autodl/oceanbase_ai_complete_gate.sql deploy/autodl/README.md code/requirements.txt
git commit -m "feat: add oceanbase ai baseline"
```

---

### Task 6: Add the Unified CLI, Templates and Gate Validation

**Files:**
- Create: `code/scripts/run_official_baseline.py`
- Create: `code/src/baselines/cli.py`
- Create: `code/src/baselines/gate.py`
- Create: `code/tests/test_official_baseline_cli.py`
- Create: `code/tests/test_official_baseline_gate.py`
- Create: `deploy/autodl/dual_gpu_official_baseline_gate.example.json`
- Create: `deploy/autodl/dual_gpu_official_baseline_calibration.example.json`
- Modify: `code/scripts/README.md`
- Modify: `code/README.md`
- Modify: `code/INFRA_STATUS.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces `run_cli(argv: Sequence[str]) -> dict[str, object]`,
  `main(argv: Sequence[str] | None = None) -> int`, and one CLI with:
  - `export-manifest`
  - `run-shard`
  - `normalize-vllm-bench`
  - `validate-gate`

- [ ] **Step 1: Write failing CLI dry-run tests**

```python
def test_run_shard_dry_run_is_side_effect_free(self) -> None:
    result = run_cli(
        [
            "run-shard",
            "--adapter",
            "bounded_http",
            "--manifest",
            str(self.manifest),
            "--endpoint-index",
            "0",
            "--endpoint-url",
            "http://127.0.0.1:8000/v1/chat/completions",
            "--model",
            "qwen",
            "--concurrency",
            "32",
            "--output-dir",
            str(self.output_dir),
            "--dry-run",
        ]
    )
    self.assertEqual(result["status"], "dry_run")
    self.assertEqual(result["request_count"], 32)
    self.assertFalse(self.output_dir.exists())
```

- [ ] **Step 2: Run and witness RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_official_baseline_cli.py
```

Expected: import failure because the CLI does not exist.

- [ ] **Step 3: Implement thin CLI dispatch**

The CLI parses and validates arguments, loads one immutable shard and dispatches
to one adapter. It writes:

```text
manifest_metadata.json
requests.csv
summary.json
raw/
```

using an atomic temporary-file rename. It never owns experiment-matrix policy.

- [ ] **Step 4: Write failing gate tests**

```python
def test_gate_rejects_endpoint_work_skew_over_two_percent(self) -> None:
    report = validate_gate(
        manifest=self.manifest,
        summaries=(summary(endpoint=0, predicted_work=100), summary(endpoint=1, predicted_work=90)),
        request_results=self.results,
    )
    self.assertFalse(report.passed)
    self.assertIn("endpoint_work_skew", report.incidents)
```

Add tests for duplicate/missing/failed requests, unused endpoint, mismatched
model/protocol/service metadata and non-empty final vLLM queues.

- [ ] **Step 5: Implement fail-closed gate validation**

The validator reports every incident and returns non-zero from the CLI when any
hard gate fails. It never deletes artifacts or retries.

- [ ] **Step 6: Add gate and calibration templates**

Gate:

- 64 total rows;
- fixed two-endpoint manifest;
- adapters `vllm_bench`, `bounded_http`, `daft_native`, `daft_ray`,
  `ray_data_http`, `project_static`, `project_token_work`;
- OceanBase as a separately enabled fatal-flaw cell;
- one run per cell;
- no formal repeats.

Calibration:

- B0/B2 concurrency `{16,32,64,128,256}`;
- Daft batch/partition/concurrency minimal grid;
- Ray Data `batch_size {1,16,32,64} × concurrency {1,2,4,8,16,32}`;
- project static capacity curve;
- project token-work recheck around 65,536.

- [ ] **Step 7: Run focused, full, lint and compile checks**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_official_baseline_cli.py
.conda\pg-ai-profile\python.exe code\tests\test_official_baseline_gate.py
.conda\pg-ai-profile\python.exe -m unittest discover -s code\tests -p "test_*.py"
.conda\pg-ai-profile\python.exe -m compileall -q code
.conda\pg-ai-profile\Scripts\ruff.exe check code
git diff --check
```

Expected: all PASS with no diff errors.

- [ ] **Step 8: Commit**

```powershell
git add code/scripts/run_official_baseline.py code/src/baselines/cli.py code/src/baselines/gate.py code/tests/test_official_baseline_cli.py code/tests/test_official_baseline_gate.py deploy/autodl/dual_gpu_official_baseline_gate.example.json deploy/autodl/dual_gpu_official_baseline_calibration.example.json code/scripts/README.md code/README.md code/INFRA_STATUS.md PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "feat: add official baseline gate"
```

---

### Task 7: Publish and Run the Remote Fatal-Flaw Gate

**Files:**
- Modify only if evidence requires a fix:
  `code/src/baselines/*`, `code/tests/test_*baseline*.py`,
  `deploy/autodl/dual_gpu_official_baseline_gate.example.json`
- Create results only after a passing gate:
  `experiments/results/dual_gpu_official_baseline_gate_<unique_id>/`

**Interfaces:**
- Consumes: a clean pushed `main`, existing AutoDL runtime env and two idle
  vLLM endpoints.
- Produces: durable gate evidence; does not start formal.

- [ ] **Step 1: Run verification-before-publish**

Run all commands from Task 6 Step 7 and inspect:

```powershell
git status --short
git diff --stat
git log -5 --oneline
```

Expected: only intended baseline files differ and all tests pass.

- [ ] **Step 2: Push `main`**

```powershell
git push origin main
```

Expected: pushed commit SHA matches local `HEAD`.

- [ ] **Step 3: Perform remote read-only preflight**

Following `deploy/autodl/README.md`, verify:

- no existing scenario/shared/baseline runner;
- no output lease;
- both endpoints healthy and running/waiting are zero;
- no Ray workload;
- remote git state and untracked results are preserved.

- [ ] **Step 4: Safely fast-forward the idle checkout**

Use `git pull --ff-only` only when the preflight proves there is no active
runner and no tracked-file conflict. Never delete or overwrite untracked
results.

- [ ] **Step 5: Install only declared missing dependencies**

In the base conda environment, verify imports first. Install only
`httpx/openai/pandas/aiohttp/PyMySQL` packages that are missing, using the
documented mirror and pinned project requirements.

- [ ] **Step 6: Run the 64-row two-endpoint gate in a fresh directory**

Start exactly one gate runner. Monitor manifest, incidents, per-request results,
GPU, endpoint distribution and final queues. Do not start OceanBase formal;
the OceanBase cell is only a one-row capability check in this gate.

- [ ] **Step 7: Debug failures systematically**

For any failure:

1. preserve the full output directory and lease evidence;
2. write a local failing regression test reproducing the cause;
3. witness RED;
4. implement the minimal fix;
5. run focused and full tests;
6. commit and push;
7. use a new remote output directory.

- [ ] **Step 8: Stop after gate analysis**

Even if the gate passes, do not start calibration/formal automatically.
Analyze:

- request-body equivalence;
- exactly-once;
- endpoint work skew;
- real HTTP request count versus rows;
- Daft/Ray Data batch semantics;
- OceanBase CE capability;
- zero worker failure and final empty queues.

Update `PROJECT_LOG.md` and the formal experiment plan with the evidence, then
choose whether all arms are safe for calibration.
