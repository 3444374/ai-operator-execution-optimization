# vLLM and Ray Tuning Experiments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Select a reproducible single-GPU vLLM execution mode and Ray executor configuration using real PostgreSQL, Daft, Ray, and vLLM runs.

**Architecture:** Service-mode, Ray-executor, and vLLM-capacity decisions run as sequential gates. Each gate fixes the winner from the previous gate, uses the same 512-request workload, and records complete service metadata in the scenario manifest.

**Tech Stack:** Docker Desktop, vLLM 0.25.1, Qwen2.5-1.5B-Instruct, RTX 5070 12 GB, PostgreSQL 18.4 + pgvector 0.8.2, Daft 0.7.20, Ray 2.56, scenario runner, CSV traces.

## Global Constraints

- Complete `2026-07-26-ray-execution-foundation-implementation.md` first.
- Use the real `compatible_http` backend; fake is forbidden for GPU results.
- Preserve complete prompts; do not split or truncate rows.
- Use `sharegpt_burstgpt`, the same selected 512 document IDs, arrival order, ChatML, temperature 0, output cap 512, token budget 6144, K8, fixed 50 ms, no writeback, and cache-off unless the prefix stage says otherwise.
- Record PostgreSQL, pgvector, Daft, Ray, vLLM, model, tokenizer, service, GPU, MFU, power, and request/submission trace metadata.
- A 64-request gate is infrastructure evidence only.
- Do not merge to `main`; the user requires completed experiments and reviewed data first.

---

### Task 1: Validate complete service metadata

**Files:**
- Modify: `code/src/experiment_scenarios.py`
- Test: `code/tests/experiments/test_experiment_scenarios.py`

**Interfaces:**
- Produces:
  `validate_service_metadata(metadata: Mapping[str, object]) -> None`.
- Required keys:
  `vllm_version`, `enforce_eager`, `compilation_mode`,
  `chunked_prefill`, `max_num_batched_tokens`, `max_num_seqs`,
  `gpu_memory_utilization`, `prefix_caching`, and `mfu_metrics`.

- [ ] **Step 1: Write the failing validation tests**

```python
def test_service_metadata_requires_execution_parameters() -> None:
    metadata = {
        "vllm_version": "0.25.1",
        "enforce_eager": False,
        "compilation_mode": "default",
        "chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_num_seqs": 64,
        "gpu_memory_utilization": 0.75,
        "prefix_caching": False,
        "mfu_metrics": True,
    }
    validate_service_metadata(metadata)


def test_service_metadata_rejects_missing_capacity() -> None:
    with pytest.raises(ValueError, match="max_num_batched_tokens"):
        validate_service_metadata({"vllm_version": "0.25.1"})
```

- [ ] **Step 2: Run and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\experiments\test_experiment_scenarios.py -q
```

Expected: FAIL because the validator does not exist.

- [ ] **Step 3: Implement exact-key and value validation**

```python
REQUIRED_SERVICE_METADATA = (
    "vllm_version",
    "enforce_eager",
    "compilation_mode",
    "chunked_prefill",
    "max_num_batched_tokens",
    "max_num_seqs",
    "gpu_memory_utilization",
    "prefix_caching",
    "mfu_metrics",
)


def validate_service_metadata(metadata: Mapping[str, object]) -> None:
    missing = [key for key in REQUIRED_SERVICE_METADATA if key not in metadata]
    if missing:
        raise ValueError(
            "service_metadata missing required keys: " + ", ".join(missing)
        )
    utilization = float(metadata["gpu_memory_utilization"])
    if not 0.0 < utilization <= 1.0:
        raise ValueError("gpu_memory_utilization must be in (0, 1]")
    for key in ("max_num_batched_tokens", "max_num_seqs"):
        if metadata[key] != "unknown" and int(metadata[key]) <= 0:
            raise ValueError(f"{key} must be positive or 'unknown'")
```

Call the validator when a scenario config opts into
`"require_complete_service_metadata": true`; preserve historical configs.

- [ ] **Step 4: Run and verify GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m pytest code\tests\experiments\test_experiment_scenarios.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add code/src/experiment_scenarios.py code/tests/experiments/test_experiment_scenarios.py
git commit -m "feat: validate service experiment metadata"
```

---

### Task 2: Capture the eager baseline and prepare recoverable service switching

**Files:**
- Create: `experiments/results/vllm_cuda_graph_512_20260726/eager_service.json`
- Create: `experiments/results/vllm_cuda_graph_512_20260726/scenario_config_eager.json`
- Create: `experiments/results/vllm_cuda_graph_512_20260726/scenario_config_eager_gate.json`
- Create after run: `experiments/results/vllm_cuda_graph_512_20260726/eager/`

**Interfaces:**
- Consumes the current Docker container `ai-operator-vllm-qwen`.
- Preserves it as `ai-operator-vllm-qwen-eager-backup` before graph-mode start.

- [ ] **Step 1: Record the exact existing service**

Run:

```powershell
docker inspect ai-operator-vllm-qwen --format '{{json .Config.Cmd}}' > experiments\results\vllm_cuda_graph_512_20260726\eager_service.json
docker inspect ai-operator-vllm-qwen --format '{{json .Mounts}} {{json .Config.Image}}' >> experiments\results\vllm_cuda_graph_512_20260726\eager_service.json
```

Expected: the file records vLLM 0.25.1, model mount, `--enforce-eager`,
`--enable-mfu-metrics`, and cache disabled.

- [ ] **Step 2: Create the eager scenario config**

The JSON uses:

```json
{
  "schema_version": 1,
  "experiment_id": "vllm_eager_512_20260726",
  "seed": 20260731,
  "require_complete_service_metadata": true,
  "service_metadata": {
    "vllm_version": "0.25.1",
    "enforce_eager": true,
    "compilation_mode": "disabled",
    "chunked_prefill": "unknown",
    "max_num_batched_tokens": "unknown",
    "max_num_seqs": "unknown",
    "gpu_memory_utilization": 0.75,
    "prefix_caching": false,
    "mfu_metrics": true
  },
  "warmup_runs_per_scenario": 1,
  "formal_repeats": 3,
  "scenarios": [
    {"scenario_id": "ray_task_k8", "args": []}
  ]
}
```

Set `common_args` exactly to:

```json
[
  "--database-url", "postgresql://postgres:postgres@localhost:5432/ai_operator",
  "--total-rows", "512",
  "--db-fetch-rows", "512",
  "--source-max-prompt-tokens", "1500",
  "--operator", "ai_complete",
  "--executor", "ray_task",
  "--model-backend", "compatible_http",
  "--completion-endpoint-url", "http://localhost:8000/v1/completions",
  "--completion-model", "qwen2.5-1.5b",
  "--completion-max-tokens", "512",
  "--completion-return-token-ids",
  "--completion-prompt-format", "chatml",
  "--completion-temperature", "0",
  "--completion-request-timeout-s", "300",
  "--model-metrics-url", "http://localhost:8000/metrics",
  "--source-workload-name", "sharegpt_burstgpt",
  "--source-order", "arrival_time",
  "--data-source", "daft_postgres",
  "--organizer", "daft",
  "--organizer-partition-mode", "none",
  "--daft-runner", "native",
  "--ray-batch-rows", "64",
  "--batching-policy", "token_budget",
  "--token-budget", "6144",
  "--cost-model-id", "qwen2.5-1.5b",
  "--cost-tokenizer-id", "qwen2.5-1.5b",
  "--output-cost-mode", "fixed_output_cap",
  "--scheduling-policy", "static",
  "--max-inflight", "8",
  "--model-workers", "2",
  "--ray-worker-num-cpus", "0.25",
  "--arrival-replay",
  "--arrival-time-scale", "0.0005",
  "--flush-policy", "fixed_timeout",
  "--flush-timeout-ms", "50",
  "--flush-max-wait-ms", "50",
  "--writeback-mode", "none",
  "--request-slo-ms", "180000",
  "--resource-sample-interval-s", "0.25",
  "--gpu-peak-tflops", "61.7",
  "--mfu-precision", "bf16_dense_fp32_accumulate"
]
```

- [ ] **Step 3: Run the eager 64-request gate**

Create `scenario_config_eager_gate.json` by copying the JSON in Step 2 with:

```json
{
  "experiment_id": "vllm_eager_gate_64_20260726",
  "warmup_runs_per_scenario": 0,
  "formal_repeats": 1
}
```

Replace both `--total-rows` and `--db-fetch-rows` values with `64`, then run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_ai_operator_scenarios.py experiments\results\vllm_cuda_graph_512_20260726\scenario_config_eager_gate.json --output-dir experiments\results\vllm_cuda_graph_512_20260726\eager_gate --health-url http://localhost:8000/health --metrics-url http://localhost:8000/metrics
```

Expected:

- 64 request trace rows;
- 64 unique doc IDs;
- 64 actual output-token counts and finish reasons;
- positive vLLM FLOP delta;
- valid MFU, energy, and resource trace.

- [ ] **Step 4: Run eager 512 repeats**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_ai_operator_scenarios.py experiments\results\vllm_cuda_graph_512_20260726\scenario_config_eager.json --output-dir experiments\results\vllm_cuda_graph_512_20260726\eager --health-url http://localhost:8000/health --metrics-url http://localhost:8000/metrics
```

Expected: one warm-up plus three formal completed runs, zero incidents.

- [ ] **Step 5: Commit configuration and audited results**

```powershell
git add experiments/results/vllm_cuda_graph_512_20260726
git commit -m "results: capture eager vLLM baseline"
```

---

### Task 3: Run the CUDA Graph service gate and comparison

**Files:**
- Create: `experiments/results/vllm_cuda_graph_512_20260726/graph_service.json`
- Create: `experiments/results/vllm_cuda_graph_512_20260726/scenario_config_graph.json`
- Create after run: `experiments/results/vllm_cuda_graph_512_20260726/graph/`

**Interfaces:**
- Uses the same image and read-only model mount as the eager service.
- Removes only `--enforce-eager`; all other initial service flags remain fixed.

- [ ] **Step 1: Preserve and stop the eager container**

Run:

```powershell
docker stop ai-operator-vllm-qwen
docker rename ai-operator-vllm-qwen ai-operator-vllm-qwen-eager-backup
```

Expected: the stopped eager container remains recoverable under the backup
name.

- [ ] **Step 2: Start graph mode**

Run:

```powershell
docker run -d --name ai-operator-vllm-qwen --gpus all -p 8000:8000 -v "D:\Code\ai-operator-execution-optimization\models\Qwen2.5-1.5B-Instruct:/models/qwen:ro" vllm/vllm-openai:v0.25.1-cu129-ubuntu2404 --model /models/qwen --served-model-name qwen2.5-1.5b --dtype auto --max-model-len 2048 --gpu-memory-utilization 0.75 --enable-mfu-metrics --no-enable-prefix-caching
```

Expected: `/health` returns success after compile/capture startup; logs contain
no OOM or CUDA Graph failure.

- [ ] **Step 3: Record actual startup and run the 64-request gate**

Record `docker inspect` output in `graph_service.json`. Run the same 64-request
command as Task 2.

Expected: all correctness, token, trace, MFU, and incident gates pass.

- [ ] **Step 4: Run one warm-up and three formal 512 repeats**

Use `scenario_config_graph.json`, identical to eager except:

```json
{
  "enforce_eager": false,
  "compilation_mode": "default"
}
```

Expected: four completed runs and zero incidents.

- [ ] **Step 5: Compare and choose the service mode**

Create `experiments/results/vllm_cuda_graph_512_20260726/README.md` with:

- means and standard deviations for tokens/s, E2E, P99, SLO goodput, GPU
  utilization, memory, power, energy/1k tokens, and MFU;
- exact startup/compile cost;
- correctness audit;
- selection gate from the design;
- facts, inference, open questions, and prohibited claims.

- [ ] **Step 6: Commit**

```powershell
git add experiments/results/vllm_cuda_graph_512_20260726 PROJECT_INDEX.md PROJECT_LOG.md PROJECT_OUTLINE.md
git commit -m "results: compare eager and CUDA Graph vLLM"
```

---

### Task 4: Screen Ray task and actor layouts

**Files:**
- Create: `experiments/results/ray_executor_tuning_512_20260726/scenario_config.json`
- Create after run: `experiments/results/ray_executor_tuning_512_20260726/screen/`
- Create after selection: `experiments/results/ray_executor_tuning_512_20260726/formal/`
- Create: `experiments/results/ray_executor_tuning_512_20260726/README.md`

**Interfaces:**
- Uses the service mode selected in Task 3.
- Scenarios: task K8; actor 8×1, 4×2, 2×4, and 1×8.

- [ ] **Step 1: Create a one-repeat screening config**

Each actor scenario sets:

```json
[
  "--executor", "ray_actor",
  "--actor-workers-per-endpoint", "4",
  "--ray-actor-max-concurrency", "2",
  "--ray-worker-num-cpus", "0.25"
]
```

The task scenario sets `--executor ray_task`. All scenarios keep K8 and all
workload/service inputs fixed.

- [ ] **Step 2: Run 64-request contract gates**

Expected for every scenario:

- 64 exactly-once requests;
- `endpoint_count=1`;
- actor scenarios report the configured worker count;
- task and actor rows report `ray_worker_num_gpus=0`.

- [ ] **Step 3: Run the 512-request screen**

Expected: five completed runs and zero incidents.

- [ ] **Step 4: Select formal candidates**

Keep task baseline plus actor layouts that:

- are within 5% of the best screen tokens/s;
- have no SLO regression;
- have balanced per-actor submission counts with difference at most one.

- [ ] **Step 5: Run one warm-up and three formal repeats**

Use a new formal config containing only selected candidates.

- [ ] **Step 6: Write and commit the report**

Report normal system metrics plus actor creation, submit/fan-in, assignment
balance, and failures.

```powershell
git add experiments/results/ray_executor_tuning_512_20260726 PROJECT_INDEX.md PROJECT_LOG.md PROJECT_OUTLINE.md code/INFRA_STATUS.md
git commit -m "results: select Ray execution layout"
```

---

### Task 5: Screen vLLM scheduling capacity

**Files:**
- Create: `experiments/results/vllm_capacity_tuning_512_20260726/`
- Create: `experiments/results/vllm_capacity_tuning_512_20260726/README.md`

**Interfaces:**
- Uses the service mode and Ray executor selected by Tasks 3 and 4.
- Changes only `max_num_batched_tokens` and `max_num_seqs`.

- [ ] **Step 1: Query accepted vLLM 0.25.1 options**

Run:

```powershell
docker run --rm vllm/vllm-openai:v0.25.1-cu129-ubuntu2404 vllm serve --help
```

Record the exact accepted option names and defaults in the result README.

- [ ] **Step 2: Define the bounded screen**

Use:

```text
max_num_batched_tokens: default, 2048, 4096
max_num_seqs: default, 32, 64
```

Start with `(default, default)`, `(2048, 32)`, `(4096, 64)`. Add no other
combination unless all three pass and their ordering is unresolved within 2%.

- [ ] **Step 3: Run the 64-request gate per service**

Prune on OOM, preemption, timeout, invalid MFU, or correctness failure.

- [ ] **Step 4: Run one 512-request screen per survivor**

Keep at most two non-default candidates whose tokens/s or P99 improves by at
least 3% without more than one percentage point absolute SLO regression.

- [ ] **Step 5: Run three formal repeats and select**

Write the report with exact container commands and complete service metadata.

- [ ] **Step 6: Commit**

```powershell
git add experiments/results/vllm_capacity_tuning_512_20260726 PROJECT_INDEX.md PROJECT_LOG.md PROJECT_OUTLINE.md code/INFRA_STATUS.md
git commit -m "results: select vLLM scheduling capacity"
```

---

### Task 6: Close documentation and preserve future gates

**Files:**
- Modify: `code/INFRA_STATUS.md`
- Modify: `code/README.md`
- Modify: `code/scripts/README.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Records one selected single-GPU service/Ray execution default.
- Keeps prefix-cache and real multi-GPU experiments open.

- [ ] **Step 1: Audit all manifests and traces**

Verify:

- completed manifests;
- unique request IDs and exact row coverage;
- actual output tokens and finish reasons;
- positive vLLM prompt/generation/FLOP deltas;
- MFU, energy, power, and resource time series;
- no unresolved incident.

- [ ] **Step 2: Update the default only from repeated evidence**

State the selected:

```text
vLLM execution mode
vLLM capacity
Ray executor
actor workers/concurrency when actor wins
```

If no candidate passes, retain the current default and report the negative
result.

- [ ] **Step 3: Preserve open future work**

Keep explicit:

- cache-on prefix mechanism experiment;
- logical dual-endpoint contract gate;
- real homogeneous and heterogeneous multi-GPU routing;
- Daft Ray-runner sweep trigger;
- multimodal source/cost adapter validation.

- [ ] **Step 4: Run final repository verification**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m compileall -q code
.conda\pg-ai-profile\python.exe -m pytest code\tests -q
git diff --check
git status --short
```

Expected: code checks pass; only intentional result/document changes are
tracked; `.superpowers/` remains untouched.

- [ ] **Step 5: Commit**

```powershell
git add code/INFRA_STATUS.md code/README.md code/scripts/README.md experiments/plans/experiment_status_and_gaps.md PROJECT_OUTLINE.md PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "docs: record execution tuning decision"
```
