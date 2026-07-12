# CPU vs GPU Embedding Comparison, 2026-07-12

## Purpose

This report compares real embedding endpoint placement:

```text
CPU endpoint on localhost:8001
GPU endpoint on localhost:8000
```

Both endpoints use the same local model files:

```text
.cache/models/all-MiniLM-L6-v2
```

Both are called through the same PostgreSQL external execution profile script. This is a CPU/GPU baseline and ablation, not a claim that the project is about moving database kernels to GPU.

Raw CSV files:

```text
motivation/results/cpu/ai_embed_cpu_profile.csv
motivation/results/gpu/ai_embed_profile.csv
```

## Time Boundary

The `e2e_s` value is the whole profile-driver wall-clock time for one run:

```text
create job
  -> fetch rows from PostgreSQL
  -> build Arrow RecordBatch
  -> call embedding HTTP endpoint through Python/Ray
  -> fan-in results
  -> write embeddings back to PostgreSQL as JSON text
```

It is **not** a separate measurement of "moving the AI operator from CPU to GPU".

For GPU runs, endpoint-internal work includes tokenization, tensor creation, CPU-to-GPU transfer, model forward on GPU, GPU-to-CPU output transfer, and JSON response construction. The current script does **not** break these sub-stages apart.

The `model_service_s` column is a legacy CSV field name. It is the **sum of observed embedding HTTP request durations**. When multiple requests overlap, this sum can be larger than `e2e_s`; therefore it is useful as endpoint load, but it is not a wall-clock model-stage duration.

If a future report needs a wall-clock model stage, add a separate field such as:

```text
operator_wall_s = time from first endpoint request submission to final endpoint response collection
```

## Endpoint Setup

GPU endpoint:

- Port: `8000`
- Device: `cuda`
- Started manually by the user before profiling
- Health check returned `embedding_dim=384`

CPU endpoint:

- Port: `8001`
- Device: `cpu`
- Started by Codex for this comparison and stopped after the run
- Health check returned `embedding_dim=384`

Reproducible CPU endpoint command:

```powershell
$env:HF_HOME="D:\Code\ai-operator-execution-optimization\.cache\huggingface"
$env:HF_HUB_CACHE="D:\Code\ai-operator-execution-optimization\.cache\huggingface\hub"
$env:TRANSFORMERS_CACHE=$env:HF_HUB_CACHE
$env:TORCH_HOME="D:\Code\ai-operator-execution-optimization\.cache\torch"

.conda\pg-ai-profile\python.exe code\scripts\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cpu `
  --batch-size 64 `
  --port 8001
```

## Commands

The CPU runs used:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows <rows> --total-rows <rows> `
  --db-fetch-rows <256-or-512> --ray-batch-rows 256 `
  --embedding-dim 384 --model-workers 2 --max-inflight 8 `
  --executor ray_actor `
  --strategy fine|coalesced `
  --model-backend http_openai `
  --embedding-endpoint-url http://localhost:8001/v1/embeddings `
  --embedding-model all-MiniLM-L6-v2-cpu `
  --writeback-mode json_text --write-batch-rows 256 `
  --warmup-runs 1 --repeats 2 `
  --output motivation\results\cpu\ai_embed_cpu_profile.csv
```

The compared GPU runs are from:

```text
motivation/results/gpu/ai_embed_profile.csv
```

## Results

Only `phase=formal` rows are averaged.

| endpoint | rows | strategy | calls | e2e_s | rows/s | model request time sum (`model_service_s`) | bounded_wait_s | fanin_s | writeback_s |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| CPU | 1024 | `coalesced` | 4 | 2.948 | 347.4 | 2.407 | 0.000 | 0.002 | 0.406 |
| GPU | 1024 | `coalesced` | 4 | 0.990 | 1034.0 | 0.451 | 0.000 | 0.002 | 0.402 |
| CPU | 1024 | `fine` | 1024 | 9.186 | 111.5 | 15.899 | 7.771 | 0.227 | 0.430 |
| GPU | 1024 | `fine` | 1024 | 13.458 | 76.1 | 24.648 | 12.027 | 0.165 | 0.394 |
| CPU | 4096 | `coalesced` | 16 | 10.662 | 384.3 | 16.995 | 0.000 | 0.009 | 1.627 |
| GPU | 4096 | `coalesced` | 16 | 3.165 | 1294.2 | 2.561 | 0.000 | 0.007 | 1.554 |

Derived ratios:

| Comparison | Ratio |
|---|---:|
| 1024 coalesced CPU/GPU e2e | `2.98x` |
| 4096 coalesced CPU/GPU e2e | `3.37x` |
| CPU 1024 fine/coalesced e2e | `3.12x` |
| GPU 1024 fine/coalesced e2e | `13.59x` |
| 4096 GPU writeback/e2e | `49.1%` |
| 4096 CPU writeback/e2e | `15.3%` |

## Interpretation

Local experiment facts:

- With proper batching (`coalesced`), the GPU endpoint is faster than the CPU endpoint on this local setup.
- At 1024 rows, GPU coalesced e2e is `0.990s`, CPU coalesced e2e is `2.948s`.
- At 4096 rows, GPU coalesced e2e is `3.165s`, CPU coalesced e2e is `10.662s`.
- Row-by-row endpoint calls are bad for both endpoints.
- The GPU fine run is slower than the CPU fine run in e2e. This is plausible because each call handles only one text; small GPU calls pay HTTP, tokenization, tensor transfer, kernel launch, and response overhead without enough batch work to amortize it.
- In 4096-row GPU coalesced mode, writeback is about half of e2e (`1.554s / 3.165s`). Once model compute is accelerated, PostgreSQL writeback becomes a major cost.
- In 4096-row CPU coalesced mode, endpoint request time dominates more strongly; writeback is only about `15.3%` of e2e.

Reasonable inference:

- The project should not frame the main result as "GPU is faster than CPU". That is expected and not the research contribution.
- The useful finding is more specific: GPU acceleration changes the bottleneck balance. After batching and GPU service are in place, external-chain costs such as writeback become large enough to optimize.
- Batch construction is essential. GPU does not rescue a row-by-row invocation pattern; it can make tiny-call overhead more visible.
- CPU baseline is still useful because it shows whether an optimization only helps GPU-backed service or also helps the general external execution path.

待确认问题:

- The current endpoint is a minimal HTTP server, not Ray Serve or vLLM. A production-like model service may batch requests differently.
- The current GPU metrics are `nvidia-smi` snapshots, not continuous utilization traces.
- The current profile does not separate tokenization, CPU-to-GPU transfer, GPU forward, GPU-to-CPU transfer, and JSON serialization inside the endpoint.
- The current profile does not record `operator_wall_s`, so `model_service_s` must not be used as a wall-clock stage duration.
- The current writeback is JSON text because the model returns 384-dimensional vectors while the existing pgvector column is `vector(128)`.

Cannot claim:

- Cannot claim PostgreSQL 18.3 platform results.
- Cannot claim GPU kernel optimization results.
- Cannot claim that GPU is always faster. In the fine-grained 1024-row case, GPU e2e is slower than CPU because the invocation pattern is poor.
- Cannot claim final Ray scheduling benefit from this comparison. The endpoint is a single local service, not multiple GPU replicas managed by Ray.

## Meaning For The Research Direction

This comparison supports the current direction, but with a sharper framing:

```text
The key problem is not simply moving AI compute to GPU.
The key problem is coordinating database fetch, batch sizing,
external model-service calls, backpressure/fan-in, and writeback
so that GPU-backed AI operators do not expose new external-chain bottlenecks.
```

The next experiment should be:

1. Add a 384-dimensional pgvector writeback table or column.
2. Compare JSON text vs pgvector writeback under the same GPU endpoint.
3. Add endpoint-internal timing fields if possible: tokenization, H2D, model forward, D2H/serialization.
4. Run row-count scaling after writeback mode is made realistic.
