# 模拟 Embedding 测试结果，2026-07-12

## Purpose

This run is a local PG18.4 simulated embedding test after checking the GPU-backed E2E path:

1. Try to prepare the production-style GPU-backed E2E profile path.
2. If no GPU-backed embedding endpoint is available, run the local PG18.4 simulated embedding tests without claiming GPU conclusions.

The local machine had an NVIDIA GPU visible through `nvidia-smi`, and PostgreSQL 18.4 + pgvector 0.8.2 was reachable on `localhost:5432`. No OpenAI-compatible local embedding service was reachable on `localhost:8000`, and no Ollama-style service was reachable on `localhost:11434`. Therefore this report is a PostgreSQL 18.4 simulated embedding result, not a GPU-backed model-service result.

Raw CSV:

```text
motivation/results/pg18_4_fake/simulated_embed_test_20260712.csv
```

GPU profile configuration dry-run:

```text
feasibility/results/gpu_ai_embed_config_dry_run.csv
```

## Commands

All runs used:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 128 --model-workers 2 --max-inflight 8 `
  --model-backend fake --write-batch-rows 256 `
  --warmup-runs 1 --repeats 2 `
  --output motivation\results\pg18_4_fake\simulated_embed_test_20260712.csv
```

The varied parameters were:

| experiment_id suffix | executor | strategy | writeback_mode |
|---|---|---|---|
| `actor_coalesced` | `ray_actor` | `coalesced` | `json_text` |
| `actor_fine` | `ray_actor` | `fine` | `json_text` |
| `python_coalesced` | `python` | `coalesced` | `json_text` |
| `task_coalesced` | `ray_task` | `coalesced` | `json_text` |
| `actor_coalesced_pgvector` | `ray_actor` | `coalesced` | `pgvector` |

Plain-language mapping:

- `4096 rows` means the script reads 4096 text records from the PostgreSQL `documents` table.
- `operator invocation` means one simulated embedding call.
- In `coalesced` mode, each simulated embedding call handles 256 rows, so 4096 rows become `4096 / 256 = 16` calls.
- In `fine` mode, each simulated embedding call handles 1 row, so 4096 rows become 4096 calls.

## Formal Results

The table uses only `phase=formal` rows and averages the two formal repeats.

| executor | strategy | writeback | objects | invocations | e2e_s | rows/s | db_fetch_s | arrow_build_s | model_service_s | bounded_wait_s | fanin_s | writeback_s |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `ray_task` | `coalesced` | `json_text` | 16 | 16 | 2.239 | 1829.3 | 0.022 | 0.002 | 3.309 | 0.000 | 0.006 | 0.492 |
| `ray_actor` | `coalesced` | `pgvector` | 16 | 16 | 2.259 | 1813.5 | 0.021 | 0.002 | 3.356 | 0.000 | 0.006 | 0.391 |
| `ray_actor` | `coalesced` | `json_text` | 16 | 16 | 2.375 | 1724.9 | 0.023 | 0.002 | 3.359 | 0.000 | 0.006 | 0.518 |
| `python` | `coalesced` | `json_text` | 16 | 16 | 3.764 | 1088.1 | 0.020 | 0.002 | 3.228 | 0.000 | 0.000 | 0.506 |
| `ray_actor` | `fine` | `json_text` | 4096 | 4096 | 33.210 | 123.4 | 0.020 | 0.002 | 58.232 | 28.158 | 0.637 | 0.489 |

## Interpretation

Local experiment facts:

- Fine-grained Ray actor execution is about `13.98x` slower than Ray actor/coalesced on this PG18.4 simulated embedding chain (`33.210s / 2.375s`).
- The fine run turns 4096 rows into 4096 operator invocations and 4096 Ray objects, while coalesced uses 16 invocations and 16 objects.
- The fine run spends most extra time in bounded waiting and accumulated fake model-service calls: average `bounded_wait_s=28.158`, `model_service_s=58.232`.
- Ray task and Ray actor are close under coalesced fake-model settings. Ray task averaged `2.239s`; Ray actor averaged `2.375s`.
- Python coalesced averaged `3.764s`, so Ray parallel execution helped in this fake sleep-style operator.
- Batched pgvector writeback was not slower than JSON text in this run: pgvector writeback averaged `0.391s`, JSON text averaged `0.518s` for the matching actor/coalesced case.

Reasonable inference:

- The most urgent mechanism to preserve for GPU-backed validation is coarse AI operator batching / invocation control. If a real endpoint is called once per row, queue wait and endpoint invocation overhead are likely to dominate before object-store fan-in is even the main issue.
- Ray task remains a strong baseline for stateless fake embedding. Real model endpoints may still favor actors or persistent service clients, so actor should not be dropped.

Cannot claim:

- This is not a GPU-backed embedding result. The model backend was `fake`.
- This is not a PostgreSQL 18.3 internal validation-platform result. The measured database was PostgreSQL `18.4 (Debian 18.4-1.pgdg12+1)`.
- The `nvidia-smi` GPU snapshot only proves a GPU was visible on the host. It does not prove the fake model used GPU compute.
- The result cannot prove real vLLM, Ray Serve, or sentence-transformer endpoint bottlenecks. A real endpoint must be connected before GPU-backed bottleneck attribution.

## Next Priority

The next result that should go into `motivation/results/gpu/` is still:

```text
gpu/ai_embed_profile.csv
gpu/ai_embed_profile.md
```

To produce it, start an OpenAI-compatible embedding endpoint, for example:

```text
http://localhost:8000/v1/embeddings
```

Then run the same DB-triggered profile with:

```powershell
--model-backend http_openai `
--embedding-endpoint-url http://localhost:8000/v1/embeddings `
--embedding-model <model-name>
```

The first GPU-backed matrix should keep the same large blocks:

1. Python coalesced vs Ray task coalesced vs Ray actor coalesced.
2. Ray actor fine vs Ray actor coalesced on a small row count.
3. JSON text vs pgvector batched writeback, only if the endpoint returns 128-dimensional embeddings or the schema is changed.
4. Bounded vs unbounded in-flight after the real endpoint is stable.

