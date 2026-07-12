# GPU-Backed AI_EMBED Profile, 2026-07-12

## Experiment Setup

This is the first GPU-backed end-to-end profile in this project.

Chain:

```text
PostgreSQL 18.4 documents table
  -> Python profile driver
  -> Arrow RecordBatch
  -> Python / Ray task / Ray actor external execution
  -> local CUDA embedding HTTP endpoint
  -> fan-in
  -> PostgreSQL document_embeddings writeback as JSON text
```

Raw CSV:

```text
motivation/results/gpu/ai_embed_profile.csv
```

Model endpoint:

```text
http://localhost:8000/v1/embeddings
```

Model:

```text
sentence-transformers/all-MiniLM-L6-v2
```

Endpoint status:

- The endpoint used in this run was started manually by the user before the profiling runs.
- The endpoint was verified through `/health`; it reported `device=cuda` and `embedding_dim=384`.
- A two-text embedding request succeeded before the profile runs.
- Future automated runs should first check port `8000`; if absent, start the endpoint with the command below.

Reproducible endpoint startup command:

```powershell
$env:HF_HOME="D:\Code\ai-operator-execution-optimization\.cache\huggingface"
$env:HF_HUB_CACHE="D:\Code\ai-operator-execution-optimization\.cache\huggingface\hub"
$env:TRANSFORMERS_CACHE=$env:HF_HUB_CACHE
$env:TORCH_HOME="D:\Code\ai-operator-execution-optimization\.cache\torch"

.conda\pg-ai-profile\python.exe code\scripts\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cuda `
  --batch-size 64 `
  --port 8000
```

Important schema note:

- The model returns 384-dimensional embeddings.
- The current `document_embeddings.embedding_vector` column is `vector(128)`.
- Therefore this run uses `--writeback-mode json_text`. pgvector writeback for 384-dimensional real embeddings should be a separate schema-change experiment.

## Commands

4096-row coalesced baseline:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 384 --model-workers 2 --max-inflight 8 `
  --executor python|ray_task|ray_actor `
  --strategy coalesced `
  --model-backend http_openai `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model all-MiniLM-L6-v2 `
  --writeback-mode json_text --write-batch-rows 256 `
  --warmup-runs 1 --repeats 2 `
  --output motivation\results\gpu\ai_embed_profile.csv
```

1024-row fine/coalesced contrast:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 1024 --total-rows 1024 `
  --db-fetch-rows 256 --ray-batch-rows 256 `
  --embedding-dim 384 --model-workers 2 --max-inflight 8 `
  --executor ray_actor `
  --strategy fine|coalesced `
  --model-backend http_openai `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model all-MiniLM-L6-v2 `
  --writeback-mode json_text --write-batch-rows 256 `
  --warmup-runs 1 --repeats 2 `
  --output motivation\results\gpu\ai_embed_profile.csv
```

## Results

The table uses only `phase=formal` rows and averages two formal repeats.

`model_service_s` is a legacy CSV field name. In this report it means the **sum of embedding HTTP request durations**, not the wall-clock duration of the model stage. Because requests can overlap, it may be larger than `e2e_s`.

| rows | executor | strategy | objects | invocations | e2e_s | rows/s | model request time sum (`model_service_s`) | bounded_wait_s | fanin_s | writeback_s | avg GPU util % |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1024 | `ray_actor` | `coalesced` | 4 | 4 | 0.990 | 1034.0 | 0.451 | 0.000 | 0.002 | 0.402 | 24.5 |
| 1024 | `ray_actor` | `fine` | 1024 | 1024 | 13.458 | 76.1 | 24.648 | 12.027 | 0.165 | 0.394 | 13.5 |
| 4096 | `python` | `coalesced` | 16 | 16 | 3.436 | 1192.3 | 1.808 | 0.000 | 0.000 | 1.595 | 7.5 |
| 4096 | `ray_task` | `coalesced` | 16 | 16 | 3.175 | 1290.5 | 2.705 | 0.000 | 0.007 | 1.594 | 6.5 |
| 4096 | `ray_actor` | `coalesced` | 16 | 16 | 3.165 | 1294.2 | 2.561 | 0.000 | 0.007 | 1.554 | 6.0 |

## Interpretation

Local experiment facts:

- Real CUDA embedding is now in the loop. The endpoint reported CUDA and returned 384-dimensional embeddings.
- At 1024 rows, fine-grained actor execution is about `13.6x` slower than coalesced actor execution (`13.458s / 0.990s`).
- The fine run makes 1024 model endpoint calls; the coalesced run makes 4 calls. This is the main reason for the large gap.
- Fine-grained execution also creates queue/backpressure symptoms: average `bounded_wait_s=12.027s`.
- At 4096 rows with coalesced batches, Python, Ray task, and Ray actor are close: `3.436s`, `3.175s`, and `3.165s`.
- At 4096 rows, writeback is already a large wall-clock stage: about `1.55s-1.60s`.
- Do not read `model_service_s` as model-stage wall-clock time. It is the sum of all model request durations; for concurrent runs, summed request time can exceed elapsed time.
- `db_fetch_s` and `arrow_build_s` are small in these runs, so they are not the current bottleneck.

Reasonable inference:

- The strongest immediate motivation is not only "Ray vs no Ray"; it is avoiding row-by-row model endpoint calls in database AI operator pipelines.
- Once batching is correct, writeback becomes a major cost on this local PostgreSQL path. The next optimization target should include 384-dimensional pgvector schema/writeback or a staging/COPY-style writeback path.
- Ray task and Ray actor remain important baselines, but with this single local endpoint, they do not yet show a large advantage over Python coalesced execution.

Cannot claim:

- This is not a PostgreSQL 18.3 internal-platform result. The database is local PostgreSQL 18.4.
- GPU utilization is sampled through `nvidia-smi` snapshots from the profile driver, not a continuous GPU trace.
- This endpoint is a minimal local HTTP service, not Ray Serve, vLLM, or a production model service.
- The current result cannot prove final scheduling or routing gains. It only establishes the first real-model E2E profile and the first real-model batching/fine-grained contrast.
- pgvector writeback for real embeddings is not tested here because the current vector column is `vector(128)` while the model returns 384 dimensions.

## Next Step

The next GPU-backed experiment should keep the same endpoint and change one variable at a time:

1. Add a 384-dimensional pgvector output column or table, then compare `json_text` vs pgvector writeback.
2. Run a scaling matrix at 1024 / 4096 / 16384 rows with coalesced batches.
3. Add bounded vs larger `max_inflight` comparisons against the same endpoint.
4. If the endpoint remains the bottleneck, replace the minimal HTTP server with Ray Serve or another production-like model-service layer.
