# PGAI-Integrated GPU-Backed Key Rerun, 2026-07-14

## Question

After the pgai SQL trigger surface was integrated and validated, rerun the key
AI_EMBED timing experiments on the real local GPU-backed embedding endpoint.

This run answers four narrow questions:

1. Does batch granularity visibly affect end-to-end time?
2. How much of the full chain is model request time versus writeback time?
3. Does one endpoint versus two local endpoint replicas change the operator
   stage?
4. How does the same chain scale from 4096 to 8192 rows?

## Environment

| Item | Value |
|---|---|
| Date | 2026-07-14 |
| Database | PostgreSQL 18.4 local rehearsal |
| pgvector | 0.8.2 |
| Trigger surface | `job_table` external execution profile |
| Model service | `code/scripts/local_embedding_server.py` |
| Endpoint 1 | `http://localhost:8000/v1/embeddings` |
| Endpoint 2 | `http://localhost:8001/v1/embeddings` |
| Model | `.cache/models/all-MiniLM-L6-v2` |
| Device reported by endpoint | `cuda` |
| GPU | NVIDIA GeForce RTX 5070 |
| Embedding dimension | 384 |
| CSV | `motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv` |

The pgai SQL surface was already validated separately in
`feasibility/results/trigger_surface_validation_20260714.md`. The GPU rerun here
uses the established job-table profile path because it exposes the external
execution and writeback timing boundaries.

## Commands

Endpoint startup:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cuda `
  --batch-size 64 `
  --port 8000
```

Second endpoint for the multi-endpoint test:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cuda `
  --batch-size 64 `
  --port 8001
```

Representative profile command:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 384 `
  --executor python `
  --model-backend http_openai `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model local-embedding `
  --writeback-mode json_text `
  --strategy coalesced `
  --warmup-runs 0 --repeats 1 `
  --output motivation\results\gpu\ai_embed_pgai_integrated_key_20260714.csv
```

Warm-up was not separated in this rerun. Each row is one formal run.

## Results

| Experiment | Rows | Executor | Strategy | Writeback | Endpoints | Invocations | e2e_s | model_request_wall_s | operator_wall_s | writeback_s | rows/s |
|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| batch coalesced | 1024 | python | coalesced | none | 1 | 4 | 0.550 | 0.543 | 0.537 | 0.000 | 1861.3 |
| batch fine | 1024 | python | fine | none | 1 | 1024 | 20.614 | 20.607 | 20.597 | 0.000 | 49.7 |
| chain no writeback | 4096 | python | coalesced | none | 1 | 16 | 1.944 | 1.936 | 1.915 | 0.000 | 2106.7 |
| chain JSON writeback | 4096 | python | coalesced | json_text | 1 | 16 | 3.420 | 1.855 | 1.834 | 1.557 | 1197.6 |
| Ray actor single endpoint | 4096 | ray_actor | coalesced | json_text | 1 | 16 | 3.621 | 1.933 | 2.009 | 1.585 | 1131.2 |
| Ray actor dual endpoint | 4096 | ray_actor | coalesced | json_text | 2 | 16 | 2.862 | 1.204 | 1.292 | 1.541 | 1431.2 |
| scale JSON writeback | 8192 | python | coalesced | json_text | 1 | 32 | 7.100 | 3.930 | 3.903 | 3.159 | 1153.8 |

## Interpretation

Local experiment facts:

- Batch granularity has a large visible effect. With 1024 rows, 4 coalesced
  embedding calls finished in 0.550 s, while 1024 fine-grained calls took
  20.614 s.
- With 4096 coalesced rows on one endpoint, no-writeback e2e time was 1.944 s.
  Adding JSON writeback raised e2e time to 3.420 s, with 1.557 s spent in
  writeback.
- In the 4096-row Ray actor test, a second local endpoint reduced
  `model_request_wall_s` from 1.933 s to 1.204 s and e2e time from 3.621 s to
  2.862 s.
- From 4096 to 8192 rows on one endpoint with JSON writeback, e2e time grew
  from 3.420 s to 7.100 s, and writeback time grew from 1.557 s to 3.159 s.

Reasonable inference:

- Once the model endpoint is GPU-backed, writeback becomes a large part of the
  full-chain time for this local workload. Optimizing only model-service
  routing will not remove the writeback cost.
- Coalescing requests is necessary before judging Ray or endpoint scaling,
  because fine-grained per-row calls dominate the whole chain.
- Multi-endpoint routing can reduce the external operator stage, but the e2e
  gain is bounded by downstream writeback.

## Limits

Do not claim:

- These are PostgreSQL 18.3 internal platform results. They are PostgreSQL 18.4
  local rehearsal results.
- These are pgai SQL performance results. The pgai SQL surface is validated in
  feasibility, but this GPU rerun uses the job-table external execution profile
  to expose timing boundaries.
- These are multi-GPU results. Both `8000` and `8001` are local service replicas
  on the same RTX 5070.
- This rerun does not include 384-dim pgvector writeback. It used JSON text
  writeback; the follow-up pgvector sink comparison is recorded in
  `motivation/results/gpu/pgvector_writeback_20260714.md`.
- This is a final optimization result. It is a motivation/profile rerun with
  one formal repeat per setting.

## Next Step

The strict follow-up was completed in
`motivation/results/gpu/pgvector_writeback_20260714.md`. The same GPU-backed
Ray actor chain now compares:

```text
no writeback
JSON text writeback
pgvector vector(384) writeback
```

That result should now be used when discussing database sink and vector landing
costs in the same full-chain timing framework.
