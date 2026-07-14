# GPU-Backed pgvector(384) Writeback Test, 2026-07-14

## Question

Compare three sink modes in the same GPU-backed `AI_EMBED` chain:

```text
no writeback
JSON text writeback
pgvector vector(384) writeback
```

The goal is not to prove an optimization, but to check whether the previous
JSON text writeback result was overstating or understating the cost of a real
vector sink.

## Environment

| Item | Value |
|---|---|
| Database | PostgreSQL 18.4 local rehearsal |
| pgvector | 0.8.2 |
| Trigger/profile surface | `job_table` external execution profile |
| Executor | Ray actor |
| Model service | `code/scripts/local_embedding_server.py` |
| Endpoint | `http://localhost:8000/v1/embeddings` |
| Model | `.cache/models/all-MiniLM-L6-v2` |
| Endpoint health | `device=cuda`, `embedding_dim=384` |
| GPU | NVIDIA GeForce RTX 5070 |
| CSV | `motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv` |

The profile table was migrated by `postgres_ai_operator_profile.py --setup
--embedding-dim 384` so `document_embeddings.embedding_vector` is
`vector(384)`.

Database verification after the pgvector run:

```text
non-null embedding_vector rows = 4096
min(vector_dims(embedding_vector)) = 384
max(vector_dims(embedding_vector)) = 384
```

## Command Template

Only `--writeback-mode` changed across the three groups.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 384 `
  --model-backend http_openai `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model local-embedding `
  --executor ray_actor --strategy coalesced `
  --model-workers 2 --max-inflight 8 `
  --writeback-mode none|json_text|pgvector `
  --write-batch-rows 512 `
  --warmup-runs 1 --repeats 3 `
  --output motivation\results\gpu\ai_embed_pgvector_writeback_20260714.csv
```

## Results

Formal repeats only; warm-up rows are excluded.

| writeback_mode | e2e_s mean | e2e_s std | model_request_wall_s mean | operator_wall_s mean | writeback_s mean | writeback_s std | rows/s mean |
|---|---:|---:|---:|---:|---:|---:|---:|
| none | 1.635 | 0.013 | 1.518 | 1.609 | 0.000 | 0.000 | 2505.0 |
| json_text | 3.198 | 0.026 | 1.516 | 1.603 | 1.567 | 0.041 | 1280.8 |
| pgvector | 2.524 | 0.022 | 1.512 | 1.600 | 0.897 | 0.006 | 1623.2 |

## Interpretation

Local experiment facts:

- The three groups used the same Ray actor, batch, endpoint, row count, and
  embedding dimension. The changed variable was the sink mode.
- `model_request_wall_s` stayed around 1.51-1.52 s and `operator_wall_s` stayed
  around 1.60 s, so the difference is not from the GPU model stage.
- JSON text writeback took 1.567 s on average.
- pgvector `vector(384)` writeback took 0.897 s on average.
- End-to-end time was 3.198 s with JSON text writeback and 2.524 s with
  pgvector `vector(384)` writeback.

Reasonable inference:

- In this local PG18.4 rehearsal, JSON text writeback was a conservative sink:
  it was slower than pgvector `vector(384)` for the same 4096 embeddings.
- pgvector writeback still remains a visible end-to-end component. It reduces
  sink time relative to JSON text, but it does not remove the storage-side cost.

## Limits

Do not claim:

- This is a PostgreSQL 18.3 internal platform result.
- This is pgai SQL performance. The timing profile uses the job-table external
  execution path so stage boundaries are visible.
- This is a final storage optimization result. It is a sink-mode motivation
  ablation on one local machine.
- pgvector is always faster than JSON text. This run covers one model, 4096
  rows, `vector(384)`, one local PostgreSQL instance, and one write batch size.

## Next Step

Use this result as the baseline for writeback design experiments:

```text
driver fan-in writeback
worker-side writeback
queue/vectorizer-like writeback
Lance or external vector sink
```

Those follow-up tests should keep the same GPU-backed chain and vary only the
result landing path.
