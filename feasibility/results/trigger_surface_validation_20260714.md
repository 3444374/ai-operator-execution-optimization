# Trigger surface validation, 2026-07-14

## Question

After adding the pgai SQL surface, do the existing PostgreSQL 18.4 job-table
chain and the new pgai SQL chain both still run?

This is a functional validation and small trigger-surface comparison. It is not
a GPU-backed performance result.

## Scripts

| Script | Role |
|---|---|
| `code/scripts/postgres_ai_operator_profile.py` | Existing PostgreSQL 18.4 job-table simulated trigger chain |
| `code/scripts/pgai_sql_operator_profile.py` | New pgai SQL trigger chain using `ai.ollama_embed(...)` |

## Step 1: PostgreSQL 18.4 Health

The existing `ai-operator-postgres18` container was started directly because a
stopped container with that name already existed. The container reports:

| Item | Value |
|---|---|
| PostgreSQL | 18.4 |
| pgvector | 0.8.2 |

Smoke command:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 256 --total-rows 256 `
  --db-fetch-rows 128 --ray-batch-rows 64 `
  --embedding-dim 128 --model-workers 1 --max-inflight 2 `
  --executor python --model-backend fake --strategy coalesced `
  --warmup-runs 0 --repeats 1 `
  --experiment-id pg18_4_post_migration_health_20260714 `
  --output feasibility\results\pg18_4_post_migration_health_20260714.csv
```

Result:

| Rows | Written | e2e_s | operator_wall_s | writeback_s |
|---:|---:|---:|---:|---:|
| 256 | 256 | 0.341351 | 0.265808 | 0.064750 |

This only proves the local PG18.4 job-table chain still runs after the Docker
environment changes.

## Step 2: pgai SQL 1024-row Smoke

Command:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\pgai_sql_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5433/postgres `
  --setup --seed-rows 1024 --total-rows 1024 `
  --sql-batch-rows 256 --embedding-model all-minilm `
  --warmup-runs 0 --repeats 1 `
  --experiment-id pgai_sql_1024_smoke_20260714 `
  --output feasibility\results\pgai_sql_profile_20260714.csv
```

Result:

| Rows | Written | SQL statements | Embedding dim | e2e_s | rows/s |
|---:|---:|---:|---:|---:|---:|
| 1024 | 1024 | 4 | 384 | 41.019620 | 24.964 |

Boundary:

- PostgreSQL is 17.10, not 18.4.
- The model backend is Ollama CPU inference.
- SQL embedding and pgvector writeback are not separated in this script.
- The result proves the SQL trigger surface can run at 1024 rows; it is not a
  bottleneck conclusion.

## Step 3: Small Trigger-Surface Comparison

The two runs below are not an apples-to-apples performance comparison. They use
different PostgreSQL versions and different writeback paths:

- `job_table`: PG18.4, Python driver, Ollama OpenAI-compatible HTTP endpoint,
  JSON text writeback.
- `pgai_sql`: PG17.10, SQL `ai.ollama_embed(...)`, pgvector `vector(384)`
  writeback.

They are useful only to confirm both trigger surfaces can execute the same
small 1024-row embedding workload.

| Trigger surface | Rows | Batch | Model | Writeback | e2e_s | rows/s |
|---|---:|---:|---|---|---:|---:|
| `job_table` | 1024 | 256 | Ollama `all-minilm` via `/v1/embeddings` | JSON text | 7.805081 | 131.197 |
| `pgai_sql` | 1024 | 256 | Ollama `all-minilm` via `ai.ollama_embed` | `vector(384)` | 40.963752 | 24.998 |

Do not cite the time ratio as a performance conclusion. The key validated fact
is that both trigger surfaces are runnable and produce 1024 embeddings.

## Local Facts

- PG18.4 job-table smoke is still healthy.
- pgai SQL can generate 384-dimensional embeddings for 1024 rows.
- Ollama `all-minilm` is available in the Docker named volume
  `ai-operator-pgai_pgai_ollama`.
- The new reusable script is `code/scripts/pgai_sql_operator_profile.py`.

## Cannot Claim

- Cannot claim PostgreSQL 18.3 internal platform behavior.
- Cannot claim PG18.4 pgai behavior; the pgai container is PG17.10.
- Cannot claim GPU-backed behavior; this pgai validation used CPU Ollama.
- Cannot claim pgai SQL is slower or faster from this run because the two paths
  differ in PostgreSQL version, driver path, and writeback format.

## Next Step

If this trigger-surface evidence is useful, the next engineering step is to add
a controlled trigger mode to the main profiler:

```text
--operator-surface job_table|pgai_sql
```

That should only be done once we decide the comparison needs identical data,
identical writeback, repeated runs, and a clearer CPU/GPU model-service boundary.
