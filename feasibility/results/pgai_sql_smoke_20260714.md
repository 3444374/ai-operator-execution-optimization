# pgai SQL embedding smoke validation, 2026-07-14

## Question

Can a PostgreSQL SQL surface call a real embedding function through pgai and write
the result into a pgvector column?

This is a functional smoke validation only. It is not a performance experiment.

## Setup

| Item | Value |
|---|---|
| Compose file | `deploy/pgai/compose.yaml` |
| Database container | `ai-operator-pgai-db` |
| Model service container | `ai-operator-pgai-ollama` |
| Database port | `localhost:5433` |
| PostgreSQL | 17.10 |
| pgai extension | 0.11.2 |
| pgvector extension | 0.8.4 |
| Ollama image | `ollama/ollama:0.31.2` |
| Model | `all-minilm:latest` |
| Model storage | Docker named volume `ai-operator-pgai_pgai_ollama` |

The model is not stored under the project `.cache/` directory. It is stored in
the Ollama Docker named volume. If Docker Desktop's data disk has been moved to
D:, the physical storage is inside Docker's D: data disk, not in the Git
workspace.

## Commands

Start the environment:

```powershell
docker compose -f deploy/pgai/compose.yaml up -d
```

Pull the embedding model:

```powershell
docker compose -f deploy/pgai/compose.yaml exec ollama ollama pull all-minilm
```

Verify installed extensions:

```powershell
docker exec ai-operator-pgai-db psql -U postgres -d postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('ai','vector','plpython3u') ORDER BY extname;"
```

Run SQL smoke validation:

```powershell
Get-Content deploy\pgai\smoke_ai_embed.sql | docker exec -i ai-operator-pgai-db psql -U postgres -d postgres
```

## Result

The database container became healthy, and the required extensions were present:

| Extension | Version |
|---|---|
| `ai` | 0.11.2 |
| `plpython3u` | 1.0 |
| `vector` | 0.8.4 |

Ollama model list after pull:

| Model | Size |
|---|---:|
| `all-minilm:latest` | 45 MB |

The smoke SQL created three rows, called `ai.ollama_embed('all-minilm', text)`,
and inserted the returned embeddings into a `vector(384)` column.

Observed output:

| doc_id | embedding_dims |
|---:|---:|
| 1 | 384 |
| 2 | 384 |
| 3 | 384 |

## Interpretation

Local validation facts:

- pgai SQL function calling is available in the isolated `deploy/pgai/` environment.
- The SQL path can generate embeddings through Ollama and write them to pgvector.
- This is a real SQL-triggered embedding call, unlike the earlier `ai_operator_jobs`
  simulation in `postgres_ai_operator_profile.py`.

Cannot claim:

- This is not a PostgreSQL 18.4 result; the pgai container runs PostgreSQL 17.10.
- This is not a PostgreSQL 18.3 internal platform result.
- This is not a GPU-backed result; the Ollama log reports CPU inference.
- This is not a performance result; it only proves the SQL trigger surface works.

## Next Step

Use this environment as a reference SQL surface for a future
`job_table|pgai_sql` trigger-mode comparison in `postgres_ai_operator_profile.py`.
Formal bottleneck and optimization claims should still come from GPU-backed
end-to-end profiles under `motivation/results/`.
