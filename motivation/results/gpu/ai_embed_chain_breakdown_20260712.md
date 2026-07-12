# GPU-Backed AI_EMBED Chain Breakdown, 2026-07-12

## Question

This experiment asks whether the real GPU-backed `AI_EMBED` chain has measurable, decomposable external-link costs after PostgreSQL triggers the workload.

The chain is:

```text
PostgreSQL fetch
  -> Arrow / batch construction
  -> Python / Ray task / Ray actor external operator
  -> HTTP embedding endpoint on CUDA
  -> fan-in
  -> PostgreSQL writeback
```

Raw CSV:

```text
motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv
```

The earlier file name suffix `clean` only meant "rerun after fixing timing-field semantics". It is not an experiment concept. The draft file is kept as:

```text
motivation/results/gpu/ai_embed_chain_breakdown_draft_20260712.csv
```

Do not use the draft file for analysis.

## Setup

Environment:

| Item | Value |
|---|---|
| Database | PostgreSQL 18.4 local rehearsal |
| pgvector | 0.8.2 |
| Model endpoint | `http://localhost:8000/v1/embeddings` |
| Endpoint device | CUDA |
| GPU | NVIDIA GeForce RTX 5070 |
| Model | local `all-MiniLM-L6-v2` |
| Embedding dimension | 384 |
| Writeback mode | `json_text` |

Important schema boundary:

- The model returns 384-dimensional embeddings.
- The current pgvector column is `vector(128)`.
- Therefore this experiment writes JSON text, not 384-dimensional pgvector. A separate schema-change experiment is needed for real 384-dim pgvector writeback.

Endpoint startup command:

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

## Timing Fields

| Field | Meaning |
|---|---|
| `e2e_s` | End-to-end wall-clock time for one profiling run |
| `db_fetch_s` | Time to fetch rows from PostgreSQL |
| `arrow_build_s` | Time to build Arrow RecordBatch objects |
| `operator_wall_s` | Wall-clock time spent in the external AI operator stage |
| `model_request_wall_s` | Wall-clock span from first embedding request start to last embedding request end |
| `model_service_s` | Sum of all embedding request durations; kept for compatibility, not a stage share |
| `submit_s` | Local time to submit Ray task/actor calls |
| `bounded_wait_s` | Time waiting because in-flight requests hit the configured limit |
| `fanin_s` | Time spent collecting Ray results |
| `writeback_s` | Time to write results back to PostgreSQL |

`model_service_s` can be larger than `e2e_s` when requests overlap. Use `model_request_wall_s` and `operator_wall_s` for chain decomposition.

## Formal Averages

The table below averages formal repeats only and excludes warm-up.

| Rows | Executor | Strategy | Calls | `e2e_s` | `operator_wall_s` | `model_request_wall_s` | `writeback_s` | `bounded_wait_s` | `rows/s` |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1024 | ray_actor | coalesced | 4 | 0.888 | 0.505 | 0.397 | 0.374 | 0.000 | 1153.9 |
| 1024 | ray_actor | fine | 1024 | 11.925 | 11.528 | 11.423 | 0.386 | 10.124 | 86.0 |
| 4096 | python | coalesced | 16 | 3.353 | 1.784 | 1.805 | 1.542 | 0.000 | 1221.6 |
| 4096 | ray_task | coalesced | 16 | 3.291 | 1.677 | 1.685 | 1.588 | 0.000 | 1250.2 |
| 4096 | ray_actor | coalesced | 16 | 3.355 | 1.677 | 1.587 | 1.651 | 0.000 | 1220.8 |
| 16384 | ray_actor | coalesced | 64 | 13.168 | 6.473 | 6.448 | 6.586 | 0.000 | 1244.3 |

## Interpretation

Local experiment facts:

- For 1024 rows, fine-grained execution makes 1024 endpoint calls and coalesced execution makes 4 calls.
- Fine-grained execution is about `13.4x` slower end-to-end than coalesced execution: `11.925s / 0.888s`.
- In the fine-grained run, `operator_wall_s` is `11.528s` and `bounded_wait_s` is `10.124s`, so the external operator stage dominates the run.
- For 4096 rows, Python, Ray task and Ray actor coalesced runs are close: about `3.29-3.36s`. This does not prove Ray is already better.
- For 16384 rows with Ray actor coalesced, `operator_wall_s` is `6.473s` and `writeback_s` is `6.586s`; both are about half of end-to-end time.

Reasonable inference:

- The strongest current opening motivation is not "GPU is faster"; it is "once a real GPU-backed model service is in the database AI operator path, endpoint call granularity and writeback become first-order system costs."
- Batch/coalescing is a necessary baseline because row-by-row model calls create a very large external-operator wall-clock cost.
- Ray/no-Ray needs a more specific motivation than "Ray is faster". With one local endpoint and 16 coalesced calls, Ray task/actor are only comparable to Python. Ray becomes more relevant when we test multiple model replicas, routing, bounded in-flight, queue-aware scheduling or worker-side writeback.
- The 16384-row result supports the claim that the external link is decomposable: model request wall time and PostgreSQL writeback are both large and separately measurable.

Cannot claim:

- Cannot claim PostgreSQL 18.3 platform performance. This is local PostgreSQL 18.4 rehearsal.
- Cannot claim 384-dimensional pgvector performance. This experiment used JSON text writeback.
- Cannot claim Ray is generally faster. Current results only show Ray is a necessary baseline and future scheduling substrate.
- Cannot claim internal GPU kernel bottlenecks. GPU metrics are `nvidia-smi` snapshots, not a continuous GPU profile.
- Cannot use `model_service_s` as a stage share because it is a sum over requests.

## Opening-Motivation Sentence

For proposal writing, a safe statement is:

> In a local PostgreSQL 18.4 rehearsal of a database-triggered `AI_EMBED` operator, a real CUDA embedding endpoint shows that execution cost is not confined to model inference. Row-by-row endpoint invocation is about `13.4x` slower than batched invocation, and at 16K rows the external operator stage and PostgreSQL writeback each account for about half of end-to-end time. This supports studying the external distributed execution chain of database AI operators, including batch construction, Ray task/actor execution, model-service request control, fan-in and writeback.

## Next Experiments

1. Add a 384-dimensional pgvector writeback schema and compare JSON text vs real vector writeback.
2. Run multiple endpoint replicas or Ray Serve to test whether Ray becomes useful through routing, concurrency control and backpressure.
3. Keep `AI_EMBED` as the first proposal-stage real chain, but promote `AI_COMPLETE` / offline LLM to the next main AI infra workload with token-aware and prefix-aware scheduling.
4. Add figures for proposal materials: fine vs coalesced e2e, 16K stage breakdown, and Python/Ray coalesced comparison.
