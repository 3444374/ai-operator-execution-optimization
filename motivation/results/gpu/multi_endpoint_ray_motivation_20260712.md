# Multi-Endpoint Ray Motivation Test, 2026-07-12

## Question

The previous chain-breakdown experiment showed that Python, Ray task and Ray actor were close when using one local GPU endpoint and 16 coalesced batches. This experiment asks a narrower question:

> If there are two GPU-backed embedding endpoints, does Ray start to show value by concurrently routing batches to multiple endpoints?

This is a motivation-strengthening experiment, not a final evaluation.

Raw CSV:

```text
motivation/results/gpu/ai_embed_multi_endpoint_20260712.csv
```

## Setup

Environment:

| Item | Value |
|---|---|
| Database | PostgreSQL 18.4 local rehearsal |
| Model | local `all-MiniLM-L6-v2` |
| Endpoint 1 | `http://localhost:8000/v1/embeddings` |
| Endpoint 2 | `http://localhost:8001/v1/embeddings` |
| Endpoint device | CUDA for both endpoints |
| GPU | NVIDIA GeForce RTX 5070 |
| Writeback | JSON text |

The profile driver now supports:

```text
--embedding-endpoint-urls url1,url2
```

Routing policy in this experiment:

- Python baseline: round-robin endpoints, but calls are still sequential.
- Ray task: round-robin endpoints across submitted tasks.
- Ray actor: two actors, each bound round-robin to one endpoint.

This means Python can see both endpoints, but cannot exploit endpoint concurrency in this script. Ray can exploit concurrency by running multiple tasks/actors in parallel.

## Formal Averages

Formal repeats only:

| Rows | Executor | Endpoints | Calls | `e2e_s` | `operator_wall_s` | `model_request_wall_s` | `writeback_s` | `rows/s` |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 4096 | python | 2 | 16 | 3.444 | 1.845 | 1.865 | 1.573 | 1189.3 |
| 4096 | ray_task | 2 | 16 | 2.761 | 1.144 | 1.153 | 1.591 | 1484.1 |
| 4096 | ray_actor | 2 | 16 | 2.780 | 1.188 | 1.099 | 1.565 | 1473.3 |
| 16384 | ray_actor | 2 | 64 | 11.100 | 4.628 | 4.620 | 6.363 | 1476.3 |

Reference from the previous single-endpoint chain-breakdown experiment:

| Rows | Executor | Endpoints | `e2e_s` | `operator_wall_s` | `model_request_wall_s` | `writeback_s` |
|---:|---|---:|---:|---:|---:|---:|
| 4096 | ray_actor | 1 | 3.355 | 1.677 | 1.587 | 1.651 |
| 16384 | ray_actor | 1 | 13.168 | 6.473 | 6.448 | 6.586 |

## Interpretation

Local experiment facts:

- At 4096 rows, two-endpoint Ray task reduces e2e from Python's `3.444s` to `2.761s`, about `1.25x` faster.
- At 4096 rows, Ray actor is similar to Ray task: `2.780s`.
- Ray reduces the external AI operator stage more clearly than total e2e: Python `operator_wall_s = 1.845s`, Ray task `1.144s`, Ray actor `1.188s`.
- At 16384 rows, two-endpoint Ray actor reduces `operator_wall_s` from the previous single-endpoint `6.473s` to `4.628s`.
- At 16384 rows, total e2e only improves from `13.168s` to `11.100s` because writeback is still about `6.36s`.

Reasonable inference:

- Ray's value is not proven by the single-endpoint experiment, but it starts to appear when there are multiple endpoints to route work to concurrently.
- The main visible Ray benefit here is reducing external AI operator wall time, not reducing PostgreSQL writeback.
- This supports keeping Ray as a scheduling substrate for later multi-replica, routing and backpressure experiments.
- The next bottleneck after improving operator concurrency is writeback, so worker-side writeback or alternative vector-store writeback should be tested.

Cannot claim:

- Cannot claim Ray is generally faster in all settings.
- Cannot claim this is a real multi-GPU experiment. Both endpoints run locally on the same GPU.
- Cannot claim Ray Serve / vLLM routing results. This is a minimal OpenAI-compatible endpoint test.
- Cannot claim final system contribution. This is a motivation-strengthening test.

## Proposal Use

A safe proposal sentence is:

> In the single-endpoint setting, Ray and Python were close, so Ray's value was not established. A follow-up two-endpoint test shows that Ray task/actor can reduce the external AI operator wall time by routing coalesced batches concurrently to multiple model endpoints, but end-to-end gains are capped by PostgreSQL writeback. This motivates studying model-service routing, concurrency control, backpressure and writeback together rather than treating Ray as automatically beneficial.

## Next Steps

1. Replace the two local endpoints with Ray Serve or vLLM-style model replicas.
2. Test bounded vs unbounded in-flight under multiple endpoints.
3. Add worker-side writeback to check whether writeback remains the cap.
4. Move this mechanism to `AI_COMPLETE` / offline LLM, where token-aware routing and prefix locality should matter more than row-count batching.
