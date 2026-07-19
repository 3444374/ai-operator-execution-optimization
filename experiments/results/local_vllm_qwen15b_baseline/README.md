# Local vLLM Qwen2.5-1.5B AI_COMPLETE Baseline

Date: 2026-07-18

## Status

The first CSV in this directory used the legacy synthetic prompt seed:

```text
document <id> tenant <id> category <id> token token ...
```

That run is useful only as a local path smoke and static row-batch sanity check. It should not be used as the final baseline for strategy comparisons.

The comparable baseline for later optimized policies should use the same final workload as the optimized runs. In this project, the first controlled text workload is:

```text
--seed-workload ai_complete_controlled --reset-documents
```

This workload has short/medium/long prompt buckets and repeated task prefixes for `AI_COMPLETE` scheduling experiments.

## Question

Establish a local baseline for the text `AI_COMPLETE` path before implementing token-aware or queue-adaptive scheduling policies:

```text
PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> vLLM OpenAI-compatible completions -> no writeback
```

The legacy run below measures fixed row-batch sizes only. Treat it as a smoke baseline until the controlled workload run is recorded.

## Environment

- GPU: NVIDIA GeForce RTX 5070, 12 GB VRAM.
- Model service: `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404`.
- Model: local Hugging Face files at `models/Qwen2.5-1.5B-Instruct`.
- Served model name: `qwen2.5-1.5b`.
- vLLM startup used `--max-model-len 2048`, `--gpu-memory-utilization 0.75`, and `--enforce-eager`.
- Database: local Docker PostgreSQL on `localhost:5432`.
- Script: `code/scripts/postgres_ai_operator_profile.py`.

Boundary: this is a local PG rehearsal. Do not cite it as a PostgreSQL 18.3 internal-platform result.

## Command

```powershell
$env:NO_PROXY='localhost,127.0.0.1,::1'
$env:no_proxy='localhost,127.0.0.1,::1'
$output = 'experiments\results\local_vllm_qwen15b_baseline\static_batch_sweep.csv'
$batchRows = @(1,2,4,8,16)
foreach ($batch in $batchRows) {
  $experiment = "vllm_qwen15b_static_batch_${batch}"
  .conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
    --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
    --setup `
    --seed-rows 32 `
    --total-rows 32 `
    --db-fetch-rows 32 `
    --ray-batch-rows $batch `
    --operator ai_complete `
    --executor python `
    --model-backend compatible_http `
    --completion-endpoint-url http://localhost:8000/v1/completions `
    --completion-model qwen2.5-1.5b `
    --completion-max-tokens 8 `
    --completion-request-timeout-s 180 `
    --data-source daft_postgres `
    --organizer daft `
    --writeback-mode none `
    --warmup-runs 1 `
    --repeats 3 `
    --experiment-id $experiment `
    --output $output
}
```

CSV: `experiments/results/local_vllm_qwen15b_baseline/static_batch_sweep.csv`

## Ray Rerun On ShareGPT/BurstGPT

CSV:
`experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv`

This rerun uses the imported `sharegpt_burstgpt` workload and the full local
execution path:

```text
PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> Ray -> vLLM completions -> no writeback
```

The workload was re-imported with the local
`models\Qwen2.5-1.5B-Instruct` tokenizer so `prompt_tokens` match the serving
model. Rows are filtered with `prompt_tokens + completion_max_tokens <= 2048`
for the local vLLM server configured with `max_model_len=2048`.

Command shape:

```powershell
$output = 'experiments\results\local_vllm_qwen15b_baseline\sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv'
$executors = @('ray_task', 'ray_actor')
$batches = @(1, 2, 4, 8, 16, 32)
foreach ($executor in $executors) {
  foreach ($batch in $batches) {
    $experiment = "sharegpt_burstgpt_${executor}_batch${batch}"
    .conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
      --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
      --setup `
      --total-rows 128 `
      --db-fetch-rows 128 `
      --ray-batch-rows $batch `
      --operator ai_complete `
      --executor $executor `
      --model-backend compatible_http `
      --completion-endpoint-url http://localhost:8000/v1/completions `
      --completion-model qwen2.5-1.5b `
      --completion-max-tokens 16 `
      --completion-request-timeout-s 180 `
      --data-source daft_postgres `
      --source-workload-name sharegpt_burstgpt `
      --source-order doc_id `
      --organizer daft `
      --writeback-mode none `
      --warmup-runs 1 `
      --repeats 3 `
      --experiment-id $experiment `
      --output $output
  }
}
```

All 48 CSV rows completed with `status=ok`: 12 warmup rows and 36 formal rows.
The table below excludes warmup rows.

| executor | row batch size | formal repeats | mean rows/s | mean e2e_s | mean operator_wall_s | mean model_request_wall_s | model calls |
|---|---:|---:|---:|---:|---:|---:|---:|
| ray_actor | 1 | 3 | 9.283 | 13.792 | 13.664 | 12.251 | 128 |
| ray_actor | 2 | 3 | 16.117 | 7.942 | 7.841 | 6.413 | 64 |
| ray_actor | 4 | 3 | 27.332 | 4.691 | 4.594 | 3.155 | 32 |
| ray_actor | 8 | 3 | 40.874 | 3.132 | 3.047 | 1.602 | 16 |
| ray_actor | 16 | 3 | 51.749 | 2.474 | 2.397 | 0.937 | 8 |
| ray_actor | 32 | 3 | 59.128 | 2.165 | 2.088 | 0.644 | 4 |
| ray_task | 1 | 3 | 36.067 | 3.550 | 3.432 | 3.426 | 128 |
| ray_task | 2 | 3 | 68.343 | 1.876 | 1.774 | 1.767 | 64 |
| ray_task | 4 | 3 | 124.037 | 1.032 | 0.949 | 0.942 | 32 |
| ray_task | 8 | 3 | 173.845 | 0.736 | 0.660 | 0.652 | 16 |
| ray_task | 16 | 3 | 259.802 | 0.493 | 0.418 | 0.410 | 8 |
| ray_task | 32 | 3 | 245.994 | 0.521 | 0.448 | 0.441 | 4 |

Source-order boundary: this rerun uses `--source-order doc_id`, so it is an
offline throughput scan over rows already present in PostgreSQL. It should not
be interpreted as an arrival-aware service scheduling experiment.

Interpretation boundary: this is a fixed row-batch Ray baseline on one local
vLLM server. It shows that the current static row batch has a useful local
control point near 16-32 rows for `ray_task`, but it does not prove the final
token-aware or queue-adaptive policy.

## Token-tail Baseline Revision

CSV:
`experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv`

This revision changes the baseline question. The old row-batch sweep answers
"how many rows per Ray invocation gives good local throughput?" It does not
explain why fixed row batches are a weak proxy for model cost. The revised
baseline therefore records token-tail and service-tail metrics for each fixed
row batch size, including `64` and `128` as stress points.

Experimental question:

```text
Does increasing fixed row batch size continue to help, or does the heavier
per-request token tail eventually offset the reduction in model-service calls?
```

Command shape:

```powershell
$env:NO_PROXY='localhost,127.0.0.1,::1'
$env:no_proxy='localhost,127.0.0.1,::1'
$output = 'experiments\results\local_vllm_qwen15b_baseline\sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv'
$batches = @(1,2,4,8,16,32,64,128)
foreach ($batch in $batches) {
  $experiment = "sharegpt_burstgpt_ray_task_token_sweep_batch${batch}"
  .conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
    --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
    --setup `
    --total-rows 512 `
    --db-fetch-rows 512 `
    --ray-batch-rows $batch `
    --operator ai_complete `
    --executor ray_task `
    --model-backend compatible_http `
    --completion-endpoint-url http://localhost:8000/v1/completions `
    --completion-model qwen2.5-1.5b `
    --completion-max-tokens 16 `
    --completion-request-timeout-s 180 `
    --model-metrics-url http://localhost:8000/metrics `
    --data-source daft_postgres `
    --source-workload-name sharegpt_burstgpt `
    --source-order doc_id `
    --organizer daft `
    --writeback-mode none `
    --warmup-runs 1 `
    --repeats 3 `
    --experiment-id $experiment `
    --output $output
}
```

All 32 CSV rows completed with `status=ok`: 8 warmup rows and 24 formal rows.
The table below excludes warmup rows.

| row batch size | formal repeats | mean rows/s | model calls | max inflight seen | token P50 | token P95 | token max | service P50 (s) | service P95 (s) | vLLM queue mean (s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3 | 35.609 | 512 | 8 | 38 | 766 | 1867 | 0.206 | 0.254 | 0.000 |
| 2 | 3 | 68.082 | 256 | 8 | 99 | 1229 | 2281 | 0.217 | 0.256 | 0.000 |
| 4 | 3 | 121.270 | 128 | 8 | 259 | 2156 | 3415 | 0.245 | 0.279 | 0.000 |
| 8 | 3 | 186.363 | 64 | 8 | 733 | 2730 | 6108 | 0.313 | 0.343 | 0.000 |
| 16 | 3 | 286.767 | 32 | 8 | 1559 | 5260 | 6793 | 0.404 | 0.448 | 0.000 |
| 32 | 3 | 291.125 | 16 | 8 | 3449 | 11049 | 11049 | 0.780 | 0.879 | 0.000 |
| 64 | 3 | 287.406 | 8 | 8 | 7250 | 16377 | 16377 | 0.977 | 1.651 | 0.332 |
| 128 | 3 | 288.979 | 4 | 4 | 15251 | 26678 | 26678 | 0.941 | 1.644 | 0.341 |

Interpretation:

Fixed row batch size reduces model-service call count, but it also increases
the per-request token tail. In this local run, throughput improves up to
`16-32` rows and then plateaus through `64-128`. From `16` to `128`, model
calls fall from 32 to 4, but token P95 increases from 5260 to 26678 and service
P95 increases from 0.448s to about 1.64s. At `64` and `128`, vLLM mean queue
time is visible. At `128`, only 4 model-service calls are submitted, so the run
cannot fill the configured `max_inflight=8` window. This is the main baseline
signal for dynamic token-budget batching.

Source-order boundary: the 2026-07-19 token-tail sweep also uses
`--source-order doc_id`. It answers the offline data-organization question:
when rows are already available in PostgreSQL, how weak is fixed row count as a
proxy for model cost? Later K_max, queue-adaptive flush, and service-tail
experiments should add `--source-order arrival_time` to preserve BurstGPT-like
arrival order and evaluate request rhythm, queueing, and backpressure.

Boundary:

- `64` and `128` are stress points, not recommended defaults.
- This does not prove that token-budget batching is better; it defines the
  fixed-row baseline that token-budget batching must beat.
- This run has no writeback and uses one local vLLM endpoint.
- The strongest supported claim is that row count is an imprecise proxy for
  model request cost; larger row batches can trade fewer calls for heavier
  token and service-time tails.

Next dynamic-batch comparison should keep the same `total_rows`, executor,
endpoint, model, completion limit, and `max_inflight`, then compare fixed row
batch `16/32/64/128` against token-budget batches such as `4096/6144/8192`
using:

```text
rows_per_s
batch_tokens_p95
batch_service_s_p95
vllm_request_queue_time_mean_s
operator_wall_s
```

## Token-budget vs Fixed Row Batch

CSV:
`experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv`

This run is the first direct data-organization comparison after the fixed-row
token-tail baseline. It keeps the same local ShareGPT/BurstGPT workload, local
vLLM endpoint, `ray_task` executor, `total_rows=512`, `completion_max_tokens=16`,
`max_inflight=8`, Daft source/organizer, and no writeback. The only policy
change is upstream batch construction:

```text
fixed_rows:   ray_batch_rows = 16, 32, 64, 128
token_budget: token_budget = 4096, 6144, 8192
```

The profiling script now records `batching_policy`, `token_budget`, and
`model_request_timeout_s`. This matrix used `model_request_timeout_s=300`.
A prior attempt with the default 120s timeout completed fixed 16/32/64 and one
fixed 128 formal repeat, then hit HTTP timeouts and left vLLM with a large
server queue (`num_requests_running=256`, `num_requests_waiting=100`). That
failed attempt is not used for the table below, but it is useful boundary
evidence that large submissions can exceed the client timeout when the service
queue is already stressed.

All 28 rows in the timeout-300 matrix completed with `status=ok`: 7 warmup rows
and 21 formal rows. The table below excludes warmup rows.

| policy | setting | formal repeats | rows/s | operator_wall_s | model calls | max inflight seen | mean rows/request | token P95 | token max | service P95 (s) | service P99 (s) | vLLM queue mean (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fixed rows | 16 | 3 | 282.729 | 1.684 | 32 | 8 | 16.000 | 5260 | 6793 | 0.444 | 0.454 | 0.000 |
| fixed rows | 32 | 3 | 325.123 | 1.449 | 16 | 8 | 32.000 | 11049 | 11049 | 0.779 | 0.779 | 0.000 |
| fixed rows | 64 | 3 | 325.886 | 1.470 | 8 | 8 | 64.000 | 16377 | 16377 | 1.460 | 1.460 | 0.291 |
| fixed rows | 128 | 3 | 334.649 | 1.423 | 4 | 4 | 128.000 | 26677 | 26677 | 1.416 | 1.416 | 0.269 |
| token budget | 4096 | 3 | 300.986 | 1.590 | 19 | 8 | 26.947 | 4092 | 4092 | 0.635 | 0.635 | 0.000 |
| token budget | 6144 | 3 | 324.431 | 1.474 | 13 | 8 | 39.385 | 6141 | 6141 | 1.368 | 1.368 | 0.015 |
| token budget | 8192 | 3 | 324.832 | 1.475 | 9 | 8 | 56.889 | 8171 | 8171 | 1.451 | 1.451 | 0.181 |

Interpretation:

- Token-budget batching does what it is supposed to do at the data-organization
  layer: it bounds the per-request token tail. `token_budget=4096/6144/8192`
  produced token P95 values close to their configured budgets, while fixed 64
  and fixed 128 reached 16377 and 26677 tokens.
- The throughput result is a tradeoff, not a monotonic win. `token_budget=4096`
  has the smallest token tail and near-zero vLLM queue time, but lower
  throughput than fixed 32/64/128 because it submits more model-service calls.
  `token_budget=6144/8192` keeps throughput close to fixed 32/64 while
  substantially lowering token P95 compared with fixed 64/128.
- Fixed 128 looks fast in this small 512-row offline scan because it submits
  only 4 model-service calls, but that also means it cannot fill the configured
  `max_inflight=8` window. It is not evidence that 128 is generally better;
  it is a stress point with a very heavy token tail.
- This run supports the motivation for dynamic token-budget batching, but it
  does not yet prove the final method. The next experiment must combine this
  policy with arrival-aware `K_max` and queue-adaptive submission, because token
  budget alone controls request shape, not submission rhythm.

Figure:

```text
figures/data/backup/b15_local_vllm_token_budget_throughput.png
figures/data/backup/b15_local_vllm_token_budget_throughput.svg
figures/data/backup/b16_local_vllm_token_budget_tail_queue.png
figures/data/backup/b16_local_vllm_token_budget_tail_queue.svg
```

## Arrival-aware K_max Sweep

CSV:
`experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv`

This preliminary run fixes the request shape at `token_budget=6144` and changes
only the Ray submission window:

```text
K_max = 1, 2, 4, 8, 16, unbounded
```

`unbounded` is implemented as `--max-inflight 100000`; in this 512-row run that
means all 13 token-budget requests can be submitted without client-side
backpressure. The run uses `--source-order arrival_time`, so it is the first
arrival-aware scheduling sweep in this local baseline series.

Command shape:

```powershell
$output = 'experiments\results\local_vllm_qwen15b_baseline\sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv'
$kmaxValues = @(1,2,4,8,16,100000)
foreach ($kmax in $kmaxValues) {
  if ($kmax -eq 100000) { $label = 'unbounded' } else { $label = "k${kmax}" }
  $experiment = "sharegpt_burstgpt_arrival_token6144_${label}"
  .conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
    --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
    --setup `
    --total-rows 512 `
    --db-fetch-rows 512 `
    --ray-batch-rows 32 `
    --batching-policy token_budget `
    --token-budget 6144 `
    --operator ai_complete `
    --executor ray_task `
    --model-backend compatible_http `
    --completion-endpoint-url http://localhost:8000/v1/completions `
    --completion-model qwen2.5-1.5b `
    --completion-max-tokens 16 `
    --completion-request-timeout-s 300 `
    --model-metrics-url http://localhost:8000/metrics `
    --data-source daft_postgres `
    --source-workload-name sharegpt_burstgpt `
    --source-order arrival_time `
    --organizer daft `
    --writeback-mode none `
    --warmup-runs 1 `
    --repeats 3 `
    --max-inflight $kmax `
    --experiment-id $experiment `
    --output $output
}
```

All 24 CSV rows completed with `status=ok`: 6 warmup rows and 18 formal rows.
The table below excludes warmup rows.

| K_max | formal repeats | rows/s | operator_wall_s | max inflight seen | service P95 (s) | service P99 (s) | vLLM queue mean (s) | bounded wait (s) | token P95 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3 | 140.442 | 3.550 | 1.000 | 0.345 | 0.345 | 0.000 | 3.344 | 6141 |
| 2 | 3 | 201.496 | 2.440 | 2.000 | 0.498 | 0.498 | 0.000 | 2.244 | 6141 |
| 4 | 3 | 267.985 | 1.805 | 4.000 | 0.674 | 0.674 | 0.000 | 1.599 | 6141 |
| 8 | 3 | 328.689 | 1.454 | 8.000 | 1.345 | 1.345 | 0.015 | 0.782 | 6138 |
| 16 | 3 | 326.813 | 1.451 | 13.000 | 1.439 | 1.439 | 0.287 | 0.000 | 6141 |
| unbounded | 3 | 318.785 | 1.480 | 13.000 | 1.462 | 1.462 | 0.290 | 0.000 | 6138 |

Interpretation:

- Increasing `K_max` from 1 to 8 improves throughput from about 140 to 329
  rows/s because the client no longer serializes token-budget requests.
- `K_max=8` is the best mean-throughput point in this small local run. Raising
  the limit to 16 or unbounded does not improve throughput because the workload
  only has 13 token-budget requests, but it moves waiting into the vLLM queue:
  queue mean rises from about 0.015s at 8 to about 0.29s at 16/unbounded.
- Token P95 stays near 6144 for every row because the data-organization policy
  is fixed. The observed change is therefore submission rhythm / queueing, not
  token-shape drift.

Boundary: this is a local single-endpoint, no-writeback, 512-row scheduling
sweep with one fixed request shape. It shows that too-small `K_max` serializes
Ray submissions and too-large `K_max` moves waiting into vLLM, but by itself it
does not answer how batch shape and admission control interact. The next matrix
below supersedes it for choosing static baselines. Windows Ray printed shutdown
access-violation logs during process teardown; the CSV rows completed
successfully and vLLM returned to zero running/waiting requests after the run.

Figure:

```text
figures/data/backup/b17_local_vllm_arrival_kmax_sweep.png
figures/data/backup/b17_local_vllm_arrival_kmax_sweep.svg
```

## Batch Policy x K_max Matrix

CSV:
`experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv`

This run is the corrected scheduling experiment. It treats `K_max` as admission
control over already-formed Ray submissions, so it must be tested together with
the upstream batch shape:

```text
fixed_rows:   ray_batch_rows = 32, 64, 128
token_budget: token_budget = 4096, 6144, 8192
K_max:        2, 4, 8, 16, unbounded
```

Common setup: ShareGPT/BurstGPT workload, `source_order=arrival_time`,
`total_rows=512`, `ray_task`, Daft source/organizer, local vLLM
Qwen2.5-1.5B endpoint, `completion_max_tokens=16`, no writeback, 1 warmup and
3 formal repeats. `unbounded` is implemented as `--max-inflight 100000`.

All 120 CSV rows completed with `status=ok`: 30 warmup rows and 90 formal rows.
The table below excludes warmup rows.

| config | K_max | rows/s | e2e_s | operator_wall_s | service P95 (s) | vLLM queue mean (s) | max inflight seen | model calls | token P95 | bounded wait (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fixed32 | 2 | 179.130 | 2.860 | 2.748 | 0.361 | 0.000 | 2 | 16 | 11046 | 2.395 |
| fixed32 | 4 | 277.804 | 1.843 | 1.728 | 0.449 | 0.000 | 4 | 16 | 11049 | 1.289 |
| fixed32 | 8 | 337.457 | 1.517 | 1.422 | 0.759 | 0.000 | 8 | 16 | 11044 | 0.754 |
| fixed32 | 16 | 323.664 | 1.582 | 1.457 | 1.440 | 0.281 | 16 | 16 | 11049 | 0.000 |
| fixed32 | unbounded | 328.370 | 1.559 | 1.453 | 1.441 | 0.284 | 16 | 16 | 11044 | 0.000 |
| fixed64 | 2 | 276.289 | 1.854 | 1.753 | 0.463 | 0.000 | 2 | 8 | 16377 | 1.330 |
| fixed64 | 4 | 322.588 | 1.587 | 1.461 | 0.776 | 0.000 | 4 | 8 | 16372 | 0.776 |
| fixed64 | 8 | 326.700 | 1.567 | 1.448 | 1.437 | 0.279 | 8 | 8 | 16377 | 0.000 |
| fixed64 | 16 | 319.848 | 1.601 | 1.488 | 1.477 | 0.291 | 8 | 8 | 16374 | 0.000 |
| fixed64 | unbounded | 330.267 | 1.550 | 1.437 | 1.427 | 0.276 | 8 | 8 | 16376 | 0.000 |
| fixed128 | 2 | 317.241 | 1.614 | 1.509 | 0.772 | 0.000 | 2 | 4 | 26674 | 0.774 |
| fixed128 | 4 | 328.203 | 1.560 | 1.451 | 1.444 | 0.299 | 4 | 4 | 26674 | 0.000 |
| fixed128 | 8 | 332.441 | 1.540 | 1.427 | 1.418 | 0.268 | 4 | 4 | 26679 | 0.000 |
| fixed128 | 16 | 331.861 | 1.543 | 1.426 | 1.418 | 0.286 | 4 | 4 | 26680 | 0.000 |
| fixed128 | unbounded | 333.934 | 1.533 | 1.413 | 1.405 | 0.260 | 4 | 4 | 26680 | 0.000 |
| token4096 | 2 | 168.847 | 3.032 | 2.915 | 0.380 | 0.000 | 2 | 19 | 4092 | 2.674 |
| token4096 | 4 | 255.817 | 2.003 | 1.900 | 0.474 | 0.000 | 4 | 19 | 4092 | 1.471 |
| token4096 | 8 | 298.774 | 1.714 | 1.593 | 0.676 | 0.001 | 8 | 19 | 4092 | 1.170 |
| token4096 | 16 | 324.998 | 1.576 | 1.460 | 1.426 | 0.173 | 16 | 19 | 4092 | 0.734 |
| token4096 | unbounded | 321.525 | 1.593 | 1.489 | 1.454 | 0.186 | 19 | 19 | 4092 | 0.000 |
| token6144 | 2 | 202.285 | 2.531 | 2.424 | 0.524 | 0.000 | 2 | 13 | 6137 | 2.219 |
| token6144 | 4 | 260.724 | 1.965 | 1.846 | 0.705 | 0.000 | 4 | 13 | 6134 | 1.617 |
| token6144 | 8 | 326.181 | 1.570 | 1.457 | 1.352 | 0.015 | 8 | 13 | 6140 | 0.778 |
| token6144 | 16 | 330.278 | 1.550 | 1.453 | 1.442 | 0.289 | 13 | 13 | 6141 | 0.000 |
| token6144 | unbounded | 327.995 | 1.561 | 1.463 | 1.448 | 0.288 | 13 | 13 | 6141 | 0.000 |
| token8192 | 2 | 254.893 | 2.009 | 1.902 | 0.423 | 0.000 | 2 | 9 | 8171 | 1.491 |
| token8192 | 4 | 299.275 | 1.713 | 1.611 | 0.676 | 0.001 | 4 | 9 | 8171 | 1.164 |
| token8192 | 8 | 324.015 | 1.582 | 1.467 | 1.401 | 0.164 | 8 | 9 | 8171 | 0.692 |
| token8192 | 16 | 332.144 | 1.542 | 1.448 | 1.431 | 0.280 | 9 | 9 | 8168 | 0.000 |
| token8192 | unbounded | 330.775 | 1.548 | 1.444 | 1.431 | 0.281 | 9 | 9 | 8171 | 0.000 |

Interpretation:

- `K_max` and batch shape are coupled. `K_max` only controls the number of
  in-flight Ray submissions that already exist. With `fixed128`, the run has
  only 4 model-service calls, so `K_max=8/16/unbounded` cannot create additional
  useful concurrency.
- Too-small `K_max` hurts end-to-end time by holding requests on the Ray side.
  For example, `token6144` improves from 2.531s E2E at `K_max=2` to 1.570s at
  `K_max=8`.
- Too-large `K_max` stops improving E2E but increases service pressure. For
  `fixed32`, service P95 rises from 0.759s at `K_max=8` to about 1.44s at
  `K_max=16/unbounded`, while vLLM queue mean rises from near zero to about
  0.28s.
- Token must remain in the experiment because fixed row count hides service
  cost. `fixed128` has only 4 calls and good E2E in this short no-writeback run,
  but its token P95 is about 26680. `token4096/6144/8192` bound token P95 to
  their configured budgets and expose a different admission-control problem:
  more, smaller requests need a suitable `K_max`.

Conclusion: the static baseline should not be "batch only" or "K_max only".
It should be a pair `(batch policy, K_max)`, because `K_max` only controls the
submissions created by the upstream batch policy. However, this offline
single-job matrix does not prove that `K_max` is necessary as a standalone
method. In this setup, larger `K_max` mostly improves or plateaus E2E time; its
cost is visible mainly in vLLM queue time and service-tail pressure. To prove
the need for admission control, the next scheduling experiment must use a
scenario where unbounded inflight violates an external objective, such as SLO
tail latency, fairness between concurrent jobs, timeout rate, or writeback
stability.

Figures:

```text
figures/data/backup/b18_local_vllm_batch_kmax_e2e.png
figures/data/backup/b18_local_vllm_batch_kmax_e2e.svg
figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.png
figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.svg
figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.png
figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.svg
```

## Shared-vLLM K_max Interference

CSV:

```text
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv
```

Runner:

```text
code/scripts/run_kmax_interference_experiment.py
```

This experiment targets the missing motivation for `K_max`: if each query has
an exclusive vLLM endpoint, then larger inflight often looks best for that
single query. `K_max` becomes necessary when multiple database AI jobs share a
vLLM service and one bulk job can harm another foreground job.

Design:

```text
foreground small job: 128 rows, fixed16, K_max=8, max_tokens=64
background bulk job: 1024 rows observed in CSV, fixed16, max_tokens=64
background policies: K_max=8 versus unbounded
shared service: both jobs call http://localhost:8000/v1/completions
repeats: 3
```

The foreground-only `solo` rows measure unloaded latency. The
`during_bulk_k8` rows run the foreground job while the background job uses
bounded inflight. The `during_bulk_unbounded` rows run the same foreground job
while the background job submits all 64 background requests.

Foreground small-job results:

| scenario | repeats | small E2E (s) | operator_wall_s | request wall (s) | service P95 (s) | vLLM queue mean (s) | running after | waiting after | rows/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| solo | 3 | 4.923 | 4.043 | 1.424 | 1.318 | 0.000 | 0.000 | 0.000 | 26.132 |
| during bulk K=8 | 3 | 6.535 | 5.382 | 2.683 | 2.580 | 0.001 | 111.667 | 0.000 | 19.724 |
| during bulk unbounded | 3 | 11.384 | 10.065 | 4.557 | 4.386 | 0.445 | 230.333 | 8.333 | 11.244 |

Background bulk-job summary:

| background policy | repeats | bulk E2E (s) | operator_wall_s | service P95 (s) | vLLM queue mean (s) | max inflight seen | model calls |
|---|---:|---:|---:|---:|---:|---:|---:|
| K_max=8 | 3 | 19.364 | 18.570 | 2.394 | 0.012 | 8 | 64 |
| unbounded | 3 | 18.320 | 17.165 | 4.065 | 0.397 | 64 | 64 |

Interpretation:

- This is the first local result that makes `K_max` meaningful: the bulk job is
  slightly faster when unbounded, but the foreground job becomes much worse.
  Small-job E2E rises from 6.535s under bounded bulk to 11.384s under unbounded
  bulk.
- The degradation appears in vLLM service pressure, not Ray bounded wait. Under
  unbounded bulk, foreground vLLM queue mean rises to 0.445s and the small job
  finishes while the shared service still has about 230 running and 8 waiting
  requests.
- Therefore the right claim is not "K_max always improves throughput." The
  claim is: `K_max` is an admission-control guardrail for shared vLLM services;
  it can trade a small amount of bulk-job speed for much better foreground-job
  latency and queue stability.

Boundary: this is still a local single-GPU endpoint and a two-job interference
micro-experiment. It supports the need for a shared-service scheduling
experiment, but the next step should sweep foreground size, background policy,
and arrival offset before making a general fairness/SLO claim.

Figure:

```text
figures/data/backup/b21_local_vllm_kmax_interference_small_job.png
figures/data/backup/b21_local_vllm_kmax_interference_small_job.svg
```

## Data Organization Ablation: Length-align and Prefix-aware

CSV:

```text
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_length_prefix_ablation_20260719.csv
```

Question:

```text
After token-budget batching exists, do length-align and prefix-aware ordering
change the upstream request shape enough to justify treating them as separate
data-organization policies?
```

Design:

```text
workload: ShareGPT/BurstGPT AI_COMPLETE, source_order=arrival_time
total_rows=512, executor=ray_task, organizer=daft, max_inflight=8
completion_max_tokens=16, no writeback
policies: fixed32, token6144, length+fixed32, length+token6144,
          prefix+fixed32, prefix+token6144
repeats: 3 formal + 1 warmup
```

Formal means:

| policy | repeats | rows/s | token P95 | service P95 (s) | queue mean (s) | prompt-token spread | prefix ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| fixed 32 | 3 | 316.914 | 11048.333 | 0.803 | 0.000 | 995.250 | 0.041 |
| token 6144 | 3 | 306.066 | 6141.000 | 1.407 | 0.016 | 1014.077 | 0.039 |
| length + fixed 32 | 3 | 318.227 | 33407.000 | 0.746 | 0.000 | 114.375 | 0.055 |
| length + token 6144 | 3 | 281.618 | 6125.667 | 1.462 | 0.261 | 103.846 | 0.047 |
| prefix + fixed 32 | 3 | 314.193 | 7904.667 | 0.769 | 0.000 | 1092.438 | 0.064 |
| prefix + token 6144 | 3 | 339.385 | 6144.000 | 1.326 | 0.085 | 1248.167 | 0.057 |

Interpretation:

- `length_align_fixed_rows` is a useful negative result. It reduces
  within-batch prompt-token spread from about 995 to about 114, but it also
  concentrates the longest prompts into the same fixed-row request group, so
  token P95 rises to about 33407. Length-align should be paired with a token
  budget before being evaluated as a positive policy.
- `prefix_aware_fixed_rows` lowers token P95 in this workload, but the measured
  same-prefix adjacent-pair ratio only rises from 4.1% to 6.4%. This is too
  weak to claim prefix-cache or APC benefit.
- `prefix_aware_token_budget` is the best point in this small run by rows/s,
  but this is still an organization ablation, not a proof of prefix-aware
  effectiveness. The next prefix-aware experiment needs controlled prefix-share
  ratios and vLLM APC/cache metrics if available.

Figures:

```text
figures/data/backup/b22_local_vllm_length_prefix_tail.png
figures/data/backup/b22_local_vllm_length_prefix_tail.svg
figures/data/backup/b23_local_vllm_length_prefix_signal.png
figures/data/backup/b23_local_vllm_length_prefix_signal.svg
```

## Shared-vLLM K_max and Queue-adaptive Sweep

CSV:

```text
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_small_20260719.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_bulk_20260719.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_small_20260719.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_bulk_20260719.csv
```

Question:

```text
If a 1024-row background job and a 128-row foreground job share the same vLLM
endpoint, does larger background inflight continue to help, or does it mostly
increase queue/service pressure for the foreground job?
```

Design:

```text
foreground small job: fixed16, K_max=8, 128 rows
background bulk job: fixed16, 1024 rows
background static policies: K_max=8, K_max=16, unbounded
background adaptive policy: first permissive run min=8/max=64; tuned run
                            min=8/max=16/running-threshold=64
shared service: one local vLLM Qwen2.5-1.5B endpoint
repeats: 3 formal
```

Foreground small-job means:

| background policy | repeats | E2E (s) | request wall (s) | service P95 (s) | queue mean (s) | rows/s |
|---|---:|---:|---:|---:|---:|---:|
| solo | 3 | 5.048 | 1.595 | 1.451 | 0.000 | 25.398 |
| K=8 | 3 | 7.289 | 2.625 | 2.387 | 0.000 | 17.800 |
| K=16 | 3 | 10.900 | 4.210 | 3.996 | 0.313 | 11.975 |
| unbounded | 3 | 9.474 | 4.273 | 4.009 | 0.213 | 13.547 |
| adaptive tuned | 3 | 10.214 | 3.536 | 3.311 | 0.227 | 12.771 |

Background bulk-job means:

| background policy | repeats | E2E (s) | rows/s | service P95 (s) | queue mean (s) | max inflight | downshifts | upshifts | limit mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| K=8 | 3 | 19.463 | 53.128 | 2.459 | 0.017 | 8.000 | 0.000 | 0.000 | 8.000 |
| K=16 | 3 | 19.363 | 52.949 | 4.273 | 0.371 | 16.000 | 0.000 | 0.000 | 16.000 |
| unbounded | 3 | 19.266 | 53.217 | 4.304 | 0.341 | 64.000 | 0.000 | 0.000 | 100000.000 |
| adaptive tuned | 3 | 21.073 | 48.606 | 2.578 | 0.076 | 16.000 | 102.000 | 18.000 | 9.200 |

Interpretation:

- This formal sweep strengthens the `K_max` motivation: increasing background
  inflight from 8 to 16/unbounded does not materially improve background
  throughput, but foreground E2E and service P95 get much worse.
- `K_max=8` is the best static guardrail in this run: it keeps bulk throughput
  around 53 rows/s while keeping foreground queue mean at zero and service P95
  much lower than K=16/unbounded.
- Queue-adaptive flush is implemented and tested. The first permissive run
  (`min=8`, `max=64`) did not trigger downshift
  (`adaptive_downshifts=0`, `adaptive_limit_mean=64`), which exposed that an
  adaptive max equal to the full request count leaves too little room for
  control.
- The tuned run (`min=8`, `max=16`, `running-threshold=64`) does trigger
  downshift (`adaptive_downshifts=102` on average, `adaptive_limit_mean=9.2`),
  but it is not yet better than static `K_max=8`. It reduces the bulk
  service-tail/queue pressure relative to K=16/unbounded, but foreground E2E is
  still worse than K=8.

Next scheduling step: tune the adaptive rule rather than only the thresholds.
The current two-level controller can downshift, but it reacts coarsely; the
next version should avoid the initial overshoot and compare against static
`K_max=8` under the same shared-vLLM interference setup.

Figures:

```text
figures/data/backup/b24_local_vllm_interference_sweep_small_job.png
figures/data/backup/b24_local_vllm_interference_sweep_small_job.svg
figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.png
figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.svg
```

## Remaining Formal Experiments

Status after the 2026-07-19 shared-vLLM sweep and data-organization ablation:

- Completed in this directory: fixed-row token-tail sweep and the first
  token-budget vs fixed-row comparison on the local ShareGPT/BurstGPT
  `AI_COMPLETE` workload.
- Completed in this directory: preliminary arrival-aware `K_max` sweep with
  `token_budget=6144`, using `K_max={1,2,4,8,16,unbounded}`.
- Completed in this directory: corrected `batch policy x K_max` matrix, using
  fixed rows `{32,64,128}`, token budgets `{4096,6144,8192}`, and
  `K_max={2,4,8,16,unbounded}`.
- Completed in this directory: first shared-vLLM two-job interference test,
  showing that unbounded bulk inflight harms foreground small-job latency and
  vLLM queue stability.
- Completed in this directory: formal shared-vLLM `K_max` sweep over
  background policies `{8,16,unbounded,adaptive}`.
- Completed in this directory: first implementation and test of
  queue-adaptive flush. The permissive thresholds did not downshift; the tuned
  thresholds did downshift but still did not beat static `K_max=8`, so the
  policy needs controller refinement before any positive adaptive claim.
- Completed in this directory: first length-align and prefix-aware data
  organization ablation with local `batch_prompt_token_spread_mean` and
  `prefix_group_ratio` metrics.
- Pending next: refine queue-adaptive control in the shared-vLLM setup and
  compare against static `K_max=8`.
- Pending next: controlled prefix-share workload with prefix ratios
  `{0,30,70,100}%` and vLLM APC/cache metrics if exposed.
- Pending next: broaden shared-service scheduling by sweeping foreground size,
  arrival offset, and token-budget background policy.
- Deferred by current decision: COPY + deferred-index writeback baseline. It is
  still documented in the plan files, but it is not the next experiment.

Boundary: the completed runs support the claim that fixed row batch size is a
weak proxy for model request cost and that token-budget batching can control
token tails. The batch-policy x `K_max` matrix supports a weaker scheduling
claim: request shape and admission window interact, and excess inflight can
increase vLLM queue/service-tail pressure. The shared-vLLM sweep supports a
stronger admission-control claim for concurrent jobs. It does not yet prove
queue-adaptive scheduling or prefix-aware cache effectiveness.

## Latency Metrics Probe

CSV:
`experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv`

This small probe verifies the extra metric columns added to
`postgres_ai_operator_profile.py`. It uses:

```text
total_rows=32
ray_batch_rows=8
executor=ray_task
data_source=daft_postgres
organizer=daft
source_order=doc_id
model_metrics_url=http://localhost:8000/metrics
warmup_runs=1
repeats=3
```

All 4 CSV rows completed with `status=ok`; all rows also had
`vllm_metrics_status=ok`. Formal means:

| metric | mean |
|---|---:|
| rows_per_s | 89.913 |
| e2e_s | 0.357179 |
| operator_wall_s | 0.248223 |
| batch_tokens_mean | 1553.666667 |
| batch_service_s_p50 | 0.239984 |
| batch_service_s_p95 | 0.241361 |
| batch_service_s_p99 | 0.241361 |
| vllm_prompt_tokens_delta | 5705.000000 |
| vllm_generation_tokens_delta | 509.666667 |
| vllm_request_success_delta | 32.000000 |
| vllm_e2e_request_latency_mean_s | 0.224940 |
| vllm_request_queue_time_mean_s | 0.000017 |
| vllm_request_inference_time_mean_s | 0.209580 |
| vllm_request_prefill_time_mean_s | 0.025479 |
| vllm_request_decode_time_mean_s | 0.184101 |

Interpretation boundary: this probe validates metric collection for later
scheduling experiments. It is not a full latency-distribution experiment; vLLM
histograms are currently recorded as run-level counter deltas and means.

## Results

All 20 CSV rows completed with `status=ok`: 5 warmup rows and 15 formal rows. The table below excludes warmup rows.

| row batch size | formal repeats | mean e2e_s | std e2e_s | mean rows/s | mean model_service_s | model calls |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3 | 3.098746 | 0.044051 | 10.328 | 3.022310 | 32 |
| 2 | 3 | 1.738694 | 0.005708 | 18.405 | 1.665406 | 16 |
| 4 | 3 | 0.918562 | 0.030050 | 34.862 | 0.852476 | 8 |
| 8 | 3 | 0.491903 | 0.028013 | 65.191 | 0.420825 | 4 |
| 16 | 3 | 0.348578 | 0.018468 | 91.976 | 0.274502 | 2 |

## Interpretation

For this fixed local setup, increasing static row batch size from 1 to 16 reduced the number of model-service calls from 32 to 2 and improved mean throughput from 10.328 rows/s to 91.976 rows/s.

This supports using the 16-row static batch as the first local baseline point for the next experiment layer. It does not prove that 16 is optimal, because this sweep used only 32 prompts, `max_tokens=8`, the Python executor, no writeback, and an eager-mode local vLLM server.

## Next Step

Use this baseline as the control for the first data-organization experiments:

- compare fixed row batches against token-budget batching;
- keep the same vLLM endpoint and model;
- add latency distribution metrics before making P99 or queueing claims.
