# Local vLLM Ray Baseline Charts Audit

Date: 2026-07-18

Updated: 2026-07-19

## Figure Role

These charts are backup and learning-support figures for the local
`AI_COMPLETE` baseline. They should not be cited as final optimized scheduling
results.

Recommended outputs:

- `figures/data/backup/b07_local_vllm_ray_throughput.png`
- `figures/data/backup/b07_local_vllm_ray_throughput.svg`
- `figures/data/backup/b08_local_vllm_ray_e2e_time.png`
- `figures/data/backup/b08_local_vllm_ray_e2e_time.svg`
- `figures/data/backup/b09_local_vllm_ray_task_stage_timing.png`
- `figures/data/backup/b09_local_vllm_ray_task_stage_timing.svg`
- `figures/data/backup/b10_local_vllm_request_count_inflight.png`
- `figures/data/backup/b10_local_vllm_request_count_inflight.svg`
- `figures/data/backup/b11_local_vllm_token_tail_performance.png`
- `figures/data/backup/b11_local_vllm_token_tail_performance.svg`
- `figures/data/backup/b12_local_vllm_latency_probe_breakdown.png`
- `figures/data/backup/b12_local_vllm_latency_probe_breakdown.svg`
- `figures/data/backup/b13_local_vllm_token_tail_penalty.png`
- `figures/data/backup/b13_local_vllm_token_tail_penalty.svg`
- `figures/data/backup/b14_local_vllm_service_tail_gap.png`
- `figures/data/backup/b14_local_vllm_service_tail_gap.svg`
- `figures/data/backup/b15_local_vllm_token_budget_throughput.png`
- `figures/data/backup/b15_local_vllm_token_budget_throughput.svg`
- `figures/data/backup/b16_local_vllm_token_budget_tail_queue.png`
- `figures/data/backup/b16_local_vllm_token_budget_tail_queue.svg`
- `figures/data/backup/b17_local_vllm_arrival_kmax_sweep.png`
- `figures/data/backup/b17_local_vllm_arrival_kmax_sweep.svg`
- `figures/data/backup/b18_local_vllm_batch_kmax_e2e.png`
- `figures/data/backup/b18_local_vllm_batch_kmax_e2e.svg`
- `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.png`
- `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.svg`
- `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.png`
- `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.svg`
- `figures/data/backup/b21_local_vllm_kmax_interference_small_job.png`
- `figures/data/backup/b21_local_vllm_kmax_interference_small_job.svg`

Generation script:

- `figures/scripts/generate_local_vllm_ray_baseline_charts.py`

## Data Sources

- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_length_prefix_ablation_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_small_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_bulk_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_small_20260719.csv`
- `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_bulk_20260719.csv`

Only rows with `status=ok` and `phase=formal` are used. Warmup rows are
excluded.

The recorded baseline and token-tail sweep use the offline source order
(`source_order=doc_id`, or the equivalent default before the field was added).
They should be read as data-organization and throughput baselines, not as
arrival-aware service scheduling runs. `b17` and `b18`-`b20` use
`--source-order arrival_time`; future queue-adaptive flush and backpressure
figures should use the same arrival-aware ordering when evaluating request
rhythm and queue behavior.

## Design Decisions

The original dashboard layout was split into separate single-purpose charts.
Each chart answers one question:

- throughput versus fixed row batch size for the 2026-07-19 Ray task token-tail sweep;
- end-to-end time versus fixed row batch size for the same 1-128 Ray task sweep;
- stage timing for the 1-128 Ray task path;
- model-service request count and in-flight window utilization versus fixed row batch size;
- token cost per model-service request versus fixed row batch size.

This keeps each figure meaningful without stuffing multiple unrelated panels
into one large chart. The throughput and end-to-end charts use the newer
`1,2,4,8,16,32,64,128` Ray task sweep so they can show the large-batch stress
points. The 128-row point is especially useful for showing that throughput is
flat while request granularity becomes too coarse to fill the configured
in-flight window.
The older actor/task executor comparison remains a historical sanity sweep and
is not mixed with the 2026-07-19 data because the row count differs. `b09` and
`b10` also use the newer 1-128 sweep so all row-batch trend figures share the
same data source. `b10` uses two aligned panels: model-service request count on
top and in-flight utilization (`max_inflight_seen / max_inflight_limit`) below.
This keeps the two quantities readable without mixing counts that differ by two
orders of magnitude on one axis. `b12` intentionally remains the batch=8
latency probe because its role is metric-collection validation rather than
large-batch trend evidence.

The original batch=8 token-range diagnostic was removed from the active figure
set because it only showed that token range is large. It did not connect token
range to performance.

The revised primary token figure is
`b11_local_vllm_token_tail_performance`. It uses the 2026-07-19 `ray_task`
token-tail sweep with row batch sizes `1,2,4,8,16,32,64,128`. The figure aligns
throughput, token P50/P95/max, service P50/P95, and vLLM queue mean on the same
fixed row-batch x-axis. Its intended claim is narrower and stronger: fixed row
batching reduces request count, but larger row batches also increase token-tail
and service-tail cost; after `16-32` rows, throughput plateaus while token and
service tails keep growing.

`b13_local_vllm_token_tail_penalty` is the direct explanatory companion figure:
it plots token-tail cost (`batch_tokens_p95`) against service-tail latency
(`batch_service_s_p95`). Point labels are fixed row batch sizes. This chart is
meant to show why token range matters for performance, not merely that the
range exists.

`b14_local_vllm_service_tail_gap` focuses only on latency spread. It plots
service P50 and service P95 with a shaded P50-to-P95 band. This figure supports
the more precise statement that large fixed row batches widen the model-service
latency tail gap; it avoids overclaiming full latency range because min/max
service latency is not currently recorded.

`b15_local_vllm_token_budget_throughput` and
`b16_local_vllm_token_budget_tail_queue` are the first direct
data-organization policy comparison. They compare fixed row batches
`16/32/64/128` with token-budget batches `4096/6144/8192` under the same local
vLLM endpoint, `max_inflight=8`, 512 rows, no writeback, and 300s request
timeout. The comparison is split into a throughput figure and a token-tail /
queue figure so the reader can see the tradeoff directly: token-budget batching
can bound token tail and reduce queue pressure at comparable throughput points,
but the policy still needs K_max tuning.

`b17_local_vllm_arrival_kmax_sweep` is a preliminary scheduling figure. It
fixes the upstream request shape at `token_budget=6144` and varies only
`K_max`. Its role is to show the basic transition from Ray-side bounded wait to
vLLM-side queue time. It should not be used as the final scheduling baseline
because `K_max` only has meaning relative to how many submissions the batch
policy creates.

`b18_local_vllm_batch_kmax_e2e`,
`b19_local_vllm_batch_kmax_service_pressure`, and
`b20_local_vllm_batch_kmax_request_granularity` are the corrected coupled
scheduling figures. They use the same arrival-aware workload and vary both
upstream batch shape (`fixed32/64/128`, `token4096/6144/8192`) and admission
window (`K_max=2/4/8/16/unbounded`). `b18` shows end-to-end time improving and
then reaching a plateau as `K_max` increases. It should not be read as evidence
that `K_max` is required for end-to-end improvement. `b19` shows the weaker but
useful pressure signal: vLLM queue time and batch service P95 rise once the
service receives too many concurrent submissions. `b20` explains why large
fixed batches limit the usefulness of `K_max`: if fixed128 creates only four
upstream submissions, higher `K_max` values cannot create more scheduling
choice, while token P95 remains much larger than token-budget settings.

`b21_local_vllm_kmax_interference_small_job` is the first figure that directly
targets `K_max` necessity. It uses a shared-vLLM two-job setup: a 128-row
foreground job runs alone, then during a bounded background bulk job
(`K_max=8`), then during an unbounded background bulk job. It shows that
unbounded background inflight improves the bulk job slightly but harms the
foreground job: foreground E2E rises to about 11.4s, foreground service P95 to
about 4.4s, and vLLM queue mean to about 0.445s. This supports the admission
control motivation under shared service, not a universal single-job throughput
claim.

`b22_local_vllm_length_prefix_tail` and
`b23_local_vllm_length_prefix_signal` are the first data-organization ablation
figures for length-align and prefix-aware variants. They are intentionally
split: b22 reports tail-performance outcomes, while b23 reports organization
signals. The key boundary is that length-align alone is not a positive result:
it reduces within-batch prompt-token spread but can concentrate long prompts
into one fixed-row batch. Prefix-aware currently records a weak local
prefix-group ratio only, so it cannot claim KV-cache or APC benefit.

`b24_local_vllm_interference_sweep_small_job` and
`b25_local_vllm_interference_sweep_bulk_tradeoff` replace the initial
three-scenario intuition with a formal shared-vLLM sweep over background
`K_max=8`, `K_max=16`, `unbounded`, and a tuned queue-adaptive implementation.
The supported claim is that higher background inflight does not materially
improve bulk throughput in this local setup but raises foreground E2E, service
P95, and queue pressure. The tuned adaptive row proves that the control path can
downshift, but it is still a negative/control point for effectiveness because
it does not beat static `K_max=8`.

`b12_local_vllm_latency_probe_breakdown` combines eight latency items on one
axis: three client batch quantiles and five vLLM server-side mean metrics. It is
still a small metric probe, not a full sweep.

Rejected chart forms:

- pie charts, because there is no stable whole-to-part claim across all runs;
- dual-y charts, because they would make stage timing and throughput visually
ambiguous;
- a dashboard-style 2x2 panel, because the figures need to be individually
  citable.

## Boundary

The figures support this statement:

> The local PostgreSQL -> Daft -> Ray -> vLLM baseline exposes fixed row-batch
> overhead and throughput tradeoffs. It also shows that fixed row batches do
> not fix token cost per request. In the 2026-07-19 token-tail sweep, larger
> row batches reduce request count but make token and service-time tails much
> heavier. The first token-budget comparison shows that bounding tokens per
> request can reduce token tail and queue pressure at comparable throughput
> points. The coupled batch-policy x K_max matrix shows that request shape and
> admission window interact: too-small K_max waits in Ray, while too-large
> K_max tends to raise vLLM queue time and service tails without meaningful
> end-to-end gains in this local offline run. The shared-vLLM interference run
> gives the first direct K_max motivation: bounding a background bulk job can
> protect a foreground job's latency and queue stability on the same endpoint.

The figures do not support these stronger statements:

- queue-adaptive scheduling is better;
- prefix-aware grouping improves KV-cache or APC behavior;
- K_max is required for end-to-end improvement;
- K_max is necessary when every job has an exclusive vLLM endpoint;
- token-budget alone is the final optimized method;
- Ray actors are generally slower than Ray tasks;
- batch size 16 or 32 is globally optimal;
- batch size 64 or 128 is a recommended default;
- the result represents PostgreSQL 18.3 internal-platform behavior;
- this is a writeback-inclusive result.

## Visual QA

Checked generated PNG outputs manually after regeneration:

- all split figures are readable as standalone charts;
- legends do not cover critical trends;
- axes and captions are readable;
- no chart uses 3D, rainbow colors, pie slices, or dual axes;
- the latency probe is explicitly labeled as a small metric probe.
