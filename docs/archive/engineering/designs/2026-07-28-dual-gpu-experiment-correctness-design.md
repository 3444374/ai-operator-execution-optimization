# Dual-GPU Experiment Correctness and Shared Scheduling Design

## Goal

Make the dual-GPU experiment sequence measure the intended upstream control
variables before adding more adaptive policy complexity. The implementation
must prevent a token-budget curve from being silently capped by a row limit,
keep organization-batch metrics distinct from Ray submission metrics, and
ensure multi-job shared credit actually lives in one Ray cluster.

## Evidence motivating the change

The completed remote token-budget curve is stable but does not close the
capacity question:

- fixed admission was expressed as four batches per endpoint, so increasing
  rows per batch also increased the number of active requests offered to
  vLLM;
- mean rows per batch rose from about 2.3 to 64, making the approximate
  per-endpoint request envelope rise from 9 to 256 even though batch K stayed
  fixed;
- mean vLLM running requests rose from about 15.5 to 310.7 across the same
  sweep;
- the 32768-token arm has `batch_rows_mean=64`, exactly equal to
  `ray_batch_rows=64`;
- its mean estimated batch cost is about 21.5k tokens and mean budget
  utilization is about 72.6%;
- throughput is still increasing at the upper tested point;
- batch-mode request traces use `latency_granularity=submission`, so all rows
  inside one HTTP list submission share one completion timestamp;
- service metadata records vLLM `max_num_seqs` and
  `max_num_batched_tokens` as `unknown`.

The data-organization follow-up also changes the number of submissions:
sequential produces 8 batches with a 4/4 endpoint split, while row-cap-aware
and length-align produce 9 batches with a 4/5 split. This is useful
exploratory evidence, but it is not a clean membership-only comparison.

The current multi-job runner starts separate profiler processes. Each profiler
calls `ray.init()` without an explicit shared address. A named detached actor
cannot be treated as a cross-job coordinator unless all jobs connect to the
same Ray cluster and namespace.

## Design

### 1. Separate organization shape from submission shape

The profiler will retain existing compatibility columns and add explicit
organization metrics:

- `organization_batch_count`
- `organization_batch_rows_mean`
- `organization_batch_rows_max`
- `organization_batch_cost_units_mean`
- `organization_batch_cost_units_p95`
- `organization_row_cap_hit_ratio`

`batch_rows_*` continues to describe the payloads actually submitted to Ray.
In request-level mode it is therefore valid for `batch_rows_mean` to be 1,
while `organization_batch_rows_mean` records the pre-expansion grouping.

The organization metrics are computed at the organizer/replay boundary before
`submission_granularity=request` expands a group into request envelopes.

### 2. Calibrate active work before token budget

The scenario runner will validate complete service metadata before launching
external work. When `require_complete_service_metadata=true`, an empty value
or the sentinel string `unknown` is invalid for:

- `vllm_version`
- `max_num_batched_tokens`
- `max_num_seqs`
- `gpu_memory_utilization`
- `prefix_caching`

The first formal capacity experiment will vary request-level active work, not
token budget:

- use `submission_granularity=request`;
- keep organization settings, routing, arrival pattern, model, and output
  bound fixed;
- scan per-endpoint active-work limits expressed in estimated token work;
- set request-count admission high enough that active work is the binding
  limit;
- select a saturation region using throughput, MFU, P95/P99, SLO goodput, and
  queue growth rather than throughput alone.

Only after that calibration will the token-budget template:

- raise `ray_batch_rows` from 64 to 256;
- scan `{8192, 16384, 24576, 32768, 49152, 65536}`;
- hold the selected per-endpoint active-work limit constant in every arm;
- set batch-count admission high enough that active work remains the binding
  offered-load control;
- read `max_num_batched_tokens`, `max_num_seqs`, and request SLO from explicit
  runtime environment variables;
- continue to disable arrival replay and use deterministic `doc_id` order;
- retain one fixed routing policy.

This second curve asks whether organization and submission shape matter at
equal offered work. It is not a vLLM capacity curve.

A completed curve may select a budget only when the top candidate is not
row-cap truncated. The audit rule is:

```text
organization_row_cap_hit_ratio < 0.5
and organization_batch_cost_units_mean / token_budget >= 0.5
```

If the highest-throughput arm fails this rule or throughput is still rising at
the upper bound, the result is reported as `BEST_TESTED_TOKEN_BUDGET`, not a
capacity sweet point.

### 3. Make data-organization comparisons work-normalized and auditable

The data-organization template will use 1024 rows, the same row cap and
service metadata contract as the corrected token-budget curve, the selected
active-work limit, and a fixed routing policy. It will continue to compare
fixed rows, sequential token budget, row-cap-aware packing, and length
alignment.

The result must report, per policy:

- organization batch count and row/cost distribution;
- per-endpoint submission count and token work;
- total tokens/s and job completion time;
- submission-granularity P95/P99 with an explicit label;
- endpoint imbalance ratio.

Differences caused by batch-count parity or endpoint work imbalance are
reported as mediating mechanisms, not silently attributed to membership.
Endpoint-aware least-work routing remains a later, separately ablated
submission-policy candidate.

### 4. Require one Ray cluster for shared multi-job credit

The profiler will accept an explicit `--ray-address`. Its default remains the
existing local single-process behavior.

If shared-credit coordination is enabled, the profiler must reject execution
unless either:

- `--ray-address` is supplied, or
- `RAY_ADDRESS` is set.

The multi-job runner will forward the same address and namespace to every
profiler process. Remote shared-job smoke tests must start a Ray head first and
use `--ray-address=auto` or the concrete head address. This makes the named
credit actor genuinely shared across jobs.

### 5. Keep the current request-replenishment experiment running

The current request-replay matrix is not invalidated by the capacity-curve
audit. Its matched comparison is based on the observed online organization
shape: about 3 rows per batch makes batch K16 per endpoint comparable to
request K48 per endpoint.

Its report must state that the 50 ms flush closes groups at roughly 3.4% of the
32768-token budget. It tests request-level replenishment under low-fill online
arrival replay, not the optimality of a 32768-token organization budget.

### 6. Stage the remaining experiments by causal question

The formal order is:

1. request-level active-work capacity calibration;
2. token-budget comparison at fixed active work;
3. membership comparison at fixed budget and active work;
4. batch-barrier versus request-level replenishment;
5. burst, heterogeneous-length, and SLO-sensitive dynamic workloads;
6. shared-vLLM multi-job fairness and isolation.

The completed token-budget and organization runs remain diagnostic evidence.
They are not promoted to causal conclusions because offered work, batch count,
or endpoint parity changed with the nominal policy variable.

## Non-goals

- Do not modify vLLM or Ray's internal scheduler.
- Do not introduce PID, UCB, or a joint adaptive controller.
- Do not replace the existing active-work or least-work implementations before
  their isolated GPU experiments.
- Do not rerun formal GPU matrices from the local development machine.
- Do not claim true per-request completion times for a non-streaming list
  submission.

## Test strategy

1. Unit-test rejection of `unknown` required service metadata before health
   checks or profiler subprocess creation.
2. Unit-test organization metrics for batch and request submission
   granularity, proving that pre-expansion group shape is preserved.
3. Unit-test `--ray-address` propagation and rejection of shared credit on an
   implicit local Ray cluster.
4. Validate all revised JSON templates through the deterministic scenario
   loader without contacting PostgreSQL or vLLM.
5. Run the full local test suite and Ruff.
6. Push the development branch, create an independent remote worktree, run the
   same tests, expand the templates with the remote runtime environment, and
   perform only a small Ray/shared-credit smoke. Formal GPU data collection is
   left to the remote experiment agent.

## Acceptance criteria

- A formal scenario cannot start with required vLLM capacity metadata set to
  `unknown`.
- Request-level runs expose both organization group shape and one-row
  submission shape without overloading one metric.
- Shared-credit jobs cannot start in separate implicit Ray clusters.
- The first capacity template controls request-level active work directly.
- The corrected token-budget and organization templates hold active work
  constant, can form groups larger than 64 rows, and cover a budget range
  above 32768.
- Local and remote-worktree tests pass without starting the formal GPU matrix.
