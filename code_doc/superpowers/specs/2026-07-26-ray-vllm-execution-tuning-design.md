# Ray and vLLM Execution-Layer Tuning Design

Date: 2026-07-26

## 1. Goal

Tune the execution layer underneath the existing upstream scheduling policies
without changing the research boundary:

```text
Daft -> Arrow -> Ray task/actor -> external vLLM service
```

The project does not modify Ray's scheduler or vLLM internals. It selects and
records supported runtime parameters, then measures whether those parameters
change the effectiveness of the upstream batching and submission policies.

The work is successful when it produces:

1. a reproducible, non-debug vLLM service baseline;
2. a maintainable Ray task/actor resource and concurrency contract;
3. an attributable single-GPU experiment that selects one execution-layer
   default;
4. a clean endpoint/actor-worker boundary that can be reused by future
   multi-GPU experiments.

## 2. Current Evidence

### 2.1 Service configuration

The running vLLM 0.25.1 container currently uses:

```text
--max-model-len 2048
--gpu-memory-utilization 0.75
--enforce-eager
--enable-mfu-metrics
--no-enable-prefix-caching
```

It does not explicitly set `max_num_batched_tokens` or `max_num_seqs`.
`--enforce-eager` disables the CUDA Graph/compiled steady-state path and is a
debug/startup-oriented configuration, so it must not remain the only formal
performance baseline.

### 2.2 Ray and Daft configuration

The 2048-request held-out experiment uses:

```text
executor=ray_task
model_workers=2
max_inflight=8
daft_runner=native
organizer_partition_mode=none
```

The code exposes Ray actor and Daft Ray-runner paths, but there is no formal
task/actor concurrency sweep or Daft partition sweep.

Ray remote functions and actors are created without explicit CPU resource,
actor concurrency, restart, or retry options. Ray-facing HTTP workers must not
reserve GPU resources because the external vLLM process owns the GPU.

### 2.3 Measured overhead boundary

For the fixed-50 2048-request held-out run:

```text
E2E                 456.55 s
Ray submit            1.54 s
Ray fan-in             0.17 s
source fetch           0.48 s
```

Submit plus fan-in is below 0.4% of E2E. Object-store micro-tuning, explicit
`ray.put`, and batched result draining are therefore not first-stage
optimizations.

### 2.4 Endpoint/actor conflation

The actor path currently creates one topology endpoint per Ray actor:

```text
actor-0 -> actor handle 0
actor-1 -> actor handle 1
...
```

This conflates two different concepts:

- service endpoint: independently observable vLLM capacity, usually one GPU;
- Ray actor worker: a client-side submitter that calls a service endpoint.

Changing `model_workers` therefore also changes the apparent endpoint count,
which would confound actor-concurrency experiments and future multi-GPU
routing. This boundary must be corrected before multi-endpoint performance
tests.

## 3. Alternatives

### 3.1 Tune only Ray parameters

Keep the current eager vLLM service and compare task/actor settings.

This is easy to attribute but may optimize a client layer around a deliberately
slower service configuration.

### 3.2 Staged service then Ray tuning -- selected

First select eager versus CUDA Graph service execution, then compare Ray task
and actor concurrency on the selected service configuration. Finally perform a
small vLLM capacity sweep.

This preserves attribution and tests the largest plausible bottleneck first.

### 3.3 Joint Ray × vLLM grid

Search all service and Ray parameters together.

The Cartesian product is expensive and would make a result difficult to
explain. Existing joint batching/submission experiments also found no evidence
that a larger online controller was necessary.

## 4. Architecture

### 4.1 Keep policy layers unchanged

The selected single-GPU upstream policy remains fixed during execution-layer
tuning:

```text
sequential token-budget
static K_max=8
fixed 50 ms arrival-replay flush
round-robin single-endpoint routing
```

Batch membership, admission, flush, and endpoint routing are not modified by
the execution parameter sweep.

### 4.2 Separate service endpoints from actor workers

An endpoint remains one `EndpointSnapshot` and one routing target. Each
endpoint may own one or more Ray actor submitters:

```text
EndpointSnapshot endpoint-0
  -> actor worker 0
  -> actor worker 1
  -> actor worker 2
```

The existing `RaySubmissionAdapter` interface remains:

```text
submit(envelope, endpoint_id)
```

For an actor executor, the callable registered for an endpoint becomes a small
round-robin actor-worker-pool submitter. The scheduler still chooses a service
endpoint; the submitter then chooses one actor inside that endpoint. Task
execution continues to register one callable per service endpoint.

This avoids changing scheduling models or duplicating endpoint routing inside
the actor pool.

### 4.3 Explicit Ray resource contract

Add explicit configuration for:

- actor workers per endpoint;
- actor maximum concurrency;
- CPU resources per task/actor.

The GPU resource request is fixed at zero for HTTP submitters and is not exposed
as a tuning option. The actor/task does not execute model kernels.

Automatic retry remains disabled for formal HTTP completion calls. A worker
failure after the endpoint accepted a non-idempotent generation request can
make an automatic retry duplicate work. Fault injection and idempotency keys
must be designed before enabling Ray retries.

### 4.4 Actor concurrency

The completion actor stores immutable endpoint configuration. Its `complete`
method has no mutable request state, so the first candidate may use Ray's
threaded actor concurrency.

The effective client concurrency is:

```text
actor_workers_per_endpoint * actor_max_concurrency
```

The experiment holds this value at eight when comparing actor layouts. This
isolates actor topology from total upstream concurrency.

HTTP connection pooling is not added in the first implementation. It would
introduce a new client dependency and is not justified by the current measured
driver overhead. It may be reconsidered only after actor timing separates
connection/setup time from endpoint service time.

## 5. Configuration and Recorded Metadata

Proposed profiler options:

```text
--actor-workers-per-endpoint
--ray-actor-max-concurrency
--ray-worker-num-cpus
```

`--model-workers` remains a compatibility alias for single-endpoint runs during
the transition. New scenario files use the explicit actor option. Conflicting
values fail fast.

Every result and scenario manifest records:

```text
ray_version
ray_executor
actor_workers_per_endpoint
ray_actor_max_concurrency
ray_worker_num_cpus
ray_worker_num_gpus
daft_runner
organizer_partition_mode
organizer_partitions
vllm_version
vllm_enforce_eager
vllm_compilation_mode
vllm_chunked_prefill
vllm_max_num_batched_tokens
vllm_max_num_seqs
vllm_gpu_memory_utilization
vllm_prefix_caching
```

Unknown service values remain explicitly `unknown`; they are never inferred
from a result directory name.

## 6. Experiment Stages

All stages use the same model, tokenizer, prompt set, arrival order, output
settings, source path, token budget, row cap, K_max, flush policy, metrics
sampling, and no-writeback sink unless the stage explicitly changes one
variable.

### 6.1 CUDA Graph gate and comparison

Compatibility gate:

- current eager service, 64 requests;
- default compiled/CUDA Graph service, 64 requests.

The compiled service must pass:

- 64 unique requests completed exactly once;
- identical prompt/doc set;
- valid finish reasons and actual output-token counts;
- positive vLLM FLOP delta and valid MFU;
- no OOM, compilation, CUDA Graph, or timeout incident.

Performance comparison:

- one warm-up plus three formal 512-request repeats per service mode;
- randomized formal order where service restart cost permits;
- graph capture/compile startup is recorded separately and excluded from
  steady-state E2E;
- steady-state result reports tokens/s, E2E, request P50/P95/P99, SLO goodput,
  GPU utilization, memory, power, energy, and MFU.

The compiled mode is selected when it has no correctness incident and either:

- improves tokens/s by at least 5%; or
- reduces request P99 by at least 10% without reducing tokens/s by more than
  2%.

Otherwise eager remains an explicit compatibility baseline.

### 6.2 Ray task/actor comparison

Run on the selected service configuration:

| Executor | Workers per endpoint | Actor concurrency | Effective client concurrency |
|---|---:|---:|---:|
| Ray task baseline | n/a | n/a | K_max=8 |
| Ray actor | 8 | 1 | 8 |
| Ray actor | 4 | 2 | 8 |
| Ray actor | 2 | 4 | 8 |
| Ray actor | 1 | 8 | 8 |

Use a 64-request contract gate, then one 512-request screen. Only candidates
within 5% of the best tokens/s and with no SLO regression enter three formal
repeats.

In addition to the normal metrics, record:

- actor creation time;
- submission and fan-in time;
- per-actor assigned submission count;
- maximum pending calls if available;
- Ray task/actor failure count.

### 6.3 vLLM scheduling-capacity screen

On the winning Ray executor, screen a small explicit set of
`max_num_batched_tokens` and `max_num_seqs` values. The exact values are chosen
from the running vLLM version's accepted CLI range and the 12 GB GPU memory
gate; no broad Cartesian grid is used.

The service restarts between configurations. Each configuration first runs 64
requests and is pruned on OOM, preemption, timeout, or correctness failure.
Survivors run the identical 512-request screen. At most two candidates enter
three formal repeats.

The selected service capacity must not increase absolute SLO violation by more
than 1 percentage point.

### 6.4 Prefix-cache mechanism experiment

This is a separate mechanism experiment after the generic execution default is
selected:

- ordinary workload remains cache-off;
- controlled 30%, 70%, and 100% repeated-prefix workloads run cache-on;
- sequential, prefix-aware organization, and prefix-affinity routing remain
  separately named ablations;
- cache hit/token evidence is mandatory.

Prefix results do not replace the generic cache-off default unless the workload
has a declared repeated-prefix property.

### 6.5 Daft Ray runner -- deferred gate

The current source and organization stages are below 0.5 seconds in a
456-second run, so Daft runner and partition tuning are not included in the
first performance stage.

The existing native/Ray runner and partition interfaces are retained. A formal
Daft sweep is triggered when either:

- source plus organization exceeds 5% of E2E; or
- a multimodal workload adds material image decode/preprocessing work.

## 7. Tests

Production changes follow red-green-refactor.

1. One service endpoint can own multiple actor workers without creating
   multiple topology endpoints.
2. Actor worker selection rotates deterministically within an endpoint.
3. Endpoint routing remains independent of actor-worker selection.
4. Effective actor concurrency and CPU resource options are passed to
   `ray.remote(...).options(...)`.
5. HTTP Ray workers always request zero GPU resources.
6. Conflicting legacy and explicit worker options fail fast.
7. Dry-run and result rows contain complete Ray and service metadata.
8. Existing single-endpoint task, actor, lifecycle, and Daft-Ray contract tests
   remain green.

The full code test suite, CLI checks, compilation checks, and real
Daft->Arrow->Ray contract test run before GPU experiments.

## 8. Non-Goals

This stage does not:

- modify vLLM scheduling algorithms;
- modify Ray's scheduler;
- add placement groups for one local endpoint;
- tune object-store capacity without spill evidence;
- add explicit `ray.put` for batches consumed once;
- optimize Arrow serialization as a research contribution;
- introduce a new learned controller;
- claim multi-GPU scaling from logical endpoints or shared-GPU replicas.

## 9. Maintainability Constraints

- Keep service, endpoint, actor worker, and request concepts distinct.
- Add no framework around three Ray options; use typed configuration or
  explicit function arguments.
- Preserve existing defaults until a real repeated experiment promotes a new
  default.
- Keep experiment stages resumable and service configuration auditable.
- Do not retry non-idempotent completion calls automatically.
- Do not combine service-mode, Ray-executor, and endpoint-routing changes in
  one comparison.
- Every performance conclusion must state the complete service and Ray
  configuration.

## 10. Relationship to Multi-Endpoint Work

Complete the execution-layer contract and single-endpoint selection before the
multi-endpoint routing experiment in
`2026-07-26-multi-endpoint-routing-readiness-design.md`.

Future multi-GPU runs reuse:

- the selected service execution mode;
- explicit Ray resource and actor concurrency settings;
- endpoint-local actor worker pools;
- the endpoint routing policies and evidence gates from the multi-endpoint
  design.
