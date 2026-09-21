# Runtime Scheduling Strategy Suite Design

Date: 2026-07-25

## 1. Objective

Build a maintainable runtime scheduling strategy suite for the upstream
execution path of database AI operators. The suite must support:

- independent queue-adaptive flush;
- actor-pool partitioning and dynamic routing;
- multiple endpoints and a future multi-GPU topology;
- static, AIMD, EWMA-AIMD, PID, and UCB admission control;
- independent optimization and joint search across batching and submission
  control;
- reproducible tests, single-GPU experiments, statistical comparison, and
  plot-ready raw data.

The suite optimizes how database rows are organized and submitted to model
services. It does not modify vLLM internals, the Ray scheduler, model kernels,
or database query operators.

The formal execution framework is fixed:

```text
PostgreSQL -> Daft -> Arrow payload boundary -> Ray task/actor -> endpoint
```

Daft is the production data engine and Ray is the production distributed
execution framework. Pure-Python or fake adapters exist only for deterministic
tests and are not method baselines or formal experiment paths.

The existing
`2026-07-25-adaptive-admission-controller-design.md` remains the detailed
design for the AIMD admission slice. This document defines the larger system
that composes that slice with flush, routing, topology, learning, search, and
metrics.

## 2. Evidence and Resource Boundary

The current machine has one NVIDIA GeForce RTX 5070 with 12 GB memory. Current
real experiments therefore have three evidence levels:

1. **Code evidence**: unit, invariant, and deterministic integration tests.
2. **Single-GPU evidence**: real vLLM experiments, including multiple logical
   pools and, when memory permits, multiple endpoint processes sharing the
   same GPU.
3. **Future multi-GPU evidence**: the topology and routing interfaces support
   endpoints on different GPUs, but no multi-GPU performance claim is made
   until independent GPU endpoints are available.

Multiple endpoints on one GPU validate health checks, fallback, routing
correctness, and resource competition. They do not validate multi-GPU
scalability.

## 3. Design Principles

- Strategies have one decision responsibility and communicate through typed
  dataclasses or protocols.
- Strategy code is independent of Daft, Arrow, Ray, and HTTP.
- Engine adapters bind the pure strategy core to Daft-produced Arrow payloads
  and Ray task/actor execution; no second production execution framework is
  introduced.
- The scheduler owns orchestration but no policy algorithm.
- Static and simple policies remain first-class baselines.
- Complex policies are enabled explicitly and may be rejected by experiments.
- Missing metrics cause deterministic hold/fallback behavior.
- No request is split within a row; batching only combines complete rows.
- No policy is declared successful from a smoke run or one repeat.
- New policy modules remain small; a module that exceeds roughly 200 lines
  must be reviewed for mixed responsibilities.

## 4. Alternatives Considered

### 4.1 Layered strategy framework — selected

Flush, admission, pool selection, endpoint selection, and search use separate
interfaces. Each layer has isolated baselines and experiments.

This requires initial interface work but makes ablation, maintenance, and
negative-result handling reliable.

### 4.2 One integrated adaptive scheduler

A single controller could choose flush, K_max, pool, endpoint, and batching
parameters. It would be faster to prototype but would confound causal
attribution and produce a large state machine.

### 4.3 Independent experiment prototypes

Separate scripts could implement each mechanism. This would start individual
experiments quickly but duplicate Ray submission, metrics, retry, and trace
logic and make later composition unsafe.

## 5. Runtime Architecture

```text
PostgreSQL / Daft source
  -> BatchBuilder
  -> pending BatchRequest
  -> FlushPolicy
  -> closed BatchRequest
  -> AdmissionController
  -> PoolRouter
  -> EndpointRouter
  -> Ray submission
  -> vLLM-compatible endpoint
  -> completion event
  -> MetricsRecorder
```

### 5.1 `requests.py`

`BatchRequest` contains only strategy-facing metadata and a reference to the
complete-row payload:

- request, job, and batch identifiers;
- operator type;
- row count;
- prompt, estimated output, and total estimated tokens;
- prefix key or prefix group;
- first-row and oldest-row arrival times;
- opaque payload identifier.

It contains no Daft dataframe, Arrow table, Ray object reference, endpoint
client, or controller state. A scheduler-owned `PayloadEnvelope` associates
the metadata with the engine payload; policies receive only the metadata.

In production, that opaque payload is an Arrow table emitted by the Daft
organizer. The payload type is hidden from policies but remains zero-copy
compatible with the existing Daft/Ray boundary.

### 5.2 `batching.py`

`BatchBuilder` converts a row stream or fetched morsel into a pending batch.
Implementations:

- fixed-row builder;
- token-budget builder;
- length-aligned token-budget builder;
- prefix-aware token-budget builder;
- bin-packing builder.

The existing organizer remains the engine adapter. Batching policy operates on
complete rows and common metadata.

### 5.3 `flush.py`

`FlushPolicy` decides whether a pending batch is closed now or allowed to
accumulate:

- `ImmediateFlush`;
- `FixedTimeoutFlush`;
- `QueueAdaptiveFlush`.

It does not choose K_max, pool, or endpoint.

`QueueAdaptiveFlush` behavior:

- flush immediately when row/token budget is reached;
- flush a partial batch when vLLM has no waiting work and running sequences
  are below the configured low watermark;
- wait briefly during congestion to coalesce more complete rows;
- force flush when oldest-row age reaches `max_wait_ms`.

`max_wait_ms` is a hard safety bound.

Initial reproducible flush defaults are:

- fixed-timeout baseline: `25ms`;
- queue-adaptive hard maximum: `50ms`;
- low-load signal: `waiting == 0` and `running < 64`;
- congestion signal: `waiting > 0` or KV usage at least `0.85`.

These are tuning defaults. The held-out evaluation reuses the selected values
without retuning.

### 5.4 `admission.py`

`AdmissionController` chooses the current in-flight window:

- `StaticAdmission`;
- `AimdAdmission`;
- `EwmaAimdAdmission`;
- `PidAdmission`;
- `UcbAdmission`.

All controllers consume the same typed observation and return:

- new window;
- action;
- reason;
- strategy-specific diagnostic fields.

`EwmaAimdAdmission` smooths input signals but uses the AIMD control law.
`PidAdmission` controls queue error and includes integral clamping and output
clamping. `UcbAdmission` chooses from a finite K_max action set and does not
generate arbitrary windows.

Initial reproducible controller defaults are:

- EWMA alpha `0.3`, with the same `+2`/`x0.5` AIMD update as the unsmoothed
  baseline;
- PID target waiting depth `1`, gains `Kp=0.5`, `Ki=0.1`, `Kd=0.05`, and
  output window bounds `[2, 16]`;
- UCB1 arms `{4, 8, 16}`, exploration coefficient `sqrt(2)`, and one reward
  update per 64 completed model sequences.

PID gains are tuning-workload defaults, not universal constants. The
evaluation workload reuses the selected gains without retuning.

The initial UCB reward is SLO-constrained throughput:

```text
throughput_ratio = epoch_tokens_per_s / static_k8_tuning_tokens_per_s
reward = min(2, throughput_ratio)                      if p99 <= slo
reward = min(2, throughput_ratio) * (slo / p99)^2     otherwise
```

Reward inputs and the SLO are stored with every update. The static tuning
baseline is measured before UCB runs.

### 5.5 `routing.py`

Routing has two levels:

- `PoolRouter`: chooses a logical short, long, prefix-affinity, or
  operator-specific pool;
- `EndpointRouter`: chooses a healthy endpoint inside that pool.

Initial endpoint strategies:

- round-robin baseline;
- least-queued;
- prefix-affinity with least-queued fallback.

Pool classification uses request metadata only. Endpoint health and queue
state come from the topology snapshot.

Initial pool precedence is:

1. a non-empty reusable prefix group;
2. long request;
3. short request.

The long-request threshold is the tuning workload's P75 estimated total-token
cost. Its resolved numeric value is stored in every run configuration and is
reused unchanged for held-out evaluation.

Least-queued routing minimizes `running + waiting` and breaks ties by endpoint
identifier. Prefix affinity uses deterministic rendezvous hashing over healthy
endpoints; if the selected endpoint becomes unhealthy, it falls back to
least-queued.

### 5.6 `topology.py`

Topology types describe:

- endpoint identifier and URL;
- logical pool membership;
- GPU identifier;
- model and operator capability;
- health;
- running, waiting, KV usage, and observation time.

The scheduler never assumes that two endpoints share or do not share a GPU.
Single-GPU and future multi-GPU deployments differ only in topology
configuration.

### 5.7 `scheduler.py`

The scheduler owns one event loop:

1. accept complete rows;
2. update the pending batch;
3. ask the flush policy whether to close it;
4. ask admission whether a closed request may enter in-flight;
5. select pool and endpoint;
6. submit through a Ray adapter;
7. process completions and update observations;
8. emit request, control, endpoint, and resource records.

The scheduler contains no AIMD, PID, EWMA, UCB, or routing formula.

The scheduler runs in the Ray-facing execution layer. A deterministic
synchronous adapter is permitted only in tests. Production adapters are:

- Ray task adapter for the existing task baseline;
- stateful Ray actor adapter for endpoint pools and adaptive scheduling.

The actor adapter is the primary method path. It owns endpoint-local queue
state and asynchronous submission but delegates policy decisions to the typed
strategy interfaces.

### 5.8 `search.py`

Search code generates configurations and aggregates results:

- independent search by strategy dimension;
- reduced joint grid search;
- UCB action-space construction;
- held-out evaluation configuration.

It invokes the same experiment runner as individual policies and does not
duplicate the execution path.

## 6. Runtime Semantics

### 6.1 Batching versus flush

Batching decides which complete rows belong together. Flush decides when the
currently accumulated group is closed. Admission decides when a closed batch
may be submitted.

These terms must not be used interchangeably in code, CSV columns, reports, or
figures.

### 6.2 Metrics availability

- Missing controller metrics hold the current admission window.
- Missing queue metrics make queue-adaptive flush rely on budget and
  `max_wait_ms`.
- Missing endpoint metrics make endpoint routing fall back to round-robin
  across healthy endpoints.
- Stale observations are marked and cannot update adaptive controller state.

### 6.3 Failure and retry

- Unhealthy endpoints are excluded from new routing decisions.
- Only requests confirmed not to have started may be automatically rerouted.
- An ambiguous post-submission failure is recorded for manual/reconciliation
  handling rather than blindly retried.
- Request IDs make duplicate completion and duplicate writeback detectable.
- If every endpoint is unhealthy, new admission pauses and the run fails after
  a configured experiment timeout.

### 6.4 Sampling

- Strategy decision events are recorded for every decision.
- Resource and endpoint state default to a 250 ms sampling interval.
- A cached observation may be recorded but cannot repeatedly update a
  controller.
- Sampling uses monotonic time and never sleeps inside a policy decision.

## 7. Test Design

Implementation follows red-green-refactor for every behavior.

### 7.1 Unit tests

- batch builders preserve every input row exactly once;
- no row content is split;
- flush budget, low-load, congestion, and hard-timeout behavior;
- static/AIMD/EWMA/PID/UCB window bounds;
- PID integral anti-windup;
- EWMA deterministic smoothing;
- UCB visits every arm before exploitation and updates the selected arm only;
- pool classification;
- round-robin, least-queued, prefix-affinity, unhealthy fallback;
- topology validation;
- metric absence and staleness behavior;
- stable trace schemas.

### 7.2 Invariant tests

Deterministic pseudo-random event sequences verify:

- no lost or duplicate request;
- in-flight count never exceeds the applied window;
- pending rows do not wait beyond the hard flush limit;
- completed request count plus failed request count equals admitted count;
- unhealthy endpoints receive no new request;
- every decision has a trace row;
- static baselines do not depend on adaptive metrics.

These tests use the standard library and fixed seeds; no property-testing
dependency is required initially.

### 7.3 Integration tests

- deterministic fake endpoint with controllable latency and failures;
- Daft organizer -> Arrow payload -> single-node Ray adapter contract smoke;
- Ray task adapter smoke;
- Ray actor adapter smoke;
- CLI configuration and dry-run;
- one real local vLLM endpoint;
- two local endpoint processes on one GPU only when memory permits.

### 7.4 Completion gates

- **Code complete**: focused and full test suites pass, schemas validate,
  Daft-to-Ray contract smoke passes, and dry-run commands succeed.
- **Single-GPU experiment complete**: formal result tables and raw traces exist
  with reproducible commands.
- **Multi-GPU validation pending**: topology/routing code passes tests, but no
  scalability result is claimed.

## 8. Experiment Program

### 8.1 Common controls

- ShareGPT/BurstGPT workload;
- arrival-time order for scheduling experiments;
- identical data read, model, writeback, and warm-up paths;
- one warm-up plus three tuning repeats; promote primary/final comparisons to
  at least five formal repeats;
- interleaved or seeded randomized scenario order;
- actual model, database, pgvector, Ray, Daft, GPU, and Git versions recorded.

### 8.2 Isolated ablations

#### Flush

- immediate;
- fixed timeout;
- queue adaptive.

Admission, batching, and routing remain fixed.

#### Admission

- static K_max in `{4, 8, 16}`;
- AIMD;
- EWMA-AIMD;
- PID;
- UCB with a finite K_max arm set.

Batching, flush, and routing remain fixed.

#### Routing

- round-robin;
- least-queued;
- workload pool;
- prefix-affinity.

On one GPU this experiment establishes correctness, behavior, overhead, and
resource contention only.

### 8.3 Independent versus joint optimization

The required core comparison follows
`experiments/plans/experiment_status_and_gaps.md` P0-2:

```text
token_budget in {4096, 6144, 8192}
K_max       in {4, 8, 16}
```

This is a `3 x 3 = 9` point grid on the 512-row ShareGPT/BurstGPT tuning
workload. Compare the direct composition of independently selected optima with
the joint-grid optimum.

Only after that required comparison is complete, run the extended strategy
suite grid using the top two batching configurations, top two admission
configurations, and all three flush policies (`2 x 2 x 3 = 12`). UCB online
selection is reported separately rather than substituted for either grid.

Final comparison uses a separate 2048-row evaluation workload. Selection and
evaluation results are stored separately.

### 8.4 Statistical reporting

For each formal scenario report:

- mean, median, standard deviation;
- deterministic bootstrap 95% confidence interval;
- paired differences when runs share the same workload seed;
- throughput/latency Pareto membership.

Primary metrics:

- total, prompt, and generation tokens/s;
- foreground E2E and service P99;
- SLO violation rate;
- background throughput.

Shared-service metrics:

- Jain fairness;
- per-job slowdown relative to solo;
- queue and admission wait;
- request failure and retry rates.

Negative results remain in the comparison table. A complex strategy is not a
main method unless it consistently improves a predeclared primary objective
over the tuned static or simpler adaptive baseline.

## 9. Metrics and Plot-Ready Artifacts

Each run has a stable `run_id` and schema version.

### 9.1 `runs.csv`

One row per run:

- experiment identity, repeat, seed, Git commit;
- database, pgvector, vLLM, Ray, Daft, model, endpoint, and GPU metadata;
- complete strategy configuration;
- rows/s, requests/s, batches/s;
- prompt, generation, and total tokens/s;
- E2E, service, queue, TTFT, and TPOT P50/P90/P95/P99/max;
- fetch, organization, buffer, admission, submit, fan-in, and writeback times;
- SLO, fairness, failure, retry, and duplication metrics;
- instrumentation overhead and artifact paths.

### 9.2 `submissions.csv`

One row per upstream HTTP/Ray submission:

- submission/job/batch/pool/endpoint identifiers;
- row count and input/output/total tokens;
- flush, admit, submit, service start, and completion times;
- buffer, admission, queue, service, and E2E latency;
- retry and final status.

### 9.3 `requests.csv`

One row per complete input row/model sequence:

- request/job/submission/pool/endpoint identifiers;
- input/output/total tokens;
- prefix group and finish reason;
- arrival, flush, admit, submit, service start, and completion times;
- buffer, admission, queue, service, and E2E latency;
- TTFT/TPOT availability;
- latency granularity (`request` or `submission`);
- retry and final status.

When an endpoint returns timing only for a multi-prompt submission, each row
records the shared submission timing and explicitly sets latency granularity
to `submission`. It must not be presented as true per-request service timing.

### 9.4 `control_trace.csv`

One row per policy event:

- pending rows/tokens and oldest age;
- in-flight, K_max, running, waiting, and KV usage;
- flush, admission, pool, and endpoint actions/reasons;
- PID error/integral/derivative;
- EWMA raw/smoothed signals;
- UCB arm/count/mean reward/confidence bonus;
- decision overhead and observation freshness.

### 9.5 `resource_trace.csv`

One row per sampling interval:

- GPU utilization, memory, power, and temperature;
- CPU and RSS;
- Ray object-store availability when exposed;
- endpoint health, running, waiting, and KV usage.

### 9.6 `manifest.json`

- exact command;
- artifact paths and schema versions;
- workload fingerprint;
- start/end times;
- warm-up/formal status;
- claim eligibility (`code_only`, `single_gpu`, or `multi_gpu`).

Every CSV row includes `run_id`, actual server version, and pgvector version.
Unavailable values remain empty with an availability field; zero is not used
as a missing-value sentinel.

Instrumentation overhead is measured. If periodic resource sampling exceeds
2% of E2E, its interval is reduced, but policy decision events are retained.

The artifacts must support, without rerunning:

- throughput/latency Pareto plots;
- latency CDF and tail bars;
- queue/K_max/flush time series;
- PID and EWMA trajectories;
- UCB convergence;
- endpoint and pool heatmaps;
- GPU utilization plots;
- workload fairness and slowdown plots;
- independent-versus-joint search comparisons.

## 10. Implementation Order

The work is divided into independently reviewable subprojects:

1. typed request, observation, topology, and metrics schemas;
2. scheduler extraction with static/immediate/round-robin behavior parity,
   first through the Ray task baseline and then the Ray actor method path;
3. batch builders and independent flush policies;
4. admission controller family;
5. actor pools and endpoint routing;
6. metrics recorder and plot-ready artifacts;
7. experiment matrix and statistical analysis;
8. independent versus joint search;
9. single-GPU formal runs and comparison report.

Each subproject receives its own implementation plan and test gate. Later
subprojects do not start by copying unfinished code from earlier ones.

## 11. Documentation and Claim Discipline

Code completion updates code documentation and learning walkthroughs.
Formal GPU results update experiment reports, project outline, and project
log. Multi-GPU claims remain explicitly pending.

The following statements are prohibited without corresponding evidence:

- multiple endpoints on one GPU scale like multiple GPUs;
- PID, EWMA, or UCB is better because it is more sophisticated;
- queue-adaptive flush is equivalent to dynamic K_max;
- a tuning-set optimum generalizes without held-out evaluation;
- a smoke test is a formal performance result.
