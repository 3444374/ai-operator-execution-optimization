# Saturated Active-Work and Ray Actor-Pool Replenishment Design

Date: 2026-07-29

Status: design approved in discussion; implementation plan and production code
remain gated on document review.

## 1. Goal

This change must answer two separate research questions without conflating them:

1. How much predicted active work is required to keep each vLLM endpoint saturated?
2. After saturation, can upstream organization and submission policies improve
   throughput, tail latency, or SLO goodput at the same offered work?

The target is the external execution path of a database AI operator:

```text
Daft rows
  -> planning batches
  -> service quanta
  -> Ray endpoint router
  -> endpoint-local Ray dispatcher
  -> bounded Ray HTTP worker pool
  -> existing vLLM endpoint
```

Ray schedules upstream stateful submitters to external GPU-backed endpoints. This
design does not modify Ray's cluster scheduler, vLLM's scheduler, CUDA kernels, or
the model server.

## 2. Evidence and problem statement

### 2.1 Capacity is still a confound

The completed active-work curve increased throughput from about 4,888 tokens/s at
16K predicted tokens per endpoint to about 8,129 tokens/s at 65K. The 49K to 65K
step still gained about 5.5%, while tail latency increased. Therefore 49K is only a
knee candidate and 65K is only the highest tested throughput boundary. Neither is
proof that the endpoint is saturated.

The fixed-work token-budget results show that a larger planning batch is not
monotonically better: at 49K active work, the 32K token-budget arm was about 23%
slower than the 16K arm, while the 65K arm at 65K active work had high repeat
variance. However, this run was also affected by a duplicate-runner incident
described in section 2.4, so it is diagnostic evidence rather than a promotable
formal result.

The first gate is consequently a wider active-work capacity curve. Strategy
claims are not allowed until a saturation point has been selected by a documented
rule.

### 2.2 Whole-submission completion creates avoidable coupling

In current batch mode, one Ray submission sends one multi-prompt HTTP request.
The submission completes only after the endpoint returns the whole response.
Admission credit associated with that submission is therefore held by the
slowest prompt in the request.

This creates three coupled effects:

- within-submission head-of-line blocking;
- wave-like replenishment instead of completion-driven replenishment;
- idle admission credit when most prompts have completed but one straggler
  remains.

Request mode avoids the shared completion boundary, but it also changes the
amount of work carried by each admission slot. Existing results already show why
request-count `K` cannot be compared directly with batch-count `K`. The durable
abstraction must be predicted token work, not submission count.

### 2.3 The current actor pool is not yet a scheduling strategy

The existing Ray implementation can create multiple workers per endpoint, but
selection is round-robin and the router does not maintain per-worker active work,
free slots, service rate, or health. Endpoint topology begins with zero running
and waiting work, so names such as least-work do not yet imply a live,
work-conserving scheduler.

A synchronous actor with high `max_concurrency` may also move waiting work into
Ray's internal actor queue. Hidden queued work is outside the admission
controller's direct accounting and can weaken backpressure.

The actor-pool contribution must therefore be tested as:

- bounded visible capacity;
- explicit per-actor state;
- fixed total concurrency while pool shape changes;
- completion-triggered credit release;
- no dependency on actor count alone as a proxy for more offered load.

### 2.4 Duplicate-runner incident

During the fixed-work curve, an incorrect process-name check failed to find the
original scenario runner. A second `--resume` process was then started against
the same output directory. Two writers raced on the same CSV and manifest.

The current correctness check assumes a single writer:

```text
row count after run == row count before run + 1
```

That invariant no longer held, producing `missing_expected_csv_row` failures.
The affected 21 of 36 run records are an infrastructure incident, not evidence
that the scheduling policies failed. No further formal resume is allowed on that
directory until exclusive-runner ownership is implemented.

## 3. Relationship to existing designs

This document is incremental:

- It retains the endpoint/worker separation, zero actor GPU reservation, and
  zero automatic retry boundary from
  `2026-07-26-ray-vllm-execution-tuning-design.md`.
- It retains organization/submission metric separation and shared active-work
  semantics from
  `2026-07-28-dual-gpu-experiment-correctness-design.md`.
- It refines request-level replenishment into a fixed service-quantum design so
  organization batches can remain large without making one row the only
  submission unit.
- It promotes live actor-pool state and endpoint-local replenishment from future
  work to explicit strategy dimensions.
- It adds runner exclusivity because the latest incident invalidated the
  single-writer assumption.

Where this document conflicts with those designs for the next execution-control
experiments, this document takes precedence.

## 4. Alternatives considered

### 4.1 One HTTP request per row

This gives the finest completion signal and removes within-request head-of-line
blocking. It also maximizes Ray and HTTP overhead and makes connection behavior a
larger confound. It remains a diagnostic baseline, not the default policy.

### 4.2 Fixed service quantum

This is the selected first implementation. A planning batch is sliced into
ordered quanta whose predicted work is bounded by a configurable token target.
A single prompt row is never split. Oversized rows form one-row quanta and are
marked explicitly.

This preserves token-budget and length-aware data organization while making the
completion and credit-release unit smaller than a full planning batch.

### 4.3 Dynamic service quantum

A later policy may adapt quantum size from queue delay, replenish gap, service
EWMA, and oldest-request slack. It is deferred until fixed quanta show a stable
benefit. Otherwise it would combine mechanism validation and adaptive-policy
tuning in one experiment.

### 4.4 Central driver versus endpoint-local dispatcher

A driver-owned bounded pool is the smallest correctness slice and is useful for
testing state accounting. The selected final architecture moves the endpoint
pending queue and replenishment loop into one stateful dispatcher actor per
endpoint. This reduces driver round trips and lets each endpoint refill a freed
slot immediately.

The move occurs only after the bounded driver-owned implementation passes trace
and failure tests. Both variants use the same scheduler interfaces and metrics.

## 5. Core abstractions

### 5.1 Planning batch

A planning batch is the output of data organization. Its policy may be sequential
token-budget, row-cap-aware, length-aligned, or prefix-aware. It owns row order
and organization metrics, but it is not an admission-credit unit.

### 5.2 Service quantum

A service quantum contains one or more complete rows from one planning batch.
Required fields are:

- quantum ID and parent planning-batch ID;
- ordered request IDs and document IDs;
- predicted input, output, and total token work;
- row count and oversized-row marker;
- organization policy and quantum policy;
- creation, admission, submit, service-start, and completion timestamps.

The first algorithm is deterministic ordered first-fit slicing:

1. retain the planning batch's row order;
2. append a row while the quantum target is not exceeded;
3. close the current quantum before a row that would exceed the target;
4. emit a row larger than the target as an oversized one-row quantum.

Slicing policy is isolated from scheduling policy so it can be unit tested and
ablated independently.

### 5.3 Global endpoint router

The global router chooses an endpoint with advertised pending capacity and sends
one bounded enqueue operation. Static round-robin remains the control. A later
least-predicted-drain policy may use:

```text
predicted drain = visible active work / service-rate EWMA
```

Routing uses snapshots reported by dispatchers. The router permits at most one
unacknowledged enqueue call per dispatcher; the acknowledgement means the quantum
has entered the dispatcher's visible pending queue, not that service has
completed. This prevents the router from replacing the explicit queue with an
unbounded Ray actor mailbox.

### 5.4 Endpoint dispatcher

One dispatcher actor owns one logical endpoint:

- a bounded pending queue, limited by quantum count and predicted work, outside
  HTTP worker actor mailboxes;
- endpoint active-work credit;
- worker free-slot and active-work state;
- service-rate and latency EWMA;
- the completion loop;
- endpoint-local trace emission.

The dispatcher is an async actor. It acknowledges enqueue only after the quantum
has been copied into visible pending state. True service admission then requires
both:

```text
endpoint_active_work + quantum_work <= active_work_limit
```

and an available visible worker slot. An explicitly marked oversized quantum may
cross the work limit only when the endpoint has no other active quantum.

Each completed or failed quantum releases its exact reserved credit in a
`finally` path. The dispatcher then admits the next eligible quantum without
waiting for siblings from the same planning batch.

The async control loop never calls blocking `ray.get`. It awaits object
references or delegates blocking transport to worker actors so one slow result
cannot stop completion processing for the other workers.

### 5.5 HTTP worker actor pool

Workers perform transport and response parsing only. They do not decide endpoint
routing or active-work admission. Each worker reports:

- actor ID, actor index, and process ID;
- configured slots and current running calls;
- predicted active work;
- call start, HTTP start/end, and result publication timestamps;
- success or structured failure.

Workers retain `num_gpus=0`; GPUs belong to vLLM endpoint processes. Initial
experiments compare pool shapes at fixed total call slots:

- 1 actor x 16 slots;
- 2 actors x 8 slots;
- 4 actors x 4 slots.

Connection reuse, if introduced, is a separate ablation. It must not be silently
combined with the first service-quantum comparison.

## 6. Saturation calibration

The saturation sweep precedes strategy comparison.

### 6.1 Controlled variables

- same model, endpoint flags, data, seeds, row ordering, and output cap;
- same number of endpoints and same per-endpoint work semantics;
- fixed planning and submission policy;
- one warm-up and at least three formal repeats;
- no concurrent experiment runner;
- per-endpoint metrics captured for every formal run.

The initial extension points are 65K, 82K, 98K, and 131K predicted tokens per
endpoint. Unsafe points may be stopped by the predeclared memory or latency guard,
and the stop is recorded rather than removed.

### 6.2 Selection rule

Let `T_max` be the highest repeat-mean throughput among safe tested points. Select
the smallest work limit `W` satisfying all of:

- mean throughput is at least 97% of `T_max`;
- the next safe increment improves throughput by less than 3%;
- endpoint GPU utilization is high and waiting work is non-zero for a material
  portion of the run;
- failure, memory, and tail-latency guards pass.

If no point satisfies the rule, report “saturation not reached” and extend or
stop the curve. Do not rename the highest tested point as saturation.

## 7. Scheduling behavior

### 7.1 Work-conserving replenishment

At each completion, the dispatcher:

1. records actual service and output work;
2. releases exactly the quantum's reserved work and worker slot;
3. updates endpoint and worker EWMAs;
4. selects the next eligible pending quantum;
5. submits immediately and records the replenish gap.

The policy is work-conserving subject to bounded active work and visible worker
slots.

### 7.2 Worker selection

Round-robin is the control. The first state-aware candidate chooses the healthy
worker with the smallest:

```text
predicted active work / configured slots
```

Ties are deterministic by actor index. Queue length alone is not sufficient
because request work is heterogeneous.

### 7.3 Adaptive control is a later layer

Oldest-request slack, token backlog, arrival EWMA, and service EWMA may later
control flush timing, active-work limit, or quantum size. The first implementation
exposes these signals but does not use all of them in one policy. This preserves a
causal path from mechanism to adaptive algorithm.

## 8. Reliability and failure semantics

### 8.1 Exclusive runner lease

Each formal output directory has one atomic lease containing:

- host and process ID;
- process start identity where available;
- start timestamp;
- command/config fingerprint;
- repository commit;
- output directory.

A new run or `--resume` refuses to start when a live matching owner exists.
Stale recovery requires an explicit flag and records the previous lease as an
incident. A config fingerprint mismatch is always rejected unless a new output
directory is selected.

The lease covers manifest and CSV writes and is released on normal exit. Abrupt
termination leaves evidence for stale-owner inspection.

### 8.2 Exception-safe accounting

Ray task failure, actor failure, response parsing failure, and cancellation all
pass through one completion path. Reserved credit, pending context, and visible
slots are released exactly once. The failed quantum remains traceable.

### 8.3 Retry boundary

Automatic retry remains disabled for ambiguous completion calls because the
remote request may have executed even if the response was lost. Safe retry
requires an explicit idempotency contract and is outside this change.

## 9. Observability

The trace must make queueing location and credit lifetime observable.

Required events or derived intervals include:

- driver-ready, Ray-submit, actor-start, HTTP-start, HTTP-end, and completion;
- Ray mailbox/dispatch delay;
- endpoint pending delay and worker wait;
- HTTP service time and response publication delay;
- credit reserve/release time and held-credit duration;
- completion-to-next-submit replenish gap;
- endpoint and worker predicted active work;
- inflight quanta, rows, and call slots;
- predicted versus actual input/output token work;
- actor identity, endpoint identity, and parent planning-batch identity;
- per-endpoint vLLM running, waiting, cache, throughput, and GPU metrics.

Timestamps from different processes are used for duration comparison only when
their clock source and synchronization are valid. Otherwise each process emits
local intervals and the driver derives only causally safe spans.

Primary outcome metrics are tokens/s, request P99, SLO goodput, MFU, actor-slot
utilization, Ray queue P95/P99, replenish gap, endpoint work imbalance, and
failure/exactly-once counts.

## 10. TDD and implementation boundaries

Production code is implemented only after a separate implementation plan is
approved. The intended test-first slices are:

1. exclusive runner lease and stale-owner behavior;
2. exception-safe release of scheduler context and token credit;
3. deterministic service-quantum slicing, including oversized rows;
4. bounded visible worker slots and per-worker state;
5. completion-driven replenishment;
6. endpoint-local dispatcher parity with driver-owned behavior;
7. trace/schema integration and backwards-compatible defaults;
8. scenario-runner configuration and resume validation.

Policy functions remain small and pure where practical. Models, policy decisions,
Ray transport, metrics, and CLI/config parsing stay in separate modules. Existing
batch behavior remains the default until the new path passes local and remote
gates.

## 11. Causal experiment sequence

Experiments run in this order:

1. **Reliability gate**: lease collision, crash cleanup, error accounting, and
   trace completeness.
2. **Active-work curve**: find or explicitly fail to find saturation.
3. **Total actor-slot capacity**: retain 256 visible HTTP call slots per
   endpoint from the request-level saturation baseline. The original 16-slot
   draft was rejected before launch: at about 332 work/request or 1337
   work/organization-batch it exposes only about 5.3K/21K work, below the
   65K–131K saturation range.
4. **Pool shape**: compare 1x256, 2x128, and 4x64 at fixed total slots, fixed
   0.5 Ray CPU reservation per endpoint, and fixed active work.
5. **Service quantum**: compare whole planning batch, fixed quantum sizes, and
   one-row diagnostic at the selected saturation work.
6. **Worker routing**: round-robin versus least-active-work.
7. **Replenishment location**: driver-owned push versus endpoint-local
   completion replenishment.
8. **Combination**: combine only individually supported mechanisms.

Only one causal variable changes in each formal comparison. Resource settings,
connection behavior, planning policy, and active work are otherwise held fixed.

## 12. Promotion and negative-result rules

A strategy is promoted when repeated results exceed observed noise and satisfy
one of:

- at least 5% throughput or SLO-goodput improvement with request P99 no more than
  5% worse; or
- equivalent throughput with a material, predeclared tail-latency improvement.

All promoted runs require zero unexpected failures, trace completeness, and
exactly-once accounting. Confidence intervals or repeat distributions are
reported; a single best run is not sufficient.

If actor-pool shape, service quanta, or local replenishment do not outperform the
saturated simple baseline, record the negative result and retain the simpler
implementation. “No strategy benefit after saturation” is a valid research
result; increasing offered work is not reported as a scheduling contribution.

## 13. Non-goals

- modifying vLLM continuous batching, PagedAttention, or model kernels;
- modifying Ray's cluster scheduler;
- claiming GPU scheduling inside the upstream Ray layer;
- adding retries without idempotency;
- combining persistent HTTP connections with the first scheduling ablation;
- starting multimodal experiments before the text mechanism gates are complete.

## 14. Literature-derived design patterns

The implementation is literature-first, while every mapping remains a design
inference until validated in this workload:

- Orca and vLLM motivate completion-driven replenishment and work-conserving
  serving.
- Clipper motivates separating batching delay from capacity control.
- Scorpio-style variable batching motivates work-weighted rather than
  request-count credit.
- Ray's actor, backpressure, and actor-pool patterns motivate stateful endpoint
  control and keeping waiting work out of hidden actor mailboxes.
- SFS/SABER-style service prediction motivates the later service-rate and
  predicted-drain policy.
- CONCUR-like concurrency signals are treated as observations, not proof that
  more concurrency is always beneficial.

The project literature notes remain the citation and evidence source. This design
does not itself upgrade a literature analogy into a measured project conclusion.

## 15. Experiment record

Every tested policy is registered in the infra/result ledger with:

- code commit, config fingerprint, model and endpoint flags;
- selected saturation work and the evidence used to select it;
- planning policy, quantum policy, routing policy, pool shape, and total slots;
- raw trace locations and summary CSV;
- repeat statistics, incidents, interpretation, and promotion decision.

This preserves the full optimize-measure-explain loop and prevents a later
conversation from reconstructing runtime assumptions from chat history.
