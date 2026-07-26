# Multi-Endpoint Routing Readiness Design

Date: 2026-07-26

## 1. Goal

Make the existing Daft -> Arrow -> Ray -> vLLM execution path ready for a
future real multi-GPU environment without claiming multi-GPU performance from
the current single-GPU machine.

The immediate implementation has two narrowly scoped goals:

1. remove deterministic endpoint pinning when multiple healthy endpoints have
   the same observed queue depth;
2. add an optional routing policy that balances estimated request work rather
   than request count.

The existing single-endpoint defaults remain unchanged. The new policy is a
candidate until real multi-GPU experiments satisfy the promotion gates in this
document.

## 2. Current Finding

`LeastQueuedEndpointRouter` currently selects:

```text
min(running + waiting, endpoint_id)
```

When endpoint observations are equal, the endpoint with the lexicographically
smallest ID wins every decision. A fresh homogeneous multi-endpoint topology
can therefore route an initial burst to one endpoint even though all endpoints
are equally available.

This is a correctness and readiness problem, not evidence of a measured GPU
performance regression. The current formal experiments use one endpoint, so
they cannot expose or quantify this behavior.

## 3. Evidence Boundary

Three environments must be labeled separately:

| Environment | What it validates | What it does not validate |
|---|---|---|
| One endpoint on one GPU | Current single-GPU performance baseline | Routing balance or multi-GPU scaling |
| Two logical endpoint IDs targeting one real vLLM service | Routing decisions, trace attribution, exactly-once execution, client-side overhead | Independent service capacity, failure isolation, multi-GPU speedup |
| Two real endpoints on separate GPUs | Load balance, throughput scaling, tail latency, fairness, failover | Heterogeneous-GPU generalization unless the GPUs differ |

Two vLLM processes sharing the current GPU are not required. They would measure
single-device replica contention and must not be presented as a multi-GPU
result.

## 4. Alternatives

### 4.1 Documentation only

Record future experiments and leave the router unchanged.

This is the smallest change but preserves a known equal-load endpoint bias.

### 4.2 Tie-fair least-queued plus estimated-work balancing -- selected

Keep the established routers, make equal-load least-queued selection rotate
deterministically, and add a separate router that balances cumulative
pre-execution cost.

This is small, testable, and useful for both homogeneous and heterogeneous
request sizes. It does not require a speculative online capacity model.

### 4.3 Online learned capacity routing

Estimate each endpoint's service rate from EWMA latency, GPU memory, and queue
time, then route by normalized predicted completion time.

This may eventually help heterogeneous GPUs, but the current machine cannot
produce independent endpoint observations. Implementing it now would create an
untestable control mechanism and duplicate lessons from the negative
single-GPU adaptive-controller experiments.

## 5. Routing Design

Before routing, the execution adapter must distinguish a vLLM service endpoint
from the Ray actor workers that submit to it. One endpoint may own multiple
actors, but those actors must not appear as separate `EndpointSnapshot`
instances. The endpoint-local actor pool is specified in
`2026-07-26-ray-vllm-execution-tuning-design.md`.

### 5.1 Tie-fair least-queued

`LeastQueuedEndpointRouter` continues to minimize observed
`running + waiting`. When two or more healthy endpoints share the minimum, it
selects them in deterministic round-robin order within each pool.

Required properties:

- endpoints with a strictly smaller observed queue always win;
- equal-load endpoints receive decisions evenly;
- unhealthy endpoints and endpoints outside the selected pool are excluded;
- behavior is deterministic for a fixed decision sequence;
- router state is isolated per router instance and pool.

### 5.2 Least estimated work

Add `LeastEstimatedWorkEndpointRouter`. For each pool, it tracks cumulative
work assigned by this router instance and selects the healthy endpoint with the
lowest assigned work. Request work is:

```text
estimated_work = max(1, request.estimated_total_tokens)
```

After selection, the request's estimated work is added to that endpoint. Ties
use deterministic endpoint ID ordering only for the first decision; subsequent
decisions are separated by accumulated work.

This policy intentionally balances job-level assigned work, not instantaneous
service backlog. It needs no completion callback and is suitable for finite
database AI operator jobs. The future online multi-GPU experiment may add
outstanding-work accounting only if cumulative balancing fails a stated
fairness or tail-latency gate.

The first implementation uses `BatchRequest.estimated_total_tokens`, so it
remains independent of Daft, Arrow, Ray, HTTP, model names, and prompt content.
It is therefore a text-workload candidate, not yet a modality-neutral routing
claim. A future image workload must add one explicit neutral `estimated_work`
field at the scheduling-model boundary and reuse the router without inspecting
image data directly.

## 6. Interfaces and Configuration

The public endpoint-routing choices become:

```text
round_robin
least_queued
least_estimated_work
prefix_affinity
```

No existing CLI value changes meaning. `round_robin` remains the safe
single-endpoint default.

The new router lives in `code/src/scheduling/routing.py` and implements the
existing `EndpointRouter` protocol. No new framework, background thread, or
engine-specific dependency is introduced.

Routing decisions retain the selected endpoint, pool, and a stable reason:

```text
least_queued
least_estimated_work
```

Existing submission lifecycle traces remain the source of endpoint assignment
evidence.

## 7. Tests

Production changes follow red-green-refactor.

### 7.1 Unit tests

1. Equal-load least-queued decisions rotate across healthy endpoints.
2. A strictly lower observed queue wins even when rotation state exists.
3. Rotation state is independent by pool.
4. Estimated-work routing sends a large request to one endpoint and enough
   small requests to the other until assigned work is balanced.
5. Unhealthy endpoints are excluded.
6. Existing round-robin, request-pool, prefix-affinity, and single-endpoint
   behavior remain unchanged.

Tests instantiate the real routing and scheduler objects. No fake model result
is used as performance evidence.

### 7.2 Real-service logical dual-endpoint gate

Use two endpoint IDs that target the same currently running real vLLM service.
Run the same 512-request workload for:

- one endpoint with round-robin;
- two logical endpoints with round-robin;
- two logical endpoints with least-queued;
- two logical endpoints with least-estimated-work.

This gate verifies:

- 512 unique requests complete exactly once;
- both logical endpoint IDs appear in submission traces;
- assignment count and estimated-work imbalance are reported;
- request, submission, resource, and vLLM metrics remain complete;
- client-side routing does not create a material regression.

For this gate, a material client-side regression means median E2E or tokens/s
worsens by more than 5% relative to the one-endpoint run. Passing the gate
means the routing path is operational, not faster.

## 8. Future Real Multi-GPU Experiment

When at least two independently observable GPU endpoints are available, run
the following in randomized order with one warm-up and at least three formal
repeats:

1. round-robin;
2. tie-fair least-queued;
3. least-estimated-work;
4. prefix-affinity when prefix caching is explicitly enabled and audited.

Workloads:

- identical 512 and 1024 request sets used by the single-GPU baseline;
- mixed short/long requests;
- burst arrival replay;
- controlled repeated-prefix workload;
- one endpoint made unhealthy after the run reaches steady state.

Required metrics:

- tokens/s, E2E, request P50/P95/P99, and SLO goodput;
- per-GPU utilization, memory, power, energy, and MFU;
- per-endpoint assigned requests and estimated work;
- endpoint throughput and queue-time imbalance;
- Jain's fairness index over completed estimated work;
- failure detection and successful reroute time;
- exactly-once request and submission coverage.

Promotion requires all of:

- no correctness or trace incidents;
- at least 95% Jain fairness on homogeneous GPUs;
- no more than 1% absolute SLO-violation increase;
- either at least 5% tokens/s improvement or at least 10% request-P99 reduction
  relative to round-robin in two workload classes.

If no candidate passes, round-robin remains the default and the negative result
is retained.

## 9. Maintainability Constraints

- Keep each routing policy independent and selectable; do not hide multiple
  mechanisms behind one policy name.
- Do not add endpoint-capacity fields without real measurements that consume
  them.
- Do not modify vLLM internals or Ray's scheduler.
- Do not duplicate routing logic between Ray task and actor paths.
- Keep policy code dependent only on scheduling models.
- Preserve stable, deterministic tests and reason strings.
- Every performance conclusion must identify whether endpoints share a service,
  share a GPU, or use independent GPUs.

## 10. Documentation and Handoff

Implementation completion updates:

- `code/README.md`, `code/scripts/README.md`, and `code/INFRA_STATUS.md`;
- `learning/` routing explanation;
- `PROJECT_INDEX.md` and `PROJECT_LOG.md`.

The future multi-GPU experiment remains explicitly listed in
`code/INFRA_STATUS.md` until real independent endpoints have completed the
matrix above. A future agent must not close that item from unit tests or the
logical dual-endpoint gate.

Execution order is:

```text
single-endpoint vLLM/Ray tuning
-> endpoint-local actor pool boundary
-> logical dual-endpoint contract gate
-> real multi-GPU routing experiment
```
