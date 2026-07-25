# Row-Cap-Aware Packing and Non-Blocking Observation Design

Date: 2026-07-26

## 1. Goal

Make the next data-organization and adaptive-submission experiments
attributable:

1. adaptive admission decisions must never perform metrics network I/O on the
   submission path;
2. packing must optimize under both the token budget and the row cap that the
   runtime actually enforces;
3. classic Best-Fit Decreasing (BFD) remains an experimental baseline and is
   not retained as the default unless real single-GPU results beat the simpler
   sequential baseline.

This phase succeeds when focused and full tests pass, a real 64-row
PostgreSQL→Daft→Ray→vLLM gate produces valid lifecycle/resource/MFU evidence,
and the 512-row comparison identifies at least one candidate worth confirming
on the held-out 1024-row scale. A strategy that loses to sequential packing is
reported and removed from the recommended path.

## 2. Scope

### In scope

- wire the existing `NonBlockingMetricsObservationProvider` into AIMD,
  EWMA-AIMD, and PID production paths;
- fail closed to a hold decision while no fresh metrics sample exists;
- record the observation age/freshness needed to audit controller decisions;
- add one deterministic row-cap-aware packing candidate;
- compare sequential, classic BFD, and row-cap-aware packing under identical
  token budget, row cap, workload, model, output cap, admission limit, and
  measurement settings;
- keep `mfu_estimate`, vLLM FLOP delta, GPU utilization, power, energy,
  request latency, submission count, and packing utilization in every new
  formal result;
- stop escalation when a gate or comparison fails.

### Out of scope

- streaming Daft source/organizer refactoring;
- multi-endpoint topology refresh;
- UCB runtime integration;
- prefix-affinity experiments;
- multimodal execution;
- changing vLLM internals;
- making classic BFD the default.

Those are separate, independently testable phases. Streaming is intentionally
deferred until the packing objective is selected because global packing and
bounded-lookahead streaming have different semantics.

## 3. Alternatives

### 3.1 Recommended: row-cap-first best fit

Build batches deterministically while enforcing both hard constraints. Select
the feasible open batch that first minimizes the number of remaining row
slots, then minimizes remaining token capacity, with stable batch-index
tie-breaking.

This directly addresses the observed 1024-row failure: classic BFD achieved
high token-budget utilization but created 87 submissions under a 16-row cap,
versus 65 for sequential fixed-output packing.

Advantages:

- small change to the existing pure packing module;
- deterministic and easy to test;
- makes submission count a first-class objective;
- preserves the existing engine-independent `cost_units` boundary.

Trade-off:

- it is a heuristic, not a proof of global optimality;
- token utilization may be lower than classic BFD.

### 3.2 Weighted scalar score

Rank feasible batches with
`row_weight * remaining_rows + token_weight * remaining_tokens`.

Rejected for the first version because the weights introduce another search
space and make the mechanism harder to interpret. It is only justified if the
lexicographic rule produces a clear but inadequate trade-off.

### 3.3 Remove BFD immediately

Use sequential token-budget batching only.

Rejected as the experiment design because the 512-row run showed a positive
signal. Classic BFD should remain as a controlled baseline long enough to
locate its boundary, but it has no privileged place in the final system.

## 4. Architecture

### 4.1 Observation path

`postgres_ai_operator_profile.py` constructs a
`NonBlockingMetricsObservationProvider` for typed adaptive controllers. The
provider owns the only Prometheus sampling thread. `DynamicAdmissionGate`
reads its latest immutable snapshot and never performs network I/O.

The provider lifecycle is explicit:

1. start before submission;
2. optionally wait for one bounded initial sample during setup, outside the
   measured submission loop;
3. return `fresh=False` when the sample is missing or stale;
4. close in a `finally` path after submission;
5. fail the formal gate if the sampler thread does not stop.

No second background framework is added.

### 4.2 Packing path

The existing `PackItem(cost_units, row_index, stable_id)` remains the public
input. Add a sibling pure function rather than changing classic BFD:

```text
row_cap_aware_best_fit(items, capacity, max_rows) -> list[list[int]]
```

Both algorithms share the same validation and oversized-row semantics:

- every input row appears exactly once;
- no batch exceeds `max_rows`;
- no non-oversized batch exceeds `capacity`;
- an item larger than `capacity` occupies a singleton batch;
- output is deterministic for equal-cost inputs.

`OrganizerConfig.batching_policy` gains one explicit candidate name. Arrow and
Daft call the same packing function; no policy code imports Daft or Ray.

### 4.3 Strategy selection

There is no permanent “advanced” default in this phase. The default remains
the current sequential token-budget behavior. Experiment results decide:

- row-cap-aware beats sequential at 512 and does not reverse at 1024:
  retain as the leading candidate;
- it wins at 512 but reverses at 1024:
  retain only as a conditional policy with a documented boundary;
- it loses at 512:
  stop and keep sequential;
- classic BFD loses:
  keep its code only as a small reproducible baseline, not in the recommended
  runtime configuration.

## 5. Experiment Design

### 5.1 Real gate

Run 64 identical rows per scenario against the real local stack. The gate is
not performance evidence. It verifies:

- PostgreSQL 18.4 and pgvector version fields;
- Daft source and organizer;
- Ray task or actor submission;
- real vLLM success count equals input rows;
- request and submission exactly-once coverage;
- valid resource trace;
- non-zero vLLM FLOP delta and `mfu_status=ok`;
- batch row and token hard constraints.

### 5.2 Tuning comparison

Use the same 512 documents and three formal repeats for:

- sequential fixed-output cost;
- classic BFD fixed-output cost;
- row-cap-aware fixed-output cost.

The unpaired BurstGPT target-output metadata is excluded from the primary
comparison. It may remain a sensitivity-only secondary group.

Search:

- row cap: `16`, `32`, `64`;
- token budget: `4096`, `6144`, `8192`.

Use a staged search rather than running every cross-product immediately:

1. one repeat per point for screening;
2. discard points that violate correctness or are more than 10% below the
   sequential throughput baseline without a compensating request-P95 or
   energy improvement;
3. run three repeats only for the baseline and surviving candidates.

### 5.3 Held-out confirmation

Run the winning 512-row configuration and the unchanged sequential baseline
at 1024 rows. Do not retune on 1024.

Primary selection metrics:

- observed tokens/s and rows/s;
- request E2E P95/P99 and SLO goodput;
- submission count;
- energy per 1k observed tokens;
- `mfu_estimate`.

Packing utilization and GPU utilization are diagnostic metrics, not standalone
success criteria.

## 6. Error and Evidence Semantics

- Missing/stale adaptive observations hold the current window.
- Sampler exceptions become unavailable samples and are visible in traces.
- A metrics scrape never blocks a scheduling decision.
- A non-zero FLOP delta is mandatory for new MFU-bearing formal runs.
- MFU remains labelled as an estimate based on vLLM FLOPs and
  `operator_wall_s`; it is not kernel-profiler MFU.
- An algorithm is never called “better” from GPU utilization or packing
  utilization alone.
- Negative BFD results are retained as boundary evidence and do not require
  further optimization.

## 7. Testing

All production behavior follows RED→GREEN:

1. a test proves typed adaptive construction uses the non-blocking provider;
2. a test proves a slow sampler does not delay `latest()` or admission;
3. lifecycle tests prove the provider closes after success and failure;
4. canonical packing tests prove row-slot priority changes membership where
   classic BFD fragments by row cap;
5. invariant tests prove exactly-once, capacity, row cap, oversized singleton,
   and determinism;
6. Arrow and Daft organizer contract tests prove shared behavior;
7. profiler CLI/dry-run tests prove stable scenario names and metrics fields;
8. the full code test suite, compile check, and real Daft→Ray contract pass
   before GPU work.

## 8. Maintainability Constraints

- no new dependency;
- no duplicate Arrow/Daft packing implementation;
- no controller-specific sampling thread;
- no changes to unrelated formatting or legacy baselines;
- new strategy code stays engine-independent;
- no generic multi-resource abstraction until a real second resource unit is
  implemented;
- every new formal CSV records the algorithm, token budget, row cap, cost
  source, model/tokenizer IDs, FLOP source, MFU method, and hardware identity.

