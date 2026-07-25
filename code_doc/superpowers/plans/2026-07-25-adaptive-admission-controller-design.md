# Adaptive Admission Controller Design

Date: 2026-07-25

## 1. Goal

Replace the current coarse two-level adaptive in-flight rule with a small,
testable AIMD admission controller and collect enough time-series evidence to
explain its behavior under the existing shared-vLLM interference workload.

This work addresses the current highest-risk experiment gap:

- static `K_max=8` foreground mean E2E: `6.602s`;
- current adaptive foreground mean E2E: `10.214s`;
- current adaptive is therefore not evidence that dynamic submission control
  outperforms a tuned static guardrail.

The controller is successful only if the formal shared-service experiment
reaches foreground mean E2E at or below `8s` without materially reducing
background throughput relative to static `K_max=8`. "Without materially
reducing" means both background tokens/s and rows/s are at least 90% of the
corresponding static-baseline means. If three controller iterations fail this
criterion, the adaptive contribution is downgraded as already specified in
the project plan.

## 2. Scope

### In scope

- isolate controller state and decisions from Ray, HTTP, Daft, and Arrow;
- remove the blocking sleep from the admission decision path;
- retain static `K_max=8` and the existing two-level policy as baselines;
- add a deadband asymmetric AIMD policy;
- record K_max, in-flight, vLLM queue/running, and KV-cache time series;
- add `tokens_per_s` and systematic service P99 to new result rows;
- compare length-capped output with an EOS-permissive output cap;
- add unit, CLI, and local integration checks.

### Out of scope

- EWMA, PID, learned controllers, or automatic parameter search;
- actor-pool routing, multiple endpoints, or joint batching/scheduling search;
- background-thread frameworks;
- unrelated refactoring of the profiling script;
- multimodal experiments.

## 3. Alternatives Considered

### 3.1 Instrument first, then replace the control law — selected

Add observable traces, isolate the fixed-output confounder, and then compare
the AIMD controller with both static and existing adaptive baselines.

This adds one baseline run but keeps performance changes attributable and
debuggable.

### 3.2 Replace the controller immediately

This is faster to code, but another negative result could not distinguish
control-law failure from synchronous scrape overhead, initial overshoot, or
output-length homogeneity.

### 3.3 Offline trace replay before implementation

This could reduce GPU search time, but the existing experiment lacks the
required K_max/queue/in-flight time series. Synthesizing that trace would
weaken rather than strengthen the evidence.

## 4. Architecture

Add `code/src/admission.py` with four engine-independent types:

- `AdmissionConfig`: minimum/maximum window and AIMD thresholds;
- `AdmissionObservation`: current in-flight count plus vLLM
  running/waiting/KV-cache signals;
- `AdmissionDecision`: resulting window, action, and reason;
- `AimdAdmissionController`: the only stateful control-law implementation.

The controller accepts one observation and returns one decision. It performs
no networking, sleeping, Ray calls, file I/O, or engine-specific work.
Strategy code therefore remains independent of Daft and Arrow, as required by
`code/AGENTS.md`.

`code/src/metrics.py` remains responsible for parsing and scraping Prometheus
metrics. `code/scripts/postgres_ai_operator_profile.py` owns submission,
backpressure execution, and trace collection. The shared-vLLM experiment
runner only constructs reproducible scenario commands.

The first controller implementation must remain below 100 lines excluding
dataclasses and abstract definitions. It uses:

- additive increase: `window + 2`;
- multiplicative decrease: `floor(window * 0.5)`;
- congestion when `waiting > 0` or KV usage is at least `0.85`;
- low load when `waiting == 0`, KV usage is at most `0.50`, and running
  sequences are below `64`;
- a hold region between low-load and congestion conditions;
- initial/minimum window `4` and maximum window `16`;
- explicit minimum and maximum bounds.

This is inspired by the asymmetric AIMD and deadband pattern in CONCUR, while
remaining explicit that CONCUR is an unreviewed agentic-inference preprint and
that this project controls upstream request in-flight limits rather than
agent-level KV continuity. Clipper provides the established NSDI AIMD
baseline. The implementation is an adaptation to this project's shared-vLLM
submission boundary, not a claim of reproducing either system.

## 5. Data Flow

For each admission decision:

1. The submission loop obtains the latest vLLM metrics snapshot.
2. It builds an `AdmissionObservation` with current in-flight state.
3. The controller returns `(window, action, reason)`.
4. The loop either submits the next request or waits for one completion.
5. The loop appends a trace row.

Each trace row contains:

```text
elapsed_s, inflight, k_max, running, waiting, kv_usage, action, reason
```

The main experiment CSV stores summary metrics and the trace path. The full
time series is written as a separate CSV so the main result remains tabular
and easy to aggregate.

Sampling is throttled to one fresh observation per `0.25s` of monotonic time
and does not call `sleep`. When the sample interval has not elapsed, the
latest snapshot may be recorded for observability but must not update the
controller window again.

## 6. Error Semantics

- Missing metrics or a scrape failure holds the current window.
- A deadband observation holds the current window.
- Every updated window is clamped to configured bounds.
- A trace write failure fails the experiment rather than silently producing
  an undiagnosable formal result.
- Static scheduling remains independent of Prometheus availability.
- The EOS-permissive group is described by the configured cap until the
  observed output-token distribution confirms that requests actually stop at
  varied lengths.

## 7. Experiment Design

### 7.1 Confounder check

Use the existing ShareGPT/BurstGPT arrival-ordered shared-service workload.
Compare:

- static `K_max=8`;
- existing two-level adaptive;
- the existing length cap (`max_tokens=64`);
- a higher EOS-permissive cap (`max_tokens=256`).

Actual output-token counts must be recorded and summarized. A higher cap alone
is not evidence of variable output. The EOS-permissive label is validated only
when at least 80% of requests finish below the cap and output-token P95 exceeds
P50 by at least 16 tokens; otherwise the result is reported only as a
`max_tokens=256` cap comparison.

### 7.2 Controller comparison

Under identical workload, foreground/background sizes, arrival order, ramp-up
offset, model endpoint, and three formal repeats, compare:

- static `K_max=8`;
- existing two-level adaptive;
- AIMD adaptive.

Primary metrics:

- foreground E2E mean and service P99;
- background tokens/s and rows/s;
- output-token distribution;
- queue, in-flight, and K_max time series.

Secondary metrics:

- background E2E;
- bounded wait;
- AIMD increase/decrease/hold counts.

The controller is not declared superior from a smoke run or a single repeat.

## 8. Test Strategy

Production changes follow red-green-refactor:

1. A low-load observation increases the window by two and respects the maximum.
2. Queue or KV congestion halves the window and respects the minimum.
3. Deadband and missing observations hold the current window.
4. Decisions expose complete, stable trace fields.
5. The experiment runner emits static, legacy adaptive, and AIMD scenarios
   for both output-cap groups.

After focused tests pass:

- run the complete `code/tests` suite;
- run profiling and experiment-runner CLI help/dry-run checks;
- run syntax/import checks;
- inspect local PostgreSQL and vLLM availability;
- if services are available, run a small integration smoke before formal GPU
  repeats.

Unit-test success and formal GPU experiment success are reported separately.

## 9. Maintainability Constraints

- Use descriptive domain names rather than policy abbreviations in public APIs.
- Keep controller decisions deterministic for a given state and observation.
- Do not expose dictionaries as the controller's public API.
- Do not duplicate the AIMD rule in actor and task submission paths.
- Do not change unrelated formatting or existing baseline behavior.
- Every changed production line must trace to controller isolation,
  observability, experiment construction, or a required metric.

## 10. Documentation and Result Updates

Implementation updates:

- `code/README.md` and `code/scripts/README.md`;
- relevant `learning/` walkthrough after behavior is verified;
- `PROJECT_INDEX.md` and `PROJECT_LOG.md`.

Formal result updates, only after GPU repeats:

- `experiments/results/local_vllm_qwen15b_baseline/README.md`;
- `experiments/plans/experiment_status_and_gaps.md`;
- `PROJECT_OUTLINE.md`;
- `PROJECT_LOG.md`.

No performance claim is updated from unit tests or dry runs.
