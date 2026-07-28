# SLO-aware EWMA flush implementation plan

**Date:** 2026-07-29

## Goal

Replace the current two-level `queue_adaptive` baseline with one small,
auditable candidate that has an actual decision opportunity under burst/gap
load. The controller must use oldest-request slack and arrival/service EWMA,
retain a fixed-50ms fallback and hard deadline, and keep request-level
completion as the credit boundary.

This plan follows the negative saturated-work results:

- more active work plateaus above 65,536/endpoint;
- more Ray actors do not pass the 5% promotion gate;
- complete-row quantum reduces credit-held but not steady-state throughput.

The next experiment therefore changes workload dynamics, not offered-work
capacity.

## Design constraints

- Pure policy code remains independent of Arrow, Daft, Ray and HTTP.
- The rule implementation stays below 100 lines excluding dataclasses.
- No prompt is split.
- Missing/stale feedback falls back to the already validated fixed 50ms.
- Budget reached or exhausted SLO slack flushes immediately.
- State is local to one policy instance; production creates one instance per
  replay/endpoint control domain.
- The formal comparison fixes request granularity, 65,536 active work and
  1×256 actor topology.

## Task 1: Policy RED tests

**Files**

- Modify `code/tests/test_flush_policies.py`
- Modify `code/tests/test_runtime_batching.py`

Add tests for:

1. stale/missing rates select the fixed maximum fallback;
2. budget reached selects zero wait;
3. exhausted SLO slack selects zero wait;
4. an idle service selects the minimum wait;
5. a busy service interpolates the min/max window from EWMA load ratio;
6. small target changes inside the deadband hold the previous window;
7. replay passes token budget and arrival/service rates into the policy and
   records the selected reason.

Run the two focused modules and require RED before production edits.

## Task 2: Implement the pure controller

**Files**

- Modify `code/src/scheduling/flush.py`
- Modify `code/src/scheduling/__init__.py`

Extend `FlushObservation` with backwards-compatible defaults for token budget,
arrival-rate tokens/s and service-rate tokens/s/endpoint. Add
`SloAwareEwmaFlush` with:

```text
budget full              -> 0ms
stale/missing feedback   -> fixed max wait
predicted SLO slack <= 0 -> 0ms
service idle             -> min wait
service busy             -> interpolate arrival/aggregate-service load ratio
small target movement    -> previous window (deadband)
```

The service EWMA estimates the current submission's service time only; it does
not claim to predict full autoregressive output or vLLM internals.

## Task 3: Wire replay, CLI and observability

**Files**

- Modify `code/src/scheduling/batching.py`
- Modify `code/src/profiling/cli.py`
- Modify `code/src/profiling/replay.py`
- Modify `code/scripts/postgres_ai_operator_profile.py`
- Modify `code/src/profiling/schema.py`
- Modify relevant profiler tests

Add `slo_ewma` to `--flush-policy`, plus recorded
`--flush-ewma-alpha` and `--flush-deadband-ratio`. Require a positive
`--request-slo-ms` for this policy. Pass the current builder token budget,
arrival EWMA and sampled service rate into every flush observation.

The existing flush trace already records pending work, oldest age, selected
wait/reason and arrival/service rates; preserve schema 3 and verify the new
reasons are visible.

## Task 4: Add the remote gate and formal matrix

**Files**

- Create `deploy/autodl/dual_gpu_slo_ewma_flush.example.json`
- Modify `code/tests/test_experiment_scenarios.py`
- Modify `deploy/autodl/README.md`

Use request granularity, active work 65,536/endpoint and 1×256 actors. Compare
fixed 50ms, current two-level queue baseline and SLO-EWMA at:

- high-rate replay (`arrival_time_scale=0.001`);
- near-capacity burst/gap replay (`arrival_time_scale=0.006`).

Run a 128-row correctness/trace gate first, then one warm-up plus three formal
repeats per arm. Require exactly-once rows, no worker failures, fresh trace
reasons after warm-up, resource/MFU status, and no runner incidents.

Promotion requires at least 5% SLO goodput or throughput gain with P99 no more
than 5% worse, or a material predeclared P99/SLO improvement at equivalent
throughput. If the controller collapses to one fixed window, retain fixed-50.

## Task 5: Verify and close the loop

Run:

```powershell
.\.conda\pg-ai-profile\python.exe -m pytest code/tests -q -p no:cacheprovider
.\.conda\pg-ai-profile\python.exe -m ruff check code
git diff --check
```

Update code/runbook/result ledgers, commit without AI attribution, push `main`,
safely synchronize the idle remote checkout while preserving untracked traces,
then execute the gate and formal matrix. Archive compact result files and keep
large traces on the server.

## Execution note: feedback-provider gate

The first 128-row gate and a 512-row feedback gate exposed a wiring omission:
the replay layer recognized `slo_ewma` as feedback-driven, but the profiler
provider-lifecycle condition still created a live metrics provider only for
`queue_adaptive` or dynamic token budget. Every new-policy event therefore used
`fixed_fallback`. The fix uses one shared feedback predicate for both call
sites and adds a regression test. These two pre-fix gates are wiring evidence,
not performance evidence; a post-fix gate must show positive service-rate rows
and non-fallback controller reasons before the formal matrix starts.

The post-fix gate then showed that full-budget fill time still collapsed to
50ms: the 32K budget required 1.54s/2.78s at the high/near p50 arrival rates,
far outside a 25–50ms control window. The busy rule was therefore revised to
interpolate on global-arrival / aggregate-service EWMA around ratio 1.0. The
same trace showed that scale 0.002 remained overloaded (p50 ratio about 2.9),
so the near-capacity arm was moved to 0.006 before formal execution.

The second load-ratio gate exposed another identifiability issue: achieved
service throughput falls with offered load in a burst/gap run, so it is not an
independent capacity estimate. The high/near p50 online ratios remained
3.68/2.52. The third revision therefore floors each endpoint's service EWMA
with the independently measured saturated capacity (4,000 tokens/s/endpoint
for this model/GPU setup). The calibrated value is explicit in CLI, CSV and the
scenario template and must be remeasured when hardware or model changes.

The calibrated-capacity gate finally separated the selected windows
(high mean/P50 46.86/50ms; near 37.29/29.03ms), but exposed an execution-loop
bug before formal promotion. Under sparse replay, the synchronous scheduler
blocked inside the input generator until the next arrival and did not poll
already-complete Ray ObjectRefs. Backend service ended in seconds while
completion timestamps and credits remained held for almost the full 239s
replay. The implementation now decouples arrival production through a
one-element bounded queue and polls Ray completions whenever no arrival is
ready. A regression test requires completion collection before the next
delayed envelope. The formal matrix remains blocked until a remote gate proves
that request E2E follows backend completion rather than replay duration.
