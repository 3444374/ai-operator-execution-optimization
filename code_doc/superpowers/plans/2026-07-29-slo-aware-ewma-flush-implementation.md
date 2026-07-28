# SLO-aware EWMA flush implementation plan

**Date:** 2026-07-29

## Goal

Replace the current two-level `queue_adaptive` baseline with one small,
auditable candidate that has an actual decision opportunity under burst/gap
load. The controller must use oldest-request slack, token fill time and
arrival/service EWMA, retain a fixed-50ms fallback and hard deadline, and keep
request-level completion as the credit boundary.

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
5. a busy service selects EWMA-estimated fill time, bounded by min/max;
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
service busy             -> clamp(predicted fill time, min, max)
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
- near-capacity burst/gap replay (`arrival_time_scale=0.002`).

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
