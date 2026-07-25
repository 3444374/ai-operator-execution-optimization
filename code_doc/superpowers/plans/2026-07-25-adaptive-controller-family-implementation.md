# Adaptive Controller Family Implementation Plan

> **Execution rule:** implement task-by-task with RED/GREEN tests. Formal
> execution remains PostgreSQL -> Daft -> Arrow -> Ray task/actor -> endpoint.

**Goal:** Add maintainable, engine-independent AIMD, EWMA-AIMD, PID, and UCB
admission controllers, then connect them to the typed Ray scheduler without
changing static behavior.

**Architecture:** Controllers consume immutable observations and emit typed
window decisions. They perform no network calls, sleeps, Ray calls, or file
I/O. A later Ray-facing observation provider supplies cached vLLM metrics and
records every decision. Static admission remains the control baseline.

**Experiment-plan sources:** This implementation is subordinate to
`experiments/plans/experiment_status_and_gaps.md` P0-1,
`service_scheduling_backpressure.md` §0.5/§5.3,
`strategy_design_implementation_reference.md` §8.2, and
`baseline_reference.md`. In particular, unsmoothed CONCUR-style AIMD is tried
before EWMA/PID/UCB; EWMA remains optional because the related Ray policy was
deprecated; fixed K=8, the legacy two-level policy, and Clipper-style AIMD are
retained as baselines.

## Constraints

- Keep each policy module focused and near or below 200 lines.
- Missing or stale observations hold the current window.
- Every window is clamped to explicit minimum and maximum bounds.
- Static scheduling never reads adaptive metrics.
- The first UCB implementation uses a finite action set and deterministic
  tie-breaking.
- No GPU performance claim follows from controller unit tests.
- Before the formal adaptive comparison, run the fixed-cap versus
  EOS-permissive output-length confounder check and record actual completion
  tokens. Controller coding may proceed in parallel, but this experiment gate
  cannot be skipped.
- Formal adaptive runs must record tokens/s, service P99, and inflight,
  vLLM-queue, and K_max time series.
- Apply the existing stop rule after three evidence-bearing controller
  iterations: if foreground E2E remains above 8 seconds or background
  throughput falls below 90% of static K=8, downgrade the adaptive claim.

## Task 1: Typed observations and decisions

- Add `AdmissionObservation` with monotonic timestamp, freshness, in-flight,
  running, waiting, and KV-cache signals.
- Add `WindowDecision` with window, action, reason, and typed diagnostics.
- Validate non-negative counts, KV range, and non-empty action/reason.
- Add schema tests before implementation.

## Task 2: AIMD and EWMA-AIMD

- Add bounded AIMD with `+2` increase and `x0.5` decrease.
- Congestion: waiting > 0 or KV >= 0.85.
- Low load: waiting == 0, KV <= 0.50, and running < 64.
- Deadband, incomplete metrics, and stale samples hold.
- EWMA alpha defaults to 0.3 and smooths each signal exactly once per fresh
  observation before applying the same AIMD law.
- Test increase, decrease, bounds, deadband, missing metrics, staleness, and
  deterministic smoothing.

## Task 3: PID

- Control waiting-depth error around target 1.
- Defaults: Kp=0.5, Ki=0.1, Kd=0.05, bounds [2, 16].
- Clamp the integral term and output window.
- Ignore stale/missing queue samples.
- Test direction, bounds, anti-windup, and deterministic timestamp handling.

## Task 4: UCB

- Arms default to {4, 8, 16}; visit every arm before exploitation.
- Use UCB1 with deterministic smallest-window tie-breaking.
- Update only the selected arm and reject unknown arms/non-finite rewards.
- Keep reward calculation separate and typed so experiment traces retain
  throughput, P99, and SLO inputs.
- Test initial exploration, exploitation, update isolation, and reward penalty.

## Task 5: Ray scheduler integration

- Add a controller protocol and a dynamic admission gate that exposes the
  current limit to the scheduler.
- Add a cached observation provider outside policy modules.
- Record one trace event per fresh controller update and per admission wait.
- Remove blocking sleep from the new adaptive path.
- Preserve the legacy two-level branch as an explicit baseline until parity
  tests and GPU experiments complete.

## Task 6: Verification and documentation

- Run focused controller tests.
- Run full `code/tests` suite in `.conda/pg-ai-profile`.
- Run real Daft -> Arrow -> Ray task/actor contract tests.
- Run compile/import and policy dependency scans.
- Update code documentation, learning walkthrough, project index, and log.
- Do not merge to `main`; continue to flush, routing, metrics, search, and GPU
  experiment stages.
