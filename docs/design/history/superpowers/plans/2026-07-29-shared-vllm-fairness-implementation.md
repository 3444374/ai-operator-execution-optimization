# Shared-vLLM 1/2/4-Job Fairness Implementation Plan

> Scope: implement and gate the preregistered experiment in
> `experiments/plans/service_scheduling_backpressure.md` §13. Do not tune the
> completed SLO-EWMA controller and do not start a formal matrix before the
> real dual-GPU gate passes.

## Goal

Provide one reproducible group runner that launches 1/2/4 concurrent database
AI jobs against the same two vLLM endpoints and the same explicit Ray cluster,
then proves exactly-once execution, endpoint-global request/work safety,
work-conserving borrowing, and per-job fairness without double-counting global
service metrics.

## Task 1: Make shared-credit state auditable

Files:

- Modify `code/src/scheduling/submission_control/shared_credit.py`
- Modify `code/src/scheduling/runtime/shared_credit_ray.py`
- Modify `code/tests/scheduling/test_shared_credit.py`
- Modify `code/tests/scheduling/test_shared_credit_ray.py`

Steps:

1. Add failing tests for exact peak request/work tracking, waiting/active work
   by job, cumulative granted request/work by job, and client `snapshot()`.
2. Extend the immutable snapshot schema without changing admission decisions.
3. Update peaks and cumulative grants only when a new lease is granted;
   idempotent polling must not double-count.
4. Expose snapshot through `RaySharedCreditClient`.
5. Run the two shared-credit test modules.

## Task 2: Add a synchronized replay start boundary

Files:

- Modify `code/src/profiling/cli.py`
- Modify `code/scripts/profiling/postgres_ai_operator_profile.py`
- Modify `code/tests/observability/test_postgres_profile_scheduling.py`

Steps:

1. Add failing tests for a future `--arrival-replay-start-epoch-s` value and
   rejection when arrival replay is disabled.
2. Wait only after source preload and immediately before replay scheduling.
3. Record configured and observed replay start epochs in the summary CSV.
4. Keep the default unset so existing experiments are unchanged.
5. Run profiler scheduling tests.

## Task 3: Build the formal group-runner core

Files:

- Add `code/src/shared_vllm_experiment.py`
- Add `code/scripts/experiments/run_shared_vllm_experiment.py`
- Add `code/tests/experiments/test_shared_vllm_experiment.py`

Steps:

1. Test and implement strict schema parsing for group scenarios, policies,
   job counts, weights, offsets, total endpoint limits, service metadata, and
   environment expansion.
2. Require one explicit Ray address. Reject runner-owned profiler flags,
   `--setup`, reset, output paths, shared-credit flags, and implicit Ray.
3. Reuse `RunnerLease`; write an atomic manifest with deterministic
   warmup/formal schedule, incidents, commands with secrets redacted, and
   verified completed groups.
4. Give each profiler a separate output and trace stem. Never concurrently
   append to one CSV.
5. Build the three preregistered policies:
   `independent_full`, `static_partition`, and `shared_drr`.
6. Launch all jobs toward one future replay epoch. On any child failure,
   terminate remaining children, preserve stdout/stderr, and mark the group
   failed.
7. Verify exactly one successful profiler row per job and expected
   request/submission counts before marking the group complete.
8. Run the new unit tests.

## Task 4: Add group-level observation and fairness summaries

Files:

- Modify `code/src/shared_vllm_experiment.py`
- Modify `code/tests/experiments/test_shared_vllm_experiment.py`

Steps:

1. Add tests proving overlapping per-job vLLM deltas are never summed.
2. Scrape endpoint metrics once before and after the whole group and sample
   service/resource state at group level.
3. Poll the unique coordinator actor for shared-credit scenarios and write a
   versioned global credit trace.
4. Persist the actor's final exact snapshot even if a child fails.
5. Compute per-job JCT/P99/completion lag, endpoint counts, slowdown inputs,
   normalized service, Jain fairness, and group token throughput into
   `group_runs.csv`.
6. Validate final active/waiting zero before cleaning up only the unique actor
   owned by that group.

## Task 5: Add AutoDL gate/formal templates and runbook

Files:

- Add `deploy/autodl/dual_gpu_shared_vllm_gate.example.json`
- Add `deploy/autodl/dual_gpu_shared_vllm_formal.example.json`
- Modify `deploy/autodl/README.md`
- Modify `code/scripts/README.md`
- Modify `code/README.md`
- Modify `code/INFRA_STATUS.md`
- Modify `experiments/plans/experiment_status_and_gaps.md`
- Modify `PROJECT_LOG.md`

Steps:

1. Gate template: two jobs, 64 rows/job, all three policies once.
2. Formal template: 1/2/4 jobs × three policies, one warmup and three formal
   repeats. Keep staggered and weighted scenarios disabled until the core
   matrix passes.
3. Document boot-time Ray head startup, one explicit address for every child,
   preflight no-runner/lease/endpoint/git checks, setup-once rule, new output
   directory rule, gate checks, actor/Ray cleanup, and failure evidence.
4. Keep service credentials in the runtime environment, never in committed
   templates or manifests.

## Task 6: Verify, publish, and run the remote gate

1. Run targeted tests after each task, then the full `code/tests` suite.
2. Run `compileall`, parse every AutoDL JSON, and run `git diff --check`.
3. Review the diff for accidental result deletion, secrets, AI attribution,
   and unrelated changes.
4. Commit without AI attribution and push `main`.
5. On AutoDL, perform read-only process/lease/endpoint/git checks first.
6. Synchronize only an idle checkout while preserving untracked results.
7. Start one clean Ray head and export its explicit address.
8. Run the gate in a brand-new result directory. If it is still running,
   monitor it; never launch a duplicate.
9. Start the formal matrix only if every §13.4 gate condition passes.

Remote progress on 2026-07-29:

- AutoDL dependency-complete suite passed 433/433 tests; both templates and
  `compileall` passed.
- The first fresh gate directory failed before workload execution because a
  relative profiler path was interpreted from the runner's `code/` child cwd.
  All failure evidence remains in
  `experiments/results/dual_gpu_shared_vllm_gate_20260729_1047/`.
- A failing CLI regression test was added first. The CLI now resolves every
  filesystem argument before child cwd changes; 142 related tests pass.
- Publish this fix, fast-forward the idle remote checkout, rerun the full
  remote suite, and use a second brand-new gate directory. Do not resume or
  reuse the failed directory.
- Commit `96a24a8` passed 434/434 remote tests and reached real dual-GPU
  execution in the second fresh directory. Both child processes exited zero
  and all 128 request rows were `completed` with empty `error_type`, but the
  group validator incorrectly expected the runs-summary value `ok`.
- A second failing contract test now distinguishes the two schemas. Request
  evidence accepts only `completed` plus an empty error type; 143 related
  tests pass. Publish this fix and use a third fresh gate directory.
- Commit `983e6e1` passed 435/435 remote tests. The third gate completed the
  independent arm, then stopped because shared DRR's first submit was 3.90s
  late even though both jobs crossed the barrier within 0.2ms and their first
  submits were only 11.8ms apart.
- The shared-credit actor was lazily created after the replay barrier. Keep
  the 2s hard gate: pre-create and validate the actor, then compute the future
  epoch and launch children. The new prewarm contract and 144 related tests
  pass. Publish and use a fourth fresh gate directory.
- Commit `e45fe1c` passed 436/436 remote tests. The fourth gate stopped before
  the shared arm because the group runner's Ray connection did not export
  `code/` on worker `PYTHONPATH`; the prewarmed actor could not import `src`.
- Keep prewarming before the barrier and give the observer the same explicit
  runtime environment as the profiler. The new Ray-init contract and 145
  related tests pass. Publish and use a fifth fresh gate directory.
- Commit `e322183` passed 437/437 remote tests. The fifth gate reached 3/3
  completed and zero incidents; exactly-once, replay timing, endpoint
  coverage, final-zero credit, actor cleanup, and failure gates all passed.
- Independent trace audit found invalid aggregate quantiles: raw execution
  windows reached 100% GPU with 71–74% means, while recorded GPU P95 was zero.
  The shared percentile helper expects 0–100, but this runner passed
  `0.95/0.99` for GPU P95 and job P99. Fix both call sites; the two failing
  contracts and 146 related tests now pass. Use a sixth fresh gate because the
  fifth directory is functionally valid but statistically invalid.

## Stop conditions

- Do not repair a failed gate by deleting its result directory or lease.
- Do not weaken exactly-once, capacity, failure, or final-zero checks.
- Do not add FIFO or another scheduling policy unless the three-arm ablation
  shows that a separate fairness mechanism is needed.
- Do not claim a performance improvement from the functional gate.
