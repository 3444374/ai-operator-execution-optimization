# Daft+Ray Baseline Advantage Validation Implementation Plan

> **For agentic workers:** execute tasks in order. Every production behavior
> starts with a failing test and a witnessed RED run.

**Goal:** remove the full-concurrency cold-path measurement confound, publish a
reproducible staged baseline matrix, and make the first remote equivalence gate
directly executable by another agent.

**Architecture:** keep the profiler as the project runtime. Add an explicit
actor-ready contract before the measured timer, expose local HTTP transport
intervals in result and submission traces, and use scenario-runner warm-ups at
the exact formal pressure. Keep direct/official baselines in the independent
`src.baselines` adapters.

**Constraints:**

- Do not modify vLLM, Ray's scheduler, Daft internals, or request semantics.
- Do not enable streaming merely to obtain TTFT.
- Do not touch opening/PPT files or sync Wiki.
- Use fresh remote output directories and preserve every failed gate.
- Commit messages contain no AI attribution.

## Task 1: Actor-ready barrier

**Files:**

- Modify `code/src/model_backends.py`
- Modify `code/src/scheduling/runtime/ray_adapter.py`
- Modify `code/scripts/postgres_ai_operator_profile.py`
- Modify `code/src/profiling/schema.py`
- Test `code/tests/test_model_backends.py`
- Test `code/tests/test_ray_adapter.py`
- Test `code/tests/test_postgres_profile_scheduling.py`

- [ ] Write tests proving every actor has a side-effect-free `ready()` result.
- [ ] Write a test proving `ActorSubmissionState.wait_until_ready()` resolves
  every actor through Ray before returning.
- [ ] Write a profiler test proving the barrier occurs before the E2E timer and
  that `actor_ready_s` is present in the formal row.
- [ ] Run tests and witness the expected RED failures.
- [ ] Implement the minimal ready method and state-level barrier.
- [ ] Call the barrier after actor creation and before `StageTimer.start("e2e")`.
- [ ] Record barrier duration separately; use zero for non-actor executors.
- [ ] Run focused tests to GREEN.

## Task 2: HTTP transport timing

**Files:**

- Modify `code/src/model_backends.py`
- Modify `code/src/profiling/traces.py`
- Modify `code/src/profiling/schema.py`
- Modify `code/scripts/postgres_ai_operator_profile.py`
- Test `code/tests/test_model_backends.py`
- Test `code/tests/test_profiling_modules.py`
- Test `code/tests/test_postgres_profile_scheduling.py`

- [ ] Write a deterministic-clock test for request start, response headers,
  body complete, headers-wait, and body-read intervals.
- [ ] Write a submission-trace schema test for the new HTTP fields.
- [ ] Write a summary-schema test for HTTP headers-wait/body-read P50/P95/P99.
- [ ] Witness RED.
- [ ] Extend `CompletionEndpointResult` and completion actor results without
  changing the public request body.
- [ ] Add trace schema version 5 fields.
- [ ] Aggregate only observed HTTP results; use explicit zero/empty sentinels
  for non-HTTP paths.
- [ ] Run focused tests to GREEN.

## Task 3: Same-pressure equivalence template

**Files:**

- Create
  `deploy/autodl/dual_gpu_same_condition_project_equivalence_gate.example.json`
- Modify
  `deploy/autodl/dual_gpu_same_condition_project_calibration.example.json`
- Modify `deploy/autodl/README.md`
- Modify `code/tests/test_postgres_profile_scheduling.py`
- Modify `code/tests/test_experiment_scenarios.py`

- [ ] Write template-contract tests requiring one same-pressure warm-up, three
  formal repeats, explicit Ray address, Chat/no-replay/request semantics, and
  exactly the K256/nonbinding-W98 pair.
- [ ] Witness RED because the new template does not exist.
- [ ] Add the gate template and set the broad calibration template to one
  warm-up and three formal repeats.
- [ ] Document the exact remote command, output directory naming, pass/fail
  rule, and prohibition on starting calibration after a failed gate.
- [ ] Run focused tests to GREEN.

## Task 4: Analysis and handoff contract

**Files:**

- Modify `experiments/plans/database_ai_operator_baseline_matrix_20260729.md`
- Modify `experiments/plans/experiment_status_and_gaps.md`
- Modify `code/INFRA_STATUS.md`
- Modify `code/scripts/README.md`
- Modify `PROJECT_OUTLINE.md`
- Modify `PROJECT_INDEX.md`
- Modify `PROJECT_LOG.md`
- Modify `code_doc/superpowers/README.md`

- [ ] Record the invalid single-repeat calibration and its preserved output.
- [ ] Record the read-only diagnosis and current leading hypothesis.
- [ ] Add the staged single-job/transient/scale/multi-job matrix and approved
  thresholds.
- [ ] Add a concise new-agent handoff that starts at the equivalence gate.
- [ ] State that all primary arms use the same two one-GPU endpoints.
- [ ] State that OceanBase-style emulation is secondary and not official.

## Task 5: Verification, publication, and safe remote sync

- [ ] Run all focused baseline/profiler/scheduling tests.
- [ ] Run the full unit-test suite.
- [ ] Run Ruff, compileall, JSON parsing, and `git diff --check`.
- [ ] Inspect the complete diff and staged file list.
- [ ] Commit without AI attribution and push
  `codex/baseline-comparison`.
- [ ] On the remote server, perform read-only runner/lease/endpoint/Ray/GPU/git
  checks.
- [ ] If idle and clean for tracked files, fetch and fast-forward or create a
  dedicated worktree at the pushed commit without deleting untracked results.
- [ ] Do not launch formal runs. Provide the exact Stage A command to the next
  agent.
