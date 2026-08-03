# Single-GPU Text Research Closure Implementation Plan

**Goal:** Close the remaining single-GPU AI_COMPLETE evidence gaps before
starting multimodal validation: cross-arrival-rate flush behavior, a 2048-row
held-out run, controlled prefix reuse, and an operator cost estimator.

**Architecture:** Keep PostgreSQL/Daft ingestion, Arrow organization, Ray
execution, and vLLM serving unchanged. Add only workload construction and
offline analysis at existing boundaries. Runtime policies remain deterministic
and engine-independent; experiments decide whether a policy is promoted.

**Success criteria:** Every performance row comes from the real local vLLM
path, has exact request lifecycle/output-token evidence, zero manifest
incidents, positive resource/MFU evidence, and an explicit statement of what
the data cannot establish.

---

## Task 1: Cross-arrival-rate flush boundary

**Files:**
- Create: `experiments/results/adaptive_flush_cross_rate_20260726/scenario_config.json`
- Create: `experiments/results/adaptive_flush_cross_rate_20260726/README.md`
- Modify: `experiments/results/README.md`

1. Derive low/current/high replay scales from the filtered 512-row arrival
   span; record the derivation rather than naming them production QPS.
2. Compare fixed 25ms, fixed 50ms, and queue-adaptive 25/50ms with the same
   natural-EOS ChatML workload and static `K_max=8`.
3. Use one screen repeat at the two new rates. Repeat only a rate at which the
   best static window changes or adaptive is meaningfully different.
4. Audit exact request count, finish reasons, SLO, MFU, energy, submission
   identity, and manifest incidents before interpreting throughput.

## Task 2: 2048-row held-out validation

**Files:**
- Create: `experiments/results/text_heldout_2048_20260726/scenario_config.json`
- Create: `experiments/results/text_heldout_2048_20260726/README.md`

1. Run the simplest policy supported by Task 1 against queue-adaptive at 2048
   rows, preserving token budget, `K_max`, prompt filter, model, and output
   semantics.
2. Use a screen repeat first; add repeats only when the result would change
   policy selection.
3. Report scale-up behavior separately from statistical superiority.

## Task 3: Controlled prefix workload

**Files:**
- Modify: `code/scripts/data/import_ai_complete_workload.py`
- Modify: `code/tests/data/test_import_ai_complete_workload.py`

1. Add failing tests for deterministic prefix-ratio construction, unique-row
   identity, unchanged suffix content, and invalid ratios.
2. Implement a pure constructor that replaces only a configured share of
   prompt prefixes with one common instruction while preserving row metadata.
3. Add importer flags for controlled prefix ratios and workload naming. Do not
   silently mutate the existing ShareGPT/BurstGPT workload.
4. Run focused tests and import dry-run validation.

Implementation note: controlled-prefix construction remains beside
`WorkloadRow` and database materialization in the importer rather than adding
a second, incompatible row type to `src/workloads.py`.

## Task 4: Prefix-aware batching experiment

**Files:**
- Create: `experiments/results/prefix_aware_batching_20260726/scenario_config.json`
- Create: `experiments/results/prefix_aware_batching_20260726/README.md`

1. Materialize 0%, 30%, 70%, and 100% controlled-prefix workloads using the
   same selected rows and arrival metadata.
2. Compare sequential token-budget and prefix-aware token-budget organization
   under identical fixed submission control.
3. Keep vLLM prefix caching disabled for the organization-only comparison.
   Treat a later cache-enabled run as a separate mechanism experiment.
4. Audit token spread, prefix-group ratio, batch/submission counts, E2E,
   tokens/s, P99, energy, and MFU.

## Task 5: Operator cost estimation

**Files:**
- Create: `code/src/cost_estimation.py`
- Create: `code/scripts/analysis/estimate_operator_cost.py`
- Create: `code/tests/planning/test_cost_estimation.py`
- Create: `code/tests/planning/test_estimate_operator_cost.py`
- Create: `experiments/results/operator_cost_estimation_20260726/README.md`

1. Add failing tests for deterministic train/test splitting, feature schema,
   leakage prevention, constant-baseline behavior, MAPE edge cases, and JSON
   output.
2. Implement a small dependency-light estimator over existing profile data.
   Candidate features are known before execution: row count, prompt-token
   summaries, estimated output cost, batch count, timeout, and admission
   window. Actual output tokens and measured service time are targets only.
3. Compare a mean baseline with a regularized linear model on held-out
   scenarios. Report MAE, MAPE, RMSE, and R²; do not claim scheduling gains.
4. Persist feature schema, split identity, coefficients, metrics, and source
   result paths for reproducibility.

## Task 6: Verification and project closeout

**Files:**
- Modify: `code/INFRA_STATUS.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`
- Modify: `code_doc/README.md`

1. Update the completion matrix using only audited results.
2. Run focused tests, all `code/tests`, CLI dry runs, result-manifest audits,
   and `git diff --check`.
3. Review changes for duplicated policy logic, backend leakage, unstable
   schemas, and conclusions stronger than the data.
4. Commit the coherent closure increment on
   `feat/runtime-scheduling-foundation`. Do not merge to `main` or push without
   the user's explicit authorization.
