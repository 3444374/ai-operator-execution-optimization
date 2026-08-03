# code_doc/superpowers/plans/

Design specifications and implementation plans for focused engineering tasks.

These files are process artifacts. They do not replace `PROJECT_OUTLINE.md`,
`experiments/plans/`, or formal experiment records.

## Files

| File | Purpose |
|---|---|
| `2026-07-17-daft-postgres-entry-existing-writeback.md` | Daft PostgreSQL entry and existing writeback implementation plan |
| `2026-07-25-adaptive-admission-controller-design.md` | Approved RC2 adaptive admission controller and experiment design |
| `2026-07-25-runtime-scheduling-strategy-suite-design.md` | Full flush/admission/routing/topology/search/metrics strategy-suite design |
| `2026-07-25-scheduling-foundation-implementation.md` | TDD plan for typed scheduling schemas, topology, static admission/routing, and deterministic scheduler |
| `2026-07-25-ray-static-wiring-implementation.md` | TDD plan for production static Ray task/actor delegation through the typed scheduling core |
| `2026-07-25-arrival-replay-flush-runtime-implementation.md` | TDD plan for monotonic arrival replay, pending batch construction, and real flush-policy runtime wiring |
| `2026-07-29-shared-vllm-fairness-implementation.md` | TDD plan for the 1/2/4-job shared-credit group runner, global observation, fairness summaries, and AutoDL gate |
| `2026-07-29-same-condition-official-baselines-design.md` | Two-layer baseline design: no-Daft/no-Ray OceanBase and same-PostgreSQL controls, plus official Daft prompt/Ray Data framework controls under one Chat protocol |
| `2026-07-29-official-baseline-matrix-implementation.md` | TDD implementation plan for the Chat request contract, direct/OceanBase/Daft/Ray Data adapters, unified gate and AutoDL fatal-flaw run |
| `2026-07-29-same-condition-project-runtime-comparison-implementation.md` | TDD plan for manifest-locked no-replay request execution, pinned routing, 512-row calibration, and 2,048-row same-condition formal comparison |
