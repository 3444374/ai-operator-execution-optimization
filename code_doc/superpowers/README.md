# code_doc/superpowers/

Local planning artifacts produced through the Superpowers workflow for code changes.

## Files

| Path | Purpose |
|---|---|
| `specs/` | Approved behavioral designs written before implementation planning |
| `plans/` | Detailed TDD implementation plans written after design review |

Current designs:

- `specs/2026-08-13-saor-native-system-matched-comparison-design.md`:
  approved five-arm two-Job complete-system comparison, with scheduler-owner
  isolation, common PostgreSQL timing, metric availability and fail-closed
  evidence rules.
- `specs/2026-07-28-dual-gpu-experiment-correctness-design.md`: dual-GPU
  experiment correctness and shared scheduling.
- `specs/2026-07-29-saturated-ray-actor-pool-replenishment-design.md`: saturated
  active-work calibration, service quanta, bounded Ray actor pools, and
  endpoint-local completion replenishment.
- `specs/2026-07-29-daft-ray-baseline-advantage-validation-design.md`:
  pre-registered direct/official/project baseline ladder, cold-path
  measurement gate, pressure efficiency, transient scaling, and multi-job
  promotion thresholds.

Current implementation plans:

- `plans/2026-08-13-saor-native-system-matched-comparison-implementation.md`:
  TDD plan for timed PostgreSQL native source, eager Project arrival, eight-arm
  balanced orchestration, and separate system/internal fail-closed summaries.
- `plans/2026-07-28-dual-gpu-experiment-correctness-implementation.md`:
  completed dual-GPU correctness foundation.
- `plans/2026-07-29-saturated-ray-execution-foundation-implementation.md`:
  runner exclusivity, failure-safe Ray fan-in, saturation calibration, fixed
  service quanta, and a bounded observable actor pool.
- `plans/2026-07-29-daft-ray-baseline-advantage-validation-implementation.md`:
  actor readiness, HTTP transport timing, same-pressure equivalence gate,
  staged baseline handoff, verification, and remote synchronization.
