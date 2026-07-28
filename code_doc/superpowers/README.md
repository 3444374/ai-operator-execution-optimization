# code_doc/superpowers/

Local planning artifacts produced through the Superpowers workflow for code changes.

## Files

| Path | Purpose |
|---|---|
| `specs/` | Approved behavioral designs written before implementation planning |
| `plans/` | Detailed TDD implementation plans written after design review |

Current designs:

- `specs/2026-07-28-dual-gpu-experiment-correctness-design.md`: dual-GPU
  experiment correctness and shared scheduling.
- `specs/2026-07-29-saturated-ray-actor-pool-replenishment-design.md`: saturated
  active-work calibration, service quanta, bounded Ray actor pools, and
  endpoint-local completion replenishment.

Current implementation plans:

- `plans/2026-07-28-dual-gpu-experiment-correctness-implementation.md`:
  completed dual-GPU correctness foundation.
- `plans/2026-07-29-saturated-ray-execution-foundation-implementation.md`:
  runner exclusivity, failure-safe Ray fan-in, saturation calibration, fixed
  service quanta, and a bounded observable actor pool.
