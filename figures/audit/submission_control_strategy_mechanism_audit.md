# Submission Control Strategy Mechanism Figures Audit

Date: 2026-07-21

## Figure Contract

Core conclusion: submission-control optimization should be explained as three
upstream decisions before requests enter the model service: when to flush, how
many requests may remain in flight, and which actor pool or service entrance
receives each request.

Archetype: schematic-led composite mechanism figures.

Outputs:

```text
figures/architecture/submission_control_queue_adaptive_mechanism.png
figures/architecture/submission_control_queue_adaptive_mechanism.svg
figures/architecture/submission_control_kmax_admission_mechanism.png
figures/architecture/submission_control_kmax_admission_mechanism.svg
figures/architecture/submission_control_pool_routing_mechanism.png
figures/architecture/submission_control_pool_routing_mechanism.svg
figures/scripts/generate_submission_control_mechanisms.py
```

## Design Decisions

- The figure set uses a consistent axis:
  - queue-adaptive flush: when to submit.
  - K_max admission control: how many requests are allowed in flight.
  - actor-pool routing: where a request is submitted.
- The visible titles are mostly Chinese to avoid mixed-font issues in large
  headings. Necessary technical tokens such as `K_max`, `queue wait`, `flush`,
  `actor pool`, `token`, and `prefix` are kept in smaller labels.
- Each diagram contrasts a fixed or single-entry submission path with the
  proposed control point, then lists validation metrics instead of claiming
  proven gains.
- The figures focus on the external upstream execution chain. They do not
  imply modifications to vLLM internal scheduling, Ray's scheduler, or the
  database optimizer.

## Evidence Boundary

| Figure | Mechanism Role | Required Validation |
|---|---|---|
| `submission_control_queue_adaptive_mechanism.*` | Converts fixed flush into queue-state feedback | queue wait, P95/P99 latency, tokens/s, end-to-end time |
| `submission_control_kmax_admission_mechanism.*` | Bounds in-flight requests at the service entrance | K_max sweep, foreground/background interference, throughput/tail-latency tradeoff |
| `submission_control_pool_routing_mechanism.*` | Binds submission parameters to request shape and actor-pool choice | queue balance, request-shape ablation, prefix locality, throughput |

The current figures are mechanism diagrams. They can motivate experiments and
method sections, but they should not be cited as experimental results.

## QA

- Regenerated PNG and SVG from a UTF-8 Python/PIL script.
- Manually previewed the PNG exports for title readability, spacing, text
  clipping, and arrow placement.
- Ran coordinate-bound checks on the three SVG exports: no rectangle, line,
  polygon, or text anchor coordinate falls outside the declared canvas.
- Ran PNG non-background bounding-box checks. All three exports keep visible
  content inside the canvas with at least 21 px top margin, 24 px bottom margin,
  44 px right margin, and 45 px left margin.
- Scanned formal SVGs for forbidden formal-material tokens and old mojibake
  fragments: `RC/BL`, `Phase`, `边界确认`, `Workload 入口`, `效果：`, and common
  encoding-corruption characters.
- Kept captions conservative: every figure states candidate mechanism and
  validation metrics rather than performance conclusions.

## 2026-07-22 Redesign QA

- Reworked the three figures from large red/green comparison blocks into
  restrained white-card mechanism diagrams with subtle gray panels and a single
  blue accent bar for the controlled side.
- Fixed the K_max figure overflow by centering all request-slot glyphs inside
  their parent service card instead of using fixed absolute coordinates.
- Reworked the actor-pool routing arrows so each arrow starts from the request
  classifier boundary and terminates at a specific target pool boundary. This
  avoids floating arrowheads and makes the routing direction explicit.
- After another arrow-direction review, replaced the three diagonal routing
  arrows with a classifier-to-routing-bus connector plus three horizontal
  pool-entry arrows. This makes every arrowhead terminate on a target pool's
  left boundary and avoids ambiguous diagonal routing lines.
- Added the arrow-boundary inspection as a required future figure QA rule:
  direction must match the semantic flow, the line must start from the source
  component boundary, and the arrowhead must land at the target component
  boundary rather than floating in whitespace or penetrating the card.
- Re-ran canvas-bound checks on all three SVGs: `canvas issues = 0`.
- Re-ran PNG non-background bounding-box checks:
  - queue-adaptive: margins L/T/R/B = 48/22/47/29 px.
  - K_max admission: margins L/T/R/B = 48/22/47/28 px.
  - pool routing: margins L/T/R/B = 48/22/47/18 px.

## 2026-07-22 Color Refinement

- Reduced the saturation of the actor-pool routing palette. The target pool
  cards now use neutral gray borders, while method identity is carried by a
  thin left accent strip, colored title text, and one colored configuration
  line.
- Changed routing arrows from per-pool saturated colors to a neutral blue-gray
  route color, so arrows explain connectivity without visually competing with
  the target pool labels.
- Re-generated PNG/SVG and re-ran SVG canvas-bound and forbidden-token checks;
  all three submission-control mechanism SVGs pass.
