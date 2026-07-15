# Research Plan Figure Audit

Figure files:

- `figures/architecture/cross_layer_method_framework.png`
- `figures/architecture/cross_layer_method_framework.svg`

Script:

- `figures/scripts/generate_cross_layer_method_framework.py`

## Figure Type

Research plan / methodology figure.

This figure explains the Section 4.1 research plan in the opening report:
start from three database AI workloads, build stage-level performance profiles,
tune the upstream execution path through a three-layer strategy, then include
writeback in the end-to-end evaluation to check whether persistence swallows
upstream gains.

## Design Decisions

- Workload entry is shown as three equal-width cards:
  `AI_EMBED`, `AI_FILTER / AI_CLASSIFY`, and `AI_COMPLETE`.
- Each workload card uses three lines: scenario name, SQL operator name, and
  the scheduling pressure introduced by that workload.
- The middle relation is an explicit `三层上游执行策略` card: plan-time data
  organization, runtime admission/routing, and service-side dynamic
  `micro-batch`.
- 2026-07-15 later revision: the three strategy layers are now drawn as three
  separate neutral cards, not as two side cards plus a middle summary box. This
  avoids implying that layer colors correspond to the workload colors above.
- Dynamic batching is described as service-side `micro-batch`, not as recutting
  materialized database-side batches.
- The result-writeback section uses concrete labels: `瓶颈判定`, `sink 对比`,
  `COPY + deferred index`, and `防止写回吞噬上游调度收益`.
- Visible abbreviations such as `RC` / `BL`, the vague phrase `边界确认`, and an
  unexplained `vs` comparison were removed from the figure.

## Checks

- PNG preview: passed. No visible clipping in workload cards, method cards,
  strategy card, writeback card, metric bar, or caption.
- SVG text scan: passed. No visible `RC1`, `BL3`, `边界确认`, `Workload 入口`,
  or unexplained `vs` label remains in the SVG.
- Programmatic layout checks: passed.
  - Workload card 1 border and bounds: pass.
  - Workload card 2 border and bounds: pass.
  - Workload card 3 border and bounds: pass.
  - Data organization card border and bounds: pass.
  - GPU scheduling card border and bounds: pass.
  - Writeback card border: pass.
  - Three-layer strategy card border: pass.
  - Metric bar: pass.
- Color use: pass. Blue, orange, purple, yellow, and green encode different
  workload/result roles, with text labels as redundant encoding. Strategy-layer
  cards use neutral borders to avoid false color correspondence.
- Chartjunk: pass. No 3D effects, shadows, gradients, or decorative background.

## Remaining Notes

- This is a research-plan figure, not an experimental result chart.
- The report cites this figure where it introduces workload coverage,
  stage-level tuning variables, three-layer upstream execution strategy,
  end-to-end evaluation, and writeback bottleneck test.
