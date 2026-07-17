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

- Workload cards reordered: AI_COMPLETE first as primary scenario (purple),
  AI_EMBED second as preliminary validation (blue), AI_FILTER third as extension (orange).
- Each workload card uses three lines: scenario name, SQL operator name with role label, and
  the scheduling pressure introduced by that workload.
- The three strategy layers replaced by two strategy cards + one validation card:
  1. 数据组织策略 (token-budget / length-aligned / prefix-aware / actor pool routing)
  2. 调度与提交控制策略 (queue-adaptive flush / K_max 动态控制 / actor pool 分池路由 / 去中心化)
  3. 端到端验证：耦合与写回 (independent vs joint / writeback check / end-to-end evaluation)
- Removed "服务端：批处理形成" card (was describing vLLM internals, not our research).
- Removed "三层" framing from title, subtitle, section header, and caption.
- Removed "COPY + deferred index" specific method name → generic "工程最优写回方案 baseline".
- Replaced "micro-batch" with "coupling gap" in evaluation metrics.
- Updated caption to reflect current framing: two upstream strategies + end-to-end validation.

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
