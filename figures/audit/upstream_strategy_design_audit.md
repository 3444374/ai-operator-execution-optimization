# Upstream Strategy Design Figure Audit

Figure:

```text
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.png
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.svg
```

Script:

```text
figures/scripts/generate_upstream_strategy_design.py
```

## Role

This is a methodology/detail figure. It answers what the strategy design does after staged profiling has already located the bottleneck.

It does not claim a finalized learned optimizer, a cost model, or a full end-to-end scheduler. The visible design is conservative:

```text
identified bottlenecks
  -> data-organization actions + model-service scheduling actions
  -> execution configuration
  -> end-to-end validation including writeback ratio
  -> update the next parameter sweep / rule table
```

## Content Check

- The left panel is explicitly named "前置诊断结论"; it does not present stage profiling as the strategy itself.
- The middle panel is the strategy-design core: data organization, model-service scheduling, and writeback constraint handling.
- Model-service scheduling is marked as the main focus, matching the current research emphasis.
- Writeback is treated as a constraint and validation signal rather than the main optimization target.
- Workload-specific pressure is concrete: FILTER selectivity, COMPLETE token length, and EMBED output/writeback sensitivity.

## Literature-Informed Checks

- Selectivity/prefix/output-size reordering is consistent with AI-aware query and semantic optimization ideas from Cortex AISQL / Smart-style references.
- Batch, partition, object count and fan-in controls follow distributed data-processing patterns from Ray Data / Daft / Spark-style references.
- Actor pool, bounded in-flight, queue/backlog routing, token-aware and prefix-aware dispatch follow inference-service scheduling patterns from vLLM / Orca / Ray Serve-style references.
- COPY + deferred index, pgai queue-worker, TurboVecDB and Delta-style write-path references are used as writeback baselines or constraints, not overclaimed as this figure's main contribution.

## Visual Check

- PNG and SVG are generated from the same script.
- The script checks visible borders for all core cards and verifies that arrows stay inside the canvas without crossing unrelated card rectangles.
- Manual preview on 2026-07-15 confirmed no obvious text overflow, card-border clipping, or arrow/card overlap in the PNG.

## Forbidden-Label Check

The formal figure should not contain:

```text
RC1
RC2
RC3
BL1
BL2
Workload 入口
联合决策面
边界确认
unexplained "vs"
```

Use concrete labels such as "数据组织瓶颈", "模型服务瓶颈", "针对瓶颈的优化设计", "模型服务调度动作", and "端到端验证".
