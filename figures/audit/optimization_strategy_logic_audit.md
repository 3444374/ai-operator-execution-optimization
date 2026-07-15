# Optimization Strategy Logic Figure Audit

Figure:

```text
figures/archive/architecture/20260715_strategy_iterations/optimization_strategy_logic.png
figures/archive/architecture/20260715_strategy_iterations/optimization_strategy_logic.svg
```

Script:

```text
figures/scripts/generate_optimization_strategy_logic.py
```

## Role

This is a strategy-design detail figure. It is more specific than the upstream strategy overview figure:

```text
input signals
  -> rule-table selector
  -> strategy actions and executable configuration
  -> end-to-end validation and rule feedback
```

The figure should be used when explaining what the proposed optimization strategy actually does. It should not be used as a result figure.

## Strategy Claim

The figure presents the current conservative strategy:

```text
Ours-v0: rule-table guided upstream execution strategy
```

Supported claim:

- The strategy chooses upstream data organization and model-service scheduling actions after bottlenecks have been diagnosed.
- Workload semantics and model-service state are explicit strategy inputs.
- Writeback is treated as a constraint, baseline, and validation signal.
- End-to-end metrics decide whether a rule is retained or revised.

Unsupported claims:

- It does not claim a finalized learned optimizer.
- It does not claim a new database optimizer, inference engine, Ray Serve scheduler, or storage engine.
- It does not require "independent best vs joint optimal" to be the core opening-report claim.

## Literature Boundary

- Cortex AISQL / Smart: support AI-operator semantic awareness and selectivity/cascade thinking.
- vLLM / Orca / Sarathi-Serve: support throughput-latency, queue wait, batching and service-state metrics.
- Ray Data / Daft / Spark: support batch, partition, object and fan-in controls.
- COPY / pgai / Delta Lake / TurboVecDB: support writeback baseline and write-path boundary handling.

## Visual Check

- PNG and SVG are generated from the same script.
- The script checks every core card border, the validation box, and all seven arrows.
- Script self-check on 2026-07-15 passed: no card boundary failure and no arrow crossing unrelated cards.
- Manual PNG preview on 2026-07-15 found no obvious text overflow, border clipping, or arrow/card overlap.

## Forbidden-Label Check

The visible figure must not contain:

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

Visible labels should remain concrete: "输入信号", "规则表选择器", "策略动作与配置", "端到端验证与回填".
