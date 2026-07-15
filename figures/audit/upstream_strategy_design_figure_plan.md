# Upstream Strategy Design Figure Plan

Purpose: define the strategy-design figure after bottlenecks have been located, without claiming a finalized learned optimizer or a full end-to-end scheduler.

## Figure Contract

Core conclusion: staged profiling is a prerequisite diagnosis step; the strategy design starts from identified bottlenecks, then selects upstream data-organization and model-service scheduling actions, and finally verifies whether the gains survive the full path including writeback.

Figure archetype: schematic-led methodology figure.

Target output: opening report / PPT / thesis method section; export as editable SVG plus PNG preview.

Evidence hierarchy:
- Hero evidence: identified bottleneck categories.
- Method evidence: data-organization actions and model-service scheduling actions.
- Constraint evidence: writeback as a boundary condition and evaluation signal.
- Validation evidence: end-to-end metrics that decide whether the strategy is retained or adjusted.

Reviewer risk: the figure must not imply that a final cost model, online learner, or complete end-to-end optimizer already exists. The current opening-report claim should remain conservative: rule-table plus parameter sweep first; adaptive control or learned selection can be later work if experiments support it.

## Literature-Informed Design Logic

The figure follows these design patterns from the project literature notes:

- Cortex AISQL / Smart style ideas: use workload semantics such as selectivity, token length, prefix similarity and output size to decide whether to reorder, cascade, group, or route operations.
- Ray Data / Daft / Spark style ideas: expose batch size, partition count, object count and fan-in shape as upstream execution controls.
- vLLM / Orca / Ray Serve style ideas: treat actor pool, bounded in-flight, queue wait, backlog, token-aware batching and prefix-aware dispatch as model-service scheduling controls.
- COPY + deferred index / pgai / TurboVecDB / Delta-style write path references: writeback is not the core strategy in this figure, but it must be measured because it can swallow upstream gains.

## Figure Role

This is a methodology/detail figure, not a result chart. It should answer:

```text
When profiling has already shown where the bottleneck is,
what optimization action is selected,
what execution configuration is produced,
and how the full-path metrics decide whether the action works.
```

It should not replace `figures/architecture/cross_layer_method_framework.*`. The research-plan figure answers "what is the overall route"; this figure answers "what does the strategy design look like after bottleneck diagnosis".

## Current Layout

Use a left-to-right three-panel layout:

1. **前置诊断结论**
   - 数据组织瓶颈: batch / partition 不匹配, operator 调用粒度过细, object / fan-in 过多
   - 模型服务瓶颈: queue wait / backlog 过高, GPU 利用率不足, endpoint routing 不均衡
   - workload 特异压力: FILTER selectivity 未知, COMPLETE token 长尾, EMBED 输出写回敏感

2. **针对瓶颈的优化设计**
   - 数据组织优化动作: 调 batch / partition, 合并 operator invocation, 控制 object 和 fan-in, 按 selectivity / prefix / 输出大小重排
   - 模型服务调度动作: 调 actor pool 与 bounded in-flight, 按 queue wait / backlog 路由, token-aware / prefix-aware dispatch, 避免把等待推到模型服务队列
   - 写回约束处理: COPY + deferred index 作为工程 baseline, sink mode / write batch rows 对比, writeback ratio 高时切换 worker-direct, 判断上游收益是否被持久化吞噬

3. **配置与验证**
   - 优化执行配置: batch / partition, actor pool / in-flight, routing / dispatch, fan-in / sink mode
   - 端到端验证: latency / rows per second, tokens per second / queue wait, model wall / writeback ratio, GPU utilization

## Labeling Rules

- Do not use `RC`, `BL`, "边界确认", "联合决策面", or unexplained `vs`.
- Do not use vague labels such as "Workload 入口".
- Do not call the middle strategy block "端到端流程调优"; the middle block focuses on upstream strategy actions, while end-to-end metrics appear in validation.
- Keep visible labels concrete and short enough to fit inside their cards.
- Arrows should be dark enough to read, and should not touch or cross unrelated card borders.

## Current Generated Assets

```text
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.png
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.svg
figures/scripts/generate_upstream_strategy_design.py
figures/audit/upstream_strategy_design_audit.md
```
