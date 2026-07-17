# Research Experiment Plans

本目录保存正式研究实验计划，不保存原始结果。

## 重点入口

| 文件 | 作用 |
|---|---|
| `research_design_catalog.md` | **课题研究方案候选目录**（2026-07-15）：28 个候选方案的六维评估矩阵、分阶段路线图、风险分析、实验矩阵和 Baseline 设计考量 |
| `baseline_reference.md` | **实验 Baseline 参考矩阵**：从 CCF-A 文献中提取的各方向最优 baseline 策略（GPU 调度 / 数据组织 / 提交控制），用于实验设计时对照 |
| `strategy_design_literature_basis.md` | **策略设计思路的文献依据与边界**：区分可借鉴优化思想、baseline/边界和本文自己的策略定义，用于支撑策略设计图和方法口径 |
| `strategy_design_implementation_reference.md` | **策略设计与系统实现参考**：把 Ray、vLLM、Daft、GPU 数据放置和 DB AI 算子文献机制沉淀为两项策略 + 端到端验证、实验变量和实现优先级（2026-07-17 已统一口径）|

## 实验计划

| 文件 | 对应研究内容 | 内容 |
|---|---|---|
| `data_organization_batching.md` | **研究内容一**：数据组织策略 | 静态 batch_size、token-budget、length-aligned、prefix-aware grouping 等候选方案 |
| `service_scheduling_backpressure.md` | **研究内容二**：提交控制策略 | 固定 K_max、adaptive K_max、routing 策略、queue-adaptive flush、actor pool 分池路由等候选方案 |
| `sink_writeback_coordination.md` | **写回工程参考**（不作为独立实验阶段） | COPY + deferred index baseline，仅在实验设置中说明 |
| `cross_layer_killer_experiment.md` | **耦合验证** | 独立最优拼接 vs 联合 grid search（含策略级 + 引擎级参数）|

## 实验计划的共同评估标准（来自 CCF-A 论文）

所有四个实验计划遵循从 [vLLM (SOSP 2023)]、[Orca (OSDI 2022)]、[TurboVecDB (VLDB 2025)]、[GaussML (ICDE 2024)]、[FlexPushdownDB (VLDB 2021)] 五篇 CCF-A 论文提取的共同方法论：

1. **曲线 > 单点**：不报"快 X×"，而是画吞吐-延迟曲线展示全工作点
2. **先暴露瓶颈再讲优化**：用阶段拆解展示瓶颈位置，再针对优化
3. **同硬件公平 baseline**：所有对照跑在同一机器、同一数据、同一模型
4. **消融拆开**：每个优化的独立贡献可量化
5. **诚实报告边界**：每个实验有计划地验证"什么时候不 work"
6. **统计严谨**：重复次数、集中趋势（中位数）、warm-up 策略、Ray 状态重置

## 实验前置依赖

```
前置：vLLM + Qwen2.5-1.5B baseline 建立 + Daft 文本阶段接入
  ↓
第一阶段：研究内容一 数据组织策略消融（token-budget + 分组策略 + Daft 引擎参数）
  ↓
第二阶段：研究内容二 提交控制策略消融（queue-adaptive flush + routing + Daft engine 参数）
  ↓
第三阶段：耦合验证（独立最优拼接 vs 联合 grid search，判定是否需要联合调优）
  ↓
第四阶段：多模态泛化验证（图像 workload，同一套策略代码）
```

**在 vLLM baseline 建立之前，所有基于手动 HTTP endpoint 的实验结果都基于 suboptimal baseline，不能作为论文最终数据。**

## 设计规则

设计实验 baseline 前，先查阅 `baseline_reference.md`——优先从已有 CCF-A 文献中提取最优策略作为对照，不凭空设计 strawman baseline。设计“本文策略”或更新策略设计图前，先查阅 `strategy_design_literature_basis.md`，区分哪些是可借鉴思想、哪些只是 baseline/边界、哪些才是本文自己的策略。实验设计方法论参照 AGENTS.md §6.5（文献优先设计规则）和 `research/README.md` §文献优先设计方法论。

进入具体实现或实验矩阵设计时，再查阅 `strategy_design_implementation_reference.md`：该文件把两项策略拆成数据组织策略（研究内容一）、调度与提交控制策略（研究内容二），加上多模态泛化验证和算子代价估计补充，并列出每部分的信号、变量、指标、baseline 和实现优先级。
