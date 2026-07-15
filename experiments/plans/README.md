# Research Experiment Plans

本目录保存正式研究实验计划，不保存原始结果。

## 重点入口

| 文件 | 作用 |
|---|---|
| `research_design_catalog.md` | **课题研究方案候选目录**（2026-07-15）：RC1/RC2/RC3/跨层 共 28 个候选方案的六维评估矩阵、分阶段路线图、风险分析、Killer Experiment 矩阵和 Baseline 设计考量 |
| `baseline_reference.md` | **实验 Baseline 参考矩阵**：从 CCF-A 文献中提取的各方向最优 baseline 策略（GPU 调度 / 写回 / 数据组织 / 跨层决策），用于实验设计时对照 |
| `strategy_design_literature_basis.md` | **策略设计思路的文献依据与边界**：区分可借鉴优化思想、baseline/边界和本文自己的策略定义，用于支撑策略设计图和方法口径 |
| `strategy_design_implementation_reference.md` | **策略设计与系统实现参考**：把 Ray、vLLM/Ray Serve/Triton、GPU 数据放置和 DB AI 算子文献机制沉淀为三层策略、实验变量和实现优先级 |

## 实验计划

| 文件 | 对应研究内容 | 内容 |
|---|---|---|
| `data_organization_batching.md` | **RC1**：数据组织与批处理构造 | Grid search 建立静态最优 baseline、workload 对比、selectivity-aware 策略、模型 batch scaling 前置实验 |
| `service_scheduling_backpressure.md` | **RC2**：GPU 推理服务感知调度与反压 | K_max sweep、routing 策略对比、adaptive vs static K_max、vLLM baseline 前置实验 |
| `sink_writeback_coordination.md` | **RC3**：结果汇聚与持久化协同 | B 系列工程 baseline（UPSERT vs COPY、logged vs unlogged、online vs deferred index）、三路写回架构对比、sink 对照 |
| `cross_layer_killer_experiment.md` | **端到端效果评估增强实验** | 在上游执行链路调优和写回瓶颈判定完成后，用 BL1-BL4、全链路配置、代价模型准确性评估、消融瀑布和跨 workload 泛化分析阶段间耦合 |

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
P0: vLLM 接入 + B 系列写回工程 baseline
  ↓
P1: RC1 Grid Search → RC2 K_max Sweep → RC3 三路架构对比
  ↓                     ↓                     ↓
  └─────────────────────┴─────────────────────┘
                        ↓
P2: 端到端效果评估；必要时补充 cross_layer_killer_experiment.md 的增强对照
```

**在 P0 完成之前，所有 Grid Search 的结果都基于 suboptimal baseline，不能作为论文最终数据。**

## 设计规则

设计实验 baseline 前，先查阅 `baseline_reference.md`——优先从已有 CCF-A 文献中提取最优策略作为对照，不凭空设计 strawman baseline。设计“本文策略”或更新策略设计图前，先查阅 `strategy_design_literature_basis.md`，区分哪些是可借鉴思想、哪些只是 baseline/边界、哪些才是本文自己的策略。实验设计方法论参照 AGENTS.md §6.5（文献优先设计规则）和 `research/README.md` §文献优先设计方法论。

进入具体实现或实验矩阵设计时，再查阅 `strategy_design_implementation_reference.md`：该文件把三层策略拆成计划层、运行层和服务端 micro-batching，并列出每层的信号、变量、指标、baseline 和实现优先级。
