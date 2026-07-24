# Research Experiment Plans

本目录保存正式研究实验计划、设计参考与状态审计，不保存原始结果。文档按性质分三类：

- **实验计划**（按研究内容）：跑实验时看——变量、假设、矩阵。
- **设计参考**：设计/答辩时查——baseline 矩阵、文献边界、实现映射。
- **状态审计**：看当前进展、缺口、路线图、审稿人风险。

> 技术基础知识（decode memory-bound、AIMD、continuous batching 等）的单一来源在 `research/` 和 `research/reading_notes/`，本目录只引用、不重复。

## 一、实验计划（按研究内容）

| 文件 | 对应研究内容 | 内容 |
|---|---|---|
| `data_organization_batching.md` | **研究内容一**：数据组织策略 | 静态 batch_size、token-budget、length-aligned、prefix-aware grouping 等候选方案 |
| `service_scheduling_backpressure.md` | **研究内容二**：提交控制策略 | 固定 K_max、adaptive K_max、routing 策略、queue-adaptive flush、actor pool 分池路由等候选方案；§0.5 自回归物理前提 |
| `cross_layer_killer_experiment.md` | **耦合验证** | 独立最优拼接 vs 联合 grid search（含策略级 + 引擎级参数）|
| `sink_writeback_coordination.md` | **写回工程参考**（已降级为实验设置，不作为独立实验阶段） | COPY + deferred index baseline，仅在实验设置中说明 |

## 二、设计参考

| 文件 | 用途 | 回答什么 |
|---|---|---|
| `baseline_reference.md` | 选 baseline 时查 | 从 CCF-A 文献提取的各方向最优 baseline（G/W/D/X 系列），避免 strawman 对照 |
| `strategy_design_literature_basis.md` | 写论文 / 答辩 / reviewer 防御时查 | **为什么这样设计 + 不能过度声称什么**：可借鉴思想 vs baseline/边界 vs 本文策略定义、fatal flaws、§3.1 借鉴论文适用边界 |
| `strategy_design_implementation_reference.md` | 写代码 / 设计实验变量时查 | **怎么实现**：信号→变量→指标→baseline→§8 目标代码架构→实现优先级 |

> **两个 strategy_design 的区别**（名字接近但分工不同）：`literature_basis` 是**边界论证**（为什么、不能声称什么），`implementation_reference` 是**工程映射**（怎么实现）。两者有部分重叠（都讲 vLLM/Orca/Sarathi 借鉴）但服务不同场景，不合并不替换。"先试哪个机制"的跨论文优先级不单列文档，并入 `experiment_status_and_gaps.md` §4「候选机制优先级」。

## 三、状态审计

| 文件 | 内容 |
|---|---|
| `experiment_status_and_gaps.md` | 已完成/未完成实验表、证据链完整性、指标盲区、P0/P1/P2 路线图、审稿人视角风险、§6 完整问题审计。**当前实验设计的第一参考。** |
| `archive/research_design_catalog.md` | **课题研究方案候选目录（已归档）**：28 个候选方案的六维评估矩阵，作为设计历史参考 |

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

## 文档维护纪律（2026-07-24）

1. **默认并入现有文档，不新建。** plans/ 里已存在的文档是某类内容的自然归属——"机制优先级"进 `experiment_status_and_gaps.md` §4，不另建索引；策略边界进 `strategy_design_literature_basis.md`；实现映射进 `strategy_design_implementation_reference.md`。深度内容进 `research/reading_notes/` 或对应 `*_reference.md`。**只有当某类内容在所有现有文档中都找不到自然归属时才新建文件，且必须在 `PROJECT_LOG.md` 说明为什么现有文档都不合适。**
2. **计划文档只保留待做内容。** 实验一旦完成（结果已记入 `experiments/results/` + `experiment_status_and_gaps.md`），其设计/变量/矩阵从对应计划文档（`data_organization_batching.md`、`service_scheduling_backpressure.md` 等）删除——计划文档只回答"接下来做什么"，不积累已完成实验的存量。**前提**：完成实验的 results 报告必须自包含该实验的设计；否则删除前先把设计迁移到 results。
