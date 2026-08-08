# Research Experiment Plans

## Baseline / benchmark：单一入口与文档边界

[`baseline_reference.md`](baseline_reference.md) 是 AI_COMPLETE、AI_EMBED、
AI_CLASSIFY 的**唯一总入口**，集中维护四层对照、原生性准入、证据等级、指标合同和
当前门禁。不要再从旧矩阵、结果 README 或部署文档拼接一份新的 baseline 总表。

| 需要回答的问题 | 唯一入口 |
|---|---|
| 比谁、为什么比、能否进入数字排名、必须记录什么 | `baseline_reference.md` |
| 文本 Chat/Completions 怎么运行 | `text_native_baseline_rerun_20260802.md` |
| DuckDB bounded-output 怎么对比（任务/指标/两计时边界/请求等价门禁；单 endpoint 产品语义、双 endpoint project/control 方法轨、可选 gateway 系统轨分开） | `bounded_output_duckdb_comparison_protocol_20260805.md` |
| 图像 workload、质量语义和执行矩阵怎么运行 | `image_clip_workload_lock_20260731.md` |
| work-unit、状态感知与图像动态主实验怎么分层验证 | `state_aware_work_unit_evaluation_20260808.md` |
| 当前做完什么、下一步是什么 | `experiment_status_and_gaps.md` §0 |
| 厂商/论文为什么采用这些指标 | `../../research/evaluation_metrics_survey_20260731.md` |
| 初学者如何理解四类 baseline | `../../learning/text_native_baseline_guide.md` |
| 真实数字与结论 | 对应 `../results/` 或 `../../motivation/results/` |
| 写多卡/吞吐/身份/统计报告前必读的诚实性检查清单（6 类反复错误 + §7 gate 4 项 + 8 步勾选） | `experiment_report_honesty_checklist.md` |
| 4 臂（bounded/duckdb/lb_rr/project）× 规模 × 并发全网格怎么规划（含 project_static 2-endpoint hang 根因+修复+256 门、十字切片 vs 完整矩形、成本排序、能/不能回答） | `full_grid_sweep_plan.md` |

2026-07-29 的数据库 AI 算子矩阵混有预注册和逐日执行日志，现完整保存在
`archive/database_ai_operator_baseline_matrix_20260729.md`，仅用于历史追溯，不再指导
新实验。每个正式 arm 仍需独立 calibration；相同 work 的 runtime 比较与改变调用数、
模型或语义的 system-level 比较必须分开。

本目录保存正式研究实验计划、设计参考与状态审计，不保存原始结果。文档按性质分三类：

- **实验计划**（按研究内容）：跑实验时看——变量、假设、矩阵。
- **设计参考**：设计/答辩时查——baseline 矩阵、文献边界、实现映射。
- **状态审计**：看当前进展、缺口、路线图、审稿人风险。

> 技术基础知识（decode memory-bound、AIMD、continuous batching 等）的单一来源在 `research/` 和 `research/reading_notes/`，本目录只引用、不重复。

## 〇、workload 选型与锁定（学长原则：先锁被认可场景）

学长反馈（`notes/communication_notes.md` §5）明确：**先定 benchmark/workload，场景被认可后任何正指标都被接受**。以下为 workload 选型文档：

| 文件 | 状态 | 内容 |
|---|---|---|
| `image_clip_workload_lock_20260731.md` | 🔴 **首个 workload**（当务之急） | 图像 AI_EMBED 执行门禁 + AI_CLASSIFY 正式候选。公开对齐轨道为 ImageNet/ResNet18，数据库泛化轨道为 COCO/CLIP multi-label；包含质量门禁、fused/staged baseline 与 host-data-path go/no-go。 |
| `msmarco_embedding_workload_20260731.md` | ⏸ 文本轻对照（降级） | MS MARCO 8.8M 批 embedding。仍是文本，token ID 紧凑，搬运轻，**瓶颈不显现**——仅作"文本下不显现"的边界对照。 |

方向 scope（DB↔GPU 经 Daft 桥接 + 三痛点 + 待验证的 offline-batch 候选界面）见
`research/daft_db_gpu_bridge_direction_scope_20260731.md`；强 claim 以 staged baseline
和 R0→R4 结果为准。

## 一、实验计划（按研究内容）

| 文件 | 对应研究内容 | 内容 |
|---|---|---|
| `data_organization_batching.md` | **研究内容一**：数据组织策略 | 静态 batch_size、token-budget、length-aligned、prefix-aware grouping 等候选方案 |
| `service_scheduling_backpressure.md` | **研究内容二**：提交控制策略 | 固定 K_max、adaptive K_max、routing 策略、queue-adaptive flush、actor pool 分池路由等候选方案；§0.5 自回归物理前提 |
| `cross_layer_killer_experiment.md` | **耦合验证** | 独立最优拼接 vs 联合 grid search（含策略级 + 引擎级参数）|
| `sink_writeback_coordination.md` | **写回工程参考**（已降级为实验设置，不作为独立实验阶段） | COPY + deferred index baseline，仅在实验设置中说明 |
| `operator_cost_profile_dual4090_formal_20260804.md` | **算子代价估计共同使能组件** | 当前 320-run formal 合同；§8 保留通过晋级门槛后才启动的 TPC-H-derived AI 查询计划 held-out |
| `opening_database_e2e_p0_20260807.md` | **开题前证据闭环** | 仅允许的两组文本三臂统一 database-E2E：SQuAD 均匀控制组与 ShareGPT controlled-skew；冻结 source/manifest/sink/计时/质量/资源合同和停止规则 |
| `state_aware_work_unit_evaluation_20260808.md` | **开题最小方法证据 + 论文主实验总合同** | 开题原生单 Job、原生两 Job 观察和项目 short/long 两臂均已完成；phase-change、weighted/held-out、多模态动态与 cost held-out 留论文阶段 |

双 GPU 7B 复验遵循分层门禁：先在 `service_scheduling_backpressure.md` 确定
相同 per-GPU credit 下的容量曲线，再按 `data_organization_batching.md` 关闭
arrival replay 隔离数据组织，最后回到 arrival replay 检验 request-level
持续补位。不能用同一个大矩阵同时搜索三层参数。

**当前执行状态（2026-08-09）**：开题 database-E2E replacement、文本原生单 Job、
`Short@0s → Long@5s` guaranteed-overlap 原生观察和项目 static/shared 因果 A/B 均已
完成，开题前停止新增 baseline、offset、weight、4+ Job 或参数扫描。当前只维护报告、
紧凑数据和待画图合同。开题后的最小工程顺序为 production descriptor builder →
observe-only snapshot → no-op/fallback gate → 同上限单一控制动作消融；随后才进入
phase-change、weighted/SLO、图像动态和 held-out cost。旧 49K/65K 扫描顺序与
2026-07-30 fail-closed 筛选只保留为历史设计记录，不再是执行指令。权威状态只看
`experiment_status_and_gaps.md` 的开题冻结段与 §0。

## 二、设计参考

| 文件 | 用途 | 回答什么 |
|---|---|---|
| `baseline_reference.md` | 选 baseline 时查 | 从 CCF-A 文献提取的各方向最优 baseline（G/W/D/X 系列），避免 strawman 对照 |
| `strategy_design_literature_basis.md` | 写论文 / 答辩 / reviewer 防御时查 | **为什么这样设计 + 不能过度声称什么**：可借鉴思想 vs baseline/边界 vs 本文策略定义、fatal flaws、§3.1 借鉴论文适用边界 |
| `strategy_design_implementation_reference.md` | 写代码 / 设计实验变量时查 | **怎么实现**：信号→变量→指标→baseline→§8 目标代码架构→实现优先级 |
| `literature_driven_pipeline_optimization_guide.md` | 继续从文献寻找优化点时查 | **怎么发现下一项机制**：三层 batch 边界、Orca 式上游持续补位、完整 adaptive flush 缺口、机制卡模板、fatal-flaw audit、候选池与晋级/放弃条件 |
| `text_native_baseline_rerun_20260802.md` | 重跑文本 baseline 前查 | **怎么严谨重测**：区分 service ceiling、direct control、framework/product native；定义 Chat/Completions 分轨、64-row validity gate、512-row calibration 与 2,048-row held-out 合同；当前仅 gate runner 可执行 |

> **文档分工**：`literature_basis` 是论文边界论证，`implementation_reference`
> 是已有工程映射，`literature_driven_pipeline_optimization_guide` 是今后重复使用的
> 机制发现与筛选流程。具体完成度仍以 `experiment_status_and_gaps.md` 为准。
> baseline 的当前口径只以 `baseline_reference.md` 和对应模态执行合同为准；
> `code_doc/superpowers/` 与 `plans/archive/` 中的文档仅保留设计历史。正式数据只进入
> `experiments/results/` 或 `motivation/results/` 的对应结果目录。

## 三、状态审计

| 文件 | 内容 |
|---|---|
| `experiment_status_and_gaps.md` | 开题冻结段与 §0 是当前状态；后续 P0/P1/P2、旧扫描顺序只作已标记的历史记录。**当前实验设计的第一参考。** |
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

进入具体实现或实验矩阵设计时，再查阅 `strategy_design_implementation_reference.md`：该文件把两项策略拆成数据组织策略（研究内容一）、调度与提交控制策略（研究内容二），加上多模态泛化验证和贯穿两项策略的算子代价估计，并列出每部分的信号、变量、指标、baseline 和实现优先级。

## 文档维护纪律（2026-07-24）

1. **默认并入现有文档，不新建。** plans/ 里已存在的文档是某类内容的自然归属——实验完成度进 `experiment_status_and_gaps.md`，策略边界进 `strategy_design_literature_basis.md`，已有实现映射进 `strategy_design_implementation_reference.md`，可重复使用的跨论文机制发现流程统一进 `literature_driven_pipeline_optimization_guide.md`。深度内容进 `research/reading_notes/` 或对应 `*_reference.md`。**只有当某类内容在所有现有文档中都找不到自然归属时才新建文件，且必须在 `PROJECT_LOG.md` 说明为什么现有文档都不合适。** `state_aware_work_unit_evaluation_20260808.md` 是一次明确例外：它冻结跨研究内容一、研究内容二和图像泛化的同一因果矩阵，避免三个旧计划分别定义不兼容的 work/credit/state 合同。
2. **计划文档只保留待做内容。** 实验一旦完成（结果已记入 `experiments/results/` + `experiment_status_and_gaps.md`），其设计/变量/矩阵从对应计划文档（`data_organization_batching.md`、`service_scheduling_backpressure.md` 等）删除——计划文档只回答"接下来做什么"，不积累已完成实验的存量。**前提**：完成实验的 results 报告必须自包含该实验的设计；否则删除前先把设计迁移到 results。
