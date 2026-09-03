# Research Directory

本目录维护数据库 AI 算子、LLM serving、Ray/数据引擎、算子代价估计与写回相关的一手论文、官方资料、泛读笔记和精读笔记。

## 重点入口

| 文件 | 用途 |
|---|---|
| `knowledge_hub.md` | 项目知识总汇：机制、文献地图、可迁移策略和研究空白；不维护当前代码状态或实施顺序 |
| `top15_ranked_papers.md` | 当前开题 Top 15；15/15 为严格 CCF-A 正式 research paper |
| `ai_operator_literature_inventory.md` | Top 15、核心补充、题录勘误、baseline 与代价估计文献清单 |
| `inference_pipeline_interaction_literature.md` | 上游数据管线、continuous batching、semantic operator、公平调度和代价估计交互综述 |
| `reading_notes/` | 49 篇历史文献笔记；从 2026-08-21 起按泛读、筛选和快速回顾管理 |
| `精读文献笔记/` | 精读笔记权威库；当前有十七篇主笔记、160 张论文原图裁剪件。新增 IMLane 使用正式 PVLDB 2026 版本，正文 Figure 1–15 已加入对应讲解位置；Kalypso 继续按 arXiv 核心补充管理。两篇均未并入原十五篇横向速览或已定稿开题正文。各篇选图与版本说明见目录 README 和 `figures/audit/` |
| `reference/REFERENCE_INDEX.md` | 历史题录与用途索引；当前工作区可解析实体为 Galois、Abacus、Palimpzest、Sema、Parrot、Kalypso、IMLane 七份 |
| `existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子执行链路对比 |
| `sema_native_semantic_operator_architecture_reference_20260827.md` | PostgreSQL 语义算子的理论与迁移审计：Sema/Cortex 说明数据库语义所有权，LOTUS 提供 reference/optimized algorithms，IMLane/Kalypso 提供后续执行参照；工程实施另看 `../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md` |
| [语义前缀候选审查](semantic_prefix_reuse_design_audit_20260903.md) | 专项研究辅助材料：前缀/表示的源码事实、最近邻反例、瓶颈推导、质量条件与创新性验证；不是新的架构主计划或已运行实验 |
| `lotus_postgresql_execution_layer_fit_20260821.md` | PG、LOTUS 与 SAOR 的分层审计：LOTUS 作语义前端候选/独立系统 baseline，不作为当前方法的强制执行依赖 |
| `vllm_continuous_batching_reference.md` | vLLM continuous batching、KV/cache、metrics 和集成边界 |
| `ray_actor_dynamic_batching_reference.md` | Ray actor/Serve 动态 batching 与路由机制 |
| `heterogeneous_ai_dataflow_execution_model_20260811.md` | CPU–GPU 异构分阶段执行模型候选：typed block、byte-bounded ready queue、SAOR 控制面、数学模型、数据通路消融与 prompt/复用/增量推理待办 |
| `saor_model_scenario_audit_20260811.md` | SAOR capacity-only/fixed-envelope 数学审计；§12 规定实验开始前选定并在运行期间保持不变的 $H_B/W_e$、有界 priority/debt 与 release-opportunity 条件，并记录 bounded-ready、同窗口 selector 归因、observation bridge 及 native-system matched comparison 适用条件 |
| `evaluation_metrics_survey_20260731.md` | AI 算子/推理服务论文与数据库厂商的 workload、执行条件和指标定义；§9.3 规定当前单租户多 Job 的 equal-share/differentiated-service、公平/隔离、三个 JCT 反事实、未来 tenant 层次、原生 baseline、同 ready-window 的项目内部消融与隐藏缓冲成本；当前运行状态不在此维护 |
| `daft_db_gpu_bridge_direction_scope_20260731.md` | 方向 reframe scope：保留 Daft 三痛点与 offline-batch 候选，已按 08-01 审计撤回“传输瓶颈/结构性空白”预设，并要求 staged baseline |

## 文献分级

1. **Top 15**：CCF-A 正式 research paper，必须有本地 PDF；现有开题 Top 15 笔记是历史快照，承担关键论点的论文应在正式写作前补齐 `精读文献笔记/` 全文精读。
2. **核心补充**：高度相关的 CIDR、MLSys、Tutorial、Companion、benchmark 或 arXiv；精确标注轨道，不冒充 CCF-A。
3. **工程资料**：官方文档、源码和产品资料，只证明接口/工业需求，不证明学术新颖性。
4. **项目证据**：真实实验 CSV/报告，用于验证本地因果，不由论文结论替代。

当前 Top 15 结构：

- AI 算子与数据库系统：LOTUS、Galois、GaussML；
- LLM 推理与公平调度：vLLM、Orca、Sarathi-Serve、SGLang、VTC、Llumnix、DistServe；
- Ray：Ray OSDI 2018；
- 算子代价估计：Learned Cost Models、GRACEFUL、COSTREAM、Abacus。

## 文献优先设计方法

1. 明确当前问题属于数据组织、serving capacity、公平调度、代价估计还是写回。
2. 从 Top 15 和核心补充中提取机制、假设和实验 baseline。
3. 做迁移审计：本项目不修改 vLLM，固定双 GPU，不把 autoscaling、KV migration 或 kernel 优化当可直接实现机制。
4. 先定义强 baseline、相同实验条件和候选被采用前必须满足的正确性/性能标准，再实现候选。
5. 用真实实验决定是否采用候选；负结果停止参数挖掘并收窄设计空间。

## 算子代价估计定位

代价估计是数据组织与提交控制的共同使能组件，不独立扩张为第三项研究内容。首版：

```text
简单解析模型 + profile 校准 + residual correction
```

预测 prompt/output work、service time、JCT、remaining work 和 SLO slack，用于 active-work/K、组织、路由和提交选择。除 MAE/MAPE/R² 外，必须报告配置 ranking、决策 regret 与 prediction interval。

## Baseline 规则

- direct vLLM 是 serving ceiling，不是竞争系统。
- 无 Daft/Ray 强客户端用于隔离上游框架成本。
- Daft Native/Ray 和 Ray Data 是官方 runtime baseline。
- LOTUS/Palimpzest 是数据库 AI 系统 baseline；SemBench 提供 workload 和多维指标。
- VTC 是引擎内多 job service-counter 相关工作；SemLoom 的 external VTC-style 复现只能标为
  SemLoom internal control，历史 `Project` 身份按原 schema 保留，不能冒充原生 VTC baseline；
  Llumnix 是动态负载表征参考。
- 每个 arm 独立 calibration；不要求无限调优，但必须合理强并进入平台期。

完整 baseline 矩阵见 `../experiments/plans/baseline_reference.md`。

## 维护规则

- 新增文献先核验正式题录与轨道，再下载、精读和分类。
- 不根据摘要直接重排 Top 15。
- 新增/删除泛读或精读笔记时同步本 README、`PROJECT_INDEX.md` 和 `PROJECT_LOG.md`；PDF 有变化时再同步 `REFERENCE_INDEX.md`。
- 泛读可使用 `reading_notes/tpl-文献泛读.md`；精读不使用统一模板，按 `精读文献笔记/README.md` 的目录与内容要求组织。
- 结论标明来源：论文、官方文档、源码、本地实验、合理推断或待确认。
