# Research Directory

本目录维护数据库 AI 算子、LLM serving、Ray/数据引擎、算子代价估计与写回相关的一手论文、官方资料和精读笔记。

## 重点入口

| 文件 | 用途 |
|---|---|
| `knowledge_hub.md` | 项目知识总汇：机制、文献地图、研究空白、设计模式和实验边界 |
| `top15_ranked_papers.md` | 当前开题 Top 15；15/15 为严格 CCF-A 正式 research paper |
| `ai_operator_literature_inventory.md` | Top 15、核心补充、题录勘误、baseline 与代价估计文献清单 |
| `inference_pipeline_interaction_literature.md` | 上游数据管线、continuous batching、semantic operator、公平调度和代价估计交互综述 |
| `reading_notes/` | 49 篇权威精读笔记及模板 |
| `reference/REFERENCE_INDEX.md` | 当前工作区 21 份可解析 PDF 的权威题录和用途 |
| `existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子执行链路对比 |
| `vllm_continuous_batching_reference.md` | vLLM continuous batching、KV/cache、metrics 和集成边界 |
| `ray_actor_dynamic_batching_reference.md` | Ray actor/Serve 动态 batching 与路由机制 |
| `heterogeneous_ai_dataflow_execution_model_20260811.md` | CPU–GPU 异构分阶段执行模型候选：typed block、byte-bounded ready queue、SAOR 控制面、数学模型、数据通路消融与 prompt/复用/增量推理待办 |
| `saor_model_scenario_audit_20260811.md` | SAOR capacity-only/fixed-envelope 负结果后的数学审计：签名化 K、parked dynamic-K governor、oracle 证明骨架；§12 冻结通用有界词典序 priority/SLO-budget/actual-work-debt 设计、release-opportunity 非饥饿界、事件证据与 2-Job 首验门 |
| `evaluation_metrics_survey_20260731.md` | AI 算子/推理服务论文与数据库厂商的 workload 场景、执行边界、评估指标和公平对比合同；当前 baseline 身份与运行状态不在此维护 |
| `daft_db_gpu_bridge_direction_scope_20260731.md` | 方向 reframe scope：保留 Daft 三痛点与 offline-batch 候选，已按 08-01 审计撤回“传输瓶颈/结构性空白”预设，并要求 staged baseline |

## 文献分级

1. **Top 15**：CCF-A 正式 research paper，必须有全文精读和本地 PDF。
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
4. 先定义强 baseline、同条件契约和晋级门槛，再实现候选。
5. 用真实实验决定晋级；负结果停止参数挖掘并收窄设计空间。

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
- VTC 是多 job service-counter 算法 baseline；Llumnix是动态负载表征参考。
- 每个 arm 独立 calibration；不要求无限调优，但必须合理强并进入平台期。

完整 baseline 矩阵见 `../experiments/plans/baseline_reference.md`。

## 维护规则

- 新增文献先核验正式题录与轨道，再下载、精读和分类。
- 不根据摘要直接重排 Top 15。
- 新增/删除笔记或 PDF 时同步 `REFERENCE_INDEX.md`、`PROJECT_INDEX.md` 和 `PROJECT_LOG.md`。
- 结论标明来源：论文、官方文档、源码、本地实验、合理推断或待确认。
