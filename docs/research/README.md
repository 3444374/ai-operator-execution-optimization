# 研究文档

本目录维护文献、机制分析、方法选择与待验证问题。代码用法见所属模块，
运行事实见[实验报告](../../experiments/results/README.md)，实施顺序见[研究计划](../../experiments/plans/README.md)。

| 主题 | 入口 |
|---|---|
| 研究问题、最近邻与待证增量 | [研究定位](研究定位.md) |
| 方法选择、反例与最小实验 | [优化方法依据](优化方法依据.md) |
| 文献题录与阅读状态 | [文献清单](ai_operator_literature_inventory.md) |
| 泛读与快速回顾 | [reading_notes/](reading_notes/README.md) |
| 逐篇精读与图解 | [精读文献笔记](精读文献笔记/README.md) |
| 推理服务、上游管线与调度交互 | [交互综述](inference_pipeline_interaction_literature.md) |
| 语义系统与执行接入参照 | [执行链路](existing_ai_operator_execution_chains.md)、[数据库语义参照](sema_native_semantic_operator_architecture_reference_20260827.md) |
| 服务机制与数据引擎 | [vLLM](vllm_continuous_batching_reference.md)、[Ray](ray_actor_dynamic_batching_reference.md)、[Daft](daft_ray_multimodal_reference.md) |
| 异构阶段与工作量 | [分阶段执行模型](heterogeneous_ai_dataflow_execution_model_20260811.md) |
| 共享服务研究 | [SAOR 方法审计](saor_model_scenario_audit_20260811.md) |
| 表示与前缀复用 | [专题审查](semantic_prefix_reuse_design_audit_20260903.md) |
| 指标与比较方法 | [指标专题](evaluation_metrics_survey_20260731.md)、[对照规范](../../experiments/plans/reference/对照规范.md) |
| PDF 与资料定位 | [资料入口](reference/README.md) |

目录不缓存论文数量、图数、逐次运行结果或实现进度；相应事实由所属笔记、报告与代码状态维护。
文献阅读和研究证据要求见[本目录规则](AGENTS.md)。
