# SemLoom

本项目研究**AI 语义算子与多模态数据任务的物理执行、数据组织与调度优化**。
PostgreSQL 是数据库接入场景之一，拥有该路径的 SQL、计划、快照、权限及查询生命周期；SemLoom 利用 Ray Core 的任务、actor 和对象设施组织外部执行，
与 Ray Data、Daft 等原生数据执行路径比较。合理调优的原生批处理、actor 池、背压与 CPU/GPU 流水是共同起点。

近期优先加速**单查询／一批数据的完整执行**，检验能否少准备、少搬运、少做重复计算。
端到端任务时间及有效吞吐是主要目标；GPU 利用率解释供给与等待，多查询共享属于后续场景。
原生 tensor 路径可以研究表示、阶段工作量、准备位置、后继消费者和局部性；HTTP 路径控制供给与路由，
模型服务仍拥有其内部 tensor、KV 和计算调度。方向与顺序只在[总纲](PROJECT_OUTLINE.md)维护。

| 要查什么 | 权威入口 |
|---|---|
| 规则、术语与内容归属 | [AGENTS.md](AGENTS.md)、[CONTEXT.md](CONTEXT.md)、[项目导航](PROJECT_INDEX.md) |
| 方向、优先级与历史范围 | [项目总纲](PROJECT_OUTLINE.md) |
| 实际能力与尚未验证项 | [代码状态](code/INFRA_STATUS.md)、[模块与命令](code/README.md) |
| 架构职责与接入设计 | [系统架构](experiments/plans/系统架构.md) |
| 最近邻、机制与文献版本 | [研究定位](docs/research/研究定位.md)、[方法依据](docs/research/优化方法依据.md)、[文献入口](docs/research/README.md) |
| 实验设计与比较口径 | [方案](experiments/plans/README.md)、[对照规范](experiments/plans/reference/对照规范.md)、[报告核对](experiments/plans/reference/报告核对.md) |
| 实际结果、全部重复与失败 | [结果导航](experiments/results/README.md)、[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 环境、数据及历史材料 | [运行手册](deploy/runtime/README.md)、[数据](data/README.md)、[文档](docs/README.md)、[图资产](figures/README.md) |
| 变更与来源恢复 | [项目日志](PROJECT_LOG.md) |

Palimpzest 与 LOTUS 是平行的语义系统参照；其[研究定位](docs/research/研究定位.md#palimpzest-comparison)和[五组执行目标](experiments/plans/reference/对照规范.md#palimpzest-fixed-plan)区分完整系统与固定计划比较，目标接入仍需分别验证。

当前图像接点与固定来源资格从[LOTUS 模块](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/code/src/semantic_methods/lotus/README.md#原生-clip-图像检索)和
[主报告](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#final-integrated-qualification)进入；
外部文件 CLIP 冷算子、文本原 Q2、PG 路径及负结果保留各自运行身份，不能互借验证范围。
常驻完整查询、一次性启动／建库及逐查询原生重建的计时按[统一口径](experiments/plans/reference/对照规范.md#resident-query-timing)解释。

本次 main 仅更新文档，代码仍以 `425f0145` 为基点；上面的新 LOTUS 图像入口和结果来自
[integration/baselines 固定快照 `9c59dc9b`](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/code/INFRA_STATUS.md)，研究分支按具体假设独立评价。
Git 合并不等于性能、质量或新来源实测通过。分支与工作区安排由集成负责人维护，根入口不缓存临时支线清单。
