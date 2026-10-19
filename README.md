# SemLoom

本项目研究**AI 语义算子与多模态数据任务的物理执行、数据组织与调度优化**，面向研究工作负载构建可扩展的语义数据执行层。
PostgreSQL 是数据库接入场景之一，拥有该路径的 SQL、计划、快照、权限及查询生命周期；SemLoom 通过中立接口组织外部执行，具体准备与模型设施按路径选择，
与 Ray Data、Daft 等原生数据执行路径比较。合理调优的原生批处理、actor 池、背压与 CPU/GPU 流水是共同起点。

近期优先加速**单查询／一批数据的完整执行**，检验能否少准备、少搬运、少做重复计算。
端到端任务时间及有效吞吐是主要目标；GPU 利用率解释供给与等待，多查询共享属于后续场景。
原生 tensor 路径可以研究表示、阶段工作量、准备位置、后继消费者和局部性；当前完整 HTTP 请求接点控制供给与路由，
模型服务仍拥有其内部 tensor、KV 和计算调度。方向与顺序只在[总纲](PROJECT_OUTLINE.md)维护。

LOTUS 主线由 SemBench 提供任务与评价、LOTUS 保留原算法及优化；SemLoom 的目标底座保持 Daft 扫描／普通关系处理＋Ray Core／Actor，
自管准备、供给与回收，Ray Data／Daft／IMLane 对照各自原生执行。下一单元是[普通／级联 Filter 记录级执行](experiments/plans/数据组织.md#lotus-filter-data-execution)，
records→结果独立建设，先用原生 Ray Data 普通 Filter 与原生 LOTUS 核对；旧 request 冻结范围作参照，选择性复用，不作为新主链的必经依赖。
并沿同一合同将[真实 LLM／VLM 引擎纳入模型 worker／actor](experiments/plans/系统架构.md#model-engine-execution)，成为数据执行链的一部分。
HTTP records 流水保留为阶段实现与对照；记录级入口与生成模型引擎尚未纳入 main，功能分支的实现及资格统一从[实施来源](experiments/plans/数据组织.md#model-engine-roadmap)核对。
集成分支的 [IMLane普通关系链入口](code/integrations/imlane_records/chain.README.md#公开-ordinary-chain-入口)复用原作者执行与LOTUS方法；旧作者CPU和最终入口资格按固定源码分列。

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

main 的[原生CLIP查询入口](code/scripts/README.md#原生-lotus-clip-查询)保留RM、FAISS建库／检索及完整结果；
[本批接纳记录](experiments/results/diagnostics/main_lotus_clip_20261015/README.md)分列CPU接入、固定d2历史资格及main570四路径八臂40行严格单卡实机结果。
历史外部文件CLIP、文本原Q2、PG路径与负结果保留各自身份。
常驻完整查询、一次性启动／建库及逐查询原生重建的计时按[统一口径](experiments/plans/reference/对照规范.md#resident-query-timing)解释。

main 已接纳统计修复、[Core／完整响应基础](code/src/execution_provider/adapters/README.md)和[LOTUS Movie／Animals 四臂](code/src/semantic_methods/lotus/README.md)，原提示、SDK解析和查询评价仍由LOTUS拥有；CLIP条件入口及main570实机资格从上方同题报告核对。
[固定 main 的原Movie／Animals实际模型报告](experiments/results/diagnostics/main_queries_20261015/README.md)覆盖四臂完整查询，保留质量不足和单次边界。
[可选vLLM部署资源](experiments/results/diagnostics/main_service_auto_20261015/README.md)按显式分配解析auto／数量，候选服务和main2041的单卡原Movie Q2／Animals Q1复验分列，未覆盖CLIP或多卡。
Git 合并不等于性能、质量或新来源实测通过。分支与工作区安排由集成负责人维护，根入口不缓存临时支线清单。
