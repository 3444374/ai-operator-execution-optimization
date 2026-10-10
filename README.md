# SemLoom

本项目研究**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。
PostgreSQL负责SQL、计划、snapshot、权限及查询生命周期；SemLoom在数据库管理下组织任务并调用可替换的外部执行服务。

研究内容包括按token、frame、阶段工作量与局部性组织数据，以及固定容量下的提交、路由和多Job调度。
算子代价估计为两项内容提供信息。文本`AI_COMPLETE`是首版主场景，图像用于跨模态验证。
方向与近期顺序见[项目总纲](PROJECT_OUTLINE.md)，实际能力与待完成项见[代码状态](code/INFRA_STATUS.md)。

| 需求 | 入口 |
|---|---|
| 查长期规则与领域术语 | [AGENTS.md](AGENTS.md)、[CONTEXT.md](CONTEXT.md) |
| 查内容归属、目录与旧路径 | [项目导航](PROJECT_INDEX.md) |
| 查实现与命令 | [代码](code/README.md)、[脚本](code/scripts/README.md) |
| 查研究依据与文献 | [研究入口](docs/research/README.md)、[研究定位](docs/research/研究定位.md) |
| 按流程和场景分析优化 | [应用、工程与理论分析](docs/research/优化方法依据.md#query-specific-execution-opportunities)、[物理需求与准备时机](docs/research/优化方法依据.md#heterogeneous-demand-supplement)、[共享准备切片](experiments/plans/数据组织.md#scene-preparation-pilot) |
| 查计划、结论和失败证据 | [实验方案](experiments/plans/README.md)、[结果](experiments/results/README.md)、[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 准备机器和数据 | [运行手册](deploy/runtime/README.md)、[数据来源](data/README.md) |
| 查图表与历史材料 | [图资产](figures/README.md)、[项目文档](docs/README.md) |
| 追溯项目变更 | [项目日志](PROJECT_LOG.md) |

[文本Map四路径比较](experiments/results/postgresql/text_map_four_path_comparison_20260930/README.md)与
[worker复用](experiments/results/postgresql/text_map_worker_reuse_real_20260930/README.md)、
[gateway诊断](experiments/results/postgresql/text_map_gateway_lifecycle_20261001/README.md)、
[线程记账](experiments/results/postgresql/text_map_threaded_accounting_20261002/README.md)、
[Arrow分批](experiments/results/postgresql/text_map_arrow_batches_20261002/README.md)分别保留实际重复值、负结果和适用条件。
完整查询、质量与资源结论以各报告为准。

[同应用原生语义系统](experiments/results/postgresql/semantic_system_comparison_20261008/README.md)已完成五系统512行重复评价、
PG／LOTUS原始Movie Q3及三种同后端控制的模型工程检查，共62条查询／14,408次真实请求。
Sema格式与Daft文件容量修订通过，原32次和8,009次停止保留；方法性能、多Job及图像仍按自身计划。

[数据就绪后的SQL／原生API比较](experiments/results/postgresql/query_ready_timing_20261008/README.md)
补充分离准备后的查询、单行交付和统一HTTP计时，保留独立运行及全部重复值。

[跨系统方法与执行接入](experiments/results/postgresql/native_adapter_integration_20261009/README.md)保存公共任务、LOTUS、DuckDB、Sema和原生Daft／Ray的接入、常驻检查及全部历史结果。
当前[修复来源模型观察](experiments/results/postgresql/native_adapter_integration_20261009/README.md#repair-model)按来源区分工程修复、质量和实际请求容量；
[有限容量筛查](experiments/results/postgresql/native_adapter_integration_20261009/README.md#capacity-screening-stop)已因运行包标签错误停止，尚无有效容量测量。实现状态见[代码状态](code/INFRA_STATUS.md)。

[批量持久记账](experiments/results/diagnostics/request_budget_batch_20261003/README.md)保留组件、无模型改善与真实模型回退结果，默认同步保持。

[gateway共享执行干扰](experiments/results/diagnostics/gateway_isolation_20261004/README.md)区分本地组件行为，另有Linux/真实Ray/本机HTTP核对，实际SQL和GPU验证另列。
[共享预付观察API](experiments/results/diagnostics/mapped_request_budget_20261003/README.md#server-observer)完成有限服务器接入，完整消费未获净收益，正式同步默认保持。
