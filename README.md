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
