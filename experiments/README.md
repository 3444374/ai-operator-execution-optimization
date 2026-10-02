# 实验与证据

本目录统一保存方法实验、动机画像和组件验证。方案记录问题、实施与对照要求，
报告记录实际执行、全部重复、失败、数据和结论；真实能力从[代码状态](../code/INFRA_STATUS.md)核对。

| 内容 | 入口 |
|---|---|
| 现行方案与共同对照规范 | [plans/](plans/README.md) |
| 已执行与历史方案 | [已执行](plans/completed/README.md)、[历史](plans/archive/README.md) |
| 实验报告与原始证据 | [results/](results/README.md)、[证据台账](results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 动机画像 | [动机结果](results/motivation/README.md) |
| 组件与能力检查 | [诊断结果](results/diagnostics/README.md) |
| 保存、精简与恢复 | [保存规则](results/AGENTS.md)、[恢复说明](../code/scripts/README.md#实验结果恢复) |

研究内容是数据组织与外部执行、调度和提交控制；图像用于多模态验证，算子代价估计提供共同支撑。
写回采用PostgreSQL与pgvector的COPY加延后索引工程参照，按实际实验终点解释。
设计与运行规则见[AGENTS.md](AGENTS.md)，文献和方法依据见[研究文档](../docs/research/README.md)。
