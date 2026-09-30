# SemLoom

本项目研究**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。PostgreSQL
负责 SQL、计划、snapshot、权限及查询的取消、错误和结果生命周期；SemLoom 在数据库管理下
组织任务，并调用可替换的外部执行服务。DB-AIEL（Database-Aware AI Execution Layer）
是架构层名称。

## 当前状态

- PostgreSQL 18.3 extension 已接入 SemFilter 与 SemMap 的受限真实语义、公共 provider 接口、
  Map 有界多在途执行和可选图像嵌入路径。实现范围见[代码状态](code/INFRA_STATUS.md)。
- [可选 Daft/Ray 文本传输](experiments/results/postgresql/incremental_transport_20260927/README.md)
  已完成受控 PostgreSQL 检查。[后续真实模型检查](experiments/results/postgresql/transport_real_20260927/README.md)
  完成 12 条 SQL、4,144 次请求；配对结果有 0–2 行输出差异，尚不能认定质量等价或稳定性能收益。
- [完整容量复查](experiments/results/postgresql/m1_full_recheck_20260920/README.md)中，两条路径均未满足持续供给要求，
  尚未选定容量参照。[全局信息方式与图像检查](experiments/results/postgresql/m1_m2_f_real_20260920/README.md)
  分别留下无稳定优势的结果和有限的真实 CLIP 验证。具体数字及未完成项以各结果报告为准。

[新版文本Map主表](experiments/results/postgresql/text_map_main_real_20260930/README.md)已完成68条查询、41,024次真实请求及独立评价。
1024行三次时间中位数：SemLoom18.101秒、direct7.705秒、Ray22.849秒、Daft11.668秒，质量与全部原值并列报告。
前次失败与诊断保留；下一项按[计划](experiments/plans/completed/text_map_matched_comparison.md)定位SemLoom查询专属准备成本。
旧容量实验未选点不作为所有系统比较的前置条件。

## 研究内容

1. 按 token、frame、阶段 work 与局部性组织数据，比较不同组织方式对完整查询和资源使用的影响。
2. 在固定 request/work capacity 下研究提交、服务实例路由及单租户多 Job 调度。

算子代价估计为两项研究内容提供信息，并支持数据库计划比较。文本 `AI_COMPLETE` 是首版主场景；
图像 `AI_EMBED/AI_CLASSIFY` 用于跨模态验证。研究问题与执行顺序见[项目总纲](PROJECT_OUTLINE.md)。

## 阅读入口

| 需求 | 文件 |
|---|---|
| 两分钟了解方向和近期工作 | [当前方向速览](overview/current_direction_and_plan.md) |
| 查文件、目录职责与历史材料 | [项目导航](PROJECT_INDEX.md) |
| 查项目长期规则与术语 | [AGENTS.md](AGENTS.md)、[CONTEXT.md](CONTEXT.md) |
| 核对实现与运行入口 | [代码状态](code/INFRA_STATUS.md)、[代码目录](code/README.md)、[脚本说明](code/scripts/README.md) |
| 查实验计划、证据和失败记录 | [实验计划](experiments/plans/README.md)、[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 理解相关系统与论文 | [知识库](research/knowledge_hub.md) |
| 追溯历史设计与实施计划 | [历史设计记录](docs/README.md) |
| 准备机器 | [运行手册](deploy/runtime/README.md) |
| 追溯已结束的开题材料 | [归档入口](opening/README.md) |

原始结果和失败记录保留在各实验目录。运行条件与结论以结果报告为准；机器准备和真实实验分别遵守
[运行手册](deploy/runtime/README.md)及目标计划。
