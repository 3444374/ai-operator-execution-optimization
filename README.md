# SemLoom

研究 **AI 语义算子与多模态数据任务的物理执行、数据组织与调度优化**。
SemLoom 面向 LOTUS 等语义 API 与数据库前端；各前端拥有语义与查询生命周期，执行层组织合法工作。
近期问题与推进顺序由[项目大纲](PROJECT_OUTLINE.md)维护。

## 内容与文档职责

| 要查什么 | 唯一维护位置 |
|---|---|
| 长期行为与领域术语 | [AGENTS.md](AGENTS.md)、[CONTEXT.md](CONTEXT.md) |
| 研究内容与近期顺序 | [项目大纲](PROJECT_OUTLINE.md) |
| 已接纳能力与未验证范围 | [代码状态](code/INFRA_STATUS.md) |
| 实现、接口与命令 | [代码入口](code/README.md)、[脚本入口](code/scripts/README.md)，再进入对应模块 |
| 架构与实验设计 | [方案入口](experiments/plans/README.md)、[对照规范](experiments/plans/reference/对照规范.md) |
| 运行身份、共享证据与结论 | [证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)、[结果导航](experiments/results/README.md)；完整数字、重复与失败只在主报告维护 |
| 文献与方法依据 | [研究入口](docs/research/README.md)、[研究定位](docs/research/研究定位.md)、[优化方法依据](docs/research/优化方法依据.md) |
| 环境与数据 | [运行手册](deploy/runtime/README.md)、[数据来源与导入](data/README.md) |
| 图与历史材料 | [图资产](figures/README.md)、[历史文档](docs/README.md)、[项目日志](PROJECT_LOG.md) |

目录变化只更新直接入口；事实变化只更新拥有该事实的正文和实际受影响的引用，具体要求见根规则。
旧目录、旧方案和原知识总汇的迁移表保留在[精简前导航](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/PROJECT_INDEX.md)，按历史日期查阅。
