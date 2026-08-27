# 代码设计与实施记录

本目录保存已经形成代码或实验基础设施的设计规格和实施计划。它是历史追溯面，不是当前执行
入口，也不是研究证据。

## 阅读规则

- 想了解当前代码结构：读 `../code/README.md`。
- 想了解当前实现完成度：读 `../code/INFRA_STATUS.md`。
- 想运行脚本：读 `../code/scripts/README.md`。
- 想核对方法是否已被实验支持：读
  `../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`。
- 只有在追溯某项实现为何这样设计时，才进入本目录的 `superpowers/`。

## 目录

| 路径 | 内容 | 状态 |
|---|---|---|
| `superpowers/specs/` | 设计规格、接口与约束 | 历史记录 |
| `superpowers/plans/` | 分步实施计划 | 历史记录 |

文件名中的日期表示设计当时的上下文。文中出现的“当前”“下一步”或分支状态均按该日期读取，
不得覆盖今天的 `PROJECT_OUTLINE.md`、实验合同或部署 runbook。
