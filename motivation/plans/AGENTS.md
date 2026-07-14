# motivation/plans/AGENTS.md

本目录保存动机测试相关的路线、场景定义、workload 设计和实验计划。它服务于“为什么这个课题值得做”，不承担完整论文实验规划。

## 可以放

- AI 算子场景和 workload 设计。
- GPU-backed E2E 主动机实验计划。
- PostgreSQL / worker / Ray / GPU model service / writeback 集成路线，用于跑通动机画像。
- 为开题和课题方向服务的 baseline 选择、变量选择和证据边界说明。

## 不放

- 原始 CSV。
- 已完成实验的正式结果分析。
- 组件可行性脚本。
- 三项研究内容的完整实验矩阵。
- 已经进入方法验证、优化收益验证或小改动调优的实验计划；这些应放到 `experiments/plans/`。

## 规则

- 计划必须区分“主动机实验”“baseline”“谨慎消融”“环境检查”。
- 不把 CPU/fake 计划写成真实 GPU 链路结论。
- 不把动机测试计划写成完整论文实验规划。
- 修改计划后同步检查 `motivation/README.md`、`PROJECT_INDEX.md`、`PROJECT_OUTLINE.md` 和 `overview/current_direction_and_plan.md`。
