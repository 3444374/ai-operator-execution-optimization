# motivation/plans/AGENTS.md

本目录保存研究路线、场景定义、workload 设计和实验计划。

## 可以放

- AI 算子场景和 workload 设计。
- GPU-backed E2E 主动机实验计划。
- PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线。

## 不放

- 原始 CSV。
- 已完成实验的正式结果分析。
- 组件可行性脚本。

## 规则

- 计划必须区分“主动机实验”“baseline”“消融”“环境检查”。
- 不把 CPU/fake 计划写成真实 GPU 链路结论。
- 修改计划后同步检查 `motivation/README.md`、`PROJECT_INDEX.md` 和 `overview/current_direction_and_plan.md`。

