# feasibility/results/AGENTS.md

本目录只放可行性结果、连接验证、smoke CSV 和自动报告。

## 可以放

- 组件级 benchmark CSV。
- 数据库连接验证、脚本 dry-run、small smoke run 结果。
- 自动生成的 feasibility report。

## 不放

- GPU-backed E2E 主动机结果。
- 数据库 AI 算子系统画像、瓶颈定位和优化收益报告。
- 可作为论文主证据的端到端 baseline。

这些结果应放在 `motivation/results/`。

## 规则

- 每个报告必须说明“能证明什么”和“不能证明什么”。
- CSV 文件名应对应脚本或验证对象。
- 若旧结果只是历史背景，保留但不要在新计划里当作主证据。

