# validation/AGENTS.md

本目录维护可行性 benchmark、组件级 microbenchmark、环境验证和连接验证结果。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 用最小实验验证技术路线是否有可观察系统信号。
- 记录 Phase 0 组件级实验、CSV 结果和自动分析报告。
- 记录 PG18.4 连接验证和脚本 dry-run 等环境可用性结果。

## 放什么

放在这里：

- Ray small task / object transfer / many-object fan-in。
- Arrow serialization。
- shuffle simulation。
- PG18.4 connection validation。
- PG18.4 smoke / dry-run CSV。
- 自动生成的 feasibility report。

不放在这里：

- 数据库 AI 算子端到端动机测试正式结果。
- PG18.4 系统画像、瓶颈定位和可优化点分析。
- 需要用于开题动机论证的正式端到端结果。

这些应放在 `motivation/results/`。

## 当前证据

- Ray small task 不是当前最强瓶颈。
- Arrow IPC 本身不是当前明显瓶颈。
- Ray many-object fan-in 有放大。
- Arrow RecordBatch `N upstream -> P downstream` fan-in 是当前贴近课题的组件级证据。
- PG18.4 连接验证已完成，结果见 `validation/results/pg18_4_connection_validation.md`。

## 代码与结果规则

- 新 benchmark 必须有明确问题、命令、CSV 输出和结果解释。
- 不混淆数据生成、序列化、`ray.put`、fan-in、writeback 等时间边界。
- warm-up 要忽略或明确标注。
- 连接验证只能证明系统可用，不能用于性能收益结论。
- 正式系统画像和动机结果优先引用 `motivation/results/`。
