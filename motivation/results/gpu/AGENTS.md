# motivation/results/gpu/AGENTS.md

本文件继承根、`motivation/AGENTS.md` 与 `motivation/results/AGENTS.md`，只增加真实 GPU-backed
端到端主动机结果规则。

## 目录定位

- 只放真实 GPU-backed 模型端点参与的结果。
- 优先用于回答：为什么数据库 AI 算子的外部执行链路值得优化。
- 当前正式入口看同目录 `README.md`，不要只看本文件判断结果是否更新。

## 必须覆盖

- PostgreSQL 表读取或数据库触发。
- 外部 Python / Ray task / Ray actor 链路。
- GPU-backed model service、Ray Serve、vLLM 或等价真实模型端点。
- 模型请求墙钟时间、queue wait / in-flight、fan-in、writeback。
- 结论边界：哪些能说，哪些不能说。

## 不放这里

- CPU-only 结果。
- fake-model 结果。
- 只证明环境连通的连接验证结果。
- GPU kernel 或数据库内 GPU 查询算子实验。
