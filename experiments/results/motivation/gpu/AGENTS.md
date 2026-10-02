# motivation/results/gpu/AGENTS.md

继承上级规则。当前真实 GPU-backed 端到端结果见 [README.md](README.md)。

- 结果必须包含真实 GPU 模型端点；CPU-only、fake-model、连接 smoke、GPU kernel 或库内 GPU 查询算子实验另归对应目录。
- 主动机报告覆盖 PG 表读取或数据库触发、外部 Python/Ray task/actor、实际模型服务，记录模型请求
  墙钟时间、queue wait/in-flight、fan-in 和 writeback，说明观察支持什么以及不能推出什么。
