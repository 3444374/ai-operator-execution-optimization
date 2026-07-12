# motivation/results/gpu/AGENTS.md

本目录保存生产式 GPU-backed 端到端主动机结果。

## 必须覆盖

- 数据库触发或数据库表读取。
- 外部 worker / Ray task / Ray actor。
- GPU-backed model service、Ray Serve、vLLM 或等价真实模型端点。
- queue wait、in-flight、模型服务时间、fan-in/consolidation、writeback。

## 规则

- 这里的结果优先用于回答“为什么外部服务链路值得优化”。
- 不研究 GPU kernel 本身。
- CPU-only 或 fake-model 结果不能放这里。

