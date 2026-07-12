# GPU-Backed E2E Results

本目录预留给生产式 GPU-backed 端到端主动机结果。

第一组优先补：

```text
ai_embed_profile.md
ai_embed_profile.csv
```

结果应覆盖：

- PostgreSQL 表或 SQL 触发；
- 外部 worker / Ray task / Ray actor；
- GPU-backed embedding endpoint、Ray Serve 或 vLLM；
- queue wait、in-flight、GPU utilization 或模型服务吞吐；
- fan-in / consolidation；
- PostgreSQL + pgvector 或 Lance 写回；
- 不能声称的结论。

这里的结果优先用于回答“为什么外部服务链路值得优化”。

