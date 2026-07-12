# Fake/CPU Historical Results

本目录保存 fake/CPU 历史预研结果。

| 文件 | 作用 |
|---|---|
| `analysis.md` | fake/CPU 历史预研综合分析 |
| `fake_embed_pipeline.csv` | fake `AI_EMBED(text)` 端到端预研 |
| `workload_matrix.csv` | 三类 AI 算子 fake 场景对比 |
| `granularity.csv` | task/object/fan-in/operator invocation 归因 |
| `backpressure.csv` | 模型服务 queue wait / token backlog / in-flight 反压模拟 |

这些结果只用于追溯历史信号、调试计时边界和设计后续 GPU-backed 消融，不能替代 GPU-backed 主动机。

