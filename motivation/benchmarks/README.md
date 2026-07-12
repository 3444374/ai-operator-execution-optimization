# Motivation Benchmarks

本目录保存动机实验脚本。当前脚本主要服务历史 fake/CPU 预研、计时边界验证和机制消融设计，不能直接外推为真实 GPU-backed 链路结论。

| 文件 | 默认输出 | 作用 |
|---|---|---|
| `fake_embed_pipeline.py` | `motivation/results/fake_cpu/fake_embed_pipeline.csv` | fake `AI_EMBED(text)` 历史预研 |
| `workload_matrix.py` | `motivation/results/fake_cpu/workload_matrix.csv` | 三类 AI 算子场景对比 |
| `granularity.py` | `motivation/results/fake_cpu/granularity.csv` | task/object/fan-in/operator invocation 归因 |
| `backpressure.py` | `motivation/results/fake_cpu/backpressure.csv` | 模型服务 queue wait / token backlog / in-flight 反压模拟 |

GPU-backed 主动机实验可以复用这里的计时思想，但应新增或迁移到真实链路脚本，结果放 `motivation/results/gpu/`。

