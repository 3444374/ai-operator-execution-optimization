# 组件测量与历史模拟

原组件和动机命令入口统一在这里。它们用于组件测量、fake/CPU历史复算及机制诊断，证据用途由对应报告说明。
依赖清单见[component_benchmarks.txt](../../requirements/component_benchmarks.txt)，机器准备按[运行手册](../../../deploy/runtime/README.md)执行。

| 脚本 | 内容 | 默认输出目录 |
|---|---|---|
| `ray_small_task_benchmark.py` | Ray小任务开销 | `experiments/results/diagnostics/` |
| `ray_object_transfer_benchmark.py` | Ray对象传输 | 同上 |
| `arrow_recordbatch_serialization_benchmark.py` | Arrow批次序列化 | 同上 |
| `shuffle_simulation_benchmark.py` | shuffle模拟 | 同上 |
| `ray_many_objects_benchmark.py` | 固定总量下的对象数量与fan-in | 同上 |
| `ray_arrow_fanout_fanin_benchmark.py` | Arrow批次fan-out/fan-in | 同上 |
| `fake_embed_pipeline.py` | fake embedding链路 | `experiments/results/motivation/fake_cpu/` |
| `workload_matrix.py` | 三类算子场景模拟 | 同上 |
| `granularity.py` | 任务、对象与调用数量归因 | 同上 |
| `backpressure.py` | 生产与消费压力模拟 | 同上 |
| `analyze_results.py` | 从保留CSV生成组件分析 | `experiments/results/diagnostics/` |

例如只读取已有数据生成分析：

```sh
python code/scripts/benchmarks/analyze_results.py --output /path/to/private/component-analysis.md
```

使用明确的新输出路径。重复、模型与平台身份以原CSV和报告为准；模拟数据用于理解机制。
