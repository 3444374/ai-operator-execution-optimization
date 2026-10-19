# 组件测量

本目录保留真实 Arrow／Ray 组件测量入口，证据用途由对应报告说明。
依赖清单见[component_benchmarks.txt](../../requirements/component_benchmarks.txt)，机器准备按[运行手册](../../../deploy/runtime/README.md)执行。

| 脚本 | 内容 | 默认输出目录 |
|---|---|---|
| `ray_small_task_benchmark.py` | Ray小任务开销 | `experiments/results/diagnostics/` |
| `ray_object_transfer_benchmark.py` | Ray对象传输 | 同上 |
| `arrow_recordbatch_serialization_benchmark.py` | Arrow批次序列化 | 同上 |
| `ray_many_objects_benchmark.py` | 固定总量下的对象数量与fan-in | 同上 |
| `ray_arrow_fanout_fanin_benchmark.py` | Arrow批次fan-out/fan-in | 同上 |

六个早期 CPU 模拟及专用汇总入口已退出当前目录，完整实现与命令见
[固定历史版本](https://github.com/3444374/ai-operator-execution-optimization/tree/b8b5f3d11576e4c06953e38732fdd3d010807a85/code/scripts/benchmarks)。
复算这些历史输出时使用该提交的完整源码。原始 CSV、分析及绘图读取保持在
[动机结果](../../../experiments/results/motivation/fake_cpu/README.md)和
[组件结果](../../../experiments/results/diagnostics/README.md)。
