# 组件测量与历史模拟

[IMLane监督计数隔离](probe_send_progress.py)仅使用Mac本地账本副本和四worker localhost替身，默认打印计划；
完整配对、索引写入负担、闭合账本和锁边界见[同题报告](../../../experiments/results/diagnostics/imlane_progress_scan_20261016/README.md)。
它不启动作者runtime或模型，不把进度读CPU改善解释为完整查询收益。

[可选 Ray 图诊断](probe_optional_ray_timing.py)默认只打印有限计划，`--execute` 才执行四个独立新 owner、
0／2520 行副本和总计最多 8 次 localhost 响应；不启动模型、不改原账本。环境、lease、停止条件及结果见
[计时报告](../../../experiments/results/diagnostics/optional_execution_timing_20261014/README.md)。

[文本Ray清理配对核验](probe_native_ray_cleanup.py)默认只打印固定源码的四图计划；显式执行只使用
已核验的Ray2.56.1／Arrow24.0.0环境、新私有输出及共享lease，4 CPU／0 GPU、每图两行、最多8次localhost POST。
实际close／shutdown回调返回后才注入故障，ON另外注入一次writer错误；真回调失败、进程出生身份未知或强制清理均不算通过。
源码清单、执行前审阅、60秒无进度保护及[限定软件结果](../../../experiments/results/diagnostics/optional_execution_timing_20261014/README.md#ray-cleanup-sdk)
保持完整来源；没有累计wall deadline、模型请求或性能矩阵。

[本轮私有发送hook](sembench_pre_send_ledger.py)配合[持久计数组件](../../src/experiments/send_counts.py)，没有模型启动或预算授权功能；部署导入名、指纹与离线迁移要求见[脚本入口](../README.md#sembench-send-accounting)。原件、本地配对及复算入口统一在[Q2补测主报告](../../../experiments/results/diagnostics/native_sembench_model_dual_supplement_20261012/README.md#retained-stage-analysis)。

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
