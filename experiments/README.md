# Research Experiments

本目录是正式研究实验入口，用于规划、运行和记录三项研究内容的优化实验与消融实验。它不同于 `motivation/`：动机测试回答“为什么这个课题值得做”，本目录回答“提出的方法或调优是否真的有效”。

## 当前状态

正式研究实验尚未全面开始。当前已有 GPU-backed `AI_EMBED` 链路画像属于动机证据，仍放在 `motivation/results/gpu/`。后续在该链路上做 384 维 pgvector 写回、worker-side writeback、bounded in-flight、Ray Serve / vLLM、三类 workload 对照等方法验证时，应逐步登记到本目录。

## 目录分工

| 路径 | 作用 |
|---|---|
| `plans/` | 正式研究实验计划，按三项研究内容组织 baseline、变量、消融和指标 |
| `results/` | 正式研究实验结果、小改动调优记录和结论边界 |

## 三项研究内容对应实验

| 研究内容 | 主要实验问题 | 初始候选实验 |
|---|---|---|
| 数据组织与批处理构造 | batch、partition、operator invocation、object 合并如何影响端到端性能 | fine/coalesced、partition 数、object 合并、不同输出规模 |
| GPU 服务状态感知调度与反压 | Ray actor/task、endpoint routing、bounded in-flight 如何影响 queue wait、operator wall 和 GPU utilization | single/dual endpoint、actor pool、bounded/unbounded in-flight、Ray Serve / vLLM |
| 结果汇聚与持久化协同 | fan-in、worker writeback、sink 类型如何限制端到端收益 | driver fan-in 写回、worker-side writeback、queue worker、JSON text、pgvector(384)、Lance / Parquet |

## 结果记录要求

每个结果至少包含：

1. 对应研究内容和研究问题。
2. 实验链路与运行命令。
3. 参数、指标和 CSV / 日志路径。
4. baseline、优化方案和消融设置。
5. 真实结果和主要数字。
6. 能说明什么、不能说明什么。
7. 下一步需要补的验证。

图表统一放在 `figures/`。本目录只引用图，不长期保存图副本。
