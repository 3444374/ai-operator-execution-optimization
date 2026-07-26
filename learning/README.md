# Learning Notes

## 2026-07-26 Ray endpoint 与 actor worker 执行契约

一个 service endpoint 是独立的 HTTP 模型服务地址；一个 Ray actor worker 是向该
地址发送请求的客户端执行单元，两者不能混为一谈。配置并发上界是
`endpoint 数 × 每 endpoint 的 actor worker 数 × 每 actor 最大并发`。HTTP worker
不承载模型，因此 Ray GPU 配额为 0；GPU 由外部 vLLM endpoint 持有。正式完成请求
禁用 Ray 自动重试，避免完成结果被静默重复。CSV 现在显式记录这些配置、拓扑和
逐 worker 提交计数。Python 路径没有 Ray worker，因此 concurrency/CPU 用 0/0.0
表示“不适用”；Ray task 没有 actor worker，因此 actor concurrency 也记 0，但仍
记录实际 task CPU。fake Ray worker 同样接受 CPU、零 GPU 和禁重试配置，只用于
调试。CSV 追加前会核对已有 header，旧 schema 不匹配会拒绝写入而不是把数据写到
错误列。多 GPU 性能仍须用独立 GPU endpoint 验证，当前契约测试不构成多 GPU
性能证据。

## 2026-07-26 动态 flush 与联合搜索结论

`learning/experiment_walkthrough.md` 新增 2026-07-26 章节，解释为什么
queue-adaptive 可以优于 25ms baseline，却未必优于最佳静态 50ms；同时说明
独立拼接与联合搜索在当前单 GPU 实验中为何不可分辨。

## 2026-07-20 指标选择方法论

New learning note:

```text
learning/metric_selection_methodology.md
```

解释为什么从 AI_EMBED 转向 AI_COMPLETE 后，实验观察变量需要从"阶段时延拆分"转向"请求形状 + 服务端压力 + 端到端分布"的四层变量体系。包含每个实验的最低推荐变量集和当前指标盲区。

## 2026-07-18 local vLLM Ray baseline walkthrough

New learning note:

```text
learning/local_vllm_ray_baseline_walkthrough.md
```

Read this when explaining the local `AI_COMPLETE`
`PostgreSQL -> Daft -> Ray -> vLLM` fixed row-batch baseline charts and their
boundaries.

本目录用于把项目实验、代码和术语讲成学习材料。

正式 CSV、严谨结果报告和论文式结论仍放在：

```text
feasibility/results/
motivation/results/
```

`learning/` 负责回答更基础的问题：

- 这个实验为什么要做？
- 数据从哪里来，经过哪些系统，再写到哪里？
- Ray / Arrow / pgvector / batch / actor / fan-in / backpressure / writeback 是什么意思？
- 每个参数在控制什么？
- 每个结果字段怎么读？
- 这个结果对课题下一步有什么用？
- 这个实验不能证明什么？

## 阅读顺序

1. `experiment_walkthrough.md`：按项目推进顺序讲解已经完成的实验。
2. `figures/README.md`：学习用实验图表清单。

## 当前重点章节

| 章节 | 内容 |
|---|---|
| 第 9 节 | GPU-backed 真实 embedding 画像 |
| 第 10 节 | CPU/GPU 对比，以及 `model_service_s` 为什么不能直接当阶段占比 |
| 第 13 节 | 真实 embedding 链路拆分：当前开题动机最应优先学习的一组结果 |
| 第 14 节 | pgai SQL 触发面冒烟验证：真实 SQL 调用 embedding 与 pgvector 写回 |
| 第 14.8 节 | GPU-backed Ray actor 链路中的 pgvector(384) 写回对比 |

## 当前重点图表

项目级图资产统一放在：

```text
figures/
```

当前学习材料、开题报告、PPT、中期汇报和毕业论文应复用同一套图：

- `figures/architecture/`：系统架构图和流程结构图；
- `figures/data/report_main/`：正文主线实验图；
- `figures/data/backup/`：解释场景选择、变量选择和实验边界的支撑图；
- `figures/scripts/`：可复现绘图脚本。

学习材料可以引用 `figures/data/backup/` 中的支撑图讲解实验来源，但不能改变图中实验事实和证据边界。

## 更新规则

每次完成新实验、代码实现或功能测试后，都要同步检查：

- `learning/experiment_walkthrough.md` 是否需要新增讲解；
- `figures/` 是否需要新增或更新项目级图；
- 本 README 的阅读入口是否需要更新。

学习材料可以讲得更通俗，但不能改变正式实验事实。
