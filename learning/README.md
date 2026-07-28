# Learning Notes

## 2026-07-28 双 4090 7B replenish 配置诊断

本轮现场数据中的 `replenish_static_k8_2gpu` 不是 request-level
replenishment：命令设置了 `ray_batch_rows=1`，但结果字段仍是
`submission_granularity=batch`。因此 1024 行被组织成 1024 个单行 batch，
`token_budget=8192` 没有机会装入多行。K=8 又只允许全局 8 个单行请求，
而 batch K=32 平均每批约 3 行，两个 K 不代表相同 offered load。

正确实验应保留合理的 row cap 与 token budget，让它们记录 packing/flush
边界，再用 `submission_granularity=request` 展开完整行请求。request K 应按
请求数与 batch baseline 的实际行数匹配，先比较 K64/K96，而不是直接复用
batch K8。当前 7B warm-up 只能用于定位配置问题，不能作为 replenish 策略优劣证据。

HOL-age 的 3 秒拥塞阈值也低于本轮约 4–5 秒的正常 batch 服务时间，因此会把
正常执行年龄当成拥塞并把窗口压向下限。需要先用静态配置校准正常服务时间，再
决定阈值或更换为不混淆 service age 与 queue delay 的信号。

vLLM 的 `estimated_flops_per_gpu_total` 是 per-GPU counter。多 endpoint 采集时，
工作量 counters 仍求和，KV 压力取最大值，但 per-GPU FLOPs 必须在 endpoint
之间取均值后再除以单卡峰值；相加会把双卡 MFU 高估约两倍。

## 2026-07-28 双 endpoint 指标与并发语义

多 endpoint 实验里的 `max_inflight` 是整个调度器的 submission 上限，不是
每个 endpoint 各自的上限。因此，单 endpoint 的 K=16 若要做保持“每卡 K=16”
的双 endpoint 诊断，首先应检查全局 K=32，而不能直接复用 K=16。一个
submission 还可能包含多行 prompt；只有整批 HTTP 响应完成后，该 submission
才释放 admission slot，这与真正的 request-level continuous replenishment
仍有区别。现在 arrival replay 可显式选择
`--submission-granularity request`：token-budget 与 flush 仍决定完整请求的
组织边界，但关批后每行作为一个完整 HTTP 请求提交，任一请求完成都会立即释放
一个 slot 并持续补位。该模式下 K 表示“请求数”，默认 batch 模式下 K 表示
“多行 submission 数”，两种模式不能只按相同 K 数值直接比较。

`least_queued` 现在把调度器已提交但尚未完成的 endpoint-local submission
计入负载，不再对静态全零拓扑反复选择第一个 endpoint。双 endpoint 采集应使用
`--model-metrics-urls` 传入两个 vLLM Prometheus 地址，否则单地址 counters
只能代表一个 endpoint。GPU 利用率取所有可见 GPU 的均值，显存和功耗取系统
总量；这些口径用于解释双卡系统，不应和旧的“仅第一张 GPU”记录直接混算。

动态控制器的 `fresh` 现在表示“新采样且尚未被控制决策消费”，而不只是
“采样尚未超时”。同一个 Prometheus 快照不会在调度器高速循环中重复触发
AIMD/EWMA/PID 更新；HOL-age AIMD 也按配置的采样周期更新。需要注意，现有
HOL-age 仍是最老 in-flight submission 的年龄，不等同于纯粹的提交前排队时间。

同日双 4090 单次诊断中，请求级 K=64 为约 15.20 rows/s、6784 tokens/s，
K=128 为约 13.89 rows/s、6217 tokens/s，均未超过此前 batch 级 K=32 的
约 18.71 rows/s、8317 tokens/s。这只能作为调参信号：独立 HTTP/Ray task
开销和过高并发可能抵消持续补位收益；在完成重复、交错的 K 扫描前，不能声称
请求级模式提升或降低了总体性能。

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

轮转状态的生命周期必须与实验 run 一致，而不是与单次数据库 fetch chunk 一致。
因此 endpoint 内 actor worker 与 legacy endpoint 轮转都只在 run 初始化时创建；
每个 chunk 只上报自己的提交增量。job 一旦创建，后续 Ray 初始化、提交或写回异常
都会尽力写入 `failed` 终态，同时保留原异常。主 CSV 的旧 schema 也会在数据库和
GPU 工作前被拒绝；K_max runner 使用新的 `20260726` 默认文件，历史结果保持只读。

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
