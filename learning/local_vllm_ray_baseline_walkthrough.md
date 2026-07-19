# Local vLLM + Ray Baseline 图表讲解

本文讲解 2026-07-18 本地 `AI_COMPLETE` baseline 的支撑图。它们用于理解当前链路和后续优化的对照起点，不是优化策略结论。

## Source Order Note

`--source-order doc_id` is the offline throughput mode: PostgreSQL rows are
scanned in stable `doc_id` order before Daft organization. This is the right
default for fixed-row versus token-budget organization baselines because all
rows are already available.

`--source-order arrival_time` is the arrival-aware service mode: PostgreSQL
rows are scanned by `arrival_time_s NULLS LAST, doc_id` before Daft organization
and Ray submission. This is the right setup for K_max, queue-adaptive flush,
queueing, and backpressure experiments because it preserves the workload
arrival rhythm.

## 1. 这组结果是什么

当前链路是：

```text
PostgreSQL documents
  -> DaftPostgresSource
  -> DaftOrganizer
  -> Ray task / Ray actor
  -> vLLM OpenAI-compatible completion endpoint
  -> no writeback
```

模型是本地 vLLM 服务的 `Qwen2.5-1.5B-Instruct`。workload 来自本地归一化后的 ShareGPT prompt 和 BurstGPT trace 元数据。实验使用固定行数 batch，batch size 为 1、2、4、8、16、32。

这一步的意义是建立后续优化前的本地对照：在不做 token-aware、length-aligned、queue-adaptive flush 的情况下，先看固定行 batch 经过 Daft 和 Ray 后的吞吐、端到端耗时、阶段耗时和模型服务请求数。

## 2. 固定行 Batch Baseline

这一组图已经拆成四张独立图，而不是塞进一个 dashboard。每张图只回答一个问题，并且前四张都覆盖 6 个 batch 档位。

```text
figures/data/backup/b07_local_vllm_ray_throughput.png
figures/data/backup/b08_local_vllm_ray_e2e_time.png
figures/data/backup/b09_local_vllm_ray_task_stage_timing.png
figures/data/backup/b10_local_vllm_request_count_inflight.png
figures/data/backup/b11_local_vllm_token_tail_performance.png
```

吞吐图看固定行 batch size 对 rows/s 的影响。Ray task 从 batch=1 的约 36.1 rows/s 上升到 batch=16 的约 259.8 rows/s，batch=32 略降到约 246.0 rows/s。Ray actor 从 batch=1 的约 9.3 rows/s 上升到 batch=32 的约 59.1 rows/s。

端到端耗时图看同一批数据的 e2e time。使用 log y 轴是因为不同 batch size 之间差距较大。Ray task 从 batch=1 的约 3.55s 降到 batch=16 的约 0.49s，batch=32 回升到约 0.52s。Ray actor 从约 13.79s 降到约 2.17s。

阶段耗时图只看 Ray task 路径。这里不是把所有阶段解释成互斥占比，而是用来定位主要控制点：operator wall 随 batch size 明显下降，source fetch 约 0.064s 左右，Daft organize、Ray submit、fan-in 都比较小。当前固定行 baseline 的主要变化来自模型调用粒度和调用次数，而不是 Daft collect 或 fan-in。

请求数图看模型服务调用次数。总行数固定时，batch size 从 1 增加到 32，请求数从 128 次降到 4 次。这解释了为什么吞吐会随 batch 变大而提高：不是模型突然变快，而是 Python/Ray/HTTP/model-service invocation 的固定开销被摊薄。

token 图补上了真正的动机缺口：固定行 batch 只能控制每个请求有几行，不能控制每个请求有多少 token。batch=8 的 latency probe 显示，同样 8 行的模型请求，batch token 可以从约 335 到 4663，跨度约 13.9 倍。这个范围说明 row batch 不是计算量 batch，后续需要 token-budget、length-aligned 这类 workload-aware 组织策略。

## 3. Latency Probe

图文件：

```text
figures/data/backup/b12_local_vllm_latency_probe_breakdown.png
figures/data/backup/b12_local_vllm_latency_probe_breakdown.svg
```

这张图只针对 Ray task、batch=8 的 latency probe，使用 3 次 formal repeat。它的作用不是画完整性能曲线，而是验证当前 harness 能把客户端 batch latency 和 vLLM server-side latency 指标分开。

图左侧是客户端看到的 batch service latency，P50、P95、P99 都在约 0.24s 附近。这里的 P95/P99 来自批级观测，不是 vLLM 内部 histogram 的 per-request 分位数。

图右侧是 vLLM `/metrics` 提供的均值拆分：queue time 接近 0，prefill 约 0.025s，decode 约 0.184s，inference 约 0.210s，e2e request latency 约 0.225s。这个结果说明，在当前小模型和本地低并发设置下，服务端排队不是主要部分，decode/inference 时间更显著。

## 4. 能说明什么

这组图可以说明：

- `PostgreSQL -> Daft -> Ray -> vLLM` 的文本 `AI_COMPLETE` baseline 已经跑通；
- 数据确实从 Daft source/organizer 进入后续 Ray 执行；
- 固定行 batch 会显著改变模型服务调用次数，进而影响吞吐和端到端耗时；
- 当前脚本已经能采集 batch-level latency 和部分 vLLM server-side metrics。

## 5. 不能说明什么

这组图不能说明：

- token-budget、length-align、prefix-aware grouping 已经有效；
- queue-adaptive flush 或动态并发控制已经有效；
- Ray actor 一定比 Ray task 差；
- batch=16 或 batch=32 是通用最优；
- 这是 PostgreSQL 18.3 内部平台结果；
- 这是包含写回的端到端结果。

## 6. 后续怎么用

下一步做优化实验时，建议沿用同一套图表结构：

- 继续画吞吐和端到端耗时；
- 继续画阶段耗时，用来判断优化到底影响了哪一段；
- 请求数图可以扩展为平均 tokens/request、batch token 方差、排队长度等；
- latency probe 后续应增加更多 repeat 或 per-request trace，再用于严肃的 P95/P99 分析。

这样 baseline、token-aware 数据组织、queue-adaptive 调度之间可以在同一套视觉语言下对比，避免每次实验换一种图导致结论不可比。
