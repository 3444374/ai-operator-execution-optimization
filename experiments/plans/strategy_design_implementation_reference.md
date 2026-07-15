# 策略设计与系统实现参考

整理日期：2026-07-15

用途：把 Ray、vLLM / Ray Serve / Triton、GPU 数据放置、数据库 AI 算子等文献和系统资料中的可借鉴机制，沉淀为本课题后续实验设计与原型实现的参考。本文不是最终方法章节，也不声称这些机制已经在本项目中全部实现。

## 1. 当前策略口径

当前建议采用三层策略，而不是单一的 runtime controller：

```text
Three-layer upstream execution strategy

计划层：数据库侧数据组织
  输入：行数估计、文本长度分布、AI 算子类型、历史 profile / sweep
  输出：batch_size, partition_count, object_merge, 初始资源配置

运行层：Ray / 服务入口调度
  输入：queue wait, endpoint backlog, actor load, GPU utilization, E2E metrics
  输出：K_max, routing policy, backpressure, actor pool / resource config

服务端层：推理 micro-batching
  输入：waiting requests, token/shape budget, wait timeout, compatibility key
  输出：inference micro-batch

Guardrail：P99、throughput、writeback ratio、质量指标
```

关键边界：

- 不重切数据库侧已经物化或已经排队的 `RecordBatch`。
- 动态 batch 放在模型服务侧尚未执行的请求队列中，不放在数据库侧已切分 batch 上。
- 不声称发明 vLLM continuous batching、Ray scheduler 或 Triton dynamic batcher；本文借鉴这些机制，把它们放到数据库 AI 算子外部执行链路中协调验证。

## 2. 文献与机制映射

| 来源 | 机制 | 支持本课题的哪一层 | 可做成的实验变量 | 边界 |
|---|---|---|---|---|
| Ray OSDI 2018 | task / actor、dynamic task graph、distributed scheduler、object store、resource-aware scheduling | 计划层 + 运行层 | task granularity、actor pool size、resource requirement、placement/locality、object count、fan-in shape | 不改 Ray 内部 scheduler，只做应用层 admission/routing |
| Ray Data / Ray Serve | `map_batches`、concurrency、dynamic request batching、routing、autoscaling | 三层都有接口参考 | batch size、concurrency、`max_batch_size`、wait timeout、replica count | 官方接口存在不等于本项目一定有收益，必须用 GPU-backed profile 验证 |
| vLLM / Orca | continuous batching、iteration-level scheduling、吞吐-延迟曲线、SLO 约束 | 服务端层 | max tokens、request admission、TTFT/TPOT/P99、serving throughput | 作为强 baseline 或机制借鉴，不写成本文原创 |
| Triton Inference Server | dynamic batcher、preferred batch size、queue delay | 服务端层 | max batch size、preferred batch size、max queue delay | 更适合模型服务 baseline，不直接解决数据库侧数据组织 |
| Sarathi-Serve / DistServe / Mooncake / SGLang | phase splitting、KV/prefix reuse、SLO-aware routing | 服务端层 + 运行层 | token-aware routing、prefix-aware routing、long/short request isolation | 主要服务 `AI_COMPLETE`，对 `AI_EMBED` 只作为边界和扩展 |
| GPU 数据库 / GPU-resident 结构 | 数据搬运、materialization、GPU 内存驻留、算子融合 | 计划层 | 是否搬到 GPU、批量大小、列式表示、object/materialization 数量 | 本课题不做传统 GPU 查询算子 kernel 优化 |
| Cortex AISQL / GaussML / Galois / LEADS / NeurDB | AI 算子语义影响执行计划和代价估计 | 计划层 + 端到端评价 | 按 `AI_EMBED` / `AI_FILTER` / `AI_COMPLETE` 分 workload | 多数系统不暴露外部链路细节，不能过度类比 |

## 3. 可以落地的设计点

### 3.1 计划层：workload-aware 数据组织

目标：在执行前选择合理的数据组织，避免过多小 task、小 object 和不合适的 GPU 请求粒度。

可观测信号：

- 行数估计；
- 文本长度分布采样；
- AI 算子类型：`AI_EMBED`、`AI_FILTER`、`AI_CLASSIFY`、`AI_COMPLETE`；
- 输出大小：embedding 维度、过滤选择率、生成 token 边界；
- 历史 profile / 参数 sweep 结果。

可调变量：

- `batch_size`；
- `partition_count`；
- `object_merge` / coalescing；
- 初始 actor pool / resource config。

实现边界：

- 在构造 `RecordBatch` / Ray task 输入队列前确定；
- 一次执行中不重切已构造 batch；
- 如果后续要研究 adaptive repartition，需要单独作为新机制证明开销收益，不放进当前主方案。

### 3.2 运行层：服务入口 admission / routing / backpressure

目标：控制数据库侧请求进入模型服务的节奏，避免 GPU 吃不满或 endpoint queue 被打爆。

可观测信号：

- in-flight request count；
- endpoint backlog / queue wait；
- actor load；
- GPU utilization；
- E2E latency / P99；
- writeback ratio。

可调变量：

- `K_max`：最大在途请求数；
- `routing policy`：round-robin、least-queued、GPU-util-aware、token-aware、prefix-aware；
- `backpressure`：队列积压时降低提交速率；
- actor pool size / endpoint replica count。

实现边界：

- 不改 Ray scheduler；
- 在应用层 driver / gateway / actor pool manager 中实现；
- 只影响尚未提交或尚未执行的请求。

### 3.3 服务端层：dynamic micro-batching

目标：借鉴 vLLM / Ray Serve / Triton，将短时间内到达且兼容的请求合成一次模型 forward，提高 GPU 利用率，同时控制 tail latency。

可观测信号：

- waiting requests；
- per-request token length / input shape；
- GPU batch utilization；
- queue wait；
- P99 / TTFT / TPOT；
- timeout / cancellation。

可调变量：

- `max_batch_size`；
- `max_tokens` 或 shape budget；
- `batch_wait_timeout` / max queue delay；
- compatibility key：模型、维度、token/shape bucket、prefix/cache affinity。

实现边界：

- 动态 batch 不等于数据库侧 RecordBatch 重切；
- 对 `AI_EMBED`，可先按样本数和文本长度 bucket 做 micro-batch；
- 对 `AI_COMPLETE`，再考虑 token-aware / prefix-aware / KV cache 相关策略；
- 强 baseline 应优先使用 vLLM / Ray Serve / Triton 的现有机制。

## 4. 系统优化蓝图

这一节把文献机制转成系统设计点。原则是：能复用成熟系统就复用，能在应用层控制就不改底层调度器，所有优化都必须能被指标验证。

### 4.1 Workload Profiler：把 SQL 算子变成可调度 workload

借鉴来源：

- DB AI 算子系统强调 AI 算子语义会改变执行代价；
- GPU serving 系统强调 token/shape/request shape 会改变 batching 和 latency；
- Ray/Data 系统强调 task 粒度和 object 数量会影响调度与数据移动。

系统设计：

```text
SQL AI operator
  -> workload profile
      operator_type: AI_EMBED / AI_FILTER / AI_COMPLETE
      row_count_estimate
      text_length_histogram
      output_shape: embedding_dim / selectivity / token_bound
      sink_type: PostgreSQL / pgvector / Lance
```

用途：

- 给计划层选择 `batch_size`、`partition_count`、`object_merge`；
- 给运行层选择初始 `K_max` 和 routing；
- 给服务端层选择 dynamic micro-batching 的 compatibility key。

最小实现：

- 先只采样 `row_count`、文本长度 P50/P95、算子类型；
- 后续再加 token 估计、selectivity 估计和 prefix/cache 信息。

放弃条件：

- 如果 workload profile 与最优配置相关性很弱，计划层退化为固定最优配置；不要强行做复杂 cost model。

### 4.2 Plan-time Data Organizer：先解决数据库侧数据组织

借鉴来源：

- Ray task 粒度和 object store 机制；
- Ray Data / Daft 的 batch、partition、shuffle/coalescing 思想；
- GPU 数据库工作中关于 materialization 和数据搬运的边界。

系统设计：

```text
profile + rule table / sweep table
  -> batch_size
  -> partition_count
  -> object_merge / coalescing
  -> initial actor pool / resource hints
```

优化目标：

- 降低过多小 task / 小 object；
- 避免 fan-in 过宽；
- 让模型服务收到足够大的请求，但不把写回批量推到失控；
- 不在运行时重切已经构造的 `RecordBatch`。

实现建议：

- P0：固定候选集合，例如 `batch_size ∈ {32, 64, 128, 256}`，`partition_count ∈ {1, 2, 4, 8}`；
- P1：按 workload profile 查表选择配置；
- P2：如果规则不稳定，再考虑轻量 cost model，不直接做 learned optimizer。

验证指标：

- `RecordBatch count`；
- Ray task count；
- object count；
- fan-in width；
- model request count；
- writeback batch count；
- E2E latency / throughput。

### 4.3 Ray Admission Controller：用 Ray 思想做应用层门控

借鉴来源：

- Ray 的 dynamic task graph、local/global scheduler、resource-aware scheduling；
- 推理服务的 admission control 和 backpressure。

系统设计：

```text
submission gate
  state:
    in_flight
    endpoint_backlog
    actor_load
    gpu_utilization
    e2e_p99
  action:
    increase/decrease K_max
    pause/resume submit
    route to actor pool
```

优化目标：

- 防止模型服务 queue 被数据库侧请求打爆；
- 防止 `K_max` 太小导致 GPU 吃不满；
- 把 Ray task/actor 的并发控制暴露成可测变量。

实现建议：

- 不改 Ray scheduler；
- 在 driver 或 gateway 层维护 `asyncio.Semaphore(K_max)`；
- 每个 endpoint / actor 维护 backlog、running、recent latency；
- 先做静态 `K_max` sweep，再做规则型 adaptive `K_max`。

规则示例：

```text
if gpu_utilization < low && queue_wait < low:
    K_max += step
if queue_wait > high or p99 rises:
    K_max -= step
if endpoint_backlog skew high:
    switch routing to least-queued
```

放弃条件：

- 如果接入 vLLM / Ray Serve 后，外部 `K_max` 和 routing 的 E2E 差异小于 5%，则把运行层贡献降级为边界分析，不强行写成核心优化。

### 4.4 Endpoint Router：把 Ray resource/locality 思想迁移到服务选择

借鉴来源：

- Ray resource-aware scheduling；
- Ray actor 适合有状态服务；
- SGLang / Mooncake / DistServe 等关于 prefix / KV / phase-aware routing 的思想。

系统设计：

```text
request
  -> routing policy
      round-robin
      least-queued
      gpu-util-aware
      token-aware
      prefix-aware
      workload-aware
  -> endpoint / actor
```

不同算子的策略：

| 算子 | 初始 routing 策略 | 后续增强 |
|---|---|---|
| `AI_EMBED` | least-queued / GPU-util-aware | 文本长度 bucket |
| `AI_FILTER` / `AI_CLASSIFY` | workload-aware | selectivity-aware |
| `AI_COMPLETE` | token-aware | prefix-aware / KV-cache affinity |

实现建议：

- P0：round-robin vs least-queued；
- P1：加入 GPU utilization；
- P2：`AI_COMPLETE` 跑通后再加 token-aware / prefix-aware。

放弃条件：

- 如果 endpoint 同质且请求长度分布稳定，least-queued 可能已经足够；不需要过早实现复杂 router。

### 4.5 Service-side Micro-batcher：把 vLLM/Ray Serve/Triton 思想放在正确位置

借鉴来源：

- vLLM continuous batching；
- Orca iteration-level scheduling；
- Ray Serve dynamic request batching；
- Triton dynamic batcher。

系统设计：

```text
endpoint waiting queue
  -> group by compatibility key
  -> wait until:
       max_batch_size reached
       max_tokens / shape budget reached
       batch_wait_timeout reached
  -> model forward
```

优化目标：

- 增加 GPU 每次 forward 的有效 batch；
- 控制 queue wait 和 P99；
- 避免把数据库侧 batch 重切误写成 dynamic batching。

可调变量：

- `max_batch_size`；
- `max_tokens` / shape budget；
- `batch_wait_timeout`；
- compatibility key。

实现建议：

- 首选接入已有 vLLM / Ray Serve / Triton 作为强 baseline；
- 如果自建 endpoint，只实现最小 queue + timeout + max_batch_size；
- `AI_EMBED` 先按文本长度 bucket，`AI_COMPLETE` 再考虑 token/prefix。

放弃条件：

- 如果服务端 dynamic batching 已经把 GPU 利用率拉满，本文不再声称服务端层有原创贡献，而转向数据库侧数据组织和端到端协调。

### 4.6 E2E Guardrail：防止局部优化吞掉端到端收益

借鉴来源：

- GPU serving 论文常用吞吐-延迟 / SLO 曲线；
- 数据库和存储系统强调 materialization、写回、索引维护可能转移瓶颈。

系统设计：

```text
guardrail checker
  input:
    e2e latency
    p99
    throughput
    writeback ratio
    error/timeout
  decision:
    keep config
    rollback config
    mark boundary
```

优化目标：

- 不只看 GPU model time；
- 防止增大 batch 后写回或 fan-in 成为新瓶颈；
- 为论文消融提供可解释边界。

实现建议：

- 每组实验记录阶段 breakdown；
- 正式结果报告吞吐-延迟曲线，不只报告单点速度提升；
- 每个优化点都要有 “when not help” 条件。

### 4.7 机制到实现任务优先级

| 优先级 | 实现任务 | 借鉴机制 | 最小实现 | 验证问题 |
|---|---|---|---|---|
| P0 | 可观测模型服务 baseline | vLLM / Ray Serve / Triton dynamic batching | 记录 queue wait、micro-batch size、model time、GPU utilization | 服务端 batching 能吃掉多少 GPU 侧收益？ |
| P0 | B 系列写回工程 baseline | PostgreSQL COPY、pgvector deferred index、TurboVecDB 写回思想 | COPY / unlogged staging / deferred index 对比当前 UPSERT | 写回是否仍会吞掉上游收益？ |
| P1 | `K_max` admission gate | Ray task scheduling + serving admission control | `asyncio.Semaphore(K_max)` 或等价提交门控 | bounded in-flight 是否改善 P99 / throughput？ |
| P1 | least-queued endpoint routing | Ray resource/load-aware scheduling | 每个 endpoint 维护 backlog，提交到最短队列 | routing 是否优于 round-robin？ |
| P1 | 计划层 batch/partition sweep | Ray object/task 粒度、Ray Data batch、Daft partition | 固定候选集 sweep，记录 object/task/fan-in | 数据组织是否仍影响端到端？ |
| P2 | workload-aware 配置表 | DB AI 算子代价感知、profile-guided selection | profile -> 推荐 batch/partition/K_max 初值 | workload-aware 是否优于固定最优？ |
| P2 | backpressure rule table | serving SLO guardrail | queue wait / P99 超阈值时降 K_max 或暂停提交 | 规则是否能避免 tail latency 恶化？ |
| P3 | token-aware / prefix-aware routing | SGLang / Mooncake / DistServe | 仅在 `AI_COMPLETE` 跑通后加入 token/prefix bucket | 长短请求混合时是否改善 P99？ |

当前最小闭环不需要一次实现全部任务。建议先做：

```text
P0 dynamic batching baseline
  -> P1 K_max sweep
  -> P1 routing comparison
  -> P1 plan-time batch/partition sweep
  -> P2 workload-aware rule table
```

## 5. 后续实验设计建议

### 5.1 最小可验证路径

1. 固定数据库侧 `batch_size / partition_count`，接入模型服务 dynamic micro-batching baseline，观察端到端吞吐、P99 和 GPU utilization。
2. 固定服务端 dynamic batching，扫描 `K_max` 和 routing，确认入口调度是否仍有额外收益。
3. 固定最优 `K_max / routing`，扫描计划层 `batch_size / partition_count / object_merge`，确认数据库侧数据组织是否影响 Ray object、fan-in 和写回。
4. 做组合策略消融：计划层 only、运行层 only、服务端 dynamic batching only、三层组合。

### 5.2 必须记录的指标

| 层 | 指标 |
|---|---|
| 计划层 | row count、text length distribution、RecordBatch count、Ray task count、object count、fan-in width |
| 运行层 | in-flight count、queue wait、endpoint backlog、routing decision、actor utilization |
| 服务端层 | micro-batch size、token/shape budget、batch wait time、GPU utilization、model time |
| 端到端 | latency、throughput、P50/P95/P99、writeback ratio、failure/timeout |

### 5.3 Baseline 顺序

| Baseline | 目的 |
|---|---|
| 固定 batch + 固定 routing + 无 dynamic batching | 合理默认链路，不作为 strawman |
| 固定 batch + 服务端 dynamic batching | 检查 vLLM/Ray Serve/Triton 机制能吃掉多少收益 |
| 固定 batch + dynamic batching + tuned `K_max` | 检查入口 admission 是否仍有价值 |
| workload-aware batch + tuned admission + dynamic batching | 本文候选组合策略 |
| 最优单层配置拼装 | 只在阶段间耦合明显时作为增强对照 |

## 6. 当前建议的实现结构

```text
SQL / table scan
  -> workload profiler
      row count, text length, operator type
  -> plan-time data organizer
      batch_size, partition_count, object_merge
  -> Ray task / actor submission gate
      K_max, backpressure
  -> endpoint router
      least-queued / token-aware / prefix-aware
  -> model service queue
      dynamic micro-batching
  -> GPU model forward
  -> fan-in / sink
  -> PostgreSQL / Lance / pgvector writeback
  -> E2E metrics and guardrail
```

实现优先级：

1. P0：接入一个可观测的服务端 dynamic batching baseline，记录 micro-batch size 和 queue wait。
2. P1：实现应用层 `K_max` 和 least-queued routing，验证和 dynamic batching 的互补性。
3. P2：实现 workload-aware plan-time batch/partition 选择，并和固定最优配置比较。
4. P3：如果 `AI_COMPLETE` 进入主线，再加入 token-aware / prefix-aware routing。

## 7. 不能写成的内容

- 不能写成本文提出 continuous batching。
- 不能写成本文改造 Ray scheduler。
- 不能写成动态 batch 会重切数据库侧已物化 `RecordBatch`。
- 不能只用文献证明本项目瓶颈；必须用本地 GPU-backed E2E profile 和消融实验验证。
- 不能把 `AI_COMPLETE` 的 token/KV 策略直接套到 `AI_EMBED`，两者机制不同。
