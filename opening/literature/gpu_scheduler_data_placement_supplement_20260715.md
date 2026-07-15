# GPU 调度与数据放置补充调研：策略控制器设计依据

整理日期：2026-07-15

用途：补充说明“运行时信号感知的上游执行控制器”不是凭空设计，而是从 GPU 推理服务、异构数据管线、GPU 数据库算子和数据库 AI 算子几类顶会/系统论文中抽取设计模式后，落到本课题的数据库 AI 算子外部执行链路。

## 1. 本次聚焦的问题

本轮调研围绕三个问题：

1. 数据库触发 AI 算子后，是否需要 scheduler / controller 一类设计？
2. `K_max`、`routing policy`、`guardrail` 这些策略变量分别从哪些已有系统思想中来？
3. GPU 算子调度和数据放到 GPU 上这部分，哪些顶会工作值得继续精读，哪些只能作为边界或 baseline？

结论先写清楚：

> 当前设计不应表述为“重新发明 GPU scheduler”或“改造 Ray 调度器”。更稳妥的表述是 three-layer upstream execution strategy：计划层在一次 SQL 进入外部执行链路前选择 batch/partition/object_merge；运行层在执行过程中根据模型服务队列、GPU 服务状态和写回占比等信号调整 `K_max`、`routing policy` 和 backpressure；模型服务侧再借鉴 vLLM / Ray Serve / Triton 的动态 batching 思想，把尚未执行的请求合并成推理 micro-batch，并用 P99、吞吐和写回占比作为 guardrail。

## 2. 可借鉴的前沿论文线索

| 方向 | 代表工作 | 可借鉴思想 | 对本课题的边界 |
|---|---|---|---|
| LLM/GPU 推理服务调度 | vLLM, Orca, Sarathi-Serve, DistServe, Mooncake, SGLang | continuous batching、iteration-level scheduling、prefill/decode phase splitting、KV cache / prefix reuse、SLO-aware scheduling | 它们主要优化模型服务内部；本课题的控制器在模型服务入口之前，不应声称发明这些内部机制 |
| 异构数据管线 | Ray, Ray Data Streaming Batch | task/actor、动态资源调度、partition-at-a-time、heterogeneous CPU/GPU pipeline | 可作为外部执行链路和 batch/partition 控制的系统依据 |
| GPU 数据库算子 | Crystal / GPU database analytics, GPU-resident index papers | GPU 上 selection/projection/sort/join 等算子的收益取决于内存带宽、materialization 和算子链融合 | 本课题不是传统 GPU 查询算子优化，但可借鉴“数据是否值得搬到 GPU、搬多大批量”的判断 |
| 数据放置与 GPU-resident 结构 | GPU-resident index, KV cache disaggregation, Lance/Arrow/Ray object path | 数据驻留、块化管理、跨 CPU/GPU/SSD 的状态放置会决定端到端收益 | 当前阶段主要作为动机和边界；只有实验显示数据传输/驻留成为瓶颈时再展开 |
| DB AI 算子与 AI-aware 查询优化 | Cortex AISQL, Smart, GaussML, Galois, LEADS, NeurDB | AI 算子不能按普通 UDF 估算；selectivity、token length、model cost 会改变执行计划 | 这些工作多在数据库内部或闭源系统，本课题聚焦外部执行链路 |

## 3. 当前策略控制器的来源映射

### 3.1 `batch / partition`

来源思想：

- Ray Data 的 streaming batch model 强调异构 CPU/GPU pipeline 中，传统 batch 或 stream 模型会 under-utilize heterogeneous resources，partition-at-a-time 能同时提供 pipeline、弹性和容错。
- GPU database analytics 的 Crystal 说明 GPU 数据库算子收益不是“GPU 一定快”，而取决于算子特性、内存带宽、materialization 和算子链组合。

落到本课题：

- `batch_size` 和 `partition_count` 不是普通调参，而是数据库表数据进入 AI 算子外部执行链路时的第一层控制变量。
- 对 `AI_EMBED`，主要影响模型调用次数、Ray task/object 数、fan-in 和写回批量。
- 对 `AI_FILTER/AI_CLASSIFY`，还要考虑 selectivity。
- 对 `AI_COMPLETE`，还要考虑 token length 和 prefix sharing。

### 3.1.1 Ray 调度思想可以迁移出的策略点

Ray OSDI 2018 的核心启发不是“我们要改 Ray scheduler”，而是它把 AI workload 的执行拆成了 task、actor、object store、local scheduler、global scheduler 和 resource constraint 几个可观测/可控制的面。落到本课题，比较稳的借鉴方式是：

| Ray 机制思想 | 可迁移到本课题的策略 | 不应过度声称 |
|---|---|---|
| task / actor 统一抽象 | 把一次数据库 AI 算子执行拆成无状态数据处理 task 与有状态模型服务 actor | 不声称本文重新设计 task/actor 模型 |
| local scheduler 优先、global scheduler 兜底 | 先在本地/当前 actor pool 内提交，队列积压或资源不匹配时再换 endpoint / actor pool | 不改 Ray 内部调度器，只在应用层做 admission/routing |
| resource-aware scheduling | 根据 CPU/GPU、模型服务实例、连接数和写回线程设置资源约束 | 不写成通用集群资源管理系统 |
| data locality / transfer cost | 避免过多小 object 和跨 worker fan-in；数据批次与模型服务入口尽量减少无意义搬运 | 不能只凭文献断言当前瓶颈，一定要用本地 profile 证明 |
| actor for stateful service | 用 actor / endpoint 表示 GPU model service replica，便于维护队列、warm model、micro-batch | 不把 actor 本身当贡献，贡献在于 AI 算子链路中的策略选择 |

因此，Ray 给我们的不是一个单独的“调度优化算法”，而是一组可实验变量：

```text
task granularity
actor pool size
resource requirement
placement / locality hint
object count and fan-in shape
admission limit K_max
endpoint routing policy
```

这些变量和 vLLM / Ray Serve / Triton 的 dynamic batching 思想可以组合：Ray 层控制请求如何进入模型服务，模型服务层控制等待请求如何形成 micro-batch。

### 3.2 `K_max`

来源思想：

- vLLM、Sarathi-Serve、DistServe、Mooncake 都说明 GPU 服务性能不能只看单请求 latency，而要看队列、batch、显存/KV cache、TTFT/TPOT、SLO 约束下的吞吐。
- Sarathi-Serve 特别强调吞吐-延迟 tradeoff：大 batch 有利于吞吐，但会影响 tail latency，需要 scheduler 控制请求进入方式。

本课题定义：

> `K_max` 是模型服务入口处允许同时在途的最大请求数，即 bounded in-flight 上限。

作用：

- `K_max` 太小：GPU 可能吃不满，吞吐下降。
- `K_max` 太大：endpoint queue/backlog 变高，P99 上升。
- 控制器根据 queue wait、backlog、GPU utilization 和 P99 调整 `K_max`，本质是反压控制。

注意边界：

- 如果后续直接接入 vLLM/Ray Serve 后，内部 scheduler 已经消化了大部分收益，外部 `K_max` 的贡献可能收窄。此时应把它写成“服务入口约束与数据库侧 batch 组织的协调”，不要硬写成独立核心贡献。

### 3.3 `routing policy`

来源思想：

- LLM serving 系统中，请求分配不是简单 round-robin。DistServe/Mooncake 等 disaggregated serving 工作将不同 phase、KV 状态、SLO 和资源放置纳入调度。
- SGLang/Parrot 这类工作说明上层应用语义、prefix/KV reuse 和请求依赖关系会改变服务端调度空间。

本课题定义：

> `routing policy` 是多个模型服务 endpoint / actor / worker 之间的请求分配策略。

候选策略：

- `round-robin`：默认轮询，作为简单 baseline。
- `least-queued routing`：发给队列最短或 backlog 最低的 endpoint。
- `token-aware routing`：长文本/长输出和短请求分开，避免长请求拖慢短请求。
- `prefix-aware routing`：对 `AI_COMPLETE` 场景，具有相似 prompt/prefix 的请求尽量送到能复用 KV/prefix cache 的服务。
- `workload-aware routing`：`AI_EMBED`、`AI_FILTER`、`AI_COMPLETE` 使用不同服务选择策略。

### 3.4 `guardrail`

来源思想：

- 现代推理服务论文通常不只报告平均 latency，而会报告 SLO、P99、TTFT/TPOT、goodput 等约束指标。
- 数据库/存储系统中，一个局部阶段变快不代表端到端变快，写回、materialization、shuffle/fan-in 可能吞掉收益。

本课题定义：

> `guardrail` 是保护约束，用来防止局部优化导致端到端退化。

可用 guardrail：

- P99 不明显上升。
- 吞吐不明显下降。
- 写回占比不过高，或者端到端收益仍保留。
- 对 `AI_FILTER/AI_CLASSIFY`，质量指标不下降。
- 对 `AI_COMPLETE`，长请求不明显拖慢短请求。

## 4. 这些论文如何支撑“需要 scheduler/controller”

从文献看，scheduler/controller 的必要性来自三类动态性：

1. workload 动态性：行数、文本长度、token length、selectivity、prefix sharing 都会变。
2. 服务动态性：GPU utilization、endpoint backlog、queue wait、KV cache 状态会变。
3. 端到端动态性：模型调用变快后，瓶颈可能转移到 fan-in 或 writeback。

因此，本课题不适合只写“把各阶段参数调好”。更稳的说法是：

> 先用阶段画像定位瓶颈，再用 three-layer strategy 区分计划层、运行层和服务端 micro-batching，并用端到端 guardrail 判断调整是否有效。需要注意：运行时不应把已经物化或已经排队的数据库侧 RecordBatch 重新合并再重切；这类重分批/重分区开销和实现复杂度都可能抵消收益。动态 batch 应放在模型服务侧尚未执行的请求队列中：通过 `max_batch_size`、`max_tokens`、`batch_wait_timeout`、shape/token compatibility 等约束，把短时间内到达的请求合并成推理 micro-batch。

实现上可以先不做复杂 learned optimizer：

1. 第一阶段：rule table + parameter sweep。
2. 第二阶段：按 workload 类型维护推荐配置。
3. 第三阶段：如果规则边界不稳定，再考虑 bandit / learned cost model。

这样比一上来声称“学习型调度器”更稳，也更符合开题阶段证据。

## 5. 对后续实验和图的建议

策略设计图应增加一个明确的组件名：

```text
Three-layer Upstream Execution Strategy
计划层输入：workload 特征 + 历史 profile + 参数 sweep 结果
计划层输出：batch_size, partition_count, object_merge
运行层输入：queue wait, backlog, GPU utilization, E2E metrics
运行层输出：K_max, routing policy, backpressure
服务端输入：waiting requests, token/shape budget, wait timeout
服务端输出：inference micro-batch
约束：P99, throughput, writeback ratio, quality
```

建议后续实验分三组：

| 实验组 | 目标 | 对应文献思想 |
|---|---|---|
| `batch_size × partition_count` | 验证数据组织是否影响模型调用、fan-in、写回 | Ray Data, Crystal |
| `K_max × endpoint_count` | 验证 bounded in-flight 和 queue/backlog 控制 | vLLM, Sarathi-Serve, DistServe |
| `routing policy` 对比 | 验证 least-queued / token-aware / prefix-aware 是否优于 round-robin | SGLang, Mooncake, Parrot |

## 6. 建议补充下载 / 精读的论文

优先级 P0：

- vLLM: Efficient Memory Management for Large Language Model Serving with PagedAttention. SOSP 2023.
- Sarathi-Serve: Taming Throughput-Latency Tradeoff in LLM Inference. OSDI 2024.
- DistServe: Disaggregating Prefill and Decoding for Goodput-optimized LLM Serving. OSDI 2024.
- Ray Data / Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution. arXiv 2025.
- A Study of the Fundamental Performance Characteristics of GPUs and CPUs for Database Analytics / Crystal. SIGMOD 2020.

优先级 P1：

- Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving.
- SGLang: Efficient Execution of Structured Language Model Programs. NeurIPS 2024.
- Parrot: Efficient Serving of LLM-based Applications with Semantic Variable. OSDI 2024.
- GPU-resident indexing papers such as RTIndeX / cgRX，用于理解“数据放到 GPU 上后结构设计和访问方式仍是核心问题”。

## 7. 当前可写入开题报告的方法口径

可以写：

> 借鉴 GPU 推理服务系统中吞吐-延迟约束下的 continuous batching / dynamic request batching、队列感知调度和 SLO guardrail 思想，以及异构数据管线中 partition-at-a-time 和动态资源分配思想，本文拟设计一个面向数据库 AI 算子外部执行链路的 three-layer upstream execution strategy。该策略不替代底层 Ray/vLLM 调度器，而是把数据库侧数据组织、服务入口调度和模型服务侧 micro-batching 分开：执行前根据 workload/profile 选择 batch/partition/object_merge；运行中根据服务队列、GPU 利用率和端到端指标调整 `K_max`、`routing policy` 与 backpressure；服务端将尚未执行的请求合并为推理 micro-batch，并通过 latency、throughput、P99 和 writeback ratio 判断优化是否有效。

不要写：

- 本文提出新的通用 GPU scheduler。
- 本文重新发明 continuous batching / PagedAttention / prefill-decode disaggregation。
- 本文已经证明跨层最优一定优于分别调优。
- 数据放到 GPU 上一定更快。

## 8. 参考链接

- vLLM / PagedAttention: https://arxiv.org/abs/2309.06180
- Sarathi-Serve: https://arxiv.org/abs/2403.02310
- DistServe: https://arxiv.org/abs/2401.09670
- Ray: https://arxiv.org/abs/1712.05889
- Ray Data streaming batch model: https://arxiv.org/abs/2501.12407
- Crystal / GPU database analytics: https://arxiv.org/abs/2003.01178
- Mooncake: https://arxiv.org/abs/2407.00079
- SGLang: https://arxiv.org/abs/2312.07104
- Parrot: https://arxiv.org/abs/2405.19888
- RTIndeX: https://arxiv.org/abs/2303.01139
