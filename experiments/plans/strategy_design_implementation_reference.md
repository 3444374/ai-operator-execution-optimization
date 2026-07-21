# 策略设计与系统实现参考

整理日期：2026-07-15（2026-07-17 更新：统一为两项策略 + 端到端验证口径，新增 Daft 引擎抽象层）

用途：把 Ray、vLLM / Ray Serve / Triton、GPU 数据放置、数据库 AI 算子、Daft 等文献和系统资料中的可借鉴机制，沉淀为本课题后续实验设计与原型实现的参考。本文不是最终方法章节，也不声称这些机制已经在本项目中全部实现。

## 1. 当前策略口径

课题方向已于 2026-07-16 收敛为两项上游策略 + 一项端到端验证：

```text
研究内容一：数据组织策略
  输入：行数估计、token 长度分布、prefix 结构、AI 算子类型
  输出：batch 构造方式（token-budget / length-aligned / prefix-aware grouping）
  实现载体：异构 Ray actor pool + 引擎无关的 DataOrganizer 抽象层

研究内容二：调度与提交控制策略
  输入：vLLM queue depth、in-flight count、GPU utilization、E2E metrics
  输出：flush 时机（queue-adaptive）、K_max 动态范围、routing policy
  实现载体：Ray actor 去中心化异步循环

端到端验证：写回瓶颈判定
  输入：operator_wall_s、writeback_s、e2e_s
  输出：写回占比是否吞噬上游收益的判定
  工程 baseline：COPY + deferred index
```

关键边界：

- vLLM 作为部署平台和强 baseline，不修改其内部 continuous batching 机制。GPU 侧仅观测 Prometheus metrics（`num_requests_running`、`num_requests_waiting`、`gpu_cache_usage_perc`）作为反馈信号。
- 数据组织层的当前实现仍是 Arrow RecordBatch；按 2026-07-17 总纲，近期需要在文本阶段接入 Daft DataFrame，替代裸 Arrow RecordBatch 构造。引擎切换通过 DataOrganizer 抽象接口隔离，避免把策略层贡献绑定到具体引擎。
- 两项策略分别独立搜索最优配置后拼接，再与联合 grid search 对比，判定耦合程度。
- 不声称发明 vLLM continuous batching、Ray scheduler 或 Daft 执行引擎；本文贡献在策略选择和协调验证。

## 2. 文献与机制映射

| 来源 | 机制 | 对应研究内容 | 可做成的实验变量 | 边界 |
|---|---|---|---|---|
| Ray OSDI 2018 | task / actor、dynamic task graph、distributed scheduler、object store、resource-aware scheduling | 研究内容一 + 研究内容二 | task granularity、actor pool size、resource requirement、placement/locality、object count、fan-in shape | 不改 Ray 内部 scheduler，只做应用层 admission/routing |
| Ray Data / Ray Serve | `map_batches`、concurrency、dynamic request batching、routing、autoscaling | 研究内容一 + 研究内容二（接口参考） | batch size、concurrency、`max_batch_size`、wait timeout、replica count | 官方接口存在不等于本项目一定有收益，必须用 GPU-backed profile 验证 |
| vLLM / Orca | continuous batching、iteration-level scheduling、吞吐-延迟曲线、SLO 约束 | 部署平台（不修改内部） | max tokens、request admission、TTFT/TPOT/P99、serving throughput | 作为强 baseline 或机制借鉴，不写成本文原创 |
| Triton Inference Server | dynamic batcher、preferred batch size、queue delay | 部署平台参考 | max batch size、preferred batch size、max queue delay | 更适合模型服务 baseline，不直接解决数据库侧数据组织 |
| Sarathi-Serve / DistServe / Mooncake / SGLang | phase splitting、KV/prefix reuse、SLO-aware routing | 研究内容二（prefix-aware routing 设计参考） | token-aware routing、prefix-aware routing、long/short request isolation | 主要服务 `AI_COMPLETE`，对 `AI_EMBED` 只作为边界和扩展 |
| GPU 数据库 / GPU-resident 结构 | 数据搬运、materialization、GPU 内存驻留、算子融合 | 研究内容一 | 是否搬到 GPU、批量大小、列式表示、object/materialization 数量 | 本课题不做传统 GPU 查询算子 kernel 优化 |
| Cortex AISQL / GaussML / Galois / LEADS / NeurDB | AI 算子语义影响执行计划和代价估计 | 研究内容一 + 端到端验证 | 按 `AI_EMBED` / `AI_FILTER` / `AI_COMPLETE` 分 workload | 多数系统不暴露外部链路细节，不能过度类比 |
| Daft (Flotilla) / `@daft.cls` | Rust 执行引擎、Morsel 流式 Push 模型、Arrow 零拷贝、GPU Stateful UDF | 研究内容一（引擎层，通过 DataOrganizer 抽象隔离） | batch_size、concurrency、morsel size、GPU 分配策略 | Daft 不观测 vLLM 内部状态，不做 token-aware 调度；本文优化策略层而非引擎层 |

## 3. 可以落地的设计点

### 3.1 研究内容一：数据组织策略（对应旧"计划层"）

目标：在执行前选择合理的数据组织方式（token-budget batching、length-aligned grouping、prefix-aware grouping），通过异构 Ray actor pool 实现，避免过多小 task、小 object 和不合适的 GPU 请求粒度。

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

### 3.2 研究内容二：调度与提交控制策略（对应旧"运行层"）

目标：每个 Ray actor 独立观测 vLLM 队列状态，自主决定 flush 时机、K_max 动态范围和 routing 目标，避免 GPU 吃不满或模型服务队列被打爆。

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

### 3.3 vLLM 部署平台（对应旧"服务端层"，不修改内部）

目标：vLLM continuous batching 作为部署平台和强 baseline。上游策略的目标不是替代 vLLM 的调度器，而是给它构造最优的请求流（shape + rhythm）。GPU 侧仅观测 Prometheus metrics 作为反馈信号。

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
      prompt_tokens: target-model tokenizer count per row
      token_count_source: model_tokenizer / trace_metadata / char_proxy
      tokenizer_name_or_path
      tokenizer_add_special_tokens
      output_shape: embedding_dim / selectivity / token_bound
      sink_type: PostgreSQL / pgvector / Lance
```

用途：

- 给研究内容一（数据组织）选择 batch 构造策略和 token budget；
- 给研究内容二（调度提交）选择初始 `K_max` 和 routing；
- 给 vLLM 部署平台提供请求 shape/rhythm 特征，辅助上游决策。

最小实现：

- 对 `AI_COMPLETE`，必须优先计算并保存目标模型 tokenizer 下的 `prompt_tokens`。当前实现路径是在 workload 导入阶段用 Hugging Face `AutoTokenizer` 计算，写入 PostgreSQL `documents.prompt_tokens`，再由 Daft/Arrow table 传给 `DataOrganizer`。
- `prompt_tokens` 的来源必须可追溯：记录 `tokenizer_name_or_path`、`tokenizer_add_special_tokens`、`token_count_source`、`max_model_len` 和 `completion_max_tokens`。只有 `token_count_source=model_tokenizer` 的结果可以支撑正式 token-aware 策略结论。
- token-budget batching 的单行估计代价为 `prompt_tokens + completion_max_tokens`；该公式属于策略层输入，不改变每行 prompt 的语义边界，也不把单行 prompt 拆成多条请求。
- 如果暂时没有 tokenizer，只能用 trace token 或字符长度作为 fallback，并在实验报告中标注为诊断/预研，不作为正式结论。
- 后续再加 selectivity 估计和 prefix/cache 信息。

放弃条件：

- 如果 workload profile 与最优配置相关性很弱，数据组织策略退化为固定最优配置；不要强行做复杂 cost model。

### 4.2 Data Organizer：引擎无关的数据组织抽象

借鉴来源：

- Ray task 粒度和 object store 机制；
- Ray Data / Daft 的 batch、partition、shuffle/coalescing 思想；
- GPU 数据库工作中关于 materialization 和数据搬运的边界；
- Daft 的 Morsel Push 模型和 `@daft.cls` GPU UDF 接口。

引擎抽象设计：

当前代码仍使用 Arrow RecordBatch 作为数据载体，但通过 `DataOrganizer` 抽象接口隔离引擎细节；下一步应实现 Daft DataFrame 文本后端，替代裸 Arrow RecordBatch 构造，同时保留 ArrowOrganizer 作为对照/回退：

```text
DataOrganizer.organize(rows, strategy)
  → 当前实现：ArrowOrganizer（RecordBatch 构造 + Ray actor 分发）
  → 近期目标：DaftOrganizer（Daft DataFrame → into_batches / repartition → Ray actor / @daft.cls GPU UDF）
```

策略层代码（token-budget 决策、queue-adaptive flush、routing）只依赖 `BatchRequest` 抽象，不感知底层引擎。

`BatchRequest` 至少应携带以下与 token-aware 组织有关的元数据：`row_count`、`prompt_tokens_sum`、`prompt_tokens_min/p50/p95/max`、`estimated_total_tokens`、`completion_max_tokens`、`token_count_source`、`prefix_key`（如可用）。这些字段用于策略选择、CSV 记录和事后审计；不应由下游 vLLM 响应反推后再补写为执行前决策依据。

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

- 如果接入 vLLM 后，外部 `K_max` 和 routing 的 E2E 差异小于 5%，则把研究内容二贡献降级为边界分析，不强行写成核心优化。

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

### 4.5 vLLM Deployment Platform：作为强 baseline 和观测源

vLLM continuous batching 作为部署平台，不做修改。上游策略通过以下方式与 vLLM 交互：

**观测（被动）**：Prometheus metrics endpoint
- `vllm:num_requests_running` → 判断 GPU 是否接近 `max_num_seqs`
- `vllm:num_requests_waiting` → 判断队列积压程度
- `vllm:gpu_cache_usage_perc` → 判断 KV cache 压力

**控制（上游主动）**：通过请求的 shape 和 rhythm 影响 vLLM 行为
- token-budget batching → 影响 `max_num_batched_tokens` 约束的命中率
- prefix-aware grouping → 提高 APC (Automatic Prefix Caching) 命中率
- queue-adaptive flush → 影响 `num_running_seqs` 利用率

实现建议：
- P0：接入 vLLM + Qwen2.5-1.5B，记录 Prometheus metrics 和 TTFT/TPOT/吞吐
- P1：对比 vLLM 默认行为 vs 上游策略介入后的端到端差异
- 不做：修改 vLLM scheduler、自建 continuous batching、替换 vLLM 为自研推理引擎

### 4.6 端到端验证：写回瓶颈判定与 Guardrail

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
| P0 | vLLM + Qwen2.5-1.5B baseline 建立 | vLLM continuous batching + Prometheus metrics | 记录 queue depth、running reqs、KV cache usage、TTFT/TPOT/吞吐 | vLLM 在 RTX 5070 上的实际性能曲线？ |
| P0 | 写回工程 baseline | PostgreSQL COPY、pgvector deferred index | COPY / unlogged staging / deferred index 对比当前 UPSERT | 写回是否吞噬上游收益？ |
| P1 | 研究内容一消融：数据组织策略 | vLLM max_num_batched_tokens、Ray Serve batch_size_fn、Daft Morsel 流式 | token-budget vs length-align vs prefix-aware 消融 | 哪种数据组织策略最优？差距多大？ |
| P1 | 研究内容二消融：调度与提交控制策略 | Clockwork 确定性调度、Clipper AIMD、Ray actor async loop | queue-adaptive flush vs 固定 K_max sweep | 自适应提交是否优于静态配置？ |
| P1 | actor pool 分池路由 | Ray resource-aware scheduling、SGLang prefix-aware routing | 异构 actor pool：按 token 长度/prefix 分组 | 分池路由是否优于 uniform pool？ |
| P2 | 耦合验证：独立拼接 vs 联合 grid search | — | RC1* + RC2* 拼接 vs joint grid search | 两项策略是否需要联合调优？ |
| P0/P1 | Daft 文本后端接入 | Daft DataFrame、into_batches、repartition、@daft.cls GPU UDF | DataOrganizer 从 ArrowOrganizer 扩展为 DaftOrganizer；Arrow 后端保留为对照/回退 | 接入 Daft 后策略层结论是否一致？Daft 引擎级参数如何影响数据组织与提交控制？ |
| P3 | token-aware / prefix-aware routing | SGLang RadixAttention、Parrot Semantic Variable | 仅在 AI_COMPLETE 跑通后加入 | 长短请求混合时是否改善 P99？ |

当前最小闭环不需要一次实现全部任务。建议先做：

```text
P0 vLLM baseline 建立 + 写回工程 baseline
  → P1 研究内容一消融（数据组织策略：token-budget / length-align / prefix-aware）
  → P1 研究内容二消融（调度与提交控制：queue-adaptive flush / K_max / routing）
  → P2 耦合验证（独立拼接 vs 联合 grid search）
  → P0/P1 Daft 文本后端接入（与 vLLM baseline 和文本消融同步推进）
```

## 5. 后续实验设计建议

### 5.1 实验阶段（与 knowledge_hub.md §7.2 对齐）

| 阶段 | 内容 | 核心消融 |
|---|---|---|
| 前置 | vLLM + Qwen2.5-1.5B baseline | 替代手动 HTTP endpoint |
| 第一阶段 | 研究内容一：数据组织策略消融 | 静态 batch_size vs token-budget vs length-align vs prefix-aware |
| 第二阶段 | 研究内容二：调度与提交控制策略消融 | 固定 K_max vs queue-adaptive vs actor pool 分池 |
| 第三阶段 | 耦合验证 | 独立最优拼接 vs 联合 grid search |
| 第四阶段 | 写回瓶颈判定 | COPY + deferred index vs 其他 sink |

### 5.2 必须记录的指标

| 研究内容 | 指标 |
|---|---|
| 研究内容一（数据组织） | row count、token length distribution、tokenizer_name_or_path、token_count_source、completion_max_tokens、batch token distribution、RecordBatch count、object count、operator invocations、fan-in width |
| 研究内容二（调度提交） | in-flight count、queue wait、vLLM num_requests_running/waiting、K_max 实际值、routing decision |
| vLLM 部署平台（观测） | TTFT、TPOT、throughput、GPU utilization、KV cache usage、batch size per forward |
| 写回 | writeback_s、writeback ratio、sink type |
| 端到端 | e2e_s、rows/s、P50/P95/P99、failure/timeout |

### 5.3 Baseline 顺序

| Baseline | 目的 |
|---|---|
| 固定 batch_size + 无自适应提交 + vLLM 默认 | 合理默认链路，不作为 strawman |
| 研究内容一 only（最优数据组织 + 无自适应提交） | 数据组织策略的独立贡献 |
| 研究内容二 only（固定数据组织 + 最优调度提交） | 调度提交策略的独立贡献 |
| RC1* + RC2* 拼接 | 两项策略独立最优的叠加效果 |
| 联合 grid search | 判定耦合程度：联合显著优于拼接则需联合调优 |

## 6. 当前建议的实现结构

```text
PostgreSQL / table scan
  -> workload profiler
      row count, token length distribution, prefix structure, operator type
  -> DataOrganizer (引擎抽象层)
      当前：ArrowOrganizer → RecordBatch → Ray actor 分发
      后续：DaftOrganizer → Daft DataFrame → morsel 流式 → @daft.cls GPU UDF
  -> 研究内容一：数据组织策略
      token-budget / length-aligned / prefix-aware grouping
  -> 研究内容二：调度与提交控制策略
      Ray actor async loop: queue-adaptive flush / K_max / routing
  -> vLLM Continuous Batching (部署平台，不修改)
      观测 Prometheus metrics 作为反馈信号
  -> GPU model forward
  -> fan-in / sink
  -> PostgreSQL / pgvector writeback
      COPY + deferred index (工程最优 baseline)
  -> 端到端验证：写回瓶颈判定
  -> E2E metrics and guardrail
```

实现优先级：

1. P0：建立 vLLM + Qwen2.5-1.5B baseline，记录 Prometheus metrics 和 TTFT/TPOT/吞吐。
2. P1：研究内容一消融（数据组织策略：token-budget / length-align / prefix-aware）。
3. P1：研究内容二消融（调度与提交控制：queue-adaptive flush / K_max / actor pool 分池）。
4. P2：耦合验证（RC1* + RC2* 拼接 vs 联合 grid search）。
5. P0/P1：Daft 文本后端接入（见 knowledge_hub.md §10.5.1；当前 Arrow 后端仅表示实现状态，不代表路线仍延后）。

## 7. 不能写成的内容

- 不能写成本文提出 continuous batching 或改造 vLLM scheduler。
- 不能写成本文改造 Ray scheduler 或重新设计 task/actor 模型。
- 不能写成本文提出 Daft 执行引擎、Morsel 流式模型或 `@daft.cls` 机制——Daft 是引擎层工具，本文贡献在策略层。
- 不能写成动态 batch 会重切数据库侧已物化 `RecordBatch`。
- 不能只用文献证明本项目瓶颈；必须用本地 GPU-backed E2E profile 和消融实验验证。
- 不能把 `AI_COMPLETE` 的 token/KV 策略直接套到 `AI_EMBED`，两者机制不同。
- 不能声称"数据组织层已实现 Daft 后端"——当前仅实现 Arrow 后端；Daft 后端是近期必须补齐的文本阶段实现目标，并应保留 Arrow 后端作为对照/回退。
- 不能声称"本文方法在具身智能/多模态场景中有效"——只有在真实多模态 workload 上验证后才能说。
