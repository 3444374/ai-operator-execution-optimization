# 策略设计与系统实现参考

整理日期：2026-07-15（2026-07-17 更新：统一为两项策略 + 端到端验证口径，新增 Daft 引擎抽象层）

用途：把 Ray、vLLM / Ray Serve / Triton、GPU 数据放置、数据库 AI 算子、Daft 等文献和系统资料中的可借鉴机制，沉淀为本课题后续实验设计与原型实现的参考。本文不是最终方法章节，也不声称这些机制已经在本项目中全部实现。

> **与 `strategy_design_literature_basis.md` 的分工（2026-07-24）**：本文是**工程映射**——信号→变量→指标→§8 目标代码架构→实现优先级（写代码/设计实验变量时查）；`literature_basis` 是**边界论证**——为什么、不能声称什么、fatal flaws（答辩/写论文时查）。两者用途不同，不合并不替换，见 `README.md` §二。

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

---

## 8. 目标代码架构与模块接口规范

> 整理日期：2026-07-23
> 来源：全维度综合评估（Wiki 知识库 + 33 篇精读论文 + 现有代码审计）

以下架构基于现有 6 模块（`sources.py` / `organizers.py` / `model_backends.py` / `sinks.py` / `metrics.py` / `workloads.py`），新增 4 个核心模块。每个模块的设计决策标注文献来源，遵循 `research/README.md` §文献优先设计方法论。

### 8.1 目标模块全景

```text
code/src/
├── sources.py          # 数据源（已有，不变）
├── organizers.py       # + bin_packing policy, + frame_budget（扩展）
├── model_backends.py   # + CLIPEmbeddingActor, + VLMCompletionActor（扩展）
├── sinks.py            # 写回（已有，不变）
├── metrics.py          # + tokens/s, + inflight 时间序列, + K_max 时间序列（扩展）
│
├── admission.py        # 【新增】K_max admission controller
├── routing.py          # 【新增】算子类型感知路由器
├── request_pool.py     # 【新增】跨查询请求池
└── pipeline.py         # 【新增】端到端 pipeline 编排
```

### 8.2 admission.py — K_max Admission Controller

**设计来源**：

| 设计决策 | 文献/系统来源 |
|---|---|
| AIMD（加性增/乘性减）控制律 | Clipper (NSDI 2017) §4.3.1 — AIMD **思想**来源（原文 +1 增 / ×0.9 减）；具体 α/β 参数参考 CONCUR (2025) §4.3 Eq1（α=2 增 / β=0.5 减） |
| EWMA 平滑 vLLM metrics | **本课题工程综合（非单篇来源）**：精读确认 CONCUR (2025) **不使用 EWMA**（用瞬时 KV 使用率/命中率 + 宽死区 0.2–0.5 + 双信号）；EWMA 平滑技术本身来自 Ray ConcurrencyCapBackpressurePolicy（已废弃）。"EWMA 平滑 + AIMD" 的组合是本课题综合，可作为方法贡献点表述（详见 `clipper_nsdi2017.md` / `concur_2025.md`） |
| 前瞻性准入判断（不只检查当前 queue） | SABER (2025) — Universal Scalability Law 拟合 `生成速度 = f(并发数)`，预测"提交后是否会违反 SLA"。**注意**：SABER 用 USL 做 per-request 准入预测，**不推导聚合 K_max**；K_max = √((1−α)/β) 上界推导是本课题扩展（见 `saber_2025.md` 第四层） |
| 多信号感知（running + waiting + KV cache） | running/waiting ← vLLM Prometheus（`num_requests_running`/`num_requests_waiting`）；KV cache ← **CONCUR (2025)**；多信号融合闭环**架构** ← CoLoRA (2026, ASP-DAC, **CCF-C**)。**注意**：CoLoRA 实际三信号是排队延迟 + adapter 驻留 + SLA 紧急度（多租户 LoRA），**不含 KV cache**（见 `colora_2026.md` 第四层校正） |
| 保持简单（<100 行）| Ray ConcurrencyCapBackpressurePolicy 废弃教训 — ~400 行复杂控制逻辑反而不如简单方案 |

**接口规范**：

```python
# admission.py

from dataclasses import dataclass
from abc import ABC, abstractmethod

@dataclass(frozen=True)
class AdmissionConfig:
    """AIMD + EWMA admission controller configuration."""
    min_inflight: int = 4
    max_inflight: int = 64
    ewma_alpha: float = 0.3          # EWMA 平滑系数（通用信号平滑技术；CONCUR 不用 EWMA，此值为工程取值）
    aimd_add_step: int = 2            # 加性增步长（AIMD 思想 ← Clipper NSDI'17 §4.3.1；+2 参考 CONCUR §4.3 α=2）
    aimd_mult_factor: float = 0.5     # 乘性减因子（参考 CONCUR §4.3 β=0.5；Clipper 原文为 ×0.9 / 10% backoff）
    queue_threshold: int = 10         # vllm:num_requests_waiting > this → decrease
    kv_cache_threshold: float = 0.85  # vllm:gpu_cache_usage_perc > this → decrease
    check_interval_s: float = 0.1     # 最小检查间隔，避免过度轮询

class AdmissionController(ABC):
    """Abstract admission controller for K_max regulation."""

    @abstractmethod
    async def should_submit(self, metrics: VLLMMetrics) -> bool: ...
    @abstractmethod
    def current_limit(self) -> int: ...

class StaticAdmissionController(AdmissionController):
    """Fixed K_max baseline. 不做自适应，仅作为对照."""

class AIMDAdmissionController(AdmissionController):
    """加性增/乘性减控制器。
    
    每 check_interval_s 检查 vLLM Prometheus metrics:
      - running < max_num_seqs * 0.5 AND waiting == 0 → K_max += aimd_add_step
      - waiting > queue_threshold OR kv_cache > kv_cache_threshold → K_max *= aimd_mult_factor
      - 否则不变
    K_max 使用 EWMA 平滑（alpha=0.3），避免瞬时 spike 导致过度反应.
    """

def scrape_and_decide(metrics_url: str, config: AdmissionConfig) -> int:
    """抓 Prometheus metrics → 决策 → 返回新 K_max."""
```

**最小实现约束**：
- 第一版 <100 行（不含 dataclass 和 abstract 定义）
- 仅依赖 `metrics.py` 中的 `scrape_prometheus_metrics()` 和 `vllm_metric_delta_stats()`
- 不使用 Ray 内置反压机制（`max_concurrency`、`should_add_input`），因为在 vLLM 场景下需要的是连续值决策而非二元开关

**实现注意事项（2026-07-24，对照当前 `postgres_ai_operator_profile.py:512` 双态实现）**：

- **EWMA 默认关闭**：上文 EWMA 平滑来源是已废弃的 Ray ConcurrencyCapBackpressurePolicy，而最相关的 CONCUR 不用 EWMA（瞬时值 + 宽死区 0.2–0.5）。AIMD 实现里建议默认走 CONCUR 风格，EWMA 作为可关闭选项，数据证明需要时再开（code/AGENTS.md §1"先用简单规则、数据证明再加复杂度"）。
- **AIMD 作对照，不设默认**：当前是双态 bang-bang，AIMD 应作为对照选项加入，数据证明优于双态后再换。
- **抓取节流**：当前每次提交同步 `scrape_prometheus_metrics`（timeout 1s）+ `sleep(poll_interval)`，紧密循环里会引入控制延迟、扭曲 K_max 真实效果；建议后台线程刷新快照、控制器读最新快照、去掉强制 sleep。
- **flush 与 K_max 口径**：当前代码只有 K_max adaptive，无独立 flush（攒批等待时机）逻辑；文档若提"queue-adaptive flush"，需注明 flush 尚未独立实现，或改名"adaptive K_max"对齐代码现状。

### 8.3 routing.py — 算子类型感知路由器

**设计来源**：

| 设计决策 | 文献/系统来源 |
|---|---|
| 按算子类型路由到不同 endpoint | Actor pool 分池路由（本课题方案设计） |
| Least-queued 路由 | Ray resource-aware scheduling (OSDI 2018) — local scheduler 优先思想 |
| Prefix-aware 路由 | SGLang RadixAttention (NeurIPS 2024) — 按 prefix hash 路由到亲和 replica |
| Token-aware 路由 | Parrot Semantic Variable (OSDI 2024) — 跨请求 prompt 共享 |
| 多模态 endpoint 异构 | Snowflake Cortex AISQL (SIGMOD 2026) — 工业多模态 AI SQL 算子需求证据 |

**接口规范**：

```python
# routing.py

from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class RouteRequest:
    """Router 输入：一个待路由的 batch."""
    operator_type: Literal["ai_complete", "ai_embed", "ai_classify"]
    batch_tokens: int        # 用于 token-aware 路由
    prefix_key: str | None   # 用于 prefix-aware 路由

@dataclass(frozen=True)
class RouteDecision:
    """Router 输出：路由目标."""
    endpoint_url: str
    model_name: str
    backend_type: Literal["vllm", "clip", "vlm"]

@dataclass(frozen=True)
class EndpointState:
    """单个 endpoint 的实时状态."""
    url: str
    operator_types: list[str]        # 该 endpoint 可服务的算子类型
    num_requests_running: int
    num_requests_waiting: int
    kv_cache_usage_perc: float
    model_name: str

class OperatorAwareRouter:
    """按算子类型 + 队列状态选择目标 endpoint.

    决策优先级:
      1. 过滤 operator_type 匹配的 endpoint
      2. 在其中选 num_requests_waiting 最小的
      3. 如 prefix_key 非空 → 优先路由到已有该 prefix cache 的 endpoint
         (来源: SGLang RadixAttention, NeurIPS 2024)
    """

    def __init__(self, endpoints: list[str], operator_map: dict[str, list[str]]): ...
    def route(self, request: RouteRequest,
              endpoint_states: dict[str, EndpointState]) -> RouteDecision: ...
    def update_states(self, metrics_by_endpoint: dict[str, dict]) -> None: ...
```

**最小实现约束**：
- P0：RoundRobinRouter + LeastQueuedRouter（不依赖 prefix/token 信息）
- P1：OperatorAwareRouter（按算子类型过滤 endpoint）
- P2：PrefixAwareRouter（按 prefix hash 路由）

### 8.4 request_pool.py — 跨查询请求池

**设计来源**：

| 设计决策 | 文献/系统来源 |
|---|---|
| 按 operator_type 分 bucket | 本课题方案设计 — 同类合并、异类分池 |
| 按 token/frame budget 合并提交 | vLLM `max_num_batched_tokens` (SOSP 2023) — token-budget batching 思想 |
| 异步 enqueue + 定时/阈值 flush | Ray Serve `batch_wait_timeout_s` (Ray 官方文档) — 攒批超时机制 |
| CLIP 无 continuous batching → 必须显式合并 | CLIP/OpenAI embedding API 工程事实 — 多模态场景的强制需求 |

**接口规范**：

```python
# request_pool.py

from dataclasses import dataclass, field
import pyarrow as pa
from typing import Literal

@dataclass(frozen=True)
class PoolBatch:
    """池中取出的就绪 batch."""
    operator_type: str
    table: pa.Table
    token_count: int
    row_count: int
    enqueue_time_s: float

@dataclass
class PoolStats:
    total_enqueued: int = 0
    total_flushed: int = 0
    merge_ratio: float = 0.0   # flushed_rows / enqueued_rows（合并效率）
    buckets: dict = field(default_factory=dict)

class GlobalRequestPool:
    """跨查询异步请求池.

    不同 SQL 查询的 AI 算子请求通过 enqueue() 进入池中，按 operator_type
    自动分 bucket。flush_ready() 按 token/frame budget 合并同 bucket 的
    请求为 PoolBatch，提交给下游 model_backend。

    纯文本场景（vLLM 有 continuous batching）：
      - 此池可选的——vLLM 内部自动合并请求
    多模态场景（CLIP 无 continuous batching）：
      - 此池必须的——不做显式合并则 GPU 利用率低
    """

    def __init__(self, budget_strategy: BudgetStrategy): ...

    async def enqueue(self, operator_type: str, batch: pa.Table,
                      arrival_time: float) -> None:
        """接收来自不同 SQL 查询的请求 batch."""
        ...

    async def flush_ready(self) -> list[PoolBatch]:
        """按 budget 策略合并各 bucket 的就绪请求并返回."""
        ...

    def pool_stats(self) -> PoolStats:
        """返回各 bucket 积压量、合并效率等统计."""
        ...
```

**最小实现约束**：
- 使用 `asyncio.Queue` 按 `operator_type` 分桶
- Token-budget 合并复用 `organizers.py` 中的 `_token_budget_batches()`
- 多模态扩展：`token_budget` → `frame_budget` 仅替换计数函数，合并逻辑不变

### 8.5 pipeline.py — 端到端 Pipeline 编排

**设计来源**：

| 设计决策 | 文献/系统来源 |
|---|---|
| 单一入口覆盖文本+图像 | Daft DataFrame 统一 API（`df["prompt"]` / `df["image"]`） |
| 阶段拆分计时（DB fetch → organize → request wall → writeback） | 本项目 AI_EMBED 预研方法论（`motivation/results/gpu/`） |
| 策略层不依赖引擎层 | DataOrganizer 抽象接口 — 策略代码只依赖 BatchRequest 元数据 |

**接口规范**：

```python
# pipeline.py

from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class PipelineConfig:
    """组合所有子模块的配置，单入口启动."""
    source: SourceConfig
    organizer: OrganizerConfig
    admission: AdmissionConfig | None   # None → 不限制 inflight
    router_config: RouterConfig | None  # None → 直接使用默认 endpoint
    backend: BackendConfig
    sink: SinkConfig
    metrics: MetricsConfig

def run_text_pipeline(config: PipelineConfig) -> PipelineResult:
    """AI_COMPLETE 文本 pipeline.

    PostgreSQL → DaftPostgresSource → Organizer
      → [RequestPool] → Admission → Router → vLLM completions
      → Sink (none/pgvector)
    """
    ...

def run_image_pipeline(config: PipelineConfig) -> PipelineResult:
    """多模态 pipeline（AI_EMBED/AI_CLASSIFY 图像）.

    复用 run_text_pipeline 的同一套策略代码。
    仅替换:
      - 数据列: df["prompt"] → df["image"]
      - 后端: vLLM completions → CLIP embeddings / Qwen2.5-VL
      - 预算单位: token_budget → frame_budget
    """
    ...
```

### 8.6 新增模块的文献来源汇总

| 模块 | 核心机制 | 主要文献依据 |
|---|---|---|
| `admission.py` | AIMD + EWMA 自适应 K_max | Clipper (NSDI'17), CONCUR (2025), SABER (2025), CoLoRA (2026) |
| `routing.py` | 算子类型 + 队列状态感知路由 | SGLang (NeurIPS'24), Parrot (OSDI'24), Ray (OSDI'18), Cortex AISQL (SIGMOD'26) |
| `request_pool.py` | 跨查询按预算合并 | vLLM (SOSP'23), Ray Serve batching, CLIP 工程约束 |
| `pipeline.py` | 模态无关的统一编排 | Daft DataFrame API, 本项目 AI_EMBED 预研 |

### 8.7 实现优先级

| 优先级 | 模块 | 触发条件 | 阻塞项 |
|---|---|---|---|
| **P0** | `admission.py`（AIMD + EWMA） | 立即 | 无 — 用已有 vLLM Prometheus 数据即可开发测试 |
| **P0** | `metrics.py` 扩展（tokens/s + 时间序列） | 立即 | 无 — 已有 CSV 可直接计算 |
| **P1** | `organizers.py` 扩展（bin-packing） | P0 完成后 | 需先确定 bin-packing 在 chunked prefill 下的行为假设 |
| **P1** | `request_pool.py`（最小实现） | P0 完成后 | 多模态实验的前置依赖 |
| **P1** | `model_backends.py` 扩展（CLIP） | P0 完成后 | 需 CLIP 模型 + GPU 显存验证 |
| **P2** | `routing.py`（OperatorAware） | 多 endpoint 环境就绪后 | 需至少 2 个异构 endpoint |
| **P2** | `pipeline.py` | 以上模块稳定后 | 不阻塞实验——脚本可直接调用各模块 |
