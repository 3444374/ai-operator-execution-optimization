# 动机测试结果分析

生成日期：2026-07-10

## 1. 实验目的

本目录中的实验不是为了证明最终论文方向已经成熟，而是做研究方向筛选和动机补强。当前要回答的问题是：

> 数据库 AI 算子外部分布式执行链路中，性能问题是否只来自 RecordBatch/object fan-in，还是同时涉及 task 划分、operator invocation、模型服务队列和 backpressure？

结论先行：

> 当前本地实验已经足以支持继续验证“AI 算子特征感知的任务划分、并行执行与跨层调度”。但这些结果仍然是 fake / 模拟实验，不能替代真实数据库、真实 AI 算子或真实模型服务验证。

## 2. 结果文件

| 文件 | 实验 |
|---|---|
| `fake_ai_embed_pipeline.csv` | fake `AI_EMBED(text)` 端到端动机测试 |
| `ai_operator_scenario_motivation.csv` | 三类 AI 算子场景对比 |
| `ai_operator_granularity_attribution.csv` | task/object/fan-in/operator invocation 收益来源拆分 |
| `ai_operator_backpressure.csv` | 模型服务 queue wait / token backlog / backpressure 模拟 |

## 3. fake AI_EMBED(text) 端到端动机测试

### 3.1 实验设置

脚本：

```bash
motivation/fake_ai_embed_pipeline_benchmark.py
```

正式结果：

```text
motivation/results/fake_ai_embed_pipeline.csv
```

核心参数：

| 参数 | 值 | 含义 |
|---|---:|---|
| total_rows | 65536 | 固定总行数 |
| embedding_dim | 128 | fake embedding 输出维度 |
| upstream | 8 / 32 | 上游分区数 |
| downstream | 8 / 32 | 下游 fan-in 分区数 |
| strategy | fine / coalesced | 是否产生大量小 object |
| repeats | 3 | repeat 0 作为 warm-up，不作为主要结论 |

### 3.2 实验过程

实验链路：

```text
generate documents
  -> Arrow RecordBatch
  -> ray.put
  -> fake AI_EMBED
  -> downstream fan-in
  -> write Arrow output
```

记录指标包括 input object 数、fake embedding 时间、fan-in 时间、写回时间和端到端时间。

### 3.3 真实数据

以下为 `repeat != 0` 的平均值：

| upstream | downstream | strategy | input objects | fan-in ms | e2e ms |
|---:|---:|---|---:|---:|---:|
| 8 | 8 | fine | 64 | 55.37 | 282.98 |
| 8 | 8 | coalesced | 8 | 16.65 | 199.18 |
| 8 | 32 | fine | 256 | 59.95 | 704.05 |
| 8 | 32 | coalesced | 32 | 43.04 | 281.63 |
| 32 | 8 | fine | 256 | 24.08 | 537.24 |
| 32 | 8 | coalesced | 8 | 11.03 | 178.15 |
| 32 | 32 | fine | 1024 | 65.99 | 1318.44 |
| 32 | 32 | coalesced | 32 | 49.05 | 430.10 |

### 3.4 解释

本地实验事实：

- `fine` 策略在 4 组 upstream/downstream 组合中端到端都慢于 `coalesced`。
- `upstream=32, downstream=32` 时，input objects 从 `32` 增加到 `1024`，端到端从 `430.10 ms` 增加到 `1318.44 ms`。

合理推断：

- 大量小 RecordBatch/object 会伤害 fake `AI_EMBED(text)` 外部执行链路。

不能声称：

- 不能说真实 embedding 模型也一定有同等收益。
- 不能说收益全部来自 fan-in，因为该实验同时改变了 task 数、object 数和 operator invocation 数。

## 4. 三类 AI 算子场景对比

### 4.1 实验设置

脚本：

```bash
motivation/ai_operator_scenario_motivation_benchmark.py
```

正式结果：

```text
motivation/results/ai_operator_scenario_motivation.csv
```

场景：

| 场景 | 代表 workload |
|---|---|
| embed_rag | chunk / embedding / vector write |
| classify_filter | AI_CLASSIFY / AI_FILTER / rerank |
| offline_llm | AI_COMPLETE / extract / offline generation |

### 4.2 实验过程

三个场景使用相同 Ray/Arrow 外部执行骨架，只改变 fake AI operator 的输出形态。该设计用于判断 object/task 粒度问题是否只存在于 embedding 特例。

### 4.3 关键结果

正式 CSV 显示，在 `upstream=32, downstream=32` 时：

| 场景 | fine objects | coalesced objects | fine e2e ms | coalesced e2e ms | e2e ratio |
|---|---:|---:|---:|---:|---:|
| embed_rag | 1024 | 32 | 1168.64 | 291.70 | 4.01x |
| classify_filter | 1024 | 32 | 1073.00 | 248.57 | 4.32x |
| offline_llm | 1024 | 32 | 1105.02 | 252.89 | 4.37x |

### 4.4 解释

本地实验事实：

- 三类 fake AI 算子都对 object/task 粒度敏感。
- 小输出的 `classify_filter` 和 `offline_llm` 也出现明显端到端差异。

合理推断：

- 问题不应只写成 embedding vector 写回，也可以抽象成数据库 AI 算子外部执行链路中的 task/object 粒度问题。

不能声称：

- 不能说真实 LLM 推理中一定有 4x 收益。
- 不能说当前 fake `offline_llm` 已模拟 prefill/decode、KV cache、continuous batching 或 GPU 队列。

## 5. Granularity attribution 动机补强实验

### 5.1 实验设置

脚本：

```bash
motivation/ai_operator_granularity_attribution_benchmark.py
```

正式结果：

```text
motivation/results/ai_operator_granularity_attribution.csv
```

核心参数：

| 参数 | 值 | 含义 |
|---|---:|---|
| total_rows | 65536 | 固定总行数 |
| payload_bytes_per_row | 512 | 每行模拟输出大小 |
| compute_us_per_row | 0.25 | 每行模拟计算时间 |
| upstream | 8 / 32 | 上游分区数 |
| downstream | 8 / 32 | 下游 fan-in 分区数 |

策略：

| 策略 | 含义 |
|---|---|
| fine | 每个 upstream/downstream slot 一个 AI operator task，下游直接 fan-in |
| two_stage_coalesced | 先细粒度执行，再按 downstream 增加 coalesce task |
| downstream_coalesced | 直接按 downstream 形成粗粒度 AI operator task |
| upstream_bundled | 按 upstream 打包执行，保留下游逻辑 payload |

### 5.2 关键数据

以下为 `upstream=32, downstream=32, repeat != 0` 的平均值：

| 策略 | total Ray tasks | fan-in Ray refs | operator ms | fan-in ms | e2e ms |
|---|---:|---:|---:|---:|---:|
| fine | 1056 | 1024 | 127.80 | 0.32 | 139.27 |
| two_stage_coalesced | 1088 | 32 | 158.96 | 0.13 | 176.00 |
| downstream_coalesced | 64 | 32 | 9.33 | 0.17 | 16.41 |
| upstream_bundled | 64 | 1024 | 7.28 | 0.48 | 19.24 |

### 5.3 解释

本地实验事实：

- `two_stage_coalesced` 把 fan-in Ray refs 从 `1024` 降到 `32`，但端到端从 `139.27 ms` 增加到 `176.00 ms`。
- `downstream_coalesced` 把 total Ray tasks 降到 `64`，端到端降到 `16.41 ms`。
- `upstream_bundled` 仍保留 `1024` 个 fan-in 逻辑 payload，但 total Ray tasks 降到 `64` 后，端到端为 `19.24 ms`。

合理推断：

- 当前 fake 链路中，端到端收益不只是 fan-in refs 减少，更强信号来自减少过细 AI operator task / invocation。
- 只在细粒度执行之后追加 coalesce 不一定有效；更值得研究的是在任务划分阶段就控制 AI operator invocation、Ray task 数和下游 fan-in 依赖。

不能声称：

- 不能说真实模型服务也一定适合 `downstream_coalesced`。
- 不能说 fan-in 不重要；该实验中的 payload 是轻量模拟，真实 Arrow RecordBatch / object store 可能更突出 fan-in 或对象传输。

## 6. Backpressure 动机补强实验

### 6.1 实验设置

脚本：

```bash
motivation/ai_operator_backpressure_benchmark.py
```

正式结果：

```text
motivation/results/ai_operator_backpressure.csv
```

核心参数：

| 参数 | 值 | 含义 |
|---|---:|---|
| total_requests | 512 | 离线 AI 任务数量 |
| producer_rate | 2000 / 8000 rps | 数据库/CPU 侧请求生产速率 |
| replicas | 2 / 4 | 模型服务副本数 |
| queue_limit | 0 / 8 / 32 | 0 表示无界提交，非 0 表示有界 in-flight |
| prompt_tokens | 128 | 基础输入 token |
| completion_tokens | 64 | 基础输出 token |
| long_request_ratio | 0.2 | 长请求比例 |
| long_token_multiplier | 8 | 长请求 token 放大倍数 |

### 6.2 实验过程

该实验是离散事件模拟，不启动 Ray，不接真实模型。它模拟：

```text
database / CPU producer
  -> submit requests
  -> model replicas consume tokens
  -> optional queue_limit backpressure
```

记录指标包括 tokens/s、平均 queue wait、max in-flight、max token backlog、backpressure events。

### 6.3 关键数据

以下为 `repeat != 0` 的平均值：

| producer rate | replicas | queue limit | tokens/s | avg queue wait | max in-flight | max token backlog |
|---:|---:|---:|---:|---:|---:|---:|
| 2000 rps | 2 | 0 | 23864.52 | 4768.50 ms | 499 | 231552 |
| 2000 rps | 2 | 8 | 23864.52 | 114.41 ms | 8 | 5568 |
| 2000 rps | 4 | 0 | 47408.83 | 2299.64 ms | 487 | 227904 |
| 2000 rps | 4 | 8 | 47408.83 | 37.98 ms | 8 | 6912 |

### 6.4 解释

本地实验事实：

- 在模型服务吞吐固定时，无界提交没有提高 tokens/s。
- 无界提交会显著放大 queue wait、in-flight 请求数和 token backlog。
- `queue_limit=8` 在保持 tokens/s 不变的同时，把队列压力控制在较低范围。

合理推断：

- 数据库 AI 算子外部执行链路需要模型服务状态感知的 backpressure 或 routing，否则 CPU/数据库侧可能持续生产超过模型服务消费能力的请求。

不能声称：

- 不能说真实 Ray Serve / vLLM 一定表现相同。
- 不能说 backpressure 一定提高吞吐；当前模拟显示它主要降低队列压力，而不是提高 tokens/s。

## 7. 综合判断

当前动机测试已经从“object/fan-in 有问题”推进到：

```text
object/fan-in
  + task / operator invocation
  + 模型服务 queue wait / token backlog / backpressure
```

因此，当前更合理的候选问题是：

> 面向数据库 AI 算子的特征感知任务划分、并行执行与跨层调度优化。

但证据等级仍然是：

| 结论 | 证据等级 |
|---|---|
| 大量小 RecordBatch/object 会带来 fan-in 成本 | 本地实验事实 |
| fake AI_EMBED 链路中 fine/coalesced 有端到端差异 | 本地实验事实 |
| 当前 fake 链路收益混合了 task 数、operator invocation、fan-in refs | 本地实验事实 |
| 模型服务无界提交会造成 backlog 风险 | 模拟实验事实 |
| 真实数据库 AI 算子也存在同样瓶颈 | 待确认 |
| 真实 LLM / embedding 服务中跨层调度能提升吞吐 | 待确认 |

## 8. 下一步

优先补三件事：

1. **Ray actor / Ray Serve 真实队列实验**：把 backpressure 从离散模拟迁移到 Ray actor 或 Ray Serve endpoint。
2. **PostgreSQL + 外部 worker 形态**：验证数据库读取、批处理、AI 算子执行和写回链路中是否仍有同样瓶颈。
3. **真实或半真实模型服务**：至少接本地 embedding 模型、小 LLM 或 vLLM/Ray Serve，记录 queue wait、token backlog、actor idle time、tokens/s。

