# AI infra 候选 AI 算子场景与动机测试

生成日期：2026-07-10

## 1. 目的

本文件用于回答：

> 在“数据库 / 企业数据系统中的 AI 算子”场景下，哪些 workload 更贴近 AI infra / inference infra，应该优先做动机测试和可行性测试？

当前不能直接把 Object Transfer、fan-in、Shuffle 或 coalescing 写成最终方向。正确路径是：

```text
找真实 AI 算子场景
  -> 明确输入输出和用户
  -> 设计动机测试
  -> 跑可复现实验
  -> 根据瓶颈形态收敛优化点
```

## 2. 外部依据

### 2.1 数据库 AI 算子已经是现实场景

Snowflake Cortex AISQL 提供 `AI_COMPLETE`、`AI_CLASSIFY`、`AI_FILTER`、`AI_AGG`、`AI_EMBED`、`AI_EXTRACT`、`AI_PARSE_DOCUMENT` 等 SQL 级 AI 函数，并明确提到这些函数适合对大 SQL 表中的大量文本输入做 batch processing。

来源：

- Snowflake Cortex AI Functions: https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql
- Snowflake `AI_COMPLETE`: https://docs.snowflake.com/en/sql-reference/functions/ai_complete
- BigQuery ML `ML.GENERATE_TEXT`: https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text

### 2.2 PostgreSQL / 开源生态的状态需要更谨慎

PostgreSQL 生态中确实存在数据库内或 SQL 层发起 AI/LLM 调用的项目，但成熟度和场景标准化程度不一致：

- PostgresML 提供 PostgreSQL 中调用机器学习 / transformer 能力的路径，属于“SQL / PostgreSQL 内触发模型推理”的开源证据。
- Timescale `pgai` 曾提供 PostgreSQL AI 工作流能力，尤其强调 embedding、vectorizer 和 RAG 数据准备；但该仓库当前已归档，不能作为后续核心依赖的默认前提。
- MindsDB 这类系统提供 SQL 调用 AI 模型、连接数据库数据源的能力，但它更像外部 AI/SQL 层，不等同于 PostgreSQL 扩展内置执行链路。

因此，当前可以说：

> 数据库 / SQL 出发的离线 AI 调用是现实需求；PostgreSQL 生态中有相关路径，但 PostgreSQL 上“离线 LLM 生成 / 评测”还不能被当成和 pgvector embedding/RAG 一样稳定、标准的主场景。

来源：

- PostgresML: https://github.com/postgresml/postgresml
- Timescale `pgai`: https://github.com/timescale/pgai
- MindsDB docs: https://docs.mindsdb.com/

### 2.3 AI infra 中 batch inference 是明确 workload

Ray Data 官方文档将 offline batch inference 定义为对固定输入数据集生成模型预测，并提供 Ray Dataset -> model map_batches -> output/save 的执行路径。

来源：

- Ray Data offline batch inference: https://docs.ray.io/en/latest/data/batch_inference.html

### 2.4 推理 infra 中 batching 不只是 row count

Ray Serve dynamic request batching 文档说明，ML 框架通常能同时处理多个样本；Ray Serve 会把请求排队形成 batch。文档还特别指出，很多 workload 的计算成本取决于 item 属性而不是样本数，例如 NLP Transformer 更适合按 total token count batching。

来源：

- Ray Serve dynamic request batching: https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html
- vLLM offline inference examples: https://docs.vllm.ai/en/latest/examples/offline_inference/basic.html
- vLLM automatic prefix caching: https://docs.vllm.ai/en/latest/features/automatic_prefix_caching.html

### 2.5 Ray 提供了跨层调度实验接口

Ray Data、Ray Core 和 Ray Serve 提供了可以用于验证调度假设的公开接口：

- Ray Data 支持面向 AI workload 的批处理数据处理，以及 `map_batches()` 中的 batch 和 concurrency 控制；
- Ray Core 支持 task / actor 的 CPU、GPU、自定义资源声明，以及 placement group / scheduling strategy；
- Ray Serve 支持 dynamic request batching、LLM request routing 和 autoscaling。

这说明“任务划分、并行度、资源配置、模型服务 batching / routing / backpressure”不是纯概念，具备实验实现路径。但这些文档只能证明接口存在，不能证明它们就是本课题链路中的主瓶颈。

来源：

- Ray Data overview: https://docs.ray.io/en/latest/data/data.html
- Ray Data `map_batches`: https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html
- Ray Core accelerator resources: https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html
- Ray Core placement groups: https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html
- Ray Core scheduling: https://docs.ray.io/en/latest/ray-core/scheduling/index.html
- Ray Serve LLM request routing: https://docs.ray.io/en/latest/serve/llm/architecture/routing-policies.html
- Ray Serve autoscaling: https://docs.ray.io/en/latest/serve/autoscaling-guide.html

### 2.6 对参考文本的批判性校正

前三个参考文本最有价值的地方不是“已经确定了最终论文方向”，而是提出了正确的筛选逻辑：

```text
不要先决定优化 object/fan-in/coalescing
而是先确认数据库 AI 算子场景
再确认真实瓶颈
最后收敛系统优化点
```

需要吸收的关键点：

- 当前实验只能说明方向筛选和初步动机验证，不能证明成熟论文方向。
- 三个场景不应写成三个互不相关的方向；更合理的是一个统一问题，再用 2-3 个 AI 算子 workload 验证。
- 当前 `offline_llm` fake 实验没有模拟 Transformer prefill/decode、KV cache、continuous batching、prefix cache 或 GPU 服务队列，不能支撑真实 LLM 推理收益结论。
- 当前 fine/coalesced 实验把 task 数、object 数、`ray.put` 次数、fan-in 依赖数和 operator invocation 次数一起改变了，收益来源尚未拆开。
- 新文本提醒了一个此前容易遗漏的点：partition/object 粒度会直接影响并行 task 数，不能把“粒度控制”和“并行调度”割裂；更完整的问题应包括 task/actor 并行度、CPU/GPU 资源配比、模型服务队列和 backpressure。

需要修正或暂不采信的点：

- 不能直接说“开源 PostgreSQL 已经有标准化离线 LLM workload”。更准确说法是：SQL/数据库发起 LLM 调用在商业数据仓库中已明确存在；PostgreSQL 生态有 PostgresML、pgai、MindsDB 等相关路径，但离线 LLM 生成/评测是否适合作为本课题主 workload，还需要具体平台验证。
- 不能把“场景 C 最贴近 inference infra”直接等价为“下一步主线一定选 C”。场景 C 适合作为推理 infra 压力测试，但数据库落地 baseline 仍应保留场景 A。
- 不能把当前 4x e2e 差异解释成模型推理优化收益；目前它主要是 Ray/Arrow fake 链路中的粒度敏感性信号。
- 不能把“跨层调度”直接升级成最终论文贡献。当前还没有 CPU/GPU pipeline、actor pool、request routing、queue backlog 或 backpressure 的实验数据。

## 3. 候选场景

### 场景 A：批量 Embedding / RAG ingestion

对应 SQL / AI 算子：

```sql
AI_CHUNK(text) -> chunk_text
AI_EMBED(chunk_text) -> vector
```

真实用户：

- 企业知识库 / RAG 平台；
- 数据库或数据平台提供向量检索能力的团队；
- 需要把 documents 表批量转成 vector index 的业务方。

输入：

```text
documents(doc_id, tenant_id, category, text, updated_at)
```

输出：

```text
document_embeddings(doc_id, tenant_id, category, embedding/vector)
```

为什么需要分布式 / 批处理：

- 文档行数可能很大；
- chunk + embedding 是典型 batch inference；
- 输出 embedding 较大，容易放大写回和中间 object 传输；
- 后续还要写入 pgvector / Lance / vector table。

可能瓶颈：

- chunk 后行数膨胀；
- embedding 输出对象大；
- Ray object 数量和 `ray.put` 次数；
- fan-in / shuffle / groupby by tenant；
- vector 写回小 batch / 大 batch 差异。

反证条件：

- 接真实 embedding 后，模型推理时间完全淹没数据链路；
- coalescing 对端到端没有稳定收益；
- PostgreSQL / Lance 写回成为绝对主瓶颈，object/fan-in 不再重要。

初步判断：

> 这是数据库落地最自然的 baseline 场景，但它不一定最贴近“推理 infra”。它更偏 AI data pipeline + vector ingestion。

### 场景 B：批量 AI_CLASSIFY / AI_FILTER / rerank

对应 SQL / AI 算子：

```sql
AI_CLASSIFY(text, labels) -> label
AI_FILTER(text, condition) -> bool
AI_RERANK(query, candidate_text) -> score
```

真实用户：

- 客服工单分类、舆情/合规筛查、内容审核；
- 数据分析师在 SQL 中用自然语言过滤或分类文本；
- 数据库 AI UDF / 外部执行器团队。

输入：

```text
records(row_id, tenant_id, text, metadata, candidate_label_or_query)
```

输出：

```text
classified_records(row_id, label, score, keep)
```

为什么需要分布式 / 批处理：

- 这类算子通常作用在大量表行上；
- 输出较小，但每行需要模型调用或模型服务请求；
- SQL `WHERE AI_FILTER(...)` / `JOIN ... ON AI_FILTER(...)` 可能产生大量 predicate 调用；
- 真实系统中 selectivity 可能未知，优化器很难提前估算成本。

可能瓶颈：

- 小 batch 导致模型服务吞吐低；
- LLM / reranker 调用成本远高于普通 SQL predicate；
- 过多 Ray task / object 导致调度和 object store 成本；
- filter 后输出较小，fan-in 可能不是主瓶颈；
- 可进一步研究 model cascade、predicate ordering、AI-aware query planning。

反证条件：

- 真实业务中这类 AI predicate 只用于小数据交互式查询；
- 主要瓶颈是模型质量或提示词，不是执行链路；
- 数据库侧已有成熟 batch API，外部分布式执行没有收益。

初步判断：

> 这个场景比 embedding 更贴近“AI SQL 算子执行优化”，也更容易引出 inference cost、batching、routing、selectivity 估计等 AI infra 问题。

### 场景 C：离线 LLM 生成 / 数据增强 / 评测

对应 SQL / AI 算子：

```sql
AI_COMPLETE(prompt) -> generated_text
AI_EXTRACT(text_or_file) -> json
AI_AGG(text_column, prompt) -> insight
```

真实用户：

- 离线数据增强 / 合成数据生成；
- 批量摘要、实体抽取、标签生成；
- RAG 评测、prompt evaluation、LLM 结果回填数据库；
- 企业内部推理平台或模型服务团队。

输入：

```text
tasks(task_id, tenant_id, prompt, shared_prefix, max_tokens, metadata)
```

输出：

```text
llm_outputs(task_id, generated_text/json, prompt_tokens, completion_tokens, status)
```

为什么需要分布式 / 批处理：

- 固定任务集很适合 offline batch inference；
- 输入 token 长度差异大；
- 不同 task 可能共享 system prompt / prefix；
- 需要调度模型服务、GPU、KV cache、失败重试和写回。

可能瓶颈：

- token-aware batching，而不是 row-count batching；
- prefix-aware routing / cache locality；
- prefill/decode 阶段资源不均衡；
- 长短 prompt 混批导致 padding 或等待；
- 输出写回和失败重试；
- 数据库表到模型服务之间的 backpressure。

反证条件：

- 当前环境无 GPU / 无真实 LLM 服务，长期只能 fake；
- 如果真实模型服务内部已经完全解决 batching/routing，外部数据链路优化空间可能有限；
- 如果业务只关心在线低延迟而非离线吞吐，Ray/DataFrame 路线不匹配。

初步判断：

> 这是最贴近 inference infra 的场景，但第一阶段需要把 fake token workload 做得更严谨，后续最好接 vLLM / Ray Serve / 本地小模型服务。

## 4. 场景动机测试

新增脚本：

```text
motivation/ai_operator_scenario_motivation_benchmark.py
```

正式运行命令：

```bash
.venv/bin/python motivation/ai_operator_scenario_motivation_benchmark.py \
  --scenarios embed_rag classify_filter offline_llm \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --text-tokens 32 \
  --repeats 3 \
  --output validation/results/ai_operator_scenario_motivation.csv
```

结果文件：

```text
validation/results/ai_operator_scenario_motivation.csv
```

### 4.1 实验设置

固定设置：

| 参数 | 值 | 含义 |
|---|---:|---|
| total_rows | 65536 | 每组实验固定总行数 |
| text_tokens | 32 | 每行模拟文本长度 |
| embedding_dim | 128 | 仅 `embed_rag` 使用的向量维度 |
| upstream | 8 / 32 | 上游 partition 数 |
| downstream | 8 / 32 | 下游 fan-in 分区数 |
| repeats | 3 | repeat 0 作为 warm-up，分析主要看 repeat 1/2 |

对比策略：

| 策略 | 含义 |
|---|---|
| fine | 产生 `upstream × downstream` 个小 object |
| coalesced | 按 downstream 合并为更少的大 object |

### 4.2 实验数据汇总

以下数据基于 `repeat != 0` 的平均值。

| 场景 | upstream | downstream | fine objects | coalesced objects | fine e2e ms | coalesced e2e ms | e2e 比值 | put 比值 | operator 比值 | fan-in 比值 | 输出大小 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| embed_rag | 8 | 8 | 64 | 8 | 180.04 | 134.73 | 1.34x | 3.56x | 1.52x | 1.96x | 33.56 MB |
| embed_rag | 8 | 32 | 256 | 32 | 415.94 | 178.75 | 2.33x | 6.66x | 4.40x | 1.69x | 33.56 MB |
| embed_rag | 32 | 8 | 256 | 8 | 402.24 | 139.01 | 2.89x | 18.16x | 7.08x | 3.14x | 33.56 MB |
| embed_rag | 32 | 32 | 1024 | 32 | 1168.64 | 291.70 | 4.01x | 32.72x | 11.78x | 4.03x | 33.56 MB |
| classify_filter | 8 | 8 | 64 | 8 | 142.77 | 101.33 | 1.41x | 4.94x | 2.48x | 1.37x | 2.07 MB |
| classify_filter | 8 | 32 | 256 | 32 | 345.31 | 155.32 | 2.22x | 7.43x | 5.09x | 1.15x | 2.07 MB |
| classify_filter | 32 | 8 | 256 | 8 | 343.81 | 127.66 | 2.69x | 20.01x | 7.88x | 2.65x | 2.07 MB |
| classify_filter | 32 | 32 | 1024 | 32 | 1073.00 | 248.57 | 4.32x | 28.15x | 19.21x | 2.93x | 2.07 MB |
| offline_llm | 8 | 8 | 64 | 8 | 142.60 | 100.67 | 1.42x | 4.89x | 2.52x | 1.27x | 2.56 MB |
| offline_llm | 8 | 32 | 256 | 32 | 334.22 | 155.45 | 2.15x | 6.92x | 4.52x | 1.65x | 2.56 MB |
| offline_llm | 32 | 8 | 256 | 8 | 326.97 | 127.58 | 2.56x | 19.63x | 8.31x | 2.32x | 2.56 MB |
| offline_llm | 32 | 32 | 1024 | 32 | 1105.02 | 252.89 | 4.37x | 24.60x | 18.51x | 2.65x | 2.56 MB |

## 5. 动机测试解释

### 5.1 共同现象

三个场景都显示：

> 当 object 数从几十/几百增长到 1024 时，fine 策略端到端明显慢于 coalesced。

这说明当前系统的可优化点不只是 embedding vector 写回，也包括更一般的：

- batch / partition 粒度控制；
- 减少 Ray object 数量；
- 减少 `ray.put` 次数；
- 减少小 task / 小 batch 的 operator invocation；
- 控制下游 fan-in 依赖数。

### 5.2 场景差异

`embed_rag` 的输出最大，约 33.56 MB，因此更适合暴露 vector output、RecordBatch object 和写回路径问题。

`classify_filter` 和 `offline_llm` 的输出很小，只有约 2 MB 级别，但端到端比值仍然达到 4x 以上。这说明：

> 即使输出不是大 embedding，过细 object / task 粒度也会伤害 AI 算子执行链路。

这对课题方向很重要，因为它把问题从“embedding 特例”扩展成了“AI 算子外部执行链路中的 batch / task / object 粒度问题”。

### 5.3 当前不能声称什么

不能声称：

- 真实 LLM 推理中一定有 4x 收益；
- coalescing 的收益全部来自 fan-in；
- 当前 fake `offline_llm` 已经模拟了真实 prefill/decode、KV cache、GPU batching；
- 当前结果已经足以确定最终论文方向。

当前只能说：

- 本地实验事实：三类 fake AI 算子在同一 Ray/Arrow 链路中都对 object/task 粒度敏感；
- 合理推断：AI 算子外部执行链路的 batch / partition / object 粒度控制值得继续验证；
- 待确认：真实模型服务、PostgreSQL / pgvector、Daft / Ray / Lance 真实路径中是否仍然成立。

### 5.4 动机补强实验：收益来源与反压

新增两个实验用于补足上一轮动机不足：

| 实验 | 文件 | 目的 |
|---|---|---|
| granularity attribution | `validation/results/ai_operator_granularity_attribution.csv` | 拆分 task 数、Ray 引用数、fan-in 依赖数和 operator invocation 数 |
| backpressure simulation | `validation/results/ai_operator_backpressure.csv` | 模拟模型服务消费慢时的 queue wait、token backlog 和 backpressure |

关键结果：

- 在 `upstream=32, downstream=32` 时，`fine` 策略有 `1056` 个 total Ray tasks、`1024` 个 fan-in Ray refs，`repeat 1/2` 平均 e2e 为 `139.27 ms`。
- `two_stage_coalesced` 把 fan-in Ray refs 从 `1024` 降到 `32`，但 total Ray tasks 增至 `1088`，平均 e2e 为 `176.00 ms`。
- `downstream_coalesced` 同时把 AI operator tasks 降到 `32`、fan-in Ray refs 降到 `32`，平均 e2e 为 `16.41 ms`。
- `upstream_bundled` 把 total Ray tasks 降到 `64`，即使 fan-in 逻辑 payload 仍是 `1024`，平均 e2e 为 `19.24 ms`。

解释：

> 当前 fake 链路里，收益来源不是单一 fan-in 依赖数，而是 task 数、operator invocation 数、Ray refs 和 fan-in 依赖数共同作用。只在细粒度执行之后追加 coalesce，不一定能改善端到端；更有价值的是在任务划分阶段就减少过细 AI operator invocation。

backpressure 模拟结果显示：

- 固定 `512` requests、`2` 个 replica、无界队列时，平均 queue wait 为 `4768.50 ms`，max in-flight 为 `499`，max token backlog 为 `231552`。
- 设置 `queue_limit=8` 后，tokens/s 不变，但平均 queue wait 降到 `114.41 ms`，max in-flight 降到 `8`，max token backlog 降到 `5568`。
- `4` 个 replica 时同样成立：无界队列平均 queue wait 为 `2299.64 ms`，`queue_limit=8` 时为 `37.98 ms`。

解释：

> 在模型服务吞吐固定时，无界提交不会提高 tokens/s，只会放大队列和 token backlog；有界 backpressure 可以把模型服务压力控制在较低范围。这支持继续做模型服务状态感知调度，但仍需 Ray actor / Ray Serve / vLLM 或真实模型验证。

## 6. idea-evaluator 评估

| 场景 | Paper type | Faster | Stronger | Cheaper | Broader | 主要 fatal flaw | 当前 verdict |
|---|---|---:|---:|---:|---:|---|---|
| 批量 Embedding / RAG ingestion | New Setting | 7 | 5 | 6 | 6 | 动机可能被认为只是 RAG ingestion 常规工程 | Accept with Revisions |
| AI_CLASSIFY / AI_FILTER / rerank | New Setting + Novel Problem | 7 | 6 | 7 | 7 | 需要真实 AI predicate workload，否则容易变成 fake benchmark | Accept with Revisions |
| 离线 LLM 生成 / 数据增强 / 评测 | New Setting + Frontier Exploration | 8 | 6 | 7 | 8 | 无真实模型服务时说服力不足 | Accept with Revisions |

当前没有 Strong Accept，因为三个方向都还缺真实模型/数据库形态验证。

## 7. 建议收敛方向

目前最合理的系统问题不应写成狭窄的：

> RecordBatch fan-in 优化。

上一轮候选问题是：

> 面向数据库 AI 算子的外部分布式执行链路中，如何根据 AI 算子输入输出形态控制 batch、partition、task 和 object 粒度，以降低数据传输、算子调用和下游 fan-in 成本。

结合新文本，建议进一步升级为候选问题：

> 面向数据库 AI 算子的外部分布式执行链路中，如何根据输入数据、AI 算子特征、资源状态和模型服务状态，联合决定任务划分、并行度、actor 路由、数据局部性、推理批处理与反压策略。

其中可优化点分四层：

1. **数据链路层**：RecordBatch/object coalescing、`ray.put` 数量控制、fan-in 依赖数控制。
2. **任务并行层**：partition-aware batching、task/actor 选择、batch_size × concurrency、避免过细 operator invocation。
3. **资源调度层**：CPU preprocess worker 与 GPU/model actor 配比、placement / locality、object store 压力控制。
4. **推理服务层**：token-aware batching、prefix-aware routing、replica backlog、模型服务 backpressure、失败重试和写回批处理。

更适合作为论文组织方式的是：

> 提出一套面向数据库 AI 算子的特征感知并行执行与跨层调度方法，再用 embedding/RAG、AI_FILTER/classify、offline LLM 三类 workload 做覆盖不同瓶颈形态的验证。

这比把三个场景拆成三个独立方向更稳，因为它共享同一个系统问题：AI 算子外部执行链路如何决定 batch、partition、task、actor、object、路由、反压和写回粒度。

## 8. 下一步动机测试

修正后的优先级建议：

1. **收益来源拆分实验**：固定 object 数但改变 task 数；固定 task 数但改变 object 数；固定 `ray.put` 次数但改变 fan-in 依赖数。目标是拆开 coalescing 的收益来源，避免把所有收益都归因于 fan-in。
2. **task/actor/concurrency 实验**：在同一 fake AI 算子 workload 下比较 Ray task、Ray actor、不同 actor pool size、不同 `batch_size × concurrency`。指标至少包括 rows/s、task 数、object 数、operator invocation 数、actor idle time、队列等待时间。
3. **CPU/GPU 或 CPU/model-service pipeline 实验**：模拟 CPU tokenizer / preprocess worker 与 GPU/model actor 的生产消费关系，控制 preprocess concurrency、model actor 数、in-flight batch 数和队列上限。指标至少包括吞吐、P95 latency、队列长度、object store 使用量和 backpressure 次数。
4. **offline_llm token-aware / prefix-aware routing 实验**：加入 `prompt_tokens`、`completion_tokens`、`prefix_id`、`tenant_id`，比较 row-count batching、token-budget batching、prefix-grouped routing、token-backlog routing。指标至少包括 tokens/s、batch token 分布、模拟 prefill/decode 时间、replica backlog、object 数、fan-in 时间和写回时间。
5. **classify_filter selectivity-aware 实验**：加入普通 predicate selectivity、AI predicate selectivity、cheap model / expensive model cascade，对比先普通 filter 再 AI_FILTER、先 AI_FILTER、cascade 三种路径。指标至少包括 model_calls_saved、rows/s、e2e、object 数、输出行数和动态下游 partition 数。
6. **真实形态验证**：先接 PostgreSQL + 外部 worker + pgvector / Lance 的 embedding/RAG baseline；offline LLM 只有在 PostgresML、外部 vLLM/Ray Serve 或企业目标平台接口可落地后，再作为推理 infra workload 接入。

当前不建议直接优先推进单一场景 C。更稳妥的推进顺序是：

```text
统一问题：数据库 AI 算子的特征感知并行执行与跨层调度
  -> 场景 A 作为数据库落地 baseline
  -> 场景 C 作为 inference infra 压力 workload
  -> 场景 B 作为 AI predicate / selectivity workload
```

原因：

- 场景 A 最容易和数据库 AI 算子、pgvector、Lance 对齐，适合作为真实系统 baseline。
- 场景 C 最贴近用户想学习的 inference infra，但必须先补 token-aware / prefix-aware 实验，不能只用当前 fake object 实验支撑。
- 场景 B 能补足 AI predicate、selectivity 和模型调用次数控制，但要防止课题滑向传统查询优化器。
- 三者共享 batch / partition / task / actor / object / routing / backpressure / writeback 问题，适合服务于同一个系统方法。
