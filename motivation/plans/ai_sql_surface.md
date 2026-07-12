# 数据库 AI 算子场景与动机测试方案

生成日期：2026-07-10

## 1. 结论

现在确实已经有数据库或数据平台提供“内置 AI 算子 / AI SQL 函数”。

但本项目不建议直接做模型 kernel 或传统数据库 GPU 查询算子。更适合的场景是：

> 数据库内置 AI 算子背后的批量数据处理执行链路。

初步动机测试建议选择：

> 批量 Embedding / RAG 数据准备场景。

原因：

- 贴近 AI infra / inference infra；
- 和数据库内置 AI 算子关系清楚；
- 可以自然产生 batch、partition、object transfer、fan-in、shuffle；
- 可以用 Daft + Ray + Lance / Arrow 搭最小原型；
- 不需要一开始接真实大模型或 GPU，就能先验证系统瓶颈。

## 2. 现在有哪些数据库 AI 算子

| 系统 | 已有 AI / ML / 向量能力 | 对本项目的启发 |
|---|---|---|
| Snowflake Cortex AISQL | `AI_COMPLETE`、`AI_CLASSIFY`、`AI_FILTER`、`AI_AGG`、`AI_EMBED`、`AI_EXTRACT`、`AI_SENTIMENT`、`AI_SIMILARITY` 等 SQL 函数 | 说明 AI 函数进入 SQL 已是现实，且官方建议批量处理更适合 AI Functions |
| BigQuery ML / BigQuery AI | `AI.GENERATE_TEXT`、`ML.GENERATE_EMBEDDING` 等，可从表或查询生成 prompt / embedding | 说明“从表数据批量调用 AI 函数”是主流数据平台场景 |
| Oracle AI Database | `VECTOR_EMBEDDING`，用于通过 embedding 或 feature extraction 模型生成向量 | 说明传统数据库也在加入 embedding / vector 相关 SQL 能力 |
| PostgreSQL + pgai / pgvector | pgai 支持 vectorizer，pgvector 支持向量存储和相似度搜索 | 说明开源数据库生态也在做 RAG / semantic search / embedding pipeline |

参考来源：

- Snowflake Cortex AI Functions: https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql
- BigQuery `AI.GENERATE_TEXT`: https://cloud.google.com/bigquery/docs/generate-text
- BigQuery `ML.GENERATE_EMBEDDING`: https://cloud.google.com/bigquery/docs/generate-embeddings
- Oracle `VECTOR_EMBEDDING`: https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/vector_embedding.html
- pgai: https://github.com/timescale/pgai
- pgvector: https://github.com/pgvector/pgvector

### 2.1 能不能直接用开源 AI 算子

可以，但建议不要把它作为第一阶段唯一实验。

可选开源路线：

| 方案 | 能做什么 | 适合程度 | 注意事项 |
|---|---|---|---|
| PostgreSQL + pgai | 定义 vectorizer，自动从 PostgreSQL 表生成 embedding，用于 RAG / semantic search | 适合做真实业务原型参考 | pgai GitHub 仓库标注 2026 年 2 月起不再维护，不建议作为长期核心依赖 |
| PostgreSQL + pgvector | 存储 vector，做相似度搜索和 HNSW/IVFFlat 索引 | 很适合作为 embedding 输出存储和检索 baseline | pgvector 本身不负责生成 embedding |
| PostgreSQL + 外部 embedding worker | 数据库表作为任务队列，Python/Ray/Daft worker 批量生成 embedding 写回 | 最贴合本项目 | 可以自然测试外部执行链路、batch、object transfer、fan-in |
| PostgresML / MADlib 类 in-database ML | SQL 内执行部分 ML/推理任务 | 可作为参考 | 可能更偏数据库内执行，不一定贴合 Daft/Ray/Lance |

推荐策略：

> 第一阶段用 fake embedding 隔离系统瓶颈；第二阶段用 pgvector + 外部 embedding worker 或 pgai 风格 vectorizer 做真实场景验证。

原因：

- 直接使用 pgai / 真实 embedding 会引入模型、网络、worker、数据库写入等复杂因素；
- 如果一开始就慢，难判断瓶颈来自模型推理、数据库、网络，还是 Daft/Ray/Lance；
- 但最终论文不能只停留在 fake embedding，需要至少有一个真实 embedding 或真实数据库 AI 算子风格的 end-to-end 验证。

因此，“模拟”只用于 microbenchmark 和瓶颈定位，不应作为最终业务证明。

## 3. AI 算子和数据库 AI 算子的区别

| 概念 | 含义 | 本项目如何使用 |
|---|---|---|
| AI 算子 | 泛指 AI pipeline 中的处理步骤，如 embedding、分类、摘要、批量推理、特征处理 | 作为通用 workload 背景 |
| 数据库内置 AI 算子 | 数据库用 SQL 函数、表函数、UDF、外部执行器暴露 AI 能力，如 `AI_EMBED(text)` | 作为达梦/数据库落地场景 |
| 模型算子 / kernel | 深度学习框架中的底层算子，如 matmul、attention、CUDA kernel | 当前不做 |
| 传统数据库查询算子 | filter、join、aggregate、sort 等 SQL 算子 | 只在服务 AI 数据处理链路时涉及，不作为主线 |

本项目要做的是：

> 数据库 AI 算子背后的数据处理执行链路，而不是 AI 模型 kernel 本身。

## 4. 推荐初步场景：批量 Embedding / RAG 数据准备

### 4.1 场景描述

模拟一个数据库表：

```text
documents(
  doc_id,
  tenant_id,
  category,
  text,
  updated_at
)
```

用户希望执行类似：

```sql
SELECT
  doc_id,
  AI_EMBED(text) AS embedding
FROM documents
WHERE updated_at >= ?
```

或者：

```sql
CREATE TABLE document_embeddings AS
SELECT
  d.doc_id,
  d.tenant_id,
  d.category,
  AI_EMBED(chunk_text) AS embedding
FROM documents d,
     AI_CHUNK(d.text) c;
```

实际系统链路可以映射为：

```text
数据库表 / Parquet / CSV
  -> Arrow RecordBatch
  -> Daft DataFrame
  -> partition / batch
  -> Ray task
  -> embedding 或模拟 embedding
  -> fan-in / shuffle / groupby / join
  -> Lance / Parquet / Arrow 输出
```

### 4.2 为什么选这个场景

这个场景比“传统 SQL 查询”更贴近 AI infra：

- embedding 是 RAG、语义搜索、推荐、向量检索的基础；
- 批量 embedding 有明显 throughput / batching 问题；
- 文本切分、过滤、去重、join metadata、写向量表会产生数据处理链路；
- 可以自然测试 object transfer、fan-in、partition 粒度；
- 后续可以接真实 embedding 模型或 GPU，但第一阶段不需要。

## 5. 最小原型设计

第一阶段不要接真实数据库内核，也不要接真实大模型 API。先做可控模拟：

这里的“模拟”不是最终目标，而是为了先隔离系统瓶颈。

| 阶段 | 是否使用真实 AI 算子 | 目的 |
|---|---|---|
| 第一阶段 | 不使用真实模型，用 deterministic fake embedding | 隔离数据读取、batch、object transfer、fan-in、shuffle 成本 |
| 第二阶段 | 使用本地轻量 embedding 模型，或 PostgreSQL + pgvector + 外部 worker | 验证真实 embedding 计算加入后，系统瓶颈是否仍存在 |
| 第三阶段 | 可选接 pgai 风格 vectorizer、真实数据库 AI 函数或外部 inference service | 验证产品化形态，但不作为第一阶段前提 |

如果第一阶段直接接真实模型或云端 AI 函数，容易出现两个问题：

- 模型推理或网络 API 延迟掩盖 Daft/Ray/Lance 的系统瓶颈；
- 结果不可控，不利于判断 object 数量、batch size、partition 粒度是否真的影响执行链路。

因此第一阶段要模拟 `AI_EMBED(text)` 的语义，但不要模拟掉数据链路本身。

### 5.1 数据层

使用本地生成数据：

- `documents.csv` 或 `documents.parquet`；
- 字段：`doc_id`、`tenant_id`、`category`、`text`；
- 数据规模：1万、10万、100万行；
- 文本长度：短文本、中等文本、长文本。

### 5.2 AI 算子层

先用模拟 embedding 函数：

```text
text -> fixed-size numpy vector
```

不要一开始调用真实 OpenAI / 本地大模型，避免网络、模型延迟、GPU 环境污染系统瓶颈判断。

后续再加真实模型：

- 本地 sentence-transformers；
- ONNX Runtime；
- vLLM / embedding server；
- GPU batch inference。
- PostgreSQL + pgvector 存储 embedding；
- pgai 风格 vectorizer 作为数据库 AI 算子参考原型。

### 5.3 执行层

对比三条路径：

| 路径 | 作用 |
|---|---|
| Python / Arrow baseline | 判断纯本地处理成本 |
| Daft local | 判断 Daft 本地执行成本 |
| Ray fan-out/fan-in | 判断 Ray object / task / fan-in 成本 |

第一阶段可以先不强依赖 Daft，先做 Ray + Arrow RecordBatch fan-out/fan-in。确认 object 数量问题后，再接 Daft。

## 5.4 批量 Embedding / RAG 数据准备链路怎么搭

建议分四步搭建，不要一步到位。

### Step 1：生成数据库表模拟数据

生成 `documents.parquet`：

```text
doc_id: int64
tenant_id: int32
category: string
text: string
updated_at: timestamp/string
```

数据规模从小到大：

```text
10K rows -> 100K rows -> 1M rows
```

文本可以先用可控模板生成，例如不同长度的重复句子，避免随机文本生成成本污染实验。

### Step 2：实现 fake AI_EMBED

第一阶段用 deterministic fake embedding：

```text
text batch -> numpy array[batch_size, dim]
```

要求：

- 输入输出形状像真实 embedding；
- 计算量可控；
- 不访问网络；
- 不依赖 GPU；
- 结果稳定可复现。

例如：

```text
hash(text) -> fixed-size float vector
```

这一步不是为了评测模型效果，而是为了让链路具备真实 AI 算子的输入输出形态。

### Step 3：搭 Ray + Arrow RecordBatch 链路

先不要接 Daft，直接构造：

```text
Parquet / generated rows
  -> Arrow RecordBatch
  -> split into N batches
  -> Ray tasks run fake AI_EMBED
  -> produce embedding RecordBatch objects
  -> downstream fan-in / optional groupby by tenant_id
  -> write Parquet / Lance-like output
```

对比两种策略：

| 策略 | 含义 |
|---|---|
| small-object | 很多小 RecordBatch，每个 task 输出一个小 object |
| coalesced-object | 合并 batch，减少下游 fan-in 的 object 数量 |

核心变量：

- batch size；
- RecordBatch 大小；
- Ray object 数量；
- embedding dim；
- downstream fan-in 个数；
- 是否按 `tenant_id` / `category` 做 groupby 或 join metadata。

### Step 4：接 Daft/Lance

只有当 Step 3 证明 object 数量或 fan-in 是问题后，再接 Daft：

```text
daft.read_parquet / read_lance
  -> filter updated_at
  -> projection doc_id, tenant_id, text
  -> map_batches(fake AI_EMBED)
  -> repartition / groupby tenant_id
  -> collect / write parquet or lance
```

这一阶段要回答：

- Daft local 和 Ray mode 的差异；
- partition 数量是否影响端到端时间；
- batch size 是否影响 object 数量；
- Ray fan-in 现象是否在 Daft workload 中复现；
- 如果 coalescing 有收益，应该在 Daft 层做，还是 Ray/object 层做。

## 5.5 什么时候需要接真实 AI 算子

需要，但不是第一步。

建议触发条件：

1. fake embedding 链路已经证明系统瓶颈存在；
2. object / fan-in / shuffle 优化在 fake embedding 下有效；
3. 需要证明该优化在真实 AI workload 下仍然有效。

可选真实算子：

| 真实算子 | 优点 | 风险 |
|---|---|---|
| sentence-transformers 本地 embedding | 接近真实 RAG，容易理解 | 依赖模型下载，CPU 慢 |
| ONNX Runtime embedding | 更像推理 infra，性能可控 | 需要模型转换或下载 |
| vLLM / embedding server | 贴近 inference infra | 部署成本高，可能需要 GPU |
| 云端 API embedding | 接近产品形态 | 网络和费用会污染系统实验 |
| PostgreSQL + pgvector | 开源、稳定、适合存储和检索 embedding | 只负责 vector 存储/检索，不生成 embedding |
| PostgreSQL + pgai 风格 vectorizer | 更像数据库 AI 算子业务形态 | pgai 当前维护状态需谨慎，可能不适合作为长期依赖 |

论文里可以把 fake embedding 作为 microbenchmark，把真实 embedding 作为 end-to-end validation。

## 5.6 推荐的真实算子验证形态

如果要尽快减少“模拟感”，建议不要直接把 pgai 作为主系统，而是搭一个 pgai 风格的最小链路：

```text
PostgreSQL documents 表
  -> Python/Ray worker 读取待处理 rows
  -> batch embedding
  -> 写入 pgvector embedding 表
  -> 查询 / join metadata / RAG retrieval
```

然后把中间的 Python/Ray worker 替换或扩展为：

```text
PostgreSQL / Parquet
  -> Arrow RecordBatch
  -> Daft + Ray batch execution
  -> embedding worker
  -> pgvector / Lance output
```

这样比直接使用 pgai 更适合本课题，因为优化对象仍然是你关心的 AI infra 链路：

- batch；
- partition；
- Ray task；
- object transfer；
- fan-in；
- shuffle；
- embedding 输出写回。

## 6. 要测的瓶颈

| 阶段 | 指标 | 可能瓶颈 |
|---|---|---|
| 数据读取 | read time、rows/s、MB/s | CSV/Parquet/Arrow scan |
| batch 构造 | batch size、RecordBatch size | 太小 batch 导致 object 数量膨胀 |
| Ray put/get | put_ms、get_ms、roundtrip_ms | object store / serialization |
| Ray fan-in | fanin_ms、object_count | 大量小 object 拉取和依赖解析 |
| 模拟 embedding | rows/s、batch latency | 算子计算本身 |
| shuffle / groupby | end-to-end time、objects、partitions | partition 粒度和中间 object 数量 |
| 输出 | write time、file count | 小文件 / 小 batch 输出 |

## 7. 动机测试判定标准

### 支持继续做 Object Transfer / fan-in / Shuffle 优化

满足以下任意两条：

- 固定总数据量下，object 数量增加导致 fan-in 明显变慢；
- Arrow RecordBatch 也出现 many-object fan-in 放大；
- batch size 太小导致端到端吞吐下降；
- groupby / join / repartition 后 object 数量显著增加；
- coalescing / partition 调整能稳定降低端到端时间。

### 转向 Daft / Lance scan 或 pushdown

如果观察到：

- 大部分时间花在读取或过滤；
- object transfer / fan-in 不明显；
- projection / filter pushdown 能显著减少上层数据量。

### 转向真实模型推理 infra

如果观察到：

- 系统开销很小；
- 主要瓶颈是 embedding 模型推理；
- batch size、GPU 利用率、推理服务吞吐才是主问题。

此时课题可转向：

> 数据库 AI 算子的批量推理执行与调度优化。

但这会更接近 inference infra，需要重新设计实验。

## 8. 当前建议

当前最稳的第一步：

> 做“模拟数据库 AI_EMBED 算子”的批量 embedding / RAG 数据准备原型。

第一轮实验不要接真实大模型，先验证：

1. Arrow RecordBatch object 数量是否影响 Ray fan-in；
2. batch size / partition 数是否影响端到端吞吐；
3. coalescing 是否能降低 fan-in 和 shuffle 成本。

如果这些成立，再把场景升级到：

```text
Daft read parquet/lance
  -> filter / projection
  -> chunk / embedding
  -> join metadata / groupby tenant
  -> write Lance vector table
```

这样既能保持数据库 AI 算子的业务动机，也能让技术贡献落在 AI infra 的数据处理执行链路上。
