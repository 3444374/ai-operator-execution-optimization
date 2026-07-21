# Existing AI Operator Execution Chains

更新日期：2026-07-12

## 结论

现有数据库 AI 算子和 AI 数据处理系统并不都使用 Ray。更准确的分类是：

| 系统 / 路线 | 用户看到的算子形态 | 公开可确认的执行链路 | 是否公开使用 Ray | 对本项目的意义 |
|---|---|---|---|---|
| Snowflake Cortex AISQL | SQL / Python AI functions，如 `AI_COMPLETE`、`AI_FILTER`、`AI_EMBED` | 托管在 Snowflake Service perimeter 内的 AI functions；官方强调 throughput 与 batch processing | 未见公开证据 | 证明数据库 AI SQL 算子是真实工业问题，但不能复现其闭源内部链路 |
| pgai Vectorizer | PostgreSQL 中声明 vectorizer pipeline | PostgreSQL + stateless vectorizer workers；worker 读取队列、调用 embedding endpoint、写回数据库 | 否 | 与本项目“外部 worker + 模型服务 + 写回”最接近 |
| PostgresML | PostgreSQL 扩展中的 `pgml.embed`、`pgml.transform` 等 | 模型靠近数据库或在数据库内/近数据库执行，强调减少数据搬运 | 否 | 代表“把模型移到数据附近”的对照路线 |
| pgvector | PostgreSQL 向量类型、索引与相似度查询 | 存储和查询向量，不负责 embedding 计算 | 否 | 是本项目 PostgreSQL 写回与检索 baseline |
| Daft + Ray | DataFrame / batch inference / AI functions | Daft 可运行在 Ray 上，负责 DataFrame、partition、batch、shuffle 等数据处理抽象 | 是，可选 Ray runner | 更适合作为后续 batch/partition 表达层，不是已有数据库 AI 算子的必要事实 |
| Ray Serve / Ray Data | Python API / serving API | Ray Data 做 batch data processing，Ray Serve 做 model serving、batching、routing、autoscaling | 是 | 适合作为本项目多 endpoint、backpressure、routing 的实验机制 |

因此，本项目不要表述为“现有 AI 算子都用 Ray，所以我们优化 Ray”。更稳的表述是：

> 现有系统已经证明数据库 AI 算子、vectorizer worker、模型服务调用、batch processing 和写回是实际工程形态；本项目选择 Ray/Daft/Lance-like 系统机制作为可控实验平台，研究数据库 AI 负载 触发后的 batch、partition、task/actor、模型服务路由、backpressure 和 writeback 优化。

## 对 Snowflake 是否需要测性能

当前不建议把 Snowflake 性能作为本项目必测 baseline。

原因：

1. Snowflake Cortex AISQL 是托管闭源系统，内部执行器、模型服务队列、调度、写回路径不可见。
2. 本项目目标是优化可控的数据执行与存储过程；Snowflake 测出来的端到端时间只能说明用户可见性能，不能拆分 `DB fetch -> batch -> scheduling -> model service -> writeback`。
3. 如果后续有 Snowflake 账号和预算，可以做“小规模用户可见参考实验”，例如同等语义的 `AI_EMBED` / `AI_COMPLETE` SQL 吞吐，但只能作为工业参照，不能作为严格 apples-to-apples baseline。

更适合当前阶段的做法是：复刻 Snowflake 用户可见的算子语义，而不是复刻 Snowflake 闭源实现。

```text
AI_EMBED(text)      -> vector
AI_FILTER(text, p)  -> boolean
AI_CLASSIFY(text)   -> label
AI_COMPLETE(prompt) -> text / json
```

然后在本项目可控链路中测：

```text
PostgreSQL fetch
  -> Arrow / batch
  -> Ray task / actor / endpoint routing
  -> HTTP / Ray Serve model service
  -> fan-in
  -> PostgreSQL / pgvector / Lance writeback
```

## 对 pgai 是否需要测性能

pgai 更值得参考，是否要测取决于实验问题。

可以测的部分：

1. PostgreSQL + vectorizer worker 的异步写回形态。
2. worker 轮询队列、失败重试、限流、批处理对端到端延迟和吞吐的影响。
3. PostgreSQL 写回与 pgvector 查询链路。

不建议把 pgai 当成长期核心依赖。它的仓库 README 标注自 2026 年 2 月起不再维护或支持，因此更适合作为架构参考和 baseline 思路，而不是论文系统的核心组件。

对本项目最有价值的是借鉴它的链路形态：

```text
application / SQL declaration
  -> PostgreSQL source table
  -> queue / vectorizer config
  -> stateless workers
  -> embedding endpoint
  -> destination table / pgvector
```

这说明后续实验必须加入“worker 写回”而不只是“driver fan-in 后统一写回”。

## 对方向的微调

当前方向不需要推翻，但需要从“Ray 调度优化”微调为：

> 面向数据库 AI 算子的模型服务感知外部执行链路优化。

更具体地说，Ray 是机制之一，不是唯一研究对象：

- Ray：task / actor / endpoint routing / backpressure / resource control。
- Daft：batch / partition / DataFrame / map_batches / shuffle 表达层。
- Lance：AI-native 或向量数据写回与外部存储候选。
- PostgreSQL / pgvector：数据库触发、结果写回、向量查询 baseline。

## 下一步实验优先级

1. 继续完善当前 GPU-backed E2E profile，固定链路分段：

```text
PostgreSQL fetch
  -> Arrow / batch 构造
  -> Ray task / actor 调度
  -> HTTP 模型服务调用墙钟时间
  -> fan-in
  -> writeback
```

2. 做 worker 写回对照：

| 对照 | 目的 |
|---|---|
| driver fan-in 后统一写回 | 当前 baseline，计时边界清楚 |
| Ray worker / actor 各自写回 | 验证是否减少 driver fan-in 与单点写回瓶颈 |
| queue / vectorizer-like worker 写回 | 模拟 pgai 式异步 worker 形态 |

3. 做写回 sink 对照：

| sink | 目的 |
|---|---|
| PostgreSQL JSON text | 当前真实模型临时 baseline |
| PostgreSQL `pgvector(384)` | 真实 embedding 维度下的数据库向量写回 |
| Lance / Parquet | 外部 AI-native 存储或文件式落盘 baseline |

4. 保留 Snowflake 为工作负载与论证参照，不把它作为当前必须复现实验。

## 证据来源

- Snowflake Cortex AISQL 官方文档：`https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql`
- Snowflake `COMPLETE` / `AI_COMPLETE` 文档：`https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex`
- pgai README：`https://github.com/timescale/pgai`
- PostgresML README：`https://github.com/postgresml/postgresml`
- pgvector README：`https://github.com/pgvector/pgvector`
- Daft on Ray 文档：`https://docs.daft.ai/en/stable/distributed/ray/`
- Ray Serve 文档：`https://docs.ray.io/en/latest/serve/index.html`
