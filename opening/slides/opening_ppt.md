# 开题汇报 PPT 源稿

> 当前状态：本版内容和形式先作废，不作为下一版开题汇报 PPT 的内容依据。页面布局经验仍可保留：标题区、正文安全区、图表区、页脚和备注结构是可参考的。下一版 PPT 应重新依据 `opening/report/opening_report.md`、`figures/` 主图和当前项目口径重写。

> PPT 正式生成前，需要读取学校模板和旧版开题 PPT。当前文件是内容源稿，不直接代表最终页面排版。每页备注均保留 `汇报讲稿` 和 `答辩备注` 两块，便于后续转 PPTX。

## 1. 题目页

标题：数据库 AI 负载的执行优化与调度研究方向

副标题：Daft/Arrow 数据组织、Ray 执行调度、GPU 推理服务与 AI 数据持久化协同

页面要点：

- 学生、导师、专业方向、日期待补
- 关键词：数据库 AI 负载、Daft/Arrow batch、GPU-backed model service、Ray task/actor、Lance / pgvector sink

备注：

```text
汇报讲稿：
本课题关注数据库 AI 负载 进入分布式数据执行系统后的性能问题。研究对象不是单个模型 kernel，也不是完整改造 Ray，而是数据组织、Ray 调度、GPU 模型服务和结果持久化之间的协同。

答辩备注：
如果被问题目是否太大，回答时强调会固定 Database source、Daft/Arrow batch、Ray task/actor、GPU endpoint、fan-in 和 Lance / pgvector / PostgreSQL sink 这条可控执行路径，再逐步做消融。
```

## 2. 研究背景

主结论：数据库正在成为 AI workload 的入口，执行过程从传统 SQL 执行扩展到外部数据处理、GPU 推理服务和 AI 数据存储。

页面要点：

- Snowflake Cortex AISQL、BigQuery AI、Oracle `VECTOR_EMBEDDING` 说明 AI SQL 是真实趋势
- PostgreSQL 生态中 pgvector、pgai、PostgresML 提供向量存储、外部 vectorizer 或近数据库模型能力
- AI workload 会引入 batch、partition、模型服务、队列、fan-in、写回等新环节

备注：

```text
汇报讲稿：
传统 SQL 算子主要在数据库执行器内部完成。AI workload 不同，它经常需要把表数据组织成 batch，调用外部模型服务，并把结果写回数据库或向量存储。因此执行过程比传统查询多了数据组织、调度、模型服务队列和写回这些阶段。

答辩备注：
Snowflake 等系统只作为工业背景，不声称它们使用 Ray，也不复现其闭源内部实现。
```

## 3. 问题定义

主结论：本课题研究数据库 AI 负载 的分布式数据执行与存储链路，不研究数据库 GPU kernel 或单个模型优化。

页面要点：

```text
Database AI workload source
  -> Daft / Arrow batch
  -> Ray task / actor
  -> GPU-backed model endpoint
  -> fan-in
  -> Lance / pgvector / PostgreSQL sink
```

- 关注端到端吞吐和阶段占比
- 关注 batch、partition、task/actor、endpoint routing、backpressure、writeback
- Ray 是机制之一，不是题目本身

备注：

```text
汇报讲稿：
我把研究对象收敛为这条执行路径：数据库表或 SQL 工作流提供 AI workload，Daft/Arrow 组织 batch，Ray 调度执行，GPU endpoint 完成推理，最后写入 Lance、pgvector 或 PostgreSQL。这里的核心问题是如何定位和优化各阶段的系统成本。

答辩备注：
不能把题目解释成“优化 Ray”。Ray 是执行调度机制之一，真正问题是数据库 AI 负载 的数据组织、服务状态感知调度和持久化协同。
```

## 4. 相关系统

主结论：现有系统提供了场景和机制依据，但缺少可控的端到端阶段拆分。

页面要点：

| 类别 | 代表 | 对本课题的作用 |
|---|---|---|
| AI SQL | Snowflake Cortex AISQL | 证明场景真实 |
| PostgreSQL AI | pgai / pgvector / PostgresML | 提供外部 worker、向量写回和近数据库对照 |
| 执行框架 | Daft / Ray | 提供 batch、partition、shuffle、task、actor 机制 |
| AI 数据存储 | Arrow / Lance | 提供列式中间表示和 AI 数据存储参考 |

备注：

```text
汇报讲稿：
现有系统对本课题的意义不同。Snowflake 证明 AI SQL 是真实需求；pgai 更接近外部 worker 加写回形态；Daft 和 Ray 提供可控的数据组织与执行机制；Arrow 和 Lance 提供数据表示和 AI 数据存储参考。

答辩备注：
强调不会把 pgai 作为长期核心依赖，尤其它的维护状态需要谨慎；更重要的是借鉴外部 vectorizer worker 的执行形态。
```

## 5. 研究缺口

主结论：现有资料难以同时回答数据组织、模型服务、调度、fan-in 和持久化写回各占多少成本。

页面要点：

- 托管 AI SQL 系统内部执行细节不可见
- 单纯 Ray / Daft benchmark 脱离数据库 workload、模型服务和持久化写回
- 单纯模型推理 benchmark 看不到数据库 fetch、batch、fan-in、writeback
- 需要数据库触发、GPU endpoint 和写回共同参与的阶段画像

备注：

```text
汇报讲稿：
如果只测模型服务，就看不到数据组织和写回；如果只测 Ray，就脱离数据库驱动 workload 和 AI 数据 sink；如果只看 Snowflake 端到端时间，又拆不开内部阶段。因此我先构建可控执行路径，把阶段时间拆开。

答辩备注：
研究缺口不要说成“没人做过 AI SQL”，而是“缺少面向本课题执行路径的可控阶段画像与优化验证”。
```

## 6. 技术路线

主结论：先构建可控执行路径，再基于阶段画像做大块消融和策略优化。

页面要点：

1. 构建 `AI_EMBED` 端到端执行路径
2. 记录 DB fetch、Daft/Arrow build、operator wall、model request wall、fan-in、sink writeback
3. 对比 Python、Ray task、Ray actor、single/multi endpoint、fine/coalesced
4. 扩展到 worker writeback、bounded in-flight、pgvector(384)、Lance / Parquet sink
5. 扩展到 `AI_FILTER/AI_CLASSIFY` 和 `AI_COMPLETE`

备注：

```text
汇报讲稿：
技术路线不是一开始就提出复杂调度器，而是先把执行路径跑通并拆分阶段。只有知道哪些阶段真正大，后续的优化才有依据。

答辩备注：
如果被问为什么先做 embedding，回答它最容易形成数据库读取、模型服务、向量写回的闭环，但后续不会只停留在 embedding。
```

## 7. 初步实验设置

主结论：当前已有一条真实 GPU-backed embedding 执行路径作为开题主动机。

页面要点：

- 数据库：本地 PostgreSQL 18.4 同构预演
- 模型：`sentence-transformers/all-MiniLM-L6-v2`
- Endpoint：OpenAI-compatible HTTP embedding endpoint on CUDA
- GPU：NVIDIA GeForce RTX 5070
- 执行器：Python / Ray task / Ray actor
- 写回：当前为 JSON text，后续补 384 维 pgvector

备注：

```text
汇报讲稿：
这组实验不是最终系统性能结论，而是开题阶段的真实执行路径主动机。它已经包含数据库读取、外部执行、真实 GPU 模型端点和写回。

答辩备注：
必须主动说明当前是 PG18.4 本地预演，不是 PostgreSQL 18.3 平台结果；当前真实模型返回 384 维，但写回仍是 JSON text。
```

## 8. 初步结果 1：Batch 粒度

主结论：逐行 endpoint 调用比 batch 调用慢约 `13.4x`。

页面要点：

| Rows | Strategy | Calls | E2E |
|---:|---|---:|---:|
| 1024 | coalesced | 4 | 0.888s |
| 1024 | fine | 1024 | 11.925s |

- fine 的 `operator_wall_s = 11.528s`
- fine 的 `bounded_wait_s = 10.124s`
- 说明 endpoint 调用粒度是一阶执行成本

备注：

```text
汇报讲稿：
同样是 1024 行，逐行调用会产生 1024 次 endpoint 请求，而 coalesced 只有 4 次请求。真实 GPU 模型加入后，调用粒度仍然会显著影响端到端时间。

答辩备注：
这里不能说优化了模型推理 kernel。收益来自调用粒度和外部 operator 阶段，不是 GPU kernel 优化。
```

## 9. 初步结果 2：Ray 的价值边界

主结论：单 endpoint 下 Ray 不天然优于 Python，多 endpoint 下才开始体现调度价值。

页面要点：

- 单 endpoint、4096 行 coalesced：Python / Ray task / Ray actor 约 `3.29s-3.36s`
- 双 endpoint、4096 行 coalesced：
  - Python：`3.444s`
  - Ray task：`2.761s`
  - Ray actor：`2.780s`
- Ray 主要降低 external operator wall time

备注：

```text
汇报讲稿：
这组结果说明我不会把 Ray 当成天然加速器。单 endpoint 时差距不明显；当有多个模型 endpoint 可以并发路由时，Ray task 和 actor 才开始显示价值。

答辩备注：
不能声称这是多 GPU 实验；当前两个 endpoint 在本地同一 GPU 上，作用是验证路由和并发机制。
```

## 10. 初步结果 3：Writeback 成本

主结论：16K 行时外部 operator 和 PostgreSQL writeback 都接近端到端时间的一半。

页面要点：

| Rows | Executor | Strategy | Operator wall | Writeback | E2E |
|---:|---|---|---:|---:|---:|
| 16384 | Ray actor | coalesced | 6.473s | 6.586s | 13.168s |

- 只优化模型调用可能被写回限制
- 后续需要 driver writeback vs worker writeback
- 需要 JSON text vs pgvector(384) vs Lance / Parquet 对照

备注：

```text
汇报讲稿：
这个结果是当前最重要的动机之一。模型服务调用不是唯一大块成本，写回同样大。如果后续只做调度或模型服务优化，端到端收益会被写回限制。

答辩备注：
当前写回不是 384 维 pgvector，所以只能作为写回成本进入动机，不能作为 pgvector 性能结论。
```

## 11. 研究内容

主结论：围绕数据组织、Ray 调度反压和持久化协同展开。

页面要点：

1. AI workload 感知的数据组织与批处理调度
2. Daft/Arrow batch、partition、object 粒度控制
3. GPU 服务状态感知的 Ray routing / backpressure
4. driver / worker / queue 写回与 Lance / pgvector sink 协同
5. `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 三类 workload 验证

备注：

```text
汇报讲稿：
研究内容会从一个统一问题展开，而不是把三个 AI workload 写成三个独立方向。它们共享数据组织、Ray 调度、模型服务和持久化写回这些系统问题。

答辩备注：
如果被问创新点是否分散，回答三类 workload 服务同一套数据执行与存储协同优化方法。
```

## 12. 预期创新点

主结论：创新点按“数据组织、服务感知调度、持久化协同”组织。

页面要点：

- 数据组织与批处理调度：把 DB、Daft/Arrow batch、Ray execution、model request、fan-in、sink writeback 拆开
- AI workload 特征感知粒度控制：embedding / predicate / LLM 对应不同 batch、partition 和 routing 特征
- GPU 服务状态感知调度：endpoint、in-flight、queue wait、token backlog、GPU utilization
- 持久化协同优化：driver fan-in、worker writeback、vectorizer-like queue、pgvector / Lance sink

备注：

```text
汇报讲稿：
这些都是预期创新点，还需要后续实验验证。开题阶段不把初步结果写成已经完成的最终贡献。

答辩备注：
创新点要和研究内容、实验指标一一对应，避免只写概念。
```

## 13. 实验计划

主结论：先用 `AI_EMBED` 建立真实闭环，再扩展到 AI predicate 和 offline LLM。

页面要点：

| 阶段 | Workload | 重点 |
|---|---|---|
| 1 | `AI_EMBED` | batch、endpoint、pgvector/writeback |
| 2 | `AI_FILTER/AI_CLASSIFY` | selectivity、cascade、model calls |
| 3 | `AI_COMPLETE` | token-aware batching、prefix routing、queue backlog |

- 统一使用 Database source / Daft-Arrow batch / Ray execution / model service / Lance-数据库 sink 框架
- CPU/fake 只作为脚本调试和历史对照

备注：

```text
汇报讲稿：
Embedding 是第一组真实闭环，不是最终唯一场景。后续 AI_COMPLETE 会引入 token、prefix、队列和失败重试等更复杂的模型服务状态，用来检验方法是否能覆盖批处理推理场景。

答辩备注：
如果被问为什么不直接做 LLM，回答 LLM 场景需要更严谨的 token、prefix、队列和服务端指标，先用 embedding 建立数据库闭环更稳。
```

## 14. 风险与控制

主结论：主要风险来自平台差异、模型服务复杂度、写回口径和题目发散。

页面要点：

| 风险 | 控制方式 |
|---|---|
| PG18.4 结果不能代表 PG18.3 | 明确标注本地预演，后续迁移内部平台 |
| Ray 效果不稳定 | 保留 Python / worker / vectorizer-like baseline |
| GPU/model service 掩盖其他执行阶段 | 使用阶段计时和多 workload 判断 |
| 写回成为绝对瓶颈 | 纳入 worker writeback、pgvector、Lance 对照 |
| 题目发散 | 不做 GPU kernel，不改造完整 Ray，不复现闭源系统，不做简单 Daft-Ray-Lance 产品集成 |

备注：

```text
汇报讲稿：
这几个风险都不是回避，而是通过实验设计控制。尤其是 Ray 和写回，如果结果不支持预期，就会收敛研究问题，而不是强行解释。

答辩备注：
主动说明不能声称什么，比被问到后再补充更稳。
```

## 15. 进度安排

主结论：开题后按真实执行路径消融、场景扩展、方法收敛和论文整理推进。

页面要点：

- 2026.07：开题材料、GPU-backed 动机结果整理、pgvector(384) 设计
- 2026.08：`AI_EMBED` 大块消融和 worker writeback
- 2026.09：`AI_FILTER/AI_CLASSIFY` selectivity-aware 实验
- 2026.10：`AI_COMPLETE` token-aware / prefix-aware / queue-aware 实验
- 2026.11：统一方法、baseline、反证实验
- 2026.12+：论文、图表、答辩材料

备注：

```text
汇报讲稿：
时间安排按先闭环、再消融、再扩展 workload 的顺序推进。每个阶段都保留可反证条件，避免只朝一个预设结论跑。

答辩备注：
如果学校进度节点不同，需要按学院模板调整。
```

## 16. 总结

主结论：题目有真实场景、已有初步阶段画像证据，后续路线可执行且边界明确。

页面要点：

- 场景真实：数据库 AI SQL / AI vectorizer 已经出现
- 问题明确：AI 数据执行中的 batch、调度、模型服务、写回
- 证据初步成立：fine vs coalesced、multi endpoint、writeback 成本
- 后续聚焦：数据组织、模型服务感知调度与持久化协同
- 边界清楚：不做数据库 GPU kernel，不改造完整 Ray，不把 CPU/fake 当最终结论

备注：

```text
汇报讲稿：
最后总结为三点：第一，数据库 AI 负载 带来了新的分布式数据执行问题；第二，真实 GPU-backed 画像已经显示 batch 和写回是大块成本；第三，后续会围绕数据组织、模型服务感知调度和持久化协同做可验证优化。

答辩备注：
回答问题时优先回到阶段画像和不能声称的边界。
```
