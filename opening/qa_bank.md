# 开题答辩问答库

## 题目是否太大？

答法：

> 题目涉及数据库、Daft/Ray/Lance、GPU 模型服务和写回，但研究对象不是数据库 AI 算子本身，而是数据库 AI 负载 的分布式数据执行与存储链路。具体实验会先固定 Database source、Daft/Arrow batch、Ray task/actor、GPU endpoint、fan-in 和 Lance / pgvector / PostgreSQL sink 这条可观测路径，再逐步做消融，不会同时改造数据库内核和模型 kernel。

## 为什么不是只做 Ray？

答法：

> Ray 是可控执行机制，不是唯一研究对象。现有实验显示单 endpoint 下 Ray 不一定明显优于 Python，只有多 endpoint、路由、反压和 worker 写回等场景下才可能体现价值。因此本课题更准确地说是数据库 AI 负载的执行优化与调度研究，其中 Ray 主要承担执行调度和服务状态感知并行控制。

## 和 Snowflake / pgai / PostgresML 有什么关系？

答法：

> Snowflake 说明 AI SQL 算子是工业真实问题，但其内部执行细节闭源，不能作为可拆分 baseline。pgai 代表 PostgreSQL + 外部 vectorizer worker + embedding endpoint + 写回的路线，和本课题更接近。PostgresML 代表把模型能力放到数据库内或近数据库的路线。本课题把这些系统作为 workload 和架构背景，研究主体是 Daft/Arrow 数据组织、Ray 调度、GPU 模型服务和 Lance / 数据库 sink 之间的数据执行链路。

## 当前实验能证明什么？

答法：

> 当前实验能证明这条 GPU-backed 执行路径可以拆分计时，并且 batch、模型服务调用墙钟时间、writeback 都会显著影响端到端时间。它还不能证明最终方法已经优于所有系统，也不能证明 Ray 在所有场景都更好。

## 如果 Ray 效果不好怎么办？

答法：

> 这本身也是有价值的消融结果。课题不会只押注 Ray。后续会比较 Python worker、Ray task、Ray actor、worker 写回和 vectorizer-like 队列写回。如果 Ray 只在多 endpoint 或反压场景有效，研究内容会收敛到这些条件下的调度与资源配比。

## 为什么需要写回优化？

答法：

> 初步实验中，16384 行 Ray actor/coalesced 下 writeback 和 AI operator 阶段已经接近同一量级。如果只优化模型调用或调度，端到端收益会被持久化阶段限制。因此后续必须比较 PostgreSQL JSON、pgvector(384)、worker 写回和 Lance / Parquet 这类 sink。

## 为什么第一阶段选 AI_EMBED，而不是直接做 AI_COMPLETE？

答法：

> `AI_EMBED` 最容易形成数据库读取、GPU-backed 模型服务、向量结果写回和后续检索的真实闭环，适合建立开题阶段的端到端主动机。`AI_COMPLETE` 会引入 token-aware batching、prefix-aware routing、KV cache、模型服务队列和失败重试等更复杂指标。后续会把它作为批处理推理 workload，而不是一开始用不严谨的 fake LLM 实验支撑结论。

## 当前实验为什么不能直接写成 PG18.3 结论？

答法：

> 当前真实 GPU-backed 实验是在本地 PostgreSQL 18.4 同构预演环境中完成的，目的是验证执行路径可跑通、阶段可拆分、瓶颈形态值得继续研究。它不能代表公司 PostgreSQL 18.3 内部平台的性能。后续需要迁移到 PG18.3 平台或至少保持同构配置后重新采集。

## 为什么当前写回不是 pgvector 结论？

答法：

> 当前模型返回 384 维 embedding，但已有 pgvector 列是 `vector(128)`，所以这组实验使用 JSON text 写回。它可以说明 writeback 是端到端大块成本，但不能说明 384 维 pgvector 写回性能。下一步会补 384 维 pgvector schema 和写回对照。

## 如果模型推理时间完全淹没其他执行阶段怎么办？

答法：

> 如果真实模型服务完全主导端到端时间，就需要把问题收敛到模型服务 batch、routing、backpressure 或 workload 选择，而不是强行做 object/fan-in 优化。开题阶段的路线本来就要求通过阶段画像判断真实瓶颈，再决定优化点。

## 和传统数据库查询优化有什么区别？

答法：

> 传统查询优化主要围绕 scan、join、filter、aggregate 等数据库内部算子。本课题关注的是数据库 AI 负载 进入外部分布式数据执行系统后的过程，核心成本包括 Daft/Arrow batch、GPU endpoint、Ray task/actor、queue wait、fan-in 和 Lance / 数据库写回。AI_FILTER 的 selectivity 可能会借鉴查询优化思想，但研究边界仍是 AI 数据执行与存储协同优化。
