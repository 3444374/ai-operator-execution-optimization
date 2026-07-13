# 硕士生论文开题报告

**题 目：** 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究

| 项目 | 内容 |
|---|---|
| 学 号 |  |
| 姓 名 |  |
| 专 业 |  |
| 指 导 教 师 |  |
| 院（系、所） | 计算机科学与技术学院 |

**华中科技大学研究生院制**

## 1. 课题背景、目的和意义

数据库系统正在从管理结构化数据，扩展到承载文本、图像、向量和模型推理结果等 AI 数据处理任务。Snowflake Cortex AISQL、BigQuery AI、Oracle `VECTOR_EMBEDDING`、PostgreSQL 生态中的 pgvector、pgai 和 PostgresML 等系统或组件表明，用户已经希望在 SQL 或数据库工作流中直接发起 embedding、分类、过滤、摘要、抽取和生成等 AI 操作。结合本项目已有场景分析，当前可落地的数据库 AI 算子主要包括三类：批量 embedding / RAG ingestion、`AI_FILTER` / `AI_CLASSIFY` 类 AI predicate，以及 `AI_COMPLETE` / 离线 LLM 生成与抽取。

这类操作和传统 SQL 算子不同。传统数据库执行器主要关注 scan、filter、join、aggregate、sort 等关系算子，而 AI workload 会把数据库变成数据入口和结果落点：表数据需要被组织为 batch 或 Arrow RecordBatch，经由 Daft 类数据处理层完成 partition、batch、shuffle 等数据流组织，再交给 Ray task/actor 或 actor pool 调度 GPU-backed 模型服务，最后将 embedding、分类结果或生成文本写入 Lance、pgvector、PostgreSQL 表或其他 AI 数据存储。端到端性能不只由模型推理决定，还可能受数据组织、任务划分、模型服务队列、fan-in、写回批量和反压策略影响。

本课题的研究目的，是面向数据库驱动的 AI workload，建立一条可观测、可替换、可消融的分布式数据执行与存储链路，并研究 Daft/Ray/Lance 类系统中数据组织、并行调度、GPU 推理服务调用和结果持久化之间的协同优化方法。数据库 AI 算子在本文中主要作为 workload 入口和验证场景，不作为单独的数据库内核问题；Daft、Ray 和 Lance 则分别对应数据流组织、分布式执行调度和 AI 数据持久化三个系统层次。与传统数据库 GPU 查询算子或模型 kernel 优化相比，本课题更关注 AI workload 在数据执行系统中的批处理、调度、反压和存储协同。

课题的理论意义在于，将数据库 AI workload 从单一 SQL 函数调用扩展为可观测、可拆分、可优化的数据执行系统问题，补充现有数据库执行优化、分布式数据处理框架和 AI 推理服务系统之间的研究空白。课题的应用意义在于，为 PostgreSQL / pgvector、Daft/Ray 执行层、GPU-backed 模型服务和 Lance 类 AI 数据存储之间的协同执行提供实验依据和方法参考。

## 2. 国内外研究现状

### 2.1 数据库 AI SQL 算子现状

Snowflake Cortex AISQL 提供 `AI_COMPLETE`、`AI_CLASSIFY`、`AI_FILTER`、`AI_EMBED` 等函数，说明 AI SQL 算子已经进入工业系统。BigQuery ML / BigQuery AI 和 Oracle AI Vector Search 也提供了从表数据调用模型或生成向量的能力。这些系统证明数据库已经成为 AI workload 的重要入口，但多数托管系统内部执行细节不可见，难以作为本课题可拆分的实验 baseline。

这类系统说明，研究重点不应停留在“模型能否被 SQL 调用”，而应进一步分析大量表数据进入 AI 数据执行系统后，如何被批处理、调度、推理和持久化。Snowflake 文档中 embedding、filter/classify、completion 等函数的并存，对应本项目三类 workload 的划分：向量生成与写回、AI predicate 批处理、离线生成式推理。它们用于定义工作负载，不直接决定本文的系统实现路线。

### 2.2 PostgreSQL AI 生态现状

PostgreSQL 生态中，pgvector 负责向量类型、索引和相似度检索；pgai 曾提供 PostgreSQL + stateless vectorizer worker + embedding endpoint + 写回数据库的执行形态；PostgresML 代表把模型能力放到数据库内或近数据库执行的路线。这些系统说明 PostgreSQL 生态确实存在 AI 数据处理需求，但也提示本课题需要区分“模型靠近数据库执行”和“外部 worker / 模型服务执行”两种路线。

pgvector 能支撑向量结果的存储和检索，但不负责 embedding 计算。pgai 的 vectorizer worker 形态更接近本课题关注的数据库表、外部模型服务和写回过程，但其工程实现不宜直接作为长期核心依赖。PostgresML 代表另一类对照路线，即把模型执行能力放在数据库内或数据库附近。

### 2.3 分布式数据与 AI 执行框架现状

Ray 支持 task、actor、object store、资源声明和模型服务相关组件；Daft 可以运行在 Ray 上，提供 partition、batch、shuffle、join 等数据处理抽象；Lance 和 Arrow 面向列式、向量和 AI 数据场景。这些框架共同构成本文关注的 AI 数据执行链路：Daft 负责数据组织和批处理计划，Ray 负责分布式任务执行和资源调度，Lance / pgvector / PostgreSQL 负责结果持久化和后续检索。本文不把 Daft/Ray/Lance 写成固定产品集成，而是以它们为开放可控的系统载体，研究数据库驱动 AI workload 在数据组织、调度和存储层之间的性能问题。

Spark SQL、Daft 和 Ray 文档都强调 partition、shuffle、batch、object 数量和 task 粒度对性能的影响。它们说明本课题关注的粒度控制和数据移动问题有系统研究基础。但数据库驱动 AI workload 还额外引入模型服务队列、GPU-backed endpoint、token-aware batching、prefix-aware routing、写回一致性和失败重试等问题，需要在真实 GPU-backed 执行链路中重新验证。

### 2.4 当前研究存在的问题

当前研究和系统实践中仍存在以下问题。

第一，现有 AI SQL 系统证明了需求，但托管系统内部执行过程通常不可见，难以拆分数据库读取、批处理构造、模型服务调用、fan-in 和写回的阶段成本。

第二，现有分布式执行框架提供了 task、actor、batch、shuffle、routing 等机制，但这些机制在数据库驱动 AI workload 中如何与 GPU 推理服务和 AI 数据存储组合，仍缺少面向端到端执行过程的验证。

第三，模型服务侧的 queue wait、bounded in-flight、token backlog、endpoint routing 与数据库侧 writeback 往往被分开讨论，缺少统一的协同优化视角。

第四，embedding、AI predicate 和 offline LLM completion 的执行压力不同。单一 embedding 场景不足以证明方法适用于更一般的数据库驱动 AI workload。

第五，已有本地预研显示，task 数、operator invocation、object 数、fan-in 依赖和模型服务队列可能同时影响执行过程。仅把问题写成 object/fan-in 或仅写成 Ray 调度都过窄；需要把 Daft 数据组织、Ray 并行执行、GPU 模型服务状态和 Lance / 数据库写回压力放在同一条 AI 数据执行路径中分析。

## 3. 研究目标与研究内容

### 3.1 研究目标

本课题的总体目标是：面向数据库驱动 AI workload，构建基于 Daft/Ray/Lance 类系统机制的端到端实验链路，分析数据组织、task/actor 调度、GPU 模型服务请求、fan-in、backpressure 和持久化写回的瓶颈形态，并提出分布式数据执行与存储协同优化方法。

具体目标包括：

1. 建立数据库驱动 AI workload 的分阶段执行画像方法，明确 DB fetch、Arrow/Daft batch、Ray task/actor、model request wall、fan-in 和 sink writeback 等阶段边界。
2. 分析 batch 粒度、partition 数、task/actor 并行度、endpoint routing 和 bounded in-flight 对端到端性能的影响。
3. 研究 GPU 推理服务状态感知的 Ray 并行调度与反压策略，避免无界提交导致 queue wait 和 token backlog 放大。
4. 研究结果汇聚与 AI 数据持久化协同优化，比较 driver fan-in 后统一写回、worker 写回、vectorizer-like queue worker 写回和 Lance / pgvector 等 sink。
5. 通过 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY` 和 `AI_COMPLETE` 三类 workload 验证方法的适用范围，分别覆盖向量写回、AI predicate 选择率和 token / prefix / queue 感知推理三类压力。

### 3.2 研究内容

阶段划分、执行画像和瓶颈归因用于支撑动机测试、方案设计和效果评价。围绕这些观测结果，本课题进一步研究以下三个可优化、可验证的方法问题。

研究内容一：AI workload 感知的数据组织与批处理执行调度方法。

数据库驱动 AI workload 进入 Daft/Ray 类执行系统时，batch size、partition 数、task/actor 粒度、object 合并方式、endpoint routing 和 in-flight 请求数需要一起考虑。难点在于这些变量会相互影响：增大 batch 可以减少模型请求次数，但也会改变 partition 粒度、object 数量和长尾等待；提高并发度可能提升多 endpoint 利用率，也可能放大 queue wait、replica backlog 和 fan-in 压力；不同 workload 的输出大小、selectivity、token 长度和共享 prefix 又会改变合适的策略。如果把 Daft batch、Ray 并行或 endpoint routing 分开处理，容易得到局部有效但端到端收益不稳定的方案。

本课题拟把 workload 特征和模型服务状态同时纳入执行策略。策略输入包括输入行数、输出大小、token 数、selectivity、共享 prefix、endpoint 数量、queue wait、bounded wait、replica backlog 和 GPU utilization；策略输出包括 Daft/Arrow batch、partition、Ray task/actor、routing 和 backpressure 的组合选择。实验中比较逐行调用与 batch 调用、不同 partition 数、Python 顺序执行、Ray task、Ray actor、single endpoint、multi endpoint、unbounded 与 bounded in-flight 等方案，分析数据组织和模型服务状态共同作用时，批处理并行调度在哪些条件下有效。

评价时与固定 batch、固定 partition、固定并发度、Python 顺序执行、Ray task 和 Ray actor 等方案对比。指标包括端到端耗时、rows/s、tokens/s、operator wall time、model request wall time、queue wait、bounded wait、object 数、task 数、fan-in time、endpoint 利用率和 GPU utilization。消融实验分别去掉 routing、去掉 bounded in-flight、固定 batch 粒度或固定 worker 形态，用来判断收益来自模型请求合并、调度并行、服务端排队控制，还是 object / fan-in 减少。

研究内容二：GPU 推理服务状态感知的 Ray 并行调度与反压控制方法。

AI workload 的执行瓶颈不只来自数据切分，也来自 GPU 推理服务的动态状态。难点在于模型服务的 queue wait、replica backlog、bounded wait、GPU utilization 和 token throughput 会随 batch、并发度和请求长度变化；Ray task/actor 提交过快时，表面上提高了并行度，实际可能只是把等待堆到模型服务队列中。单 endpoint 下 Ray 不一定优于 Python，多 endpoint 或多 replica 下 Ray 才可能通过并发 routing、actor pool 和反压控制体现价值。

本课题拟构建 GPU 推理服务状态感知的 Ray 调度策略，在请求提交前根据 endpoint 数量、队列等待、replica backlog、token 长度和 GPU 利用率调节 actor 数、batch 分配、routing 和 in-flight 上限。该方法不把 Ray 简化为“是否比 Python 快”的二元对比，而是研究 Ray runtime 在多 endpoint、多 replica、token 长短不均和服务端排队存在时如何调节任务粒度和资源使用。

评价时比较 Python 顺序执行、Ray task、Ray actor、actor pool、single endpoint、multi endpoint、unbounded in-flight 和 bounded in-flight 等方案。指标包括端到端耗时、operator wall time、model request wall time、queue wait、bounded wait、tokens/s、endpoint 利用率和 GPU utilization。消融实验分别固定 batch 粒度只改变 Ray 形态，固定 Ray 形态只改变 endpoint 数量，固定 endpoint 只改变 in-flight 策略，用来判断收益来自并发 routing、服务端排队控制还是单纯请求合并。

研究内容三：面向 AI 数据流的结果汇聚与 Lance / 数据库持久化协同方法。

模型调用阶段被优化后，结果持久化可能成为新的端到端限制。本部分研究 fan-in、worker 并行度和 AI 数据 sink 如何配合。难点在于 AI workload 结果不一定是传统标量：embedding 会产生高维向量，AI predicate 会改变写回行数，LLM 类 workload 会产生变长文本。所有结果先在 driver fan-in 后统一写回，可能抵消并行阶段收益；worker 各自写回，又会带来连接、事务、批量写入、失败重试和一致性控制问题。Lance / Parquet、pgvector 和 PostgreSQL JSON text 的写入特征不同，也会改变上游调度策略的收益边界。

本课题拟比较 driver fan-in 后统一写回、worker-side writeback、vectorizer-like queue worker 写回等方式，并在 PostgreSQL JSON、pgvector(384)、Lance / Parquet 等 sink 上记录端到端影响。同时分析模型调用并行度、fan-in 位置、写回批量大小、连接数和 sink 类型之间的关系，使持久化策略能够与上游 Daft batch、Ray 调度和反压策略配合。

评价时比较不同写回路径的 writeback time、端到端耗时、吞吐、失败重试成本和结果一致性边界。消融实验分别固定模型调用策略只改变写回方式，固定写回方式只改变 worker 并行度，固定 sink 只改变写回批量大小，以判断写回是否限制模型服务优化收益，以及 worker-side writeback 或 queue worker 是否能降低 driver fan-in 压力。三类 workload 用作适用边界验证：embedding 关注向量写回，AI predicate 关注选择率和下游数据量变化，LLM 类 workload 关注 token / prefix / queue-aware 调度。

### 3.3 总体研究框架

本课题的总体框架如图 3-1 所示。数据库 AI workload 是场景入口，统一进入由 Daft/Arrow 数据组织、Ray 分布式执行、GPU 模型服务和 Lance / 数据库 sink 组成的可观测执行链路；研究内容一关注数据组织与批处理调度，研究内容二关注 Ray 并行调度与模型服务反压，研究内容三关注结果汇聚与持久化协同。三类 workload 用于验证策略边界，避免把方法只建立在单一 embedding 场景上。

```mermaid
flowchart LR
  subgraph S[数据库驱动 AI workload]
    S1[AI_EMBED / RAG ingestion]
    S2[AI_FILTER / AI_CLASSIFY]
    S3[AI_COMPLETE / 抽取与生成]
  end

  subgraph P[分布式 AI 数据执行链路]
    P1[Database source / SQL workflow]
    P2[Daft / Arrow batch 与 partition]
    P3[Ray task / actor / object]
    P4[GPU-backed model service]
    P5[fan-in / result consolidation]
    P6[Lance / pgvector / PostgreSQL sink]
  end

  subgraph R[研究内容]
    R1[workload 感知数据组织与批处理调度]
    R2[GPU 服务感知 Ray 调度与反压]
    R3[AI 数据流汇聚与持久化协同]
  end

  subgraph E[评价与证据]
    E1[阶段耗时 / rows/s / tokens/s]
    E2[queue wait / bounded wait / GPU utilization]
    E3[writeback time / fan-in time / 消融实验]
  end

  S1 --> P1
  S2 --> P1
  S3 --> P1
  P1 --> P2 --> P3 --> P4 --> P5 --> P6
  P2 --> R1
  P3 --> R2
  P4 --> R2
  P5 --> R3
  P6 --> R3
  R1 --> R2
  R2 --> R3
  R1 --> E1
  R1 --> E2
  R2 --> E2
  R3 --> E1
  R3 --> E3
```

图 3-1 课题总体研究框架。数据库 AI workload 作为入口，Daft/Arrow、Ray、GPU 模型服务和 Lance / 数据库 sink 共同构成研究对象；数据组织、服务感知调度和持久化协同分别构成三个研究内容，并通过对照实验和消融实验验证。

## 4. 研究方案与可行性分析

### 4.1 研究方案

本课题采用“可控执行路径构建 -> 阶段画像 -> 大块消融 -> 方法设计 -> 多 workload 验证”的研究路线。

基础执行路径如下：

```text
Database AI workload source
  -> Daft / Arrow RecordBatch / batch construction
  -> Ray task / Ray actor / actor pool
  -> GPU-backed model service
  -> fan-in / result consolidation
  -> Lance / pgvector / PostgreSQL sink
```

第一阶段以 `AI_EMBED(text)` 为主，跑通真实 GPU-backed embedding endpoint，并记录分阶段指标。第二阶段做大块消融，包括 Python vs Ray、single endpoint vs multi endpoint、fine vs coalesced batch、driver writeback vs worker writeback、unbounded vs bounded in-flight。第三阶段引入 `AI_FILTER/AI_CLASSIFY` 与 `AI_COMPLETE`，验证方法是否能覆盖输出小但调用多、token 长度不均、prefix 共享和队列反压等不同瓶颈形态。

评价指标包括端到端耗时、rows/s、tokens/s、operator wall time、model request wall time、queue wait、bounded wait、fan-in time、writeback time、object 数、task 数、endpoint 利用情况和 GPU utilization。实验分析将区分实验现象、原因解释、适用边界和后续待验证问题。

拟解决的关键技术问题包括：

1. 数据库驱动 AI workload 的端到端成本如何拆分。需要避免只用总耗时判断系统瓶颈，而要把数据库读取、Daft / Arrow batch 构造、Ray execution、模型服务请求、fan-in 和 sink writeback 分开记录。
2. 数据组织和任务粒度如何影响批处理执行过程。初步实验显示逐行模型调用会显著放大 external operator wall time，但实际收益来源可能同时来自 partition 数、task 数、operator invocation 数、Ray refs、object 数和 fan-in 依赖数，需要进一步消融。
3. Ray 的价值边界是什么。当前单 endpoint 下 Python、Ray task、Ray actor 差距不大；多 endpoint 下 Ray 开始体现并发路由价值。因此需要研究 Ray 在多 replica、routing、反压和 worker 写回中的适用条件。
4. 持久化写回是否会限制端到端收益。真实 GPU-backed 画像显示，在 16K 行 coalesced 执行过程中，external operator 阶段和 PostgreSQL writeback 都接近端到端时间的一半。后续如果只优化模型调用，收益可能被 Lance / pgvector / PostgreSQL sink 阶段限制。
5. 如何从 embedding 场景扩展到更一般的 AI workload。`AI_EMBED` 容易形成 pgvector 写回闭环；`AI_COMPLETE` 会引入 token-aware batching、prefix-aware routing、模型服务队列和失败重试；`AI_FILTER/AI_CLASSIFY` 则需要 selectivity-aware 执行和 cascade 策略。三类 workload 对应不同的执行压力，用来验证方法是否只适用于单一 embedding 场景。

### 4.2 可行性分析

目前已完成本地 PostgreSQL 18.4 同构预演环境、PG18.4 + pgvector 连接验证、fake-model 画像、真实 GPU-backed embedding 端到端画像和双 endpoint Ray 动机测试。PG18.4 仅作为 PostgreSQL 18.3 内部平台的本地预演替身，相关结果用于验证实验方法和瓶颈形态，不代表 PostgreSQL 18.3 内部平台性能。

表 4-1 汇总了当前可行性证据的来源、作用和边界。可以看出，本课题已经具备数据库读写、Arrow batch、Ray task/actor、GPU-backed endpoint 和写回阶段计时的基础；同时，CPU/fake 结果只用于解释早期问题来源，不能作为真实 GPU 链路结论。

```mermaid
flowchart LR
  A[PostgreSQL 18.4 同构预演] --> B[Arrow / batch 构造]
  B --> C[Python / Ray task / Ray actor]
  C --> D[CUDA embedding endpoint]
  D --> E[fan-in / result consolidation]
  E --> F[PostgreSQL writeback]
  C -. 对照 .-> G[Python baseline]
  C -. 多 endpoint .-> H[endpoint 8000 / 8001]
```

图 4-1 当前已跑通的 GPU-backed 数据库驱动 AI workload 画像链路。该链路覆盖数据库读取、batch 构造、Ray/Python 执行、模型服务调用、fan-in 和写回，能够支撑后续数据组织、调度与写回协同优化实验。

| 证据来源 | 已完成内容 | 支撑的可行性 | 边界 |
|---|---|---|---|
| PG18.4 连接验证 | PostgreSQL 18.4 + pgvector 可连接、可读写 | 数据库和向量扩展环境可用 | 只证明环境可用，不证明性能收益 |
| PG18.4 fake-model 画像 | PostgreSQL -> Arrow -> Python/Ray -> fake operator -> writeback | 阶段计时口径和脚本链路可运行 | fake-model 结果不能外推为真实 GPU 瓶颈 |
| GPU-backed `AI_EMBED` 画像 | PostgreSQL -> Arrow -> Ray/Python -> CUDA embedding endpoint -> writeback | 真实模型服务可接入端到端执行路径 | 当前写回为 JSON text，不代表 pgvector(384) 性能 |
| 双 endpoint Ray 动机测试 | Python / Ray task / Ray actor 调用两个本地 endpoint | 可验证并发 routing 对 operator wall time 的影响 | 两个 endpoint 在同一 GPU 上，不代表多 GPU 或 Ray Serve 结论 |
| 三类 workload 预研 | `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 的 fake/CPU 对比 | 说明三类场景可复用同一执行骨架 | 只作为机制提示和实验设计依据 |

真实 GPU-backed `AI_EMBED` 实验首先说明，batch 粒度本身会显著影响端到端执行。表 4-2 中，1024 行 fine 策略发起 1024 次 endpoint 调用，coalesced 策略只发起 4 次调用；fine 的端到端耗时约为 coalesced 的 `13.4x`。这说明在真实 CUDA embedding endpoint 接入后，逐行调用不是合理 baseline，批处理执行是必须研究的对象。

```mermaid
xychart-beta
  title "1024 行 GPU-backed AI_EMBED：fine 与 coalesced 端到端耗时"
  x-axis ["coalesced", "fine"]
  y-axis "e2e_s" 0 --> 12
  bar [0.888, 11.925]
```

图 4-2 逐行调用与 batch 调用的端到端对比。fine 策略将 endpoint 调用数从 4 次放大到 1024 次，端到端耗时约为 coalesced 的 `13.4x`，说明模型服务调用粒度是必须控制的一阶成本。

| 行数 | 执行方式 | 策略 | endpoint 调用数 | e2e_s | operator_wall_s | writeback_s | 结论 |
|---:|---|---|---:|---:|---:|---:|---|
| 1024 | Ray actor | coalesced | 4 | 0.888 | 0.505 | 0.374 | batch 调用形成可用基线 |
| 1024 | Ray actor | fine | 1024 | 11.925 | 11.528 | 0.386 | 逐行调用显著放大 operator 阶段 |
| 4096 | Python | coalesced | 16 | 3.353 | 1.784 | 1.542 | 单 endpoint 下 Python 可作为强 baseline |
| 4096 | Ray task | coalesced | 16 | 3.291 | 1.677 | 1.588 | Ray task 与 Python 接近 |
| 4096 | Ray actor | coalesced | 16 | 3.355 | 1.677 | 1.651 | Ray actor 与 Python 接近 |
| 16384 | Ray actor | coalesced | 64 | 13.168 | 6.473 | 6.586 | operator 与 writeback 都是大块成本 |

表 4-2 还说明，单 endpoint 下 Ray 并不天然优于 Python。4096 行 coalesced 场景中，Python、Ray task 和 Ray actor 的端到端时间都在 `3.29s-3.36s` 区间。因此，后续研究需要进一步分析 Ray 在多 endpoint、bounded in-flight、routing、actor pool 和 worker-side writeback 等条件下的适用范围。

```mermaid
xychart-beta
  title "16384 行 Ray actor / coalesced 阶段耗时"
  x-axis ["operator_wall", "writeback"]
  y-axis "seconds" 0 --> 7
  bar [6.473, 6.586]
```

图 4-3 16K 行场景下 operator 与 writeback 的阶段对比。模型服务调用阶段和 PostgreSQL 写回阶段耗时接近，说明后续如果只优化模型调用，端到端收益可能被写回阶段限制。

双 endpoint 实验进一步补充了 Ray 的使用动机。表 4-3 中，4096 行、16 个 coalesced batch 下，Python 顺序轮询两个 endpoint 的端到端均值为 `3.444s`，Ray task 和 Ray actor 分别为 `2.761s` 和 `2.780s`；Ray 主要降低 external operator wall time。16384 行 Ray actor 双 endpoint 的 operator wall time 也从单 endpoint 的 `6.473s` 降到 `4.628s`，但 writeback 仍约 `6.363s`，说明写回会限制端到端收益。

```mermaid
xychart-beta
  title "4096 行双 endpoint：Python 与 Ray 的端到端耗时"
  x-axis ["Python", "Ray task", "Ray actor"]
  y-axis "e2e_s" 0 --> 3.6
  bar [3.444, 2.761, 2.780]
```

图 4-4 双 endpoint 场景下 Python 与 Ray 的端到端对比。Ray task / actor 可以通过并发 routing 降低 operator wall time，但端到端收益仍受 writeback 约束。

| 行数 | 执行方式 | endpoint 数 | e2e_s | operator_wall_s | writeback_s | 结论 |
|---:|---|---:|---:|---:|---:|---|
| 4096 | Python | 2 | 3.444 | 1.845 | 1.573 | 顺序调用无法充分利用两个 endpoint |
| 4096 | Ray task | 2 | 2.761 | 1.144 | 1.591 | 并发 routing 降低 operator wall time |
| 4096 | Ray actor | 2 | 2.780 | 1.188 | 1.565 | actor 与 task 接近，写回仍是大块成本 |
| 16384 | Ray actor | 1 | 13.168 | 6.473 | 6.586 | 单 endpoint 下 operator 与写回接近 |
| 16384 | Ray actor | 2 | 11.100 | 4.628 | 6.363 | operator 降低后，writeback 成为收益上限 |

历史 fake/CPU 预研主要用于说明为什么研究内容要覆盖 task/object 粒度、模型服务反压和三类 workload。表 4-4 汇总了这些结果。它们不能替代真实 GPU-backed 链路，但可以作为实验变量设计的依据。

```mermaid
flowchart TB
  A[fake/CPU 预研] --> B[三类 workload 对比]
  A --> C[granularity attribution]
  A --> D[backpressure simulation]
  B --> E[验证三类场景可复用同一执行骨架]
  C --> F[定位 task 数 / invocation / fan-in refs]
  D --> G[验证 bounded in-flight 控制队列压力]
  E --> H[进入 GPU-backed E2E 验证]
  F --> H
  G --> H
```

图 4-5 fake/CPU 预研结果在课题中的作用。预研结果用于确定实验变量和对照组，正式结论仍需要在 GPU-backed 数据库驱动 AI workload 链路上验证。

| 实验 | 关键结果 | 对研究方案的含义 | 不能声称 |
|---|---|---|---|
| 三类 workload fake 对比 | `upstream=32, downstream=32` 时，`embed_rag`、`classify_filter`、`offline_llm` 的 fine/coalesced e2e 比值约为 `4.01x`、`4.32x`、`4.37x` | 三类 workload 都值得纳入统一执行策略验证 | 不能说真实 LLM 推理一定有 4x 收益 |
| granularity attribution | `downstream_coalesced` 将 total Ray tasks 降到 `64`，e2e 为 `16.41 ms`；`fine` 为 `1056` 个 tasks，e2e 为 `139.27 ms` | 收益不只来自 fan-in refs，过细 operator invocation 也是重要变量 | 不能直接外推到真实模型服务 |
| backpressure simulation | `queue_limit=8` 不提高 tokens/s，但将平均 queue wait 从 `4768.50 ms` 降到 `114.41 ms` | bounded in-flight 可控制模型服务队列压力 | 不能说 backpressure 一定提高吞吐 |
| PG18.4 fake-model 画像 | 4096 行 Ray actor fine/coalesced e2e 比约 `13.52x` | 数据库触发链路中 batch/invocation 粒度值得继续验证 | 不能代表 PostgreSQL 18.3 或真实 GPU 结果 |

综合上述结果，当前可行性结论有三点。第一，数据库驱动 AI workload 的端到端画像链路已经跑通，且真实 GPU-backed 模型服务能够接入本地 PostgreSQL 同构预演环境。第二，已有实验显示 batch 粒度、endpoint routing 和 writeback 都会影响端到端性能，研究内容中的数据组织、调度与持久化协同不是凭空提出。第三，三类 workload 的输入输出形态和瓶颈差异已经在项目材料中定义清楚，后续可以在同一套阶段计时框架下逐步验证。

当前仍需补齐的关键环节也比较明确：将 JSON text 写回替换为 384 维 pgvector 写回；比较 driver fan-in 写回、worker-side writeback 和 queue worker 写回；用 Ray Serve / vLLM 或等价本地模型服务替代两个手动 endpoint；把链路迁移到 PostgreSQL 18.3 内部平台，并继续区分本地预演事实、模拟实验事实和正式平台结论。

## 5. 进度安排

2026 年 7 月：完成开题报告、PPT、文献清单和现有 GPU-backed 主动机实验整理；补充 384 维 pgvector 写回实验设计；明确 PostgreSQL 18.3 内部平台与本地 PG18.4 同构预演环境之间的迁移边界。

2026 年 8 月：完善 PostgreSQL 18.3 / PG18.4 同构执行路径，完成 `AI_EMBED` 的 batch、Ray task/actor、多 endpoint、bounded in-flight 和 worker 写回大块消融；形成 JSON text、pgvector(384)、worker-side writeback 的对照结果。

2026 年 9 月：扩展到 `AI_FILTER/AI_CLASSIFY`，设计 selectivity-aware predicate pipeline、cheap/expensive model cascade 和输出行数变化下的下游 partition 调整；分析 AI predicate 场景中模型调用次数、选择率和写回数据量之间的关系。

2026 年 10 月：扩展到 `AI_COMPLETE` / offline LLM 场景，接入 vLLM / Ray Serve 或等价本地模型服务，验证 token-aware batching、prefix-aware routing、queue-aware backpressure；记录 token throughput、queue wait、replica backlog 和失败重试信息。

2026 年 11 月：整理统一方法，实现稳定原型，补齐 baseline、消融和反证实验；形成可复现实验脚本、结果 CSV、图表和阶段分析报告。

2026 年 12 月以后：完成论文实验、图表、正文撰写、答辩材料和结果复核；根据导师和企业侧反馈收敛题目表述、贡献边界和最终实验组合。

## 6. 预期成果

预期形成以下成果：

1. 一个数据库驱动 AI workload 分阶段执行画像原型，支持 PostgreSQL 表读取、Daft/Arrow batch、Ray task/actor、GPU-backed endpoint、fan-in 和写回阶段计时。
2. 一组覆盖 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 的可复现实验 workload。
3. 一套面向数据组织、模型服务状态和 AI 数据持久化压力的分布式执行优化方法。
4. 实验报告、开题 PPT、论文图表和硕士论文正文。

预期关键技术指标包括：

1. 阶段计时完整性：实验结果至少覆盖 DB fetch、Arrow build、operator wall、model request wall、bounded wait、fan-in 和 writeback 等字段。
2. 可复现性：每组正式实验保留运行命令、参数、CSV 输出、warm-up / formal repeat 标记和结果解释。
3. 对照完整性：`AI_EMBED` 场景至少比较 Python、Ray task、Ray actor、single endpoint、multi endpoint、fine/coalesced batch 和不同写回方式。
4. 边界清晰性：实验结论明确区分 PG18.4 本地预演、PostgreSQL 18.3 内部平台、JSON text 写回和 pgvector(384) 写回。

预期创新点包括：

1. AI workload 感知的数据组织与批处理执行调度方法。针对 embedding 输出大、AI predicate 选择率未知、LLM token 长度不均等不同 workload 特征，结合 Daft / Arrow batch、partition、operator invocation、Ray object 粒度和 in-flight 请求数调整执行策略。
2. GPU 推理服务状态感知的 Ray 并行调度与反压控制方法。将 endpoint routing、actor pool、bounded in-flight、queue wait、replica backlog 和 GPU utilization 纳入同一端到端评价，说明 Ray 在多 endpoint 和服务端排队存在时的收益边界。
3. 面向 AI 数据流的结果汇聚与持久化协同方法。将 driver fan-in、worker-side writeback、vectorizer-like queue worker、pgvector/Lance sink 纳入同一端到端评价，避免持久化阶段吞噬上游调度收益。

## 7. 主要参考文献

主要参考文献如下。

[1] Ionel Gog, Malte Schwarzkopf, Natacha Crooks, Matthew P. Grosvenor, Allen Clement, Steven Hand. Musketeer: all for one, one for all in data processing systems. In: Proceedings of the 10th European Conference on Computer Systems, Bordeaux, France, 2015

[2] Philipp Moritz, Robert Nishihara, Stephanie Wang, Alexey Tumanov, Richard Liaw, Eric Liang, et al. Ray: A Distributed Framework for Emerging AI Applications. In: Proceedings of the 13th USENIX Symposium on Operating Systems Design and Implementation, Carlsbad, CA, USA, 2018: 561-577

[3] Matei Zaharia, Mosharaf Chowdhury, Michael J. Franklin, Scott Shenker, Ion Stoica. Spark: Cluster Computing with Working Sets. In: Proceedings of the 2nd USENIX Workshop on Hot Topics in Cloud Computing, Boston, MA, USA, 2010

[4] Apache Spark. Spark SQL Performance Tuning. https://spark.apache.org/docs/latest/sql-performance-tuning.html

[5] Ray Documentation. Ray Core Objects. https://docs.ray.io/en/latest/ray-core/objects.html

[6] Ray Documentation. Anti-pattern: Too fine-grained tasks. https://docs.ray.io/en/latest/ray-core/patterns/too-fine-grained-tasks.html

[7] Ray Documentation. Ray Serve Dynamic Request Batching. https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html

[8] Daft Documentation. Distributed Execution with Ray. https://docs.daft.ai/en/stable/distributed/ray/

[9] Daft Documentation. Partitioning and Batching. https://docs.daft.ai/en/stable/optimization/partitioning/

[10] Daft Documentation. Shuffle Algorithms. https://docs.daft.ai/en/stable/optimization/shuffle/

[11] Apache Arrow. Arrow Flight RPC. https://arrow.apache.org/docs/format/Flight.html

[12] Snowflake Documentation. Cortex AISQL. https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql

[13] Timescale. pgai README. https://github.com/timescale/pgai

[14] pgvector. pgvector README. https://github.com/pgvector/pgvector

[15] PostgresML. PostgresML README. https://github.com/postgresml/postgresml

[16] 本项目实验报告. GPU-Backed AI_EMBED Chain Breakdown, 2026-07-12. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`

[17] 本项目实验报告. Multi-Endpoint Ray Motivation Test, 2026-07-12. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`

