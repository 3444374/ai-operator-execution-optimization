# 当前方向与计划

生成日期：2026-07-10

## 1. 当前方向

开题报告当前采用的正式题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

该题目是当前项目规划的收敛口径。后续工作不再把“阶段划分”或“外部链路画像”单独写成研究内容，而是把它们作为动机测试、方案设计和评价依据；研究内容围绕 AI workload 感知的数据组织与批处理构造、GPU 推理服务状态感知的 Ray 调度与反压、AI 数据流汇聚与 Lance / 数据库持久化协同展开。

本文件与开题报告保持双向同步。开题报告需要基于本文件、实验结果和后续规划来写；反过来，开题报告如果根据导师反馈、实验结果或题目收敛调整了研究内容和侧重点，本文件也要同步更新当前方向、后续阶段、四周计划和不能做成什么。项目执行时不能让本文件与 `opening/report/opening_report.md` 长期描述两个不同方向。

推荐题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

候选技术副标题：

> 基于 Daft/Ray/Lance 类系统机制的 AI 数据执行链路调优。

候选升级表述：

> 面向数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同优化。

该方向比传统数据库内核方向更贴近用户的 AI infra / inference infra 目标，也能通过数据库 AI workload 场景与达梦需求对齐。但具体优化点尚未最终确定，当前首先需要用生产式 GPU-backed E2E profile 建立主动机：数据库驱动 AI workload 进入 Daft/Ray/Lance-like 数据执行链路后，是否产生足够严重、可分解、可优化的损耗。Object/fan-in/coalescing 是已有实验证据支持的机制入口，不应成为整个课题的全部。

当前计划明确把“GPU-backed AI 数据执行链路调优”作为主攻方向：参考 Snowflake Cortex AISQL、pgai vectorizer、pgvector、PostgresML、OceanBase/达梦类分布式数据库等工业背景，把数据库作为 workload source 和 sink，研究 Daft/Arrow 数据组织、Ray task/actor 执行、GPU 模型服务、fan-in、backpressure 和 Lance / pgvector / PostgreSQL 写回之间的协同。把 AI 算子搬进数据库内核、证明 GPU 迁移收益或直接做 GPU kernel/算子优化，只作为背景或必要 baseline，不作为主要时间投入。

这里的实验形态不要求复现 Snowflake 或 pgai 的内部实现。更稳的做法是构建一个 **AI-SQL-compatible operator surface**：在 PostgreSQL / PG18.4 同构环境中暴露类似 `AI_EMBED`、`AI_FILTER`、`AI_CLASSIFY`、`AI_COMPLETE` 的算子语义，用 pgai/vectorizer-like 的外部 worker 形态执行，再用 Ray 调度、GPU-backed model service 和写回链路完成端到端实验。这样既能站在工业系统的语义和流程上，又不会把论文变成“复现某个闭源系统”。

## 2. 不能做成什么

当前不把论文写成：

- 跑通 Daft/Ray/Lance；
- 改造整个 Ray；
- 重写 Ray Core / Ray Data / Ray Serve 的完整调度器；
- 传统数据库 GPU 查询算子优化；
- 单纯模型推理 kernel 优化；
- 把主要工作量放在“AI 算子上 GPU / 数据库内执行”的 baseline 测试；
- 单纯 Arrow 序列化优化；
- 没有真实 workload 的 benchmark 堆砌。

## 3. 当前系统链路

```text
PostgreSQL 18.3 documents table / parquet
  -> AI-SQL-compatible operator surface
  -> Arrow RecordBatch
  -> task partitioning / batch construction
  -> Ray tasks / actors / object store
  -> CPU preprocess / tokenizer
  -> external model service / CPU or GPU-backed AI operator
  -> routing / batching / backpressure
  -> fan-in / shuffle / writeback
  -> pgvector / Lance / output table
```

粗粒度阶段先按下面划分，后续再细分：

```text
DB trigger/fetch
  -> AI operator external service path
  -> GPU model service
  -> result consolidation/writeback
```

其中 `AI data execution path` 是本文档后续所说的“AI 数据执行链路”，包含 Daft/Arrow RecordBatch 构造、batch/partition、Ray task/actor、object store、queue/in-flight、fan-in 等环节。

## 4. 当前证据判断

已完成的 fake/CPU 预研结果位于 `feasibility/results/` 和 `motivation/results/`。这些结果用于历史背景和工具调试，不替代 GPU-backed 主动机实验，也不作为真实 GPU 链路瓶颈归因依据。

关键结论：

- Ray small task：暂不支持优先做 scheduler/runtime。
- Arrow serialization：不是当前主瓶颈。
- Ray many-object fan-in：object 数量增加会放大下游 fan-in。
- Ray Arrow fan-out/fan-in：fine/coalesced 平均 fan-in 比约 `3.17x`。
- fake `AI_EMBED(text)` 端到端链路：fine/coalesced 平均 fan-in 比约 `2.16x`，端到端耗时比约 `2.51x`。

阶段性判断：

> 当前最值得继续验证的候选方向是数据库驱动 AI workload 的 GPU-backed 数据执行链路中的特征感知数据组织、并行度控制、模型服务背压和写回调优，而不是 runtime 重写或 GPU kernel 化。但现有证据仍偏 fake / CPU workload，只能作为历史信号；是否足以作为课题主线，必须用生产式 GPU-backed E2E profile 先证明真实链路中的数据执行与存储损耗足够明显。

真实形态验证判断：

> 下一步不能只继续扩展 fake benchmark。必须优先跑通一条可分阶段计时的 GPU-backed E2E 主动机链路：数据库表/SQL 触发、Daft/Arrow 数据组织、Ray 执行、GPU-backed 模型服务、fan-in/writeback 和指标采集。只有该链路中的画像数据才能回答“为什么数据执行与存储链路值得优化”。如果 GPU 暂不可用，才用 local/CPU 模型服务作为临时 baseline，并明确不能外推为 GPU 主链路结论。

2026-07-14 状态更新：本地 PG18.4 预演环境已经完成 GPU-backed `AI_EMBED` 关键复测，并补齐同一条 Ray actor 链路下的 no writeback、JSON text 和 pgvector `vector(384)` 写回对比。结果入口为 `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md` 和 `motivation/results/gpu/pgvector_writeback_20260714.md`。下一步不再是“是否能写入 pgvector(384)”，而是比较 driver fan-in、worker-side writeback、vectorizer-like queue worker，以及用 Ray Serve / vLLM 或等价模型服务替代手动 endpoint。

主动机实验的最低要求是使用真实数据库、真实模型服务和 GPU 计算端点。三类场景的 baseline 都要保留：embedding/RAG ingestion、AI_CLASSIFY / AI_FILTER、offline LLM / AI_COMPLETE。第一组主实验可以先从 `AI_EMBED(text)` 开始，因为它最容易接 pgvector 写回、最容易形成开题阶段的真实闭环；但后续不能丢掉另外两个场景。尤其是 `AI_COMPLETE` / offline LLM 应尽量作为后续更贴近 AI infra 的重点主线候选，因为它能自然引出 token-aware batching、prefix/cache locality、模型服务队列、GPU 利用率和 backpressure 等问题。

## 5. 当前端到端动机实验

已搭建 fake `AI_EMBED(text)` 端到端动机测试。该实验现在定位为历史预研和工具调试证据：说明早期为什么关注 task/object/invocation/fan-in/writeback，但不再承担“为什么整个课题值得做”的主动机，也不承担真实 GPU 链路的瓶颈归因。

当前链路：

```text
generate documents
  -> build Arrow RecordBatch
  -> Ray fake embedding
  -> fine vs coalesced object
  -> fan-in
  -> write output / CSV
```

必须记录：

- rows/s；
- object_count；
- average object size；
- `ray.put` time；
- fan-in time；
- fake embedding time；
- end-to-end time。

当前结果文件：

- `motivation/benchmarks/fake_embed_pipeline.py`
- `motivation/results/fake_cpu/fake_embed_pipeline.csv`

下一步优先级应调整为：在已经跑通的生产式 GPU-backed E2E 主动机画像基础上，继续在同一条真实链路上做大块消融。消融实验优先回答 no-Ray vs Ray、single worker vs actor pool、主控 fan-in 写回 vs 多 worker 写回、queue/vectorizer-like 写回、unbounded vs bounded in-flight、不同 batch/partition 策略分别影响多少。场景语义上，offline LLM 需要 token-aware / prefix-aware workload，AI_FILTER 需要 selectivity-aware workload，embedding/RAG 需要继续扩展真实数据库写回 baseline。vLLM / Ray Serve / GPU-backed embedding service 是现实模型服务端点；不做大规模“算子迁移到 GPU”或 GPU kernel 优化。

## 6. 后续阶段

截至 2026-07-11，Phase 1 最小同构链路已经首次跑通：本地 PostgreSQL 18.4
与 pgvector 替身通过 job table 触发，外部 Python worker 使用 Arrow RecordBatch
和 Ray actor 处理 256 行 fake embedding，并等行数写回 PostgreSQL。该结果只
证明链路连通，不证明瓶颈或优化收益；完整记录见
`feasibility/results/pg18_4_connection_validation.md`。

同日已完成第一组本地 PG18.4 正式对照画像实验：固定 4096 行、128 维 fake
embedding，对比 `python/ray_actor × fine/coalesced`，每组 1 次 warm-up 与
3 次 formal 重复。formal 均值显示，Python baseline 下 fine/coalesced 端到端
耗时比约 `16.93x`，Ray actor 下约 `13.52x`；Ray actor fine 还暴露出明显
bounded wait 与 fan-in 成本。该结果支持继续验证 batch / invocation / object
粒度控制，但仍是本地 PG18.4 fake-model 证据，不能外推为 PostgreSQL 18.3
内部平台或真实 GPU 模型结论。完整记录见
`motivation/results/pg18_4_fake/system_profile.md`。

如果数据库驱动 AI workload 场景和系统瓶颈继续成立：

1. 在低端设备上先搭 PostgreSQL 18.3 同构预演链路，必要时用普通 PostgreSQL + pgvector 作为接口替身；
2. 把 Snowflake AISQL / pgai vectorizer / PostgresML / pgvector 等工业路线整理成场景和 baseline，不把它们作为必须复现的完整系统；
3. 把已有或开源 AI 算子迁移/等价封装为 PostgreSQL 18.3 可触发的 `AI_EMBED(text)` / batch function / 外部执行入口；
4. 外部 worker 读取 documents 表，并转换为 Arrow RecordBatch；
5. 通过 Python batched worker、Ray task、Ray actor、GPU-backed embedding service / Ray Serve / vLLM endpoint 跑端到端主动机画像；如果 GPU 暂不可用，CPU/local 模型服务只作为临时工程对照，不能作为真实瓶颈归因；
6. 记录 batch、task、ObjectRef、operator invocation、fan-in refs、queue wait、writeback 等指标；
7. 写回 embeddings 表、pgvector/Lance 或普通 output table，并验证 vector search / downstream query 可用。

如果后续真实形态实验不再显示 object/fan-in 瓶颈，应回到 AI 数据执行链路重新定位瓶颈，不继续强行做 coalescing。

## 7. 四周计划

第 1 周：整理 fake `AI_EMBED(text)` pipeline 和已有 CSV，作为历史预研材料；列出 2-3 个候选 AI 算子场景。该阶段不作为最终主动机或真实瓶颈归因。

第 2 周：优先设计并实现生产式 GPU-backed E2E 主动机实验的 thin slice：PostgreSQL / pgvector 表、AI-SQL-compatible `AI_EMBED` 算子表面、外部 worker、Ray task/actor、GPU-backed embedding endpoint 或 Ray Serve/vLLM endpoint、主控 fan-in 后写回与多 worker 各自写回、阶段计时 CSV。若 GPU 暂不可用，同步准备 CPU/local 模型服务 baseline，但只作为临时对照。

第 3 周：运行并分析 GPU-backed E2E 主动机实验，固定小规模数据、复用同一套阶段计时，回答生产式 GPU 链路中数据组织、Ray 执行、模型服务、GPU 利用率、queue wait 和 writeback 的占比关系。随后优先做真实链路上的大块消融；再把 `AI_COMPLETE` / offline LLM 提升为后续重点 workload，补 token-aware / prefix-aware / queue-aware 实验，同时用 AI_FILTER selectivity / cascade 补足 AI predicate 场景。

第 4 周：用 idea-evaluator 视角做 fatal-flaws audit，整理开题材料、实验设计、baseline、反证条件和论文贡献边界。当前不要把单个场景 C 写成唯一主线，也不要在没有跨层实验数据前把题目写成完整调度系统优化。
## 8. 现有 AI 算子系统对本项目的约束

现有数据库 AI 算子系统并不都使用 Ray。Snowflake Cortex AISQL 证明 SQL 层 AI functions 是工业真实问题，但其闭源托管链路不能作为本地可拆分 baseline；pgai vectorizer 更接近 PostgreSQL + 外部 worker + embedding endpoint + 写回数据库的形态，但它更适合作为架构参考和 worker 写回对照，而不是长期核心依赖；PostgresML 代表模型靠近数据库或数据库内/近数据库执行的对照路线；pgvector 只负责向量存储和检索，不负责 embedding 计算。

因此，当前方向不改成“复现 Snowflake / pgai”，也不改成“只优化 Ray”。更稳的方向表述是：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化。

后续实验要优先验证三类可控链路问题：

1. Ray task / actor / 多 endpoint 路由是否能降低模型服务阶段墙钟时间；
2. driver fan-in 后统一写回、Ray worker 各自写回、vectorizer-like 异步 worker 写回三种形态的差异；
3. PostgreSQL JSON text、PostgreSQL `pgvector(384)`、Lance / Parquet 作为写回 sink 时，哪一段成为瓶颈。

详细外部系统对比见 `research/existing_ai_operator_execution_chains.md`。
