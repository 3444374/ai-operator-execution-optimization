# Sema-like 数据库原生语义算子架构参照审计

更新日期：2026-08-28

当前研究判断以 §11 的 2026-08-28 增补为准；实施范围与顺序只由
[`../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)
定义。§1–10 保留最初从 LOTUS 转向 Sema 时的文献迁移审计；与 §11 冲突时由 §11 取代。

## 0. 研究问题与结论

本文回答四个问题：

1. Sema 如何划分 SQL、语义算子、查询计划、优化器、执行器和模型服务的职责；
2. 哪些设计适合作为本项目的目标架构，哪些能力当前仍未实现；
3. 为什么 LOTUS 应保留为语义兼容来源、算法参考和独立系统 baseline，而不再承担总架构参照。
4. IMLane/Kalypso 的组批、提交、dependency/KV 机制应如何进入 SemLoom，以及何时需要 PostgreSQL
   core patch。

结论如下。

> **数据库拥有 SQL、semantic algorithm、explicit physical alternatives、ordinary child plan 和结果
> 生命周期；SemLoom 只接收 sealed tasks，并负责不改变语义的 work organization、提交、路由与
> 多 Job 执行。LOTUS/Cortex/Sema 提供数据库 plan 优化参照，IMLane/Kalypso 分别提供 DB-runtime
> batch 与 dependency/KV execution 参照。PostgreSQL 先用 extension 验证，只有目标优化或 node
> lifecycle 出现已复现阻断才增加最小 core patch。**

这一判断同时受两类证据支持：

- **论文与官方材料**：Sema、Cortex AISQL、LOTUS、IMLane、Kalypso、Palimpzest、DocETL 的论文、
  官方代码仓库或实验 artifact；
- **本地实现事实**：`code/INFRA_STATUS.md` 明确记录，现有代码是 PostgreSQL 读取之后的 Daft/Ray/vLLM 外部执行基座，尚无 LOTUS adapter、`sem_map` 语义入口或 PostgreSQL `CustomScan` 实现。

## 1. 证据身份与版本

| 对象 | 本文使用的一手来源 | 证据用途 |
|---|---|---|
| Sema | [arXiv:2603.11622v1](https://arxiv.org/abs/2603.11622)、[VLDB 2026 官方程序](https://vldb.org/2026/program.html)、[作者 artifact 仓库](https://github.com/BITQiKangK/SemaSystem) | 数据库原生语义算子的语言、计划、优化器、执行器和 AQE 架构 |
| LOTUS | [PVLDB 18(11) 正式论文](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)、[v1.2.4 release](https://github.com/lotus-data/lotus/releases/tag/v1.2.4)、[固定 commit `b1a85fd`](https://github.com/lotus-data/lotus/tree/b1a85fd7a66fabed8a1585d44d7597d592b4433f) | 语义算子抽象、参考算法、准确率约束与 v1.2.4 实现接口 |
| Palimpzest | [CIDR 2025 正式论文](https://www.vldb.org/cidrdb/papers/2025/p12-liu.pdf)、[MIT 官方仓库](https://github.com/mitdbg/palimpzest) | 声明式 Dataset/Schema/Convert、逻辑—物理计划枚举、sample profile 与 time/cost/quality 选择 |
| DocETL | [PVLDB 18(9) 正式论文](https://www.vldb.org/pvldb/vol18/p3035-shankar.pdf)、[UC Berkeley 官方仓库](https://github.com/ucbepic/docetl) | 文档 ETL 的声明式 semantic pipeline、agentic rewrite 与 evaluation |
| Abacus | [PVLDB 19(5) 正式论文](https://www.vldb.org/pvldb/vol19/p1060-russo.pdf)、[VLDB 2026 官方程序](https://vldb.org/2026/program.html) | 可扩展 semantic physical implementations、operator sampling 与 constrained Pareto plan search |
| PostgreSQL 扩展机制 | [PostgreSQL 18 Custom Scan](https://www.postgresql.org/docs/18/custom-scan.html)、[PGXS](https://www.postgresql.org/docs/18/extend-pgxs.html) | 在不修改 PostgreSQL core 的条件下承载自定义 plan/executor 节点 |

Sema 的版本必须单独说明：本文阅读的完整方法和实验来自 arXiv v1，其中有 20 个查询；VLDB 2026 官方程序确认其为常规 Research Paper，并把录用版概括为 26 个查询。不能把官方摘要新增的查询数反向写进 arXiv v1 的表格、图和细分实验。

Sema 官方仓库当前公开的是技术报告、实验脚本和可执行压缩包，不是可逐文件审计的完整 DuckDB 修改源码。因此本文对其内部实现的判断以论文和公开运行说明为准，不把未公开源码细节写成已核验事实。

## 2. Sema 的职责划分

### 2.1 查询语言：自然语言表达式属于 SQL，而不是外部 runner 参数

**来源类型：论文。** SemaSQL 在普通 SQL clause 中加入带 column placeholder 的自然语言表达式 `s'...{column}...'`：

| SQL 位置 | 语义算子 | 关系结果 |
|---|---|---|
| `SELECT s'...' AS col` | `SemProj` | 为每个输入 tuple 生成新文本列 |
| `WHERE s'...'` / `HAVING s'...'` | `SemFilter` | 返回满足语义谓词的 tuple 子集 |
| `JOIN ... ON s'...'` | `SemJoin` | 按自然语言条件连接两个关系 |
| `sem_agg(s'...')` | `SemAgg` | 对一组 tuple 生成聚合结果 |

论文还定义 `SemOrderBy`，但 arXiv v1 没有给出与其他四类同等完整的 SemaSQL 示例和 benchmark 覆盖。

这里最重要的架构含义是：数据库 parser/binder 能识别 column reference、输入类型、输出 schema 和 operator kind；模型调用不是对 optimizer 不透明的普通 HTTP UDF。

### 2.2 Parser 与 planner：生成显式 semantic plan node

**来源类型：论文。** Sema 的 parser/planner 同时生成 relational operators 与 semantic operators。语义算子的身份、自然语言 expression、输入列和依赖关系保留在 query plan 中，因而后续可以执行：

- relational predicate pushdown；
- semantic predicate reordering；
- 相邻 semantic operator fusion；
- prompt batching；
- 运行期候选路径选择。

这正是本项目应借鉴的第一原则：**SQL 表面形式、逻辑语义节点和物理执行节点必须分开，但三者都由数据库管理。** 外部 provider 不应重新解析 SQL，也不应根据 prompt 字符串猜测这是 map、filter、join 还是 aggregate。

### 2.3 Logical optimizer：处理可静态识别的语义冗余

**来源类型：论文。** Sema 在传统 DuckDB 优化之前增加两类 semantic-specific rewrite：

1. **NL Expression Compression**：辅助 LLM 缩短会在大量 tuple 上重复使用的自然语言 expression；
2. **Predicate Deduction**：从 `SemFilter` 推导 entire 或 partial relational predicate。partial predicate 必须是原语义条件的 necessary condition，只有确定不可能满足语义条件的 tuple 才能被关系算子提前排除。

Predicate Deduction 使用 schema/column metadata、candidate SQL generation、语法检查和另一次 LLM self-reflection。论文没有给出形式化 soundness proof；Table 4 中 Q14 的质量下降也说明它不能被描述为普遍保持语义的确定性规则。

对本项目的含义是：未来若实现这类 rewrite，它应属于 PostgreSQL semantic optimizer，并且必须带显式验证、回退和质量审计。它不属于 Ray/vLLM 调度后端。

### 2.4 Physical executor：数据库执行器拥有 semantic execution state

**来源类型：论文。** Sema 复用 DuckDB 的 columnar storage、vectorized execution、morsel-driven pipeline parallelism 和 push-based execution，并在 executor 中增加：

- `SemFilter`、`SemProj`、`SemJoin` 的 prompt batching；
- 同一 pipeline 内相邻 unary semantic operators 的 fusion；
- `SemFilter` reordering；
- thread-local CURL multi-handle 并发 LLM 调用；
- `PhysicalSemAdaptiveFilter` 的运行期探索与路径选择。

模型可以由本地 vLLM 或远程 OpenRouter 提供。模型服务位于数据库进程之外，并不改变数据库对 operator identity、plan、pipeline state 和结果输出的所有权。

这一点直接支持本项目的目标形态：**“数据库内置”指 SQL/plan/query lifecycle 属于 PostgreSQL，不要求模型在 PostgreSQL 进程内。**

### 2.5 AQE：静态优化与运行期反馈分工

**来源类型：论文。** Sema 的 Adaptive Query Execution（AQE）目前只用于单一 DuckDB pipeline 内的连续 `SemFilter`，分三个阶段：

1. **Expression exploration**：在少量 tuple 上独立执行每个 predicate，估计 selectivity、boolean result vector 和 pairwise Matthews Correlation Coefficient（MCC）；
2. **Path exploration**：生成 reference、selectivity reorder、一次 fusion 及各自 batched counterpart，在另一小段数据上测 latency、token cost 和相对 reference path 的结果一致性；
3. **Path exploitation**：按用户的 latency-first 或 cost-first 偏好，从满足一致性阈值的 Pareto 路径中选一个处理剩余数据。

AQE 的质量约束是 candidate 与 reference path 的一致程度，不是相对人工 ground truth 的绝对正确率。MCC 只是 fusion candidate 的剪枝 heuristic，也不是 semantic equivalence proof。

Sema 最值得迁移的不是固定的 `1/32 + 3/32` 采样比例，而是两个时间尺度：

```text
compile / plan time
  处理 expression、schema、关系必要条件和候选物理实现

runtime
  用真实 selectivity、latency、token cost、质量偏差和服务环境选择 execution path
```

### 2.6 Sema 没有解决的事项

**来源类型：论文。** Sema arXiv v1 明确没有完整覆盖：

- 单个 semantic operator 的全部专用算法；
- storage-level optimization；
- model routing 与 model selection；
- external inference privacy、governance 和 fault tolerance；
- 多 pipeline AQE；
- 高并发 multi-query / multi-Job 公平；
- 多个独立 endpoint 的分布式路由；
- scale-out runtime。

因此，Sema 是本项目的数据库集成架构参照，不是可以直接替换项目外部执行与多 Job 调度研究的完整实现。

## 3. 面向本项目的目标分层

### 3.1 推荐的所有权关系

**来源类型：论文迁移 + 项目设计推断。**

| 层次 | 应拥有的对象 | 不应拥有的对象 |
|---|---|---|
| PostgreSQL semantic extension | SQL/binding、operator kind、输入输出 schema、NULL/error/order 语义、logical/physical plan、child plan、snapshot、query cancel、result lifecycle | Ray worker 选择、vLLM continuous batching 内部策略 |
| Semantic optimizer/executor | 可下推关系谓词、operator order、semantic fusion、联合 prompt batching、result parser、quality/reference policy | 多 endpoint 的具体传输与 worker 实现 |
| Execution provider interface | query session 生命周期、规范化 task 的 `open/drive/close`、backpressure 与 completion telemetry | 重新解释 SQL、改写 prompt、改变 parser 或结果语义 |
| SemLoom execution provider | work estimation、独立任务的 work-unit 组织、bounded admission、持续补位、多 Job 份额、endpoint route、Ray transport | 把多 tuple 合并成一个新语义 prompt、修改算子 reference behavior |
| vLLM | token-level continuous batching、KV management、模型推理 | 数据库 transaction/snapshot、Job 级策略和 SQL 结果管理 |

### 3.2 推荐的数据合同

数据库应先把 SQL 和 semantic expression 编译为中立任务，而不是把原始 SQL 或整张 DataFrame 交给项目后端：

```text
PreparedSemanticTask
  query_id / operator_id / task_id
  operator_kind
  stable row or group identity
  bound input values
  canonical messages
  expected output schema
  result parser identity
  generation constraints
  quality/reference policy
  ordering and error policy
```

SemLoom execution provider 将其扩展为执行描述：

```text
ExecutionEnvelope
  prepared_task identity
  source/prepare/model/result work estimate
  uncertainty interval
  locality/prefix key
  endpoint capability requirements
  arrival/deadline/SLO metadata
  calibration signature
```

完成记录必须能无歧义返回数据库：

```text
CompletionRecord
  query/operator/task identity
  raw completion + parsed value/status
  prompt/output token usage
  finish reason / error category
  timestamps and provider telemetry
  prompt/output digest
```

数据库物理算子负责把 completion 解释为 `SemMap` 新列、`SemFilter` 布尔选择、`SemExtract` 结构化列或其他关系输出。provider 不决定“哪些 tuple 留下”。

### 3.3 两种 batching 必须分开

**Sema semantic prompt batching** 将多条 tuple 内容改写进一个联合 prompt，并要求模型返回 JSON array。它改变调用次数、prompt、输出格式、tuple independence 和结果质量，因而属于 semantic optimizer/executor 的物理计划选择。

**本项目 work-unit organization** 把多个互相独立、已经规范化的 task 放入一个 ready window、Ray submission group 或 endpoint credit domain。每个 task 仍保留独立 messages、独立 completion 和独立身份，不改变 semantic reference behavior。

因此：

```text
semantic fusion / multi-tuple prompt
  由 PostgreSQL semantic executor 选择并做质量比较

independent-task grouping / admission / routing
  由 SemLoom provider 选择并保持 task 语义不变
```

如果项目后端未来需要执行数据库已经选定的 batched semantic task，它接收的也应是一个已经具有确定 parser 和输出 cardinality 的 `PreparedSemanticTask`，而不是在后端自行拼接 prompt。

## 4. LOTUS 的正确角色

### 4.1 LOTUS 提供了什么

**来源类型：论文。** LOTUS 的主要贡献包括：

- 用自然语言 specification 定义 `sem_filter`、`sem_join`、`sem_agg`、`sem_topk`、`sem_group_by`、`sem_map`；
- 用 high-quality Reference Algorithm 定义每个 operator 的参考行为；
- 让 optimized algorithm 相对 Reference Algorithm 满足统计 accuracy target；
- 为 filter、join、group-by、top-k 设计 proxy/oracle、embedding、sampling 和 threshold 等优化；
- 以 Pandas-like API 提供可运行系统，并使用 vLLM/FAISS 等后端。

这些内容仍然非常重要：它们适合作为 operator vocabulary、reference behavior、质量协议、专用算法和 full-system comparison 的来源。

### 4.2 为什么 LOTUS v1.2.4 不应是总架构中心

**来源类型：固定版本源码；详细逐文件审计见 `lotus_postgresql_execution_layer_fit_20260821.md`。**

1. LOTUS 论文和稳定产品的主要用户接口是 Pandas/DataFrame，而不是 PostgreSQL SQL/parser/planner/executor；论文只说明 semantic operators 也可以加入 SQL。
2. `LazyFrame.sem_map()` 虽会构造 Pydantic `SemMapNode`，但 `LazyFrame` 的 node list 没有稳定的公开 visitor、`to_ir()` 或 physical backend registry。
3. `LazyFrameRun.execute()` 顺序执行 node，`SemMapNode.__call__()` 直接调用 `df.sem_map(...)`；没有把 node dispatch 到用户提供的 Daft/Ray executor 的公共 protocol。
4. LOTUS `LM` 是具体 LiteLLM wrapper，原生路径由 `batch_completion(..., max_workers=max_batch_size)` 拥有 client batching、cache、rate limit 和 usage accounting；替换为项目 scheduler 后不能继续称作 LOTUS native execution。
5. PostgreSQL connector 使用 SQLAlchemy + `pd.read_sql()` 物化完整 Pandas DataFrame，不提供 database-owned child plan、streaming tuple lifecycle、query cancel 或项目所需的稳定 row/Job identity。
6. `sem_map` reference behavior 是逐 tuple projection；LOTUS 论文没有为它提出本文专用的 operator algorithm optimization，因为作者把它视为已有 batched inference 问题。

因此，将整个 PostgreSQL 插件绑定到 `lotus-ai==1.2.4` 会把架构依赖建立在 DataFrame AST 和非公共 backend seam 上，也会混淆数据库与外部 scheduler 的所有权。

### 4.3 推荐的 LOTUS 保留方式

| 用途 | 推荐形态 | 身份表述 |
|---|---|---|
| `sem_map` 行为兼容 | 可选 `lotus_compat` semantic profile，保存 v1.2.4 messages/output/error golden fixtures | 项目对 LOTUS v1.2.4 子集的兼容实现，不是 LOTUS native |
| 原生系统比较 | 原样运行 v1.2.4 `DataFrame.sem_map` + LOTUS LM/LiteLLM + vLLM | `LOTUS v1.2.4 native` |
| 算子算法参考 | filter/join/top-k/group-by Reference Algorithm 与 accuracy-guaranteed optimization | 文献/算法来源；是否实现逐项说明 |
| workload 与质量比较 | 与 Palimpzest、SemBench 等共同形成 quality/cost/latency 系统比较 | full-system baseline，与 same-work 调度比较分表 |

核心 IR 不应以 `SemMapNode` 的 Python/Pydantic 字段布局为长期 ABI。它应定义项目自己的版本化 semantic plan/task schema，再由 `lotus_compat` adapter 映射受支持的字段；未知字段或无法表达的 LOTUS plan 应明确拒绝。

## 5. Palimpzest 与 DocETL 的补充作用

### 5.1 Palimpzest：逻辑—物理—运行期 profile 的优化器参照

**来源类型：CIDR 正式论文。** Palimpzest 使用 Dataset、Schema 和 Convert 表达 semantic analytics program，随后：

1. 枚举 Filter/Convert reordering 等逻辑计划；
2. 枚举 model selection、code synthesis、prompt marshaling、input-token reduction 等物理实现；
3. 在少量输入上运行 sentinel plans，估计 selectivity、runtime、money cost 和 relative quality；
4. 保留 Pareto candidates，再按用户 Policy 选择。

它支持本项目把 `row count + token/work + selectivity + quality` 共同放入 semantic plan cost，而不是只预测 wall time。

但论文的 Workload-Aware Execution Management 仍是未来工作；没有实现 endpoint queue/KV 感知、multi-Job fairness 或多 endpoint 运行期反馈。因此它更像声明式前端和多目标 optimizer，而不是项目所需的分布式 serving runtime。

### 5.2 DocETL：agentic semantic rewrite 的相关工作

**来源类型：PVLDB 正式论文 + 官方仓库。** DocETL 让用户用 YAML/Python 描述 map、reduce、filter、resolve、gather 等 LLM document operators，并让 agent 根据 rewrite directives 分解 operator 或 data，再用 task-specific validation 评价候选。

它适合作为以下问题的来源：

- 复杂 semantic operation 如何被重写为更易执行的 pipeline；
- 质量评价函数如何参与 rewrite；
- rewrite search 为什么不能只看 token 或调用数。

它不是数据库原生 SQL/planner/executor 架构，也不应被用来证明 PostgreSQL query lifecycle 或项目多 Job runtime 已解决。

### 5.3 Abacus：可扩展 cost/quality/latency 计划搜索参照

**来源类型：PVLDB 正式论文。** Abacus 将一个 logical semantic operator 的实现空间表示为可扩展 rules；对 physical operators 做 sample-based quality、dollar cost 和 latency 估计，用 Pareto-oriented multi-armed bandit 把采样集中到可能进入 frontier 的实现，再由 Pareto-Cascades 在带约束的多目标空间中组合完整计划。

它支持本项目在通用 semantic plan 中保留 `logical operator → multiple physical implementations`，而不是把 `lotus_compat`、direct HTTP 和 SemLoom provider 写成互斥的顶层架构。它也说明 cost interface 需要同时携带 quality、cost、latency 与 uncertainty，而不是只返回一个 PostgreSQL 风格标量。

Abacus 的 operator independence、计划代价组合和 sample estimates 都有适用条件；它不观察项目 provider 的在线 endpoint queue、multi-Job service debt 或 cancel lifecycle。因此可迁移的是 rule space、sampling 和 Pareto search，不是把其当前 optimizer 直接当作项目的运行期 scheduler。

## 6. 当前项目事实与目标架构的差距

**来源类型：本地实现事实。** 依据 `code/INFRA_STATUS.md` 和 `PROJECT_OUTLINE.md` 的 2026-08-27 版本：

| 能力 | 当前事实 | 本文建议的解释 |
|---|---|---|
| PostgreSQL source → Daft → Ray → vLLM | 已有可运行代码与多轮实验 | 可复用的外部 physical provider 基座 |
| token/work organization、bounded inflight、completion replenishment | 已实现并有不同强度证据 | provider 内 same-work 执行能力，不等于 semantic optimizer |
| 多 Job shared credit / routing / selector | 已有实现与条件性结果；复杂动态策略未稳定超过强静态参照 | 继续作为研究候选，不写成 Sema 已有能力或项目默认胜出机制 |
| `WorkDescriptor` | 已有兼容合同与单测，正式运行仍主要使用已有标量 credit | 可演化为 `ExecutionEnvelope`，但尚未接到 database plan |
| 代价估计 | 已完成离线 feasibility；尚未在线驱动 organizer/scheduler | 可作为未来 semantic plan/provider cost interface，当前不能称 cost-based query optimizer |
| LOTUS adapter / `sem_map` parity | 未实现 | 改为可选 compatibility work，不再阻塞通用 semantic operator IR 的设计 |
| PostgreSQL planner-visible semantic operator / `CustomScan` | 未实现 | 首个数据库资格任务 |
| Sema-like expression compression / predicate deduction | 未实现 | 后续 semantic optimizer work |
| semantic operator fusion / multi-tuple prompt batching / AQE | 未实现 | 远期参考；只有另行立项并具备 reference/quality evaluation 后再决定是否实现 |
| `SemFilter` / `SemExtract` / `SemJoin` / `SemAgg` | 未实现 | 当前只排期 `SemFilter`；其余算子是参考方向，不能从 manifest/profiler 自动重标 |

因此，已有外部实验可以说明 provider 设计中哪些 organization/admission 机制可运行、哪些策略只在特定 workload 下有效；它们不能证明 PostgreSQL 已拥有 native semantic plan、snapshot/cancel/error/result lifecycle。

## 7. 推荐的实施顺序

**来源类型：论文迁移 + 项目实现推断。** 以下顺序将 Sema 的数据库所有权与现有 provider 基座连接起来，同时避免把全部 Sema 优化一次性复制到 PostgreSQL。

本节是早期迁移参考；当前排期以 §11.8、§11.9 和当前实施计划为准。IMLane 是数据库资格后的优先
验证，Kalypso 仅作条件性参考。

### 7.1 最小 native `SemMap`

- 定义项目自有 `SemanticPlanSpec`、`PreparedSemanticTask` 和 `CompletionRecord`；
- 用普通 extension SQL surface（例如 `ai_map`）完成 binding 和类型检查；
- 通过 planner/executor hook 或 `CustomScan` 形成显式 physical node；
- 让 node 消费 ordinary child plan 的 tuple batches；
- 验证 snapshot、cancel、error、row identity、order、result parser 和 query completion；
- 接一个最小 direct HTTP provider 与现有 SemLoom provider。

首版只要求通用、稳定的 operator/provider seam，不要求 LOTUS 运行时依赖。`lotus_compat` 可用同一 operator IR 做单独 parity test。

### 7.2 增加 `SemFilter` 与结构化 `SemExtract`

- `SemFilter` completion 由数据库物理算子解析为布尔值并决定 tuple 是否向下游输出；
- `SemExtract` 的 schema 和 field-level parser 由数据库声明；
- 明确 NULL、parse failure、retry、partial batch failure 和 cancel 行为；
- 保留 reference execution 以评价后续 semantic rewrite 的结果偏差。

### 7.3 接入 cost/quality metadata

- 计划时记录 row/cardinality、prompt/output work 分布、endpoint profile、quality policy 和 uncertainty；
- 运行时回传 selectivity、实际 token、queue/service time、parse/error 和结果一致性；
- 先让这些字段支持 explain/profile 和 provider config ranking，再决定是否驱动 SQL path choice；
- 评价 plan/config ranking 与 decision regret，不只评价点估计误差。

### 7.4 远期参考：semantic rewrite 与 AQE

- relational necessary-condition deduction；
- operator reordering；
- semantic fusion；
- multi-tuple semantic prompt batching；
- reference-path consistency 与用户 quality tolerance；
- 小样本 explore/exploit 与 Pareto path choice。

这些项目不构成当前排期；若以后另行立项，每项都应有独立 reference behavior、质量指标和 rollback
条件，不能把项目当前的独立-task batching 直接改名为 Sema prompt batching。

### 7.5 远期参考：join/aggregate 与分布式语义计划

`SemJoin` 会改变输入 cardinality 和 pair generation；`SemAgg` 是 blocking/many-to-one operator。若以后
另行立项，它们需要不同于 row-preserving `SemMap` 的 lifecycle、memory 和 failure semantics，不能只扩
一个 `operator_kind` 枚举。当前计划不承诺实现。

## 8. 可声称与不可声称

### 8.1 现在可以声称

- Sema 提供最接近本项目目标的数据库原生 semantic operator 集成参照：SQL、plan、optimizer、executor 和 runtime feedback 在同一个 DBMS 内协同。
- 模型服务可以位于数据库外；数据库原生算子与 external vLLM/Ray 并不矛盾。
- LOTUS 提供成熟的 semantic operator/reference algorithm 研究和可运行 DataFrame 系统，适合兼容、算法和 baseline 用途。
- 当前 Daft/Ray/vLLM 代码是可复用的外部 physical provider 基座，并已有数据组织、准入和多 Job 机制证据。
- 项目的研究增量应放在 PostgreSQL-managed task 与多个外部 execution/serving components 之间的 same-work organization、admission、routing 和 multi-Job execution，而不是宣称重新发明 semantic operator model 或 vLLM continuous batching。

### 8.2 现在不能声称

- PostgreSQL 已实现 Sema-like first-class semantic operator。
- 现有 `AI_COMPLETE` manifest/profiler 等于 planner-visible `SemMap`。
- LOTUS v1.2.4 已有官方 Daft/Ray/project physical backend plugin。
- 项目 backend 替换 LOTUS LM 后仍属于 LOTUS native execution。
- PostgreSQL extension 方案等于复现了 Sema；Sema 修改 DuckDB 多层内部组件，当前 PostgreSQL 方案仍只是待实现的 extension design。
- Sema 已解决 multi-Job fairness、多 endpoint routing 或 distributed scale-out。
- 项目已经实现 semantic operator fusion、Sema prompt batching、Predicate Deduction 或 AQE。
- 当前离线 cost estimator 已驱动 SQL plan 或在线 scheduler。
- 当前复杂动态调度策略已经普遍优于独立校准的强静态配置。

## 9. 对主计划文档的直接建议

后续主计划可采用以下一句话作为对象说明：

> **本研究在 PostgreSQL 进程内实现一等 AI 语义算子；先使用 extension，只有目标计划优化被明确
> 阻挡时才采用最小 core patch。数据库选择 LOTUS/Cortex/Sema-like semantic path，规范化任务再通过
> `open/drive/close` 交由 SemLoom 完成 IMLane-like physical execution、work organization、multi-Job
> scheduling、endpoint routing 和 Ray/vLLM execution；Kalypso-like lineage 仅作条件性参考。**

文档中的主次关系建议统一为：

| 对象 | 后续角色 |
|---|---|
| Sema | SQL/operator/planner/optimizer/executor 的主要架构参照 |
| PostgreSQL extension / conditional core patch | semantic operator 的条件性实现载体，由 carrier audit 选择 |
| SemLoom execution provider | 外部物理执行与调度研究主体 |
| LOTUS | 可选 `lotus_compat`、operator algorithm/reference behavior 来源、独立 baseline |
| Cortex AISQL | function-like SQL、AI-aware costing/placement/cascade/join rewrite 参照 |
| IMLane | database batch pump、async submission、Lane/resource scheduler 与 Ray adapter baseline |
| Kalypso | stage dependency、prefix lease、KV-aware admission 与 virtual pinning 参照 |
| Palimpzest / DocETL / Abacus | semantic plan/rewrite/cost-quality optimizer 相关工作 |
| Daft / Ray / vLLM | 可替换 external execution/serving infrastructure；各自拥有原生 baseline 行为 |

这会替代“先把 LOTUS `SemMapNode` 变成整个系统入口，再围绕其内部 AST 建 PostgreSQL 架构”的旧顺序。若仍需要 LOTUS parity，应把它作为与通用 `SemMap` 同期或稍后的兼容性工作，不应阻塞 PostgreSQL 自有 operator IR 和 provider seam。

## 10. 一手来源索引

- Sema 论文：<https://arxiv.org/abs/2603.11622>
- Sema VLDB 2026 官方程序：<https://vldb.org/2026/program.html>
- Sema 官方 artifact：<https://github.com/BITQiKangK/SemaSystem>
- LOTUS PVLDB 论文：<https://www.vldb.org/pvldb/vol18/p4171-patel.pdf>
- LOTUS v1.2.4 release：<https://github.com/lotus-data/lotus/releases/tag/v1.2.4>
- LOTUS v1.2.4 fixed commit：<https://github.com/lotus-data/lotus/tree/b1a85fd7a66fabed8a1585d44d7597d592b4433f>
- LOTUS 固定版本源码入口：
  - <https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/lazyframe.py>
  - <https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/nodes.py>
  - <https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/run.py>
  - <https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py>
  - <https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py>
  - <https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/data_connectors/connectors.py>
- Palimpzest CIDR 2025：<https://www.vldb.org/cidrdb/papers/2025/p12-liu.pdf>
- Palimpzest 官方仓库：<https://github.com/mitdbg/palimpzest>
- DocETL PVLDB 2025：<https://www.vldb.org/pvldb/vol18/p3035-shankar.pdf>
- DocETL 官方仓库：<https://github.com/ucbepic/docetl>
- Abacus PVLDB 2026：<https://www.vldb.org/pvldb/vol19/p1060-russo.pdf>
- PostgreSQL 18 Custom Scan：<https://www.postgresql.org/docs/18/custom-scan.html>
- PostgreSQL PGXS：<https://www.postgresql.org/docs/18/extend-pgxs.html>

## 11. 2026-08-28 审计增补：原生语义算子、最小 PG core patch 与 provider interface

本节针对后续实现再回答四个问题：数据库内部 AI 语义算子作为数据执行链起点时，数据库必须拥有
哪些状态；Sema、Cortex AISQL、IMLane、Kalypso 和 LOTUS 分别证明了哪一层；PostgreSQL 18.3/18.4 extension
能验证什么、最小 core patch 应承担什么；数据库 semantic module 与 SemLoom execution provider 之间
最小而足够深的 interface 应是什么。

本次审查采用一个新增前提：**允许但不要求维护受控的 PostgreSQL 18.4 core patch。** 因而不能预先把
extension-only 或 core fork 任何一方写成终点。先用 extension capability spike 验证执行 seam，并以
LOTUS/Cortex 类计划优化能否安全表达、prepared-plan/node identity、hook composability 和维护成本为
证据选择载体；无论载体如何，外部都只通过同一个 SemLoom execution-provider seam 接收 sealed work。

### 11.1 一手来源身份与版本校正

| 对象 | 本次核对的一手来源 | 可据此确认的事实 | 不能据此确认的事项 |
|---|---|---|---|
| Sema | [arXiv:2603.11622v1 HTML](https://arxiv.org/html/2603.11622)、[作者 artifact](https://github.com/BITQiKangK/SemaSystem) | DuckDB 的语言、parser/planner、optimizer、executor 均被扩展；AST/逻辑计划含显式 semantic nodes；模型可在本地或远端 | artifact 只公开可执行压缩包、实验脚本和补充材料，不能作完整修改源码审计 |
| Cortex AISQL | [arXiv:2511.07663v3](https://arxiv.org/html/2511.07663)、[SIGMOD Companion DOI](https://doi.org/10.1145/3788853.3803093)、[Snowflake 官方优化说明](https://www.snowflake.com/en/blog/engineering/cortex-aisql-query-optimization/)、[官方函数文档](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) | 正式题名为 *Cortex AISQL: A Production SQL Engine for Unstructured Data*；v3 标注 SIGMOD Companion 2026；SQL engine 拥有 AI operators、AI-aware plan、运行期统计、cascade 与 join rewrite；Cortex Platform 另有 API Service、Scheduler、Inference Engines/partner endpoints | 论文不公开 Snowflake core 源码，也不是 PostgreSQL extension 实现指南 |
| IMLane | [PVLDB 19(12) 正式论文](https://www.vldb.org/pvldb/vol19/p4223-xu.pdf)、[DOI `10.14778/3827998.3828028`](https://doi.org/10.14778/3827998.3828028)、[作者 artifact `072f8257`](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)、[OceanBase 官方 publication list](https://github.com/oceanbase/publications#dataai) | 正式题名为 *IMLane: Composable Framework for Efficient AI Function Execution in Database Engine*，题录为 PVLDB 19(12): 4223–4236, 2026；系统不仅提供 DBEnd/data-conversion bridge，还实现独立的 per-function scheduler、Lane/resource lifecycle、batch-wise asynchronous scheduling 和可替换 Ray executor；OceanBase 与 DuckDB 都以物理 AI operator 对接 | 论文和 artifact 没有定义 Sema-like semantic algebra 或 semantic query rewrite；当前 artifact 也不足以证明 token/work-aware multi-Job fairness、SLO scheduling 或完整 query-cancel protocol |
| Kalypso | [arXiv:2607.23815v2 HTML](https://arxiv.org/html/2607.23815v2)、[本地精读笔记](精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md) | 正式题名为 *Kalypso: Relational LLM Serving*；当前版本为 2026-08-14 的 arXiv v2，未标注正式 venue；它在 SQPS 与 vLLM 之间接收 static left-deep semantic plan，以 pipeline/stage/task、parent-child dependency、动态 KV memory budget 和 virtual pinning 控制 admission | 它不是 DBMS bridge 或 semantic optimizer；论文未研究 PostgreSQL lifecycle、vector batch transport、Ray、多 endpoint 或 multi-Job fairness，截至 2026-08-28 未定位作者公开 artifact |
| PostgreSQL | [18.3 release](https://www.postgresql.org/docs/release/18.3/)、[18.4 release](https://www.postgresql.org/docs/release/18.4/)、[Custom Scan paths](https://www.postgresql.org/docs/18/custom-scan-path.html)、[plans](https://www.postgresql.org/docs/18/custom-scan-plan.html)、[execution](https://www.postgresql.org/docs/18/custom-scan-execution.html) | 18.3 与 18.4 是同一 major 的 patch releases；18.4 含 18.3 之后的修复；extension 可增加 CustomPath/CustomScan、child paths/plans、projection 与 executor callbacks | 官方文档称 Custom Scan 为 experimental facilities；文档没有保证任意 SemaSQL 语法或任意 semantic plan rewrite 可由 extension 完成 |
| LOTUS | [PVLDB 18(11) 论文](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)、[v1.2.4 release](https://github.com/lotus-data/lotus/releases/tag/v1.2.4)、[固定源码](https://github.com/lotus-data/lotus/tree/b1a85fd7a66fabed8a1585d44d7597d592b4433f) | 参考算法定义 operator behavior；filter/join/group-by/top-k 有相对 reference algorithm 的统计 accuracy target；产品入口是 DataFrame/LazyFrame | 不能由此推出 PostgreSQL SQL/planner/executor 所有权或稳定 physical backend registry |

IMLane 的会议轨道表述存在一处需要保留的资料差异：OceanBase 官方技术介绍称其被
“PVLDB 2026 Industry Track”收录，而 PVLDB 正式 PDF 和 OceanBase publication list 给出的可直接
核验字段是卷期、页码与 DOI。本文在论文题录中只记后三者；如需讨论轨道，则单独标明
“OceanBase 官方技术介绍称 Industry Track”，不由卷期字段反向推断。

PostgreSQL 官方 release notes 分别把 18.3 和 18.4 标为 2026-02-26、2026-05-14 发布；两者的选择不
改变本节的 module/seam 设计。若限定二选一，首个 prototype 应锁定 18.4，因为它是较新的修复版本；
所有 source-level 依赖仍须锁定 `REL_18_4` 并用 18.4 headers 构建，不能把“同属 18.x”理解成任意
二进制可混用。

### 11.2 第一性原理：数据库作为执行链起点意味着什么

**来源类型：Sema/Cortex 论文归纳 + PostgreSQL source 迁移 + 架构推断。** “起点”不是 SQL 文本中
出现一个 AI 函数名，而是以下状态先在数据库内成立：

1. PostgreSQL 在 parse/analyze 后能识别这是 `SEM_MAP`、`SEM_FILTER` 等 semantic operation，而不是
   会逐行执行的普通远程 UDF；
2. ordinary child plan 在当前 snapshot、ACL 和 transaction 下产生 tuple，且只投影 semantic operation
   真正需要的列；
3. 数据库拥有 operator kind、input/output types、prompt program、result parser、NULL/error/order
   semantics 与 quality/reference policy；
4. 数据库把 child tuple 编译成有稳定 identity 和 payload digest 的 `PreparedSemanticTask` 后，外部
   execution provider 才第一次获得数据；
5. 数据库决定 completion 如何变成 SQL Datum、tuple 是否保留、何时向 parent node 发出结果，以及
   LIMIT、query cancel、statement error 和 transaction abort 后哪些结果不可再见；
6. 会改变调用结构或关系结果的 semantic algorithm 必须是可见 physical plan choice，而不是 provider
   根据字符串暗中选择。

由此得到一条可检查的 seam：

```text
PostgreSQL Query/Path/Plan + ordinary child execution
  -> SemanticPlanSpec
  -> tuple-to-task compilation
  -> PreparedSemanticTask              # 数据第一次越过进程 seam
  -> SemLoom execution provider
  -> CompletionRecord
  -> PostgreSQL parse/fan-in/tuple emit
```

若 provider 接收原始 SQL、表名、snapshot token 后自行拉表，或者由 provider 决定 `SemFilter` 真值、
parser 和重试后的可见结果，那么 PostgreSQL 不是这条执行链的起点，只是一个触发器。

### 11.3 五个系统实际让数据库拥有什么

| 系统 | 数据库或 query engine 拥有 | 外部 execution/serving 拥有 | 对 SemLoom 的直接启示 |
|---|---|---|---|
| Sema | SemaSQL、AST 与显式 logical semantic nodes；expression compression、predicate deduction；vectorized/pipelined physical semantic operators；fusion、prompt batching、SemFilter AQE 和 reference-path comparison | local vLLM 或 remote OpenRouter 完成模型推理 | 语义 identity、rewrite 和 result semantics 留在 PostgreSQL；模型位于外部不妨碍“数据库内置” |
| Cortex AISQL | `AI_COMPLETE`/`AI_FILTER`/`AI_CLASSIFY`/AI aggregation 等 SQL operators；FILE type；LLM-aware cost/placement；运行期 predicate reorder；model cascade 和 semantic join rewrite | Cortex Platform 的 API Service、Scheduler、Inference Engines/vLLM 或 partner endpoint 完成路由和推理 | 数据库 optimizer 可以减少调用、换物理算法；请求路由和模型 serving 可在独立 platform |
| IMLane | DB engine 扫描数据、执行关系算子；物理 AI operator 消费 vectorized batch，实现 `SCHEDULE/OUTPUT` 状态、pending futures、完成收集与尾批次 drain；DBEnd 做 format conversion 与 asynchronous scheduling call | coordinator 按 AI function 管理调度队列、Lane/resource lifecycle 和可用并行度；独立 Python process 或 Ray executor 执行 AI function | IMLane 是 DB-to-runtime bridge **以及** execution-batch formation、异步提交/补位、资源匹配和 executor adapter 的直接参照；它不是 semantic SQL/rewrite 的主要参照 |
| Kalypso | 上游 SQPS 拥有 operator UDF、prompt、semantic algorithm 与已选 static plan；不是 PostgreSQL 自身 | Kalypso 把 plan 编译成 pipeline/stage/task，维护 waiting/running/dependency、KV budget、admission 和 prefix retention；vLLM 继续拥有 GPU request scheduling | provider 可消费数据库导出的最小 stage/dependency metadata，做 prefix lease 与 KV-aware admission；不能把 SQL cardinality、CP child 生成或整条 query output 所有权一并外移 |
| LOTUS | 不存在 PostgreSQL-owned SQL/plan；LOTUS library 自己拥有 DataFrame semantic program、reference algorithms 和 quality-aware approximations | LOTUS LM/LiteLLM 与模型 endpoint 拥有调用执行 | 迁移 operator vocabulary、reference algorithms 与 quality evaluation；不要迁移 DataFrame runtime ownership |

Sema 与 Cortex 回答“semantic operation 为什么必须进入 query plan”；IMLane 回答“数据库物理 AI
operator 如何将 vectorized batches 与数据库扫描重叠，并按异构资源异步提交到外部 runtime”；
Kalypso 回答“已选语义计划的依赖和 prefix 生命周期如何进入 serving admission”；LOTUS 回答“一个
semantic operation 应有哪些 reference/optimized physical algorithms”。这些证据互补。不能把 IMLane
的 physical scheduling 或 Kalypso 的 relational serving 当成 Sema 的 semantic algebra，也不能把 IMLane 缩减为
只负责 IPC 的“桥”；它的 batch-wise asynchronous scheduling 和 resource-aware per-function scheduler
正是 SemLoom 数据执行链需要比较与吸收的已有方法。

#### 11.3.1 IMLane 的精确 ownership：桥接与物理调度是一条合作链

**来源类型：IMLane PVLDB 论文 + 作者 artifact 源码。** 正式论文第 4–5 节和 Algorithm 2
将责任分在数据库物理算子、DBEnd、coordinator 与 backend executor 四个位置：

| 位置 | IMLane 已实现的责任 | 不应扩张成的结论 | 一手依据 |
|---|---|---|---|
| DB engine / 物理 AI operator | 数据库仍执行 scan 和 relational operators；OceanBase 的 pull-based `next()` 与 DuckDB 的 push-based `NEED_MORE_INPUT` 继续驱动 vectorized batches；AI operator 把 batch 提交后保存 pending futures，在 Lane 不可用时转向收集结果，输入结束后 drain 剩余完成 | 不代表 IMLane 已实现 semantic operator reorder、cascade、fusion 或 quality-aware rewrite | [论文 Algorithm 2 与 §5.2](https://www.vldb.org/pvldb/vol19/p4223-xu.pdf)、[DuckDB physical prediction projection](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/database/duckdb/src/execution/operator/projection/physical_prediction_projection.cpp) |
| DBEnd Library / Runtime | 将 DB batch 在 database-native format 与 ArrowLane 之间转换；通过 scheduling primitive 请求 Lane，把输入写入 shared memory，以 future/try-pull/pull 接口与数据库 operator 协作，完成后 restore Lane | DBEnd 不拥有 SQL semantic result、prompt algorithm 或候选关系集 | [`dbend_context.hpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/include/dbend/dbend_context.hpp) |
| Coordinator / Function Scheduler | 论文设计为每个 AI function 独立 scheduler；artifact 在函数声明 `instances > 0` 时建立 standalone scheduler，否则使用 default scheduler。一个 Lane 绑定一个 executor 及 CPU/GPU/remote resource unit，可用 Lane 数量限制函数并行度，分配后由 restore 归还；artifact 另提供 CPU-core hint 队列与无本地任务时的简单 work stealing | 这不是已证明的 token/work-aware admission、multi-Job fairness、SLO priority 或 endpoint load-aware routing | [论文 §4.2.1](https://www.vldb.org/pvldb/vol19/p4223-xu.pdf)、[`coordinator.cpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/coordinator/coordinator.cpp)、[`task_scheduler.cpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/coordinator/task_scheduler.cpp)、[`proxy_executor.cpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/coordinator/proxy_executor.cpp) |
| Backend Executor | 默认 CPython executor 在独立进程中执行；Ray executor 是可替换 adapter，把 Arrow table 交给 Ray function/actor 后取回结果；它不取代上层 IMLane Function Scheduler | Ray 支持不证明 IMLane 已修改 Ray scheduler，也不证明 Ray 拥有数据库 query lifecycle | [论文 §3.2.3 与 §6.4.2](https://www.vldb.org/pvldb/vol19/p4223-xu.pdf)、[`ray_executor.cpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/backend/ray/ray_executor.cpp) |

IMLane 的“组批”也要分两层理解。一层是 database physical operator 把 partition 内的 vectorized
chunks 视为更细的 execution batches，并可借用 IMBridge 的 `BatchController`/adaptive batch tuner
调整 batch size；另一层是 Algorithm 2 中的 batch-wise asynchronous submission：只要有 Lane 就继续提交
下一批，无 Lane 时收集 ready futures，最后 drain pending queue。这两层都是**物理执行组批/提交**，
不是 Sema 将多条 tuple 改写进一个 prompt 的 semantic prompt batching。

因此 SemLoom 应对 IMLane 实现一个完整而不是“只看 bridge”的 matched baseline：

```text
PostgreSQL SemanticUnary operator
  owns: child pull, row/task mapping, execution-batch formation, pending/reorder/drain
  -> DBEnd-like submit/try-pull/pull seam
SemLoom coordinator
  owns: bounded admission, ready-task grouping, continuous refill, Job share, endpoint/Lane route
  -> direct executor or Ray/vLLM adapter
```

这里的具体放置仍是待 prototype 回答的设计变量，而不是已经确定的实现事实。至少比较两种共享同一
provider seam 的形态：一是 IMLane-like 数据库算子先形成不可拆的 execution batch，再由 gateway 提交；
二是 PostgreSQL 只编译 sealed independent tasks，由 SemLoom 按 token/work/locality 组批并决定提交时机。
两者都必须保持 PostgreSQL 的 row/task mapping、pending/reorder/drain、LIMIT 和 cancel 语义；差异只在
不改变 semantic payload 的物理 batch membership 与 submission control。以吞吐、首末结果延迟、无用
overfetch、内存上限、endpoint idle gap 和 cancel waste 决定采用哪一种，不能从论文实现位置直接推定。

这个分法与 IMLane 有直接可比性，又保留 SemLoom 的研究空间。论文自述的 scheduler limitation 是为轻量和
通用性统一抽象 local 与 heterogeneous functions，可能忽略 latency、data movement 与 resource ownership 差异。
论文还提出 Lane 数量可由用户指定，或从保守值开始递增到性能不再提升/资源分配失败；而固定
artifact 直接可见的是系统 `n_executors` 与 UDF decorator `instances` 配置，本次源码审计没有找到
论文所述的在线 Lane 递增控制回路。因此该控制回路先标为“论文设计，artifact 实现待复核”，
不与已公开的 Lane queue 和 restore lifecycle 混为同一证据等级。配置源码见
[`coordinator.cpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/coordinator/coordinator.cpp) 与
[`udf.py`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/contrib/pythonpkg/imxx/udf.py)。
固定 artifact 中 `SchedLaneArgs` 虽有 `job_id`，当前 `TaskScheduler` 并未用它选择队列或份额；也未在
`DBEndRequestKind` 中公开 cancel request。所以 SemLoom 的 request/work 双上限、估计工作量组批、
multi-Job service share、endpoint state-aware routing 和 PostgreSQL query-cancel propagation 不应被写成 IMLane 已经
解决的问题。源码依据见 [`msg_defines.hpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/include/common/messages/msg_defines.hpp)、
[`enums.hpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/include/common/messages/enums.hpp) 和
[`backend_executor.cpp`](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/coordinator/backend_executor.cpp)。

#### 11.3.2 Kalypso 条件性参考：只考虑 plan-aware metadata，不迁移 query ownership

**来源类型：Kalypso arXiv v2。** Kalypso 的 scheduling unit 是“一条 tuple 执行一个 stage”，不是
IMLane 的 vectorized execution batch，也不是 vLLM 的 continuous batch。它按 blocking operator 切
pipeline、按 Cartesian Product 切 stage；上游 task 完成后生成 children，并在所有 children 完成前保留
parent prefix。scheduler 再根据 waiting queue 与 KV memory budget 决定 task admission、动态转移 stage
budget，并用 launch timing 与 LRU 假设实现 virtual pinning。

完整照搬这套 interface 不适合本项目：Kalypso 会在外部层生成 Cartesian Product children、维护 blocking
materialization 并返回整条 query output，而本项目要求 PostgreSQL 拥有 join cardinality、blocking operator
state、结果可见性与 query lifecycle。若未来真实 workload 证明 stage-aware admission/prefix reuse 有增量，
可评估更窄的候选合同：PostgreSQL 决定是否产生 child，SemLoom 只维护物理 lease、cache-domain
stickiness 与 KV-aware admission。该合同不是当前实施项。

首版单一 `SemMap` 不实现 query graph 或 prefix lease。只有 SemLoom 实测需要感知单算子 stage 或跨算子
dependency 且另行立项后，才评估协议扩展；否则 Kalypso 元数据会成为无人消费的 options bag。
目前截至 2026-08-28 没有定位到作者公开 artifact，因此实现只能以论文合同和独立测试为依据，不能声称
复用了 Kalypso 源码。

### 11.4 PostgreSQL 18.4：extension-only 与最小 core patch 的重新取舍

**来源类型：PostgreSQL 18 官方文档 + `REL_18_4` source + 工程推断。** 关键源码入口为
[`paths.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/optimizer/paths.h)、
[`planner.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/optimizer/planner.h)、
[`extensible.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/nodes/extensible.h)、
[`plannodes.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/nodes/plannodes.h) 和
[`analyze.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/parser/analyze.h)。对原生节点形态的
判断还核对了 [`primnodes.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/nodes/primnodes.h)、
[`pathnodes.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/nodes/pathnodes.h)、
[`execnodes.h`](https://github.com/postgres/postgres/blob/REL_18_4/src/include/nodes/execnodes.h) 和
[`execProcnode.c`](https://github.com/postgres/postgres/blob/REL_18_4/src/backend/executor/execProcnode.c)。

| 目标 | 纯 extension 判断 | 依据与限制 |
|---|---|---|
| `ai_semantic.map(...)` marker function、typed options、extension catalog/GUC | 可实现 | `CREATE EXTENSION` 可注册 function/type/operator；marker implementation 必须 fail closed，不能执行远端调用 |
| 在 analyzed Query 中识别 marker | 可实现 | `post_parse_analyze_hook`/planner hooks 可检查已解析 `FuncExpr`；它们不能让 raw parser 识别一套新 grammar |
| 保留 ordinary child path/plan，并在其上插入 unary semantic execution | 值得 prototype，官方结构支持所需形态 | `CustomPath.custom_paths` 与 `CustomScan.custom_plans` 可保存 child；`CUSTOMPATH_SUPPORT_PROJECTION` 和 `custom_scan_tlist` 可描述非 Var 输出 |
| 在 final/group/window/order 等 upper relation stage 添加候选 path | 可实现但依赖内部 planner structures | `create_upper_paths_hook` 接收 `UpperRelationKind`、input/output rel；首版只允许单个 final projection pattern，不能预先宣称支持任意 query topology |
| base scan / join 的 custom path | 可实现 | `set_rel_pathlist_hook` 与 `set_join_pathlist_hook` 是公开 hook；首版 `SemMap` 不应为了计算一列而替换 relation source ownership |
| 自定义 startup/total cost、rows、pathkeys 与 EXPLAIN | 可实现 | CustomPath 要提供 rows/cost/order；`ExplainCustomScan` 可显示脱敏 plan/provider state |
| child pull、异步 submit/poll、tuple emit、cleanup | 可实现 | `BeginCustomScan`、`ExecCustomScan`、`EndCustomScan`、`ReScanCustomScan` 是 required callbacks；parallel/mark-restore callbacks 可选 |
| 首版拒绝 rescan、backward scan 和 parallel | 可实现且合理 | 不设置对应 capability flags，并在无法保证行为的 plan shape 上报错；不能留下“半支持”路径 |
| SemaSQL 的 `s'...'` raw grammar | 需要 core parser 修改或维护 fork | extension API 能增加 SQL objects，但 `parser.h` 没有 raw-parser plugin seam；新的 token/grammar production 要修改 core grammar |
| 新的 core `SemanticExpr`/`SemanticPath`/`SemanticUnary` node、core optimizer 原生理解它 | 需要 core 修改或 upstream 支持 | CustomScan 是 physical extensibility facility，不等于把新 algebra 加进 PostgreSQL 的 rewrite/cost machinery |
| 任意跨 join/subquery/aggregate 的 semantic reorder/fusion 与专用统计 | 首版 extension 不应承诺 | 可以对受限 pattern 添加 custom paths；一般化搜索、合法性证明和 core-wide costing 会与 PostgreSQL internals 深度耦合 |

这个表说明 extension-only 具有真实价值，也揭示了需要实测的限制：`CustomScan` 可以承载物理执行节点，
但 analyzed query 中的 semantic identity 仍依附 marker；`create_upper_paths_hook` 可以添加候选，却可能在
`SemFilter` 的 join 前后 placement、semantic join rewrite 与多 clause 合法性上形成脆弱的 hook-side 模拟。
如果目标 LOTUS/Cortex 优化可在受限 pattern 下安全生成显式 CustomPaths，extension 可以继续作为正式
载体；若这些目标需要稳定的原生 expression identity 或 core-wide rewrite，再采用最小 core patch。

#### 11.4.1 三条实现路线

| 路线 | 形态 | 能回答的问题 | 主要代价 | 审查结论 |
|---|---|---|---|---|
| E：extension-only | marker `FuncExpr` + planner hooks + `CustomPath/CustomScan` | child plan、snapshot、cancel/error、typed result、异步 provider seam，以及受限 LOTUS/Cortex paths 是否可行 | semantic identity 仍依附 marker；跨 join/aggregate placement 与 core rewrite 可能需要脆弱的 hook-side 模拟 | **默认先实现**；若目标优化和 lifecycle 全部可表达，可继续成为正式载体 |
| K：最小 core patch | function-like SQL 降为 `SemanticExpr`，生成原生 `SemanticPath` 与 `SemanticUnary` plan/executor node；provider 仍是 loadable adapter | extension 被哪些具体 optimizer/node-lifecycle 需求阻挡，以及原生 node 能否解除阻挡 | 需要维护 NodeTag、planner、plan creation、executor、EXPLAIN/setrefs/node walkers 等受控改动 | **条件性采用**；只有 E 的已复现阻断足以抵偿 patch 维护成本时进入 |
| F：广泛 fork | 新 raw grammar、全套 semantic algebra、storage/transaction/serving 大范围修改 | 可追求与 Sema 类似的完整语言和 engine 改造 | rebase 面过大，且把当前研究问题与 parser/storage/serving 重写耦合 | 首轮不采用；只有 function-like SQL 无法表达必要语义时才单独论证 grammar patch |

若采用路线 K，关键不是“改得越多越原生”，而是只把 extension 无法可靠承载、且必须由数据库拥有的
语义放进 core，同时把变化频繁的
prompt profile、provider 配置、transport、Ray 和 vLLM 留在 companion extension 或外部进程。Cortex 的
SQL surface 本身就是 function-like `AI_FILTER`/`AI_CLASSIFY`；因此 first-class operator 不以发明
`s'...'` token 为前提，而以 analyzed/physical plan 是否显式保留 operator identity 为判断标准。

#### 11.4.2 采用路线 K 时的 PostgreSQL logical/path/plan/executor 形态

PostgreSQL 没有一棵与 DuckDB 同名、独立存在的“logical plan tree”：parse analysis 的 `Query`/`Expr`
是 analyzed representation，planner 用 `Path` 表示 physical alternatives，随后生成 `Plan`。推荐按这个
本地结构映射 semantic operator，而不是把 DuckDB 类名机械移植过来。

1. **Analyzed logical node：`SemanticExpr`。** SQL 首轮继续使用 `ai_semantic.map(...)`、
   `ai_semantic.filter(...)` 这类 typed functions；parse-analysis lowering 只把 catalog 注册且已完成类型
   检查的 marker 转为新的 `Expr` node。节点保存 `SemanticOpKind`、ordinary argument expressions、
   result type/collation、稳定的 semantic-spec identity 与 source location，不保存 provider session、HTTP
   payload 或 Ray object。semantic-spec catalog 变化必须触发 cached plan invalidation 并重新规划；任何未被
   semantic planner lowering 的 `SemanticExpr` 到达普通 expression executor 都必须 fail closed。
2. **Physical alternative：`SemanticPath`。** 首轮它是带一个 `subpath` 的 unary `Path`，其
   `pathtarget`、rows、startup/total cost 和 pathkeys 仍遵守 PostgreSQL 规则；额外保存
   `SemanticPlanSpec`、physical algorithm identity、reference/quality policy 与预测的 request/token/
   frame work。`REFERENCE`、`PROXY_ORACLE_FILTER`、`FUSED_FILTER` 或 `ADAPTIVE_FILTER` 应是不同、可在
   `EXPLAIN` 中辨认的 path alternative，不能由 provider 在运行时暗换。
3. **Executable plan node：`SemanticUnary`。** 它是原生 `Plan`，`lefttree` 指向 ordinary child plan，
   内含可复制/可序列化的 `SemanticPlanSpec`，首轮只允许 `SEM_MAP`、`SEM_FILTER` 和受同一 unary
   lifecycle 约束的结构化 projection/classification。`SEM_MAP` 必须每个 child identity 产生一个 typed
   output；`SEM_FILTER` 只能产生零或一个输出并保持 surviving tuples 的相对次序。名称不应使用
   `SemanticScan`，因为这个 node 包住任意 child，而不拥有 base-relation scan。只有 executor 用 identity
   reorder buffer 保持 child order 时，`SemanticPath` 才能继承 child pathkeys；否则必须清空 pathkeys。
4. **Executor state：`SemanticUnaryState`。** 它拥有 child `PlanState`、query-scoped provider session、
   bounded pending/in-flight/completion buffers、identity reorder state、result parser、reference/approximation
   statistics 和 cancel/error cleanup。`PreparedSemanticTask` 才是越过进程 seam 的对象；
   `CompletionRecord` 返回后由此 state 验证、解析、重排并决定 SQL-visible tuple。
5. **不同拓扑使用不同 node family。** 以下只规定未来若另行立项时的 module 形状，不表示这些算子
   已进入当前排期。`SemanticJoin` 应是 binary plan/path，`SemanticAgg`、
   `SemanticTopK` 和 `SemanticGroupBy` 应是 blocking state；不要把它们塞进 unary node 的
   `operator_kind` 分支。它们可以复用 `SemanticPlanSpec`、provider port、quality policy 和 telemetry，
   但不能共享错误的 cardinality、rescan 与 memory invariants。

`SemanticExpr` 的依赖分析应在 ordinary planner 选择前形成 semantic stages：当某组 referenced Vars 在
base/join relation 上全部可用时，planner 才能增加合法的 `SemanticPath`；semantic join alternatives 则在
join planning 中单独生成。final projection 的 `SemMap` 可以位于 upper relation，但不能把所有 semantic
operator 都强迫经过一个 `UPPERREL_FINAL` wrapper。这样才能支持当前受限 filter–join placement，并为
普通 cheap predicate ordering 和远期 join-to-classification 研究保留可表达性。

LOTUS-like quality constraint 应先淘汰没有达到 reference-relative target 的 path，再由 query option 把
latency、monetary cost 或 token work 映射到 PostgreSQL 的 scalar startup/total cost；原始 work 与质量
字段继续留在 `SemanticPath`/`EXPLAIN` 中。否则标准 `add_path` 只看到一个未经说明的 scalar cost，会把
质量不同的 semantic alternatives 当成普通等价计划比较。依赖运行期采样的 alternative 只有在 executor
无法验证 target 时自动回退 reference algorithm，才能被视为满足该 quality policy 的可行 path。

#### 11.4.3 条件性最小 core patch 的边界与 locality

以下是条件性 core surface 的上界清单，不要求同时实现；实际按 §11.9.4 分层触发。若阻断只在 planner
identity/path generation，executor 继续使用 `CustomScan`。若载体审查选择某一层 core patch，应把改动
压在一个深 module 周围，而不是把 semantic cases 散到 provider 或各类 SQL functions：

- node 定义与生成支持：`SemanticExpr`、`SemanticPath`、`SemanticUnary`/state，以及 PostgreSQL 要求的
  copy/equal/out/read、tree walker、setrefs 和 plan serialization 支持；
- `optimizer/semantic`：lowering validation、dependency/placement、reference 与 optimized path 生成、
  semantic cost/quality feasibility；普通 relation optimizer 只在少数明确 seam 调用它；
- `executor/semantic`：初始化、child pull、bounded async drive、typed fan-in、rescan rejection、cancel 和
  cleanup；`ExecInitNode`/`ExecProcNode`/`ExecEndNode` 只新增原生 node dispatch；
- provider registry：core plan 只保存稳定 provider/spec identity；companion `semloom_pg` module 在 backend
  启动时注册 provider factory 和 SQL/catalog objects，runtime C pointer 只存在 executor state；
- `EXPLAIN`/telemetry：显示 operator、reference/selected algorithm、predicted/actual work、quality policy 和
  provider identity，但不显示 prompt payload、secret 或 endpoint credential。

明确不进入 patch 的内容包括：新 raw grammar、MVCC/storage/access method 修改、PostgreSQL generic join
algorithm 重写、HTTP/Ray client 复制进 core、vLLM continuous batching 修改，以及 provider 根据 prompt
自行选择 semantic algorithm。新代码应尽量放在 semantic module 的新文件中，对现有 planner/executor
只保留 narrow calls；基线锁定 `REL_18_4`，用持续 rebase/patch-size audit 控制维护成本。

#### 11.4.4 extension capability spike 仍需保留

在 core patch 前先做的最轻 prototype 仍不是“替换 base table scan”，而是：识别 final target 中唯一的
row-preserving marker，使用 `create_upper_paths_hook` 生成包住 ordinary child path 的 unary `CustomPath`，
再由 `CustomScan.custom_plans` 保存 child plan。这样可先验证 snapshot、ACL、child projection、typed
result、prepared plan、LIMIT、`INSERT ... SELECT`、cancel/error 和 provider lifecycle，但结论只能是
“这个执行 seam 可行”，不能外推为 semantic optimizer 已经成立。

另有两个容易被忽略的 PostgreSQL 要求：

- `CustomScan` plan 会被 `copyObject`，所以 `custom_private` 不能存 session pointer、Python object 或任意
  C struct pointer；plan-time data 应使用 PostgreSQL 可复制 Node/Const，runtime session 只存于扩展的
  `CustomScanState`；
- 官方文档明确说 `ShutdownCustomScan` 并非总会在 `EndCustomScan` 前调用。因此 cancel/transport cleanup
  不能只挂在 optional shutdown callback；executor loop、error path 和最终 close 都要可重复释放资源。

### 11.5 LOTUS 类基础优化应落在哪里

**来源类型：LOTUS PVLDB 论文 + Sema/Cortex 对照 + 迁移推断。** 数据库拥有某个 physical algorithm
的选择与结果 semantics，不要求每次模型计算都在数据库进程内。推荐按下表拆分：

无论采用路线 E 还是 K，下表所有 database-side algorithm、quality policy 和 runtime state 都属于
PostgreSQL 进程内 semantic module；companion extension 可以注册 prompt/spec 与 provider adapter，但不能
替 optimizer 选择 reference/approximate algorithm，也不能替 executor 决定关系结果。

| 机制 | 选择与状态的 module | 模型/向量计算可由谁执行 | 原因 |
|---|---|---|---|
| 每个 operator 的 reference algorithm、accuracy target、failure probability 与 quality metric | PostgreSQL semantic optimizer/executor | execution provider 只执行数据库编译出的 reference tasks | 它们定义“正确输出接近什么”，不能随 provider 改变 |
| `SemFilter` proxy/oracle cascade、threshold sampling、accept/reject/uncertain region | PostgreSQL semantic physical plan 与 runtime state | proxy/oracle inference 可作为带 role/model requirement 的 tasks 交给 provider | cascade 会改变模型、调用数和输出判定，是 semantic algorithm，不只是 routing |
| `SemJoin` nested-loop reference、embedding similarity proxy、map-search candidate generation | PostgreSQL semantic join plan、candidate identity 与 final pair decision | embedding、search、oracle pair evaluation 可由不同 provider capability 完成 | candidate omission 会改变 relation cardinality，provider 不能暗中决定 |
| `SemTopK` pairwise comparison、quick-select rounds、embedding pivot selection | PostgreSQL blocking semantic operator state | 每一轮独立 comparisons 可交给 provider 并行执行 | pivot/round 决定后续 tasks 和最终顺序，属于 query execution state |
| `SemGroupBy` label discovery、clustering、assignment 与 sample validation | PostgreSQL blocking semantic operator state | embedding/clustering/helper-model calls 可由 adapter 执行 | group labels、membership 和质量判断属于关系结果 |
| `SemAgg` hierarchical reduce tree、partial-state schema 与 combine order | PostgreSQL aggregate physical plan/state | extract/combine/summarize tasks 可由 provider 执行 | 它改变 many-to-one 依赖图和 partial state，不是通用 request batching |
| semantic fusion 或 multi-tuple prompt batching | PostgreSQL semantic optimizer/executor | fused task 可交给 provider | 它改变 canonical messages、parser、调用数或 tuple independence |
| child tuples 到 execution batch 的切片/填充、row-task mapping、pending/reorder/drain | PostgreSQL semantic physical operator；provider capability 和 work estimate 可给建议上限 | 数据库把 sealed task batches 交给 provider | 这是 IMLane-like database/operator-side batch-wise execution state，直接影响 pull/`LIMIT`/cancel 与 SQL 输出顺序 |
| sealed independent-task 的 work/locality grouping、bounded admission、continuous refill、multi-Job share、endpoint/Lane route | SemLoom execution provider | SemLoom/Ray | 这是 IMLane coordinator/scheduler 的扩展位置，但不改变数据库已选 semantic algorithm 或 task payload |
| token-level continuous batching、KV/page management | vLLM | vLLM | 属于模型 serving implementation |

LOTUS 论文没有为 `sem_map` 提出与 filter/join/group-by/top-k 同类的专用 approximation；其 reference
behavior 是逐 tuple projection。因此首版 `SemMap` 的“基础优化”应先是 database-aware projection
pushdown、只传所需列、bounded asynchronous execution 和 same-work request organization，而不是虚构
一个 LOTUS `sem_map` optimizer。真正的 LOTUS algorithm parity 应从 `SemFilter` 开始，并同时保存
reference path 与 quality evidence。

### 11.6 Design-It-Twice 方案 A：极小而深的 provider interface

本方案只设计 PostgreSQL semantic module 与 SemLoom execution provider 之间的外部 seam。DB-AIEL 是
该架构层的名称，不进入 C/Python 类型名；SemLoom 是系统名，SemLoom execution provider 是一个
adapter 身份。

#### 11.6.1 Dependency 分类与 seam placement

| Dependency | 分类 | 处理方式 |
|---|---|---|
| plan validation、task compilation、digest、result parser、row fan-in | in-process | 全部藏在 PostgreSQL semantic module implementation 内，不为测试拆成外部 port |
| PostgreSQL 18.4 backend + selected semantic carrier + companion extension | local-substitutable | 用临时 18.4 cluster + regression/isolation tests 验证；不在 external interface 暴露 PostgreSQL internals |
| SemLoom gateway/runtime | remote but owned | 在唯一 external seam 定义 port；production 使用 Unix-domain-socket adapter，测试使用 recording/in-memory adapter |
| Ray cluster、SemLoom scheduler | remote but owned，但属于 provider implementation 内部 | 使用 provider 私有 adapters；不把 Ray types 泄漏到 PostgreSQL interface |
| OpenAI-compatible endpoint 或合作方模型 endpoint | true external | 由 gateway 后方的 endpoint adapter 处理；PostgreSQL module 不直接拥有第三方 protocol |
| vLLM | owned deployment 或 true external，取决于实验 arm | 无论身份如何都藏在 provider 后方；数据库只观察 completion/usage/terminal state |

计划中的 recording adapter 与 SemLoom UDS adapter 将分别提供测试和生产实现；两者尚未实现，当前
只能把该 seam 视为待资格验证的设计，而不能写成已经成立。

#### 11.6.2 Interface：三个 entry points

```c
typedef struct AiProviderPort AiProviderPort;
typedef struct AiProvider AiProvider;           /* configured adapter instance */
typedef struct AiProviderSession AiProviderSession;

typedef struct {
    uint32 protocol_version;
    AiQueryIdentity query;
    AiOperatorInstanceIdentity operator_instance;
    AiPlanDigest plan_digest;
    AiCapabilityRequirements capabilities;
    AiAdmissionLimits limits;       /* max accepted tasks / bytes / estimated work */
    TimestampTz query_deadline;
} AiSessionSpec;

typedef struct {
    PreparedSemanticTaskSlice tasks; /* caller-owned, immutable during call */
    bool end_of_input;                /* legal only when tasks.len == 0 */
    uint32 max_completions;
    TimestampTz wait_until;           /* now = nonblocking; bounded wait otherwise */
} AiDriveRequest;

typedef struct {
    uint32 accepted_prefix;           /* caller retains the unaccepted suffix */
    CompletionRecordSlice completions;
    bool accepts_more;
    bool drained;                     /* end seen + every accepted task terminal/delivered */
    AiBackpressureSnapshot pressure;
} AiDriveResult;

typedef enum {
    AI_CLOSE_DRAINED,
    AI_CLOSE_EARLY_STOP,
    AI_CLOSE_QUERY_CANCEL,
    AI_CLOSE_QUERY_ERROR
} AiCloseDisposition;

typedef enum {
    AI_DRIVE_OK,
    AI_DRIVE_FATAL
} AiDriveStatus;

struct AiProviderPort {
    AiProviderSession *(*open)(AiProvider *provider,
                               const AiSessionSpec *spec,
                               AiProviderError *error);
    AiDriveStatus (*drive)(AiProviderSession *session,
                           const AiDriveRequest *request,
                           AiDriveResult *result,
                           AiProviderError *error);
    void (*close)(AiProviderSession **session,
                  AiCloseDisposition disposition,
                  AiCloseReport *report); /* NULL-safe, bounded, non-throwing */
};
```

协议 v1 的 `open` 固定 query identity、唯一 opaque operator-instance identity、plan digest、capability 和
admission limits；
`drive` 是唯一数据面 operation，在一个调用中同时推进 submit、completion drain、backpressure 和
end-of-input state；`close`
统一 drained/early-stop/cancel/error cleanup。六个 `submit/poll/finish/cancel/close` methods 被收敛成一个显式状态机，
使高杠杆行为集中在一个 interface entry point。

当前 v1 只处理一个 query-scoped operator。仅当 SemLoom 实测需要感知单算子 stage 或跨算子
dependency，且另行立项后，才把 lineage wire 作为协议候选；届时可评估 operator/stage descriptors、
per-operator seal 与 lineage events。当前排期不包含这些字段，也不暴露原始 PostgreSQL Plan。

#### 11.6.3 Invariants、ordering 与 error modes

调用顺序只有：

```text
open -> drive* -> close
```

interface 的完整语义还包括：

1. session 只对应一个 query 和 immutable operator/spec digest set；v1 的 set 恰有一个 operator；
2. `drive` 只能接受 task slice 的连续前缀；`accepted_prefix` 之外的 task 没有进入 provider，caller 可在
   后续调用重送；
3. v1 的 `end_of_input=true` 只允许空 task slice；第一次为 true 时封闭唯一 operator 的输入，后续携带
   空 slice 的 true 调用幂等，caller 只用这些调用排空 completions；v2 使用 per-operator seal events，
   全部 operators sealed 后才封闭 query session；
4. provider 可以乱序完成，但每个 accepted task 最多产生一个 terminal `CompletionRecord`；健康 session
   的 normal drain 必须为每个 accepted task 交付一个终态。cancel/session-fatal 时，semantic module
   将尚无 provider 终态的 task 标为本地 abandoned，并禁止其产生 SQL-visible result；
5. task 数、bytes 和 estimated work 同时有上限；`accepted_prefix=0` + `accepts_more=false` 是正常
   backpressure，不是错误；
6. provider 不得修改 canonical payload、generation constraints 或 semantic algorithm role；数据库不把
   result parser、SQL expression、child tuple pointer 或 relation name交给 provider；
7. `drained=true` 只在收到 end-of-input 且所有 accepted tasks 的终态均已交付后出现；
8. `close` 可在任何时刻调用；它通过 pointer-to-handle 将 caller handle 置为 NULL，重复 close 是 no-op；
   cancel/error close 停止新 admission、best-effort 取消在途 work、丢弃 late completion，并在有界时间
   内返回；它不声称外部 inference 已回滚；
9. `open` 的 version/capability/plan rejection 直接使 query 失败；`drive` 的 transport disconnect、protocol
   violation 和 timeout 默认使 session 失败；per-task model/endpoint failure 通过 typed terminal completion
   返回；result parse failure 由 PostgreSQL semantic executor 按 plan policy 处理；首版不自动 retry；
10. PostgreSQL rescan 不复用已结束 session：首版拒绝 rescan，或由 semantic module 明确创建全新的
    query-scoped session，不能由 provider 猜测。

`drive` 的 C ownership 也属于 interface：request task memory 由 caller 持有且调用期间不可变；返回的
completion slice 由 session 持有，只保证在下一次 `drive` 或 `close` 前有效，caller 必须在本次调用后
立即验证并复制/解析。`AiBackpressureSnapshot` 只报告剩余 task/byte/work capacity，不暴露 queue、Ray
或 endpoint 内部状态。所有 adapters 用 status/error return，只有 PostgreSQL semantic module 决定何时
`ereport(ERROR)`；fatal `drive` 的 result 不可读取，唯一合法后继是 `close`。

#### 11.6.4 Usage

```c
session = port->open(provider, &spec, &error);
PG_TRY();
{
    while (!child_exhausted || pending_tasks || !provider_drained)
    {
        CHECK_FOR_INTERRUPTS();

        /* Pull only while local pending memory and provider limits allow. */
        compile_child_tuples_into_pending_tasks();

        request.tasks = pending_prefix();
        request.end_of_input = child_exhausted && pending_tasks == 0;
        request.max_completions = completion_capacity();
        request.wait_until = next_interruptible_deadline();

        status = port->drive(session, &request, &result, &error);
        drop_accepted_prefix(result.accepted_prefix);
        validate_parse_and_fan_in(result.completions);
        provider_drained = result.drained;
    }
    port->close(&session, AI_CLOSE_DRAINED, &report);
}
PG_CATCH();
{
    port->close(&session,
                QueryCancelPending ? AI_CLOSE_QUERY_CANCEL : AI_CLOSE_QUERY_ERROR,
                &report);
    PG_RE_THROW();
}
PG_END_TRY();
```

示例只展示 seam usage；实际 `SemanticUnaryState` 不能在 error unwind 中依赖会再次抛错或无限等待的
cleanup。

#### 11.6.5 Implementation 隐藏内容与 adapter

这个 module 的 depth 来自 `drive` 隐藏以下 implementation：

- versioned framing、handshake、partial read/write、Unix socket reconnect policy 和 PG latch/interrupt wait；
- task serialization、C/Python digest golden vectors、bounded local send/receive buffers；
- gateway session state、task deduplication、completion fan-in 与脱敏 evidence；
- `PreparedSemanticTask -> WorkDescriptor -> WorkUnit` 转换；
- SemLoom admission、持续补位、多 Job service share、endpoint routing、Ray actor/task 和 vLLM request；
- external endpoint auth/rate-limit/transport adapters；
- cancel propagation、late-result suppression 和 close report。

推荐 adapters：

1. `recording` in-memory/loopback adapter：确定性乱序、backpressure、duplicate/missing/crash 注入；
2. `semloom_uds` production adapter：连接 owned gateway；
3. simple HTTP comparison 也放在 gateway 后方，作为另一 provider implementation，不在 PostgreSQL
   extension 中再写一套 OpenAI client。

调用方与测试都只穿过 `open/drive/close` interface。transport parser、scheduler 和 Ray 的单测可以作为
implementation 内部测试保留，但数据库资格测试应只断言 SQL observable result、EXPLAIN、boundedness、
cancel/error 和 report，不读取 provider 私有队列。

#### 11.6.6 Trade-offs

- **Depth / leverage**：一个 `drive` entry point 同时覆盖 submit、poll、finish 和 backpressure，适合
  PostgreSQL pull executor；加入 recording、simple HTTP 或 SemLoom adapter 时不扩调用方知识。
- **Locality**：session transition、accepted-prefix 和 terminal-state 规则集中在 provider module；修复一次
  即覆盖所有 operator callers 和 adapters。
- **Seam placement**：semantic plan/task compilation 在 seam 左侧，work organization/Ray/vLLM 在右侧；
  adapter 不会变成 SQL semantic owner。
- **代价**：`AiDriveRequest/Result` 比独立 `submit()`/`poll()` 更抽象，若不断加入 transport-specific flags，
  它会退化成 shallow “万能消息”。因此只允许任务、完成、等待和 backpressure 四类稳定概念；新的
  scheduler knob 留在 provider config，不进入 interface。
- **为何不是一个 entry point**：把 open/close 也编码成 message 会隐藏 C resource lifetime 和 error-unwind
  ordering，降低可测试性；三个 entry points 是当前最小的实用面。
- **为何不是六个 entry points**：独立 submit/poll/finish/cancel 让合法状态组合分散到 caller，删除 module
  后复杂度没有消失而是回到 executor；收敛后的 module 具有更高 depth。

### 11.7 三种 interface 方案的比较与组合

为避免只围绕现有 `submit/poll` 形态做局部修补，本次还比较了三种刻意不同的设计：

| 方案 | 核心形态 | 最强之处 | 单独采用时的问题 | 采用方式 |
|---|---|---|---|---|
| A：条件性 core semantic module + 深 provider port | core 受阻层 + 外部 `open/drive/close` | 外部 seam 小，数据库与 gateway 的 ownership 清楚 | 若预设完整 `SemanticExpr/Path/Unary` 会提前扩大 patch | 只采用 provider interface 与 ownership；carrier 仍 extension-first、分层触发 |
| B：双轴 physical registry + sealed execution graph | semantic lowerer 与 execution provider 可独立组合，受限 graph 表达 cascade/fan-out/fan-in | 最容易扩展多模型 cascade、join rewrite、AQE 和多种 provider | 首版只有一个 reference implementation 时，dynamic registry、通用 graph interpreter 与 candidate cross-product 都是过早复杂度 | 只采用“semantic algorithm 与 execution provider 是两个正交轴”以及分离 digest 的原则；暂不实现通用 graph |
| C：executor-shaped tuple pump | plan node 只调用 `begin/next/stop/explain`，内部隐藏 child slot、bounded prefetch、reorder、latch 与 cleanup | 最贴合 PostgreSQL pull executor，调用方知识最少 | 若把 `execution_profile_id` 的解释留给 gateway，provider 可能重新拥有 semantic algorithm；单一 unary pump 也不适合 future join/aggregate | 作为当前 unary carrier 的内部 interface；semantic algorithm 仍由数据库 plan 明确选择 |

推荐组合不是三者的全集，而是保留各自最有杠杆的部分。默认 carrier 仍是 extension；下面的
`SemanticExpr/State` 只表示相应层出现已复现阻断后的条件性形态：

```text
marker or conditional SemanticExpr
  -> static semantic path alternatives             # REFERENCE / minimal FILTER alternative
  -> CustomScan; conditional native UnaryState only if executor lifecycle is blocked
       -> sem_exec_begin / sem_exec_next / sem_exec_stop / sem_exec_explain
            -> AiProviderPort.open / drive / close
                 -> recording or SemLoom gateway
```

首版不建立动态 lowerer discovery，也不在 PostgreSQL 内实现通用 DAG interpreter。`SemMap` reference 与
`SemFilter` reference 是第一组静态 path；当前只要求一条由 deterministic fixture 或规划前匹配证据支持的
最小第二 path。完整 proxy/oracle query-time sampling 与非 unary execution graph 仅作参考，需另行立项。

三个 identity 也应从首版分开：

- `semantic_spec_digest`：operator、input/output types、prompt/reference behavior 与 SQL-visible semantics；
- `physical_algorithm_digest`：reference/cascade/fusion、model roles、quality target 与 plan epoch；
- `provider_execution_digest`：gateway implementation、work estimator、routing/admission profile 与 endpoint
  capability，不参与定义 SQL result semantics。

`PreparedSemanticTask` 跨进程只需要 query-scoped task identity、sealed model request、physical algorithm
role、payload digest、capability/work hint 和 deadline。child row、pair 或 group 到 task/result 的映射应保留在
PostgreSQL executor state；provider 只回传 task identity。这样一条 fused task 可以对应多行，一个 cascade
也可以为同一行产生多个阶段任务，而不用把 wire contract 固定成“一个 row identity 等于一次模型调用”。

### 11.8 对当前整体计划的审查结论

当前计划的所有权主线是正确的：PostgreSQL 拥有 SQL、ordinary child plan、snapshot、semantic algorithm、
quality policy 和 SQL result lifecycle；SemLoom provider 拥有不改变算法语义的 work organization、admission、
routing 与多 Job scheduling。需要改变的是决策方法：**`CustomScan` 先作为 capability spike，同时也是
可行的无 fork 载体；最小 core semantic module 是在明确 optimizer/node-lifecycle 阻断后才采用的升级。**

推荐的主链路是：

```text
function-like SQL surface
  -> database semantic carrier       # E: marker+CustomPath；或 K: native SemanticExpr/Path
  -> explicit physical alternatives  # reference/LOTUS-like/Cortex-like choices
  -> unary semantic executor state   # ordinary child + DB-owned runtime state
  -> PreparedSemanticTask
  -> open/drive/close provider seam
  -> SemLoom execution provider
  -> CompletionRecord
  -> typed SQL tuple
```

近期实施顺序应调整为：

1. 锁定 `REL_18_4` source/header/build identity；保留 extension-only `SemMap CustomScan` 作为短期 capability
   spike，用 recording adapter 验证 child/snapshot/cancel/error/result/provider lifecycle；
2. 用明确反例审查 carrier：marker identity、prepared-plan invalidation、hook coexistence、受限
   filter–join placement 与 semantic alternative costing；全部可安全实现则继续 extension。若只在
   identity/path generation 受阻，先补 `SemanticExpr`/path-generation seam 并继续 lower 为 `CustomScan`；
   只有 executor lifecycle 也受阻时才增加 native Plan/State；
3. 在选定的 unary carrier 上完成 `SemMap` reference execution 与 `SemFilter` reference path，明确 row identity、
   order、NULL、parse failure、rescan、parallel、LIMIT 和 prepared-plan behavior；
4. 使用 deterministic fixture 或规划前匹配的静态 evidence 建立最小 `SemFilter` 第二 path，证明
   algorithm identity、quality policy、cost、prepared-plan 与 provider role 都由 PostgreSQL 管理。

完整 LOTUS-style query-time sampling/proxy-oracle cascade、Cortex predicate ordering/filter–join 扩展、
binary `SemanticJoin`、blocking operators、join-to-classification、fusion 与 AQE 都是远期参考方向；只有
unary 路径稳定、独立研究问题与证据成立并另行排期后，才决定是否实现。

这个顺序把三类风险分开：extension spike 验证 PostgreSQL executor seam；carrier audit 决定是否值得
支付 core 维护成本；LOTUS/Cortex path alternatives 验证 semantic optimization。若第一步失败，应先修
executor seam；若 carrier 尚未通过 lifecycle 与 planner tests，不应把外部 HTTP/Ray 调用称为数据库原生
语义算子；若优化没有 reference/quality evidence，不应只凭更少调用数宣称语义等价。

IMLane 应同时作为 DBEnd/data conversion bridge 与 batch-wise asynchronous/resource-aware physical
scheduling 的直接参照，而不是 semantic SQL/rewrite 的主要参照；Sema 提供 query/plan/executor
原生化的上界，Cortex 提供 function-like SQL 加 core AI-aware optimization 的直接工程参照，
LOTUS 提供 operator reference behavior 与 accuracy-aware physical algorithms。四者组合后，推荐的
patch surface 足以支撑研究目标，又没有把 parser grammar、storage、Ray 或 vLLM 一并变成
fork 维护责任。

### 11.9 LOTUS/Cortex 优化所需 planner/executor 能力与 PostgreSQL 载体判定

**来源类型：论文 + 官方源码审计 + PostgreSQL 官方 extension 文档 + 架构迁移推断。** 本节的
“可直接实现”只表示目标机制能由当前设计的 extension carrier 与 `open/drive/close` seam 表达，**不表示
仓库已经实现该机制**。四种判定的含义如下：

- **可直接实现**：不改变现有 unary semantic module 和 provider interface 的基本形状即可实现；
- **需扩展接口**：extension-first 仍可成立，但必须增加数据库内 plan/state；只有进程外调度确实需要
  观察 stage/dependency 时才增加 sealed task/completion 字段；
- **需条件性 core patch**：只有 carrier audit 复现 marker/path/node lifecycle 阻断后才进入最小 core patch；
- **目前不应承诺**：一手来源中的前提、统计保证或通用计划能力尚未在本项目复现。

#### 11.9.1 三种容易混淆的 reference/cascade 概念

| 概念 | 一手资料中的实际含义 | 对 SemLoom 的约束 |
|---|---|---|
| LOTUS `Reference Algorithm` | 逐 tuple 或逐 pair 调用主 LM 的 operator behavior；optimized algorithm 的 precision/recall target 是相对于该 reference 的统计目标，而不是相对于人工真值。[论文](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf) | PostgreSQL plan 必须保存 reference behavior、quality target 与 sampling evidence；少调用只能证明成本变化，不能自动证明语义等价 |
| Sema `Reference Path` | 对同一组连续 `SemFilter` 保留原始顺序、无 fusion/batching 的执行路径；候选 path 通过小样本与 reference path 比较一致性，再执行被选路径。[论文](https://arxiv.org/html/2603.11622) | reference path 是数据库 runtime 的候选/对照路径，不是 provider 的 fallback 字符串，也不是 ground truth |
| Cortex proxy/oracle cascade | binary `AI_FILTER` 的小模型置信度路由：高低置信结果由 proxy 接受，中间区间送 oracle；阈值按 batch 样本与预算、precision/recall 目标更新。[论文](https://arxiv.org/html/2511.07663v3) | proxy/oracle 是模型角色与统计执行状态；它不等于 relational reference path，且首版不能把分布式统计保证委托给 gateway 暗中完成 |

LOTUS v1.2.4 还提供名为 `CascadeOptimizer` 的 LazyFrame optimizer，但其源码行为是**在训练数据上执行
pipeline，以便节点学习并缓存阈值**，并不枚举或比较 PostgreSQL 式 relational paths
（[固定源码](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/optimizer/cascade.py#L24-L76)）。
因此 LOTUS cascade 应迁移为显式 physical algorithm 及其学习状态，不能据该类名声称已经获得通用
query planner。

#### 11.9.2 逐项能力与载体映射

| 机制 | 一手资料确认的算法结构 | planner 必须拥有 | executor 必须拥有 | SemLoom/PostgreSQL 判定 |
|---|---|---|---|---|
| `SemFilter` reference | LOTUS 对每个输入 tuple 执行主 LM predicate；Sema 也以逐 tuple 过滤作为 reference operator | 显式 unary operator/path、输入列依赖、输出 rows/selectivity、NULL/error/order policy | ordinary child pull、row identity、boolean parse、稳定 subsequence、bounded outstanding work、LIMIT/cancel/error cleanup | **可直接实现**。单个 `CustomPath/CustomScan` 加 ordinary child plan 足够；provider 只执行 sealed reference tasks |
| LOTUS `SemFilter` proxy/oracle | proxy 为全体行评分；importance sampling 加 uniform 防御样本送主 LM；学习正负阈值；高置信由 proxy 决定，中间区间由 oracle 决定（[阈值学习](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_filter.py#L139-L235)、[采样与路由](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_filter.py#L433-L603)） | reference 与 proxy/oracle 两种显式 algorithm；proxy/oracle model role、quality target、sample policy、预计 proxy/oracle calls 与算法 digest | proxy pass、sample selection/weight、oracle labeling、threshold state、accept/reject/uncertain fan-out/fan-in、reference fallback 与 quality evidence | **需扩展接口**，但不先要求 core。数据库 state 保存 sample、阈值、模型角色和行映射；首版可把各阶段编译成普通 sealed tasks。只有 SemLoom 需要按 stage/prefix 调度时，wire 才增加 stage role/parent identity |
| Sema 连续 `SemFilter` 的 reorder/fusion/batching/AQE | 保留原始顺序 reference path，同时比较 reordered、fused、batched paths；样本估计 selectivity/cost/一致性后执行选择结果 | 同一 pipeline 中多个 semantic filters 的依赖、候选 path、quality gate、样本代价与 reference identity | 多 stage sample execution、reference/candidate result 对齐、探索结果与剩余输入的无重复拼接、取消时统一回收 | **需扩展接口**。当前单 unary session 需增加 multi-stage plan/state；只承诺先做有限的连续 filter reorder，不承诺 Sema AQE/fusion/batching 完整复现 |
| Cortex predicate reorder | compile time 先按相对成本排序，runtime 可用实际 cost/selectivity 调整多个 predicate 的顺序 | 多 predicate 依赖、预计调用成本/selectivity、合法顺序与重新规划边界 | 分 stage actual calls/tokens/selectivity、只对尚未处理的输入采用新顺序、结果可追溯 | 有限的单 child 连续 filters 为**需扩展接口**；跨任意子查询和 pipeline 的 adaptive reorder **目前不应承诺** |
| Cortex filter–join placement | AI-aware optimizer 可把 `AI_FILTER` 放到普通 join 前或后；论文实验显示选择取决于 join 输入/输出 cardinality 与 LLM 成本，而不是“一律下推”[论文](https://arxiv.org/html/2511.07663v3) | semantic predicate 只依赖哪一侧、join cardinality/output-input ratio、filter selectivity/LLM cost、上下两条合法 path、path identity/cost | 被选 path 的 ordinary join child、实际 rows/calls/tokens、prepared-plan/rescan/cancel 下的稳定行为 | 单 filter + 单 inner join 的候选生成和 costing 属于 extension **可直接审计**，生产实现为**需扩展接口**；若 hook coexistence、prepared plan 或跨 join path identity 出现已复现阻断，才是**需条件性 core patch**；任意多 join placement 目前不应承诺 |
| `SemJoin` reference | LOTUS reference 对左右笛卡尔积逐 pair 调主 LM；Sema 也定义 semantic join，但未给专门 optimized join path | 显式 binary operator/path、两个 child plans、pair cardinality、join type 与输出 schema | 左右 row/pair identity、nested-loop 或 materialization state、pair completion fan-in、outer/NULL/error/order/rescan/cancel semantics | **需扩展接口**。PostgreSQL Custom Scan 可携带多个 child plans，但当前 `SemanticUnaryState` 必须新增 binary module/state；不因此自动需要 core |
| LOTUS optimized `SemJoin` | 比较 `Search-Filter` 与 `Map-Search-Filter`：后者先把左值 map 到右侧 domain；两者再做 similarity candidate generation，采样 oracle 学习阈值，并仅对不确定 pairs 调主 LM；源码按预计 oracle calls 选择策略（[候选比较](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_join.py#L424-L545)、[阈值样本](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_join.py#L547-L620)、[启用条件与 reference fallback](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_join.py#L761-L812)） | `NESTED_LOOP_REFERENCE`、`SEARCH_FILTER`、`MAP_SEARCH_FILTER` 三类 algorithm；embedding/index dependency、map instruction、quality target、采样成本和漏 pair 风险 | binary row/pair lineage、map/search/sample/oracle stages、score threshold、candidate accept/reject/uncertain、reference fallback | **需扩展接口**：binary state、DB-local stage lineage、model roles 和 pair-level quality evidence；只有 SemLoom 要利用 stage/prefix dependency 时才扩 wire。extension 先行；binary lifecycle 被实测阻挡时才采用**条件性 core patch**；未复现统计目标前不承诺 LOTUS 等价精度 |
| Cortex semantic join rewrite | compiler-time AI oracle 根据 prompt、schema、distinct statistics 与 sample values 判断适用性和 label side；把 cross join + `AI_FILTER` 改成 multi-label `AI_CLASSIFY`，再展开 pairs；label 可能分块，且 precision/recall 会变化[论文](https://arxiv.org/html/2511.07663v3) | rewrite applicability/direction、distinct/cardinality statistics、不同 result semantics 的独立 physical alternative、quality policy 与回退 | label chunking、classification parse、pair expansion/dedup、reference/candidate quality comparison | 不是普通 provider routing。SemJoin reference 稳定后才可做受限研究 prototype；一般化 rewrite 和“语义等价”均**目前不应承诺**。extension 无法稳定表示 rewrite identity 时才考虑**条件性 core patch** |
| Cortex adaptive model cascade | 论文限定 binary `AI_FILTER`；workers 对 batch 独立采样与更新阈值，受 oracle budget、precision/recall 目标约束，预算耗尽时退回 proxy | cascade path、模型角色、budget/quality policy、统计版本与失败策略 | batch-local sample/threshold state、oracle budget accounting、worker telemetry、取消后的 late-result suppression | 单进程/单 worker prototype 为**需扩展接口**；论文级分布式统计保证、任意 operator/model cascade 与生产 parity **目前不应承诺** |

这里的 cost 与 quality 不能压成一个标量。planner 先用 reference-relative quality target、支持的 operator
shape 和模型能力排除不合法 path，再比较预计 request、prompt/output token、sample/oracle call、candidate pair
和 ordinary relational cost；executor 分别记录实际 calls/tokens、sample size、selectivity、阈值与
reference-relative quality evidence。SemLoom 可报告执行成本和容量，但不得用 endpoint 负载绕过数据库的
quality gate 改选 semantic algorithm。

#### 11.9.3 普通 PostgreSQL path 等价性与显式近似语义

PostgreSQL 对同一 relation 的普通 paths 做 cost competition 时，默认它们实现同一查询结果。LOTUS
proxy/cascade 只给出相对于 reference algorithm 的统计 quality target；Cortex join-to-classification 明确可能
改变 precision/recall。即使只是把随机或随时间变化的 AI predicate 移到 join 前后，若没有固定模型版本、
输入键和可复用结果，也不能自动视为等价。因此这些机制**不能未经声明就与 exact/reference path 放入同一
普通等价 path 集合，仅凭较低 cost 获胜**。

数据库应至少区分三类 `quality_policy`：

| policy | 允许的候选 | 选择条件 |
|---|---|---|
| `EXACT_REFERENCE`（默认） | reference algorithm；以及有静态证明不会改变调用输入与结果映射的关系 placement | 只比较等价候选的 cost；没有证明时保留 reference |
| `REFERENCE_RELATIVE_TARGET`（显式 opt-in） | LOTUS-style proxy/oracle、Sema-style candidate/reference comparison | plan 保存 reference identity、precision/recall/confidence/failure target 与验证方式；quality 合格后再按 cost 选择 |
| `APPROXIMATE_REWRITE`（显式 opt-in） | Cortex join-to-classification 等会改变结果集合的 rewrite | SQL/session 明确授权、`EXPLAIN` 显示 rewrite 和 quality policy，并保留可执行 reference/审计路径 |

opt-in 必须来自数据库侧 SQL 或 statement policy，进入 semantic/physical digest，并随 prepared plan 保存；
provider capability 或实时负载不能把 exact query 自动升级成 approximate query。静态 candidate 与运行期
adaptive state 也应分开：

- **静态 candidate** 在 planning 时确定 operator shape、quality policy、reference identity、模型角色和
  算法族；例如已授权的 proxy/oracle filter 或 join-to-classification rewrite；
- **运行期 adaptive node** 只在已选 algorithm 与 policy 内学习阈值、采样或调整剩余输入的 predicate
  次序。它拥有 exploration/exploitation、budget 和 evidence state，不在执行中任意替换 PostgreSQL plan；
- 若 runtime 观察证明不了目标 quality，node 按计划规定回退 reference 或报错，不能让 SemLoom 自行降低
  quality target。

首版多阶段状态可以完全留在数据库 executor：同一个 adaptive node 依次生成 proxy、sample-oracle、
uncertain-oracle tasks，并以本地 row/pair map 连接结果，wire 无需出现通用 DAG。只有 SemLoom 的 admission、
prefix reuse 或 work estimation 确实需要识别 stage dependency 时，才给 sealed task 增加最小的
`stage_role`、`parent_task_id` 或 prefix key；这项需要可以在**一个** cascade/SemJoin 内出现，不必等到查询中
存在两个 semantic nodes。

#### 11.9.4 extension-first 与 conditional-core 的具体分界

PostgreSQL 18 的 [Custom Scan path](https://www.postgresql.org/docs/18/custom-scan-path.html)、
[plan](https://www.postgresql.org/docs/18/custom-scan-plan.html) 和
[execution](https://www.postgresql.org/docs/18/custom-scan-execution.html) 已提供 private path/plan data、一个或
多个 child paths/plans、cost 字段和 begin/exec/end/rescan callbacks。因此以下能力应先在 extension 中验证：

1. unary `SemMap`/`SemFilter` reference，以及显式近似 policy 下的 LOTUS-style proxy/oracle filter；
2. 一个 child pipeline 内有限的 predicate reorder；
3. 单 inner join 周围的 filter-before-join / join-before-filter 两条候选 path，并验证 exact proof 或显式
   quality opt-in；
4. 只审查 binary child/path hooks 的载体形状，不实现 `SemJoin`；实现需另行立项。

以下情形只是 core patch 的**触发测试**，不是预设结论：

- semantic marker 在 prepared/cached plan 中丢失或被当作普通逐行函数提前执行；
- planner hooks 与其他 extension 无法共存，导致目标 alternative 不能稳定生成；
- filter 跨 join 的合法 path 只能依赖脆弱的成品 plan mutation，无法保留 path identity 和成本；
- binary semantic node 的 copy/serialization/rescan/child ownership 不能用 `CustomScan` 稳定表达；
- 多条 semantic alternatives 无法在标准 path competition 中保持独立 identity 与 quality metadata；
- extension 无法阻止未授权的近似 path 进入 exact path competition，或 prepared plan 无法保存/展示
  `quality_policy`、reference identity 与 opt-in 状态。

只有同一阻断能在锁定的 `REL_18_4` 源码、最小复现和 lifecycle 测试中重复出现，才把对应缺口补成最小
core surface，而且可以分层升级：若阻断只发生在 semantic identity、quality-aware path generation 或普通
pathlist 的等价性假设，先增加 `SemanticExpr`/path-generation seam，并继续让选出的 path lower 为
`CustomScan`；只有 CustomScan 的 child ownership、copy/serialization/rescan 或 executor lifecycle 也有独立
复现阻断，才增加 native `SemanticPlan/State`。一次 planner 阻断不构成整套 native node 的理由。

core patch 只承载 semantic identity、quality policy、path construction、cost/quality metadata，以及确有
需要时的 executor lifecycle；LOTUS/Cortex 算法状态仍在数据库 semantic module，SemLoom 的 work
organization、admission、routing 与 Ray/vLLM adapter 仍在进程外。

#### 11.9.5 当前可承诺范围

据此，近期承诺止于 `SemMap` reference、`SemFilter` reference、由 deterministic fixture 或规划前匹配
evidence 支持的最小第二 filter path，以及受限 filter–join carrier audit。完整 proxy/oracle sampling、
`SemJoin` reference/LOTUS alternatives、Cortex join-to-classification、blocking operators、fusion 与 AQE
均为条件性研究候选，不构成当前排期。

目前不应承诺任意 join graph 的 semantic placement、Sema AQE 完整复现、Cortex 分布式 cascade 的论文级
统计保证、LOTUS optimized join 的相同 precision/recall，或把 cross join + filter rewrite 宣称为语义保持。
这些结论既不能由接口预留推出，也不能由更少的模型调用数推出。
