# Sema-like 数据库原生语义算子架构参照审计

更新日期：2026-08-27

## 0. 研究问题与结论

本文回答三个问题：

1. Sema 如何划分 SQL、语义算子、查询计划、优化器、执行器和模型服务的职责；
2. 哪些设计适合作为本项目的目标架构，哪些能力当前仍未实现；
3. 为什么 LOTUS 应保留为语义兼容来源、算法参考和独立系统 baseline，而不再承担总架构参照。

结论如下。

> **主要架构参照应改为 Sema 所代表的数据库原生语义算子路线：数据库拥有 SQL 接口、算子语义、查询计划、关系 child plan、运行期执行状态和结果生命周期；外部执行 provider 只接收数据库已经规范化的语义任务，并负责不改变任务语义的 work-unit 组织、准入、路由、多 Job 调度与 Ray/vLLM 执行。LOTUS v1.2.4 不应是 PostgreSQL 插件的中心运行依赖，而应作为可选 `lotus_compat` 语义适配器、语义算子算法来源和可运行系统 baseline。**

这一判断同时受两类证据支持：

- **论文与官方材料**：Sema、LOTUS、Palimpzest、DocETL 的论文、官方代码仓库或实验 artifact；
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
| Execution provider interface | session 生命周期、规范化 task submit/poll/cancel、completion telemetry | 重新解释 SQL、改写 prompt、改变 parser 或结果语义 |
| Project provider | work estimation、独立任务的 work-unit 组织、bounded admission、持续补位、多 Job 份额、endpoint route、Ray transport | 把多 tuple 合并成一个新语义 prompt、修改算子 reference behavior |
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

项目 provider 将其扩展为执行描述：

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
  由 project provider 选择并保持 task 语义不变
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

它支持本项目在通用 semantic plan 中保留 `logical operator → multiple physical implementations`，而不是把 `lotus_compat`、direct HTTP 和 project provider 写成互斥的顶层架构。它也说明 cost interface 需要同时携带 quality、cost、latency 与 uncertainty，而不是只返回一个 PostgreSQL 风格标量。

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
| semantic operator fusion / multi-tuple prompt batching / AQE | 未实现 | 后置；必须有 reference/quality evaluation 后再做 |
| `SemFilter` / `SemExtract` / `SemJoin` / `SemAgg` | 未实现 | 逐算子扩展，不能从 manifest/profiler 自动重标 |

因此，已有外部实验可以说明 provider 设计中哪些 organization/admission 机制可运行、哪些策略只在特定 workload 下有效；它们不能证明 PostgreSQL 已拥有 native semantic plan、snapshot/cancel/error/result lifecycle。

## 7. 推荐的实施顺序

**来源类型：论文迁移 + 项目实现推断。** 以下顺序将 Sema 的数据库所有权与现有 provider 基座连接起来，同时避免把全部 Sema 优化一次性复制到 PostgreSQL。

### 7.1 最小 native `SemMap`

- 定义项目自有 `SemanticPlanSpec`、`PreparedSemanticTask` 和 `CompletionRecord`；
- 用普通 extension SQL surface（例如 `ai_map`）完成 binding 和类型检查；
- 通过 planner/executor hook 或 `CustomScan` 形成显式 physical node；
- 让 node 消费 ordinary child plan 的 tuple batches；
- 验证 snapshot、cancel、error、row identity、order、result parser 和 query completion；
- 接一个最小 direct HTTP provider 与现有 project provider。

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

### 7.4 后置实现 semantic rewrite 与 AQE

- relational necessary-condition deduction；
- operator reordering；
- semantic fusion；
- multi-tuple semantic prompt batching；
- reference-path consistency 与用户 quality tolerance；
- 小样本 explore/exploit 与 Pareto path choice。

每项都应有独立 reference behavior、质量指标和 rollback 条件，不能把项目当前的独立-task batching 直接改名为 Sema prompt batching。

### 7.5 最后扩展 join/aggregate 与分布式语义计划

`SemJoin` 会改变输入 cardinality 和 pair generation；`SemAgg` 是 blocking/many-to-one operator。它们与 row-preserving `SemMap` 的 lifecycle、memory 和 failure semantics 不同，应在 unary operators 稳定之后再设计，而不是只扩一个 `operator_kind` 枚举。

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

> **本研究在 PostgreSQL 中以扩展方式实现一等 AI 语义算子，参考 Sema 的数据库原生语义查询架构，使语义算子进入查询计划与查询生命周期；算子产生的规范化任务通过可替换 execution provider 交由项目系统完成 work estimation、work-unit organization、admission、multi-Job scheduling、endpoint routing 和 Ray/vLLM execution。**

文档中的主次关系建议统一为：

| 对象 | 后续角色 |
|---|---|
| Sema | SQL/operator/planner/optimizer/executor 的主要架构参照 |
| PostgreSQL extension | 项目数据库内 semantic operator framework 的实现位置 |
| Project execution provider | 外部物理执行与调度研究主体 |
| LOTUS | 可选 `lotus_compat`、operator algorithm/reference behavior 来源、独立 baseline |
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
