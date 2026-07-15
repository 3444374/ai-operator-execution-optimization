# Daft/Ray/Lance 方向文献与证据审查

生成日期：2026-07-09

## 1. 结论先行

当前方向不是拍脑袋。Ray object、task、object store、Daft partition/shuffle、join strategy、Arrow/Lance columnar data 这些点都有论文或官方文档依据。

但需要严格区分：

| 类型 | 当前状态 |
|---|---|
| Ray 作为 AI infra / distributed execution 框架 | 有论文支撑 |
| Ray object store / ObjectRef / task dependency 是核心机制 | 有官方文档支撑 |
| 过细 task、重复传大对象、ObjectRef 使用不当会伤性能 | 有 Ray 官方 anti-pattern 支撑 |
| Daft 在 Ray runner 上执行，partition/shuffle/join 会影响性能 | 有 Daft 官方文档支撑 |
| Daft shuffle 中 `M × N` object/slot 数会导致 head-node metadata 和调度问题 | 有 Daft 官方文档直接支撑 |
| Arrow/Lance 适合 AI/columnar 数据链路 | 有论文支撑 |
| 我们当前优化策略在真实 Daft workload 上一定有效 | 尚未证明，需要后续端到端实验 |

因此，当前最稳的课题表述是：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

技术切入点是：

> 面向数据库驱动 AI workload 的特征感知数据组织、并行执行与存储协同优化。

当前最强主动机不应来自单独的 object/fan-in microbenchmark，而应来自生产式 GPU-backed E2E profile：数据库/SQL 触发 AI workload，数据进入 Daft/Arrow 数据组织、Ray task/actor 执行和 GPU-backed 模型服务，再写回 Lance / pgvector / PostgreSQL sink，并显示数据执行、模型服务队列、fan-in 或 writeback 的损耗足够明显。Ray object、Daft shuffle、Arrow/Lance 等资料用于解释这些损耗为什么可能出现，以及后续应该调哪些变量。

## 2. 为什么这个方向和 AI infra 相关

Ray 的原始论文将 Ray 定位为面向新兴 AI 应用的分布式框架，核心是同时支持 task-parallel 和 actor-based 计算，并用动态执行引擎、分布式调度和分布式存储支撑 AI workload。

这说明 Ray 不是传统数据库组件，而是 AI infra 生态中常见的执行运行时。你的目标是未来做 AI infra，因此研究 Ray task/object/shuffle 的开销，比传统数据库 GPU 查询算子更贴近职业预期。

参考：

- Ray paper: https://arxiv.org/abs/1712.05889

## 3. Ray 优化大概是在做什么

这里的 Ray 优化不应该泛化成“改造整个 Ray”。更准确地说，有四类：

### 3.1 Task 粒度优化

Ray 官方文档明确指出，过度并行化、task 太细会伤害加速比。原因是分布式 task 的调度和执行有额外开销；如果 task 本身太小，开销可能超过计算本身。官方建议是 batching，让每个 task 做更有意义的工作。

这对应论文中的可能方向：

- task batching；
- partition 粒度控制；
- Daft physical plan 到 Ray task graph 的粒度优化。

但我们当前实验显示，warm-up 后 Ray small task 延迟约 `0.183 ms`，不是最强瓶颈。因此不建议把“轻量 scheduler / runtime”作为第一主线。

参考：

- Ray anti-pattern: too fine-grained tasks: https://docs.ray.io/en/latest/ray-core/patterns/too-fine-grained-tasks.html

### 3.2 Object 传输与引用优化

Ray 官方文档说明：

- Ray remote objects 存在分布式 shared-memory object store 中；
- ObjectRef 是远程对象的引用；
- object 可通过 remote function 返回，也可通过 `ray.put()` 创建；
- 顶层 ObjectRef 作为 task 参数时，Ray 会在 task 执行前解析并拉取对象数据；
- numpy array 等对象可有 zero-copy 路径，其他对象需要反序列化。

这说明 object passing 是 Ray 的核心执行路径。优化不是随便改，而是围绕：

- 减少 object 数量；
- 避免重复存储大对象；
- 避免不必要 `ray.put()`；
- 控制 fan-in / fan-out；
- 降低 object metadata 与 reference tracking 成本。

我们当前实验中，固定 `16MB` 总数据量时：

| object 数量 | fan-in 时间 |
|---|---|
| 1 | 约 7.27 ms |
| 256 | 约 18.85 ms |

说明 object 数量放大会拖慢下游 fan-in。这与 Ray object 机制和 Daft shuffle 文档的风险一致。

参考：

- Ray objects: https://docs.ray.io/en/latest/ray-core/objects.html
- Ray anti-pattern: pass large arg by value repeatedly: https://docs.ray.io/en/latest/ray-core/patterns/pass-large-arg-by-value.html
- Ray anti-pattern: returning `ray.put()` ObjectRefs: https://docs.ray.io/en/latest/ray-core/patterns/return-ray-put.html

### 3.3 Shuffle / fan-in 优化

Daft 官方文档直接说明：

- `repartition`、hash join、sort、groupby 都是 shuffle；
- shuffle 是 all-to-all data movement；
- partition count 是 shuffle cost 的输入；
- Daft 的 `map_reduce` shuffle 使用 Ray object store，并为每个 `(input, output)` slot 产生一个 object；
- `pre_shuffle_merge` 会先合并 input partitions，降低 slot count；
- `flight_shuffle` 用本地磁盘和 Arrow Flight 减少 head-node bookkeeping 成本。

这非常关键，因为它直接支撑我们的核心问题：

> 大量中间 object / partition slot 会导致 Ray/Daft shuffle 变重。

Daft 文档还给出量化估计：每个 tracked object 约有 metadata 成本；当 `M × N` slots 很大时，driver/head-node metadata 会达到 GB 级别，可能导致 OOM 或 scheduler stall。

因此，最有价值的优化方向不是“优化 Ray 一切”，而是：

- object coalescing；
- pre-shuffle merge；
- partition-aware execution；
- fan-in object 数量控制；
- shuffle algorithm selection；
- 在 Daft 层提前控制 repartition 数和 batch size。

参考：

- Daft partitioning and batching: https://docs.daft.ai/en/stable/optimization/partitioning/
- Daft shuffle algorithms: https://docs.daft.ai/en/stable/optimization/shuffle/
- Daft join strategies: https://docs.daft.ai/en/stable/optimization/join-strategies/

### 3.4 DataFrame / SQL 系统中的同类优化

Spark 官方文档也说明，SQL/DataFrame workload 的性能调优包括：

- partition tuning；
- join strategy selection；
- adaptive query execution；
- coalescing post-shuffle partitions；
- skewed shuffle partition splitting；
- broadcast join；
- storage partition join to avoid shuffle。

这说明“调 partition、coalesce shuffle partitions、选择 join strategy、避免不必要 shuffle”不是 Daft/Ray 特有的小技巧，而是分布式数据处理系统中的成熟问题。

参考：

- Spark SQL performance tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html

### 3.5 Ray Data / Core / Serve 支持跨层调度实验

新一轮方向不应只停留在 object/fan-in/coalescing。Ray 的公开接口已经覆盖了更高层的 AI infra 控制面：

- Ray Data 面向 AI workload 的批处理数据处理，并通过 `map_batches()` 暴露 batch 与 concurrency 控制；
- Ray Core 支持 task / actor 的 CPU、GPU、自定义资源声明，也支持 placement group 和 scheduling strategy；
- Ray Serve 支持 dynamic request batching、LLM request routing 和 autoscaling。

这些资料说明，任务划分、actor 池大小、`batch_size × concurrency`、GPU-backed model-service 资源配比、placement、模型服务 routing / batching / backpressure 都有可实验的系统接口。这里的 GPU 不是研究“数据库算子迁移到 GPU kernel”的主线，而是外部模型服务的现实计算端点，应该尽早进入端到端主动机画像。因此候选方向可以从“数据粒度控制”扩展为：

> 面向数据库 AI 算子的特征感知并行执行与跨层调度。

但这里必须保持证据边界：官方文档只能证明接口和机制存在，不能证明它们在当前数据库 AI 算子链路中就是瓶颈。是否值得作为论文核心贡献，需要后续 GPU-backed E2E profile 记录 queue wait、actor idle time、token backlog、object store pressure、GPU utilization、model-service throughput 和 writeback time。

参考：

- Ray Data overview: https://docs.ray.io/en/latest/data/data.html
- Ray Data `map_batches`: https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html
- Ray Core accelerator resources: https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html
- Ray Core placement groups: https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html
- Ray Core scheduling: https://docs.ray.io/en/latest/ray-core/scheduling/index.html
- Ray Serve dynamic batching: https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html
- Ray Serve LLM request routing: https://docs.ray.io/en/latest/serve/llm/architecture/routing-policies.html
- Ray Serve autoscaling: https://docs.ray.io/en/latest/serve/autoscaling-guide.html

## 4. Arrow 和 Lance 为什么相关

Arrow/RecordBatch 是 Daft/Lance/Ray 数据链路中常见的列式中间表示。Arrow Flight 相关论文说明，结构化数据在框架之间移动时，序列化/反序列化可能造成显著开销，而 Arrow/Flight 目标就是高性能列式数据传输。

Lance 论文则将 Lance 定位为面向 AI workload 的 columnar storage，重点是 columnar 数据在随机访问、scan 和 NVMe 场景下的效率。

这说明 Lance/Arrow 不是装饰性背景，而是和 AI 数据处理、向量/多模态数据、RecordBatch 传输直接相关。

参考：

- Arrow Flight benchmark paper: https://arxiv.org/abs/2204.03032
- Lance paper: https://arxiv.org/abs/2504.15247

## 4.5 PostgreSQL AI 链路的几类工程路线

用户近期追问的核心不是单个术语，而是：

> 数据库保存数据、外部 Python/Ray worker 计算 embedding、再写回数据库，这是不是只有本项目这么做？未来课题能否主要研究外部执行系统调优？

当前可区分四类工程路线：

| 路线 | 计算在哪里做 | 外部依据 | 对本项目的含义 |
|---|---|---|---|
| 数据库内扩展 / 近数据库扩展 | PostgreSQL 扩展或数据库附近运行时 | PostgresML README 展示 `pgml.transform()`、`pgml.predict()` 等 SQL 调用 ML/AI 能力，并定位为 “Postgres with GPUs for ML/AI apps” | 说明存在“把 AI 能力带进 PostgreSQL”的路线，但它更偏数据库内/近数据库集成，不等同于本项目当前 Ray 外部链路 |
| 外部 worker / vectorizer | 数据库外部的无状态 worker 或服务 | pgai README 描述 application + PostgreSQL + stateless vectorizer workers：worker 读配置和队列，生成 embeddings/chunked text，再写回数据库；同时支持 batch processing、失败和限流处理 | 与本项目“外部执行链路 + 写回”最接近，可作为架构合理性的官方/工程参考 |
| 向量存储扩展 | PostgreSQL 负责存向量和检索，不一定负责算 embedding | pgvector README 说明它是 Postgres 的 open-source vector similarity search，支持存 vectors、近邻检索、距离函数、upsert vectors | 支撑 PostgreSQL + pgvector 作为写回和查询端，但不证明 embedding 计算应在数据库内完成 |
| 应用层 ETL / 服务 | 应用、Python worker、Ray/Spark/Daft、模型服务等外部系统 | Psycopg 官方文档说明它是 Python 的 PostgreSQL database adapter；Python 应用可通过它读写 PostgreSQL | 说明本项目用 Python 主控程序连接 PostgreSQL、调外部 Ray、再写回，是工程上合理的原型形态 |

需要保持的证据边界：

- 本地 PG18.4 脚本采用“主控 Python 进程 `ray.get` 结果后统一写回”是为了清晰测量 `fanin_s` 和 `writeback_s`，不代表所有工程实践都必须集中 fan-in 后再写回。
- pgai 这类外部 vectorizer worker 可以由多个 worker 各自处理队列并写回数据库；这种设计会减少单一主控进程 fan-in，但会引入数据库连接并发、写回批量、事务冲突、失败重试和队列一致性问题。
- PostgresML 说明数据库内/近数据库 AI 是可行路线，但不等于本项目应该转向数据库内核或模型 kernel；它更多是对照路线。
- pgvector 支撑“向量写回和检索”这一环节，不支撑“embedding 计算在哪里最优”的结论。
- pgai README 标注 2026 年 2 月起不再维护或支持，因此只能作为架构形态参考，不能作为后续核心依赖前提。

对课题规划的影响：

1. 外部执行系统调优可以作为主线之一，但必须表述为“数据库 AI 算子的 GPU-backed 外部执行链路”，不能写成脱离数据库的 Ray 调优。
2. 后续实验需要比较至少三种写回/汇聚形态：
   - 主控进程 fan-in 后批量写回；
   - 多 worker 各自写回；
   - 队列/任务表驱动的 vectorizer worker 写回。
3. 需要把 `psycopg`/数据库连接、write batch rows、连接数、事务大小、冲突处理纳入 writeback 维度，而不是只看 Ray 侧 fan-in。
4. 与导师/企业沟通时应确认达梦目标平台更接近哪种路线：数据库内扩展、外部 worker、应用层服务，还是混合架构。

参考：

- Psycopg 3 docs: https://www.psycopg.org/psycopg3/docs/
- pgvector README: https://github.com/pgvector/pgvector
- PostgresML README: https://github.com/postgresml/postgresml
- pgai README: https://github.com/timescale/pgai

## 4.6 Snowflake / OceanBase 对本课题的参考价值

用户提出应参考 OceanBase、Snowflake 这类工业系统。当前证据可以分成强弱两档：

### 4.6.1 Snowflake：强 AI SQL 算子工业背景

Snowflake Cortex AI Functions 是当前最强的工业界参考之一。官方文档显示，Snowflake 已经把多类 AI 能力做成 SQL/Python 可调用的 AI Functions，包括：

- `AI_CLASSIFY`：分类文本或图像；
- `AI_FILTER`：在 `SELECT`、`WHERE` 或 `JOIN ... ON` 中返回布尔结果；
- `AI_EMBED`：为文本或图像生成 embedding vector；
- `AI_EXTRACT`、`AI_SENTIMENT`、`AI_AGG`、`AI_SIMILARITY` 等。

官方文档还明确给出性能提示：Cortex AI Functions 面向 throughput 优化，适合处理大 SQL 表中的大量文本，batch processing 通常更适合 AI Functions；低延迟交互场景则建议使用 REST API。

这直接支撑本项目的场景合理性：

> 数据库/SQL 层触发 AI 算子，处理大量表格行中的文本或文档，并需要关注 batch、throughput、成本和执行链路。

此外，2025 年 Snowflake AISQL 论文把 Cortex AISQL 描述为 production SQL engine for unstructured data，并指出语义/AI 操作比传统 SQL 操作更昂贵、延迟和吞吐特征不同、编译期成本和选择率未知，传统查询引擎并非为这类 semantic operations 设计。论文进一步提出 AI-aware query optimization、adaptive model cascades、semantic join rewriting 等方法，并报告生产部署中的收益。

这说明我们的方向不能只停留在“Ray object/fan-in 小优化”，更应该向下面问题靠拢：

- AI 算子成本感知的任务划分；
- batch / concurrency / worker 数与模型服务吞吐匹配；
- AI predicate / filter / classify 的选择率感知执行；
- semantic join / AI join 这类更高级 AI SQL 算子的执行策略；
- 模型服务 routing、backpressure、writeback 的跨层调度。

需要保持的边界：

- Snowflake 是云数仓内建 AISQL，不等于它使用 Ray/Daft/Lance；
- Snowflake 证明“AI SQL 算子和 AI-aware 优化是工业真实问题”，不证明本项目当前 Ray 原型已经解决了该问题；
- 后续可把 Snowflake 作为 workload 与问题定义参考，而不是照搬其闭源实现。
- 更稳的实验设计是复刻其用户可见算子语义，例如 `AI_EMBED`、`AI_FILTER`、`AI_CLASSIFY`、`AI_COMPLETE`，再用本项目可控的外部 worker / Ray / GPU-backed model service / writeback 链路实现端到端测试。

参考：

- Snowflake Cortex AI Functions docs: https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql
- Cortex AISQL paper: https://arxiv.org/abs/2511.07663

### 4.6.2 OceanBase：数据库侧工业背景，但 AI 算子证据待补

OceanBase 对本项目也有参考价值，但目前证据性质不同。当前可稳妥引用的是 OceanBase 作为工业级分布式数据库/实时分析处理系统的背景，而不是 Snowflake AISQL 这种直接 AI SQL 函数背景。

已检索到的 OceanBase 相关论文包括：

- OceanBase Mercury：面向近实时分析处理的分布式系统，强调分布式、多租户、弹性伸缩、列式存储格式、物化视图增量刷新、polymorphic vectorization engine；
- OceanBase Bacchus：云原生共享存储架构；
- OceanBase tree-structured 2PC：分布式事务一致性和扩展性。

这些资料能支撑：

- 数据库侧确实存在大规模分布式执行、列式/向量化执行、弹性伸缩和多租户等工业问题；
- 如果达梦或目标企业平台是 OceanBase/达梦这类分布式数据库，外部 AI 执行链路必须与数据库侧任务、事务、写回、资源隔离和吞吐控制配合；
- 数据库侧的列式执行、向量化执行和实时分析经验，可以作为我们组织实验指标和系统边界的参考。

但截至本次检索，尚未找到同等级官方资料证明 OceanBase 已经公开提供类似 Snowflake Cortex AISQL 的 `AI_EMBED` / `AI_FILTER` / vectorizer worker 流程。因此不能写成：

> OceanBase 已经有类似 Snowflake 的数据库 AI 算子，所以我们直接基于 OceanBase AI 架构继续做。

更严谨的表述是：

> Snowflake 提供了数据库 AI SQL 算子的强工业参照；OceanBase 提供了分布式数据库、列式/向量化执行和工业级写回/事务/资源管理的数据库侧参照。达梦内部平台是否存在类似 AI 算子或外部 worker，需要进一步确认。

对课题规划的影响：

1. 对外动机可以优先引用 Snowflake AISQL / Cortex AI Functions 说明“数据库 AI 算子”是工业真实问题。
2. OceanBase/达梦类系统更适合作为“数据库侧承载平台”的类比：分布式、列式/向量化、事务写回、资源隔离，而不是当前的 AI 函数直接依据。
3. 后续必须向企业侧确认达梦 AI 算子形态：SQL 函数、UDF、表函数、外部 worker、批处理任务，还是模型服务调用。
4. 如果确认达梦更接近 OceanBase 式分布式数据库平台，而非 Snowflake 式内建 AI SQL，需要把贡献重点落到“外部执行链路如何与数据库侧批处理/写回/资源控制衔接”。

参考：

- OceanBase Mercury paper: https://arxiv.org/abs/2602.07584
- OceanBase Bacchus paper: https://arxiv.org/abs/2602.23571
- OceanBase tree-structured 2PC paper: https://arxiv.org/abs/2603.00866

## 5. 当前实验和文献如何对应

| 观察 | 本地实验 | 外部证据 | 结论 |
|---|---|---|---|
| small task 稳定开销不高 | `0.183 ms` 量级 | Ray 文档说过细 task 是 anti-pattern | 不能直接把 runtime/scheduler 作为第一主线 |
| 小 object 有毫秒级 round-trip | 约 `1.750 ms` | Ray object store / ObjectRef 文档 | object passing 有优化空间 |
| Arrow IPC 本身不慢 | 12MB serialize 约 `1 ms` | Arrow/Flight 论文强调列式传输 | 不建议单独做 Arrow serialization 优化 |
| 本地 Python shuffle coalescing 未明显收益 | fine/coalesced 约 `0.94x` | Daft 文档说真实 Ray shuffle 受 object slot 影响 | 本地模拟不足，需真实 Ray/Daft shuffle |
| Ray many-object fan-in 变慢 | 1 object 到 256 objects 放大 `2.59x` | Daft 文档指出 `M × N` object slots 和 metadata 成本 | 支持 object coalescing/fan-in 作为机制入口，但不替代 GPU-backed 主动机 |
| GPU-backed E2E profile | 待补 | Snowflake AISQL / Ray Serve / vLLM / pgai vectorizer 等资料共同说明数据库 AI 算子、模型服务和外部 worker 是现实形态 | 这是后续最强主动机实验，用来证明 AI 算子外部服务链路是否是生产式场景中的主要损耗 |
| 三类 AI 算子 baseline | 待补 | Snowflake AI Functions 覆盖 embedding、filter/classify、completion/extract 等算子；pgai/vectorizer 体现 PostgreSQL embedding/RAG 外部 worker 形态 | 后续 baseline 不应只有 `AI_EMBED`，还要保留 `AI_FILTER/AI_CLASSIFY` 和 `AI_COMPLETE` |

## 6. 这个方向对你有没有帮助

有帮助，但前提是题目要收窄。

### 对 AI infra 的帮助

该方向会让你实际接触：

- distributed execution；
- task graph；
- object store；
- data movement；
- shuffle；
- partitioning；
- batch sizing；
- task/actor pool；
- GPU-backed model-service resource scheduling；
- model-service batching / routing / backpressure；
- Arrow/RecordBatch；
- AI data preprocessing pipeline。

这些都是 AI infra 中比“传统数据库 GPU 查询算子”更通用的能力。

### 对达梦场景的帮助

可以把达梦场景包装为：

> 数据库驱动 AI workload 或企业 AI 数据处理，需要把数据库数据组织为可批处理、可并行、可写回的 AI 数据执行过程；该过程在 batch、partition、join/groupby/repartition、embedding preprocessing、模型服务调用和结果持久化中会产生可观测损耗。

这比直接说“我要做 Daft/Ray/Lance”更容易被数据库导师和企业接受。

### 对硕士论文的帮助

论文闭环可以是：

1. 场景：数据库驱动 AI workload / 企业 AI 数据处理；
2. 系统机制：Daft/Arrow 数据组织、Ray task/actor 执行、GPU-backed 模型服务和 Lance / pgvector / PostgreSQL sink；
3. 问题：生产式 GPU-backed 数据库驱动 AI workload 链路中，固定 partition / batch / actor / routing / writeback 策略在不同 workload 下失效；
4. 方法：特征感知任务划分、并行度控制、object coalescing、模型服务状态感知路由与 backpressure；
5. 实现：Daft 策略层或独立 Ray/Arrow prototype；
6. 实验：GPU-backed E2E 主动机画像 + microbenchmark + Ray actor/service prototype + Daft end-to-end workload；
7. 对比：baseline Ray/Daft、不同 partition/shuffle 策略、不同 actor pool / routing / backpressure 策略。

## 7. 当前还不能声称什么

为了严谨，下面这些现在还不能写成结论：

1. 不能说“Ray 很慢”；
   - 当前 small task 稳定开销不高。

2. 不能说“Arrow serialization 是瓶颈”；
   - 当前 Arrow IPC 表现较好。

3. 不能说“coalescing 一定更快”；
   - 本地 shuffle 模拟没有证明；Ray many-object fan-in 支持 object 数量问题，但还不是完整 shuffle。

4. 不能说“Daft/Ray/Lance 一定适合达梦产品化”；
   - 还需要确认达梦内部是否真的会采用这些系统机制；当前只能把它们作为可控验证平台和候选实现机制。

5. 不能说“要改造整个 Ray”；
   - 当前证据只支持 object/fan-in/shuffle 层面的策略优化，不支持完整 runtime rewrite。

6. 不能说“跨层调度一定是最终贡献”；
   - 当前只确认了 Ray Data/Core/Serve 有相关接口；还需要在同一条 GPU-backed E2E 链路上继续验证 actor pool、模型服务队列、资源配比、backpressure 和 writeback 协同。

## 8. 下一步严谨验证计划

### 8.1 GPU-backed E2E 主动机画像

目标：

> 用生产式端到端链路证明外部执行链路是否值得优化。

链路：

```text
PostgreSQL / pgvector documents
  -> external worker
  -> Arrow RecordBatch
  -> Ray task / actor
  -> GPU-backed embedding service / Ray Serve / vLLM endpoint
  -> fan-in / writeback
  -> document_embeddings / pgvector
```

第一组实验可先用 `AI_EMBED(text)`，但后续必须保留三类 baseline：

| 场景 | 算子表面 | 目的 |
|---|---|---|
| Embedding / RAG | `AI_EMBED(text) -> vector` | 数据库落地、pgvector 写回、vector ingestion |
| AI predicate / classification | `AI_FILTER` / `AI_CLASSIFY` | selectivity、model call 数、cascade、AI predicate 执行 |
| Offline LLM / generation | `AI_COMPLETE(prompt) -> text/json` | token-aware batching、prefix/routing、GPU queue backlog |

指标：

- DB fetch；
- Arrow build；
- submit / `ray.put`；
- queue wait / in-flight；
- model service time；
- GPU utilization 或无法采集原因；
- fan-in；
- writeback；
- rows/s 与阶段占比。

判定：

- 如果外部链路、队列、fan-in 或 writeback 占比显著，则课题主动机成立；
- 如果 GPU/model service 完全淹没所有外部链路损耗，则需要把主线收窄为模型服务 batch/backpressure 或重新选择 workload；
- 如果写回成为绝对主瓶颈，则优先转向 writeback batching / staging table / 多 worker 写回。

### 8.2 Arrow RecordBatch fan-in

目标：

> 将 bytes 替换成 Arrow RecordBatch，验证 many-object fan-in 是否仍然放大。

变量：

- 总数据量：16MB、64MB；
- object 数量：1、16、64、256；
- object 类型：bytes、numpy、Arrow RecordBatch。

判定：

- 如果 Arrow RecordBatch 也随 object 数量放大，则 Daft/Lance 中间数据传输优化成立；
- 如果只有 bytes 放大，需重新检查数据生成和序列化路径。

### 8.3 Ray N-to-P shuffle prototype

目标：

> 构造更接近 Daft shuffle 的 `N upstream -> P downstream` task graph。

对比：

- map_reduce：每个 `(input, output)` 一个 object；
- coalesced：先合并 input-side object，再给 reducer；
- partition-aware：控制 `N` 和 `P`，避免 object slot 爆炸。

判定：

- 如果 `M × N` object slots 增加时延迟、metadata、内存明显上升，则 shuffle/object coalescing 方向强成立。

### 8.4 Daft local vs Ray end-to-end

目标：

> 确认 microbenchmark 中的开销是否会在 Daft workload 中出现。

workload：

- read -> filter -> count；
- read -> projection -> collect；
- read -> groupby -> aggregate；
- read -> join -> count；
- repartition -> groupby。

变量：

- partition 数量；
- batch size；
- shuffle algorithm；
- object 类型和大小；
- 数据源 Parquet / Lance。

### 8.5 数据库 AI 算子动机验证

必须问清楚：

- 达梦的“数据库内置 AI 算子”具体有哪些；
- 是 SQL UDF、表函数、外部执行器，还是批处理服务；
- 数据是否会以 Arrow / Parquet / Lance / IPC 格式传出；
- 是否有 join/groupby/repartition/embedding preprocessing；
- 为什么需要 Ray，而不是数据库内部线程池或普通 Python 服务。

## 9. 参考资料清单

| 资料 | 作用 |
|---|---|
| Ray paper: https://arxiv.org/abs/1712.05889 | Ray 作为 AI infra distributed execution 框架的论文依据 |
| Ray objects docs: https://docs.ray.io/en/latest/ray-core/objects.html | ObjectRef、object store、remote object 机制 |
| Ray too-fine-grained task anti-pattern: https://docs.ray.io/en/latest/ray-core/patterns/too-fine-grained-tasks.html | task batching / 粒度控制依据 |
| Ray large argument anti-pattern: https://docs.ray.io/en/latest/ray-core/patterns/pass-large-arg-by-value.html | object store、重复大对象传输依据 |
| Ray return ray.put anti-pattern: https://docs.ray.io/en/latest/ray-core/patterns/return-ray-put.html | ObjectRef / metadata / reference counting 成本依据 |
| Daft running on Ray: https://docs.daft.ai/en/stable/distributed/ray/ | Daft 可使用 Ray runner 的依据 |
| Daft partitioning: https://docs.daft.ai/en/stable/optimization/partitioning/ | partition/batch 控制依据 |
| Daft shuffle algorithms: https://docs.daft.ai/en/stable/optimization/shuffle/ | object slot、pre-shuffle merge、flight shuffle 的直接依据 |
| Daft join strategies: https://docs.daft.ai/en/stable/optimization/join-strategies/ | hash join、broadcast join、distributed join 与 shuffle 关系 |
| Spark SQL tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html | partition、shuffle coalescing、join strategy 是成熟系统问题 |
| Arrow Flight benchmark: https://arxiv.org/abs/2204.03032 | Arrow/Flight 高性能列式传输依据 |
| Lance paper: https://arxiv.org/abs/2504.15247 | Lance 面向 AI/columnar workload 的存储依据 |

## 10. 2026-07-15 数据库 AI 算子文献深度调研

调研日期：2026-07-15
触发：导师建议补充 Snowflake AI 算子 SIGMOD 论文，以及数据库 AI 算子相关文献
方法：多源并行检索（WebSearch × 8 轮，覆盖 Snowflake/BigQuery/Oracle、SIGMOD/VLDB/ICDE/CIDR/OSDI/SOSP/NeurIPS）

### 10.1 核心发现

**"数据库 AI 算子"已经是一个有 CCF-A 论文支撑的工业+学术方向。** 但现有文献分布在三个相对独立的研究岛之间，缺少端到端的协同视角。

```
岛 1: 数据库内 AI                  岛 2: GPU 推理服务              岛 3: AI 数据存储
Cortex AISQL (SIGMOD '26)       vLLM (SOSP '23 Best)           Lance (arXiv '25)
NeurDB (CIDR '25)               Orca (OSDI '22)                pgvector
GaussML (ICDE '24)              Ray Data (arXiv '25)           Parquet/Arrow
LEADS/INDICES (VLDB '24)        SGLang (NeurIPS '24)          
Smart (VLDB J '25)              DistServe (OSDI '24)
Galois (SIGMOD '25)             DeepSeek-V3
InferDB (VLDB '24)
SmartLite (VLDB '24)
        │                              │                              │
        └──────────────────────────────┼──────────────────────────────┘
                                       │
                         本课题：数据库触发 → 数据组织 →
                         分布式执行 → GPU 推理 → fan-in → 写回
                         （三个岛之间的全链路协同优化）
```

**研究空白**：没有现有工作同时覆盖"数据库侧的数据出口与写回入口"、"分布式执行与调度"、"GPU 推理服务的动态状态"和"AI 数据存储格式"——更没有人研究这些环节之间的协同关系。

### 10.2 李国良老师（清华大学）团队相关研究

李国良（ACM Fellow, IEEE Fellow）团队在 Data+AI 方向有大量 CCF-A 论文：

| 论文 | 出处 | 与本课题关系 |
|---|---|---|
| Guo, Li et al. **Smart: In-database query optimization on SQL with ML predicates.** | VLDB Journal 2025 | ML 谓词在 SQL 中的推理重写和成本最优执行——与"AI operator 执行 plan"直接对应。PostgreSQL 实现。最高 1000x 提升。 |
| Li et al. **GaussML: An End-to-End In-database ML System.** | ICDE 2024 | 将 20+ ML 算子集成进 openGauss 查询引擎。比 MADlib 快 2-6x。代表"数据库内 ML"路线。 |
| Pan, Li. **Database Perspective on LLM Inference Systems.** | VLDB 2025 Tutorial | 从 DB 视角审查 LLM 推理系统全栈。可作为"推理侧已被深入研究"的权威引用。 |
| Li, Zhou, Zhao. **LLM for Data Management.** | VLDB 2024 Tutorial | RAG、向量数据库、Agent、微调——Data+AI 的上位综述。 |
| Zhou, Li et al. **D-Bot: Database Diagnosis using LLMs.** | VLDB 2024 | LLM+数据库系统设计范式。展示"可控外部调用"的架构思路。 |
| Li et al. **openGauss: An Autonomous Database System.** | VLDB 2021 | 学习型优化器、基数估计、连接顺序——DB4AI 路线的系统级工作。 |

**关键区分**：李国良组的 GaussML 和 Smart 走的是"把 ML 能力嵌入数据库内核"（DB4AI），本课题走的是"数据库触发 → 外部执行链路 → 写回"（AI4DB 的外部执行变体）。两者形成对照而非重复——开题报告 §2.2 和 §2.4 中需明确这一边界。

### 10.3 关键文献分档（按开题模板：40 篇总，15 精读）

#### ★ 建议精读（15 篇，均为 CCF-A 或顶会）

| # | 论文 | 出处 | 精读理由 |
|---|---|---|---|
| 1 | Aggarwal et al. **Cortex AISQL: A Production SQL Engine for Unstructured Data.** | SIGMOD Companion 2026 | 最直接产业对标。六大 AI SQL 算子、AI-aware 优化。 |
| 2 | Guo, Li, Hu, Wang. **In-database query optimization on SQL with ML predicates.** | VLDB Journal 2025 | 李国良组。SQL+ML 谓词优化的学术前沿。PostgreSQL 实现。 |
| 3 | Zhao, Cai, Ooi et al. **NeurDB: An AI-powered Autonomous Database.** | CIDR 2025 | AI×DB 深度融合的系统级蓝图。 |
| 4 | Zeng, Xing, Cai, Chen, Ooi et al. **LEADS: In-Database Dynamic Model Slicing.** | VLDB 2024 | SQL-aware MoE。PostgreSQL 实现。 |
| 5 | Li et al. **GaussML: An End-to-End In-database ML System.** | ICDE 2024 | 华为+清华。数据库内 ML 算子的最强工程实现。 |
| 6 | Satriani, Papotti et al. **Galois: SQL Query Execution over LLMs.** | SIGMOD 2025 | LLM 作为存储层。挑战"算子下推最优"的传统认知。 |
| 7 | Kwon, Li, Stoica et al. **vLLM: PagedAttention for LLM Serving.** | SOSP 2023 Best Paper | GPU 推理服务的核心机制（PagedAttention + continuous batching）。 |
| 8 | Yu, Jeong et al. **Orca: Distributed Serving for Generative Models.** | OSDI 2022 | Iteration-level scheduling 开山之作。 |
| 9 | Luan, Stoica, Wang et al. **The Streaming Batch Model for Heterogeneous Execution.** | arXiv:2501.12407 | Ray Data 异构批处理。Arrow→Ray→GPU 数据路径的理论基础。 |
| 10 | Li, Zhou, Zhao. **LLM for Data Management.** | VLDB 2024 Tutorial | Data+AI 全貌。李国良组。 |
| 11 | Pan, Li. **Database Perspective on LLM Inference Systems.** | VLDB 2025 Tutorial | 推理系统 DB 视角。李国良组。 |
| 12 | Salazar-Díaz et al. **InferDB: In-Database ML Inference Using Indexes.** | VLDB 2024 | 轻量数据库内推理。延迟降 2-3 数量级。 |
| 13 | Lin, Wu, Chen, Li et al. **SmartLite: DBMS-Based DNN Inference Serving.** | VLDB 2024 | DBMS 原生 NN 算子。边缘场景。 |
| 14 | Pace, Jones, She et al. **Lance: Efficient Random Access in Columnar Storage.** | arXiv:2504.15247 | 面向 AI/ML 的列式存储格式。自适应编码。 |
| 15 | Moritz, Nishihara, Stoica et al. **Ray: A Distributed Framework for Emerging AI Applications.** | OSDI 2018 | Ray 原始论文。分布式执行框架基础。 |

#### ○ 补充引用（25 篇，构建 40 篇参考文献）

**数据库 AI 算子（CCF-A）**

| # | 论文 | 出处 |
|---|---|---|
| 16 | Wang, Xue et al. **AnDB: AI-Native Database for Universal Semantic Analysis.** | SIGMOD 2025 Demo |
| 17 | Zhu, Wu, Ding, Zhou. **Learned Query Optimizer: What is New and What is Next.** | SIGMOD 2024 |
| 18 | Kim, Ailamaki. **Trustworthy and Efficient LLMs Meet Databases.** | VLDB 2024 Tutorial |
| 19 | Zhou, Li, Sun et al. **D-Bot: Database Diagnosis using LLMs.** | VLDB 2024 |
| 20 | Qiao, Fan, Han et al. **Learning Database Optimization Techniques: Survey.** | Frontiers of CS, 2025 |
| 21 | Heinrich, Luthra et al. **How Good are Learned Cost Models, Really?** | SIGMOD 2025 |

**推理服务系统（CCF-A）**

| # | 论文 | 出处 |
|---|---|---|
| 22 | Zheng et al. **SGLang: Efficient Execution of Structured LM Programs.** | NeurIPS 2024 |
| 23 | Zhong et al. **DistServe: Disaggregating Prefill/Decode for LLM Serving.** | OSDI 2024 |
| 24 | DeepSeek-AI. **DeepSeek-V3 Technical Report.** | arXiv:2412.19437 |

**数据系统与存储**

| # | 论文 | 出处 |
|---|---|---|
| 25 | Apache Arrow. **Arrow Flight: Fast Data Transport.** | arXiv:2204.03032 |
| 26 | Daft Documentation. Distributed Execution / Partitioning / Shuffle. | docs.daft.ai |
| 27 | Spark SQL Performance Tuning Guide. | spark.apache.org |

**产业系统（非论文，需求证据）**

| # | 系统 | 关键能力 |
|---|---|---|
| 28 | Snowflake Cortex AI Docs | `AI_EMBED`, `AI_FILTER`, `AI_CLASSIFY`, `AI_COMPLETE`, `AI_JOIN` |
| 29 | BigQuery ML/AI Docs | `ML.GENERATE_TEXT`, `ML.GENERATE_EMBEDDING` |
| 30 | Oracle AI Vector Search Docs | `VECTOR_EMBEDDING` |
| 31 | Timescale pgai | PostgreSQL + vectorizer worker + embedding endpoint + 写回 |
| 32 | PostgresML | PostgreSQL 内/近数据库 ML/AI |
| 33 | pgvector | PostgreSQL 向量相似度检索 |
| 34 | vLLM Documentation | PagedAttention, continuous batching, offline inference |
| 35 | Ray Documentation | Core objects, Serve batching, Data overview |
| 36 | LanceDB Documentation | Lance format, vector search |

**本项目实验报告（自引）**

| # | 报告 | 路径 |
|---|---|---|
| 37 | GPU-Backed AI_EMBED Chain Breakdown (7/12) | `motivation/results/gpu/` |
| 38 | PGAI-Integrated GPU-Backed Key Rerun (7/14) | `motivation/results/gpu/` |
| 39 | GPU-Backed pgvector(384) Writeback Test (7/14) | `motivation/results/gpu/` |
| 40 | Fake/CPU Motivation Analysis + Workload Scenarios | `motivation/results/fake_cpu/`, `motivation/plans/` |

### 10.4 Cortex AISQL (SIGMOD 2026) 详细摘要

**六大 AI SQL 算子**：`AI_COMPLETE`, `AI_FILTER`, `AI_JOIN`, `AI_CLASSIFY`, `AI_AGG`, `AI_SUMMARIZE_AGG`

**三项核心优化**：
1. **AI-Aware Query Optimization** — 将 LLM 推理成本作为一阶优化目标。必要时将 AI 谓词上拉到 Join 后。一个案例中 LLM 调用从 11 万次降到 330 次（300x）。2-8x 加速。
2. **Adaptive Model Cascades** — 小模型（Llama 3.1-8B）处理大部分行，仅不确定行升级到大模型（Llama 3.3-70B）。2-6x 加速，90-95% 质量。
3. **Semantic Join Rewriting** — O(N×M) 语义 Join → 线性多标签分类。15-70x 加速。

**生产数据（2025年7-9月）**：AI 算子主导查询成本；~40% 查询多表（消耗 >58% 时间）；85% 为 SELECT。

### 10.5 对开题报告 §2 的更新计划

需更新：
1. §2.1 — 补充 Cortex AISQL 论文细节 + BigQuery/Oracle 最新能力
2. §2.2 — 补充 GaussML 作为"数据库内 ML"对照路线
3. §2.3 — 补充 vLLM、Ray Data Streaming Batch、Orca、Lance 论文
4. §2.4 — 用新文献精准化三个研究岛之间的空白，引用李国良组 VLDB Tutorial
5. 参考文献 — 从 23 条扩展到 40 条，标注 15 精读
