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

> 面向数据库内置 AI 算子的分布式数据处理执行链路优化研究。

技术切入点是：

> 基于 Daft/Ray/Lance 的 Object Transfer、fan-in 与 Shuffle 中间数据传输优化。

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

这些资料说明，任务划分、actor 池大小、`batch_size × concurrency`、CPU/GPU 资源配比、placement、模型服务 routing / batching / backpressure 都有可实验的系统接口。因此候选方向可以从“数据粒度控制”扩展为：

> 面向数据库 AI 算子的特征感知并行执行与跨层调度。

但这里必须保持证据边界：官方文档只能证明接口和机制存在，不能证明它们在当前数据库 AI 算子链路中就是瓶颈。是否值得作为论文核心贡献，需要后续实验记录 queue wait、actor idle time、token backlog、object store pressure、model-service throughput 和 writeback time。

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

## 5. 当前实验和文献如何对应

| 观察 | 本地实验 | 外部证据 | 结论 |
|---|---|---|---|
| small task 稳定开销不高 | `0.183 ms` 量级 | Ray 文档说过细 task 是 anti-pattern | 不能直接把 runtime/scheduler 作为第一主线 |
| 小 object 有毫秒级 round-trip | 约 `1.750 ms` | Ray object store / ObjectRef 文档 | object passing 有优化空间 |
| Arrow IPC 本身不慢 | 12MB serialize 约 `1 ms` | Arrow/Flight 论文强调列式传输 | 不建议单独做 Arrow serialization 优化 |
| 本地 Python shuffle coalescing 未明显收益 | fine/coalesced 约 `0.94x` | Daft 文档说真实 Ray shuffle 受 object slot 影响 | 本地模拟不足，需真实 Ray/Daft shuffle |
| Ray many-object fan-in 变慢 | 1 object 到 256 objects 放大 `2.59x` | Daft 文档指出 `M × N` object slots 和 metadata 成本 | 当前最强信号，支持 object coalescing/fan-in 优化 |

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
- CPU/GPU resource scheduling；
- model-service batching / routing / backpressure；
- Arrow/RecordBatch；
- AI data preprocessing pipeline。

这些都是 AI infra 中比“传统数据库 GPU 查询算子”更通用的能力。

### 对达梦场景的帮助

可以把达梦场景包装为：

> 数据库内置 AI 算子或企业 AI 数据处理，需要把数据库数据送入外部分布式 AI 数据处理链路；该链路在 join/groupby/repartition/embedding preprocessing 中会产生中间数据传输和 shuffle 开销。

这比直接说“我要做 Daft/Ray/Lance”更容易被数据库导师和企业接受。

### 对硕士论文的帮助

论文闭环可以是：

1. 场景：数据库内置 AI 算子 / 企业 AI 数据处理；
2. 系统：Daft + Ray + Lance；
3. 问题：AI 算子特征导致固定 partition / batch / actor / routing 策略在不同 workload 下失效；
4. 方法：特征感知任务划分、并行度控制、object coalescing、模型服务状态感知路由与 backpressure；
5. 实现：Daft 策略层或独立 Ray/Arrow prototype；
6. 实验：microbenchmark + Ray actor/service prototype + Daft end-to-end workload；
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
   - 还需要确认达梦内部是否真的会用 Ray/Daft/Lance 做数据库内置 AI 算子。

5. 不能说“要改造整个 Ray”；
   - 当前证据只支持 object/fan-in/shuffle 层面的策略优化，不支持完整 runtime rewrite。

6. 不能说“跨层调度一定是最终贡献”；
   - 当前只确认了 Ray Data/Core/Serve 有相关接口；还没有真实 actor pool、模型服务队列、资源配比或 backpressure 实验。

## 8. 下一步严谨验证计划

### 8.1 Arrow RecordBatch fan-in

目标：

> 将 bytes 替换成 Arrow RecordBatch，验证 many-object fan-in 是否仍然放大。

变量：

- 总数据量：16MB、64MB；
- object 数量：1、16、64、256；
- object 类型：bytes、numpy、Arrow RecordBatch。

判定：

- 如果 Arrow RecordBatch 也随 object 数量放大，则 Daft/Lance 中间数据传输优化成立；
- 如果只有 bytes 放大，需重新检查数据生成和序列化路径。

### 8.2 Ray N-to-P shuffle prototype

目标：

> 构造更接近 Daft shuffle 的 `N upstream -> P downstream` task graph。

对比：

- map_reduce：每个 `(input, output)` 一个 object；
- coalesced：先合并 input-side object，再给 reducer；
- partition-aware：控制 `N` 和 `P`，避免 object slot 爆炸。

判定：

- 如果 `M × N` object slots 增加时延迟、metadata、内存明显上升，则 shuffle/object coalescing 方向强成立。

### 8.3 Daft local vs Ray end-to-end

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

### 8.4 数据库 AI 算子动机验证

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
