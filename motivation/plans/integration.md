# AI 算子集成与测试方法

生成日期：2026-07-10（⚠ 最后更新于早期阶段，内容可能过时。当前方向以 `PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md` 为准）

## 1. 当前约束

设备状态：RTX 5070 (12GB VRAM) 已就绪，可运行 1-3B 级 LLM。

根据沟通信息：

- 设备到位时间未确定；
- 本周可能会出采购方案供总部领导讨论；
- 设备没到的情况下，可以用低端设备、小规模数据、小模型做实验；
- AI 算子可以使用已经实现 AI 算子的开源数据库；
- 建议后续把算子集成到 PostgreSQL 18.3 版本；
- PostgreSQL 18.3 是公司内部统一采用的技术验证平台。

待确认：

- PostgreSQL 18.3 内部平台的具体安装方式；
- 能否安装扩展，如 pgvector、PL/Python、FDW、自定义 UDF；
- 能否启动外部 Python/Ray/Daft worker；
- AI 算子已有实现来自哪个开源数据库或扩展；
- 是否允许接真实模型、GPU、外部服务。

## 2. 总体原则

不要等设备。

当前按三阶段推进：

| 阶段 | 环境 | 目标 | 产物 |
|---|---|---|---|
| 本地 fake/CPU 预研 | Mac / 低端设备 / 小模型 | 调试脚本和计时边界，保留历史预研 | fake embedding + Ray/Arrow benchmark |
| Phase 1 | PostgreSQL 18.3 同构预演链路；必要时用普通 PostgreSQL + pgvector 替身；优先接 GPU-backed model service | 建立生产式 GPU-backed E2E 主动机画像 | PostgreSQL 表/SQL 触发 -> 外部 worker -> GPU-backed AI 算子 -> 写回 |
| Phase 2 | PostgreSQL 18.3 内部验证平台 + 设备 | 验证外部执行链路瓶颈、baseline 和优化收益 | 外部 worker / Ray task / Ray actor / GPU model service / 写回形态对比 |

第一阶段用 fake embedding 只用于调试脚本、验证计时边界和保留历史预研；后续必须接 PostgreSQL 18.3 真实平台或同构预演链路做验证。普通 PostgreSQL + pgvector 只能作为设备未到位或内部平台暂不可用时的接口替身，不能替代最终平台结论。

当前计划不把“把 AI 算子搬到 GPU 或数据库内核里执行”作为主要工作量。GPU-backed model service / vLLM / Ray Serve 应作为生产式数据执行链路的现实计算端点优先进入端到端画像，用来建立“为什么数据库驱动 AI workload 的数据执行与存储过程值得优化”的主动机。主要实验时间优先投入 worker、Ray task/actor、模型服务队列、fan-in/writeback 和 backpressure 的链路调优。

实验应尽量靠近 Snowflake / pgai 这类工业系统的用户可见语义，但不复现它们的闭源内部实现。推荐做法是：

```text
AI-SQL-compatible operator surface
  -> pgai/vectorizer-like external worker path
  -> Ray task / actor scheduling
  -> GPU-backed model service
  -> PostgreSQL / pgvector writeback
```

这样能用完整端到端链路做动机测试，同时保持实现可控。

## 3. 推荐系统形态

目标不是把所有 AI 计算塞进数据库内核，而是构建：

```text
PostgreSQL 18.3 documents 表
  -> Daft DataFrame（数据引擎，文本 df["prompt"] / 图像 df["image"]）
  -> Ray 动态 Batching（token-budget / length-align / prefix-aware）
    + Ray actor 架构（异构 actor pool / queue-adaptive flush）
  -> AI_COMPLETE（文本 LLM）/ AI_EMBED/AI_CLASSIFY（图像，多模态泛化）
  -> vLLM Continuous Batching（部署平台，不修改）
  -> 写回 PostgreSQL + pgvector
```

> **2026-07-17 更新**：上图已按当前口径更新。原始版本（见下）保留作为历史参考。

原始版本（2026-07-10）：
```text
PostgreSQL 18.3 documents 表
  -> 导出/读取为 Arrow RecordBatch
  -> Daft / Ray 分布式执行
  -> AI-SQL-compatible AI_EMBED / AI_FILTER / AI_CLASSIFY / AI_COMPLETE 算子表面
  -> chunk / preprocess / operator-specific batching
  -> GPU-backed embedding service / Ray Serve / vLLM endpoint
  -> object transfer / fan-in / shuffle
  -> 写回 PostgreSQL + pgvector 或 Lance
```

这符合两个目标：

- 对达梦/数据库：有数据库 AI 算子落地入口；
- 对用户职业方向：核心工作仍是 AI infra 数据处理执行链路。

## 4. AI 算子选择

优先选择 embedding / RAG 数据准备，而不是泛泛 AI 函数。

推荐算子：

```sql
AI_EMBED(text) -> vector
AI_CHUNK(text) -> setof chunk_text
AI_FILTER(text, condition) -> bool
```

第一阶段主实验先做 `AI_EMBED(text)`，但三类场景的 baseline 都要进入计划：

| 场景 | 算子表面 | 输出 | 主要验证点 |
|---|---|---|---|
| Embedding / RAG ingestion | `AI_EMBED(text) -> vector` | pgvector / Lance / embedding table | vector 输出、batch、Ray task/actor、fan-in、writeback |
| AI predicate / classification | `AI_FILTER(text, condition) -> bool` / `AI_CLASSIFY(text, labels) -> label` | filtered/classified table | selectivity、model call 数、cascade、输出行数变化 |
| Offline LLM / generation | `AI_COMPLETE(prompt) -> text/json` | generated outputs table | token-aware batching、prefix/routing、queue backlog、GPU utilization |

第一组主动机实验可以先用 `AI_EMBED(text)`，因为它最容易和 pgvector 写回闭环；后续至少要保留 `AI_FILTER/AI_CLASSIFY` 和 `AI_COMPLETE` 的 baseline，否则方法覆盖面不足。

原因：

- embedding 是 RAG / semantic search / 推荐 / 向量检索基础能力；
- 输入输出清晰；
- 容易和 pgvector / Lance 对接；
- 可从 fake embedding 平滑升级到真实 embedding；
- 会自然产生 batch、partition、object、fan-in、写回等系统问题。

## 5. 集成方案

### 5.1 最小数据库形态

PostgreSQL 表：

```sql
CREATE TABLE documents (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INT,
  category TEXT,
  text TEXT,
  updated_at TIMESTAMP
);

CREATE TABLE document_embeddings (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INT,
  category TEXT,
  embedding VECTOR,
  updated_at TIMESTAMP
);
```

如果不能使用 `VECTOR` 类型，先用：

```sql
embedding FLOAT4[]
```

后续再替换为 pgvector。

### 5.2 外部执行 worker

推荐先用外部 worker，而不是数据库内核 UDF。

```text
worker:
  1. 从 PostgreSQL 读取 documents
  2. 转 Arrow RecordBatch
  3. 按 batch / partition 切分
  4. Ray task / actor 调用 GPU-backed embedding service
  5. fan-in / coalesce
  6. 写回 PostgreSQL 或 Lance
```

更一般的端到端阶段划分：

```text
DB trigger/fetch
  -> AI operator external service path
  -> GPU model service
  -> result consolidation/writeback
```

后续报告中建议把 `AI operator external service path` 翻译为“AI 算子外部服务链路”。它覆盖 Arrow/RecordBatch、batch/partition、Ray task/actor、ObjectRef/object store、queue/in-flight、fan-in 等环节，比“外部链路”更具体。

这样能测到本课题关心的瓶颈：

- batch size；
- RecordBatch size；
- Ray object 数量；
- fan-in；
- shuffle；
- 写回小 batch / 大 batch 差异。

外部 worker 形态需要至少比较两类：

| 形态 | 数据流 | 回答的问题 |
|---|---|---|
| 主控进程 fan-in 后批量写回 | Ray worker 返回结果 -> 主控进程 `ray.get` 合并 -> 主控进程写回 PostgreSQL | fan-in 和批量 writeback 的清晰边界，适合做系统画像 |
| 多 worker 各自写回 | worker 从任务表/队列取数据 -> 各自算 embedding -> 各自写回 PostgreSQL | 更接近 pgai vectorizer worker 等工业形态，测试连接并发、事务、失败重试和写回竞争 |

如果时间有限，优先保证这两类数据执行 baseline 和一条 GPU-backed E2E 主动机链路，而不是扩展大量 GPU/数据库内执行 baseline。

### 5.3 PostgreSQL 18.3 集成与画像

PostgreSQL 18.3 是后续真实实验平台，不只是远期可选项。在内部统一平台上，应尽量保持外部执行链路不变，只替换数据库入口和 AI 算子触发方式：

```text
PostgreSQL 18.3 table
  -> COPY / Arrow / FDW / connector
  -> Daft + Ray
  -> AI operator
  -> pgvector / Lance / table output
```

如果内部平台支持 UDF 或外部函数，可以再包装成：

```sql
SELECT ai_embed_batch('documents', 'text', 'document_embeddings');
```

但第一阶段不要把重点放在 SQL wrapper 上。核心是采集真实链路画像：

- 数据库侧读取批次、行数、字节数；
- Arrow RecordBatch 数量和平均大小；
- Ray task / actor 数量；
- `ObjectRef` 数量和 fan-in 依赖数；
- AI operator invocation 次数、平均输入 batch size；
- 模型服务 queue wait、service time、in-flight 请求数、token backlog；
- 写回批次、写回耗时和失败重试；
- 端到端 rows/s 与各阶段耗时占比。

## 6. 测试方法

### 6.1 本地预研：无设备 / 小规模验证

目标：

> 证明链路能跑、计时边界可记录；不把 CPU/fake 结果作为真实 GPU 链路的瓶颈归因。

数据：

- 10K / 100K rows；
- fake text；
- embedding dim：128 / 384 / 768；
- batch size：128 / 512 / 2048 / 8192。

对比：

| 实验 | 目的 |
|---|---|
| Python local baseline | 纯本地处理成本 |
| Arrow RecordBatch fan-in | 判断 RecordBatch object 数量影响 |
| Ray many objects | 判断 object count 放大 |
| coalesced vs non-coalesced | 判断合并 object 是否有效 |

指标：

- end-to-end time；
- rows/s；
- object_count；
- average object size；
- Ray put time；
- fan-in time；
- write time；
- memory peak。

### 6.2 Phase 1：PostgreSQL 18.3 同构预演链路 + GPU-backed 主动机画像

目标：

> 证明这是数据库驱动 AI workload 的分布式数据执行链路，而不只是离线脚本；如果 GPU 环境可用，进一步证明生产式 GPU-backed 链路中数据组织、Ray 执行、模型服务和写回是否存在可分解的主要损耗。

链路：

```text
PostgreSQL 18.3 documents
  -> external worker
  -> GPU-backed embedding service
  -> document_embeddings
  -> vector search query
```

如果暂时拿不到 PostgreSQL 18.3 内部平台，可以用普通 PostgreSQL + pgvector 预演同构接口，但实验报告必须标注为“接口替身/合理近似”，不能声称已经验证内部平台。

测试：

- SQL/UDF/表函数/任务表触发方式的开销；
- AI-SQL-compatible 算子表面：`AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE`；
- 小 batch 写回 vs 大 batch 写回；
- 直接逐行处理 vs batch processing；
- 单 worker vs Ray worker；
- Ray task vs Ray actor；
- CPU/local model service vs GPU-backed model service；该项只作为临时工程对照，不作为真实瓶颈归因；
- 无界提交 vs backpressure；
- pgvector 查询是否能正常使用 embedding 输出。

消融顺序应先粗后细，且优先在同一条 GPU-backed E2E 链路上做：

1. no-Ray Python worker vs Ray task/actor；
2. single worker vs actor pool；
3. 主控 fan-in 后写回 vs 多 worker 各自写回；
4. unbounded in-flight vs bounded backpressure；
5. batch/partition 大小；
6. token-aware / selectivity-aware 等算子特定策略。

### 6.3 Phase 2：PostgreSQL 18.3 内部平台 + 设备验证

目标：

> 用内部统一平台、真实或更接近真实的 AI 算子和更大数据验证瓶颈画像与优化是否仍有效。

必须优先包含：

- GPU embedding service、Ray Serve 或 vLLM embedding endpoint；
- 模型服务 queue wait / service time / in-flight / batch size；
- GPU utilization 或无法采集该指标的原因；
- Python / Ray / model service / writeback 的阶段占比。

可加：

- sentence-transformers；
- ONNX Runtime；
- 更大数据集；
- PostgreSQL 18.3 内部平台。

测试重点：

- 外部 worker / Ray task / Ray actor / 模型服务的端到端对比；
- batch size、worker 数、in-flight 限制与模型服务吞吐；
- 数据链路开销占比；
- object coalescing 是否仍有收益；
- Daft/Ray/Lance 与普通 Python batched worker 的对比；
- 主控 fan-in 写回 vs 多 worker 各自写回；
- CPU-only / fake baseline 只用于脚本调试、计时边界验证和历史对照，不作为 GPU-backed 主链路结论，也不作为真实瓶颈归因。
- GPU/数据库内执行迁移 baseline 只用于判断边界，不作为主要优化对象。

## 7. 优化点如何对应瓶颈

| 观察到的瓶颈 | 优化方向 |
|---|---|
| object 数量多导致 fan-in 慢 | object coalescing / batch size 调整 |
| batch 太小导致吞吐低 | partition-aware batching |
| 写回 PostgreSQL 慢 | 批量写入 / COPY / staging table |
| 读取 PostgreSQL 慢 | Parquet/Lance 中间层 / predicate pushdown |
| 模型推理慢或 GPU 利用率低 | batch inference / model service batching / in-flight 控制 / actor 与 GPU 资源配比 |
| Ray 调度占比高 | task batching / 减少 task 数 |

## 8. 当前下一步

不等设备，但下一步不应只继续做本地预研。当前要并行推进两件事：

1. 优先设计并跑通生产式 GPU-backed E2E 主动机画像：数据库触发、外部 worker、Ray/Arrow、GPU-backed AI 算子、模型服务队列、fan-in/writeback 和阶段指标采集；
2. 在同一条 GPU-backed E2E 链路上做大块消融，再逐步细分 task、object、operator invocation、fan-in 和 backpressure 成因；
3. 把 Snowflake Cortex AISQL、pgai vectorizer、pgvector、PostgresML、OceanBase/达梦类系统作为工业背景和 baseline 参照，不把完整复现这些系统作为当前任务。

已完成的 组件可行性证据：

- `ray_many_objects_benchmark.py`：固定 `16MB` 总数据量下，object 数从 `1` 到 `256` 时 fan-in 约放大 `2.59x`；
- `ray_arrow_fanout_fanin_benchmark.py`：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `3.17x`。

PostgreSQL 18.3 / PG18.4 同构 GPU-backed 主动机画像最小任务：

1. 准备 `documents` 表；
2. 确认 AI 算子触发方式：SQL UDF、表函数、任务表、外部执行器或批处理服务；
3. 实现 `AI_EMBED` 的触发入口，并优先对接 GPU-backed endpoint；
4. 外部 worker 读取数据库数据并构造 Arrow RecordBatch；
5. 接入 GPU-backed embedding service、Ray Serve 或 vLLM endpoint；如果暂不可用，记录为 CPU/local baseline；
6. 用 Python batched worker、Ray task、Ray actor 跑 bounded/unbounded backpressure 策略；
7. 比较主控进程 fan-in 后批量写回与多 worker 各自写回；
8. 写回 `document_embeddings` 或 Lance/output table；
9. 输出 CSV 和阶段画像报告，至少包含 DB fetch、Arrow build、submit/put、queue wait、model service、fan-in、writeback、GPU utilization 或缺失原因；
10. 更新 `motivation/results/` 和 `feasibility/results/` 中的结果分析。

如果 PostgreSQL 18.3 画像显示 object/fan-in、task 过细、operator invocation、模型队列或 backpressure 不是主要瓶颈，应及时调整优化方向。

## 9. 现有实验是否保留

现有 `benchmarks/` 里的实验需要保留，但要重新理解为 fake/CPU 历史预研和工具验证，而不是最终论文实验。

| 实验 | 是否保留 | 当前作用 |
|---|---|---|
| `ray_small_task_benchmark.py` | 保留 | 判断问题是否来自 Ray task 调度；当前结果说明 small task 不是最强瓶颈 |
| `ray_object_transfer_benchmark.py` | 保留 | 判断单个 object put/get/round-trip 成本，是后续 RecordBatch 传输的 baseline |
| `ray_many_objects_benchmark.py` | 必须保留 | 基础证据：固定总数据量下 object 数量增加会放大 fan-in 时间 |
| `ray_arrow_fanout_fanin_benchmark.py` | 必须保留 | 当前最贴近数据库 AI 算子链路的本地证据：Arrow RecordBatch fine/coalesced fan-in 有明显差异 |
| `arrow_recordbatch_serialization_benchmark.py` | 保留 | 判断 Arrow IPC 本身是否是瓶颈；当前结果说明单纯 Arrow 序列化不是主瓶颈 |
| `shuffle_simulation_benchmark.py` | 暂时保留 | 作为负结果/对照：本地 Python shuffle 不能代表真实 Ray/Daft shuffle，需要后续替换为 Ray/Arrow fan-out/fan-in |

删除标准：

- 如果某个实验既不能服务组件可行性瓶颈定位，也不能作为 baseline / negative result，再删除；
- 如果后续端到端 AI 算子实验已经覆盖 `shuffle_simulation_benchmark.py`，可以考虑把后者移入单独的历史结果目录，但不建议现在删除。

下一步新增实验应优先覆盖：

> 数据库触发的外部执行链路 baseline 矩阵：Python batched worker、Ray task、Ray actor、主控 fan-in 写回、多 worker 各自写回，以及必要的轻量模型服务 baseline。

这些实验会把 `ray_arrow_fanout_fanin_benchmark.py` 和 PG18.4 系统画像的结论迁移到更接近工业 vectorizer worker / AI SQL 函数背后执行链路的形态。
