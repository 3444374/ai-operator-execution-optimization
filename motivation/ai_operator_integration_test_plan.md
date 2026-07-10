# AI 算子集成与测试方法

生成日期：2026-07-10

## 1. 当前约束

目前处于“设备未到位”阶段。

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
| Phase 0 | Mac / 低端设备 / 小模型 | 隔离系统瓶颈，验证方法可跑 | fake embedding + Ray/Arrow benchmark |
| Phase 1 | PostgreSQL 18.3 同构预演链路；必要时用普通 PostgreSQL + pgvector 替身 | 搭真实数据库 AI 算子画像形态 | PostgreSQL 表/SQL 触发 -> 外部 worker -> AI 算子 -> 写回 |
| Phase 2 | PostgreSQL 18.3 内部验证平台 + 设备 | 验证内部平台可行性、真实性能瓶颈和优化收益 | 集成方案、端到端画像实验、优化对比 |

第一阶段用 fake embedding 是为了定位系统瓶颈；后续必须接 PostgreSQL 18.3 真实平台或同构预演链路做验证。普通 PostgreSQL + pgvector 只能作为设备未到位或内部平台暂不可用时的接口替身，不能替代最终平台结论。

## 3. 推荐系统形态

目标不是把所有 AI 计算塞进数据库内核，而是构建：

```text
PostgreSQL 18.3 documents 表
  -> 导出/读取为 Arrow RecordBatch
  -> Daft + Ray 分布式执行
  -> AI_EMBED / chunk / preprocess 算子
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

第一阶段只做 `AI_EMBED(text)`。

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
  4. Ray task 执行 fake 或真实 embedding
  5. fan-in / coalesce
  6. 写回 PostgreSQL 或 Lance
```

这样能测到本课题关心的瓶颈：

- batch size；
- RecordBatch size；
- Ray object 数量；
- fan-in；
- shuffle；
- 写回小 batch / 大 batch 差异。

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

### 6.1 Phase 0：无设备 / 小规模验证

目标：

> 证明链路能跑，并定位 object / batch / fan-in 是否是瓶颈。

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

### 6.2 Phase 1：PostgreSQL 18.3 同构预演链路

目标：

> 证明这是数据库 AI 算子触发的外部执行链路，而不只是离线脚本。

链路：

```text
PostgreSQL 18.3 documents
  -> external worker
  -> fake / local embedding
  -> document_embeddings
  -> vector search query
```

如果暂时拿不到 PostgreSQL 18.3 内部平台，可以用普通 PostgreSQL + pgvector 预演同构接口，但实验报告必须标注为“接口替身/合理近似”，不能声称已经验证内部平台。

测试：

- SQL/UDF/表函数/任务表触发方式的开销；
- 小 batch 写回 vs 大 batch 写回；
- 直接逐行处理 vs batch processing；
- 单 worker vs Ray worker；
- Ray task vs Ray actor；
- 无界提交 vs backpressure；
- pgvector 查询是否能正常使用 embedding 输出。

### 6.3 Phase 2：PostgreSQL 18.3 内部平台 + 设备验证

目标：

> 用内部统一平台、真实或更接近真实的 AI 算子和更大数据验证瓶颈画像与优化是否仍有效。

可加：

- sentence-transformers；
- ONNX Runtime；
- GPU embedding service；
- vLLM embedding endpoint；
- 更大数据集；
- PostgreSQL 18.3 内部平台。

测试重点：

- 模型推理吞吐；
- batch size 与 GPU 利用率；
- 数据链路开销占比；
- object coalescing 是否仍有收益；
- Daft/Ray/Lance 与普通 Python worker 的对比。

## 7. 优化点如何对应瓶颈

| 观察到的瓶颈 | 优化方向 |
|---|---|
| object 数量多导致 fan-in 慢 | object coalescing / batch size 调整 |
| batch 太小导致吞吐低 | partition-aware batching |
| 写回 PostgreSQL 慢 | 批量写入 / COPY / staging table |
| 读取 PostgreSQL 慢 | Parquet/Lance 中间层 / predicate pushdown |
| 模型推理慢 | batch inference / ONNX / GPU service |
| Ray 调度占比高 | task batching / 减少 task 数 |

## 8. 当前下一步

不等设备，但下一步不应只继续做 Phase 0。当前要并行推进两件事：

1. 继续用低端设备/小规模数据拆分 Phase 0 中的 task、object、operator invocation、fan-in 和 backpressure 成因；
2. 设计 PostgreSQL 18.3 真实画像实验，先用同构预演链路跑通数据库触发、外部 worker、AI 算子、Ray/Arrow、fan-in/writeback 和指标采集。

已完成的 Phase 0 证据：

- `ray_many_objects_benchmark.py`：固定 `16MB` 总数据量下，object 数从 `1` 到 `256` 时 fan-in 约放大 `2.59x`；
- `ray_arrow_fanout_fanin_benchmark.py`：固定 `65536` 行、`128` 维 embedding 下，fine/coalesced 平均 fan-in 比约 `3.17x`。

PostgreSQL 18.3 画像实验最小任务：

1. 准备 `documents` 表；
2. 确认 AI 算子触发方式：SQL UDF、表函数、任务表、外部执行器或批处理服务；
3. 实现 fake/local `AI_EMBED` 的 PostgreSQL 18.3 触发入口；
4. 外部 worker 读取数据库数据并构造 Arrow RecordBatch；
5. 用 Ray 跑 fine/coalesced、task/actor、bounded/unbounded backpressure 策略；
6. 写回 `document_embeddings` 或 Lance/output table；
7. 输出 CSV 和阶段画像报告；
8. 更新 `motivation/results/` 和 `validation/results/` 中的结果分析。

如果 PostgreSQL 18.3 画像显示 object/fan-in、task 过细、operator invocation、模型队列或 backpressure 不是主要瓶颈，应及时调整优化方向。

## 9. 现有实验是否保留

现有 `benchmarks/` 里的实验需要保留，但要重新理解为 Phase 0 基线，而不是最终论文实验。

| 实验 | 是否保留 | 当前作用 |
|---|---|---|
| `ray_small_task_benchmark.py` | 保留 | 判断问题是否来自 Ray task 调度；当前结果说明 small task 不是最强瓶颈 |
| `ray_object_transfer_benchmark.py` | 保留 | 判断单个 object put/get/round-trip 成本，是后续 RecordBatch 传输的 baseline |
| `ray_many_objects_benchmark.py` | 必须保留 | 基础证据：固定总数据量下 object 数量增加会放大 fan-in 时间 |
| `ray_arrow_fanout_fanin_benchmark.py` | 必须保留 | 当前最贴近数据库 AI 算子链路的本地证据：Arrow RecordBatch fine/coalesced fan-in 有明显差异 |
| `arrow_recordbatch_serialization_benchmark.py` | 保留 | 判断 Arrow IPC 本身是否是瓶颈；当前结果说明单纯 Arrow 序列化不是主瓶颈 |
| `shuffle_simulation_benchmark.py` | 暂时保留 | 作为负结果/对照：本地 Python shuffle 不能代表真实 Ray/Daft shuffle，需要后续替换为 Ray/Arrow fan-out/fan-in |

删除标准：

- 如果某个实验既不能服务 Phase 0 瓶颈定位，也不能作为 baseline / negative result，再删除；
- 如果后续端到端 AI 算子实验已经覆盖 `shuffle_simulation_benchmark.py`，可以考虑把后者移入单独的历史结果目录，但不建议现在删除。

下一步新增实验应优先覆盖：

> fake `AI_EMBED(text)` 端到端链路。

这个实验会把 `ray_arrow_fanout_fanin_benchmark.py` 的结论迁移到更接近批量 embedding / RAG 数据准备的端到端数据链路。
