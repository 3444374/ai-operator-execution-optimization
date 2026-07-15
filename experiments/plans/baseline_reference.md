# 实验 Baseline 参考矩阵

整理日期：2026-07-16
用途：正式实验设计时，从 CCF-A 文献中提取 baseline 策略，避免使用 strawman 对照
来源：`opening/literature/ai_operator_literature_inventory.md` v3（57 篇）

---

## 使用规则

1. 每个实验方向（GPU 调度 / 写回 / 数据组织）在选择 baseline 时，**优先从本矩阵中选取**已有文献中的最优策略。
2. 非文献来源的工程 baseline（如 COPY、unlogged table）必须先在 B 系列实验中确认其为当前最优实践。
3. 最终论文的对照表必须标注每个 baseline 的来源论文或系统。

---

## 一、GPU 调度侧 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **G1** | Continuous Batching | vLLM (Kwon et al., SOSP 2023) | A | Iteration-level 动态组 batch；PagedAttention 内存管理 | 用 vLLM / Ray Serve 替代手动 HTTP endpoint；记录到达率→batch→完成的 latency 分布 |
| **G2** | Iteration-Level Scheduling | Orca (Yu et al., OSDI 2022) | A | 调度粒度从 request-level 降到 iteration-level；GPT-3 175B 上最高 36.9× 吞吐 | 同 G1，Orca 是 vLLM 的前身，选其一即可 |
| **G3** | Chunked-Prefills | Sarathi-Serve (Agrawal et al., OSDI 2024) | A | 将 prefill 拆成 chunks 避免阻塞 decode；2.6-5.6× 服务容量提升 | 仅在 AI_COMPLETE 场景中使用（embedding 无需 decode） |
| **G4** | Streaming Batch Model | Ray Data (Luan et al., arXiv 2025) | 预印本 | CPU/GPU 异构批处理 + pipeline 执行；3-8× 吞吐 | 作为 "naive pipelining" baseline：只做 compute-write overlap，不做 joint optimization |
| **G5** | Disaggregated Prefill/Decode | DistServe (Zhong et al., OSDI 2024) | A | Prefill 和 Decode 分离到不同 GPU | AI_COMPLETE 场景的可选对照 |
| **G6** | Phase Splitting | Splitwise (Patel et al., ISCA 2024) | 顶会 | 将推理拆分为 prompt 和 token generation 两个 phase，分别优化 | AI_COMPLETE 场景的可选对照 |

### 当前状态

本项目目前使用**手动启动的 HTTP endpoint**（`local_embedding_server.py`，单/双进程各占端口）。这是"可控的最小 GPU 服务"，适合做消融，但非社区标准 baseline。

**后续计划**：
- 优先接入 vLLM offline inference 或 Ray Serve 作为 G1/G2 baseline
- 若工程条件暂不具备，论文 §8 必须写"当前 GPU baseline 是简化的；vLLM continuous batching 可能进一步缩小 GPU 侧的独立优化空间"

---

## 二、写回侧 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **W1** | COPY + 延迟建索引 | PostgreSQL 官方文档 §14.4 + pgvector Issues #400/#430 | 官方文档 | 先 COPY 到 unlogged table → `CREATE INDEX HNSW`（事后建索引远比增量插入快） | 写回侧"工程最优"baseline。跑 B 系列实验确认数字 |
| **W2** | io_uring + 空间感知插入 | TurboVecDB (PVLDB 2025) | **A** | 并行 I/O + 空间感知重排插入顺序；HNSW index build 减少 98.4%；查询吞吐 11.1× | 若 pgvector 版本已包含此优化，自动成为写回 baseline |
| **W3** | Worker-Direct Blind Append | Delta Lake (Armbrust et al., PVLDB 2020) | **A** | 多 worker 各写各的，盲追加永不冲突；optimistic concurrency | 对应本项目的 A2 实验（worker-direct 写回） |
| **W4** | Queue-Worker Decoupled | pgai Vectorizer Worker (Timescale) | 工程 | 触发器→队列表→外部 worker 轮询→各自写回；`FOR UPDATE SKIP LOCKED` + advisory lock | 对应本项目的 A3 实验（queue-worker 写回） |
| **W5** | Lazy Materialization (Merge-on-Read) | Iceberg (Okolnychyi et al., PVLDB 2024) | **A** | 先写 delete file 标记，后台 compaction 时再物理合并；避免写时重写 | 可作为"最懒写回"理论 baseline |
| **W6** | KV 分离避免 Compaction 重写 | WiscKey (Lu et al., FAST 2016) | **A** | LSM-tree 只存 key，大 value（embedding 向量）存在独立 vLog | 论证 embedding 大 value 的存储引擎选择依据 |
| **W7** | 列式格式写入（Parquet/Lance） | ColStorEval (Zeng et al., PVLDB 2023) + Lance (Pace et al., arXiv 2025) | **A** + 预印本 | Parquet/ORC 写入性能系统对比；Lance 自适应编码 | Sink 对照实验（C 系列）的格式选择依据 |

### 当前状态

本项目目前使用 `psycopg2 execute_values()` 逐批 UPSERT。这不是最优工程实践（COPY 可快 10-50×）。

**下一步**：**B 系列实验必须先做**——确认 COPY + unlogged table + 延迟建索引 是否为当前最优写回 baseline。如果 COPY 把写回从 1.5s 降到 0.3s，写回占比从 45% 降到 12%，则 RC3 的论证需要收紧——但这本身也是有价值的发现。

---

## 三、数据组织侧 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **D1** | Fixed Partition + Fixed Batch | Daft 官方文档 + Spark SQL Tuning Guide | 官方文档 | 固定 partition 数 + 固定 batch size；不做 workload 感知 | 当前已部分覆盖（coalesced vs fine） |
| **D2** | Pre-Shuffle Merge | Daft Shuffle 文档 | 官方文档 | 先合并 input partitions 降低 slot count，再进行 shuffle | Daft 层的 object coalescing baseline |
| **D3** | AI 感知查询优化（谓词上拉 + 模型级联） | Cortex AISQL (Aggarwal et al., SIGMOD 2026) | **A** | LLM 成本作为一阶优化目标；必要时将昂贵 AI 谓词上拉到 Join 后 | 数据组织层面的 selectivity-aware 策略参考 |
| **D4** | ML 谓词推理重写 | Smart (Guo et al., VLDB Journal 2025) | **A** | 推理重写、渐进式推理、成本最优物理优化 | AI_FILTER/AI_CLASSIFY 场景的 selectivity-aware 策略参考 |

---

## 四、跨层决策 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 与本课题的差异 |
|---|---|---|---|---|---|
| **X1** | 代价驱动的 Compute-vs-Storage Pushdown | FlexPushdownDB (Yang et al., PVLDB 2021) | **A** | 基于代价的 push-to-storage vs pull-to-compute 决策模型 | 只覆盖 compute↔storage 维度，不覆盖 GPU batch↔write batch 的 joint decision |
| **X2** | 稀疏物化（Sparse Materialization） | AIDB (Jin et al., SIGMOD 2024) | **A** | 不是所有 ML 推理结果都物化到数据库；350× 成本降低 | 从"是否写回"角度，不涉及"写回批量和 GPU 批量的 joint optimization" |
| **X3** | 延迟视图维护 | Deferred View Maintenance (Colby et al., SIGMOD 1996) | **A** | 攒批 → 批量维护物化视图，减少事务开销 | 经典理论，但不涉及 GPU 推理侧 |

---

## 五、端到端流程调优增强对照矩阵

该矩阵用于在阶段级调优完成后分析阶段间耦合；它是增强型对照，不作为当前开题主叙事的前置假设。

| 编号 | 组名 | GPU 策略 | 写回策略 | 来源 | 代表什么 |
|---|---|---|---|---|---|
| **BL1** | GPU-Only Optimal | G1/G2 最优 B_gpu, W, endpoint | 默认 driver 写回（当前方式） | vLLM/Orca | GPU 岛最优，不管写回 |
| **BL2** | Writeback-Only Optimal | 默认 coalesced batch | W1/W2 最优 mode, B_write, sink | TurboVecDB + COPY | 写回岛最优，不管 GPU |
| **BL3** | Independent Best | BL1 的 GPU 配置 | BL2 的写回配置 | 组合 BL1+BL2 | 增强对照：检查阶段级最优拼装是否等于端到端最优 |
| **BL4** | Naive Pipeline | 固定 B_gpu，流水线写回 | 固定 overlap | Ray Data (G4) | 只做 overlap，不做 joint optimization |
| **BL5** | Queue-Decoupled | 任意 GPU 策略 | Queue → worker 写回 | pgai (W4) | 解耦但无代价模型 |
| **BL6** | FlexPushdownDB-Style | 代价决策（compute/storage pushdown） | 代价决策（compute/storage pushdown） | FlexPushdownDB (X1) | 已有跨层决策模型，但不管 GPU batch |

**最低必跑集合**（硕士论文现实约束）：BL1, BL2, BL4，加上完整优化流程。BL3 可在阶段间耦合明显时加入，用于增强论证。

---

## 六、使用检查清单

设计新实验或新 baseline 时，逐项确认：

- [ ] GPU 调度侧 baseline 是否覆盖了 vLLM/Orca/Sarathi-Serve 中的至少一种？
- [ ] 写回侧 baseline 是否已确认 COPY + 延迟建索引为当前最优工程实践？
- [ ] 跨层对照是否包含了 FlexPushdownDB 或 AIDB 的决策模型？
- [ ] 每个 baseline 是否标注了来源论文/系统？
- [ ] 是否避免了"常识级 strawman"作为唯一 baseline？
