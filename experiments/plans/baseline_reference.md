# 实验 Baseline 参考矩阵

整理日期：2026-07-16；文献与官方 baseline 复审：2026-07-29

> **2026-07-17 口径更新**：本文中的"跨层决策""写回瓶颈""RC3"等旧术语已统一。最新 baseline 分级、研究内容定义和优先级以 `AGENTS.md` §1、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md` 为准。
用途：正式实验设计时，从正式论文、官方系统和可审计工程默认中提取 baseline，避免使用 strawman 对照
来源：`research/ai_operator_literature_inventory.md` 与 `research/top15_ranked_papers.md`

> **2026-07-16 方向更新**：vLLM 已定位为部署平台（非竞争对手），其 continuous batching 是 S 级 baseline——课题研究上游调度优化，不修改 vLLM 内部。新增 baseline 候选：Ray 2.49+ PrefixCacheAffinityRouter、Ray Serve batch_size_fn 等。详细背景见 `research/knowledge_hub.md`。

---

## 使用规则

1. 每个实验方向（GPU 调度 / 写回 / 数据组织）在选择 baseline 时，**优先从本矩阵中选取**已有文献中的最优策略。
2. 非文献来源的工程 baseline（如 COPY、unlogged table）必须先在 B 系列实验中确认其为当前最优实践。
3. 最终论文的对照表必须标注每个 baseline 的来源论文或系统。

---

## 一、GPU 调度侧 Baseline

2026-07-29 起，正式端到端实验保留四类参照，不能混称为一个 baseline：

- **direct-vLLM service-only capacity** 是物理上界参照。使用相同模型、请求
  trace、输出设置和 endpoint 数，绕过 PostgreSQL/Daft/Ray 测服务可实现容量；
  上游 infra 的目标是逼近而不是超过它。
- **现有无 Daft/Ray 数据库 AI 算子** 是产品级核心 baseline。当前首选
  OceanBase `AI_COMPLETE`，使用同机 OpenAI-compatible vLLM endpoint；
  若 Community Edition/endpoint/观测门禁未通过，只作工业参考。
- **同 PostgreSQL bounded AsyncIO** 是因果 baseline。它不是产品竞争对手，
  但能隔离 OceanBase/PostgreSQL 差异与 Daft/Ray 的净贡献；必须独立标定，
  不能使用串行 strawman。
- **Daft/Ray 官方 runtime** 是框架归因 baseline：Daft `prompt()` Native/Ray
  和 Ray Data HTTP Processor，用于判断自定义 token-work/refill 是否超越
  现有官方实现。
- **naive DB→Daft→Ray→vLLM** 是必须显著击败的工程 baseline，例如固定行数、
  job-local/global K、无 workload-aware organization。正式策略还必须对比经
  静态 sweep 得到的最佳强 baseline，不能只赢单行串行 strawman。

报告 `capacity_efficiency = pipeline_tokens_s / direct_service_tokens_s`，并在
相同 P99/SLO guardrail 下比较 goodput；若 direct 上界未测，不能用 MFU 单独
声称“GPU 已压满”或“仍有 70% 优化空间”。

上述 baseline 使用两个互不交叉排名的协议轨道：

- **Chat 产品兼容轨道**：vLLM Bench、bounded Chat、Daft Native/Ray、
  Ray Data、OceanBase capability 与项目 Ray async dispatch；
- **Completions 机制轨道**：bounded fixed-row multi-prompt 与项目原始
  multi-prompt token-budget/length-align/flush 路径。

两条轨道分别使用同一 request manifest、同一双 endpoint、同一输出上限和
独立 calibration。不得用 Completions 数值直接声称超过 Chat baseline。
详细预注册见 `database_ai_operator_baseline_matrix_20260729.md`。

正式策略比较还必须区分：calibration 得到并在 held-out 上冻结的最佳静态
baseline、每个 workload 事后 sweep 的 static oracle，以及仅冻结候选边界、
运行时自动选择的 dynamic policy。动态策略应对比冻结静态点，并报告相对
per-workload oracle 的 regret；不能把为每个 workload 人工精调的 oracle
冒充可部署静态方案，也不能完全跳过安全容量边界校准。

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **G1** | Continuous Batching | vLLM (Kwon et al., SOSP 2023) | A | Iteration-level 动态组 batch；PagedAttention 内存管理 | 用 vLLM / Ray Serve 替代手动 HTTP endpoint；记录到达率→batch→完成的 latency 分布 |
| **G2** | Iteration-Level Scheduling | Orca (Yu et al., OSDI 2022) | A | 调度粒度从 request-level 降到 iteration-level；GPT-3 175B 上最高 36.9× 吞吐 | 同 G1，Orca 是 vLLM 的前身，选其一即可 |
| **G3** | Chunked-Prefills | Sarathi-Serve (Agrawal et al., OSDI 2024) | A | 将 prefill 拆成 chunks 避免阻塞 decode；2.6-5.6× 服务容量提升 | 仅在 AI_COMPLETE 场景中使用（embedding 无需 decode） |
| **G4** | Streaming Batch Model | Ray Data (Luan et al., arXiv 2025) | 预印本 | CPU/GPU 异构批处理 + pipeline 执行；3-8× 吞吐 | 作为 "naive pipelining" baseline：只做 compute-write overlap，不做 joint optimization |
| **G5** | Disaggregated Prefill/Decode | DistServe (Zhong et al., OSDI 2024) | A | Prefill 和 Decode 分离到不同 GPU | AI_COMPLETE 场景的可选对照 |
| **G6** | Phase Splitting | Splitwise (Patel et al., ISCA 2024) | 顶会 | 将推理拆分为 prompt 和 token generation 两个 phase，分别优化 | AI_COMPLETE 场景的可选对照 |

### 当前状态

当前已使用真实双 4090 vLLM endpoint，并建立 vLLM Bench、bounded HTTP、
Daft `prompt()` Native/Ray 和 Ray Data HTTP Processor 的同 manifest 功能/计数
门禁。2026-07-30 复核发现旧 project Chat 路径最佳约
5.9K tokens/s、31.2s，而同 manifest bounded Chat C256 为
14.5K tokens/s、12.6s。新增持久异步 HTTP/Ray dispatch 后，单次 worktree
smoke 的 Chat K256 model-request wall 为 12.552s；multi-prompt
Completions fixed16 project/direct 为 11.164/10.943s，功能上已接近同协议
上限。正式策略排名仍需 1 warm-up + 3 repeats feeding gate 达到至少 95%。
之后只做必要的独立 calibration，找到合理强且进入平台期的配置；不把每个
baseline 调成无限参数搜索，也不把未到 ceiling 的默认点当最终结果。

多 job 侧增加 VTC（OSDI 2024）作为算法基线：token-cost service counter、
work-conserving borrowing 和每 job service/JCT/fairness。Llumnix（OSDI 2024）
作为多实例 virtual usage 与在线纠偏参考，不要求实现 KV live migration。

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

**下一步**：**B 系列实验必须先做**——确认 COPY + unlogged table + 延迟建索引 是否为当前最优写回 baseline。如果 COPY 把写回从 1.5s 降到 0.3s，写回占比从 45% 降到 12%，则研究内容三的论证需要收紧——但这本身也是有价值的发现。

---

## 三、数据组织侧 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **D1** | Fixed Partition + Fixed Batch | Daft 官方文档 + Spark SQL Tuning Guide | 官方文档 | 固定 partition 数 + 固定 batch size；不做 workload 感知 | 当前已部分覆盖（coalesced vs fine） |
| **D2** | Pre-Shuffle Merge | Daft Shuffle 文档 | 官方文档 | 先合并 input partitions 降低 slot count，再进行 shuffle | Daft 层的 object coalescing baseline |
| **D3** | Semantic Operator Optimization | LOTUS (Patel et al., PVLDB 2025) | **A** | 准确率约束下选择代理模型、cascade、join/ranking 算法 | 数据库 AI 系统 baseline；冻结模型调用 work 后再与运行时策略比较 |
| **D4** | ML 谓词推理重写 | Smart (Guo et al., VLDB Journal 2025) | **A** | 推理重写、渐进式推理、成本最优物理优化 | AI_FILTER/AI_CLASSIFY 场景的 selectivity-aware 策略参考 |
| **D5** | Declarative Plan Search | Palimpzest (Liu et al., CIDR 2025) | 非 CCF-A | 用样本估计 time/cost/quality 并选择 Pareto 计划 | 官方系统 baseline；不把单线程默认当强 serving baseline |
| **D6** | Semantic Query Benchmark | SemBench (Lao et al., PVLDB 2026) | **A** | 55 queries、文本/图像/音频、质量/时间/成本/内存/扩展性 | workload 和评价协议依据，不是调度算法 |

---

## 四、跨层决策 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 与本课题的差异 |
|---|---|---|---|---|---|
| **X1** | 代价驱动的 Compute-vs-Storage Pushdown | FlexPushdownDB (Yang et al., PVLDB 2021) | **A** | 基于代价的 push-to-storage vs pull-to-compute 决策模型 | 只覆盖 compute↔storage 维度，不覆盖 GPU batch↔write batch 的 joint decision |
| **X2** | 稀疏物化（Sparse Materialization） | AIDB (Jin et al., SIGMOD 2024) | **A** | 不是所有 ML 推理结果都物化到数据库；350× 成本降低 | 从"是否写回"角度，不涉及"写回批量和 GPU 批量的 joint optimization" |
| **X3** | 延迟视图维护 | Deferred View Maintenance (Colby et al., SIGMOD 1996) | **A** | 攒批 → 批量维护物化视图，减少事务开销 | 经典理论，但不涉及 GPU 推理侧 |
| **X4** | Semantic Operator Pareto Optimization | Abacus (Russo et al., PVLDB 2026) | **A** | profile + MAB/Pareto 搜索 quality/cost/latency 计划 | 优化调用/实现选择；本项目只迁移 profile 与 ranking 方法 |
| **X5** | UDF/Placement Cost Estimation | GRACEFUL (ICDE 2025) + COSTREAM (ICDE 2024) | **A** | 估计 UDF runtime 与异构 operator placement | 本项目首版为解析模型 + profile + residual，不直接采用复杂 GNN |

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
- [ ] 数据库 AI 系统 baseline 是否覆盖 LOTUS/Palimpzest，评价协议是否参考 SemBench？
- [ ] 多 job 是否包含 VTC/shared-credit，并同时报告聚合吞吐、每 job JCT/P99、Jain fairness 和 idle borrowing？
- [ ] 代价估计是否用 ranking/regret 验证了决策价值，而不只报告 MAPE？
