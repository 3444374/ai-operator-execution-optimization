# 评估指标调研：AI 算子与推理服务文献 + 数据库厂商基准

首次整理：2026-07-31；最近更新：2026-08-05
调研工具：`nature-academic-search` + `deep-research`（lit-review 口径），以一个后台工作流执行（15 个抽取 agent + 1 个综合 agent）。
证据范围：项目已有 49 篇精读笔记 + `baseline_reference.md` 已核验的数据库厂商官方
文档与标准 benchmark；论文数字优先回到本地精读笔记和一手论文，产品场景优先回到
厂商官方文档。

---

## 1. 目的

回答两个问题：

1. 相关文献（数据库 AI 算子、LLM 推理服务、调度/公平、代价估计、数据引擎/向量存储、benchmark）在实验中**实际报告哪些评估指标**？
2. 数据库厂商（Snowflake Cortex、BigQuery ML、Oracle AI Vector Search、PostgresML、pgai/pgvector）与标准基准（MLPerf Inference、向量库 benchmark、LLM goodput 口径）**用什么性能指标**？

在此基础上对照项目当前指标集，找出**细分缺口**与可补采项。不重新论证项目已有指标——只标"已覆盖/部分/缺口/不适用"。

> **文档范围扩展**：正文 §3–§5 聚焦指标；**附录 A** 扩展到 workload/数据集（含对 AI_COMPLETE 可用性判定）；**附录 B** 扩展到数据库厂商 AI 算子的测试方法论（逐家怎么测 + 跨厂商共识 + 对本项目的启示 + PolarDB Lakebase 同栈专项）。

---

## 2. 方法与证据来源

| 来源 | 簇/目标 | 数量 |
|---|---|---|
| 项目精读笔记 | A 推理服务（vLLM/Orca/Sarathi-Serve/SGLang/DistServe/Splitwise/Mooncake/FlexGen/Multi-Bin/ServerlessLLM/FlashAttention） | 11 |
| 项目精读笔记 | B1 多 job 公平（VTC/Llumnix/FairServe/DLPM/Autellix/Chiron） | 6 |
| 项目精读笔记 | B2 自适应 admission/路由（Clipper/CONCUR/SCORPIO/SABER/BucketServe/ProServe/CoLoRA/SFS） | 8 |
| 项目精读笔记 | C 数据库 AI 系统（LOTUS/GaussML/Galois/Smart/SmartLite/InferDB/LEADS/NeurDB/Cortex AISQL/Palimpzest/Abacus） | 11 |
| 项目精读笔记 | D 代价估计（Heinrich/CONCERTO/GRACEFUL/COSTREAM/Pathak/DB-Perspective/LLM4DM） | 7 |
| 项目精读笔记 | E 数据引擎/向量存储（Ray/Ray Data/Lance/Milvus/DiskANN） | 5 |
| 项目精读笔记 | F benchmark 方法论（SemBench） | 1 |
| Web 调研 | Snowflake Cortex / BigQuery ML / Oracle AI Vector Search / PostgresML / pgai+pgvector / MLPerf Inference / 向量库 benchmark / LLM goodput 口径 | 8 |

综合按 10 类归目（throughput / latency / tail-or-slo / capacity-or-scaling / efficiency-or-utilization / fairness / cost-or-energy / quality-or-accuracy / correctness-or-audit / other），逐条判定 `已覆盖 / 部分 / 缺口 / 不适用`。

**证据分级**：

- **文献事实**——精读笔记原文记载的指标用法（多数条目）。
- **厂商来源**——web 调研，标官方文档/基准规范/博客；营销博客仅作参考并标注。
- **本地代码事实**——本调研中已亲自核实（§6 列出）。
- **合理推断**——跨论文归纳或对项目缺口的解释。

---

## 3. 指标目录（按类）

状态图例：✅ 已覆盖｜🟡 部分｜🔴 缺口｜⚪ 不适用（非本课题场景）

### 3.1 Throughput（吞吐）

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| Raw throughput（req/s, tokens/s, rows/s, QPS, programs/s） | 单位时间处理的请求/token/行/程序数 | vLLM/Orca/Sarathi/DistServe/Splitwise/Mooncake/FlexGen/VTC/Clipper/Ray… | MLPerf Offline tokens/s、NVIDIA GenAI-Perf、vLLM bench、Oracle TPS/RPM、PostgresML predictions/s、pgvector QPS | ✅ |
| Goodput — SLO-constrained req/s | 满足 TTFT+TPOT 双 SLO 下单位时间达标请求数 | DistServe/Sarathi/Mooncake/SCORPIO/SABER/ProServe/BucketServe | vLLM bench `--goodput`、GenAI-Perf `-g`、LLMPerf | ✅ |
| Goodput — SLO-constrained **tokens/s** | 满足 SLO 下有效输出 token 吞吐 | arXiv 2410.14257、Mooncake | NVIDIA AIPerf（Output Token Throughput/User） | 🟡 |
| Per-GPU/per-endpoint capacity under SLO | 某 SLO attainment 下单 GPU/endpoint 最大 RPS | DistServe/Sarathi/Mooncake/BucketServe | MLPerf Server、Oracle | 🟡 |
| Normalized latency（s/token） | e2e 延迟 / 输出 token 数 | vLLM、Orca、Multi-Bin | Oracle（per-query TPS 倒数） | 🟡 |
| Latency-throughput Pareto frontier | 扫并发把吞吐与尾延迟画成前沿 | FlexGen/vLLM/Sarathi/DistServe | — | 🟡 |
| #Batched requests / running concurrency | 同时在批中被处理的请求数 | vLLM | Oracle | ✅ |
| PTU / Provisioned Throughput Unit | 商业预留吞吐单位 | — | Snowflake PTU、BigQuery Provisioned、Vertex AI | ⚪ |
| Training wall-clock / RL reward | 训练场景 | FlashAttention、Ray | — | ⚪ |

### 3.2 Latency（延迟）

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| **TTFT（Time-To-First-Token）** | 请求到达到首 token 返回（含排队+prefill） | Sarathi/DistServe/Splitwise/Mooncake/SGLang/Llumnix/SCORPIO/SFS/ServerlessLLM | vLLM `time_to_first_token` Histogram、GenAI-Perf、MLPerf、Oracle | 🟡（仅均值，无分位） |
| **TPOT / TBT / ITL** | decode 阶段每 token 生成间隔；TBT/ITL 为逐 token 区间分布 | Sarathi/DistServe/Splitwise/Mooncake/Llumnix/SCORPIO | vLLM `inter_token_latency` Histogram、GenAI-Perf、MLPerf、Oracle | 🔴 |
| E2E request latency | 单请求提交到最后一个 token | Splitwise/SGLang/ServerlessLLM/Clipper | vLLM、GenAI-Perf、Oracle | ✅ |
| Tail latency percentiles（P50/P90/P95/P99） | 请求级延迟高分位 | Sarathi/Splitwise/Mooncake/ServerlessLLM/Clipper/CoLoRA | vLLM、GenAI-Perf、MLPerf、Oracle、pgvector p95/p99 | ✅ |
| Latency stage breakdown（分阶段性能剖析） | e2e 拆为可定位子阶段 | Clipper（predict/queue/top） | Oracle semantic-cache | ✅（项目比多数文献更细） |
| Generation stall duration | prefill 抢占 decode 的秒级停顿 | Sarathi-Serve | — | 🔴 |
| Checkpoint/cold-start latency | serverless 冷启动 | ServerlessLLM | — | ⚪ |
| Task launch / object-store get-put（Ray） | Ray 组件级延迟 | Ray OSDI2018 | — | 🟡（feasibility 范畴） |
| Program-level JCT / victim latency | 多步 agent program 端到端；被牺牲请求时延 | Autellix/DLPM/Llumnix | — | ⚪ |

### 3.3 Tail / SLO

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| SLO attainment rate | 满足 SLO 的请求比例 | DistServe/Mooncake/Splitwise/Chiron/SCORPIO/ProServe/BucketServe | — | ✅ |
| SLO Scale / tightest tolerable SLO | 线性缩 SLO 阈值，找最紧可承受倍数 | DistServe | — | 🔴 |
| Cumulative SLO-met request count | trace 累计达标请求数曲线 | SCORPIO | — | 🟡 |
| Request timeout rate | 超时丢弃比例 | ServerlessLLM | — | 🟡 |
| Row-level success rate / query completion | 单行成功率（>99.99%） | — | BigQuery、Snowflake | 🟡 |
| OnTimeUtility / TDG_Ratio | deadline-aware gain | SFS、ProServe | — | ⚪ |
| p50/p95 latency by route | 按路径/缓存命中分桶报延迟 | — | Oracle semantic-cache | 🟡 |
| Coefficient of Variation（predictability） | completion/SLA 比值的 CV（稳定性） | SABER | — | 🔴 |

### 3.4 Capacity / Scaling

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| Capacity / max QPS（系统容量） | SLO 约束下最大并发/请求量 | Sarathi/DistServe/Mooncake | — | 🟡 |
| Multi-GPU/multi-node scaling | 扩展比例 | FlexGen/Ray/Milvus/Llumnix/Clipper | — | ⚪ |
| Prefill saturation threshold Lm | 饱和 GPU 的最小 prompt token 数 | DistServe/Splitwise/Sarathi/Multi-Bin | — | 🟡 |
| Data-volume scalability curve | 随行数增长的性能曲线 | NeurDB/LEADS/InferDB | — | 🟡 |
| Context window / max output / embedding dim | 模型容量参数 | — | Snowflake/Oracle/pgvector | ⚪ |
| Index / storage size | 索引/向量存储占用 | DiskANN/Milvus/InferDB | pgvector/VectorChord/ANN-benchmarks | 🟡 |

### 3.5 Efficiency / Utilization

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| GPU utilization / MFU / MBU | GPU 计算效率；MFU=实际 FLOPs/峰值 | Sarathi（Roofline）/FlexGen/BucketServe/CoLoRA | Ray Data（88.4% peak）、Oracle | ✅ |
| KV cache usage（%） | 已用 KV block 占比 | vLLM | vLLM `kv_cache_usage_perc` | ✅ |
| **Prefix cache hit rate** | prefix cache 命中 prefill token 占比 | SGLang（50–99%）/Mooncake/CONCUR/CoLoRA | vLLM `prefix_cache_queries/hits` | 🔴 |
| KV cache memory saving（sharing 节省） | CoW block 共享节省比例 | vLLM | — | ⚪ |
| Arithmetic intensity / Roofline 分析 | FLOPs/byte 划分 compute/memory-bound | Sarathi、FlashAttention | — | 🔴（设计期分析） |
| Padding waste ratio（WasteRatio） | 长度异构 padding 到 Smax 的浪费比 | BucketServe（Eq.2/3/4） | — | 🟡 |
| Scheduling/routing overhead（%） | 调度逻辑占总执行时间比例 | SGLang(<0.3%)/SCORPIO(0.12–0.17%)/BucketServe(<1%)/ProServe/SFS/Sarathi | — | 🟡 |
| KV-cache cross-pool migration overhead | disaggregation 跨池传输代价 | DistServe/Splitwise | — | ⚪ |
| Fragmentation / preemption loss | KV 碎片率/抢占损失 | Llumnix | — | 🟡 |
| Bandwidth utilization / HBM R/W | 存储/网络/HBM 带宽利用 | ServerlessLLM、FlashAttention | — | ⚪ |
| Memory footprint | 推理/训练显存占用 | FlashAttention/FlexGen/SmartLite/InferDB/SemBench | — | 🟡 |
| GPU peak utilization（% of theoretical max） | 实际/理论最大吞吐% | Ray Data | — | ✅ |
| Index build time | 向量索引构建墙钟耗时 | DiskANN/Milvus/pgvector/PostgresML | pgvector/VectorChord/Neon | 🟡 |
| Cache block hot-cold distribution | KV block 命中频次分布 | Mooncake | — | 🟡 |
| Approved semantic-cache hit rate | 语义缓存真实节省 provider 调用 | — | Oracle semantic cache | ⚪ |
| Compression ratio / BINARY-vs-FLOAT32 | 向量压缩比 | — | Oracle、Lance | ⚪ |

### 3.6 Fairness（公平）

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| Jain fairness index | 0–1 公平指数 | CoLoRA、ProServe | — | ✅ |
| **Service disparity（跨客户端累计服务差 + 上界）** | 持续 backlogged 客户端累计 virtual token counter 最大/平均差及理论上界 | VTC、DLPM | — | 🔴 |
| Throttled interactions / delayed users / token waste | 过载限流交互数/推迟用户/无效 token | FairServe | — | 🟡 |
| Per-job max-P99 / max-JCT fairness | 最差 job 维度公平 | —（项目比文献更严） | — | ✅ |

### 3.7 Cost / Energy

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| GPU power(W) / energy / Perf-per-Watt | 功耗、能耗、单位功耗吞吐 | Splitwise、Big-ANN T3 | MLCommons（perf/W 提交） | ✅ |
| **Cost per million tokens（$/M tokens）** | 按 token 用量计价的商业费率（input/output 分计） | — | Snowflake credits/M tokens、BigQuery Vertex billing、Oracle | 🔴 |
| Total #tokens / #model calls | token 总量/调用次数 | Galois/LOTUS/Cortex/SemBench | — | 🟡 |
| Money cost（$）/workload backfill | 完成数据集的美元花费 | Abacus/Palimpzest/SemBench/PostgresML | Snowflake/BigQuery/Timescale | 🔴 |
| GPU-hours | 作业消耗的 GPU 小时 | Ray Data | — | 🟡 |
| Hardware Cost（$/hr）/ Perf-per-$ | 每小时成本与单位成本吞吐 | Splitwise | Timescale、Big-ANN T3 | ⚪ |

### 3.8 Quality / Accuracy

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| Accuracy / Perplexity（压缩质量保持） | 量化/压缩后质量 | FlexGen、FlashAttention | — | ⚪ |
| **Recall@k / nDCG@10** | ANN 检索真实最近邻占比/排序质量 | — | Milvus/DiskANN/pgvector/VectorChord/Arctic Embed(MTEB)/ANN-benchmarks | 🔴（写回→检索闭环） |
| F1 / Precision / Recall / AUC | 分类质量 | InferDB/Galois/Cortex/Palimpzest/LEADS | — | 🟡 |
| RAG triad / Correctness / Coherence | RAG 三联+LLM-as-judge | — | Snowflake AI Observability、BigQuery ML.EVALUATE | ⚪ |
| Text-to-SQL / model capability benchmarks | MMLU/HumanEval/GSM8K/Spider | — | Snowflake Cortex Analyst | ⚪ |
| ROUGE / BLEU / Exact Match | n-gram 重叠/精确匹配（MLPerf 质量门禁） | MLCommons | BigQuery ML.EVALUATE | ⚪ |
| Bin classification accuracy / ±1 bin | 长度预测器分桶准确率 | Multi-Bin | — | 🟡 |
| Embedding latency（单条查询嵌入延迟） | 原始查询编码为向量的 e2e 延迟 | — | BigVectorBench、PostgresML | 🟡 |

### 3.9 Correctness / Audit（代价模型与调度审计）

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| **Q-Error（mean/median/P50/P90/P95/P99/max）** | 预测值与真实值之比（≥1） | Heinrich/CONCERTO/GRACEFUL/COSTREAM | — | 🟡（已计划补） |
| **Rank correlation（Spearman ρ）/ pairwise / Top-K** | 预测排名与真实排名一致性 | Heinrich | — | 🟡（已计划补） |
| **Pick Rate / Surpassed Plans / Balanced Accuracy / Selected Runtime** | 决策质量：选中最优计划比例 | Heinrich/CONCERTO/DACE | — | 🟡（已计划补） |
| Selected Runtime / Total slow-down % / regret | 决策与真实最优的实际执行时间及 slow-down | Heinrich/GRACEFUL/COSTREAM | — | 🟡 |
| Cost model error（MAE/RMSE/R²/MAPE） | 代价模型误差四件套 | SCORPIO/SABER/Pathak/Splitwise/DistServe | — | ✅ |
| Max Under/Overestimation factor / card-error robustness | 尾部风险与输入噪声鲁棒性 | Heinrich、CONCERTO | — | 🔴 |
| Predictor inference latency / model size / training time | 代价模型自身开销 | CONCERTO、Heinrich | — | 🟡 |
| exactly-once / completion-lag / HOL-age / credit-held audit | 调度级严谨性审计 | —（项目差异化点） | — | ✅ |
| Compliance checks（first-token/EOS-once/token-count） | 防作弊三项 | MLCommons | — | 🟡 |
| Row-level status / truncation flag | 逐行 API 状态/截断标志 | — | BigQuery | 🟡 |
| Standard deviation / error bars / CI | 多次运行方差/置信区间 | InferDB/Abacus/FlashAttention/ServerlessLLM | — | 🟡 |
| Performance regression count（safety audit） | 策略驱动后劣化案例数 | GRACEFUL | — | 🔴 |
| Algorithm solve time / rejection count / wait-time R² | 算法求解时间/拒绝数/等待预测 R² | DistServe/Mooncake/Chiron | — | 🟡 |

### 3.10 Other（控制 trace 与工程机制）

| 指标 | 定义 | 文献 | 厂商/标准 | 状态 |
|---|---|---|---|---|
| Inflight/queue time series / sample age / control decision sequence | 自适应控制 trace | CONCUR/Chiron/Clipper/CoLoRA | — | ✅（项目强制采集，差异化点） |
| Adaptive traffic control + producer-consumer retry | 自适应流量控制+重试机制 | — | BigQuery | 🟡 |
| GCS flush / actor failure recovery / fault-recovery throughput drop | 故障恢复 | Ray/Ray Data | — | ⚪ |
| Engineering robustness / failures audit | 工程缺陷审计 | SemBench | — | 🟡 |
| RL cumulative reward / convergence | RL 场景 | Ray | — | ⚪ |

---

## 4. 项目已覆盖好的方面（对照文献的强项）

1. **分阶段性能剖析极完整**——CSV 采 `db_fetch_s / arrow_build_s / source_fetch_s / organizer_from_arrow_s/plan_s/collect_s / model_service_s / model_request_wall_s / submit_s / bounded_wait_s / fanin_s / writeback_s / e2e_s` 全链路，比 Clipper 的 predict/queue/top 与多数文献更细。**本地代码事实**。
2. **tokens/s 口径与 MLPerf/vLLM/Oracle 对齐**——用 `vllm:prompt_tokens_total + generation_tokens_total` delta，并提供 `model_request_tokens_per_s / operator_tokens_per_s` 多口径。**本地代码事实**（`code/src/metrics.py:419-430`）。
3. **KV cache 利用率与 GPU 利用率已采**——`vllm_kv_cache_usage_perc` after + mean/p50/p95/max 时间序列、`gpu_utilization_pct` 系列、`gpu_memory_used_mib`、`mfu_estimate`（`vllm:estimated_flops_per_gpu_total` 法，留 `model_flops_per_token/gpu_peak_tflops` 全参数）。**本地代码事实**。
4. **物理 cost-per-token 已采**——`energy_j_per_1k_observed_tokens` 把能耗折算到 token，是 Perf/W 的 token 级表达（Splitwise/Big-ANN 关心但少有论文落到 token）。**本地代码事实**。
5. **调度级严谨性审计是差异化点**——exactly-once 请求审计 + completion-lag + HOL-age + credit-held 四件套，加 control/submission/resource/flush trace；文献笔记反复标注 inflight/queue 时序为缺失项，本项目强制采集（`code/AGENTS.md` §6）。**本地代码事实**。
6. **公平性比多数文献更严**——除 Jain（中位）外还报 per-job 聚合吞吐 / max P99 / max JCT（最差 job 维度）。**本地代码事实**。
7. **prefix 分组信号已采**——`prefix_group_ratio` + 按 endpoint 分组的 `actor_worker_submission_counts`，具备补 prefix cache hit rate 的数据基础（只需新增两个 vLLM Counter 采集）。
8. **代价模型四件套（MAE/RMSE/R²/MAPE）已落地**，且已识别 Q-Error/Spearman/ranking 为计划补充项——方向与 Heinrich SIGMOD2025 方法论分水岭一致。

> 结论：throughput / 尾延迟 / SLO attainment / MFU+KV 利用率 / 能耗 / Jain+max-JCT 公平 / exactly-once 审计 / 控制 trace 八大类，项目已覆盖或优于多数文献。下列缺口是**细分项**，不是大类缺失。

---

## 5. 缺口与建议补采

按优先级排，每条标采集成本（多数为新增 vLLM Prometheus 字段或后处理列，不改策略代码）。

### P0（采集成本极低 + 直接解锁当前被混淆的结论）

| # | 指标 | 为什么 | 采集成本 |
|---|---|---|---|
| 1 | **TTFT P50/P95/P99 + 作为 goodput SLO key** | vLLM 已暴露 `vllm:time_to_first_token_seconds` Histogram；当前 `vllm_request_prefill_time_mean_s` 仅均值，`service_p99` 不分解 prefill/decode。TTFT 是 Sarathi/DistServe/Splitwise/Mooncake/vLLM/MLPerf/Oracle 共同事实标准，也是 goodput 双 SLO 第一维。AI_COMPLETE 生成式场景下 TTFT 直接决定用户感知。 | 低：`metrics.py` 加一个 Histogram 分位采集（仿现有 `_mean_delta` 改读 quantile）；同时 `vllm_bench.py` 已解析 `ttfts` 数组却丢弃，保留分布即可。 |
| 2 | **TBT / ITL P99 分布（逐 token 区间，非均值）** | vLLM 已暴露 `vllm:inter_token_latency_seconds` Histogram；`vllm_bench.py:138-141` 已解析 `itls` 却折叠进 e2e 求和（`ttft + sum(intervals)`），丢弃分布。文献明确 TPOT 偏松、TBT 偏严不可互替；Mooncake 部署阈值 TBT≤0.1s/token。补此项使 service_p99 的 decode 部分可解释。 | 低：同上 Histogram 分位 + `vllm_bench.py` 保留 `itls` 分布。 |
| 3 | **Prefix cache hit rate（`vllm:prefix_cache_queries/hits`）** | 项目已做 prefix-affinity routing，但结论被 model×endpoint×KV 与饱和 regime 混淆（4-ep/1.5B +5.9% 跨门禁待隔离消融）。**不采命中率就无法证明 routing 收益来自 cache 复用**——这是当前 prefix 结论因果归因受阻的根因之一。 | 极低：cache-ON 实验下 vLLM 已暴露 queries/hits 两个 Counter，`rate(hits)/rate(queries)` 即得，`metrics.py` 加两行。 |

### P1（补全有效吞吐/成本/公平/代价模型决策质量的对话界面）

| # | 指标 | 为什么 | 采集成本 |
|---|---|---|---|
| 4 | Goodput as tokens/s（SLO-constrained 输出 token 吞吐） | 生成式 AI_COMPLETE 输出长度可差 13.9×（`code/AGENTS.md` §6），req/s goodput 低估有效吞吐；已有 `tokens_per_s` 与 `request_slo_goodput_per_s` 两分量，合并即得。对齐 arXiv 2410.14257 smooth goodput。 | 低：后处理合并。 |
| 5 | Cost per million tokens（$/M tokens，input/output 分计） | 已有能耗口径 `energy_j_per_1k_observed_tokens`，换算 $/M tokens 即可与 Snowflake credits/M tokens、BigQuery token billing、Oracle provider-call avoidance 对齐——课题产品化定位（vs 商业 DB AI 函数）的成本对话界面。 | 低：后处理换算（需选定单价假设并标注）。 |
| 6 | Padding waste ratio（BucketServe Eq.(2) WasteRatio + 跨桶期望浪费） | 已有 `packing_budget_utilization_mean/p95`（≈1−waste），补显式 `(Smax−Savg)/Smax` 与跨桶期望浪费——length-align/token-budget 分组策略的**直接量化收益**证据。 | 低：后处理。 |
| 7 | Q-Error 多分位 + Spearman ρ + Pick Rate / Selected Runtime | 已计划补但未实采。Heinrich SIGMOD2025 是方法论分水岭：**Q-Error 精度 ≠ 优化质量**，必须配 Selected Runtime/Surpassed Plans/Spearman ρ 才能评估"代价估计→active-work/K 初始化/路由"决策的真实收益。当前仅 MAE/RMSE/R²/MAPE 无法回答"预测准了是否选对了配置"。 | 中：`estimate_operator_cost.py` 增 ranking 输出。 |
| 8 | Service disparity（VTC/DLPM 式跨 job 累计服务差 + 上界） | 本项目 active-work/credit 即属 attained-service 调度族，VTC/DLPM 的累计 virtual-token-counter 服务差及上界是该族最直接公平量化。当前只有 Jain（中位）+ max-JCT，缺累计服务差；`credit-held` 是相关信号但非跨 job 服务差。 | 中：多 job trace 增累计服务量聚合。 |
| 9 | Recall@10 / nDCG@10（AI_EMBED 写回 pgvector 后下游检索质量） | 多模态泛化验证写回 pgvector 后未验证下游检索质量。上游调度不改变嵌入值，但需 recall@k/nDCG@10 **证伪"批处理/写回引入质量偏差"**，闭合写回→检索产品化论证。pgvector README 已给对照采集法（`SET LOCAL enable_indexscan=off`）。 | 中：下游评估脚本。 |

### P2（低成本严谨性差异化 + 鲁棒性立体证据）

| # | 指标 | 为什么 | 采集成本 |
|---|---|---|---|
| 10 | Variance / CI / error bars 跨 formal repeat 显式报告 | 已有 `repeat_index` 机制，但报告未必标 ±std/CI。文献通病是多数预印本不报方差（仅 FlashAttention/ServerlessLLM/InferDB/Abacus 做了）；显式报告提升策略对比（如 prefix +5.9% 是否显著）可信度。 | 低：后处理。 |
| 11 | SLO Scale + Coefficient of Variation + Performance regression count | SLO Scale（DistServe，最紧可承受 SLO）与 CV（SABER，可预测性）是策略鲁棒性/稳定性附加维度；regression count（GRACEFUL）是安全性审计。策略晋级判定（5% 门禁）时提供更立体证据。 | 低：后处理。 |
| 12 | 调度逻辑开销占比%（对照文献 <1% 基准） | 已有 `organizer_plan_s/collect_s/submit_s` 分量，归一化为占 `operator_wall_s` 的百分比，对照 SGLang(<0.3%)/SCORPIO(0.12–0.17%)/BucketServe(<1%) 基准，论证自适应控制工程可行性。 | 低：后处理。 |

**2026-08-04 实现状态（代码事实，尚非新实验结果）**：上述 12 项已接入下一轮
采集/后处理链路。P0 由 vLLM histogram bucket delta 与 prefix counter 直接采集；
P1 的 token-goodput、padding、代价决策指标和多 job service disparity 已进入代码，
Recall/nDCG 需要显式 relevance 真值；P2 由 SLO-scale 字段和 formal-repeat 后处理器
输出。商业成本只有显式输入 input/output 单价才计算。理论 service-disparity bound、
没有 ground truth 的检索质量、没有价格的 $/M tokens 均保持 `unavailable`，不生成
替代数值。主 profiler schema 已变化，后续必须新建结果目录，不能向旧 CSV 追加。

---

## 6. 已核实的代码级事实（P0 三条的代码证据）

本节为本调研中亲自核实，非工作流传言：

1. **`code/src/baselines/ceilings/vllm_bench.py`（当时 126-141 行）**——vLLM bench detailed-result 解析路径读取 `ttfts` 与 `itls` 数组后，把每条请求折叠为 `latency = ttft + sum(intervals)`，**TTFT 值与逐 token ITL 分布均被丢弃**，只保留单条 e2e 标量。
2. **`code/src/metrics.py:433-437`**——从 vLLM Prometheus 采集 `vllm:e2e_request_latency_seconds / request_queue_time / request_inference_time / request_prefill_time / request_decode_time` 全部以 `_mean_delta`（仅均值）记录；**未采集** `vllm:time_to_first_token_seconds` 与 `vllm:inter_token_latency_seconds` 两个 Histogram 的分位。
3. **`code/src/metrics.py` 全文无 `prefix_cache` 字段**——未采集 `vllm:prefix_cache_queries_total` / `vllm:prefix_cache_hits_total`（cache-ON 实验下 vLLM 已暴露这两个 Counter）。

> 含义：三条 P0 缺口的根因都是"vLLM 已暴露信号、采集端未落字段或折叠了分布"，补采改动集中在 `metrics.py`（加 Histogram 分位 + 两个 Counter）与 `vllm_bench.py`（保留 `ttfts/itls` 分布），不触碰策略代码。属**本地代码事实**。

> 边界：vLLM 0.25.1 是否在所有部署下都暴露上述 Histogram/Counter、以及 prefix cache Counter 是否需要 `--enable-prefix-caching` 才输出，需在补采时用一次 `/metrics` 抓取确认（标记为**待确认**）。

---

## 7. 不能声称 / 边界

- 本调研的**文献侧**基于项目 49 篇精读笔记的二手记载，指标用法以笔记原文为准；若笔记本身遗漏了某论文的指标，本目录也会遗漏。未对原文 PDF 逐表复核。
- **厂商侧**指标来自官方文档/基准规范/博客的 web 调研，标注了来源类型；营销博客（如各厂 benchmark 之争）不作性能事实，仅作"该厂商关心哪些指标"的需求证据。
- **不可外推**：PTU、$/M tokens、provisioned throughput 等商业口径只用于对齐"厂商关心什么"，不能把本地固定 2×4090 的结果换算后声称优于商业服务。
- **not-applicable 类**（训练 wall-clock、RL reward、serverless 冷启动、multi-node scaling、KV cross-pool migration、RAG triad、text-to-SQL benchmark、semantic cache、BINARY 向量压缩）是本课题场景外指标，列出只为说明边界，不建议补采。
- 本目录**不是实验结论**，是指标体系调研；任何"项目应补某指标"均为建议，需结合实验优先级与 `code/AGENTS.md` §6 指标完整性要求排期。

---

## 8. 下一步与落点

1. **P0 三条优先**——TTFT 分位、ITL 分布、prefix cache hit rate。改动集中在 `code/src/metrics.py` + `code/src/baselines/ceilings/vllm_bench.py`，不触策略代码；先在 cache-ON 路由实验上补采，**直接服务当前 prefix 结论的隔离消融**（4-ep/7B 或 2-ep/1.5B、人为缩 KV 制造可控淘汰率）。登记到 `experiments/plans/experiment_status_and_gaps.md` 指标缺口区。
2. **P1 与代价模型计划合并**——Q-Error/Spearman/Pick Rate 已在代价估计计划清单（见 `research/knowledge_hub.md` §5.7 模式 2），与本调研一致，按既定批次推进；Goodput-as-tokens、padding waste、service disparity、recall@k 作为对应研究内容实验的附加报告项。
3. **P2 作为报告期统一处理**——Variance/CI、SLO Scale、CV、regression count、调度开销% 在正式结果报告与 `figures/` 绘图阶段一次性补齐。
4. 本文件作为 `research/` 的指标体系参考入口；后续新实验设计指标时先查本目录 §3，避免重复造指标或漏报文献标准项。

---

## 附录 A：workload / 数据集调研（2026-07-31 补）

本附录回答"这些文献和厂商用了什么数据"，并分析其对项目 AI_COMPLETE 算子的可用性。来自同一工作流的 `workloads` 抽取字段。

### A.1 五类 workload 来源

**A.1.1 LLM serving 文献（最大的一支，纯文本，不碰数据库）**

vLLM / Orca / Sarathi-Serve / DistServe / Splitwise / Mooncake / SGLang / VTC / Llumnix / CONCUR / SCORPIO / SABER / Clipper / BucketServe / ProServe / arXiv 2410.14257 几乎共用同一组数据：

- 对话/指令（事实标准）：**ShareGPT / ShareGPT-gpt4**、**Alpaca / Stanford Alpaca**、LMSYS-Chat-1M、BurstGPT、openchat_sharegpt4
- 长文本：LongBench、L-Eval、arxiv_summarization、GovReport、Ruler、16k–128k 长上下文模拟
- 代码：HumanEval、Code QnA/Generation/Summary/Translation (HF)
- 生产 trace：**Azure Serverless/Inference Trace**、**Kimi 1 小时生产 trace**（23608 请求）、**DeepSeek-V3 生产 trace**（agentic ReAct）、Microsoft Azure coding/conversation trace
- 合成：Poisson 合成到达（input/output token 均匀/指数）、vLLM random / sonnet poems、LLMPerf synthetic chat N(200,40)/N(100,10)
- Agentic/多步：ReAct、Tree-of-Thought (GSM-8K)、BFCL、LATS/MCTS、LLM-as-Judge（DLPM）

> 这一支无数据库表、无写回，"请求"即输入。本项目与之同源（文本主线可比）。

**A.1.2 代价估计/查询优化文献（用真数据库 benchmark，但不含 AI 算子）**

Heinrich / CONCERTO / GRACEFUL / COSTREAM / Pathak 用经典 OLAP：**TPC-H**、**TPC-DS**、**JOB / JOB-Light（IMDB）**、IMDB (8 表)、Baseball (25 表)、SSB；Heinrich 训练集 20 真实库 × 10000 queries/db；GRACEFUL genome 数据集（zero-shot）；自建 UDF benchmark（20 真实库、90000+ 查询）；执行引擎 ClickHouse、PostgreSQL + pg_hint_plan。

**A.1.3 数据库 AI 系统文献（真正的"数据库表 + AI 算子"）**

| 系统 | 数据 |
|---|---|
| LOTUS / Abacus | FEVER (claims)、BioDEX (docs)、SciFact、HellaSwag、ArXiv 分析 |
| Galois | Flight / Geo / World / Scholar / Movies / Presidents / Premier / Fortune / Geo-Test（真实 DB 表 + LLM-as-storage） |
| Smart | Flight 表（SQL+ML 谓词推理重写） |
| SmartLite / LEADS / InferDB | NYC-rides (1.5M)、Pollution (106M)、Fraud (284k)、Hits (143k)、Digits/MNIST、Rice、Avazu(40M) |
| SemBench | filter / join / map / rank / classify 五类 × text/image/audio × MMQA |

**A.1.4 数据库厂商基准**

| 厂商 | 数据 |
|---|---|
| Snowflake Cortex | TPC-DS 100TB+10TB（Samples，派生非官方）、Cortex Analyst 150 题 BI 基准、Spider/BIRD、MTEB/CLEF/MIRACL、TRAIL/GAIA、MMLU/HumanEval/GSM8K |
| BigQuery ML | 合成 per-row text（500 in + 50 out）、合成 embedding（50 tok/row）、Faraday 客户负载（12.6M embeddings/45 min）、bigquery-public-data |
| Oracle AI Vector Search | SIFT (1M 128-d)、GIST (1M 968-d)、Fashion-MNIST、Llama 3.2 3B on OCI A10、LLMPerf synthetic、semantic-cache demo、ONNX All-MiniLM-L12-v2 |
| PostgresML | Kaggle Flight Delay、Amazon US Reviews Video DVD (5,069,140 行)、HuggingFace 模型、pgbench |
| pgai / pgvector / pgvectorscale | 50M Cohere (768-d)、LAION 5M、ann-benchmarks glove/sift、OpenAI ada-002/3-small/3-large (1536-d) |

**A.1.5 向量库 benchmark（独立一支）**

ANN-benchmarks（glove/sift/fashion-mnist/gist）、Big-ANN NeurIPS'21&'23（SIFT1B、Deep1B、BIGANN-1B、SimSearchNet++-1B、Turing-ANNS-1B、SPACEV-1B、Yandex DEEP/Text-to-Image-1B、YFCC100M 10M CLIP）、BigVectorBench（arXiv+PubMed/ImageNet/squad_v2/img-wikipedia/librispeech × 十 workload）、VectorDBBench（GIST/dbpedia-openai-1M/MS MARCO/HotpotQA）。

### A.2 对项目 AI_COMPLETE 的可用性分析

AI_COMPLETE 是**生成式**（输入文本 → LLM 生成 → 写回）。SemBench/LOTUS 算子按输出形态：

| 算子 | 输出形态 | 对应 AI 算子 | AI_COMPLETE 可用 |
|---|---|---|---|
| filter | 布尔 | AI_FILTER | ❌ |
| classify | 类别标签 | AI_CLASSIFY | ❌ |
| join | 匹配/不匹配 | AI_JOIN | ❌ |
| rank / top-k | 打分/排序 | LLM-as-judge | ❌ |
| aggregate | 计数/摘要 | 混合 | 🟡 |
| **map** | **行 → 变换文本** | **AI_COMPLETE** | ✅ 形态匹配 |
| reduce | 多文档 → 摘要 | 近亲 | 🟡 |

**结论**：只有 `map`（及部分 `reduce`）形态对得上；SemBench/LOTUS 的大头（filter/classify/join/rank）是 AI_CLASSIFY/AI_FILTER，不是 AI_COMPLETE。

即使 `map` 形态匹配，两个坑：

1. **输出短且结构化**——LOTUS map（BioDEX 抽取→JSON、FEVER 改写）输出短而均匀；而本项目调度问题（token-budget batching、尾延迟、HOL-age、goodput、prefix cache 压力）需要**变长长输出**压测。ShareGPT/BurstGPT 在调度压测上比 LOTUS map **更强**。
2. **模型与质量口径**——LOTUS/SemBench 用 GPT-4 via API（SemBench 一次 $9935 / 18 天）；本项目用本地 Qwen2.5-1.5B/7B。任务能跑（调度只看请求形态），但 recall/accuracy 不可与 LOTUS 同比。

### A.3 数据/代码可用性（已核实，官方来源）

- **SemBench**：官方站 [sembench.github.io/SemBench](https://sembench.github.io/SemBench/)，代码与数据在 [github.com/SemBench](https://github.com/SemBench)（Cornell/Google/UT Austin，定位"semantic query engine 的 TPC-H"）。**厂商来源**。
- **LOTUS**：开源仓库 [github.com/lotus-data/lotus](https://github.com/lotus-data/lotus)（Stanford/Berkeley），语义算子 map/reduce/filter 等，跑在 pandas DataFrame 上。**官方文档+源码**。
- 底层数据集 FEVER / BioDEX / SciFact / HellaSwag 均为公开 benchmark。

### A.4 推荐与缺口登记

| 目标 | 做法 |
|---|---|
| 加"数据库 AI 算子"定位味 | 采纳**一个 LOTUS `map` 任务**（BioDEX 抽取 或 ArXiv 摘要）重排版成 `df["prompt"]` 跑 AI_COMPLETE 写回 PG 表。低成本高定位价值，**补充不替代** ShareGPT。 |
| 压测调度策略 | 继续 ShareGPT/BurstGPT/agent-trace（变长长输出是调度压力源）。 |
| 多模态泛化 | SemBench image + 向量 benchmark（SIFT/LAION/Cohere 50M）更直接可用。 |

**缺口登记**：本项目目前 workload 偏"LLM serving + 写回"，**未用过 SemBench/LOTUS 式"数据库表上的语义算子"任务**。若开题/论文要把课题定位成"数据库 AI 算子"而不仅是"LLM 推理上游调度"，建议补一个 LOTUS map 任务做定位佐证（P2，多模态/数据库味优先级，不进调度主实验）。待用户确认后登记到 `experiments/plans/experiment_status_and_gaps.md`。

> 本附录的 workload 清单为**文献事实/厂商来源**；AI_COMPLETE 可用性判定为基于算子输出形态的**合理推断**；数据/代码可用性为**官方来源**（已 web 核实）。

---

## 附录 B：数据库厂商 AI 算子测试方法（2026-07-31 补）

回答"别的数据库厂商怎么测自己的 AI 算子"——逐家方法论 + 跨厂商共识 + 对本项目的启示。来自一个后台工作流（6 家厂商 × 测试方法论 web 抽取 + 综合）加一个 PolarDB 专项核查 agent。

**覆盖 8 家**：PolarDB Lakebase、OceanBase、Snowflake Cortex、BigQuery ML、Oracle AI Vector Search、PostgresML、pgai/pgvector/pgvectorscale、Databricks Lakehouse AI。

**证据分级**：官方文档 / 工程博客 / 基准规范 / 论文 / 营销——每条标注。厂商自报数字一律视作**厂商来源**，非独立第三方。

### B.1 PolarDB Lakebase（与本项目同栈，单独重点）

| 维度 | 核实结论 |
|---|---|
| 是否有 AI 算子 | ✅ 属实，但**三条平行路径**：(1) `polar_ai` SQL 扩展（`ai_text_embedding` 等）= **外挂 HTTP 调百炼/DashScope，不跑在 Daft 上**；(2) **Daft on Ray DataFrame AI 函数**（`embed_text`/`classify_image`/`prompt`）= 跑在 Daft on Ray ✅；(3) `AI_SEARCH()` 模型算子化（2026 新）= 未明说是否走 Daft |
| 是否真用 Daft on Ray | ✅ 属实——且是**开源 Eventual-Inc/Daft 本身，不是阿里自研 fork**：Swordfish 引擎、`@daft.cls(gpus=N, max_concurrency=M)`、`daft.set_runner_ray()` 全部匹配开源 Daft。PolarDB Lakebase 集成的是**和本项目同一个开源 Daft** |
| 命名陷阱 | PolarDB **没有 `AI_COMPLETE`**（那是 Snowflake 的）；等价物是 `polar_ai.*`（SQL）+ Daft `prompt()`（DataFrame） |
| 开源 | PolarDB-PG 仓库 [polardb/PolarDB-for-PostgreSQL](https://github.com/polardb/PolarDB-for-PostgreSQL)（Apache 2.0）；但 `polar_ai` 扩展源码可见性未确认；Daft on Ray 代码 = 开源 Daft，不在 PolarDB 仓库 |
| 怎么测 | [性能测试报告](https://help.aliyun.com/zh/polardb/polardb-for-postgresql/daft-performance-benchmark)：Daft vs Ray Data vs Spark，8 worker × (1×GPU 24GB + 4vCPU + 16GB)；workload = Audio 113,800 条 / Doc Embedding 10,000 PDF / Image 803,580 图 / Video 1,000 视频 + TPC-H SF=100/1000；指标 = **仅端到端 wall time**；vs Ray Data 2.2–7.6×、vs Spark 4.0–18.4× |
| vendor 缺陷 | **warm-up/重复次数未说明**；TPC-H 明确声明"不完全合规、不可与已发布 TPC-H 比较"；**数据/脚本不公开**（方法论公开、产品闭源云） |
| 相关度 | **4.5/5**（迄今最贴近本项目技术栈的工业产品） |

**对项目的核心含义（双刃）**：
- ✅ **工业正当性顶级背书**——阿里云旗舰数据库把**同一个开源 Daft on Ray** 当作"AI 原生数据库核心计算层"在卖。本项目 `code/AGENTS.md` 的 `@daft.cls` 编码规范被工业验证。
- ⚠️ **新颖性门槛拉高**——PolarDB 的卖点（CPU/GPU 异构调度、morsel+backpressure、util 60→80%）**逐条对应项目研究方向**。"Daft on Ray + 异构调度 + 背压"已是产品，项目**不能**把这一层当新颖性。
- 🎯 **新颖性边界因此切得很清**：PolarDB 的背压是**纯数据流背压**（下游慢→减缓上游），**不观测 vLLM 内部状态**（KV/prefix/queue）。项目能占的切片 = **模型服务状态感知的请求成形 + 闭源产品未公开的上游调度策略开放消融**。
- ❓ **Scoop 待确认**：未见阿里云/Daft 团队声称做"模型服务状态感知调度"的**研究论文**（产品闭源、调度细节未披露）；但**未穷尽搜 VLDB/SIGMOD/ICDE 学术文献**，正式定稿新颖性前需补一轮专门学术检索。

### B.2 OceanBase：SQL AI Function 与 Lakebase Daft-on-Ray 必须分轨

OceanBase 的公开材料覆盖了两条不同执行链路，不能合并成一个“OceanBase AI benchmark”：

| 产品面 | 公开场景与链路 | 公开指标 / benchmark | 可复现性与本项目用法 |
|---|---|---|---|
| **OceanBase Database SQL AI Function** | `AI_COMPLETE` 做生成、摘要、问答和分类；`AI_EMBED` 做文本向量化；`AI_RERANK` 做 RAG 重排。数据库注册 OpenAI-compatible endpoint，由 SQL 表达式逐行或按表调用模型 | 官方语法、quick start 和批量表列示例完整，但截至 2026-08-05 **未找到**公开的 AI Function 性能报告、固定 workload、硬件、warm-up/repeats 或 raw data | 获得可部署 VM 后接同一 vLLM endpoint，按 SQuAD bounded-output 与 ShareGPT 两轨测试；独立校准 session/concurrency，比较 correct rows/s、tokens/s、JCT、TTFT/p95/p99、错误/截断、EM/F1 和成本。功能示例不能当性能结果 |
| **OceanBase Cloud AI Services / MaaS** | API 或 SQL 调用托管模型，按供应商通道和配额管理 | 官方控制台公开 **24h success rate、TTFT、token output rate**，并支持 token/request 的月、日、分钟配额与分钟限流 | 这些是用户可见模型服务指标，不是数据库算子 E2E benchmark；本项目应采用同名指标，并另加 DB fetch、排队、写回和 exactly-once |
| **OceanBase AI Database / Lakebase / DataStudio** | 多模表与 Volumes → Daft on Ray 做 splitting、frame extraction、embedding、tagging → AI 列回填、lineage、subset 发布；Ray actor 常驻模型，按 micro-partition 动态调度 CPU/GPU stage | 官方材料公开架构和动机，但**没有**可固定 commit 的 OceanBase runner、数据集、硬件、数值结果、warm-up/repeats 或 raw logs；页面提到 Daft TPC-H 更快时也明确标为 vendor-reported、需独立验证 | 只能作为工业架构/capability evidence。Daft/Ray 数字 baseline 仍须运行 Daft 与 Ray Data 各自官方代码并在同机同模型复现，不能把 OceanBase 产品博客冒充性能对照 |

**相邻 benchmark 不得替代 AI 算子 benchmark**：

- OceanBase 的 Sysbench 测 QPS/latency，TPC-H 测 QphH@size/response time；它们验证 OLTP/OLAP 数据库引擎，不调用 `AI_COMPLETE`/`AI_EMBED`，不能支撑 AI 算子性能结论。
- OceanBase 的 VectorDBBench 使用公开 `Performance768D1M` 等 case，按并发扫描最高 QPS，并考察 HNSW/IVF 参数；它验证**已有向量的检索侧**，可用于写回后的 retrieval closure，却不测 embedding 生成、模型服务排队或 Daft-on-Ray pipeline。
- OceanBase 官方 publication list 已登记 **IMLane: Composable Framework for Efficient AI Function Execution in Database Engine**（PVLDB 2026 accepted），与本课题高度相关；但本次检索未找到公开论文正文，故 workload、baseline、指标和结果全部标为 `pending-publication`，禁止从标题推断。

来源：[OceanBase AI Function](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975)、
[AI Function overview](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678978)、
[OceanBase Cloud AI Services release notes](https://en.oceanbase.com/docs/common-oceanbase-cloud-10000000003353421)、
[OceanBase DataStudio](https://en.oceanbase.com/blog/oceanbase-datastudio-unified-ai-data-production)、
[Lakebase architecture](https://en.oceanbase.com/blog/oceanbase-ai-database-lakebase-architecture)、
[OceanBase performance testing](https://en.oceanbase.com/docs/common-oceanbase-cloud-10000000002694815)、
[VectorDBBench guide](https://en.oceanbase.com/docs/common-oceanbase-database-10000000002164117)、
[OceanBase publications](https://github.com/oceanbase/publications)。

### B.3 其他 6 家逐家方法论

| 厂商 | 怎么测（一句话） | benchmark / workload | 指标 | 对照 baseline | 可复现性 |
|---|---|---|---|---|---|
| **Snowflake Cortex** | 最接近学术：SIGMOD Companion'26 论文（arXiv 2511.07663v3）做**三臂消融**（强 oracle-only / 动态 cascade / 便宜 proxy-only）+ 单变量扫描，**每查询 5 次取均值** | 6 HF 布尔分类集（NQ/BOOLQ/IMDB/SST2/QUORA/FARL）+ 8 HF entity-matching 集（ABTBUY/NASDAQ/ARXIX/EURLEX/NYT/CNN/AG NEWS/BIODEX）+ 自建 NYT 1000 篇 | F1/precision/recall + 执行时间 + **LLM 调用次数**（110000 vs 330）+ observed delegation rate | 三臂同系统对照；Cortex Analyst 单 prompt GPT-4o（营销） | 论文 public-data-**no-code**；被测栈闭源云；营销 70% 黑盒 |
| **BigQuery ML** | 官方只发"internal benchmarking"数字：6h 单 job × 固定 QPM 配额 × 固定 tokens/row，dynamic token batching 打满 | 合成 per-row（embedding 50 tok/row、文本 500 in+50 out）；客户负载 Faraday 12.6M embeddings/45min；bbc_news 教程 | 吞吐（rows/6h）、可靠性、$ | 旧版自比、Vertex AI PT、Gemini Flash vs Pro | 多数 closed-cloud-**blackbox**；唯一 public-data-public-code 是 bbc_news 教程 |
| **Oracle AI Vector Search** | LLMPerf + ann-benchmarks 数据集 + 4 场景正态分布 token 模型 | SIFT1M/GIST1M/Fashion-MNIST；Llama 3.2 3B on OCI A10；LLMPerf synthetic chat N(200,40)/N(100,10)；semantic-cache demo | TTFT/TPOT/Latency/Throughput + MBU + MFU；recall@k；QPS | 匿名竞品 O-DB-VDB-1/2/3、O-VDB、O-DOCDB（**反模式**：匿名 + 承认对手 2–4× CPU 线程） | 协议公开（LLMPerf）+ 公开数据；但匿名 baseline 被社区批评；**自承 LLMPerf 把 tokenization 编码时间误算进 tokens/s** |
| **PostgresML** | 严谨度两极分化：唯一严谨的 Scaling 篇用 pgbench + EC2 机型 + 100+ 配置扫描 + 公开 raw CSV；其余 4 篇教学博客用 `psql \timing` 单次/25 次平均、无 warm-up | flights（航班延误）XGBoost；Amazon US Reviews 5M 行 embedding；HuggingFace 模型；pgbench | rows/s、predictions/s、p99（仅引用 OpenAI 的）、$ | Python+Flask+Redis（多跳，被 HN 批）、MindsDB（无 GPU）、OpenAI、Pinecone/Qdrant/Weaviate | Scaling 篇 public-data-public-code；其余 toy（2-文档 RAG） |
| **pgai/pgvector/pgvectorscale** | 全聚焦 read-side ANN：ANN-Benchmarks fork（修了多线程 QPS + warmup/test 分离：29k 预热 + 1000 disjoint 测试）画 recall@k vs QPS Pareto | 50M Cohere Wikipedia 768d；LAION 5M/100M；ann-benchmarks glove/sift；OpenAI ada-002/3-small | **recall@k vs QPS**（recall-defined throughput）+ p50/p95/p99 + $/month-for-target-QPS | Pinecone s1、Qdrant、pgvector HNSW/IVFFlat、pgvectorscale StreamingDiskANN、VectorChord、exact | 协议+数据+OSS 扩展公开（**最可复现**之一）；但 YDB/wasowski 第三方复现发现 harness/单节点竞争主导差距 |
| **Databricks Lakehouse AI** | 分层：(1) AI/Vector Search 最透明（Locust 逐步并发 + 二分搜索 max sustainable QPS + per-component 计时）；(2) LLM 推理指标框架最成熟（MosaicML TTFT/TPOT/Latency/Throughput+MBU+MFU）；(3) AI Functions 最不透明（10x/100x 营销） | 10亿向量 768d（Standard 320M / Storage-Opt 1B+）；2048/256 RAG summarization；512/64 静态批 | recall@10 + p50/p99；TTFT/TPOT/MBU/MFU；ann_time/embedding_gen_time/reranker_time/response_time 分解 | Standard AI Search、FasterTransformers+TensorRT-LLM、旧版自比 | 协议层公开（Locust notebook、benchmarking notebook、Eval Gauntlet PDF）；被测系统闭源云 |

### B.4 跨厂商共识与惯例

**benchmark 使用（核心发现）**：**数据库 AI 算子层目前没有任何被主流厂商采纳的现成标准 benchmark。** TPC-DS/TPC-H 仅作数据仓库语境或 Marketplace 数据，**从未用于 AI 算子基准**；MTEB 只被 PostgresML 外部引用、Databricks 明确不用；ann-benchmarks 的标准 harness 无厂商直接跑（但数据集 SIFT/GIST/Fashion-MNIST 被 Oracle/pgai 私有协议复用）；MLPerf、Spider/BIRD 全无人用。厂商实际复用的"准标准"来自三个相邻社区：NLP/HuggingFace 公开集、IR/向量检索公开语料、经典 DB-ML 公开数据。**→ 本项目没有现成 DB-AI benchmark 可套，必须自定干净合同（§7.5 已做）+ 引用上述公开数据集作 workload 锚点。**

**指标惯例**：
- 吞吐口径分裂：LLM 生成算子用 token 级（tokens/s、output tok/s across concurrent、combined in+out tok/s）；批处理/embedding 用行级（rows/s、QPS@recall、req/s）
- **LLM 推理成熟指标分解（Oracle + Databricks 共识，已成行业词汇）**：TTFT、TPOT、`Latency = TTFT + TPOT×n_out`、`Throughput = output tok/s across concurrent`、MBU（decode-bound 利用率）、MFU。**本项目已用 MFU，缺 TTFT/TPOT/MBU**
- 质量口径：分类用 ground-truth F1/precision/recall（Snowflake）；向量检索用 recall@k（Oracle/pgai/Databricks 共识）；生成质量用公开学术集（Databricks Mosaic Gauntlet）；均不主观打分
- 成本口径跨厂商一致：$/month、$/M-rows、$/M-tokens、$/vector、vectors-per-$
- 两个机制解释力强的辅助指标：**LLM 调用次数**（Snowflake 110000 vs 330）、**row-level success rate**（BigQuery >99.99%）
- GPU 利用率口径最不成熟：PostgresML 用 htop+nvidia-smi 肉眼、Oracle 自承 LLMPerf 把 tokenization 编进 tokens/s

**workload 惯例**（三种，可信度不同）：(1) 真实公开数据集（最可信）；(2) 合成固定形状（Databricks 2048/256、Oracle 4 场景 N(mean,std)、BigQuery 固定 tokens/row）——参数化清晰但非真实；(3) 私有客户负载（仅动机）。**新兴共识：按 (prompt_len, response_len) 参数化**（Oracle 的 Random-Length/Chat/Generation-Heavy/RAG 四场景）。规模：向量侧 10M/100M/1B；生成/embedding 侧 1M–10M 行。

**baseline 惯例**：两种公认做法——(1) 命名的同协议外部竞品（pgai 的 Pinecone/Qdrant、PostgresML 的 OpenAI/MindsDB）；(2) 多臂同系统对照（Snowflake 三臂、PostgresML 1/2/5 副本自比）。**反模式**（被社区批评）：Oracle 匿名+不对齐资源、PostgresML Python baseline 多跳、"自比旧版 + 无外部锚点"营销（BigQuery 100x/30x、Snowflake 70%、Databricks 10x/100x）。共识：baseline 必须共享数据读取/写回路径 + 对齐资源。

**可复现性缺口（5 层）**：① 被测系统几乎全闭源云黑盒（Snowflake/BigQuery/Oracle/Databricks）；② public-data-no-code（放数据不放 harness）；③ 营销数字无协议；④ 匿名+不对齐资源 baseline；⑤ 单次计时无 warm-up/repeats/CV。**本项目采用开源 vLLM+Ray+Daft 栈 + 公开数据 + 全量 CSV/manifest/raw traces，可在“完整执行链路证据开放”这一维度形成明确优势；但不能把这一点泛化成对每家产品整体可复现性的绝对排名。**

**独特空白（最重要）**：**没有任何厂商公开 benchmark "写入侧/上游调度 pipeline"**（embedding 生成吞吐、ingestion、writeback、批合并、提交节奏）——pgai 只测并发写回正确性不测吞吐，其余完全忽略。**本项目的主战场（数据组织 + 提交控制 + 写回）正好填这个洞。**

### B.5 对本项目的具体启示

**应采纳（让评估被社区认可）**：
1. **公开数据集作 workload 锚点**：文本分类/过滤用 Snowflake HF 集（NQ/BOOLQ/IMDB/SST2/QUORA/FARL + entity-matching）；embedding/向量用 pgai Cohere 50M + LAION 5M/100M 或 Oracle SIFT/GIST/Fashion-MNIST；文本生成用 BigQuery bbc_news 或 Databricks 2048/256 形状——多家交叉复用的"准标准"。
2. **补 TTFT / TPOT / MBU**（Oracle + Databricks 共识），与厂商级 LLM 服务指标口径对齐（与附录 A 的 P0 缺口一致）。
3. **正态分布 token workload 模型 + 4 场景分类**（Oracle 的 Random-Length/Chat/Generation-Heavy/RAG）作 AI_COMPLETE workload 生成网格。
4. **recall-defined throughput**（QPS@95% 或 99% recall，单线程 + disjoint warmup/test split）+ ANN-Benchmarks 协议框架——裸 QPS 社区无意义。
5. **LLM 调用次数**作独立于 wall-clock 的成本指标（解释 token-budget/批合并减请求数的机制）。
6. **单变量扫描**刻画机制边界（Snowflake selectivity 0.1→1.0；本项目 token-budget/active-work/K_max 容量曲线固定其余只扫一个）。
7. **成本归一化**（$/M-tokens、J/1k-tok、vectors-per-$）——本项目已有 J/1k-tok，补 $/M-tokens。
8. **per-component timing 分解**对齐 Databricks（ann_time/embedding_gen_time/reranker_time）——本项目已有 db_fetch/organizer/submit/fanin/bounded_wait 同构。
9. **Ray LLMPerf / Locust / pgbench** 作可引用的社区认可 bounded 负载生成器（feeding 门禁的 bounded HTTP baseline 显式对齐 LLMPerf/Locust：逐步并发 + 二分搜索 max sustainable QPS + Little's Law）。

**应强化（项目已有的优势）**：
10. §7.5 的"1 warmup + 3 formal repeats + CV 控制 + feeding-saturation ≥95% bounded 门禁"**已超过除 Snowflake 论文外所有厂商的公开协议**——在报告中明确声明此协议层级。
11. **三臂对照设计**（Snowflake 强 oracle / 动态 / 便宜 static）直接对应本项目"固定静态 credit 是强 baseline、动态需显著优于同上限静态才过 5% 门禁"——除 Snowflake 外无厂商做到。
12. **测量卫生**：tokens/s 计算明确排除 tokenization/编码时间（从 vLLM Prometheus prompt_tokens_total+generation_tokens_total 取）——避免重蹈 Oracle 公开 bug。

**应规避（被社区批评的反模式）**：
13. 禁止匿名 baseline（Oracle O-DB-VDB 反模式）——baseline 必须命名 + 对齐资源。
14. 禁止"自比旧版 + 无外部锚点"营销数字（BigQuery/Snowflake/Databricks 反模式）——AGENTS.md §6 已编码。
15. 禁止单次 `psql \timing` 或 toy 2-文档 RAG（PostgresML 反模式）——必须 formal repeats + 真实规模。

**定位（项目独特贡献）**：
16. **"写入侧/上游调度 pipeline benchmark"是本附录审查的 8 家厂商共同留下的空白**——本项目主战场（数据组织 + 提交控制 + 写回）正填此洞，评估 framing 中明确点出。
17. **PolarDB Lakebase 是最近、最同栈的工业系统**——Related Work 必须点名，项目差异 = 模型服务状态感知 vs 通用数据流 backpressure + 闭源产品未公开的调度消融。
18. **OceanBase 提供第二个同栈工业架构证据，但不提供 Daft/Ray 数值 baseline**——采用其 success rate、TTFT、token output rate 和配额口径；性能排名只接受同机原生 runner。SQL AI Function 与 Lakebase Daft-on-Ray 分榜。

### B.6 不能声称

- **不引用任何厂商的提速倍数**（100x/30x/10x/100x/70%/4-10x/2.2-7.6x 全是闭源黑盒或匿名 baseline 营销）。跨厂商对比只能说"采用某厂商公开 tutorial 同款公开数据（如 bbc_news、Cohere 50M）与公开过程做可复现对照"，不能声称对标某厂商闭源数字。
- PolarDB 的 scoop 核查状态为"**未见研究论文证据**"，非"确定不存在"——正式定稿新颖性前需补 VLDB/SIGMOD/ICDE 阿里云 Daft 团队学术文献检索。
- 不得声称 OceanBase 已公开证明 Daft 优于 Ray Data/Spark；当前公开页只说明采用 Daft on Ray 的机制与产品链路。Sysbench、TPC-H、VectorDBBench 也不得替代 AI Function/Daft-on-Ray benchmark。
- IMLane 目前只有 OceanBase 官方 publications 的 accepted 条目，本次检索未找到公开论文正文；在正文可登记为 watchlist，不得杜撰其 workload、baseline、指标或性能数字。
- 本附录厂商方法论为**厂商来源/官方文档**（非独立第三方）；PolarDB 同栈与开源 Daft 关系为**官方文档 + 开源仓库交叉核实**的**本地事实**；新颖性边界（PolarDB 不观测 vLLM 状态）为基于其公开文档未提及的**合理推断**（闭源内部无法确证）。

---

## 9. 按论文与数据库系统拆分的指标矩阵及本项目对比合同（2026-08-04）

本节回答两个问题：已有工作究竟用什么指标评价数据库 AI 算子；本项目与它们比较时，哪些指标可以直接比较，哪些只能作功能或方法学对照。结论先行：**不能把不同模型、质量目标、资源和执行边界下的 wall time 放在一张表里比较**。本项目应采用“先保证语义/质量等价，再比较端到端性能；同时报告服务侧、上游调度侧和代价模型决策侧指标”的三层合同。

### 9.1 学术论文：真实场景、算子、指标与可比边界

下表的“场景”不是装饰信息，而是决定指标是否可迁移的前提。例如语义 join 主要受
候选对数量和模型调用费用支配，批量 AI_COMPLETE 则同时受输出长度、continuous
batching 和队列状态支配；两者即使都调用 LLM，也不能只按 wall time 横向排名。

#### 9.1.1 数据库 AI / semantic operator 论文

| 论文/系统 | 论文实际场景与数据 | AI 算子、执行方式和优化对象 | 论文采用的核心指标 | 本项目应如何比较 |
|---|---|---|---|---|
| **LOTUS（PVLDB 2025）** | FEVER 事实验证（1,000 claims）、BioDEX 生物医学药物事件文档（250 docs）、SciFact 科学证据排序、HellaSwag 常识排序和 arXiv 文档分析 | 声明式 semantic filter/join/aggregate/top-k/group-by/map；在 embedding、小模型、LLM、cascade、join/ranking 实现之间选物理算法，并用抽样保证准确率 | accuracy、RP@5/RP@10、nDCG、执行时间、模型调用次数；目标准确率和 failure probability | 只选择能映射到同一 filter/map/classify/rank 语义的查询；统一模型、prompt、数据和质量阈值后比较 JCT、调用数、token 与成本。LOTUS 主要通过**少调用/换实现**降成本，本项目主要研究**相同 work 如何组织和提交**，必须把这两类收益分开。 |
| **Galois（SIGMOD 2025）** | Flight/Geo/World/Scholar 等模型内知识表，以及 Movies/Presidents/Premier/Fortune 等把上下文放入 prompt 的小型表格构造任务；实验表规模较小 | 让 LLM 生成关系表，选择 Table-Scan、Key-Scan 等单查询内物理计划；每次模型调用基本是独立顺序 prompt | F1-Cell、Cardinality、Tuple Constraint、AVG-Score、token 数、执行时间、最优计划 pick rate | 仅在“从 LLM 生成同一张表”的语义下比较质量—token—时间。其场景不是数据库行批量送入外部服务，也没有并发、尾延迟或服务饱和实验，不能拿来证明 submission 调度优劣。 |
| **GaussML（ICDE 2024）** | openGauss 内执行分类、回归、聚类等传统 ML 训练/推理，与 Apache MADlib 比较；公开材料对具体数据集和模型披露不足 | 将 20+ 传统 ML 算法做成数据库原生算子，优化 SQL+ML 计划并用 SIMD、预取和分布式并行加速 | 执行时间、speedup；质量和方差披露不足 | 作为“传统 ML 原生入库”路线参考，不与 LLM/VLM 外部 serving 数值排名。若复现必须补模型质量、数据集、训练/推理边界、资源和完整 E2E；不能直接引用 2–6× 到本项目。 |
| **Smart（VLDB Journal 2025）** | JOB、TPC-H、SSB、Flight 查询中含决策树/SVM 等可分析 ML 谓词，例如 `WHERE classifier(x)=...` | 从传统分类器决策边界生成 sound SQL 过滤条件，配合渐进推理和成本选择，减少真正调用模型的行 | 查询执行时间、speedup；推理成本通常是可测/固定参数 | 用作“可符号化传统模型 + 静态 per-call cost”的边界对照。本项目的 LLM/embedding 决策边界不可改写，且 service cost 随输出长度和共享状态变化；只能比较优化思想，不能直接跑同一任务。 |
| **SmartLite（PVLDB 2024）** | 资源受限 IoT/edge 上同时运行中小型 DNN；公开摘要没有充分给出具体数据集、模型和精度损失 | 把量化/剪枝后的 DNN 权重存成 DB 表，用表查找替代部分张量计算，与 TorchServe 比较 | 内存占用、推理速度/加速比；公开材料缺少充分 accuracy/F1 | 只作为 edge、压缩和 in-database execution 的资源—速度案例。若没有相同模型质量与设备，不能与双 4090 的 LLM/VLM 外部链路比较，也不能把内存节省解释为算子质量更好。 |
| **InferDB（PVLDB 2024）** | NYC rides、Pollution 回归；Fraud、Hits 二分类；MNIST/Rice 多分类，共 6 个结构化/低维任务 | 以有监督离散化把“预处理+模型”近似成 embedding key，再用 PostgreSQL 索引查找聚合预测；本质上以索引替代在线模型推理 | 回归 RMSLE；分类 F1/Recall/Precision；推理延迟、训练/索引构建时间、存储大小、5-run 标准差 | 对 AI_CLASSIFY 可复用 F1/Recall/Precision + latency，但任务必须是同一标签预测。它会改变推理算法和近似质量；本项目固定模型的执行链路不能只按其毫秒延迟比较。索引构建与在线执行应分表。 |
| **LEADS（PVLDB 2024）** | Payment、Credit、Census、Diabetes、Avazu（最高约 40M rows）上的结构化二分类；50-query SQL workload 用 WHERE 条件定义不同 subdataset | SQL-aware MoE gating 根据谓词激活专家切片；比较 PostgreSQL 内推理与导出到外部 runtime | Workload-AUC、Worst-AUC、FLOPs、response time；数据库内/外执行与模块消融 | 借鉴“平均质量 + 最坏子 workload 质量”与 matched semantics 消融。本项目可把 query predicate 换成 token/frame work 特征，但 LEADS 只覆盖结构化分类，不支持生成式输出或图像字节流水线。 |
| **NeurDB（CIDR 2025）** | Avazu CTR 与 UCI Diabetes AI analytics；YCSB、STATS、TPC-C 用于 learned DB components 和数据/负载漂移 | 数据库内 training/inference/fine-tuning/model-selection 算子、流式数据协议和增量更新；研究整个 AI 生命周期 | E2E latency、训练吞吐、loss/漂移恢复曲线、事务吞吐 | 只作“数据/负载漂移 + 分阶段测量”的系统愿景参考。本项目当前是固定模型外部推理，不应把训练 loss、事务 TPS 混进 AI_COMPLETE/EMBED/CLASSIFY 主表。 |
| **Cortex AISQL（SIGMOD 2026）** | Snowflake 2025 年生产 AI SQL workload，以及 Natural Questions 等分类/问答任务；交互与批量 AISQL 都通过独立 Cortex 多租户模型服务 | EMBED/COMPLETE/FILTER/CLASSIFY/JOIN/AGG；查询重排先缩小调用行数，并用 8B proxy→70B oracle 级联 | F1/precision/recall、执行时间、吞吐、模型调用数、delegation rate、推理 work/cost | 是强相关工业对照。相同任务上比较质量达标后的 calls/tokens/JCT；但谓词上拉和模型级联会改变 work，本项目 admission 不改变模型语义。应先单列逻辑 work reduction，再比较同 work 执行效率。 |
| **Palimpzest（CIDR 2025）** | Real Estate Search：100 个房源、每项 3 张图片、23 个正例；按用户条件从文字+图像中筛选房源 | 声明式 AI pipeline 枚举模型、prompt、context reduction、代码合成和 filter pushdown；在约 5% workload 上跑 sentinel plans | runtime、货币成本、F1，以及 quality-cost-time Pareto | 可作为本项目多模态 classify/filter 的计划选择参考。若真正比较，需统一模型/API、输入和质量；否则它的商业 API Pareto 与本地固定 GPU throughput 只可并列展示，不能相除。 |
| **Abacus（PVLDB 2026）** | BioDEX 生物医学事件抽取（250 test）、CUAD 法律合同条款抽取（100 test）、MMQA 多模态问答（100 test），每组 10 次试验 | 从约 3,000 个 semantic operator 物理实现中搜索模型选择、Mixture-of-Agents、缩减上下文、critique/refine、join 等 Pareto 计划；5–10 个 validation examples 可初始化估计 | 任务 quality、货币成本、latency、约束满足、验证样本预算与重复方差 | 借鉴“约束下选择”和小样本 profile，而非照搬其全局计划搜索。项目对应的是固定资源下选择 budget/active-work/route/submit；主指标应是 selected JCT/SLO goodput、regret 和约束违反率。 |
| **SemBench（PVLDB 2026）** | Movies、E-Commerce、Cars、Wildlife、MMQA 五类真实语义查询场景，覆盖 text/image/audio，55 个 filter/join/map/rank/classify 查询 | 在统一 workload 上画像 LOTUS、Palimpzest、ThalamusDB、BigQuery；不同系统可使用不同内部优化和模型 | quality、execution time、money、memory、scaling、model calls、timeout/rate-limit/failure；5-run 方差 | 最适合跨数据库 AI 系统做功能与 quality-cost-time 对照。若纳入本项目，先选支持的 classify/map/filter 子集并固定模型；再额外记录 TTFT/TPOT、Ray/vLLM 队列、active work、GPU/CPU 和写回。不同默认模型的原榜只作系统画像。 |

#### 9.1.2 推理服务与代价估计论文：场景不能和数据库算子论文混读

| 论文/系统 | 论文实际场景与数据 | 主要指标 | 对本项目的正确用途 |
|---|---|---|---|
| **Clipper（NSDI 2017）** | MNIST、CIFAR-10、ImageNet 图像分类和 TIMIT 语音识别；feed-forward DNN/SVM/HMM，显式 per-query latency SLO | QPS、mean/P99 latency、top-1/top-5 error；predict/queue/network 分段 | AIMD 与延迟分段的方法来源，但其一次前向、原子 batch 和较稳定最优 batch 假设不适用于自回归 continuous batching。可在 AI_CLASSIFY 上复用指标，不能直接迁移控制变量。 |
| **Orca（OSDI 2022）** | 合成在线生成 trace：input 32–512 tokens、output cap 1–128、Poisson arrivals；最大模型到 GPT-3 175B | req/s、按输出 token 归一化的 median latency | 用于解释 iteration-level scheduling 的服务引擎背景。它没有数据库 source/sink、真实 trace、P99 或 token throughput，不能作为本项目完整 E2E baseline。 |
| **vLLM（SOSP 2023）** | ShareGPT 对话、Alpaca 短指令、WMT16 翻译共享前缀；OPT/LLaMA，Poisson arrivals | normalized latency、可持续 request rate、batched requests、KV memory saving | 是部署平台与 direct service ceiling。项目应复用相同 request trace 报 tokens/s、TTFT/TPOT/E2E tail 和 capacity efficiency；不能声称上游策略改进 PagedAttention。 |
| **Sarathi-Serve（OSDI 2024）** | OpenChat-ShareGPT4 对话（prompt 中位约 1,730 tokens）和 arXiv summarization（约 7,059），长 prefill 在线服务 | 满足 P99 TBT SLO 的最大 QPS、P50 TTFT、P99 TBT、strict/relaxed SLO capacity | 支撑 AI_COMPLETE 的长 prompt/SLO 指标；chunked prefill 属 vLLM 内部机制，不能作为上游数据组织实现。短 prompt classify/embed 场景优势可能消失。 |
| **DistServe（OSDI 2024）** | ShareGPT chatbot、HumanEval code completion、LongBench summarization；多 GPU 在线自回归 serving | TTFT+TPOT SLO attainment、per-GPU goodput、SLO Scale、placement solve time | 只对 AI_COMPLETE 在线/SLO 轨道相关。prefill/decode 分离不适用于 AI_EMBED/CLASSIFY，且本项目固定 2×4090、不修改 vLLM，不能照搬其集群部署收益。 |
| **SGLang（NeurIPS 2024）** | 12 类多调用 LM programs：few-shot、ReAct、Tree/Skeleton-of-Thought、LLM judge、JSON、multi-turn chat、RAG，以及 LLaVA 图像/ActivityNet 视频 | programs/s、单程序 latency、prefix-cache hit rate、调度开销 | 为 prefix-aware 组织提供机制依据；只有数据库 workload 真有固定 system prompt/精确 token 前缀时才可比较。AI_EMBED 图像 forward 无同类 KV prefix 复用。 |
| **VTC（OSDI 2024）** | 合成多客户端与 LMSYS Chatbot Arena trace，多个 tenant 共享同一 LLM 服务 | service disparity/virtual-token counter、公平吞吐、请求延迟；含 prediction/oracle 对照 | 是多 job work accounting 的算法 baseline，不是数据库产品 baseline。项目应按真实 completion usage 校正预测 work，并同时报 Jain、service disparity、per-job JCT/P99。 |
| **Heinrich et al.（SIGMOD 2025）** | JOB-Light/IMDB join ordering，Baseball/IMDB/TPC-H access path，三数据集物理 join 算子选择 | Q-error、Spearman/ranking、pick rate、surpassed plans、selected runtime、最大高估/低估 | 直接支撑“预测误差不等于决策质量”。本项目把候选计划换成 token budget/active-work/route 配置，必须报告排序与 oracle regret。 |
| **GRACEFUL（ICDE 2025）** | 20 个数据库、90K+ 含 Python UDF 的 SPA queries；UDF 计算量、分支和 tuple cost 多档变化，并做 unseen database/UDF zero-shot | median/P95/P99 Q-error、zero-shot 误差、pull-up/push-down advisor speedup 和性能回退 | 方法上对应“黑盒算子 + 数据库基数 + 代码/输入特征”。项目没有可静态分析的 UDF CFG，但可把 prompt/frame work 与 endpoint state 作为特征；最终仍看调度决策收益。 |
| **COSTREAM（ICDE 2024）** | IoT edge→cloud streaming DAG，在 CPU/RAM/网络带宽/延迟异构硬件上做 seen/unseen query/hardware placement | throughput、E2E/per-operator latency、backpressure、成功/OOM、Q-error/分类准确率、placement speedup | 可借鉴物理量特征、未见硬件/workload 留出和“预测可运行性”；它不是 AI 算子，也不直接提供 LLM service cost 模型。 |
| **CONCERTO（arXiv 2024）** | ClickHouse 上 TPC-H/TPC-DS，包含 SIMD、并行 pipeline、资源竞争与动态执行；按 query template 留出 | mean/P50/P90/P95/P99/max Q-error、模型大小、4.2ms 级预测开销、消融 | 支撑“per-stage cost + concurrent resource competition + DAG aggregation”。本项目首版数据量不足，不应直接上 GNN；先验证简单解析+residual 是否已能降低 decision regret。 |

由两张表可见，相关论文至少分成五类：

1. **语义/逻辑优化论文**关注质量、模型调用数、token 或货币成本以及总时间；
2. **数据库内推理论文**关注单查询延迟、吞吐、内存、索引/训练开销；
3. **LLM 数据处理系统**开始报告质量—成本—延迟 Pareto，但很少公开上游 submission、服务内部排队、active work、写回和多 job 公平性；
4. **推理服务系统**关注 TTFT/TPOT、goodput、KV/显存和服务调度，但通常从请求进入 endpoint 才开始计时；
5. **代价估计论文**关注预测误差、排序和下游 plan/placement 决策，但其 SQL/UDF/streaming 场景不能自动代表可变长 LLM 请求。

本项目的区别不应表述为“我们比所有 AI 算子系统更快”，而应表述为：**在相同算子语义和模型质量下，研究它们普遍未拆开的外部执行链路——数据组织、提交控制、模型服务、fan-in 与写回——并给出可解释的调度机制指标。**

### 9.2 数据库/厂商系统：业务场景、执行边界、公开指标与合理对照

数据库产品要先按“谁拥有模型调用与调度”分类。数据库内置/扩展函数、云端托管 AI
SQL、单纯向量索引和用户自写 HTTP UDF 是四种不同系统；它们的业务名称可能都叫
`AI_EMBED`，但不能默认属于同一执行边界。

#### 9.2.1 可自托管、可争取同机同 endpoint 对照的系统

| 数据库/产品 | 典型场景（输入 → AI 算子 → 输出） | 执行边界与公开评价现状 | 本项目正确对比方式 |
|---|---|---|---|
| **Apache Doris 4.x** | 表中的评论/文档/文件引用 → `AI_GENERATE`、`AI_CLASSIFY`、`AI_EXTRACT`、`EMBED` → 文本、标签、结构化字段或向量；覆盖文本生成和多模态 embedding | 数据库 SQL 执行器调用 AI Resource；支持 local/OpenAI-compatible endpoint。官方资料以功能、语法和可部署性为主，缺少与本项目同 workload 的完整公开性能基准 | 首选同机产品 baseline：接同一 vLLM、同 manifest 和 source/sink，独立校准其并发；报 query JCT、tokens/rows/s、p95/p99、成功率、实际 calls/tokens 和数据库自身资源。没有质量与调用审计时不排名。 |
| **ClickHouse 26.6** | 分析表列 → `aiGenerate/aiClassify/aiExtract/aiEmbed` → 生成结果、标签、字段或向量，适合批量 enrichment 和入库前处理 | 官方 SQL AI functions 调 OpenAI-compatible/Ollama；部分功能较新/experimental。当前主要是 capability 证据而非标准 benchmark | 与 Doris 同合同测试，但必须冻结 feature flag、named collection、版本、重试和缓存；“能运行”只算 gate，不能以单条查询 latency 当系统上限。 |
| **StarRocks 4.1.1+** | 每行 prompt → `ai_query` → 文本/JSON，适合摘要、抽取和用 prompt 模拟分类 | 通用 OpenAI 风格 endpoint；没有独立的一等 classify/embed 语义，且存在 response cache、队列和并发配置 | 只进入 AI_COMPLETE/“prompt-emulated classify”分榜；关闭或固定 cache，记录 `llm_max_queue_size`/并发。不能与原生 embedding/classify 算子混称功能等价。 |
| **OceanBase CE 4.5.x** | SQL 文本列 → `AI_COMPLETE`/`AI_PROMPT` 生成，或 `AI_EMBED`/`AI_RERANK` → 文本向量/重排结果 | 数据库调用可配置模型服务；官方已确认文本算子，未确认图像 classify/embed。当前 AutoDL 普通容器在 observer 初始化阶段受系统条件阻塞 | 获得 VM/特权容器后，先做一行协议、N 行 exactly-once 和 cache 门禁，再接同一 vLLM 跑文本产品 baseline；当前只能写 `blocked`，不得用安装失败推断性能。 |
| **Oracle AI Database 26ai Free** | 表中文档/文本 → chainable `UTL_TO_GENERATE_TEXT`、`UTL_TO_EMBEDDING(S)`、summary/rerank → RAG/向量化/生成结果 | 数据库内链式函数可调用 OpenAI-compatible/vLLM；同时含 AI Vector Search。Free 版 CPU/RAM/数据量限制会影响 query 侧，但外部 GPU endpoint 可统一 | 文本生成/embedding 可做同 endpoint 对照；分开报告“生成/embedding 写入链路”和“ANN 检索 read-side”。Free 版资源限制必须显式列出，不外推企业版。 |
| **IBM Db2 12.1.5 Community** | 注册 external model 后，SQL 文本列 → `TEXT_GENERATION`/`TO_EMBEDDING` → 文本/向量 | 数据库拥有 SQL 调用路径，OPENAI provider 面向兼容 REST；公开资料以能力为主，Community 镜像是否完整包含功能仍需 gate | 同机只比较 AI_COMPLETE/文本 AI_EMBED；先核验版本、TLS、payload 和调用计数。未通过镜像功能门禁前只作候选。 |
| **SQL Server 2025 Developer** | 文本/文档 → `AI_GENERATE_CHUNKS` 切块 → `AI_GENERATE_EMBEDDINGS` → 向量，用于 RAG 索引构建 | 原生覆盖 embedding/chunk，不提供同等级 AI_COMPLETE；外部 endpoint 要求 HTTPS | 只进文本 embedding pipeline 分榜，并在 vLLM 前使用同一 TLS proxy；记录 chunk 数、embedding rows/s、JCT、失败与写回。不能补生成式产品 baseline。 |
| **DuckDB + `ai` community extension** | 本地分析表/文件 → `ai_complete/classify/embed/...` → 文本、标签、向量 | 扩展调用 OpenAI-compatible/Ollama/llama.cpp；调度 owner 是社区扩展而非 DuckDB core | 作为低安装成本 extension control；固定扩展版本/commit 和签名，记录扩展并发/重试。结果必须标“community extension”，不能写 DuckDB 原生内核领先/落后。 |
| **PostgreSQL + Timescale pgai 0.11.2** | PostgreSQL 行 → `ai.openai_chat_complete/embed` 或 Ollama 函数 → 文本/向量 | SQL 扩展到外部 API；仓库已经归档，是历史实现而非持续产品 | 可做历史 direct-SQL control，接同一 endpoint 并冻结最后版本；不投入大规模调参，也不作为长期产品 headline。 |
| **PostgresML 2.10** | 关系特征/文本 → `pgml.predict/transform/embed/rank` → 预测、文本或向量 | 模型在数据库侧加载，改变模型副本、GPU 内存和 scheduler owner；不是“数据库调用同一外部 vLLM” | 作为 in-database inference 机制对照单列。只有同模型/精度/资源才比较 latency/rows/s；否则主要比较部署边界、数据移动、质量和资源占用。 |

#### 9.2.2 托管数据库/数仓：产品场景与云端可观察指标

| 数据库/产品 | 典型场景与公开 workload | 公开主要指标/限制 | 本项目如何使用 |
|---|---|---|---|
| **PolarDB PostgreSQL Polar_AI** | SQL 文本 → `AI_CallModel`、文本生成/分类/embedding；面向表内批量 enrichment、检索索引构建 | 云 AI node/商业服务；公开资料以功能和调用配置为主 | 有账号时只做云端 query E2E、质量、成本、错误/配额；不要把 PolarDB-X 本地 RPM 当作同一产品。 |
| **PolarDB Lakebase + Daft on Ray** | `prompt/embed_text/classify_text/embed_image/classify_image`；官方 benchmark 还覆盖 113,800 音频转写、10,000 PDF embedding、803,580 图像分类和 1,000 视频目标检测 | 公开主指标是完整 job wall time，并与 Ray Data/Spark 对比；硬件为 8 workers×1 GPU，warm-up、重复和部分 pipeline 合同披露有限 | 是最接近本项目的同栈工业锚点。先复现公开 file/object workload，再跑 PostgreSQL database-operator track；只在同硬件、模型、输入表示和边界下排名。 |
| **Hologres** | 数仓表内 `ai_gen/AI_EMBED/ai_classify/ai_rank/...`；含 CLIP 图像 embedding，面向素材检索、分类和批量内容处理 | 托管 AI node，不公开内部 GPU/queue；主要可观察 query E2E、质量、成本、配额和错误 | 是图像 CLIP 与数据库 AI 算子较贴近的国产产品参照。有账号时用相同图片/标签测 quality-cost-time；不比较 MFU/PCIe。 |
| **AnalyticDB MySQL/PostgreSQL** | 数仓行 → generate/classify/embed 或 PAI-EAS/pgml 模型 → 文本、标签、向量 | 云模型与 endpoint 合同受控，产品线之间实现不同 | 只作云产品 capability 与 quality-cost-time 对照；不能把 MySQL、PG 两条产品线合并成一个速度数字。 |
| **Snowflake Cortex/AISQL** | 表、图片、文档 → `AI_COMPLETE/AI_CLASSIFY/AI_FILTER/AI_EMBED/...`；典型为评论分类、文档抽取、图片标签、语义 join/aggregation | 文档强调输入 token、类别描述与样例会影响成本和准确率；Cortex AISQL 论文报告 F1/precision/recall、时间、calls、delegation | 用于质量—成本—时间及模型级联/谓词重排对照；保存实际 tokens、credits、calls、row errors。托管后端不透明，不能与本地 2×4090 wall time 或 MFU 排名。 |
| **BigQuery AI/ML functions** | 百万级表行/对象表 → `AI.GENERATE*`、`AI.EMBED*`、`AI.CLASSIFY` 等 Vertex 模型调用；生成式函数公开说明约 1M–10M rows/6h job，容量随输入/输出 token 变化 | rows/job、rows/time、成功/row error、token 配额、动态 shared quota、BigQuery+模型两侧账单；公开说明采用动态 token-based batching | 借鉴大规模行级成功率、token-shaped workload 与 quota-aware reporting。有账号时单列云面板；模型、配额、region 和内部 batching 不可与本地 raw time 混排。 |
| **Databricks SQL AI Functions** | Lakehouse 表 → `ai_query/ai_gen/ai_classify/ai_extract/...` → 批量 enrichment；可接托管或公网兼容 endpoint | 可观察 SQL query time、model serving latency/cost、失败与 serverless 资源；WAN、AI Gateway 和云 scheduler 进入边界 | 可做“同公网 endpoint”的云端执行链路对照，但必须单列网络/region 和 serverless 开销；不与同机 native arm 排名。 |
| **TiDB Cloud / HeatWave / Aurora-Redshift / Azure PostgreSQL** | 分别面向 auto-embedding/vector search、生成/RAG、Bedrock/SageMaker 调用、Azure AI 生成/抽取/embedding | 均绑定特定托管模型、region、quota 和计费；self-managed 普通版本通常不含同一 AI SQL 能力 | 用作产品覆盖面、计费和可靠性证据。只有取得账号并冻结模型/region 后才报告 E2E/quality/cost；不把云服务合成一个“数据库 baseline”均值。 |

#### 9.2.3 向量 read-side 系统不是 embedding 生成 baseline

| 系统/场景 | 实际评价指标 | 与本项目的连接 |
|---|---|---|
| **pgvector/pgvectorscale、Oracle AI Vector Search、Milvus/DiskANN 类 ANN**：已有向量 → 建索引 → top-k 查询 | Recall@k、nDCG、QPS、p50/p95/p99、index build time、内存/磁盘、目标 recall 下成本 | 它们评价的是 AI_EMBED **之后**的检索质量和读取性能。本项目可把它们接在写回后做闭环，但不能用 ANN QPS 代替 embedding images/s、生成 JCT 或上游调度性能。 |

### 9.3 本项目的公平对比合同

#### 第一层：语义与资源等价门禁

任何性能表之前先冻结：数据与行数、算子语义、模型/checkpoint/精度、tokenizer 与预处理、prompt 模板、temperature/采样和最大输出、质量阈值、GPU/CPU/内存、endpoint 数、数据读取和写回路径、warm-up、重复次数与计时边界。生成算子还要同时报告实际输出 token 分布；分类报告 F1/precision/recall；embedding/检索报告 Recall@k 或 nDCG。**质量未对齐的吞吐不进入同一排名。**

#### 第二层：分组设置 baseline，避免跨层错比

- **服务上限**：直接/有界 HTTP → vLLM，只说明 endpoint ceiling，不是完整系统竞争者；
- **框架原生**：Daft native、Ray Data native，与本项目共享数据、模型、source/sink 和资源；
- **强静态策略**：固定 token/frame budget + 固定 active-work/K + fixed flush，是动态策略的主要反事实；
- **数据库/学术 AI 系统**：LOTUS、Palimpzest、Abacus、SemBench 等只在共同算子语义与共同模型上作质量—成本—时间比较；
- **闭源厂商**：只比较公开方法、指标覆盖和可复现性，不做本地 wall-time 排名。

#### 第三层：论文主表必须同时覆盖四组指标

| 组别 | 主指标 | 用途 |
|---|---|---|
| **端到端效果** | operator JCT；tokens/s（生成）或 images/rows/s（embedding/classify）；p50/p95/p99；SLO goodput | 回答策略是否真正改善用户可见结果 |
| **质量/正确性** | exact row count 与 exactly-once；F1/Recall@k/nDCG；失败率、重试率、缺失/重复行 | 防止靠少算、丢行或降低质量换速度 |
| **资源/成本** | GPU busy/MFU，CPU 利用率，峰值显存/内存，J/1k-token 或 J/image，$/M-token 或 $/M-row | 判断提速来自利用率提升还是额外资源 |
| **机制与控制面** | 实际 prompt/output work、batch work 分布、active work、Ray pending/pre-submit wait、vLLM running/waiting/KV、credit 持有时间、HOL、提交/调用次数 | 解释为何有效，并揭示队列从一个层级迁移到另一个层级 |

多 job 实验另报 Jain fairness、service disparity、各 job JCT/p99 与最大 SLO 违反率；不能只报聚合吞吐。

### 9.4 AI 算子代价估计：文献真正看重的指标

代价模型不应只回答“预测秒数准不准”，而应回答“它能否让优化器或调度器作出更好的选择”。相关文献形成了四层评价：

| 层级 | 建议指标 | 对本项目的含义 |
|---|---|---|
| **点预测** | MAE、RMSE、R²；median/p95/p99/max Q-error；分 stage 误差 | 评价 service time/JCT/remaining work 的绝对和乘法误差；MAPE 只作辅助，因为小真值会放大比例误差 |
| **不确定性** | 预测区间 coverage、平均宽度、分位数 calibration、tail underestimation rate | 可变输出长度和共享 serving 状态使点预测不够；尾部低估比同量级高估更危险 |
| **配置排序** | Spearman ρ、pairwise ranking accuracy、Top-k precision/recall、pick rate、surpassed plans | 代价模型首先要选对 token budget、active-work、路由或 flush 配置，而非复原每次秒数 |
| **下游决策** | selected JCT/throughput/SLO goodput、regret vs oracle、SLO 违反、性能回退率、决策稳定性、模型推理开销 | 这是最终主指标；高 R² 不能替代低 regret，较大点误差也可能不影响正确排序 |

传统 learned cost model 的系统性比较已经指出，Q-error 单指标不能代表计划选择质量；应同时报告 selected runtime、排序、pick rate、最大高估/低估和下游回退。GRACEFUL 在黑盒 UDF 上报告运行时间 Q-error，并用 pull-up 决策是否带来实际 speedup/回退；COSTREAM 同时预测 streaming dataflow 的吞吐、E2E/per-operator latency、backpressure 与可运行性，并在未见查询/硬件上验证；近期 LLM serving 工作则把输出长度分布、TTFT、SLO goodput 和 tail risk 纳入调度。因此，本项目首版 283 行模型的 R²=0.776、五种子平均 MAE=11.682s、MAPE=50.60% 只能证明“存在粗粒度容量信号”，不能证明可用于严格 SLO。

建议的代价目标分解为：

```text
operator JCT
  = 可观测的 pre-submit wait
  + 数据读取/组织时间
  + 条件于服务状态与提交动作的 endpoint wait + service time
  + fan-in / writeback
```

请求特征至少包括 prompt token、输出长度分布或分位数、frame/pixel work、模型与精度、batch 构成；状态特征至少包括 active work、running/waiting、KV/cache、endpoint、并发 job；动作特征包括是否提交、提交到哪个 endpoint、batch/token budget 和 credit。目标不只是总 JCT，还包括 TTFT、TPOT/ITL、remaining work、SLO slack 与预测区间。

推荐的 cost-estimator baseline 顺序是：

1. 不使用估计器的固定静态策略；
2. 仅以输入 token/frame 和 output cap 构成的解析模型；
3. 分桶/profile lookup；
4. 解析模型 + residual correction；
5. 增加输出长度分位数和 endpoint 状态的模型；
6. oracle actual output length（只作上界，不可部署）；
7. 若能取得足够细的 vLLM workload snapshot，再实现 serving-framework simulation 类对照。

所有模型使用配置组留出、独立时间段、workload 留出；进一步做模型/硬件留出。随机逐行切分会泄露同配置信息，不足以说明泛化。

### 9.5 `idea-evaluator`：Daft + Ray 队列可控设想

#### 第一印象与论文类型

**类型：Novel Setting + Method Contribution。** 最有价值的命题不是“Ray 队列让代价估计更容易”，而是：

> 在数据库 AI 算子的外部执行链路中，用轻量解析模型、profile residual 与不确定性估计，在 admission 之前预测 work/service/JCT，并利用 Daft+Ray 可控提交点选择数据组织、endpoint 和提交时机，以最小化固定资源下的决策 regret 与 SLO 违反。

代价估计应继续作为两项调度策略的共同使能组件，而不是独立第三项贡献。

#### 致命缺陷审计（最多两个）

| 风险 | 严重度 | 为什么危险 | 可行防守 |
|---|---|---|---|
| **“队列可控 + 代价感知调度”本身不新** | **MAJOR** | 数据库优化、LLM 路由和 output-length-aware scheduling 已经广泛使用成本/长度预测；若贡献只剩“用 Ray 控制提交”，容易成为 solution-first 工程组合 | 把新颖性限定为数据库 AI 外部链路的**决策导向、跨层代价模型**：共同建模组织、pre-submit、endpoint service、fan-in/writeback，并用同资源静态策略、解析模型、oracle 与 serving-aware 方法比较 regret，而不是声称首个代价感知调度器 |
| **队列控制不消除 service cost 的内生性和部分不可观测性** | **MAJOR** | pre-submit wait 可精确记录，但提交后的 service time 仍由自然 EOS、continuous batching、KV/cache、共同运行请求、其他 job 和硬件状态决定；且“暂不提交”会改变未来 batch/state。项目已有 AIMD 反例：工作积在 Ray 时，vLLM `waiting` 可保持 0 | 使用 state-action conditional 模型与预测区间；同时记录 Ray held work 和 vLLM state；用随机/受控探索覆盖动作；报告 tail underestimation、OOD 与 oracle regret；若细粒度 serving snapshot 不可得，就明确只做粗粒度 admission/capacity guidance |

两项都是可通过实验修订的 **MAJOR**，目前没有不可修复的 CRITICAL flaw。

#### 生命周期与能力匹配

| 维度 | 评估 |
|---|---|
| 原型→初步结果 | 已有双卡执行链路、request-level credit 语义和 283 行真实 profile；短周期可做 |
| 稳健结果 | 需要独立时间/workload、输出长度分布、多个 endpoint 状态与干预实验；中等工程量 |
| 可发表证据 | 需要证明较强点预测不是目标、而低 regret/SLO 改善来自模型；并完成跨模态或跨模型验证；约 6–12 个月研究周期更合理 |
| 能力/资源 | 现有 2×4090 与代码基础匹配；最大缺口是细粒度 serving snapshot 和更多外部验证数据。每周可投入时间未给出，资源匹配暂评 **Yellow** |

#### 五维评分（10 分制）

| 维度 | 分数 | 依据 |
|---|---:|---|
| Higher Effectiveness | 5 | 代价模型主要优化执行决策，不直接提升模型任务质量 |
| Faster | 8 | admission 前预测可减少过载排队和空闲，但必须由干预实验确认 |
| Stronger | 7 | 不确定性、remaining work、SLO slack 比单一 runtime 回归更稳健；尚未完成 OOD 验证 |
| Cheaper | 7 | 解析模型 + profile residual 的训练/推理成本低，并可能减少 GPU 空转和 SLO 浪费 |
| Broader | 8 | token work 可映射到 frame work，同一模型可服务数据组织、提交、路由和多 job 控制 |

该想法有两个维度达到 8 分，但 fatal-flaw audit 仍有两项 MAJOR，因此不能直接给 Strong Accept。

#### 范式潜力

- **First-principles reset：部分满足。** 把“预测误差最小”改成“选择 regret 最小”是正确重置；
- **Elephant-in-the-room：满足。** 语义系统与 serving 系统经常分别优化，外部数据库执行链路的 queue relocation 与写回被忽略；
- **Technology-cycle alignment：满足。** Daft/Ray 的可编程数据流和 vLLM 的运行指标使 admission-time 控制现在可实现；
- **Hamming importance：部分满足。** 问题重要但仍属系统子领域，不宜夸大为普适数据库代价模型革命。

结论是**渐进式工作中含有可扩展的范式种子**：从 point-estimation 转向 decision-oriented、uncertainty-aware、cross-layer admission cost。

#### 最终裁决

**Accept with Revisions。** 这个方向值得做，而且和现有项目结构高度吻合；但论点应改成“队列可控提高了决策可辨识性与干预能力”，而不是“让全部代价容易估计”。只有 pre-submit 部分更确定，endpoint service 仍是随机、内生且部分不可观测的。

### 9.6 最小决定性实验

1. **估计器消融**：固定静态、解析模型、profile lookup、解析+residual、加入输出分位数/服务状态、oracle actual output；所有臂共享同一最大 active work 与资源。
2. **开环预测 + 闭环决策同时评价**：报告 MAE/Q-error/区间 coverage，同时报告配置排序、SLO goodput、regret、回退率；若预测更准但 decision regret 不降，模型不晋级。
3. **队列迁移审计**：同步记录 Ray held/pending work、pre-submit wait、vLLM running/waiting/KV、TTFT/TPOT；证明策略不是把可见排队移到不可见层。
4. **受控干预**：在相同 arrival trace 下随机化部分 submit/hold 或 threshold，检验估计器在不同 action 下是否校准，避免只学习旧策略生成的数据分布。
5. **泛化留出**：配置组、独立时间段、短/长输出 workload、burst、不同模型/endpoint；多模态仅替换 work 定义为 frame/pixel budget，复用相同决策接口。
6. **晋级准则**：只有在至少一个主 workload 上相对同上限强静态策略显著改善 SLO goodput/JCT，且无明显 tail/fairness 回退，才把 cost-aware 动态策略写入主贡献。

### 9.7 本节主要文献来源

- LOTUS: [LOTUS: Enabling Semantic Queries with LLMs Over Tables of Unstructured and Structured Data](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)
- Abacus: [Abacus: Cost-Effective and Quality-Aware Planning for Semantic Operators](https://www.vldb.org/pvldb/vol19/p1060-russo.pdf)
- Learned cost model evaluation: [How Good are Learned Cost Models, Really?](https://arxiv.org/abs/2502.01229)
- GRACEFUL: [GRACEFUL: A Learned Cost Estimator for UDFs](https://arxiv.org/abs/2503.23863)
- COSTREAM: [Learned Cost Models for Distributed Stream Processing](https://arxiv.org/abs/2403.08444)
- LLM serving simulation/routing: [Beyond Accuracy and Cost: Latency-Aware LLM Query Routing for Dynamic Workloads](https://arxiv.org/abs/2607.18253)
- Output-length uncertainty: [Scheduling LLM Inference with Uncertainty-Aware Output Length Predictions](https://arxiv.org/abs/2604.00499)
- SLA/goodput scheduling: [Past-Future Scheduler for LLM Serving under SLA Guarantees](https://arxiv.org/abs/2507.10150)
- Fairness work accounting: [VTC: Fairness Scheduling for Serving Large Language Models](https://www.usenix.org/conference/osdi24/presentation/sheng)
- Chunked prefill/service metrics: [Sarathi-Serve](https://www.usenix.org/conference/osdi24/presentation/agrawal)
- Prediction fragility and tail risk: [Beyond Prediction: Tail-Aware Scheduling for LLM Serving](https://arxiv.org/abs/2606.18431)

产品场景和可安装性的一手入口统一维护在
[`experiments/plans/baseline_reference.md` 的厂商清单](../experiments/plans/baseline_reference.md#数据库厂商-ai-算子与可安装性清单2026-08-04)，
主要包括 [Doris AI Functions](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/ai-functions/overview/)、
[ClickHouse AI embedding](https://clickhouse.com/blog/clickhouse-release-26-06#aiembed)、
[OceanBase AI Functions](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975)、
[Oracle chainable AI functions](https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/chainable-utility-functions-and-common-use-cases.html)、
[Db2 LLM integration](https://www.ibm.com/docs/en/db2/12.1.x?topic=sql-llm-integration-db2)、
[SQL Server AI_GENERATE_EMBEDDINGS](https://learn.microsoft.com/en-us/sql/t-sql/functions/ai-generate-embeddings-transact-sql?view=sql-server-ver17)、
[PolarDB Daft benchmark](https://help.aliyun.com/zh/polardb/polardb-for-postgresql/daft-performance-benchmark)、
[Snowflake Cortex AISQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) 和
[BigQuery Generative AI](https://docs.cloud.google.com/bigquery/docs/generative-ai-overview)。
