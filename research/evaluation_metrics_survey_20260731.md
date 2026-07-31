# 评估指标调研：AI 算子与推理服务文献 + 数据库厂商基准

更新日期：2026-07-31
调研工具：`nature-academic-search` + `deep-research`（lit-review 口径），以一个后台工作流执行（15 个抽取 agent + 1 个综合 agent）。
证据范围：项目已有 49 篇精读笔记 + 8 个数据库厂商/标准基准的 web 调研。

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

---

## 6. 已核实的代码级事实（P0 三条的代码证据）

本节为本调研中亲自核实，非工作流传言：

1. **`code/src/baselines/vllm_bench.py:126-141`**——vLLM bench detailed-result 解析路径读取 `ttfts` 与 `itls` 数组后，把每条请求折叠为 `latency = ttft + sum(intervals)`（第 138-141 行），**TTFT 值与逐 token ITL 分布均被丢弃**，只保留单条 e2e 标量。
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

1. **P0 三条优先**——TTFT 分位、ITL 分布、prefix cache hit rate。改动集中在 `code/src/metrics.py` + `code/src/baselines/vllm_bench.py`，不触策略代码；先在 cache-ON 路由实验上补采，**直接服务当前 prefix 结论的隔离消融**（4-ep/7B 或 2-ep/1.5B、人为缩 KV 制造可控淘汰率）。登记到 `experiments/plans/experiment_status_and_gaps.md` 指标缺口区。
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

**覆盖 7 家**：PolarDB Lakebase、Snowflake Cortex、BigQuery ML、Oracle AI Vector Search、PostgresML、pgai/pgvector/pgvectorscale、Databricks Lakehouse AI。

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

### B.2 其他 6 家逐家方法论

| 厂商 | 怎么测（一句话） | benchmark / workload | 指标 | 对照 baseline | 可复现性 |
|---|---|---|---|---|---|
| **Snowflake Cortex** | 最接近学术：SIGMOD Companion'26 论文（arXiv 2511.07663v3）做**三臂消融**（强 oracle-only / 动态 cascade / 便宜 proxy-only）+ 单变量扫描，**每查询 5 次取均值** | 6 HF 布尔分类集（NQ/BOOLQ/IMDB/SST2/QUORA/FARL）+ 8 HF entity-matching 集（ABTBUY/NASDAQ/ARXIX/EURLEX/NYT/CNN/AG NEWS/BIODEX）+ 自建 NYT 1000 篇 | F1/precision/recall + 执行时间 + **LLM 调用次数**（110000 vs 330）+ observed delegation rate | 三臂同系统对照；Cortex Analyst 单 prompt GPT-4o（营销） | 论文 public-data-**no-code**；被测栈闭源云；营销 70% 黑盒 |
| **BigQuery ML** | 官方只发"internal benchmarking"数字：6h 单 job × 固定 QPM 配额 × 固定 tokens/row，dynamic token batching 打满 | 合成 per-row（embedding 50 tok/row、文本 500 in+50 out）；客户负载 Faraday 12.6M embeddings/45min；bbc_news 教程 | 吞吐（rows/6h）、可靠性、$ | 旧版自比、Vertex AI PT、Gemini Flash vs Pro | 多数 closed-cloud-**blackbox**；唯一 public-data-public-code 是 bbc_news 教程 |
| **Oracle AI Vector Search** | LLMPerf + ann-benchmarks 数据集 + 4 场景正态分布 token 模型 | SIFT1M/GIST1M/Fashion-MNIST；Llama 3.2 3B on OCI A10；LLMPerf synthetic chat N(200,40)/N(100,10)；semantic-cache demo | TTFT/TPOT/Latency/Throughput + MBU + MFU；recall@k；QPS | 匿名竞品 O-DB-VDB-1/2/3、O-VDB、O-DOCDB（**反模式**：匿名 + 承认对手 2–4× CPU 线程） | 协议公开（LLMPerf）+ 公开数据；但匿名 baseline 被社区批评；**自承 LLMPerf 把 tokenization 编码时间误算进 tokens/s** |
| **PostgresML** | 严谨度两极分化：唯一严谨的 Scaling 篇用 pgbench + EC2 机型 + 100+ 配置扫描 + 公开 raw CSV；其余 4 篇教学博客用 `psql \timing` 单次/25 次平均、无 warm-up | flights（航班延误）XGBoost；Amazon US Reviews 5M 行 embedding；HuggingFace 模型；pgbench | rows/s、predictions/s、p99（仅引用 OpenAI 的）、$ | Python+Flask+Redis（多跳，被 HN 批）、MindsDB（无 GPU）、OpenAI、Pinecone/Qdrant/Weaviate | Scaling 篇 public-data-public-code；其余 toy（2-文档 RAG） |
| **pgai/pgvector/pgvectorscale** | 全聚焦 read-side ANN：ANN-Benchmarks fork（修了多线程 QPS + warmup/test 分离：29k 预热 + 1000 disjoint 测试）画 recall@k vs QPS Pareto | 50M Cohere Wikipedia 768d；LAION 5M/100M；ann-benchmarks glove/sift；OpenAI ada-002/3-small | **recall@k vs QPS**（recall-defined throughput）+ p50/p95/p99 + $/month-for-target-QPS | Pinecone s1、Qdrant、pgvector HNSW/IVFFlat、pgvectorscale StreamingDiskANN、VectorChord、exact | 协议+数据+OSS 扩展公开（**最可复现**之一）；但 YDB/wasowski 第三方复现发现 harness/单节点竞争主导差距 |
| **Databricks Lakehouse AI** | 分层：(1) AI/Vector Search 最透明（Locust 逐步并发 + 二分搜索 max sustainable QPS + per-component 计时）；(2) LLM 推理指标框架最成熟（MosaicML TTFT/TPOT/Latency/Throughput+MBU+MFU）；(3) AI Functions 最不透明（10x/100x 营销） | 10亿向量 768d（Standard 320M / Storage-Opt 1B+）；2048/256 RAG summarization；512/64 静态批 | recall@10 + p50/p99；TTFT/TPOT/MBU/MFU；ann_time/embedding_gen_time/reranker_time/response_time 分解 | Standard AI Search、FasterTransformers+TensorRT-LLM、旧版自比 | 协议层公开（Locust notebook、benchmarking notebook、Eval Gauntlet PDF）；被测系统闭源云 |

### B.3 跨厂商共识与惯例

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

**可复现性缺口（5 层）**：① 被测系统几乎全闭源云黑盒（Snowflake/BigQuery/Oracle/Databricks）；② public-data-no-code（放数据不放 harness）；③ 营销数字无协议；④ 匿名+不对齐资源 baseline；⑤ 单次计时无 warm-up/repeats/CV。**最可复现的也只到 public-data-no-code 级，无人达全栈可复现。→ 本项目全开源 vLLM+Ray+Daft 栈 + 公开数据 + 全量 CSV 进 README，可复现性严格优于全部 7 家，是天然差异化卖点。**

**独特空白（最重要）**：**没有任何厂商公开 benchmark "写入侧/上游调度 pipeline"**（embedding 生成吞吐、ingestion、writeback、批合并、提交节奏）——pgai 只测并发写回正确性不测吞吐，其余完全忽略。**本项目的主战场（数据组织 + 提交控制 + 写回）正好填这个洞。**

### B.4 对本项目的具体启示

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
16. **"写入侧/上游调度 pipeline benchmark"是 7 家厂商留下的空白**——本项目主战场（数据组织 + 提交控制 + 写回）正填此洞，评估 framing 中明确点出。
17. **PolarDB Lakebase 是最近、最同栈的工业系统**——Related Work 必须点名，项目差异 = 模型服务状态感知 vs 通用数据流 backpressure + 闭源产品未公开的调度消融。

### B.5 不能声称

- **不引用任何厂商的提速倍数**（100x/30x/10x/100x/70%/4-10x/2.2-7.6x 全是闭源黑盒或匿名 baseline 营销）。跨厂商对比只能说"采用某厂商公开 tutorial 同款公开数据（如 bbc_news、Cohere 50M）与公开过程做可复现对照"，不能声称对标某厂商闭源数字。
- PolarDB 的 scoop 核查状态为"**未见研究论文证据**"，非"确定不存在"——正式定稿新颖性前需补 VLDB/SIGMOD/ICDE 阿里云 Daft 团队学术文献检索。
- 本附录厂商方法论为**厂商来源/官方文档**（非独立第三方）；PolarDB 同栈与开源 Daft 关系为**官方文档 + 开源仓库交叉核实**的**本地事实**；新颖性边界（PolarDB 不观测 vLLM 状态）为基于其公开文档未提及的**合理推断**（闭源内部无法确证）。
