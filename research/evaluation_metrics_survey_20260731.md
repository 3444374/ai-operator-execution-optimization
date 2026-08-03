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

---

## 9. 按论文与数据库系统拆分的指标矩阵及本项目对比合同（2026-08-04）

本节回答两个问题：已有工作究竟用什么指标评价数据库 AI 算子；本项目与它们比较时，哪些指标可以直接比较，哪些只能作功能或方法学对照。结论先行：**不能把不同模型、质量目标、资源和执行边界下的 wall time 放在一张表里比较**。本项目应采用“先保证语义/质量等价，再比较端到端性能；同时报告服务侧、上游调度侧和代价模型决策侧指标”的三层合同。

### 9.1 学术论文：算子、指标与可比边界

| 论文/系统 | 主要 AI 算子或任务 | 论文采用的核心指标 | 本项目应如何比较 |
|---|---|---|---|
| **LOTUS（PVLDB 2025）** | semantic filter/join/aggregation/top-k/group-by/map | 任务质量（accuracy、RP@k、nDCG 等）、执行时间、模型调用次数；部分算子带准确率/失败概率约束 | 选择可落到 `map`/AI_COMPLETE 或分类任务的同一语义；冻结模型、prompt、输出约束和数据，只比较质量达标后的 JCT、吞吐、调用数、输入/输出 token 和成本。LOTUS 的逻辑算子优化不能直接等同于本项目的物理提交调度。 |
| **Galois（SIGMOD 2025）** | 生成式数据集成/表格构造 | F1-Cell、Cardinality、Tuple Constraint、AVG-Score、token 数、执行时间 | 仅在相同生成任务上比较“质量—token—时间”；它没有并发、尾延迟、batching 或服务饱和指标，不能据此推断调度优劣。 |
| **GaussML（ICDE 2024）** | 数据库内 ML 推理的算子/计划优化 | 执行时间、speedup | 必须补上模型质量与完整 E2E 边界；只可把其计划优化思想作 related work，不应跨模型引用 speedup。 |
| **Smart（VLDB Journal 2025）** | SQL 中 ML 推理算子及计划选择 | 查询执行时间、speedup，推理成本常作为已知/固定输入 | 用作“传统优化器把推理代价视为静态参数”的对照；本项目重点检验动态 batch、服务状态和输出长度不确定性下该假设何时失效。 |
| **SmartLite（PVLDB 2024）** | 轻量推理执行/模型压缩 | 内存占用、推理延迟或加速比 | 若比较必须锁定质量目标并补 F1/accuracy；否则只能说明资源—速度权衡，不能说明算子效果更好。 |
| **InferDB（PVLDB 2024）** | 数据库内分类/回归推理 | 回归 RMSLE；分类 F1/Recall/Precision；推理延迟、训练/索引构建时间、存储大小；多次运行标准差 | 对 AI_CLASSIFY 可复用 F1/Recall/Precision + 延迟口径；训练/索引代价与本项目的外部执行调度不是同一阶段，应分表。 |
| **LEADS（PVLDB 2024）** | 工作负载感知的数据库内学习/推理 | Workload-AUC、Worst-AUC、FLOPs、响应时间，含数据库内/外执行和消融 | 可借鉴“平均质量 + 最坏工作负载质量”和完整消融；本项目应将 FLOPs 扩展为实际 token/frame work、GPU 时间和能耗。 |
| **NeurDB（CIDR 2025）** | AI 原生数据库中的训练与推理 | E2E latency、训练吞吐、loss 曲线、事务吞吐 | 只作系统愿景与分层测量参考；本项目不把训练吞吐或事务吞吐混入 AI 算子执行主表。 |
| **Cortex AISQL（SIGMOD 2026）** | AI SQL 谓词/模型级联与运行时重排 | F1/precision/recall、执行时间、吞吐、模型调用次数、delegation rate | 是强相关对照。相同任务上比较质量达标后的调用数、token、JCT；但其线性 per-row cost/谓词重排与本项目的可变长生成、连续 batching、上游 admission 是不同控制层。 |
| **Palimpzest（CIDR 2025）** | LLM 数据处理算子与物理计划搜索 | 时间、货币成本、质量/F1，输出 Pareto 计划；用少量 workload profile | 借鉴 Pareto 展示和小样本 profile；比较时统一 API/本地模型与并发，避免把单线程 API 账单和固定 GPU 的吞吐直接相除。 |
| **Abacus（PVLDB 2026）** | 语义算子的模型/计划选择 | 质量、货币成本、延迟及约束满足，验证样本预算，多次试验 | 借鉴“约束下选择”而非只做点预测；本项目对应目标是固定资源下最大 SLO goodput/最小 JCT，报告选择 regret 和约束违反率。 |
| **SemBench（PVLDB 2026）** | semantic filter/join/map 等跨系统基准 | 质量、执行时间、货币成本、内存、伸缩性、调用次数、失败/限流，多次重复 | 最适合承载跨系统功能对照。对 LOTUS/Palimpzest/托管 SQL 做质量—成本—时间比较；对本项目再追加 TTFT/TPOT、上游队列、active work、GPU 利用率和写回分段。 |

由上表可见，论文通常分成三类：

1. **语义/逻辑优化论文**关注质量、模型调用数、token 或货币成本以及总时间；
2. **数据库内推理论文**关注单查询延迟、吞吐、内存、索引/训练开销；
3. **LLM 数据处理系统**开始报告质量—成本—延迟 Pareto，但很少公开上游 submission、服务内部排队、active work、写回和多 job 公平性。

本项目的区别不应表述为“我们比所有 AI 算子系统更快”，而应表述为：**在相同算子语义和模型质量下，研究它们普遍未拆开的外部执行链路——数据组织、提交控制、模型服务、fan-in 与写回——并给出可解释的调度机制指标。**

### 9.2 数据库/厂商系统：公开指标与合理对照方式

| 数据库/产品族 | 公开评测主要指标 | 对本项目的用途 | 不可直接声称 |
|---|---|---|---|
| **PolarDB Lakebase + Daft on Ray** | 多模态 workload 的 E2E wall time，和 Ray Data/Spark 的厂商对照 | 最近的同栈工业锚点；复现公开 workload 形状，并对齐硬件、模型、数据与计时边界后比较 | 公开结果未充分披露 warm-up、重复和原始脚本，不能直接引用厂商倍数；也不能由公开材料断言其内部没有服务状态感知。 |
| **Snowflake Cortex/AISQL** | F1/precision/recall、执行时间、LLM 调用数、delegation rate | 借鉴强模型、动态级联、便宜 proxy 的多臂对照和质量门禁 | 闭源云环境数字不能与本地 2×4090 wall time 排名。 |
| **BigQuery ML/AI functions** | rows/time、成功率、固定 token/QPM 条件下吞吐、账单成本 | 借鉴大规模行级成功率和成本归一化 | 内部 benchmark、配额和后端模型不透明，不能作性能 headline。 |
| **Oracle AI Vector Search/LLM 服务** | TTFT、TPOT、latency、throughput、MBU/MFU；Recall@k、QPS | 采用成熟 LLM 服务指标分解与向量质量门禁 | 匿名或资源不对齐 baseline 不可采用；向量 read-side ANN 不能代替本项目的生成/写入侧链路。 |
| **PostgresML** | rows/s、predictions/s、部分 p99、训练/推理时间 | 对数据库内本地推理给出功能锚点 | toy query 或单次计时不足以支撑系统对比。 |
| **pgai/pgvector/pgvectorscale** | Recall@k–QPS Pareto、p50/p95/p99、目标 QPS 下成本 | 对 AI_EMBED 写回后的 read-side 质量和索引基线有用 | 主要评测 ANN 查询，不覆盖 embedding 生成、上游组织和写回吞吐。 |
| **Databricks Lakehouse AI** | Recall@10、p50/p99、TTFT/TPOT/throughput、MBU/MFU、组件分段时间 | 借鉴服务指标和 per-component breakdown | 闭源系统只作方法学对齐，不作数值速度排名。 |

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
- GRACEFUL: [Zero-Shot Cost Models for UDFs](https://arxiv.org/abs/2503.23863)
- COSTREAM: [Learned Cost Models for Distributed Stream Processing](https://arxiv.org/abs/2403.08444)
- LLM serving simulation/routing: [Beyond Accuracy and Cost: Latency-Aware LLM Query Routing for Dynamic Workloads](https://arxiv.org/abs/2607.18253)
- Output-length uncertainty: [Scheduling LLM Inference with Uncertainty-Aware Output Length Predictions](https://arxiv.org/abs/2604.00499)
- SLA/goodput scheduling: [Past-Future Scheduler for LLM Serving under SLA Guarantees](https://arxiv.org/abs/2507.10150)
- Fairness work accounting: [VTC: Fairness Scheduling for Serving Large Language Models](https://www.usenix.org/conference/osdi24/presentation/sheng)
- Chunked prefill/service metrics: [Sarathi-Serve](https://www.usenix.org/conference/osdi24/presentation/agrawal)
- Prediction fragility and tail risk: [Beyond Prediction: Tail-Aware Scheduling for LLM Serving](https://arxiv.org/abs/2606.18431)
