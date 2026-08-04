# 项目参考资料索引

更新日期：2026-08-04
本索引列出当前 `research/reference/` 中实际存在的 PDF 文件，并与精读笔记交叉引用。
Top 15 正式论文的权威排名与选择依据见 `../top15_ranked_papers.md`。

## 一、Top 15 正式论文（15/15 CCF-A research paper）

| # | 文件 | 论文 | 正式出处 | 笔记 |
|---|---|---|---|---|
| 1 | `lotus_pvldb2025.pdf` | LOTUS: Semantic Operators and Their Optimization | PVLDB 18(11), 2025 | `lotus_pvldb2025.md` |
| 2 | `galois_sigmod2025.pdf` | Galois: Logical and Physical Optimizations for SQL over LLMs | SIGMOD 2025 | `galois_sigmod2025.md` |
| 3 | `gaussml_icde2024.pdf` | GaussML: An End-to-End In-Database Machine Learning System | ICDE 2024 | `gaussml_icde2024.md` |
| 4 | `vllm_sosp2023.pdf` | vLLM: PagedAttention + Continuous Batching | SOSP 2023 | `vllm_sosp2023.md` |
| 5 | `orca_osdi2022.pdf` | Orca: Iteration-level Scheduling for Transformer Inference | OSDI 2022 | `orca_osdi2022.md` |
| 6 | `sarathi_serve_osdi2024.pdf` | Sarathi-Serve: Chunked Prefill + Stall-free Scheduling | OSDI 2024 | `sarathi_serve_osdi2024.md` |
| 7 | `sglang_neurips2024.pdf` | SGLang: Efficient Execution of Structured LM Programs | NeurIPS 2024 | `sglang_neurips2024.md` |
| 8 | `vtc_osdi2024.pdf` | VTC: Fairness in Serving LLMs | OSDI 2024 | `vtc_osdi2024.md` |
| 9 | `llumnix_osdi2024.pdf` | Llumnix: Dynamic Scheduling for LLM Serving | OSDI 2024 | `llumnix_osdi2024.md` |
| 10 | `distserve_osdi2024.pdf` | DistServe: Prefill-Decode Disaggregation | OSDI 2024 | `distserve_osdi2024.md` |
| 11 | `ray_osdi2018.pdf` | Ray: A Distributed Framework for Emerging AI Applications | OSDI 2018 | `ray_osdi2018.md` |
| 12 | `learned_cost_models_sigmod2025.pdf` | How Good are Learned Cost Models, Really? | SIGMOD 2025 | `learned_cost_models_sigmod2025.md` |
| 13 | `graceful_udf_cost_icde2025.pdf` | GRACEFUL: A Learned Cost Estimator for UDFs | ICDE 2025 | `graceful_udf_cost_icde2025.md` |
| 14 | `costream_icde2024.pdf` | COSTREAM: Learned Cost Models for Operator Placement | ICDE 2024 | `costream_icde2024.md` |
| 15 | `abacus_pvldb2026.pdf` | Abacus: A Cost-Based Optimizer for Semantic Operator Systems | PVLDB 19(5), 2026 | `abacus_pvldb2026.md` |

Top 15：15/15 已下载、15/15 有精读笔记。选择依据与排名逻辑见 `../top15_ranked_papers.md`。

## 二、核心补充论文（13 篇，有精读笔记）

| 文件 | 论文 | 正式出处 | 笔记 | 不进 Top 15 原因 |
|---|---|---|---|---|
| `palimpzest_cidr2025.pdf` | Palimpzest: Optimizing AI-Powered Analytics with Declarative Query Processing | CIDR 2025 | `palimpzest_cidr2025.md` | CIDR 非 CCF-A |
| `sembench_pvldb2026.pdf` | SemBench: A Benchmark for Semantic Query Processing Engines | PVLDB 19(8), 2026 | `sembench_pvldb2026.md` | benchmark paper |
| `fairserve_arxiv2024.pdf` | FairServe: Ensuring Fair LLM Serving Amid Diverse Applications | arXiv 2024 | `fairserve_arxiv2024.md` | 预印本 |
| `dlpm_arxiv2025.pdf` | DLPM: Locality-aware Fair Scheduling in LLM Serving | arXiv:2501.14312 | `dlpm_arxiv2025.md` | 预印本 |
| `autellix_arxiv2025.pdf` | Autellix: An Efficient Serving Engine for LLM Agents as General Programs | arXiv:2502.13965 | `autellix_arxiv2025.md` | 预印本 |
| `chiron_arxiv2025.pdf` | Chiron: Hierarchical Autoscaling for LLM Serving | arXiv:2501.08090 | `chiron_arxiv2025.md` | 预印本 |
| `clipper_nsdi2017.pdf` | Clipper: A Low-Latency Online Prediction Serving System | NSDI 2017 | `clipper_nsdi2017.md` | AIMD 历史来源 |
| `splitwise_isca2024.pdf` | Splitwise: Efficient LLM Inference with Phase Splitting | ISCA 2024 | `splitwise_isca2024.md` | prefill/decode 分池 |
| `neurdb_cidr2025.pdf` | NeurDB: An AI-Native Database System | CIDR 2025 | `neurdb_cidr2025.md` | CIDR 非 CCF-A |
| `cortex_aisql_sigmod2026.pdf` | Cortex AISQL: AI SQL Operators in Production | SIGMOD 2026 | `cortex_aisql_sigmod2026.md` | 工业 companion |
| `concur_2025.pdf` | CONCUR: Adaptive Admission Control for LLM Inference | arXiv 2025 | `concur_2025.md` | 预印本 |
| `scorpio_llm_serving_2025.pdf` | SCORPIO: Credit-Based Admission for LLM Serving | arXiv 2025 | `scorpio_llm_serving_2025.md` | 预印本 |
| `saber_batching_2025.pdf` | SABER: USL-Based Batching for LLM Serving | arXiv 2025 | `saber_2025.md` | 预印本 |

## 三、多模态与具身智能补充（8 篇，有精读笔记）

| 文件 | 论文 | 出处 | 笔记 |
|---|---|---|---|
| `db_perspective_llm_pvldb2025.pdf` | A Database Perspective on LLM Inference Systems | PVLDB 2025 Tutorial | `db_perspective_llm_pvldb2025.md` |
| `leads_pvldb2024.pdf` | LEADS: SQL-Aware Dynamic Model Slicing | PVLDB 2024 | `leads_pvldb2024.md` |
| `inferdb_pvldb2024.pdf` | InferDB: Lightweight In-Database Inference with Indexes | PVLDB 2024 | `inferdb_pvldb2024.md` |
| `smart_vldbj2025.pdf` | Smart: SQL+ML Predicate Reasoning and Cost-Optimal Execution | VLDB Journal 2025 | `smart_vldb_journal_2025.md` |
| `smartlite_pvldb2024.pdf` | SmartLite: DBMS-Native NN Operators for Edge | PVLDB 2024 | `smartlite_pvldb2024.md` |
| `diskann_neurips2019.pdf` | DiskANN: Billion-Scale Approximate Nearest Neighbor Search | NeurIPS 2019 | `diskann_neurips2019.md` |
| `milvus_sigmod2021.pdf` | Milvus: A Purpose-Built Vector Data Management System | SIGMOD 2021 | `milvus_sigmod2021.md` |
| `serverlessllm_osdi2024.pdf` | ServerlessLLM: Locality-Enhanced Serverless Inference | OSDI 2024 | `serverlessllm_osdi2024.md` |

## 四、算子代价估计新增文献（6 篇，有精读笔记）

| 文件 | 论文 | 出处 | 笔记 |
|---|---|---|---|
| `concerto_cost_estimation_arxiv2024.pdf` | CONCERTO: DAG + GAT + TCN Cost Model | arXiv 2024.12 | `concerto_cost_estimation_arxiv2024.md` |
| `redefining_cost_estimation_arxiv2025.pdf` | Redefining Cost Estimation with Tree Ensembles | arXiv 2025 | `redefining_cost_estimation_arxiv2025.md` |
| `sfs_latency_routing_arxiv2026.pdf` | SFS: Beyond Accuracy and Cost — TTFT-Aware Routing | arXiv 2026 | `sfs_latency_routing_arxiv2026.md` |
| `lance_2025.pdf` | Lance: A Modern Columnar Data Format for AI | arXiv 2025 | `lance_2025.md` |
| `llm4dm_pvldb2024.pdf` | Large Language Models for Data Management | VLDB 2024 Tutorial | `llm4dm_pvldb2024.md` |
| `mooncake_acmtos2025.pdf` | Mooncake: KV Cache-Centric Disaggregated Architecture | FAST 2025 / ACM TOS | `mooncake_acmtos2025.md` |

## 五、算子代价估计新增文献（5 篇，2026-08-04 下载，待精读）

| 文件 | 论文 | 出处 | 笔记 |
|---|---|---|---|
| `tie_icml2026.pdf` | TIE: Scheduling LLM Inference with Uncertainty-Aware Output Length Predictions | ICML 2026 | — |
| `past_future_asplos2025.pdf` | Past-Future Scheduler for LLM Serving under SLA Guarantees | ASPLOS 2025 | — |
| `jitserve_nsdi2026.pdf` | JITServe: SLO-aware LLM Serving with Imprecise Request Information | NSDI 2026 | — |
| `beyond_prediction_icml2026.pdf` | Beyond Prediction: Tail-Aware Scheduling for LLM Inference (UniBoost) | ICML 2026 | — |
| `fastserve_nsdi2026.pdf` | FastServe: Iteration-Level Preemptive Scheduling for LLM Inference | NSDI 2026 | — |

> **Note**: `fastserve_nsdi2026.pdf` 与 `fastserve_2023.pdf`（arXiv:2305.05920 v3, 2024-09）为同一篇论文的不同版本，内容相同（617,185 bytes）。NSDI 2026 为正式出版版本。

## 六、已有精读笔记的其他论文（7 篇）

| 文件 | 论文 | 出处 | 笔记 |
|---|---|---|---|
| `bucketserve_2025.pdf` | BucketServe: Adaptive Bucket Split/Merge for LLM Batching | arXiv 2025 | `bucketserve_2025.md` |
| `proserve_2025.pdf` | ProServe: Two-Level Scheduling (SlideBatching + GoRouting) | arXiv 2025 | `proserve_2025.md` |
| `colora_aspdac2026.pdf` | CoLoRA: Multi-Signal Fusion Scheduling for LoRA Multi-Tenancy | ASP-DAC 2026 (CCF-C) | `colora_2026.md` |
| `multibin_batching_2024.pdf` | MultiBin: Bin-Packing Batching for Heterogeneous LLM Requests | arXiv 2024 | `multibin_batching_2024.md` |
| `ray_data_streaming_batch_2025.pdf` | Ray Data: CPU/GPU Heterogeneous Batch Data Pipeline | arXiv 2025 | `ray_data_streaming_batch_2025.md` |
| `flashattention_neurips2022.pdf` | FlashAttention: Fast and Memory-Efficient Exact Attention | NeurIPS 2022 | `flashattention_neurips2022.md` |
| `flexgen_icml2023.pdf` | FlexGen: High-Throughput LLM Inference on a Single GPU | ICML 2023 | `flexgen_icml2023.md` |

## 七、已下载但无精读笔记的论文（39 篇）

### 7.1 GPU 推理服务（7 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `clockwork_osdi2020.pdf` | Clockwork: Deterministic DNN Latency Scheduling | OSDI 2020 |
| `parrot_osdi2024.pdf` | Parrot: Semantic Variable-Based Cross-Request Prompt Sharing | OSDI 2024 |
| `hedrarag_sosp2025.pdf` | HedraRAG: CPU/GPU Coordinated RAG Serving | SOSP 2025 |
| `hybridflow_eurosys2025.pdf` | HybridFlow: Heterogeneous DNN Pipeline Batching | EuroSys 2025 |
| `load_aware_prefill_2026.pdf` | Load-Aware Prefill for LLM Serving | arXiv 2026 |
| `fastserve_2023.pdf` | Fast Distributed Inference Serving for Large Language Models | arXiv 2023 (v3: 2024-09) |
| `deepseek_v3_2024.pdf` | DeepSeek-V3 Technical Report | arXiv 2024 |

### 7.2 数据库与 AI 算子系统（3 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `aidb_deem_sigmod2024.pdf` | AIDB: Sparse Materialization for AI Workloads | DEEM@SIGMOD 2024 |
| `anddb_sigmod2025_demo.pdf` | AndDB: Analytics-Native Database Demo | SIGMOD 2025 |
| `dbot_pvldb2024.pdf` | DBot: LLM-Based Database Tuning Assistant | PVLDB 2024 |

### 7.3 数据管线与执行引擎（8 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `arrow_flight_2022.pdf` | Arrow Flight: High-Performance Columnar Data Transfer | arXiv 2022 |
| `arrow_datafusion_sigmod2024.pdf` | Arrow DataFusion: Arrow-Native Query Engine | SIGMOD 2024 |
| `duckdb_sigmod2019.pdf` | DuckDB: An Embeddable Analytical Database | SIGMOD 2019 |
| `opengauss_pvldb2021.pdf` | openGauss: An Enterprise-Grade Open-Source Database | PVLDB 2021 |
| `delta_lake_pvldb2020.pdf` | Delta Lake: Optimistic Concurrency + Blind Append | PVLDB 2020 |
| `iceberg_row_level_vldb2024.pdf` | Apache Iceberg Row-Level Operations | VLDB 2024 |
| `gpu_cpu_db_analytics_pvldb2020.pdf` | GPU/CPU Co-Processing for Database Analytics | PVLDB 2020 |
| `flexpushdowndb_pvldb2021.pdf` | FlexPushdownDB: Compute-vs-Storage Pushdown Cost Model | PVLDB 2021 |

### 7.4 AI 数据存储与向量索引（6 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `turbocharging_vector_db_ssd_pvldb2025.pdf` | TurboVecDB: Parallel I/O + Spatial-Aware Insertion | PVLDB 2025 |
| `columnar_storage_eval_pvldb2023.pdf` | ColStorEval: Parquet/ORC Columnar Write Performance | PVLDB 2023 |
| `wisckey_fast2016.pdf` | WiscKey: Separating Keys from Values in SSD-Conscious Storage | FAST 2016 |
| `bigvectorbench_vldb2025.pdf` | BigVectorBench: Vector Database Benchmarking Methodology | VLDB 2025 |
| `rafiki_pvldb2018.pdf` | Rafiki: ML as Analytics Service | PVLDB 2018 |
| `deferred_view_maintenance_sigmod1996.pdf` | Deferred View Maintenance for Materialized Views | SIGMOD 1996 |

### 7.5 教程与综述（2 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `trustworthy_efficient_llms_db_vldb2024.pdf` | Trustworthy and Efficient LLMs Meet Databases | VLDB 2024 Tutorial |
| `vdbms_tutorial_vldb2024.pdf` | Vector DBMS — A Tutorial | VLDB 2024 |

### 7.6 OceanBase 系列（3 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `oceanbase_mercury.pdf` | OceanBase Mercury: Distributed Transaction Processing | — |
| `oceanbase_bacchus.pdf` | OceanBase Bacchus: HTAP Optimizations | — |
| `oceanbase_2pc.pdf` | OceanBase 2PC: Distributed Commit Protocol | — |

### 7.7 其他（5 篇）

| 文件 | 论文 | 出处 |
|---|---|---|
| `learning_db_optimization_fcs2025.pdf` | Learning Database Optimization: A Survey | FCS 2025 |
| `neo_learned_optimizer_sigmod2019.pdf` | Neo: A Learned Query Optimizer | SIGMOD 2019 |
| `rtindex_2023.pdf` | RT-Index: Real-Time Index for Streaming Embeddings | — |
| `rosa_robotics_2026.pdf` | RoSA: Robot Skill Acquisition with LLM | — |
| `heterohub_2025.pdf` | HeteroHub: Multi-Embodied Agent Data Management | arXiv 2025 |

## 八、未下载文献

| 文献 | 状态 | 原因 |
|---|---|---|
| Learned Query Optimizer (Zhu et al.) SIGMOD 2024 | 未下载 | ACM 付费墙，未找到 OA 版本 |

## 九、网页与系统资料入口

| 资料 | 用途 | 入口 |
|---|---|---|
| vLLM docs | model serving 配置、指标和 benchmark | https://docs.vllm.ai/ |
| Ray Core / Data docs | actor/task、Data LLM、HttpRequestProcessor | https://docs.ray.io/ |
| Daft docs | `prompt()`、Ray runner、partition 和 concurrency | https://docs.daft.ai/ |
| LOTUS | semantic operator API 与复现 | https://lotus-ai.readthedocs.io/ |
| Palimpzest | 官方实现 | 以论文/官方仓库为准 |
| SemBench | workload 与系统比较 | https://github.com/SemBench/SemBench/ |
| PolarDB Daft on Ray | 工业 Daft/Ray 背景 | https://help.aliyun.com/zh/polardb/ |

## 十、统计与一致性

- **PDF 总数**: 88（`fastserve_2023.pdf` 与 `fastserve_nsdi2026.pdf` 内容相同，后者为 NSDI 2026 正式版命名；去重后 87 唯一内容）
- **唯一内容 PDF**: 87
- **Top 15 PDF**: 15/15
- **Top 15 精读笔记**: 15/15
- **精读笔记总数**: 49
- **有笔记 + 有 PDF**: 48（`sfs_latency_routing_arxiv2026.md` 对应 `latency_aware_llm_routing_arxiv2026.pdf`，文件名不同）
- **有 PDF 无笔记**: 39
- **有笔记无 PDF**: 0
- 新增 PDF 已通过 `%PDF` 签名 + pypdf 页数解析验证。
