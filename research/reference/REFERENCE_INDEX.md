# 项目参考资料索引

> 生成日期：2026-07-22 ｜ 更新：2026-07-23（PDF 全量规范化改名 + 误下载/重复清理 + 补齐 Milvus/Clipper/CoLoRA）｜ 2026-07-24（补 Clockwork）
> 用途：汇总项目中所有被引用的文献、文档和系统资料
> 命名规范：`短名_会议年份.pdf`，与 `research/reading_notes/` 精读笔记一一对应

---

## 一、已下载 PDF 论文（68 篇）

### 1.1 数据库 AI 算子（DB4AI / DB 侧 AI 执行）

| 论文 | 文件 | 出处 |
|------|------|------|
| Cortex AISQL: A Production SQL Engine for Unstructured Data | `cortex_aisql_sigmod2026.pdf` | SIGMOD 2026, arXiv:2511.07663 |
| Galois: Logical and Physical Optimizations for SQL over LLMs | `galois_sigmod2025.pdf` | SIGMOD 2025 |
| NeurDB: An AI-powered Autonomous Database | `neurdb_cidr2025.pdf` | CIDR 2025 |
| GaussML: An End-to-End In-database ML System | `gaussml_icde2024.pdf` | ICDE 2024 |
| Smart: In-database Query Optimization on SQL with ML Predicates | `smart_vldbj2025.pdf` | VLDB Journal 2025 |
| LEADS: In-Database Dynamic Model Slicing | `leads_pvldb2024.pdf` | PVLDB Vol.17, 2024 |
| InferDB: In-Database ML Inference Using Indexes | `inferdb_pvldb2024.pdf` | PVLDB Vol.17, 2024 |
| SmartLite: A DBMS-Based DNN Inference Serving System | `smartlite_pvldb2024.pdf` | PVLDB 2024 |
| AIDB: A Sparsely Materialized Database for Queries using ML | `aidb_deem_sigmod2024.pdf` | DEEM@SIGMOD 2024 |
| AnDB: AI-Native Database for Universal Semantic Analysis | `anddb_sigmod2025_demo.pdf` | SIGMOD 2025 Demo, arXiv:2502.13805 |
| D-Bot: Database Diagnosis System using LLMs | `dbot_pvldb2024.pdf` | PVLDB Vol.17, 2024 |
| openGauss: An Autonomous Database System | `opengauss_pvldb2021.pdf` | PVLDB Vol.14, 2021 |

### 1.2 LLM 推理服务系统

| 论文 | 文件 | 出处 |
|------|------|------|
| vLLM: PagedAttention for LLM Serving | `vllm_sosp2023.pdf` | SOSP 2023 Best Paper |
| Orca: A Distributed Serving System for Generative Models | `orca_osdi2022.pdf` | OSDI 2022 |
| Sarathi-Serve: Taming Throughput-Latency Tradeoff | `sarathi_serve_osdi2024.pdf` | OSDI 2024 |
| ServerlessLLM: Low-Latency Serverless LLM Inference | `serverlessllm_osdi2024.pdf` | OSDI 2024 |
| DistServe: Disaggregating Prefill and Decoding | `distserve_osdi2024.pdf` | OSDI 2024 |
| Splitwise: Efficient Generative LLM Inference Using Phase Splitting | `splitwise_isca2024.pdf` | ISCA 2024, arXiv:2311.18677 |
| Mooncake: A KVCache-centric Disaggregated Architecture | `mooncake_acmtos2025.pdf` | ACM TOS 2025, arXiv:2407.00079 |
| SGLang: Efficient Execution of Structured LM Programs | `sglang_neurips2024.pdf` | NeurIPS 2024, arXiv:2312.07104 |
| Clipper: A Low-Latency Online Prediction Serving System | `clipper_nsdi2017.pdf` | NSDI 2017 |
| Serving DNNs like Clockwork: Performance Predictability from the Bottom Up | `clockwork_osdi2020.pdf` | OSDI 2020 |
| FastServe: Fast Distributed Inference Serving for LLMs | `fastserve_2023.pdf` | arXiv:2305.05920, 2024 |
| Parrot: Efficient Serving of LLM-based Applications with Semantic Variable | `parrot_osdi2024.pdf` | OSDI 2024, arXiv:2405.19888 |
| FlashAttention: Fast and Memory-Efficient Exact Attention | `flashattention_neurips2022.pdf` | NeurIPS 2022, arXiv:2205.14135 |
| FlexGen: High-Throughput Generative Inference with a Single GPU | `flexgen_icml2023.pdf` | ICML 2023, arXiv:2303.06865 |
| DeepSeek-V3 Technical Report | `deepseek_v3_2024.pdf` | arXiv:2412.19437, 2024 |

### 1.3 LLM 推理调度 / Batching 策略（RC1/RC2 核心参考）

| 论文 | 文件 | 出处 |
|------|------|------|
| CONCUR: Proactive Agent-Level Admission Control for Agentic Batch Inference | `concur_2025.pdf` | arXiv:2601.22705, 2026 |
| Scorpio: Serving the Right Requests at the Right Time for Heterogeneous SLOs | `scorpio_llm_serving_2025.pdf` | arXiv:2505.23022, 2025 |
| SABER: Adaptive Request Scheduling for CodeLLM Serving with SLA Guarantees | `saber_batching_2025.pdf` | arXiv:2506.19677, 2025 |
| BucketServe: Bucket-Based Dynamic Batching for LLM Inference | `bucketserve_2025.pdf` | arXiv:2507.17120, 2025 |
| Multi-Bin Batching for Increasing LLM Inference Throughput | `multibin_batching_2024.pdf` | arXiv:2412.04504, 2024 |
| CoLoRA: A Collaborative Scheduling Framework for Multi-Tenant LoRA LLM Inference | `colora_aspdac2026.pdf` | ASP-DAC 2026, DOI:10.1109/ASP-DAC66049.2026.11420717 |
| ProServe: Unified Multi-Priority Request Scheduling for LLM Serving | `proserve_2025.pdf` | arXiv:2512.12928, 2026 |
| Load-Aware Prefill Deflection for Disaggregated LLM Serving | `load_aware_prefill_2026.pdf` | arXiv:2607.02043, 2026 |

### 1.4 分布式框架与数据引擎

| 论文 | 文件 | 出处 |
|------|------|------|
| Ray: A Distributed Framework for Emerging AI Applications | `ray_osdi2018.pdf` | OSDI 2018 |
| The Streaming Batch Model for Efficient Heterogeneous Execution (Ray Data) | `ray_data_streaming_batch_2025.pdf` | arXiv:2501.12407, 2025 |
| HybridFlow: A Flexible and Efficient RLHF Framework | `hybridflow_eurosys2025.pdf` | EuroSys 2025, arXiv:2409.19256 |
| Lance: Efficient Random Access in Columnar Storage | `lance_2025.pdf` | arXiv:2504.15247, 2025 |
| Apache Arrow DataFusion: A Fast, Embeddable Analytic Query Engine | `arrow_datafusion_sigmod2024.pdf` | SIGMOD 2024 |
| Benchmarking Apache Arrow Flight | `arrow_flight_2022.pdf` | arXiv:2204.03032, 2022 |
| DuckDB: An Embeddable Analytical Database | `duckdb_sigmod2019.pdf` | SIGMOD 2019 |
| Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores | `delta_lake_pvldb2020.pdf` | PVLDB Vol.13, 2020 |
| Petabyte-Scale Row-Level Operations in Data Lakehouses (Iceberg) | `iceberg_row_level_vldb2024.pdf` | VLDB 2024 |
| FlexPushdownDB: Hybrid Pushdown and Caching in a Cloud DBMS | `flexpushdowndb_pvldb2021.pdf` | PVLDB Vol.14, 2021 |
| Rafiki: Machine Learning as an Analytics Service System | `rafiki_pvldb2018.pdf` | PVLDB Vol.11, 2018 |

### 1.5 向量检索 / 写回 / 存储

| 论文 | 文件 | 出处 |
|------|------|------|
| DiskANN: Fast Accurate Billion-point Nearest Neighbor Search | `diskann_neurips2019.pdf` | NeurIPS 2019, arXiv:1811.01324 |
| Milvus: A Purpose-Built Vector Data Management System | `milvus_sigmod2021.pdf` | SIGMOD 2021, DOI:10.1145/3448016.3457550 |
| Turbocharging Vector Databases Using Modern SSDs | `turbocharging_vector_db_ssd_pvldb2025.pdf` | PVLDB Vol.18, 2025 |
| BigVectorBench: Evaluating Vector Databases | `bigvectorbench_vldb2025.pdf` | VLDB 2025 |
| WiscKey: Separating Keys from Values in SSD-conscious Storage | `wisckey_fast2016.pdf` | FAST 2016 |
| Algorithms for Deferred View Maintenance | `deferred_view_maintenance_sigmod1996.pdf` | SIGMOD 1996（经典，支撑 pgvector deferred index） |
| An Empirical Evaluation of Columnar Storage Formats | `columnar_storage_eval_pvldb2023.pdf` | PVLDB Vol.17, 2023, arXiv:2304.05028 |

### 1.6 综述 / Tutorial / 学习型优化

| 论文 | 文件 | 出处 |
|------|------|------|
| Database Perspective on LLM Inference Systems | `db_perspective_llm_pvldb2025.pdf` | PVLDB Vol.18, 2025 |
| LLM for Data Management | `llm4dm_pvldb2024.pdf` | PVLDB Vol.17, 2024 |
| Trustworthy and Efficient LLMs Meet Databases | `trustworthy_efficient_llms_db_vldb2024.pdf` | VLDB 2024 Tutorial |
| Vector Database Management Techniques and Systems | `vdbms_tutorial_vldb2024.pdf` | VLDB 2024 Tutorial |
| Neo: A Learned Query Optimizer | `neo_learned_optimizer_sigmod2019.pdf` | SIGMOD 2019 |
| How Good are Learned Cost Models, Really? | `learned_cost_models_sigmod2025.pdf` | SIGMOD 2025, arXiv:2502.01229 |
| Learning Database Optimization Techniques: State-of-the-Art and Prospects | `learning_db_optimization_fcs2025.pdf` | FCS 2025, DOI:10.1007/s11704-025-41116-7 |
| RTIndeX: Hardware-Accelerated GPU Raytracing for Database Indexing | `rtindex_2023.pdf` | arXiv:2303.01139, 2023 |
| A Study of GPU/CPU Performance Characteristics for Database Analytics | `gpu_cpu_db_analytics_pvldb2020.pdf` | PVLDB Vol.13, 2020, arXiv:2003.01178 |

### 1.7 分布式数据库 / 具身智能（背景）

| 论文 | 文件 | 出处 |
|------|------|------|
| OceanBase Mercury: Distributed Nearly Real-time Analytical Processing DB | `oceanbase_mercury.pdf` | arXiv:2602.07584, 2026 |
| OceanBase Bacchus: Cloud-Native Shared Storage Architecture | `oceanbase_bacchus.pdf` | arXiv:2602.23571, 2026 |
| OceanBase: Tree-Structured Two-Phase Commit Framework | `oceanbase_2pc.pdf` | arXiv:2603.00866, 2026 |
| HedraRAG: Coordinating LLM Generation and DB Retrieval in Heterogeneous RAG | `hedrarag_sosp2025.pdf` | SOSP 2025, arXiv:2507.09138 |
| HeteroHub: Data Management for Heterogeneous Multi-Embodied Agent System | `heterohub_2025.pdf` | arXiv:2603.28010, 2026 |
| ROSA: A Robotics Foundation Model Serving System for Robot Factories | `rosa_robotics_2026.pdf` | arXiv:2607.01088, 2026 |

---

## 二、未下载论文（需机构访问或手动获取）

| 论文 | 出处 | 原因 |
|------|------|------|
| Learned Query Optimizer (Zhu et al.) | SIGMOD 2024 | ACM 付费墙 |
| Dostoevsky: Space-Time Trade-Offs for LSM-Tree KV Stores (Nikolov et al.) | SIGMOD 2018 | 原 PDF 为误下载（代数几何论文），真论文未取（写回 LSM 背景，优先级低） |

> 其余原"未下载"条目（Rafiki、Iceberg、openGauss、HybridFlow、DuckDB、Arrow DataFusion、BigVectorBench、Learned Cost Models、Deferred View Maintenance、Trustworthy LLMs×DB、Vector DBMS Tutorial、Learning DB Optimization）**均已补齐**，见 §1。

---

## 三、网页文档与系统参考资料

### 3.1 数据引擎与分布式框架

| 系统 | 文档类型 | URL |
|------|----------|-----|
| Daft — 官方文档 | 数据引擎 | https://docs.daft.ai/ |
| Daft — Partitioning & Batching | 优化指南 | https://docs.daft.ai/en/stable/optimization/partitioning/ |
| Daft — Shuffle Algorithms | 优化指南 | https://docs.daft.ai/en/stable/optimization/shuffle/ |
| Daft — Join Strategies | 优化指南 | https://docs.daft.ai/en/stable/optimization/join-strategies/ |
| Daft — Distributed Execution with Ray | 部署 | https://docs.daft.ai/en/stable/distributed/ray/ |
| Daft — GPU Inference with @daft.cls | 博客 | Daft Blog, 2025 |
| Daft — Flotilla: Multimodal Data Processing | 博客 | Daft Blog, October 2025 |
| Daft — SciPy 2024 Talk | 会议 Talk | SciPy 2024 |

### 3.2 Ray 生态系统

| 系统 | 文档类型 | URL |
|------|----------|-----|
| Ray — 官方文档 | 框架 | https://docs.ray.io/ |
| Ray Core — Objects | 核心机制 | https://docs.ray.io/en/latest/ray-core/objects.html |
| Ray Core — Accelerators (GPU) | 调度 | https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html |
| Ray Core — Placement Groups | 调度 | https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html |
| Ray — Anti-pattern: Too Fine-Grained Tasks | 最佳实践 | https://docs.ray.io/en/latest/ray-core/patterns/too-fine-grained-tasks.html |
| Ray — Anti-pattern: Pass Large Arg by Value | 最佳实践 | https://docs.ray.io/en/latest/ray-core/patterns/pass-large-arg-by-value.html |
| Ray — Anti-pattern: Return ray.put() ObjectRefs | 最佳实践 | https://docs.ray.io/en/latest/ray-core/patterns/return-ray-put.html |
| Ray Data — Overview | 数据处理 | https://docs.ray.io/en/latest/data/data.html |
| Ray Data — map_batches | API | https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html |
| Ray Serve — Dynamic Request Batching | 推理服务 | https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html |
| Ray Serve — LLM Request Routing | 推理服务 | https://docs.ray.io/en/latest/serve/llm/architecture/routing-policies.html |
| Ray Serve — Autoscaling | 推理服务 | https://docs.ray.io/en/latest/serve/autoscaling-guide.html |

### 3.3 模型推理服务

| 系统 | 文档类型 | URL |
|------|----------|-----|
| vLLM — 官方文档 | 推理引擎 | https://docs.vllm.ai/ |
| vLLM — PagedAttention | 核心机制 | SOSP 2023 Best Paper (`vllm_sosp2023.pdf`) |
| vLLM — Continuous Batching | 核心机制 | 同上 |
| NVIDIA Triton Inference Server | 推理服务 | https://developer.nvidia.com/triton-inference-server |

### 3.4 数据库与向量存储

| 系统 | 文档类型 | URL |
|------|----------|-----|
| PostgreSQL — 官方文档 | 数据库 | https://www.postgresql.org/docs/ |
| psycopg2 / psycopg 3 | Python 驱动 | https://www.psycopg.org/psycopg3/docs/ |
| pgvector — GitHub | 向量扩展 | https://github.com/pgvector/pgvector |
| pgvector — Issues #400/#430 (COPY 优化) | 性能参考 | https://github.com/pgvector/pgvector/issues |
| PostgresML — GitHub | 数据库内 ML | https://github.com/postgresml/postgresml |
| pgai (Timescale) — GitHub | 外部 vectorizer | https://github.com/timescale/pgai |
| Milvus — 官方文档 | 向量数据库 | https://milvus.io/docs/ |
| LanceDB — 官方文档 | 向量存储 | https://lancedb.github.io/lancedb/ |
| DuckDB — 官方文档 | 嵌入式分析 | https://duckdb.org/docs/ |
| Apache Arrow DataFusion | 查询引擎 | https://arrow.apache.org/datafusion/ |

### 3.5 工业 AI SQL 系统

| 系统 | 文档类型 | URL |
|------|----------|-----|
| Snowflake Cortex AI Functions | 官方文档 | https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql |
| Snowflake Cortex — Multimodal | 官方文档 | Snowflake Documentation, 2025 |
| BigQuery ML — ML.GENERATE_TEXT | 官方文档 | https://cloud.google.com/bigquery/docs/ |
| BigQuery — ML.GENERATE_EMBEDDING | 官方文档 | 同上 |
| Oracle AI Vector Search | 官方文档 | https://docs.oracle.com/en/database/oracle/ |
| Oracle — VECTOR_EMBEDDING | SQL 函数 | 同上 |

### 3.6 数据处理与存储

| 系统 | 文档类型 | URL |
|------|----------|-----|
| Apache Spark — SQL Performance Tuning | 优化指南 | https://spark.apache.org/docs/latest/sql-performance-tuning.html |
| Apache Arrow | 列式格式 | https://arrow.apache.org/ |
| Apache Parquet | 列式存储 | https://parquet.apache.org/ |
| Delta Lake | Lakehouse | https://delta.io/ |
| Apache Iceberg | Lakehouse | https://iceberg.apache.org/ |

### 3.7 具身智能与多模态（Daft+Ray）

| 资料 | 类型 | URL |
|------|------|------|
| 阿里云 — EMR Serverless Daft 多模态数据处理 | 技术文章 | 阿里云开发者社区, 2025 |
| IBM — The Data Gap Holding Back Robotics | 技术博客 | IBM Think Blog, 2025 |

### 3.8 本项目实验报告（自引）

| 报告 | 路径 |
|------|------|
| GPU-Backed AI_EMBED Chain Breakdown (2026-07-12) | `motivation/results/gpu/` |
| PGAI-Integrated GPU-Backed Key Rerun (2026-07-14) | `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md` |
| GPU-Backed pgvector(384) Writeback Test (2026-07-14) | `motivation/results/gpu/pgvector_writeback_20260714.md` |

---

## 四、统计

| 类别 | 数量 |
|------|------|
| ✅ 已下载 PDF 论文 | **67** |
| ❌ 未下载（需机构访问/手动获取） | **2** |
| 📄 网页文档/系统参考 | **40+** |

---

## 附：2026-07-23 PDF 规范化记录

- 全部 PDF 改名为 `短名_会议年份.pdf`，与精读笔记一一对应（如 `vllm_sosp2023.pdf` ↔ `vllm_sosp2023.md`）。
- 删除误下载 3 篇：`diskann_neurips2019.pdf`（原为凝聚态物理论文）、`milvus_sigmod2021.pdf`（原为 IR 词典翻译论文）、`dostoevsky_sigmod2018.pdf`（原为代数几何论文）——arXiv ID 被重新分配导致。
- 删除重复 2 篇：FlashAttention、FlexGen 各保留正式会议命名副本。
- 补齐：真 Milvus（SIGMOD 2021, DOI:10.1145/3448016.3457550）、Clipper（NSDI 2017）、CoLoRA（ASP-DAC 2026，注意与 CNN-PEFT / PDE-降阶 / LoRa-网络三个同名论文区分）。
- 15 个 git 跟踪文件用 `git mv` 暂存为 rename（保留历史）。
