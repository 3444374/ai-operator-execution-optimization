# 数据库 AI 算子相关文献清单

整理日期：2026-07-16（v3，扩充至 57 篇，新增写回/持久化方向 12 篇）
2026-07-17 更新（v4）：新增 Daft+Ray 多模态 + 具身智能方向 8 篇，总计 65 篇
2026-07-24 更新（v5）：补 Clockwork (OSDI 2020)，总计 66 篇
用途：开题报告 §2 国内外研究现状、参考文献
精读：33 篇 | 引用：66 篇
优先级：CCF-A > CCF-B > 顶会（CIDR/OSDI/SOSP/NeurIPS/ISCA/EuroSys）> 产业系统 > arXiv 预印本

---

## 一、建议精读（15 篇，全部 CCF-A 或顶会）

### 第一组：数据库 AI 算子（5 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 1 | Aggarwal, Chen, Datta, Han et al. **Cortex AISQL: A Production SQL Engine for Unstructured Data.** SIGMOD Companion 2026. arXiv:2511.07663. | SIGMOD | A |
| 2 | Guo, Li, Hu, Wang. **In-database query optimization on SQL with ML predicates.** VLDB Journal, Vol.34(1), Article 12, 2025. DOI:10.1007/s00778-024-00888-3 | VLDB Journal | A |
| 3 | Li, Sun, Li, Wang, Nie, Xu. **GaussML: An End-to-End In-database Machine Learning System.** ICDE 2024. | ICDE | A |
| 4 | Satriani, Veltri, Santoro, Rosato, Varriale, Papotti. **Galois: Logical and Physical Optimizations for SQL Query Execution over LLMs.** SIGMOD 2025. DOI:10.1145/3725411 | SIGMOD | A |
| 5 | Zhao, Cai, Ooi et al. **NeurDB: On the Design and Implementation of an AI-powered Autonomous Database.** CIDR 2025. | CIDR | 顶会 |

### 第二组：数据库内 AI 推理（3 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 6 | Zeng, Xing, Cai, Chen, Ooi, Pei, Wu. **LEADS: Powering In-Database Dynamic Model Slicing for Structured Data Analytics.** PVLDB Vol.17, pp.4813-4826, 2024. | VLDB | A |
| 7 | Salazar-Díaz, Glavic, Rabl. **InferDB: In-Database Machine Learning Inference Using Indexes.** PVLDB Vol.17, pp.1830-1842, 2024. | VLDB | A |
| 8 | Lin, Wu, Zhao, Dai, Shi, Chen, Li. **SmartLite: A DBMS-Based Serving System for DNN Inference in Resource-Constrained Environments.** PVLDB, 2024. | VLDB | A |

### 第三组：AI 推理服务系统（4 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 9 | Kwon, Li, Zhuang, Sheng, Zheng, Yu, Gonzalez, Zhang, Stoica. **vLLM: Efficient Memory Management for Large Language Model Serving with PagedAttention.** SOSP 2023. Best Paper. DOI:10.1145/3600006.3613165 | SOSP | A |
| 10 | Yu, Jeong, Kim, Kim, Chun. **Orca: A Distributed Serving System for Transformer-Based Generative Models.** OSDI 2022. | OSDI | A |
| 11 | Agrawal, Kedia, Panwar et al. **Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve.** OSDI 2024. | OSDI | A |
| 12 | Fu, Xue, Huang, Brabete, Ustiugov, Patel, Mai. **ServerlessLLM: Low-Latency Serverless Inference for Large Language Models.** OSDI 2024. | OSDI | A |

### 第四组：综述与 Tutorial（2 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 13 | Li, Zhou, Zhao. **LLM for Data Management.** PVLDB Vol.17, pp.4213-4216, 2024. | VLDB | A |
| 14 | Pan, Li. **Database Perspective on LLM Inference Systems.** PVLDB Vol.18, 2025. | VLDB | A |

### 第五组：分布式基础设施（1 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 15 | Moritz, Nishihara, Wang, Tumanov, Liaw, Liang, Elibol, Yang, Paul, Jordan, Stoica. **Ray: A Distributed Framework for Emerging AI Applications.** OSDI 2018. | OSDI | A |

### 第六组：结果持久化与写回优化（4 篇，新增）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 46 | Zeng, Hui, Shen, Pavlo, McKinney, Zhang. **An Empirical Evaluation of Columnar Storage Formats.** PVLDB Vol.17, 2023. | VLDB | A |
| 47 | (Authors). **Turbocharging Vector Databases Using Modern SSDs.** PVLDB, Vol.18, 2025. DOI:10.14778/3749646.3749724. | VLDB | A |
| 48 | Subramanya, Devvrit, Kadekodi, Krishnaswamy, Simhadri. **DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node.** NeurIPS 2019. | NeurIPS | A |
| 49 | Jin, Liu, Zhou, Interlandi, Krishnan, Haynes. **AIDB: A Sparsely Materialized Database for Queries using ML.** DEEM@SIGMOD 2024. | SIGMOD | A |

---

## 二、补充引用（30 篇，构建 45 篇参考文献）

### A. 数据库 AI 算子与学术系统（CCF-A，8 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 16 | Wang, Xue et al. **AnDB: Breaking Boundaries with an AI-Native Database for Universal Semantic Analysis.** SIGMOD 2025 Demo. arXiv:2502.13805. | SIGMOD | A |
| 17 | Zhu, Wu, Ding, Zhou. **Learned Query Optimizer: What is New and What is Next.** SIGMOD 2024. | SIGMOD | A |
| 18 | Heinrich, Luthra, Wehrstein, Kornmayer, Binnig. **How Good are Learned Cost Models, Really? Insights from Query Optimization Tasks.** SIGMOD 2025. | SIGMOD | A |
| 19 | Zhou, Li, Sun et al. **D-Bot: Database Diagnosis System using Large Language Models.** PVLDB Vol.17, 2024. | VLDB | A |
| 20 | Li et al. **openGauss: An Autonomous Database System.** PVLDB Vol.14, 2021. | VLDB | A |
| 21 | Qiao, Fan, Han et al. **Learning Database Optimization Techniques: The State-of-the-Art and Prospects.** Frontiers of Computer Science, 2025. | — | 综述 |
| 22 | Kim, Ailamaki. **Trustworthy and Efficient LLMs Meet Databases.** VLDB 2024 Tutorial. | VLDB | A |
| 23 | Lee et al. **Vector Database Management Systems: A Tutorial.** VLDB 2024. | VLDB | A |

### B. 推理服务系统（CCF-A，9 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 24 | Zheng et al. **SGLang: Efficient Execution of Structured Language Model Programs.** NeurIPS 2024. | NeurIPS | A |
| 25 | Zhong et al. **DistServe: Disaggregating Prefill and Decoding for Goodput-optimized LLM Serving.** OSDI 2024. | OSDI | A |
| 26 | Patel, Choukse, Zhang et al. **Splitwise: Efficient Generative LLM Inference Using Phase Splitting.** ISCA 2024. | ISCA | 顶会 |
| 27 | Qin et al. **Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving.** ACM Trans. Storage, 2025. arXiv:2407.00079. | ACM TOS | A |
| 28 | Sheng, Zhang, Ye et al. **HybridFlow: A Flexible and Efficient RLHF Framework.** EuroSys 2025. DOI:10.1145/3689031.3696075 | EuroSys | A |
| 29 | Lin et al. **Parrot: Efficient Serving of LLM-based Applications with Semantic Variable.** OSDI 2024. | OSDI | A |
| 30 | DeepSeek-AI. **DeepSeek-V3 Technical Report.** arXiv:2412.19437, 2024. | arXiv | 预印本 |
| 31 | Luan, Mao, Wang et al. **The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution.** arXiv:2501.12407, 2025. | arXiv | 预印本 |
| 66 | Gujarati, Karimi, Alzayat, Hao, Kaufmann, Vigfusson, Mace. **Serving DNNs like Clockwork: Performance Predictability from the Bottom Up.** OSDI 2020. queue-adaptive flush 的"确定性/可预测调度"思想来源（knowledge_hub §5.2 引）。 | OSDI | A |

### C. 数据管线与存储系统（4 篇）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 32 | Apache Arrow. **Arrow Flight: A Framework for Fast Data Transport.** arXiv:2204.03032. | arXiv | 工业 |
| 33 | Pace, Jones, She et al. **Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings.** arXiv:2504.15247, 2025. | arXiv | 预印本 |
| 34 | Daft Documentation. **Distributed Execution with Ray / Partitioning and Batching / Shuffle Algorithms.** docs.daft.ai | — | 官方文档 |
| 35 | Apache Spark. **Spark SQL Performance Tuning Guide.** spark.apache.org | — | 官方文档 |

### D. 产业系统（非论文，需求证据，7 篇）

| # | 系统 | 关键 AI 能力 | URL |
|---|---|---|---|
| 36 | Snowflake Cortex AI | `AI_EMBED`, `AI_FILTER`, `AI_CLASSIFY`, `AI_COMPLETE`, `AI_JOIN`, `AI_AGG` | docs.snowflake.com |
| 37 | BigQuery ML/AI | `ML.GENERATE_TEXT`, `ML.GENERATE_EMBEDDING`, `AI.GENERATE` | cloud.google.com |
| 38 | Oracle AI Vector Search | `VECTOR_EMBEDDING` SQL 函数 | docs.oracle.com |
| 39 | Timescale pgai | PostgreSQL + vectorizer worker + embedding endpoint + 写回数据库 | github.com/timescale/pgai |
| 40 | PostgresML | PostgreSQL 内/近数据库 ML/AI | github.com/postgresml/postgresml |
| 41 | pgvector | PostgreSQL 向量相似度检索 | github.com/pgvector/pgvector |
| 42 | vLLM | PagedAttention, continuous batching | docs.vllm.ai |

### E. 结果持久化、存储引擎与写入优化（8 篇，新增）

| # | 论文 | 出处 | CCF |
|---|---|---|---|
| 50 | Armbrust, Das, Davidson, Ghodsi, Or, Rosen, Stoica, Xin, Zaharia. **Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores.** PVLDB Vol.13, 2020. | VLDB | A |
| 51 | Yang, Yu, Serafini, Aboulnaga, Stonebraker. **FlexPushdownDB: Hybrid Pushdown and Caching in a Cloud DBMS.** PVLDB Vol.14, 2021. | VLDB | A |
| 52 | Wang, Jiang, Dong, Hu, Ji, Koh, Li, Liu, Ma, Ooi, Shen, Tan, Wu, Xu, Zhang. **Rafiki: Machine Learning as an Analytics Service System.** PVLDB Vol.12, 2018. | VLDB | A |
| 53 | Okolnychyi, Sun, Tanimura, Spitzer, Blue et al. **Petabyte-Scale Row-Level Operations in Data Lakehouses.** PVLDB Vol.17, 2024. | VLDB | A |
| 54 | Lu, Pillai, Gopalakrishnan, Arpaci-Dusseau, Arpaci-Dusseau. **WiscKey: Separating Keys from Values in SSD-Conscious Storage.** FAST 2016. | FAST | A |
| 55 | Dayan, Idreos. **Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores.** SIGMOD 2018. | SIGMOD | A |
| 56 | Colby, Griffin, Libkin, Mumick, Trickey. **Algorithms for Deferred View Maintenance.** SIGMOD 1996. | SIGMOD | A |
| 57 | Wang et al. **Milvus: A Purpose-Built Vector Data Management System.** SIGMOD 2021. | SIGMOD | A |

### F. 本项目实验报告（自引，3 篇）

### G. Daft+Ray 多模态引擎与具身智能（8 篇，2026-07-17 新增）

本组文献用于支撑两个论点：(1) Daft+Ray 是具身智能多模态数据处理的事实标准平台之一；(2) Snowflake Cortex 已支持多模态 AI SQL 算子，数据库 AI 算子处理多模态数据是工业现实。

| # | 论文/资料 | 出处 | CCF/来源 | 与本课题关系 |
|---|---|---|---|---|
| 58 | Chia, Jay et al. **Building Daft: Python + Rust = a better distributed query engine.** SciPy 2024 Talk. | SciPy 2024 | 会议 | Daft 三层架构（API→Plan→Execute），Arrow+Rust 核心理念 |
| 59 | Luan, Mao, Wang et al. **The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution.** arXiv:2501.12407, 2025. | arXiv | 预印本 | Ray Data 的 CPU/GPU 异构执行模型，3-8× 吞吐；Daft 的直接竞品 |
| 60 | Eventual Inc. **Flotilla: Simplifying Multimodal Data Processing at Scale.** Daft Blog, October 2025. | 官方博客 | 工业 | Daft 新分布式引擎架构：每节点一个 Swordfish Worker，Ray 降级为资源层 |
| 61 | Eventual Inc. **GPU Inference with @daft.cls.** Daft Blog, 2025. | 官方博客 | 工业 | Stateful UDF + GPU 分配 + max_concurrency 机制；模型作为管线一等公民 |
| 62 | Snowflake Inc. **Cortex AI Functions: Multimodal.** Snowflake Documentation, 2025. | 官方文档 | 工业 | AI_COMPLETE/AI_EMBED/AI_CLASSIFY 对图片/视频/音频的多模态支持，数据库 AI 算子多模态化证据 |
| 63 | Alibaba Cloud. **EMR Serverless Daft 如何简化多模态数据处理：视频抽帧、清洗、标注全流程与具身智能实践.** 阿里云开发者社区, 2025. | 技术文章 | 工业 | 视频抽帧→VLM 推理→标注的完整管线；100+ 多模态算子；具身智能场景 |
| 64 | IBM Research. **The Data Gap That's Holding Back Robotics.** IBM Think Blog, 2025. | 技术博客 | 工业 | 具身智能数据基础设施缺口——为什么需要更好的数据组织与调度 |
| 65 | Tao et al. **HeteroHub: An Applicable Data Management Framework for Heterogeneous Multi-Embodied Agent System.** arXiv:2603.28010, 2025. | arXiv | 预印本 | 具身智能数据管理三层架构（Static Knowledge Hub + Training Data Fabric + Execution Stream），与数据库 AI 算子调度形成对照 |

### F. 本项目实验报告（自引，3 篇）

| # | 报告 | 路径 |
|---|---|---|
| 43 | GPU-Backed AI_EMBED Chain Breakdown + Multi-Endpoint Ray Test (2026-07-12) | `motivation/results/gpu/` |
| 44 | PGAI-Integrated GPU-Backed Key Rerun (2026-07-14) | `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md` |
| 45 | GPU-Backed pgvector(384) Writeback Test (2026-07-14) | `motivation/results/gpu/pgvector_writeback_20260714.md` |

---

## 三、CCF 等级统计

| CCF 等级 | 数量 | 说明 |
|---|---|---|
| CCF-A 会议/期刊 | 38 | SIGMOD×8, VLDB/PVLDB×14, ICDE×1, SOSP×1, OSDI×7, NeurIPS×2, EuroSys×1, ACM TOS×1, VLDB Journal×1, ISCA×1, FAST×2 |
| 顶会（非 CCF 列表） | 1 | CIDR 2025 |
| 综述 | 1 | Frontiers of CS |
| 预印本/arXiv | 5 | DeepSeek-V3, Ray Data×2（含 arXiv:2501.12407）, Lance, HeteroHub |
| 工业论文/官方文档/技术博客 | 13 | Arrow Flight, Daft×3, Spark, Snowflake×2, BigQuery, Oracle, pgai, PostgresML, pgvector, vLLM, 阿里云 EMR Daft, IBM |
| 自引 | 3 | 本项目 GPU-backed E2E 实验报告 |
| 会议 Talk | 1 | SciPy 2024 (Daft) |
| **合计** | **66** | |

---

## 四、文献地图

```
                     ┌─── 数据库 AI 算子 ───┐
                     │ Cortex AISQL [A]      │
                     │ Smart [A]  GaussML [A]│
                     │ Galois [A]  NeurDB   │
                     │ LEADS [A]  InferDB [A]│
                     │ SmartLite [A]         │
                     │ AnDB [A]  openGauss [A]│
                     │ D-Bot [A]  Mooncake [A]│
                     └───────────┬───────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌────────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
│ 推理服务系统      │    │ 分布式数据管线    │    │ 结果持久化与写回  │
│ vLLM [A]        │    │ Ray [A]          │    │ ColStorEval [A] │
│ Orca [A]        │    │ Ray Data [预印本] │    │ TurboVecDB [A]  │
│ Sarathi-Serve[A]│    │ HybridFlow [A]   │    │ DiskANN [A]     │
│ ServerlessLLM[A]│    │ Daft/Spark 文档   │    │ AIDB [A]        │
│ SGLang [A]      │    │                 │    │ Delta Lake [A]  │
│ DistServe [A]   │    │                 │    │ FlexPushdownDB[A]│
│ Splitwise [顶会] │    │                 │    │ Rafiki [A]      │
│ Parrot [A]      │    │                 │    │ WiscKey [A]     │
└─────────────────┘    └─────────────────┘    │ Dostoevsky [A]  │
         │                       │           │ Milvus [A]      │
         │                       │           │ DefViewMaint[A] │
         │                       │           │ Iceberg RW [A]  │
         └───────────────────────┼───────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   本课题：四个岛之间的   │
                    │   全链路协同优化          │
                    │   (新增写回协同维度)      │
                    └─────────────────────────┘
```
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   本课题：四个岛之间的   │
                    │   全链路协同优化          │
                    └─────────────────────────┘
```

## 五、CCF 等级说明

根据中国计算机学会(CCF)推荐国际学术会议和期刊目录(2022版)：

**数据库/数据挖掘方向：**
- CCF-A: SIGMOD, VLDB (PVLDB), ICDE, SIGKDD, ACM TODS, IEEE TKDE, VLDB Journal
- CCF-B: CIKM, DASFAA, EDBT, ICDM, SDM

**计算机系统/操作系统方向：**
- CCF-A: SOSP, OSDI, EuroSys, ACM TOCS, IEEE TC
- CCF-B: USENIX ATC, Middleware, ICAC

**人工智能方向：**
- CCF-A: NeurIPS, ICML, AAAI, IJCAI
- CCF-B: AISTATS, UAI

**计算机体系结构方向：**
- CCF-A: ISCA, ASPLOS, MICRO, HPCA
- CCF-B: ICCD, DATE, DAC
