# 项目参考资料索引

更新日期：2026-07-29
本索引只把当前工作区实际存在且可解析的文件列为“已下载”。

## 一、Top 15 正式论文（15 份）

| 文件 | 正式题录 | DOI / 官方入口 | 分类 |
|---|---|---|---|
| `lotus_pvldb2025.pdf` | Patel et al. *Semantic Operators and Their Optimization: Enabling LLM-Based Data Processing with Accuracy Guarantees in LOTUS*. PVLDB 18(11): 4171–4184, 2025 | 10.14778/3749646.3749685 | AI 算子 |
| `galois_sigmod2025.pdf` | Satriani et al. *Logical and Physical Optimizations for SQL Query Execution over Large Language Models*. SIGMOD 2025 | 10.1145/3725411 | AI 算子 |
| `gaussml_icde2024.pdf` | Li et al. *GaussML: An End-to-End In-Database Machine Learning System*. ICDE 2024, 5198–5210 | 10.1109/ICDE60146.2024.00391 | AI 算子 |
| `vllm_sosp2023.pdf` | Kwon et al. *Efficient Memory Management for Large Language Model Serving with PagedAttention*. SOSP 2023 | 10.1145/3600006.3613165 | Serving |
| `orca_osdi2022.pdf` | Yu et al. *Orca: A Distributed Serving System for Transformer-Based Generative Models*. OSDI 2022 | USENIX OSDI 2022 | Serving |
| `sarathi_serve_osdi2024.pdf` | Agrawal et al. *Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve*. OSDI 2024 | USENIX OSDI 2024 | Serving |
| `sglang_neurips2024.pdf` | Zheng et al. *SGLang: Efficient Execution of Structured Language Model Programs*. NeurIPS 2024 | arXiv:2312.07104 | Serving |
| `vtc_osdi2024.pdf` | Sheng et al. *Fairness in Serving Large Language Models*. OSDI 2024 | USENIX OSDI 2024 / arXiv:2401.00588 | 公平调度 |
| `llumnix_osdi2024.pdf` | Sun et al. *Llumnix: Dynamic Scheduling for Large Language Model Serving*. OSDI 2024 | USENIX OSDI 2024 | 动态调度 |
| `distserve_osdi2024.pdf` | Zhong et al. *DistServe: Disaggregating Prefill and Decoding for Goodput-optimized Large Language Model Serving*. OSDI 2024 | USENIX OSDI 2024 | Serving |
| `ray_osdi2018.pdf` | Moritz et al. *Ray: A Distributed Framework for Emerging AI Applications*. OSDI 2018 | arXiv:1712.05889 | Ray |
| `learned_cost_models_sigmod2025.pdf` | Heinrich et al. *How Good are Learned Cost Models, Really? Insights from Query Optimization Tasks*. SIGMOD 2025 | arXiv:2502.01229 | 代价估计 |
| `graceful_udf_cost_icde2025.pdf` | Wehrstein et al. *GRACEFUL: A Learned Cost Estimator for UDFs*. ICDE 2025, 2450–2463 | 10.1109/ICDE65448.2025.00185 | 代价估计 |
| `costream_icde2024.pdf` | Heinrich et al. *COSTREAM: Learned Cost Models for Operator Placement in Edge-Cloud Environments*. ICDE 2024, 96–109 | 10.1109/ICDE60146.2024.00015 | 代价估计 |
| `abacus_pvldb2026.pdf` | Russo et al. *Abacus: A Cost-Based Optimizer for Semantic Operator Systems*. PVLDB 19(5): 1060–1073, 2026 | 10.14778/3796195.3796215 | 代价优化 |

以上 15 篇均按正式 CCF-A research paper 计数。Top 15 选择依据见 `../top15_ranked_papers.md`。

## 二、核心补充论文（6 份）

| 文件 | 核验题录 | 级别 | 项目角色 |
|---|---|---|---|
| `palimpzest_cidr2025.pdf` | Liu et al. *Palimpzest: Optimizing AI-Powered Analytics with Declarative Query Processing*. CIDR 2025 | 非 CCF-A | 声明式 AI 数据处理、系统 baseline |
| `sembench_pvldb2026.pdf` | Lao et al. *SemBench: A Benchmark for Semantic Query Processing Engines*. PVLDB 19(8): 1754–1767, 2026；DOI 10.14778/3811243.3811249 | 正式 benchmark paper | workload、指标、跨系统比较依据 |
| `fairserve_arxiv2024.pdf` | Khan et al. *Ensuring Fair LLM Serving Amid Diverse Applications*. arXiv 2024 | 预印本 | weighted service / interaction throttling |
| `dlpm_arxiv2025.pdf` | Cao et al. *Locality-aware Fair Scheduling in LLM Serving*. arXiv:2501.14312 | 预印本 | deficit fairness + prefix locality |
| `autellix_arxiv2025.pdf` | Luo et al. *Autellix: An Efficient Serving Engine for LLM Agents as General Programs*. arXiv:2502.13965 | 预印本 | program/job-level scheduling |
| `chiron_arxiv2025.pdf` | Patke et al. *Hierarchical Autoscaling for Large Language Model Serving with Chiron*. arXiv:2501.08090 | 预印本 | 分层 backpressure/autoscaling |

## 三、重要题录但不占 Top 15

| 文献 | 轨道判断 | 用途 |
|---|---|---|
| *Database Perspective on LLM Inference Systems* | PVLDB 2025 Tutorial | 定位框架和 open problems；不计 research paper |
| NeurDB | CIDR 2025 | AI-native database vision；不写成 CCF-A |
| Cortex AISQL | 依据正式 Companion/工业轨道标注 | 工业 AI SQL 需求；不自动计 CCF-A full paper |
| Ray Data Streaming Batch | arXiv 2025 | 数据引擎执行模型；预印本 |
| FairServe、DLPM、Autellix、Chiron | arXiv | 算法候选；不占正式 Top 15 |

## 四、网页与系统资料入口

| 资料 | 用途 | 入口 |
|---|---|---|
| vLLM docs | model serving 配置、指标和 benchmark | https://docs.vllm.ai/ |
| Ray Core / Data docs | actor/task、Data LLM、HttpRequestProcessor | https://docs.ray.io/ |
| Daft docs | `prompt()`、Ray runner、partition 和 concurrency | https://docs.daft.ai/ |
| LOTUS | semantic operator API 与复现 | https://lotus-ai.readthedocs.io/ |
| Palimpzest | 官方实现与配置 | 以论文/官方仓库为准 |
| SemBench | workload 与系统比较 | 以 PVLDB 论文及 artifact 为准 |
| PolarDB Daft on Ray | 工业 Daft/Ray 背景 | https://help.aliyun.com/zh/polardb/polardb-for-postgresql/what-is-daft-on-ray |

## 五、统计与一致性

- 当前 PDF：21。
- Top 15 PDF：15/15。
- Top 15 精读：15/15。
- 新增 PDF 已通过 `pypdf` 页面解析检查。
- “当前未在工作区出现”的旧 PDF 只保留在历史 Git/旧日志，不再计入本地下载统计。
