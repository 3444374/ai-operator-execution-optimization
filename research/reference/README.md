# Local Reference PDF Subset

整理日期：2026-07-15 / 更新：2026-07-23（全量规范化改名 + 误下载/重复清理，详见 REFERENCE_INDEX.md 附录）

本目录保存用户已下载到本地的**部分参考文献 PDF**。它不是完整文献库；完整候选文献仍以 `research/ai_operator_literature_inventory.md` 为准。本目录的作用是支持后续精读、看论文机制图、提取图形设计范式和核验引用细节。

## 当前文件清单

| 文件 | 初步识别 | 页数 | 主要用途 |
|---|---|---:|---|
| `ray_data_streaming_batch_2025.pdf` | The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution | 19 | Ray Data / heterogeneous execution；学习 pipeline、resource lane、timeline 图 |
| `anddb_sigmod2025_demo.pdf` | AnDB: Breaking Boundaries with an AI-Native Database for Universal Semantic Analysis | 4 | AI-native database demo；数据库 AI 系统架构对照 |
| `lance_2025.pdf` | Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings | 13 | AI 数据存储 / columnar layout；学习存储格式对比图 |
| `vllm_sosp2023.pdf` | Efficient Memory Management for Large Language Model Serving with PagedAttention | 16 | vLLM；学习机制总览、局部机制放大、吞吐-延迟表达 |
| `galois_sigmod2025.pdf` | Logical and Physical Optimizations for SQL Query Execution over Large Language Models | 28 | Galois；学习 SQL running example、logical/physical plan 变体图 |
| `gaussml_icde2024.pdf` | GaussML: An End-to-End In-Database Machine Learning System | 17 | DB4AI / in-database ML；作为数据库内部优化路线对照 |
| `ray_osdi2018.pdf` | Ray: A Distributed Framework for Emerging AI Applications | 18 | Ray 基础设施；学习 code example -> task graph -> system layer 的表达 |
| `sarathi_serve_osdi2024.pdf` | Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve | 19 | Sarathi-Serve；学习吞吐-延迟权衡、prefill/decode 阶段拆分图 |
| `serverlessllm_osdi2024.pdf` | ServerlessLLM: Low-Latency Serverless Inference for Large Language Models | 20 | Serverless LLM serving；学习系统架构、locality policy、migration 流程图 |
| `dbot_pvldb2024.pdf` | D-Bot: Database Diagnosis System using Large Language Models | 14 | LLM for database diagnosis；辅助理解 DB+LLM 系统图 |
| `neurdb_cidr2025.pdf` | NeurDB: On the Design and Implementation of an AI-powered Autonomous Database | 8 | AI-powered autonomous DB；数据库 AI 架构对照 |
| `llm4dm_pvldb2024.pdf` | LLM for Data Management | 4 | VLDB tutorial；研究版图和系统分层参考 |
| `leads_pvldb2024.pdf` | Powering In-Database Dynamic Model Slicing for Structured Data Analytics | 14 | LEADS；学习 in-database inference workflow 和 UDF 执行拆解 |
| `db_perspective_llm_pvldb2025.pdf` | Database Perspective on LLM Inference Systems | 4 | LLM inference stack；用于定位推理系统问题层次 |

## 2026-07-22 新增文件

| 文件 | 初步识别 | 页数 | 主要用途 | 精读笔记 |
|---|---|---|---|---|
| `diskann_neurips2019.pdf` | ✅ DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node（2026-07-22 重新下载，已修复） | 18 | 向量检索 / SSD 索引 | `diskann_neurips2019.md` |
| `inferdb_pvldb2024.pdf` | InferDB: In-Database ML Inference Using Indexes | 12 | DB4AI 对照路线——索引替代推理 | `inferdb_pvldb2024.md` |
| `orca_osdi2022.pdf` | Orca: A Distributed Serving System for Transformer-Based Generative Models | 15 | LLM 推理服务——iteration-level scheduling | `orca_osdi2022.md` |
| `smartlite_pvldb2024.pdf` | SmartLite: A DBMS-Based Serving System for DNN Inference | 14 | DB4AI 对照路线——边缘 in-DBMS 推理 | `smartlite_pvldb2024.md` |
| `turbocharging_vector_db_ssd_pvldb2025.pdf` | Turbocharging Vector Databases Using Modern SSDs | ? | 向量数据库 SSD 优化 | — |
| `columnar_storage_eval_pvldb2023.pdf` | An Empirical Evaluation of Columnar Storage Formats | ? | 列式存储格式评估 | — |
| `cortex_aisql_sigmod2026.pdf` | Cortex AISQL（与已下载 arXiv 版本内容相同） | 19 | DB4AI 产业系统 | 已有 `cortex_aisql_sigmod2026.md` |
| `aidb_deem_sigmod2024.pdf` | AIDB: A Sparsely Materialized Database for Queries using ML | ? | 稀疏物化 + ML 查询 | — |
| `smart_vldbj2025.pdf` | Smart: In-Database Query Optimization on SQL with ML Predicates | ? | DB4AI ML 谓词优化 | 已有 `smart_vldb_journal_2025.md` |

> **注意**：完整参考资料索引已迁移至 `REFERENCE_INDEX.md`（**68 篇**已下载 PDF + 2 篇未下载 + 40+ 系统文档）。本 README 不再作为唯一索引入口。

## 精读笔记索引

全部 33 篇精读笔记位于 `research/reading_notes/`，详见 `reading_list.md`。本目录中文档与精读笔记的对应关系见上表"精读笔记"列。

## 使用规则

- 这些 PDF 是当前本地可读子集，不代表文献调研已经完整。
- 引用前仍需核验作者、标题、会议、年份、DOI 或 arXiv 编号。
- 看图时优先提取：running example、系统边界、data/control path、timeline、规则表、实验曲线表达。
- 后续如继续下载 PDF，追加到本文件，不覆盖已有记录。

