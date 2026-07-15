# Local Reference PDF Subset

整理日期：2026-07-15

本目录保存用户已下载到本地的**部分参考文献 PDF**。它不是完整文献库；完整候选文献仍以 `opening/literature/ai_operator_literature_inventory.md` 为准。本目录的作用是支持后续精读、看论文机制图、提取图形设计范式和核验引用细节。

## 当前文件清单

| 文件 | 初步识别 | 页数 | 主要用途 |
|---|---|---:|---|
| `2501.12407v5.pdf` | The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution | 19 | Ray Data / heterogeneous execution；学习 pipeline、resource lane、timeline 图 |
| `2502.13805v1.pdf` | AnDB: Breaking Boundaries with an AI-Native Database for Universal Semantic Analysis | 4 | AI-native database demo；数据库 AI 系统架构对照 |
| `2504.15247v1.pdf` | Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings | 13 | AI 数据存储 / columnar layout；学习存储格式对比图 |
| `3600006.3613165.pdf` | Efficient Memory Management for Large Language Model Serving with PagedAttention | 16 | vLLM；学习机制总览、局部机制放大、吞吐-延迟表达 |
| `3725411.pdf` | Logical and Physical Optimizations for SQL Query Execution over Large Language Models | 28 | Galois；学习 SQL running example、logical/physical plan 变体图 |
| `gaussmlicde.pdf` | GaussML: An End-to-End In-Database Machine Learning System | 17 | DB4AI / in-database ML；作为数据库内部优化路线对照 |
| `osdi18-moritz.pdf` | Ray: A Distributed Framework for Emerging AI Applications | 18 | Ray 基础设施；学习 code example -> task graph -> system layer 的表达 |
| `osdi24-agrawal.pdf` | Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve | 19 | Sarathi-Serve；学习吞吐-延迟权衡、prefill/decode 阶段拆分图 |
| `osdi24-fu.pdf` | ServerlessLLM: Low-Latency Serverless Inference for Large Language Models | 20 | Serverless LLM serving；学习系统架构、locality policy、migration 流程图 |
| `p2514-li.pdf` | D-Bot: Database Diagnosis System using Large Language Models | 14 | LLM for database diagnosis；辅助理解 DB+LLM 系统图 |
| `p29-zhao.pdf` | NeurDB: On the Design and Implementation of an AI-powered Autonomous Database | 8 | AI-powered autonomous DB；数据库 AI 架构对照 |
| `p4213-li.pdf` | LLM for Data Management | 4 | VLDB tutorial；研究版图和系统分层参考 |
| `p4813-zeng.pdf` | Powering In-Database Dynamic Model Slicing for Structured Data Analytics | 14 | LEADS；学习 in-database inference workflow 和 UDF 执行拆解 |
| `p5504-li.pdf` | Database Perspective on LLM Inference Systems | 4 | LLM inference stack；用于定位推理系统问题层次 |

## 使用规则

- 这些 PDF 是当前本地可读子集，不代表文献调研已经完整。
- 引用前仍需核验作者、标题、会议、年份、DOI 或 arXiv 编号。
- 看图时优先提取：running example、系统边界、data/control path、timeline、规则表、实验曲线表达。
- 后续如继续下载 PDF，追加到本文件，不覆盖已有记录。

