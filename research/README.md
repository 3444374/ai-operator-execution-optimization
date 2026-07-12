# Research Directory

本目录保存背景调研、论文、官方文档和外部系统证据。这里的材料用于支撑开题、方向判断和实验设计，不存放原始实验 CSV。

## 重点入口

| 文件 | 作用 |
|---|---|
| `literature_and_evidence_review.md` | Ray / Daft / Lance / Snowflake / pgai 等方向的综合证据记录 |
| `existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子与 AI 数据处理系统的执行链路对比 |

## 使用规则

- 优先引用官方文档、论文、源码 README 和本项目真实实验结果。
- 外部系统只作为背景、对照路线和实验设计参考；不能把闭源系统的内部实现当成已知事实。
- Snowflake 这类托管闭源系统不作为本地必测 baseline，除非后续有账号、预算和明确的用户可见 SQL benchmark 目标。
- pgai / PostgresML / pgvector 可以作为 PostgreSQL 生态路线参考，但是否纳入实验要看它能否回答本项目的链路瓶颈问题。

