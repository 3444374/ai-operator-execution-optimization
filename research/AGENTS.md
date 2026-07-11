# research/AGENTS.md

本目录维护背景调研、文献和官方资料依据。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放论文、官方文档、源码依据和相关系统背景。
- 为开题、论文动机、导师沟通提供可追溯材料。
- 把本地实验结果与外部已有证据对应起来。

## 写作规则

- 结论必须标注来源类型：论文、官方文档、源码、本地实验事实、模拟实验事实、合理推断、待确认。
- 不把 microbenchmark 结论直接扩展成系统性结论。
- 不把 Daft/Ray/Lance 产品化适配写成既定事实，除非已有接口或实验验证。
- 不把 PG18.4 本地结果写成 PG18.3 内部平台结果。
- 优先保留可引用的一手资料：论文、官方文档、源码、真实实验结果。

## 当前重点

- Ray object store / ObjectRef / small object anti-pattern。
- Ray task、actor、object fan-in 与 queue/backpressure。
- Daft Ray runner、partition、shuffle、join strategy。
- Arrow RecordBatch / Lance 作为 AI 数据链路中的列式数据形态。
- PostgreSQL AI 算子、外部 worker、pgvector / vector writeback。
- 数据库 AI 算子与批量 embedding / RAG 数据准备的关系。

## 当前需要补的依据

- Daft/Ray/Lance 是否适合作为“数据库 AI 算子外部执行系统”的论文定位依据。
- Ray task baseline、Ray actor baseline、Python batched baseline 的外部参考或官方 anti-pattern 依据。
- pgvector 写回、vector storage、批量插入成本相关资料。
- 真实模型服务 / Ray Serve / vLLM 的 batching、queue wait、backpressure 依据。
