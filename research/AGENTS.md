# research/AGENTS.md

本目录维护背景调研、资料依据。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放文献/官方文档依据、相关系统背景。
- 为开题、论文动机、导师沟通提供可追溯材料。

## 写作规则

- 结论必须标注来源类型：论文、官方文档、本地实验、合理推断、待确认。
- 不把 microbenchmark 结论直接扩展成系统性结论。
- 不把 Daft/Ray/Lance 产品化适配写成既定事实，除非已有接口或实验验证。
- 优先保留可引用的一手资料：论文、官方文档、源码、实验结果。

## 当前重点

- Ray object store / ObjectRef / small object anti-pattern。
- Daft Ray runner、partition、shuffle、join strategy。
- Arrow RecordBatch / Lance 作为 AI 数据链路中的列式数据形态。
- 数据库 AI 算子与批量 embedding / RAG 数据准备的关系。
