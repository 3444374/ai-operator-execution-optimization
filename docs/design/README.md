# docs/design/

本项目的设计选择：各部分采用什么设计、怎样协作、为什么。与 `docs/research/` 的分工是：
`research/` 讨论可选方案与依据，`design/` 说明本项目已经采用的设计；候选机制仍在比较时留在
研究材料中，决定采用后把职责、接口、数据流和错误处理整理到这里，并引用研究依据。

## 当前内容

本目录刚随 2026-09-21 的全仓库重组建立，现有设计说明分布在既有权威文档中，按主题对应当前入口：

| 主题 | 当前权威入口 |
|---|---|
| 数据库、算子方法、外部执行层与模型服务的分工 | [`../plans/postgresql_ai_semantic_operator_architecture_20260827.md`](../plans/postgresql_ai_semantic_operator_architecture_20260827.md)（总体架构与实施安排） |
| 算子的输入输出、行关联、NULL、错误及方法行为 | [`../plans/postgresql_semmap_generation_contract.md`](../plans/postgresql_semmap_generation_contract.md) 与 `code/postgres/semloom_pg/README.md` |
| SQL 接入、计划与执行生命周期、权限、取消与事务 | [`../plans/postgresql_call_binding_design.md`](../plans/postgresql_call_binding_design.md)、[`../plans/postgresql_query_job_design.md`](../plans/postgresql_query_job_design.md) |
| 请求的接纳、推进、完成、交付与资源释放 | [`../plans/semloom_incremental_session_design.md`](../plans/semloom_incremental_session_design.md)、[`../plans/semloom_multisession_design.md`](../plans/semloom_multisession_design.md) |
| 输入如何形成工作描述、如何组织和保持关联 | [`../plans/data_organization_batching.md`](../plans/data_organization_batching.md) 的设计主张部分 |
| 提交控制、资源分配、路由、多 Job 协作 | [`../plans/service_scheduling_backpressure.md`](../plans/service_scheduling_backpressure.md)、[`../plans/baseline_reference.md`](../plans/baseline_reference.md) |
| 代价估计估计什么、怎样估计、由哪些策略使用 | [`../plans/completed/operator_cost_profile_dual4090_formal_20260804.md`](../plans/completed/operator_cost_profile_dual4090_formal_20260804.md) 与 `code/src/planning/` |
| 代码为什么这样分层 | [`../../code/code_architecture_guide.md`](../../code/code_architecture_guide.md) |

后续整理方向是把上表入口中的机制说明逐步消化成对应主题文档（`architecture.md`、
`semantic_operators.md`、`postgresql_integration.md`、`execution.md`、`data_organization.md`、
`scheduling.md`、`cost_estimation.md`），某主题内容不多时先用一个文件，不急着增加子目录；
进入本目录不代表已经完成实现。

## history/

[`history/`](history/) 保存已完成的一次性设计与实施记录（原 `code_doc/`、`docs/superpowers/`
与早期学习讲解）：只作历史追溯，文中“当前”“下一步”或旧目录路径均按文件日期读取，
不覆盖当前计划、总纲或 runbook。本目录规则见 [`AGENTS.md`](AGENTS.md)。
