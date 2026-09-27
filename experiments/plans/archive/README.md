# experiments/plans/archive/

本目录保存已归档的实验计划文档，不作为当前实验设计的参考。

| 文件 | 归档原因 |
|---|---|
| `research_design_catalog.md` | 2026-07-15 的 28 候选方案评估矩阵，已被 `experiment_status_and_gaps.md` 取代。保留作为设计历史参考。 |
| `database_ai_operator_baseline_matrix_20260729.md` | 文本 baseline 的历史预注册与逐日执行记录；当前分层/指标以 `../baseline_reference.md` 为准，已完成执行合同见 `../completed/text_native_baseline_rerun_20260802.md`。 |
| `msmarco_embedding_workload_20260731.md` | 文本 embedding 轻对照被图像泛化和 LOTUS 语义主线取代；当前无运行授权。 |
| [`opening_database_e2e_p0_20260807.md`](opening_database_e2e_p0_20260807.md) | 开题已结束；原三臂实验结果仍在结果目录，旧计划和条件性补测只供回查。 |
| `postgresql_lotus_ai_semantic_operator_implementation_20260821.md` | 以 LOTUS 为语义所有者的旧 PostgreSQL 主计划；已由 `../postgresql_ai_semantic_operator_architecture_20260827.md` 取代。 |
| `lotus_semantic_frontend_execution_integration_20260821.md` | LOTUS-first frontend/backend 子计划；源码审计仍可复用，当前仅作 compatibility/native baseline 历史参考。 |
| [`postgresql_ai_semantic_operator_architecture_serial_20260901.md`](postgresql_ai_semantic_operator_architecture_serial_20260901.md) | `31e2432b` 的主计划历史快照，保留串行顺序、接口原文、历史资格记录与请求前条件；现行架构和 choice 实施分别由上一层主计划与专项计划维护。 |

归档文件可能保留当时的“当前”“下一步”和旧术语，只能按文件首部状态与日期阅读；恢复任何方案前
必须重新建立当前版本、环境、数据许可、baseline 与执行授权。
