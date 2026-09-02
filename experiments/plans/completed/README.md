# 已完成实验计划

本目录保存主要门禁、运行或数据采集已经完成的实验合同。计划文本用于解释当时如何运行，真实
结果、有效性和结论边界以对应 `../../results/` 目录为准。

| 文件 | 完成状态 | 结果入口 |
|---|---|---|
| [`postgresql_choice_profile_engineering.md`](postgresql_choice_profile_engineering.md) | 四 C 工程验证完成：SQL/plan/wire、受控资源与受限真实服务；当前代码已集成，质量/校准未通过 | [真实服务及收尾](../../results/postgresql/choice_service_20260902/README.md)、[受控资源](../../results/postgresql/choice_resources_20260902/README.md) |
| [`image_clip_workload_lock_20260731.md`](image_clip_workload_lock_20260731.md) | 图像 workload、5K 画像、原生静态 baseline 和 matched-resource 证据已完成；动态部分迁入当前 state-aware 总合同 | [`image_ai_embed_operator_formal_20260803/`](../../results/image_ai_embed_operator_formal_20260803/)、[`motivation/results/gpu/`](../../../motivation/results/gpu/) |
| [`operator_cost_profile_pilot_20260804.md`](operator_cost_profile_pilot_20260804.md) | pilot 采样合同完成 | [`operator_cost_profile_pilot_20260804/`](../../results/operator_cost_profile_pilot_20260804/) |
| [`operator_cost_profile_dual4090_formal_20260804.md`](operator_cost_profile_dual4090_formal_20260804.md) | 首次 formal 因共享竞争/local Ray 被排除；修复版 320/320 有效 | [首次事故证据](../../results/operator_cost_profile_dual4090_formal_20260804/)、[v2 有效结果](../../results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/) |
| [`rc1_data_organization_rerun_20260731.md`](rc1_data_organization_rerun_20260731.md) | 双/四 endpoint cache-on 系统重测完成 | [`rc1_data_organization/`](../../results/rc1_data_organization/) |
| [`text_native_baseline_rerun_20260802.md`](text_native_baseline_rerun_20260802.md) | 当前开题范围的 capability、单 Job 和多 Job 原生观察完成 | [`opening_text_native_gate_20260808/`](../../results/opening_text_native_gate_20260808/)、[`opening_text_native_single_job_formal_20260808/`](../../results/opening_text_native_single_job_formal_20260808/) |

这些文件不再提供当前“下一步”。未完成的系统级比较、数据库语义算子和动态调度工作分别由顶层
[`../state_aware_work_unit_evaluation_20260808.md`](../state_aware_work_unit_evaluation_20260808.md)、
[`../postgresql_ai_semantic_operator_architecture_20260827.md`](../postgresql_ai_semantic_operator_architecture_20260827.md)
等当前计划维护。
