# 项目导航

更新时间：2026-10-02。本文件回答内容归属、阅读入口和旧路径定位，具体状态与数字从对应事实入口核对。

## 内容与文档职责

| 内容 | 维护位置 | 入口 |
|---|---|---|
| 项目范围、研究内容和近期顺序 | 根文件 | [项目总纲](PROJECT_OUTLINE.md)、[术语](CONTEXT.md) |
| 长期行为规则 | 根与目标路径AGENTS.md | [根规则](AGENTS.md) |
| 可复用实现、命令与测试 | `code/` | [代码](code/README.md)、[实际状态](code/INFRA_STATUS.md)、[命令](code/scripts/README.md) |
| 机器、服务与数据库准备 | `deploy/` | [运行手册](deploy/runtime/README.md) |
| 数据来源、哈希与导入说明 | `data/` | [数据入口](data/README.md) |
| 实验设计、实际结果与失败 | `experiments/` | [计划](experiments/plans/README.md)、[结果](experiments/results/README.md)、[证据台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md) |
| 文献、研究分析与方法依据 | `docs/research/` | [研究入口](docs/research/README.md)、[知识库](docs/research/knowledge_hub.md) |
| 历史工程与已结束的开题材料 | `docs/archive/` | [工程](docs/archive/engineering/README.md)、[开题](docs/archive/opening/README.md) |
| 图源、输入、导出与审查 | `figures/` | [图资产](figures/README.md) |
| 结构与关键事实变更 | 根文件 | [项目日志](PROJECT_LOG.md) |

研究文档回答已有工作、研究问题、方法选择与可验证假设。代码说明回答现有实现怎样工作，
实验报告回答实际执行了什么与数据支持什么，计划记录实施要求与待核对问题。
讲解按对象进入这些文档；沟通内容经事实核对后进入实际方案，不另维护记录或话术。

## 实现与实验的阅读入口

1. [总纲](PROJECT_OUTLINE.md)确定问题与当前顺序。
2. [代码状态](code/INFRA_STATUS.md)核对实际能力，再读对应源码。
3. [主架构计划](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)核对设计与实施要求。
4. [实验台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)定位原始报告与失败记录。

| 主题 | 入口 |
|---|---|
| 同步/增量生成Map | [语义规格](experiments/plans/postgresql_semmap_generation_contract.md)、[增量设计](experiments/plans/semloom_incremental_session_design.md) |
| 多Job与查询归属 | [多会话设计](experiments/plans/semloom_multisession_design.md)、[查询Job设计](experiments/plans/postgresql_query_job_design.md) |
| 调用与绑定 | [详细设计](experiments/plans/postgresql_call_binding_design.md) |
| baseline与计时角色 | [参照要求](experiments/plans/baseline_reference.md#执行者与计时的读法) |
| 观测指标 | [模块说明](code/src/observability/README.md#运行指标的读法)、[选择依据](docs/research/evaluation_metrics_survey_20260731.md#观察变量选择) |
| 文本Map四路径比较 | [真实模型结果](experiments/results/postgresql/text_map_four_path_comparison_20260930/README.md) |
| worker与gateway调优 | [计划](experiments/plans/text_map_preparation_tuning.md)、[worker](experiments/results/postgresql/text_map_worker_reuse_real_20260930/README.md)、[gateway](experiments/results/postgresql/text_map_gateway_lifecycle_20261001/README.md) |
| 同步/线程记账与Arrow分批 | [记账对照](experiments/results/postgresql/text_map_threaded_accounting_20261002/README.md)、[分批对照](experiments/results/postgresql/text_map_arrow_batches_20261002/README.md) |
| DuckDB历史比较 | [静态路径](experiments/results/text_db_e2e_duckdb_static_comparison_20260808/README.md)、[规模诊断](experiments/results/duckdb_direct_scale_diagnostic_20260807/README.md) |
| 动机与组件检查 | [动机](experiments/results/motivation/README.md)、[组件与能力](experiments/results/diagnostics/README.md) |
| 实验保存与恢复 | [保存规则](experiments/results/AGENTS.md)、[恢复入口](code/scripts/README.md#实验结果恢复) |

## 目录迁移

2026-10-02统一目录。历史文件日期、原始实验与运行标识、系统臂和配置版本继续按原材料解释。
原目录的日期沿用既有记录，补充报告日期分别登记。无起始日记录的历史组保留可解释的主题名。

| 旧入口 | 当前维护位置 |
|---|---|
| `research/` | [docs/research/](docs/research/README.md) |
| `feasibility/results/` | [组件与能力检查](experiments/results/diagnostics/README.md) |
| `motivation/results/` | [动机结果](experiments/results/motivation/README.md) |
| `motivation/plans/` | [动机计划](experiments/plans/motivation/README.md) |
| 两处`benchmarks/` | [统一脚本](code/scripts/benchmarks/README.md) |
| `overview/` | [根入口](README.md) |
| `docs/designs/`、`docs/plans/` | [历史工程](docs/archive/engineering/README.md) |
| `opening/`、旧`projects/` | [历史开题](docs/archive/opening/README.md) |
| 代码与指标讲解 | [代码组织](code/README.md#代码组织的读法)、[观测](code/src/observability/README.md#运行指标的读法)、[指标依据](docs/research/evaluation_metrics_survey_20260731.md#观察变量选择) |
| 实验讲解 | [对应结果](experiments/results/README.md#历史实验解读) |
| 沟通中的待核对问题 | [实际接入计划](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#company-integration-questions)、[文献清单](docs/research/ai_operator_literature_inventory.md) |

## 实验旧名定位

目录与标题使用实际对象及验证用途。下表定位旧名；来源记录与原始数据中的标识保持原含义。

| 旧目录标识 | 当前目录 |
|---|---|
| `opening_bounded_saturation_calibration_20260808` | [sharegpt_http_saturation_calibration_20260808](experiments/results/sharegpt_http_saturation_calibration_20260808/README.md) |
| `opening_database_e2e_text_20260807` | [text_db_e2e_static_paths_20260807](experiments/results/text_db_e2e_static_paths_20260807/README.md) |
| `opening_database_e2e_text_refeed_20260808` | [text_db_e2e_duckdb_static_comparison_20260808](experiments/results/text_db_e2e_duckdb_static_comparison_20260808/README.md) |
| `opening_fourjob_interference_20260809` | [text_four_job_interference_20260809](experiments/results/text_four_job_interference_20260809/README.md) |
| `opening_image_native_fourjob_formal_20260810` | [image_native_four_job_observation_20260810](experiments/results/image_native_four_job_observation_20260810/README.md) |
| `opening_image_project_fourjob_observe_only_formal_20260810` | [image_project_four_job_observation_20260810](experiments/results/image_project_four_job_observation_20260810/README.md) |
| `opening_multijob_interference_20260809` | [text_multi_job_interference_20260809](experiments/results/text_multi_job_interference_20260809/README.md) |
| `opening_project_short_all_at_t0_diagnostic_20260809` | [text_short_job_start_time_diagnostic_20260809](experiments/results/text_short_job_start_time_diagnostic_20260809/README.md) |
| `opening_text_native_gate_20260808` | [text_native_framework_capability_check_20260808](experiments/results/text_native_framework_capability_check_20260808/README.md) |
| `opening_text_native_single_job_formal_20260808` | [text_native_single_job_comparison_20260808](experiments/results/text_native_single_job_comparison_20260808/README.md) |
| `multicard_scale_ramp_enhanced_20260807` | [duckdb_direct_scale_diagnostic_20260807](experiments/results/duckdb_direct_scale_diagnostic_20260807/README.md) |
| `multicard_lbrr_scale_ramp_enhanced_20260807` | [duckdb_gateway_scale_diagnostic_20260807](experiments/results/duckdb_gateway_scale_diagnostic_20260807/README.md) |
| `multicard_rich_metric_2048_20260806` | [text_service_pressure_diagnostic_2048_20260806](experiments/results/text_service_pressure_diagnostic_2048_20260806/README.md) |
| `text_map_main_real_20260930` | [text_map_four_path_comparison_20260930](experiments/results/postgresql/text_map_four_path_comparison_20260930/README.md) |
| `m1_c64_errors_20260920` | [map_c64_request_diagnostic_20260920](experiments/results/postgresql/map_c64_request_diagnostic_20260920/README.md) |
| `m1_full_recheck_20260920` | [map_capacity_recheck_20260920](experiments/results/postgresql/map_capacity_recheck_20260920/README.md) |
| `m1_m2_f_real_20260920` | [capacity_organization_image_validation_20260920](experiments/results/postgresql/capacity_organization_image_validation_20260920/README.md) |
| `m1_platform_revision_20260920` | [map_capacity_selection_check_20260920](experiments/results/postgresql/map_capacity_selection_check_20260920/README.md) |
| `m1_supply_followup_20260920` | [map_supply_diagnostic_20260920](experiments/results/postgresql/map_supply_diagnostic_20260920/README.md) |
| `image_stages_f_20260920` | [image_stage_execution_check_20260920](experiments/results/postgresql/image_stage_execution_check_20260920/README.md) |
| `query_sharing_e_20260914` | [query_sharing_lifecycle_check_20260914](experiments/results/postgresql/query_sharing_lifecycle_check_20260914/README.md) |
| `rc1_data_organization` | [data_organization_comparison](experiments/results/data_organization_comparison/README.md) |
| `rc1_prefix_routing` | [prefix_routing](experiments/results/prefix_routing/) |
| `image_clip_native_baseline_20260801` | [image_clip_project_udf_diagnostic_20260801](experiments/results/motivation/gpu/image_clip_project_udf_diagnostic_20260801/README.md) |

历史运行命令和来源摘要可通过原提交追溯，迁移后的数据定位由各结果的`raw/storage-manifest.jsonl`说明。
共享归档按当前清单取用并核对摘要，持续维护的报告从仓库阅读。
