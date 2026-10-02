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
| 文献、研究分析与方法依据 | `docs/research/` | [研究入口](docs/research/README.md)、[研究定位](docs/research/研究定位.md) |
| 历史工程与已结束的开题材料 | `docs/archive/` | [工程](docs/archive/engineering/README.md)、[开题](docs/archive/opening/README.md) |
| 图源、输入、导出与审查 | `figures/` | [图资产](figures/README.md) |
| 结构与关键事实变更 | 根文件 | [项目日志](PROJECT_LOG.md) |

研究文档回答已有工作、研究问题、方法选择与可验证假设。代码说明回答现有实现怎样工作，
实验报告回答实际执行了什么与数据支持什么，计划记录实施要求与待核对问题。
讲解按对象进入这些文档；沟通内容经事实核对后进入实际方案，不另维护记录或话术。

## 实现与实验的阅读入口

1. [总纲](PROJECT_OUTLINE.md)确定问题与当前顺序。
2. [代码状态](code/INFRA_STATUS.md)核对实际能力，再读对应源码。
3. [主架构计划](experiments/plans/系统架构.md)核对设计与实施要求。
4. [实验台账](experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)定位原始报告与失败记录。

| 主题 | 入口 |
|---|---|
| 同步/增量生成Map | [语义规格](experiments/plans/生成算子.md)、[增量设计](experiments/plans/增量执行.md#session) |
| 多Job与查询归属 | [多会话设计](experiments/plans/增量执行.md#multi-query)、[查询Job设计](experiments/plans/数据库接入.md#query-job) |
| 调用与绑定 | [详细设计](experiments/plans/数据库接入.md#calls) |
| baseline与计时角色 | [参照要求](experiments/plans/reference/对照规范.md#执行者与计时的读法) |
| 观测指标 | [模块说明](code/src/observability/README.md#运行指标的读法)、[选择依据](docs/research/evaluation_metrics_survey_20260731.md#观察变量选择) |
| 文本Map四路径比较 | [真实模型结果](experiments/results/postgresql/text_map_four_path_comparison_20260930/README.md) |
| worker与gateway调优 | [计划](experiments/plans/查询调优.md)、[worker](experiments/results/postgresql/text_map_worker_reuse_real_20260930/README.md)、[gateway](experiments/results/postgresql/text_map_gateway_lifecycle_20261001/README.md) |
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
| `motivation/plans/` | [动机计划](experiments/plans/archive/README.md#动机与画像方案) |
| 两处`benchmarks/` | [统一脚本](code/scripts/benchmarks/README.md) |
| `overview/` | [根入口](README.md) |
| `docs/designs/`、`docs/plans/` | [历史工程](docs/archive/engineering/README.md) |
| `opening/`、旧`projects/` | [历史开题](docs/archive/opening/README.md) |
| 代码与指标讲解 | [代码组织](code/README.md#代码组织的读法)、[观测](code/src/observability/README.md#运行指标的读法)、[指标依据](docs/research/evaluation_metrics_survey_20260731.md#观察变量选择) |
| 实验讲解 | [对应结果](experiments/results/README.md#历史实验解读) |
| 沟通中的待核对问题 | [实际接入计划](experiments/plans/系统架构.md#company-integration-questions)、[文献清单](docs/research/ai_operator_literature_inventory.md) |

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
| `rc1_prefix_routing` | [prefix_routing](experiments/results/prefix_routing) |
| `image_clip_native_baseline_20260801` | [image_clip_project_udf_diagnostic_20260801](experiments/results/motivation/gpu/image_clip_project_udf_diagnostic_20260801/README.md) |

历史运行命令和来源摘要可通过原提交追溯，迁移后的数据定位由各结果的`raw/storage-manifest.jsonl`说明。
共享归档按当前清单取用并核对摘要，持续维护的报告从仓库阅读。

## 方案旧名定位

2026-10-02按内容职责收敛方案。现行六个入口见[实验方案](experiments/plans/README.md)，同一实验的补充报告已经并入主报告章节。
下表以原`experiments/plans/`下的相对路径定位；历史实验标识、配置版本、接口要求与来源提交继续按原材料解释。

| 旧方案 | 当前入口 |
|---|---|
| `postgresql_ai_semantic_operator_architecture_20260827.md` | [系统架构](experiments/plans/系统架构.md) |
| `postgresql_semmap_generation_contract.md` | [生成算子](experiments/plans/生成算子.md) |
| `text_map_preparation_tuning.md` | [查询调优](experiments/plans/查询调优.md) |
| `baseline_reference.md` | [对照规范](experiments/plans/reference/对照规范.md) |
| `experiment_status_and_gaps.md` | [进度汇总_20261001](experiments/plans/archive/进度汇总_20261001.md) |
| `cross_layer_killer_experiment.md` | [组织与调度联合对照](experiments/plans/archive/组织与调度联合对照.md) |
| `full_grid_sweep_plan.md` | [扩展参数矩阵](experiments/plans/archive/扩展参数矩阵.md) |
| `saor_cross_layer_scheduler_capability_20260820.md` | [调度器能力对照_20260820](experiments/plans/archive/调度器能力对照_20260820.md) |
| `state_aware_work_unit_evaluation_20260808.md` | [状态感知实验_20260808](experiments/plans/archive/状态感知实验_20260808.md) |
| `service_scheduling_backpressure.md` | [提交与调度方案](experiments/plans/archive/提交与调度方案.md) |
| `completed/image_clip_workload_lock_20260731.md` | [图像工作负载_20260731](experiments/plans/completed/图像工作负载_20260731.md) |
| `completed/operator_cost_profile_dual4090_formal_20260804.md` | [代价估计双卡采集_20260804](experiments/plans/completed/代价估计双卡采集_20260804.md) |
| `completed/operator_cost_profile_pilot_20260804.md` | [代价估计预实验_20260804](experiments/plans/completed/代价估计预实验_20260804.md) |
| `completed/postgresql_choice_profile_engineering.md` | [选择算子接入_20260902](experiments/plans/completed/选择算子接入_20260902.md) |
| `completed/rc1_data_organization_rerun_20260731.md` | [数据组织重测_20260731](experiments/plans/completed/数据组织重测_20260731.md) |
| `completed/text_map_matched_comparison.md` | [文本Map对比_20260930](experiments/plans/completed/文本Map对比_20260930.md) |
| `completed/text_native_baseline_rerun_20260802.md` | [文本原生对照_20260802](experiments/plans/completed/文本原生对照_20260802.md) |
| `archive/database_ai_operator_baseline_matrix_20260729.md` | [文本对照矩阵_20260729](experiments/plans/archive/文本对照矩阵_20260729.md) |
| `archive/lotus_semantic_frontend_execution_integration_20260821.md` | [LOTUS接入设计_20260821](experiments/plans/archive/LOTUS接入设计_20260821.md) |
| `archive/msmarco_embedding_workload_20260731.md` | [文本嵌入对照_20260731](experiments/plans/archive/文本嵌入对照_20260731.md) |
| `archive/opening_database_e2e_p0_20260807.md` | [开题三路径对照_20260807](experiments/plans/archive/开题三路径对照_20260807.md) |
| `archive/postgresql_ai_semantic_operator_architecture_serial_20260901.md` | [串行架构快照_20260901](experiments/plans/archive/串行架构快照_20260901.md) |
| `archive/postgresql_lotus_ai_semantic_operator_implementation_20260821.md` | [LOTUS语义算子方案_20260821](experiments/plans/archive/LOTUS语义算子方案_20260821.md) |
| `archive/research_design_catalog.md` | [候选方案评估_20260715](experiments/plans/archive/候选方案评估_20260715.md) |
| `reference/bounded_output_duckdb_comparison_protocol_20260805.md` | [DuckDB对照](experiments/plans/reference/DuckDB对照.md) |
| `reference/experiment_report_honesty_checklist.md` | [报告核对](experiments/plans/reference/报告核对.md) |
| `reference/sink_writeback_coordination.md` | [写回协调方案](docs/archive/engineering/designs/写回协调方案.md) |
| `reference/strategy_design_implementation_reference.md` | [策略工程映射](docs/archive/engineering/designs/策略工程映射.md) |
| `motivation/integration.md` | [早期接入方案_20260710](experiments/plans/archive/早期接入方案_20260710.md) |
| `motivation/image_host_data_path_bottleneck.md` | [图像链路诊断](experiments/plans/archive/图像链路诊断.md) |
| `postgresql_call_binding_design.md` | [数据库接入](experiments/plans/数据库接入.md#calls) |
| `postgresql_query_job_design.md` | [数据库接入](experiments/plans/数据库接入.md#query-job) |
| `semloom_incremental_session_design.md` | [增量执行](experiments/plans/增量执行.md#session) |
| `semloom_multisession_design.md` | [增量执行](experiments/plans/增量执行.md#multi-query) |
| `bounded_method_driver.md` | [增量执行](experiments/plans/增量执行.md#method-driver) |
| `reference/strategy_design_literature_basis.md` | [优化方法依据](docs/research/优化方法依据.md#basis) |
| `reference/literature_driven_pipeline_optimization_guide.md` | [优化方法依据](docs/research/优化方法依据.md#workflow) |
| `motivation/ai_sql_surface.md` | [场景探索_20260710](experiments/plans/archive/场景探索_20260710.md#sql-scenes) |
| `motivation/workloads.md` | [场景探索_20260710](experiments/plans/archive/场景探索_20260710.md#workloads) |
| `data_organization_batching.md` | [数据组织](experiments/plans/数据组织.md) |

数据组织旧方案中的已执行清单见[历史方案](experiments/plans/archive/数据组织历史方案_20260927.md)，现行假设与容量设计见[数据组织](experiments/plans/数据组织.md)。

## 知识总汇旧章节定位

原`docs/research/knowledge_hub.md`改为[研究定位](docs/research/研究定位.md)，集中维护研究问题、最近邻、机制采用条件和待证增量。
原机制说明、题录、实验摘要与工程记录按内容归回既有文档；历史状态按原日期解释，原全文可从来源提交`ab125ae4`查阅。

| 原章节 | 当前维护位置 |
|---|---|
| 阅读指南 | [研究定位](docs/research/研究定位.md)；当前职责与问题导航 |
| §1 vLLM机制 | [vllm_continuous_batching_reference](docs/research/vllm_continuous_batching_reference.md)；专篇维护版本、参数和机制；不复制默认值 |
| §2 Ray模式 | [ray_actor_dynamic_batching_reference](docs/research/ray_actor_dynamic_batching_reference.md)；专篇维护接口与批处理，示意伪代码不作为实现 |
| §3 文献地图 | [ai_operator_literature_inventory](docs/research/ai_operator_literature_inventory.md#early-candidates)；已登记题录优先；未登记名称保留为待核对线索；旧总数删除 |
| §4 最近邻与采用条件 | [研究定位](docs/research/研究定位.md)；取消绝对空白概括，保持核对版本与来源类型 |
| §5 方法模式 | [优化方法依据](docs/research/优化方法依据.md#cost-candidates)；按信息、动作、对照和否定条件组织；旧参数、误差与进度回到事实来源 |
| §5.7.4/§5.10 图像阶段 | [heterogeneous_ai_dataflow_execution_model_20260811](docs/research/heterogeneous_ai_dataflow_execution_model_20260811.md)；已有专题拥有模型、阶段和验证要求 |
| §5.7.5 SAOR观察 | [saor_model_scenario_audit_20260811](docs/research/saor_model_scenario_audit_20260811.md)；模型、实验失败和数字由专题及原始报告维护 |
| §5.7.6 公平评价 | [evaluation_metrics_survey_20260731](docs/research/evaluation_metrics_survey_20260731.md#93-本项目的公平对比合同)；指标定义及适用条件已由指标专题维护 |
| §5.7.7/§5.8/§7.1/§10.5.1 | [策略工程映射](docs/archive/engineering/designs/策略工程映射.md#knowledge-hub-history)；保留原优先级、能力映射和工程设想；不覆盖当前状态 |
| §6 实验摘要 | [README](experiments/results/motivation/README.md)；避免把旧摘要计时或版本当成当前结论 |
| §7 实施与对照分级 | [README](experiments/plans/README.md)；架构、状态和对照分别引用各自唯一入口 |
| §8 缺口表 | [研究定位](docs/research/研究定位.md#open-questions)；研究问题与已实现工程条件分开，去除旧优先级 |
| §9 文件清单 | [README](docs/research/README.md)；目录清单与变化沿革分别由README与PROJECT_LOG维护 |
| §10 Daft及多模态 | [daft_ray_multimodal_reference](docs/research/daft_ray_multimodal_reference.md)；技术和厂商资料已有专篇；本地结论回到报告 |
