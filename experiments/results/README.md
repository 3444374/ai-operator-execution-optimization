# 实验结果

本页定位报告与数据；实际配置、全部重复、失败和结论在对应实验的主报告中维护。
共享证据、负结果及尚未覆盖的能力见[证据台账](EXPERIMENT_EVIDENCE_REGISTRY.md)。

| 内容 | 报告入口 |
|---|---|
| 文本Map完整系统比较 | [四路径匹配比较](postgresql/text_map_four_path_comparison_20260930/README.md) |
| 同应用原生语义系统 | [五系统重复评价、Movie Q3与失败记录](postgresql/semantic_system_comparison_20261008/README.md) |
| 数据就绪后的查询与HTTP计时 | [SQL／原生API比较](postgresql/query_ready_timing_20261008/README.md) |
| 查询准备与消费调优 | [worker复用](postgresql/text_map_worker_reuse_real_20260930/README.md)、[gateway与消费分段](postgresql/text_map_gateway_lifecycle_20261001/README.md) |
| 准备分段的数据库与模型验证 | [PG生命周期、模型停止及物理内存](postgresql/text_map_preparation_validation_20261004/README.md) |
| 实验记录与分批的受控对照 | [线程记账](postgresql/text_map_threaded_accounting_20261002/README.md)、[Arrow分批fixture](postgresql/text_map_arrow_batches_20261002/README.md)、[Daft／Arrow真实对照](postgresql/text_map_arrow_real_20261003/README.md) |
| 生成算子、数据库接入与共享查询 | [生成算子验证](postgresql/semmap_real_model_resource_20260904/README.md)、[查询归属](scheduling/query_job_20260909/README.md)、[多查询执行](scheduling/multijob_20260908/README.md) |
| 数据组织与容量 | [历史组织比较](data_organization_comparison/README.md)、[容量复查](postgresql/map_capacity_recheck_20260920/README.md)、[有限真实检查](postgresql/capacity_organization_image_validation_20260920/README.md) |
| DuckDB比较与规模诊断 | [静态路径比较](text_db_e2e_duckdb_static_comparison_20260808/README.md)、[直接客户端规模诊断](duckdb_direct_scale_diagnostic_20260807/README.md)、[gateway规模诊断](duckdb_gateway_scale_diagnostic_20260807/README.md) |
| 代价估计 | [逐场景审计与数据](operator_cost_estimation_20260726/README.md)、[双卡采集](operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md) |
| 动机画像 | [motivation/](motivation/README.md) |
| 组件与能力检查 | [diagnostics/](diagnostics/README.md) |
| 历史综合分析 | [实验数据分析](EXPERIMENT_DATA_ANALYSIS_20260727.md) |

保存与精简按[结果规则](AGENTS.md)执行。大量原始文件按各实验的`raw/storage-manifest.jsonl`定位，
直接引用的数据仍就地读取；归档成员按[恢复说明](../../code/scripts/README.md#实验结果恢复)取用。
[存储检查](EXPERIMENT_EVIDENCE_REGISTRY.md#storage-review-20261002)、[恢复核验](EXPERIMENT_EVIDENCE_REGISTRY.md#legacy-retention-20261002)记录当前整理情况。

## 历史实验解读

早期实验的目的与解释进入对应报告；教学数字、模拟说明和真实测量保留各自来源。
组织、提交与代价估计的历史汇总见[实验数据分析](EXPERIMENT_DATA_ANALYSIS_20260727.md)，具体原始证据从台账定位。

## 原首页章节定位

原首页的运行摘要已归入所属报告；下列位置保留旧章节引用。

<a id="filtermap绑定2026-09-07"></a>
[Filter→Map绑定（2026-09-07）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="统一入口"></a>
[统一入口](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="共同调用与tuple绑定2026-09-07"></a>
[共同调用与tuple绑定（2026-09-07）](postgresql/semantic_binding_20260907/README.md)

<a id="map调用分析提取2026-09-07"></a>
[Map调用分析提取（2026-09-07）](postgresql/semantic_call_extraction_20260907/README.md)

<a id="two-filter-and-and-bounded-gateway-sessions-2026-09-07"></a>
[Two Filter AND and bounded gateway sessions (2026-09-07)](postgresql/semfilter_and_20260907/README.md)

<a id="postgresql-扩展目录整理2026-09-07"></a>
[PostgreSQL 扩展目录整理（2026-09-07）](postgresql/pg_module_layout_20260907/README.md)

<a id="semmap-resource-measurement-repair-2026-09-06"></a>
[SemMap resource measurement repair (2026-09-06)](postgresql/semmap_resource_lifecycle_20260906/README.md)

<a id="postgresql-生成型-map-真实模型与资源检查2026-09-04"></a>
[PostgreSQL 生成型 Map 真实模型与资源检查（2026-09-04）](postgresql/semmap_real_model_resource_20260904/README.md)

<a id="postgresql-生成型-map-c-v5pg-golden2026-09-03"></a>
[PostgreSQL 生成型 Map C v5/PG golden（2026-09-03）](postgresql/semmap_pg_wire_20260903/README.md)

<a id="postgresql-生成型-map-pg-plan权限2026-09-03"></a>
[PostgreSQL 生成型 Map PG plan/权限（2026-09-03）](postgresql/semmap_pg_plan_20260903/README.md)

<a id="postgresql-生成型-map-纯值与-python-v52026-09-03"></a>
[PostgreSQL 生成型 Map 纯值与 Python v5（2026-09-03）](postgresql/semmap_values_20260903/README.md)

<a id="postgresql-生成型-map-消息编译2026-09-03"></a>
[PostgreSQL 生成型 Map 消息编译（2026-09-03）](postgresql/semmap_messages_20260903/README.md)

<a id="postgresql-函数对象身份2026-09-02"></a>
[PostgreSQL 函数对象身份（2026-09-02）](postgresql/function_identity_20260902/README.md)

<a id="postgresql-choice-profile2026-09-02"></a>
[PostgreSQL choice profile（2026-09-02）](postgresql/choice_service_20260902/README.md)

<a id="postgresql-exact-semfilter-reference-calibration2026-09-01"></a>
[PostgreSQL exact SemFilter reference calibration（2026-09-01）](postgresql/semfilter_prompt_qualification_20260901/README.md)

<a id="状态感知-phase-change2026-08-11"></a>
[状态感知 phase-change（2026-08-11）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="开题统一文本-database-e2e2026-08-08-历史结果"></a>
[开题统一文本 database-E2E（2026-08-08 历史结果）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="开题文本原生框架入口2026-08-08"></a>
[开题文本原生框架入口（2026-08-08）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="开题文本多-job-干扰2026-08-09"></a>
[开题文本多 Job 干扰（2026-08-09）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="图像-ai_embed-operator2026-08-0304"></a>
[图像 AI_EMBED operator（2026-08-03/04）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="双-gpu-调度与容量2026-07-2829"></a>
[双 GPU 调度与容量（2026-07-28/29）](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="output-aware-packing-2026-07-26"></a>
[Output-aware Packing (2026-07-26)](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="local-baselines"></a>
[Local Baselines](EXPERIMENT_EVIDENCE_REGISTRY.md)

<a id="当前状态"></a>
[当前状态](EXPERIMENT_EVIDENCE_REGISTRY.md)
