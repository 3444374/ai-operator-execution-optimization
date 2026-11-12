# 实验结果

本页同时链接 main 已存报告及 [固定集成快照 `9c59dc9b`](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/README.md)的后续结果。
外链结果按其实际源码与运行身份解释，不表示 main 已包含对应实现。

本页定位报告与数据；实际配置、全部重复、失败和结论在对应实验的主报告中维护。
共享证据、负结果及尚未覆盖的能力见[证据台账](EXPERIMENT_EVIDENCE_REGISTRY.md)。

| 内容 | 报告入口 |
|---|---|
| 当前执行与资源清理 | [records 主报告](diagnostics/records_runtime_refactor_20261104/README.md) |
| 原生 Movie Q3 复测 | [功能、质量与退出记录](diagnostics/native_sembench_q3_20261106/README.md) |
| LOTUS原生CLIP建索引与检索 | [六路径历史与最终融合资格](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#final-integrated-qualification)、[PNG共享适配](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#png-candidate) |
| 单GPU文件CLIP冷算子 | [静态／HSE／原生Ray Data复验与257图数值补测](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/diagnostics/image_clip_file_operator_20261014/README.md) |
| 原生Daft／Ray／IMLane接入 | [三方法软件检查与LOTUS六路径单／双GPU模型](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/postgresql/native_executor_baselines_20261012/README.md) |
| 独立IMLane构建 | [无模型Arrow往返](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/diagnostics/imlane_build_20261012/README.md) |
| 原生方法与SemLoom执行接入 | [固定版本工程核对](https://github.com/3444374/ai-operator-execution-optimization/blob/9c59dc9be1422fca2ef75a4b4f4d41a65eb1100e/experiments/results/postgresql/native_adapter_integration_20261009/README.md) |
| 文本Map完整系统比较 | [四路径匹配比较](postgresql/text_map_four_path_comparison_20260930/README.md) |
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
