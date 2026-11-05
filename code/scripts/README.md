# SemLoom 脚本入口

当前实验以 SemBench 为主；参数、工作量和生命周期由每次实际配置与回执确定。
脚本只装配库接口，不保存另一套执行策略。

<a id="current-entry-roles"></a>
## 当前入口

| 任务 | 入口 | 范围 |
|---|---|---|
| 单次 Movie Q1/Q2/Q3 | [run_lotus_records_movie.py](experiments/run_lotus_records_movie.py) | `check/run` 使用现有方法与执行工厂 |
| 矩阵／连续查询／原生 LOTUS 对照 | [run_movie_records_matrix.py](experiments/run_movie_records_matrix.py) | 由计划选择 `backend` 与生命周期，`lotus-native` 走本地模型 IPC |
| 独立作者 Kalypso | [服务](experiments/run_kalypso_native_server.py)、[查询](experiments/run_kalypso_sembench_query.py)、[CPU](experiments/check_kalypso_native_cpu.py) | 作者服务、默认与任务适配保持 |
| 完整查询观测 | [record_observation/workflow.py](profiling/record_observation/workflow.py) | [契约和账本](profiling/record_observation/README.md)，不新增容量 |
| 离线结果读取 | [summarize_offline_results.py](analysis/summarize_offline_results.py) | [格式／NA 说明](analysis/OFFLINE_RESULTS.md) |
| 准确历史树恢复 | [materialize_native_baseline.py](experiments/materialize_native_baseline.py) | 只还原与核验，不运行模型 |
| 同步 CPU 证据复算 | [check_sync_cpu_evidence.py](analysis/check_sync_cpu_evidence.py) | 固定来源、替身和 ACK 受控行为；不是性能测试 |

CSV／本地 records 不要求 PG。真实运行需要明确输入、模型／driver Python、源码身份、
资源所有权、持久次数额度和最终关闭证据；`--help`／CPU 检查不证明这些实际环境已可运行。
新原生 LOTUS 本地 IPC 已有 CPU 检查，官方完整 Q123 因本地缺 `overrides/cdlib/duckdb` 尚未通过，
没有将 Core 替身当作原生执行资格。

## Movie session 配置

普通矩阵已有 `check`、准备、owner／附着与恢复子命令，实际帮助从上述标准入口读取。
计划的 `backend` 可选择 `semloom-core`、`semloom-static-reference`、`lotus-native`、`daft`、`ray-data`、`imlane`。
原生 LOTUS profile 只声明显式正整数 `native_lm_max_batch_size`；Core 的 token／rolling／admission 配置
不能混入原生 baseline。矩阵实际参数与容量规则见[Movie 入口说明](../src/experiments/movie_matrix.README.md)。

`resident`、`isolated`、Ray `runtime_reuse_only`、Daft `native_single_thread_session` 分别记录；
没有隐式暖机或通过省略关闭制造复用。原生 LOTUS 的冷查询在查询内启动 IPC owner，
常驻查询借用现成 owner，最后由 session 关闭；完整结果的 `result_ready` 先于质量评价和最终关闭。
`query_seconds` 若覆盖整个适配函数，不可替代纯结果可用时钟。

## 原生 LOTUS CLIP 查询

[run_lotus_image_retrieval.py](experiments/run_lotus_image_retrieval.py)与
[qualify_lotus_clip.py](experiments/qualify_lotus_clip.py)使用明确的模型、输入和执行 owner。
支持和实机范围见[方法说明](../src/semantic_methods/lotus/README.md#原生-clip-图像检索)，
不因共享 CLIP worker 就互授多卡、任意 DAG 或新模型性能资格。

## 退役与研究恢复

旧固定 Movie 五后端矩阵、Movie HTTP owner、PG／HTTP 画像和查询 CLI、旧 prepared-call 对照、
相连专用测试与失效部署示例已退出维护目录。PG 是暂停，C／SQL、部署和接线源码均可按
[准确索引](../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)在隔离 Git 树恢复。
旧命令只在其冻结树解释，不为它们添加兼容包装。

SAOR 核心调度、原始研究材料和仍能读取历史结果的分析脚本保留；依赖旧 HTTP／PG 启动器的部分随归档隔离。
已有成绩、成本、失败和 unknown 不修改；新 CPU 测试不构成模型或查询加速证据。

## 本地时间与额度诊断

[观察探针](profiling/map_observation_probe.py)、[Core 探针](profiling/mapped_core_probe.py)和
[预付计数探针](profiling/mapped_request_probe.py)仅使用本地 CPU 替身。每轮共用实际运行截止，
后续单项使用剩余时间，不再提前扣 60／30 秒；停止新增后继续取消和清理，已预付计数不退还。
单独调用 case 仍保留有限默认时限。这些诊断不代表模型或查询性能。

## 独立研究与离线分析

SAOR 声明与方法说明见[独立材料](../../experiments/saor/README.md)，旧双卡、开题和 phase-change 配方见
[精确归档](../../experiments/reproduction/legacy_recipes_20261105/README.md)。
`analysis/summarize_saor_*.py`、`replay_saor_capacity.py`及既有成本／静态曲线分析继续读取历史结果；
`run_ai_operator_scenarios.py`保留显式 profiler 装配与 CPU 行为核验用途，旧 PG profiler 已归档，
不能直接用旧配置恢复当前实验。历史方案、原始结果和必要失败证据保持。
