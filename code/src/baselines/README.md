# Baseline and comparison harnesses

本包负责文本/图像 baseline 的共同合同、薄适配和证据落盘，不承载项目调度策略。

`text/sembench_movie.py`固定上游Movie Q1/Q2/Q3与原始评价器，另做逐行真假审计。
`text/frameworks/lotus_pg.py`从PG读取原始列、保留重复reviewId，执行原生LOTUS程序；
`text/frameworks/ray_data_pg_http.py`使用Ray2.56.1的SQL reader与HTTP Processor。
原生系统拥有批次/并发，实际HTTP计数和观测在统一查询时间内；
[受控验证](../../../experiments/results/postgresql/database_queries_20260910/README.md)不表示真实质量或性能已通过。

`text/squad_map.py` 负责当前 PG Map 的 SQuAD 双消息输入身份、context 分组划分和预测关联，
复用既有答案评分；离线入口见 `code/scripts/baselines/squad_pg_map_pilot.py`。它不拥有执行或调度。

`text/squad_capacity.py` 先统计全部完整消息的输入 tokens，再按 context 分区选取自然/分层样本，
记录种子与未修改的原文。`code/scripts/baselines/map_capacity.py` 装配独立 direct 客户端或真实 PG Map；
查询与评价分别记录时间和进程内存，配置与说明见[容量单元入口](../../scripts/README.md#单-map-容量单元)。

`common/private_artifacts.py` 原样保存仓库外私有文件，公开摘要只含计数与哈希；
`text/sharegpt_inputs.py` 选择首个 human 原文，`text/map_inputs.py` 核对文本往返和完整消息上下文。
ShareGPT 数据选择与任务提示分别指定；接口见[脚本说明](../../scripts/README.md#squad-pg-map-数据准备与离线评价)。

## 分层

| 层 | 模块 | 角色 |
|---|---|---|
| 共享合同 | `common/{contracts,manifests,results,gate,provenance,database_identity,redact}.py` | immutable input、exactly-once、公共指标、数据库版本身份与脱敏检查 |
| 文本服务上限 | `text/ceilings/vllm_bench.py` | 官方 vLLM Bench；不是数据库/框架 baseline |
| 文本直接控制 | `text/controls/` | 项目自写强客户端；不是 native baseline |
| 文本框架原生 | `text/frameworks/` | Daft prompt / Ray Data vendor API graph |
| 文本数据库产品 | `text/products/oceanbase.py` | OceanBase 原生 SQL `AI_COMPLETE` adapter |
| 文本编排 | `text/orchestration/` | PostgreSQL manifest、双 endpoint gate、counter 和 CLI |
| 图像框架原生 | `image/frameworks/` | Daft built-in / Ray Data native graph |
| 图像身份检查 | `image/provenance.py` | image arm scheduler owner 与 formal eligibility |

`text/frameworks/` 和 `image/frameworks/` 内只允许 payload/response adapter 与 vendor
API graph。需要 active-work、K、
router、flush、shared credit 或自定义 actor pool 的代码属于项目方法，应放在
`src/scheduling/` / `src/observability/profiling/`，不能倒流进 native baseline。

复测合同见
`experiments/plans/completed/text_native_baseline_rerun_20260802.md`。
