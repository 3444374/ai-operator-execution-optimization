# Baseline and comparison harnesses

本包负责文本/图像 baseline 的共同合同、薄适配和证据落盘，不承载项目调度策略。

`text/sembench_movie.py`固定上游Movie Q1/Q2/Q3与原始评价器，另做逐行真假审计。
`text/frameworks/lotus_pg.py`从PG读取原始列、保留重复reviewId，执行原生LOTUS程序；
`text/frameworks/ray_data_pg_http.py`使用Ray2.56.1的SQL reader与HTTP Processor。
`text/frameworks/daft_pg_http.py`使用Daft0.7.21原生SQL reader与单行异步batch UDF，
由Daft拥有执行并行；它只复用同语义HTTP工作函数与观测，不接SemLoom执行核心。
就绪计时通过`prepare_runtime`在新driver中设置一次线程，`open_rows(runtime_prepared=True)`保留原生SQL读取与查询图；默认仍在执行时初始化。
该路径不是旧`daft_prompt.py`的内置prompt结果。主表、消融及语义系统缺项见
[当前覆盖审计](../../../experiments/plans/reference/对照规范.md#current-map-coverage)。
原生系统拥有批次/并发，实际HTTP计数和观测在统一查询时间内；
[受控验证](../../../experiments/results/postgresql/database_queries_20260910/README.md)不表示真实质量或性能已通过。

`text/frameworks/prepared_map.py`将图外已生成的完整调用送入固定版本Daft Native或Ray Data原生图，
保留其并发、缓冲及完整响应消费，不在原生kernel中套用SemLoom。
这是独立的执行器参照；旧SQL reader与内置prompt路径保持，使用方式见[成对查询入口](../../scripts/README.md#native-adapter-query)。

`text/frameworks/semantic_map.py`提供同应用Movie Map的LOTUS1.2.4 `sem_map`、
Daft0.7.21内置`prompt`、DuckDB1.5.4社区`ai`0.4.14与Sema作者二进制入口。
同模块的`prepare_rows`先装载原始输入与可复用组件，`execute()`执行原生算子；DuckDB在SQL内装配提示，
Sema持续SQL会话支持自有进程取消。[就绪计时报告](../../../experiments/results/postgresql/query_ready_timing_20261008/README.md)说明计时与验证。
`text/products/sema.py`核对作者产物摘要，以原生SQL语义投影执行；输入读取、CSV装配和完整消费计时。
Sema的应用指令明确把分类标签编码为带双引号的JSON字符串，配合作者原生scalar解析；
格式提示只进入SQL指令，不在HTTP转发或结果处理时修改值。原始非法输出与修订验证分别保存。
原生系统保留提示、解析和并行执行，使用唯一行出现编号关联重复文本与原始ID。
这些路径与固定消息的Ray／Daft HTTP比较分别报告，具体清单见[语义系统方案](../../../experiments/plans/语义系统对照.md)。

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
`experiments/plans/completed/文本原生对照_20260802.md`。
