# Baseline and comparison harnesses

[原生CLIP检索](../semantic_methods/lotus/README.md#原生-clip-图像检索)使用[Daft CLIP UDF](image/frameworks/daft_clip.py)与Ray CLIP predictor；图与模型批次仍由原生系统拥有，main570四路径40行严格单卡实机按同题报告解释，未认证HSE／多卡。

本包负责文本/图像 baseline 的共同合同、薄适配和证据落盘，不承载项目调度策略。

<a id="text-execution-boundaries"></a>
## 文本入口按交接边界分类

框架名、HTTP传输或“native”字样不决定层级。以下按main `380cfbda`与[PR #52固定候选45d184df](https://github.com/3444374/ai-operator-execution-optimization/tree/45d184dfaf0ef3a85ccccc805be7fc38e646b527)核对；“候选”行的路径和能力尚未进入main，版本存在不等于实机资格。

| 路径（相对 `code/src/`） | 来源状态 | 进入接点的单位与owner | 不能据此声称 |
|---|---|---|---|
| `baselines/text/frameworks/native_calls.py`、`prepared_map.py` | main已有 | 外部已准备完整HTTP call；Daft/Ray Data原生图拥有执行/背压，SDK接回完整响应 | 框架已接管原LOTUS扫描、提示准备或跨算子推进 |
| `baselines/text/frameworks/{daft,ray_data}_pg_http.py` | main已有 | 原生SQL reader读取源行，在图内构造同应用请求并消费；是源数据流入口 | 仅因最终走HTTP就归为prepared-call，或称它执行了原LOTUS records方法 |
| `baselines/text/frameworks/lotus_native_local_model.py` | PR #52候选 | 原LOTUS/LiteLLM执行完整查询，私有或借用的HTTP模型服务；保留原方法/调度 | 原生LOTUS调用数就是GPU批次，或借服务ready证明模型启动在计时外 |
| `baselines/text/frameworks/ray_data_lotus_records.py`、`daft_lotus_records.py` | PR #52候选 | 原始records与显式方法配置；方法准备与模型调用在原生worker路径，框架拥有图/worker；查询调用方收齐并重排完整回执后调用原LOTUS parser | 与上面的同名Ray/Daft请求接点相同，或使用SemLoom Core策略 |
| `baselines/text/frameworks/daft_lotus_session.py` | PR #52候选 | 明确的Daft单线程session配置；与isolated配置分别命名/比较 | Daft所有原生配置都跨查询常驻，或仅删close即可保留模型 |
| `baselines/text/frameworks/{ray_data,daft}_lotus_chain.py`、`*_lotus_cascade.py` | PR #52候选 | 原方法链/级联的原生执行；保留合法关系/全局屏障和算法状态 | 现有接口等于任意DAG、group提前闭合或新FIFO优化已通过 |
| `execution_provider/adapters/lotus_records_core.py`、`lotus_record_chain_core.py` | PR #52候选 | LOTUS→SemLoom Core的执行适配，属于项目方法；此处仅导航，不是本包native执行器 | 将Core credit/router注入原生baseline后仍称native |
| `baselines/text/frameworks/kalypso_native/` | PR #52候选 | SemBench任务/评价适配与外部作者服务；作者prompt/parser/执行调度独立 | LOTUS后端或严格同请求调度消融；客户端close等于作者服务退出 |

候选的Ray `runtime_reuse_only`和Daft `native_single_thread_session`是生命周期配置，不是新的执行层级；同题比较须同时记录输入边界、方法owner、模型owner和计时边界。
PR #52候选中的共享`lotus_chain_batch/results`及私有模型process也被Core引用，属于待归位的无策略共享接点，不能因现存路径在本目录就算作native专属。
只接受五个真实跨层共享文件的后续兼容迁移；清楚的vendor wrapper、旧request桥与稳定CLI保留。逐项[命名与迁移清单](../../../experiments/plans/系统架构.md#code-directory-boundaries)不授权本次改源码或改历史复现字节。

`text/sembench_movie.py`固定上游Movie Q1/Q2/Q3与原始评价器，另做逐行真假审计。
`text/frameworks/lotus_pg.py`从PG读取原始列、保留重复reviewId，执行原生LOTUS程序；
`text/frameworks/ray_data_pg_http.py`使用Ray2.56.1的SQL reader与HTTP Processor。
`text/frameworks/daft_pg_http.py`使用Daft0.7.21原生SQL reader与单行异步batch UDF，
由Daft拥有执行并行；它只复用同语义HTTP工作函数与观测，不接SemLoom执行核心。
该路径不是旧`daft_prompt.py`的内置prompt结果。主表、消融及语义系统缺项见
[当前覆盖审计](../../../experiments/plans/reference/对照规范.md#current-map-coverage)。
原生系统拥有批次/并发，实际HTTP计数和观测在统一查询时间内；
[受控验证](../../../experiments/results/postgresql/database_queries_20260910/README.md)不表示真实质量或性能已通过。

`text/squad_map.py` 负责当前 PG Map 的 SQuAD 双消息输入身份、context 分组划分和预测关联，
复用既有答案评分；离线入口见 `code/scripts/baselines/squad_pg_map_pilot.py`。它不拥有执行或调度。

`text/squad_capacity.py` 先统计全部完整消息的输入 tokens，再按 context 分区选取自然/分层样本，
记录种子与未修改的原文。`code/scripts/baselines/map_capacity.py` 装配独立 direct 客户端或真实 PG Map；
查询与评价分别记录时间和进程内存，配置与说明见[容量单元入口](../../scripts/README.md#单-map-容量单元)。

`common/private_artifacts.py` 原样保存仓库外私有文件，公开摘要只含计数与哈希；
该模块也拥有分块文件身份 `file_identity` 与保留非有限评价含义的 `json_metric`。
两者只依赖标准库；旧 `postgresql.query_workloads`／`query_evaluation` 直接重导出同一函数，供尚未迁移的外部复现使用。
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
