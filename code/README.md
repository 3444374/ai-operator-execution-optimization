# SemLoom 数据执行工程

当前实验主线是 SemBench：LOTUS 提供原方法与语义，SemLoom 提供数据执行、供给、背压与生命周期管理。
普通 Movie records 通过本地 IPC 调用模型引擎；不启动旧 Movie HTTP 服务。
当前可选能力、默认及验证限制见[配置总览](../docs/执行配置与优化开关.md)和[代码状态](INFRA_STATUS.md)。

## 真实入口

| 任务 | 入口 | 职责 |
|---|---|---|
| Movie Q1/Q2/Q3 单查询 | [run_lotus_records_movie.py](scripts/experiments/run_lotus_records_movie.py) | 原输入、任务与评价，选择执行适配 |
| 完整查询矩阵／连续查询 | [run_movie_records_matrix.py](scripts/experiments/run_movie_records_matrix.py) | 显式 backend、生命周期、额度与时钟 |
| 原生 LOTUS 对照 | [lotus_native_local.py](src/baselines/text/frameworks/lotus_native_local.py) | 原算子／LM／LiteLLM batch_completion；仅 provider 使用现有模型 IPC |
| 独立 Kalypso 对照 | [作者适配说明](src/baselines/text/frameworks/kalypso_native/README.md) | 保留作者服务、参数与调度；不是 LOTUS 后端 |
| LOTUS CLIP 检索 | [run_lotus_image_retrieval.py](scripts/experiments/run_lotus_image_retrieval.py) | 原检索方法与明确的图像执行 owner |
| 观测与有限测试 | [record_observation](scripts/profiling/record_observation/README.md) | 使用既有监督与回执，不增加执行容量 |

命令与正常配置入口见[脚本说明](scripts/README.md)。输入、私有 packet、模型环境、GPU 租约和实际版本应由每次运行绑定，
不能把示例值、历史成绩或 CPU 替身当成当前实机资格。原生 LOTUS 非 HTTP 适配已有 CPU 检查，
官方完整 Q1/Q2/Q3 尚受本地缺失的 `overrides/cdlib/duckdb` 依赖限制，真实模型／性能未验证。

## 执行与语义分层

| 层 | 维护位置 | 拥有的责任 |
|---|---|---|
| LOTUS 方法 | [semantic_methods/lotus](src/semantic_methods/lotus/README.md) | formatter、parser、方法状态、关系屏障与逐行结果合同 |
| SemLoom 执行适配 | [execution_provider/adapters](src/execution_provider/adapters/README.md) | Core／有限链／rolling／admission 与结果交付接线 |
| 调度与资源责任 | [scheduling](src/scheduling) | 就绪选择、在途额度、背压、预算和 unknown 责任 |
| 模型与传输 | [serving/backends](src/serving/backends) | 引擎构造、本地 IPC、逐请求终态及进程清理 |
| 模态公共能力 | [modalities](src/modalities) | 文本／图像合同、成本、共享 CLIP workers；不拥有 baseline 图 |
| 通用观测和工具 | [observability](src/observability)、[infrastructure](src/infrastructure) | 时钟、来源、审计、脱敏、文件／预算／运行环境 |
| 独立对照 | [baselines](src/baselines/README.md#text-execution-boundaries) | 原生系统图、作者桥接及任务对照；不注入 Core 策略 |
| 实验编排 | [experiments](src/experiments) | SemBench 工作负载、生命周期与证据组合；不是生产依赖 |

生产的 scheduling／serving／execution_provider／semantic_methods／modalities／observability／infrastructure
不从 `baselines` 取得实现。共用代码直接放在所属共享层，SemLoom 和对照分别调用；
框架专用的 Ray Data／Daft 图留在基线模块。CLIP 共享批处理 workers 已从 Ray 基线图迁至
[clip_batch_workers.py](src/modalities/image/clip_batch_workers.py)，生产不再导入该 baseline 图。

## 独立对照和可比范围

原生 LOTUS、Daft、Ray Data、IMLane 是不同执行路径。Daft 用原生 class／UDF，Ray Data 用 Dataset／actor 池，
IMLane 保留作者桥接；它们可共享 LOTUS 方法、模型内核、IPC 与清理组件，不能因此说共用了 Core 调度。
原生 LOTUS 使用原 LM 的 `max_batch_size`，配置必须显式声明；没有用 Core 请求窗口代替它。

首次查询、模型常驻复用、仅 Ray runtime 复用及 Daft 单线程 session 要分别命名。
查询结果可用时间以实际 `query_start_ns → result_ready` 为准，评价、API 返回及最终 owner 关闭另记；
不通过减去估算启动成本构造查询时间，也不把旧 API 包络当作新查询时钟。
输入、输出质量、请求／token 工作量、缓存和实测版本仍是比较条件。

原生 Ray／Daft／IMLane 保留各自的参数所有者；[sync_reference_engine.py](src/baselines/text/frameworks/sync_reference_engine.py)
和[reference_chain_batch.py](src/baselines/text/frameworks/reference_chain_batch.py)记录其历史显式参数。
本次清理不改作者 Kalypso 调度、不改模型提示或输出上限，也不把基线改成 SemLoom。

## 退役、暂停和研究保留

旧 Movie HTTP owner、旧固定五后端矩阵、PG／HTTP 实验 CLI、相连旧对照脚本／配置和专用测试已退出维护树。
PG 扩展 C／SQL、部署源和实验接线按准确 Git 树归档，**暂停而非放弃 PG**；恢复须在隔离树中进行，
不把历史源重新加入当前生产搜索路径。逐文件摘要与恢复方法见
[归档索引](../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)。

SAOR 的调度算法、离线分析与研究证据继续保留；依赖已退役 HTTP／PG 命令的旧启动胶水随归档隔离。
当前源码不再承诺这些旧命令可运行。独立 PG provider／通用传输内核和有研究价值的模态／调度组件若仍有消费者则保留，
不因停止旧实验而删除其公共合同，也不宣称它们已接入当前 SemBench。

所有历史成绩、负结果、unknown、成本和五份具名代码投影保持。
[历史恢复入口](../experiments/reproduction/native_baselines/README.md)只恢复指定源码，不执行模型。
当前新增 CPU 检查不转授历史 GPU 资格，不宣称清理带来查询加速。

本组合保留已发布6754eebe的普通Core IPC容量推导、共享endpoint active-credit、
Ray调度观测和scheduler类型拆分；其实际接口与各自CPU范围见
[执行适配说明](src/execution_provider/adapters/README.md)和
[重构证据](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)。
本次清理没有把这些改动退回旧默认或重新引入另一份实现。

链查询 owner 负责配置、组装、关系阶段及资源生命周期；融合批传输与结果迭代集中到
[lotus_chain_fused_runtime.py](src/execution_provider/adapters/lotus_chain_fused_runtime.py)，
记录级 MethodDriver 推进仍在原有 `records_query_runtime.py`。此次整理不增加控制器或改变执行顺序。

异步模型 actor 的请求／回执与 IPC 实现在
[records_async_model.py](src/execution_provider/adapters/records_async_model.py)，driver 侧 Ray 分配和 RPC
终态许可仍由原 owner 管理。Session 的批占用投影集中到既有 `session_policy.py`，预留与回退仍在 Session。
完整显式 `AsyncEngineLimits` 现在可为 `R=None` 提供有限请求数；默认 R=2 和物理容量问题仍待处理。


共享额度的租约、队列、容量与公共事件留在 `shared_credit.py`，策略选择、VTC 入队修正及完成记账集中到
[credit_policies.py](src/scheduling/submission_control/credit_policies.py)；原公开类型和调用方式保持，没有新增账本或代理。
Session 的 registered Job 关闭状态只由 JobRegistry 保存，legacy Job 仍记住待回收身份。
[源码与 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#credit-session-duties)
保存释放顺序修正、重复检查清理及 140 项组合结果；物理容量、R 和反馈试探不属于本批。


Session 的成员选择、单请求和原子批接收集中到
[session_dispatch.py](src/scheduling/core/session_dispatch.py)，flow 生命周期和唯一任务表保持原 owner；
成功接收后的游标前移只处理一次。链的 record-local 传输类与其实现同在
[lotus_chain_actor_backend.py](src/execution_provider/adapters/lotus_chain_actor_backend.py)，实际测试接点同步。
旧 profiler 解析器已退役，准确原件与恢复说明见
[历史入口](../experiments/reproduction/native_baselines/README.md#retired-profiler-parser)。
[本批源码、检查与总量](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#core-duties-4679)
保留原失败；当前多任务／SAOR 隔离由另一专项继续，本批未修改其研究策略或接线。
