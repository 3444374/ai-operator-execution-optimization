# SemLoom 数据执行工程

研究对象是 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
本目录提供外部执行核心；当前 records 使用 SemBench 等外部工作负载验证数据执行、供给、背压和生命周期，
LOTUS 提供可选兼容方法与对照。这些结果不表示已接入新的 PostgreSQL 语义算子。
普通 Movie records 通过本地 IPC 调用模型引擎；不启动旧 Movie HTTP 服务。
整查询默认不设总期限，显式期限保留，启动和关闭分别等待；旧SemLoom fixed HTTP接线正在退役，未来多查询不以HTTP为接入方式。
当前可选能力、默认及验证限制见[配置总览](../docs/执行配置与优化开关.md)和[代码状态](INFRA_STATUS.md)。

## 真实入口

| 任务 | 入口 | 职责 |
|---|---|---|
| Movie Q1/Q2/Q3/Q4 普通 Filter | [run_lotus_records_movie.py](scripts/experiments/run_lotus_records_movie.py) | 原输入、任务与评价，选择执行适配；Q4 为完整输入上的正面比例 |
| Movie Q4/Q8/Q9-map/Q10 与 MMQA 十二道文本题 | [run_sembench_text_query.py](scripts/experiments/run_sembench_text_query.py) | 原查询选择／汇总与原评价，单个 F/M/E 交既有原生图；[逐题范围](src/baselines/README.md#additional-sembench-text) |
| 完整查询矩阵／连续查询 | [run_movie_records_matrix.py](scripts/experiments/run_movie_records_matrix.py) | 显式 backend、生命周期、额度与时钟 |
| 原生 LOTUS 对照 | [lotus_native_local.py](src/baselines/text/frameworks/lotus_native_local.py) | 原算子／LM／LiteLLM batch_completion；仅 provider 使用现有模型 IPC |
| 独立 Kalypso 对照 | [作者适配说明](src/baselines/text/frameworks/kalypso_native/README.md) | 保留作者服务、参数与调度；不是 LOTUS 后端 |
| 原生 LOTUS 图像查询 | [run_sembench_lotus_image_query.py](scripts/experiments/run_sembench_lotus_image_query.py) | 已有独立 VLM 服务上的原查询、评价、逐 POST 预算和查询监督 |
| SemBench 图像准备 | [check_sembench_image_query.py](scripts/experiments/check_sembench_image_query.py) | Animals 四题与 MMQA Q2a/Q2b/Q7 的输入／预算检查；原生 LOTUS 服务运行入口已接；当前模型实机资格待验证 |
| LOTUS CLIP 检索 | [run_lotus_image_retrieval.py](scripts/experiments/run_lotus_image_retrieval.py) | 原检索方法与明确的图像执行 owner |
| DuckDB＋作者 IMLane Q3 准备 | [构建与接线](integrations/imlane_duckdb/README.md) | 隔离构建与 PROCESS／THREAD 软件CPU链路已验证；模型和额外输入／错误场景仍待验证 |
| 观测与有限测试 | [record_observation](scripts/profiling/record_observation/README.md) | 使用既有监督与回执，不增加执行容量 |

命令与正常配置入口见[脚本说明](scripts/README.md)。输入、私有 packet、模型环境、GPU 租约和实际版本应由每次运行绑定，
不能把示例值、历史成绩或 CPU 替身当成当前实机资格。原生 LOTUS 非 HTTP 适配已有 CPU 检查，
完整查询、真实模型、质量与性能按各自源码和环境核对，不能从单项 CPU 检查转授。

## 执行与语义分层

| 层 | 维护位置 | 拥有的责任 |
|---|---|---|
| LOTUS 方法 | [semantic_methods/lotus](src/semantic_methods/lotus/README.md) | formatter、parser、方法状态、关系屏障与逐行结果协议 |
| SemLoom 执行适配 | [execution_provider/adapters](src/execution_provider/adapters/README.md) | Core／有限链／rolling／admission 与结果交付接线 |
| 调度与资源责任 | [scheduling](src/scheduling)、[反馈模块职责](src/scheduling/submission_control/README.md) | 就绪选择、在途额度、背压、预算和 unknown 责任 |
| 基础多查询与独立研究 | [主线 Session](src/scheduling/core/session_jobs.py)、[research/multitask](src/research/multitask/README.md) | 主线保留固定份额、普通轮转和查询资源责任；共享额度研究、服务目标及候选编排独立维护 |
| 模型与传输 | [serving/backends](src/serving/backends) | 引擎构造、本地 IPC、逐请求终态、无调度的有限链计算及进程清理 |
| 模态公共能力 | [modalities](src/modalities) | 文本／图像协议、成本、共享 CLIP workers；不拥有 baseline 图 |
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

原生 Ray／Daft／IMLane 的 [sync_reference_engine.py](src/baselines/text/frameworks/sync_reference_engine.py)
和 [reference_chain_batch.py](src/baselines/text/frameworks/reference_chain_batch.py)保留原生 chat 调用。
三项 vLLM 容量未指定时交引擎解析，显式值保持；[参数归属](src/baselines/README.md#共享层和参数)
区分模型容量、LOTUS 客户端批大小和原生图批大小。作者 Kalypso 调度、模型提示和输出上限保持。

## 退役、暂停和研究保留

旧 Movie HTTP owner、旧固定五后端矩阵、PG／HTTP 实验 CLI、相连旧对照脚本／配置和专用测试已退出维护树。
PG 扩展 C／SQL、部署源和实验接线按准确 Git 树归档，**暂停而非放弃 PG**；恢复须在隔离树中进行，
不把历史源重新加入当前生产搜索路径。逐文件摘要与恢复方法见
[归档索引](../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)。

SAOR 与多任务共享额度一起由 [research/multitask](src/research/multitask/README.md) 维护，
离线分析与研究证据继续保留；依赖已退役 HTTP／PG 命令的旧启动胶水随归档隔离。
当前源码不再承诺这些旧命令可运行。独立 PG provider／通用传输内核和有研究价值的模态／调度组件若仍有消费者则保留，
不因停止旧实验而删除其公共协议，也不宣称它们已接入当前 SemBench。

所有历史成绩、负结果、unknown、成本和五份具名代码投影保持。
[历史恢复入口](../experiments/reproduction/native_baselines/README.md)只恢复指定源码，不执行模型。
当前新增 CPU 检查不转授历史 GPU 资格，不宣称清理带来查询加速。

## 核心文件职责

下列文件承担不同的可独立修改职责，直接使用现有对象，不增加转发层或重复状态。
完整改动、原失败、准确受测源码与恢复方式见[重构主记录](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)。

| 所属部分 | 文件与责任 |
|---|---|
| 普通 Filter | [lotus_records_core.py](src/execution_provider/adapters/lotus_records_core.py) 负责配置、模型复用与资源；[records_filter_runtime.py](src/execution_provider/adapters/records_filter_runtime.py) 负责接纳、结果和查询收尾 |
| 算子链 | [lotus_record_chain_core.py](src/execution_provider/adapters/lotus_record_chain_core.py) 负责组装和资源生命周期；[lotus_chain_fused_runtime.py](src/execution_provider/adapters/lotus_chain_fused_runtime.py) 负责融合批传输；[records_query_runtime.py](src/execution_provider/adapters/records_query_runtime.py) 持有记录级方法和关系数据 |
| 容量声明 | [records_capacity.py](src/execution_provider/adapters/records_capacity.py) 定义普通记录／融合链容量、独立预算和 IPC 解析；查询 owner 只消费这些值 |
| 扫描与模型适配 | [records_scan.py](src/execution_provider/adapters/records_scan.py) 共用 Daft Native 扫描；[records_async_model.py](src/execution_provider/adapters/records_async_model.py) 接非阻塞请求与回执，owner 保留 Ray 分配和 RPC 终态责任 |
| Session | [session.py](src/scheduling/core/session.py) 管服务回执、准备借用和任务表；[session_dispatch.py](src/scheduling/core/session_dispatch.py) 管成员选择与提交；[session_flow.py](src/scheduling/core/session_flow.py) 管交付、取消和消费者关闭 |
| 调度 | [scheduler.py](src/scheduling/core/scheduler.py) 负责普通提交与真实完成；[envelope_source.py](src/scheduling/core/envelope_source.py) 读取阻塞输入；[topology.py](src/scheduling/core/topology.py) 选择端点和投影本地占用；共享研究通过显式交接接入 |
| 反馈 | [submission_control](src/scheduling/submission_control/README.md) 分开配置、供给、请求试探、批反馈和计量；现用请求与批策略各有明确消费者 |
| 引擎与 IPC | [vllm_protocol.py](src/serving/backends/vllm_protocol.py) 保存中性规范；sync／async 各自构造引擎；[async_engine_process.py](src/serving/backends/async_engine_process.py)、[async_engine_worker.py](src/serving/backends/async_engine_worker.py)、[async_engine_wire.py](src/serving/backends/async_engine_wire.py) 分管父进程、子进程和帧编码 |
| 图像执行 | [image_backend.py](src/semantic_methods/lotus/image_backend.py) 组装原方法；[staged_execution.py](src/modalities/image/staged_execution.py) 继续服务现用HSE执行；[CLIP实验模块](src/experiments/image_clip/README.md)分别准备输入、执行观测与生成报告；增量图像backend继续使用自身任务协议 |
| 观测 | [文件职责](src/observability/README.md#current-observation-owners) 归集时钟、事件与模型复用；[phase_sampler.py](src/observability/process_resources/phase_sampler.py) 保存分阶段RSS／CPU采样，图像CLI直接导入实际实现 |
| 原生查询生命周期 | [native_query_lifecycle.py](src/experiments/native_query_lifecycle.py) 观察调用方driver的启动、完整查询与关闭；后端支持／复用声明随调用传入，Kalypso不再临时改写默认集合 |
| 独立研究 | [research/multitask](src/research/multitask/README.md) 保存共享额度和 SAOR；实验编排不进入单查询默认配置 |

SDK 与 Movie／Session 的 R 默认均为 `None`。普通 async 和已支持 rolling 从完整 Core 预算、typed IPC、
JSON 中显式的 `max_inflight` 或服务 payload 声明取得有限额度；缺少来源在启动前报告。
C 是当前请求窗口，结果槽与输入／结果字节各自计量，均不是 vLLM 内部调度参数。
静态／低层链仍需其支持的有限声明；cgroup／内存压力采样不能代替物理 RAM 授予或完整查询内存计量。

当前未完成项、真实环境检查和代码量使用[整体完成清单](INFRA_STATUS.md#refactor-completion-checklist)；
配置用途和适用范围只维护在[配置总览](../docs/执行配置与优化开关.md)，
各批次测试数字只从主报告和证据台账读取。本页不重复逐批发布历史。
