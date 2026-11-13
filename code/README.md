# SemLoom 数据执行工程

研究对象是 AI 语义算子与多模态数据任务的物理执行、数据组织与调度优化。
本目录提供外部执行核心；当前 records 使用 SemBench 等外部工作负载验证数据执行、供给、背压和生命周期，
LOTUS 提供可选兼容方法与对照。PG 实现和资料归档保留，当前不作为开发或验收前提。
普通 Movie records 通过本地 IPC 调用模型引擎。
整查询默认不设总期限，显式期限保留，启动和关闭分别等待。
普通records的新关闭请求只取得调用方剩余时间，已到期和在途引用分别处理，见[关闭时长](src/execution_provider/adapters/README.md#records-close-allowance)。
当前可选能力、默认及验证限制见[配置总览](../docs/执行配置与优化开关.md)和[代码状态](INFRA_STATUS.md)。

## 真实入口

| 任务 | 入口 | 职责 |
|---|---|---|
| Movie Q1/Q2/Q3/Q4 普通 Filter | [run_lotus_records_movie.py](scripts/experiments/run_lotus_records_movie.py) | 原输入、任务与评价，选择执行适配；Q4 为完整输入上的正面比例 |
| SemBench LOTUS原题与后端选择 | [run_sembench_text_query.py](scripts/experiments/run_sembench_text_query.py) | 原查询／评分共用；显式Ray原生路径接实际多批、视觉任务、Join→Extract、分组TopK及分层Aggregate，目标资格和计时范围另验；[实际范围](src/baselines/README.md#additional-sembench-text) |
| 完整查询矩阵／连续查询 | [run_movie_records_matrix.py](scripts/experiments/run_movie_records_matrix.py) | 显式 backend、生命周期、额度与时钟 |
| 原生 LOTUS 对照 | [lotus_native_local.py](src/baselines/text/frameworks/lotus_native_local.py) | 原算子／LM／LiteLLM batch_completion；仅 provider 使用现有模型 IPC |
| 独立 Kalypso 对照 | [作者适配说明](src/baselines/text/frameworks/kalypso_native/README.md) | 保留作者服务、参数与调度；不是 LOTUS 后端 |
| 原生 LOTUS 外部服务查询 | [run_sembench_lotus_query.py](scripts/experiments/run_sembench_lotus_query.py) | 图像／多模态原题与 Q5 原分层聚合共用查询生命周期、原评价和逐 POST 预算 |
| SemBench 图像准备 | [check_sembench_image_query.py](scripts/experiments/check_sembench_image_query.py) | Animals、MMQA、E-Commerce、Cars和Medical已接视觉题的输入／预算检查；[实际资格](INFRA_STATUS.md)按路径区分 |
| LOTUS CLIP 检索 | [run_lotus_image_retrieval.py](scripts/experiments/run_lotus_image_retrieval.py) | 原检索方法与明确的图像执行 owner |
| DuckDB＋作者 IMLane Q3 | [构建与接线](integrations/imlane_duckdb/README.md) | 原SQL／UDF路径；软件、冻结模型来源与当前计时资格分列，v2仍含内部同步回执开销 |
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
框架专用的 Ray Data／Daft 图留在基线模块。公共 CLIP 批处理由
[clip_batch_workers.py](src/modalities/image/clip_batch_workers.py)负责。

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

## 核心文件职责

下列文件承担不同的可独立修改职责，直接使用现有对象，不增加转发层或重复状态。

| 所属部分 | 文件与责任 |
|---|---|
| 普通 Filter | [lotus_records_core.py](src/execution_provider/adapters/lotus_records_core.py) 负责配置、模型复用与资源；[records_filter_runtime.py](src/execution_provider/adapters/records_filter_runtime.py) 负责接纳、结果和查询收尾 |
| 算子链 | [lotus_record_chain_core.py](src/execution_provider/adapters/lotus_record_chain_core.py) 负责组装和资源生命周期；[lotus_chain_fused_runtime.py](src/execution_provider/adapters/lotus_chain_fused_runtime.py) 负责融合批传输；[records_query_runtime.py](src/execution_provider/adapters/records_query_runtime.py) 持有记录级方法和关系数据 |
| 服务数据释放 | [传输](src/execution_provider/adapters/lotus_chain_actor_backend.py)、[准备组织](src/execution_provider/adapters/lotus_chain_token_batches.py)与[actor owner](src/execution_provider/adapters/lotus_records_actor_owner.py)各清理自身字段；链owner核实交接并按原顺序调用，不再直接改写这些私有字段 |
| 容量声明 | [records_capacity.py](src/execution_provider/adapters/records_capacity.py) 定义普通记录／融合链容量、独立预算和 IPC 解析；查询 owner 只消费这些值 |
| payload物化 | [payloads.py](src/data/materializers/payloads.py)处理sealed rows与Arrow／Daft物化；Daft配置直接调用[infrastructure.daft_runtime](src/infrastructure/daft_runtime.py) |
| 扫描与模型适配 | [records_scan.py](src/execution_provider/adapters/records_scan.py) 共用 Daft Native 扫描；[records_async_model.py](src/execution_provider/adapters/records_async_model.py) 接非阻塞请求与回执，owner 保留 Ray 分配和 RPC 终态责任 |
| Session | [session.py](src/scheduling/core/session.py) 管服务回执、准备借用和任务表；[session_dispatch.py](src/scheduling/core/session_dispatch.py) 管成员选择与提交；[session_flow.py](src/scheduling/core/session_flow.py) 管交付、取消和消费者关闭 |
| 调度 | [scheduler.py](src/scheduling/core/scheduler.py) 负责普通提交与真实完成；[envelope_source.py](src/scheduling/core/envelope_source.py) 读取阻塞输入；[topology.py](src/scheduling/core/topology.py) 选择端点和投影本地占用；共享研究通过显式交接接入 |
| 反馈 | [submission_control](src/scheduling/submission_control/README.md) 分开配置、供给、请求试探、批反馈和计量；现用请求与批策略各有明确消费者 |
| 引擎与 IPC | [vllm_protocol.py](src/serving/backends/vllm_protocol.py) 保存中性规范；sync／async 各自构造引擎；[父进程、worker与帧编码](src/serving/backends/README.md)各管自身生命周期，两种关闭均沿用调用方实际剩余时间 |
| 本地服务与任务接点 | [image_gateway.py](src/execution_provider/image_gateway.py)拥有图像连接与方法生命周期；[image_connection.py](src/execution_provider/image_connection.py)处理图像帧，[gateway_runtime.py](src/execution_provider/gateway_runtime.py)保存连接消息与状态；[incremental_execution.py](src/execution_provider/adapters/incremental_execution.py)只组合现有Core任务对象 |
| 图像执行 | [image_backend.py](src/semantic_methods/lotus/image_backend.py) 组装原方法；[staged_execution.py](src/modalities/image/staged_execution.py) 负责HSE调度与清理，[staged_metrics.py](src/modalities/image/staged_metrics.py)累计计时／字节／峰值并装配原结果；图像选择类型与分片函数归[中性图像类型](src/modalities/image/contracts.py)，[文件输入](src/modalities/image/file_source.py)读取文件资产；[检索结果写入](src/experiments/lotus_image_reporting.py)留在实验层，增量图像backend继续使用自身任务协议 |
| 观测 | [文件职责](src/observability/README.md#current-observation-owners) 归集时钟、事件与模型复用；[phase_sampler.py](src/observability/process_resources/phase_sampler.py) 保存分阶段RSS／CPU采样，图像CLI直接导入实际实现 |
| 离线记录汇总 | [record_dag_observation.py](src/experiments/record_dag_observation.py)关联回执、事件与CPU片段；[record_dag_windows.py](src/experiments/record_dag_windows.py)计算时间区间及样本重叠，读取内容同时用于解析和来源摘要 |
| Movie实验执行 | [lotus_records_movie.py](src/experiments/lotus_records_movie.py)保留参数与预检；[movie_records_query.py](src/experiments/movie_records_query.py)共享查询评价／回执合并；[movie_records_worker.py](src/experiments/movie_records_worker.py)拥有普通后端的构造、运行与关闭，Session和IMLane直接导入实际实现 |
| 原生查询生命周期 | [native_query_lifecycle.py](src/experiments/native_query_lifecycle.py) 观察调用方driver的启动、完整查询与关闭；后端支持／复用声明随调用传入 |
| 独立研究 | [研究索引](src/research/README.md)分别登记[多任务／SAOR](src/research/multitask/README.md)和[文本组织／到达回放原型](src/research/data_organization/README.md)；生产payload及请求组批直接用公共运行组件，研究不进入普通records默认配置 |
| 离线代价 | [operator_costs](src/experiments/operator_costs/README.md)拥有CSV／profile特征、既有估计比较与SemFilter参考校准；分析CLI负责输入、评估及输出，通用数学仍在planning/costs，运行链估算与反馈保持 |
| 实验服务核对 | [shared_vllm/preflight.py](src/experiments/shared_vllm/preflight.py)供场景脚本读取idle／命令行；[0.25.1源码审计](src/experiments/saor/vllm_0251_source_audit.py)拥有固定服务身份 |
| 校准身份 | [calibration/core.py](src/experiments/calibration/core.py)适配Movie身份、模型ready及完整查询回执；实际容量只取一次现有Movie解析结果，归一化与准备模式共同读取，跨候选默认对比保持 |

SDK 与 Movie／Session 的 R 默认均为 `None`。普通 async 和已支持 rolling 从完整 Core 预算、typed IPC、
JSON 中显式的 `max_inflight` 或服务 payload 声明取得有限额度；缺少来源在启动前报告。
C 是当前请求窗口，结果槽与输入／结果字节各自计量，均不是 vLLM 内部调度参数。
静态／低层链仍需其支持的有限声明；cgroup／内存压力采样不能代替物理 RAM 授予或完整查询内存计量。

当前未完成项、真实环境检查和代码量使用[整体完成清单](INFRA_STATUS.md#refactor-completion-checklist)；
配置用途和适用范围只维护在[配置总览](../docs/执行配置与优化开关.md)，
各批次测试数字只从主报告和证据台账读取。本页不重复逐批发布历史。
