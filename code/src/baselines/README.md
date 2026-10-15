# Baseline and comparison harnesses

[`image/frameworks/daft_clip.py`](image/frameworks/daft_clip.py)提供本地 CLIP 的原生 Daft 有状态 UDF 批次接点。
`DaftClipExecutor.embed_batch(doc_ids, encoded_images)`接收稳定 ID 和 encoded bytes，返回共享 `ImageEmbeddingResult`，
完整归一化 `float32[N,512]`；语义 revision 为实际本地路径，内容 SHA 与版本另记于 `identity`。
图与批次归 Daft，不接 HTTP／SemLoom 调度；自写图登记为 `diagnostic_reference`。
本机／Linux 软件资格、9 图真实 GPU 向量与模型释放核验、计时依赖见[独立报告](../../../experiments/results/diagnostics/daft_clip_native_20261014/README.md)。

NativeLOTUS、Daft、Ray Data、SemLoom、IMLane 两模式及现有图像 CLI 共用可选外层诊断。
设置 `SEMLOOM_DETAILED_TIMING_DIR` 后采集，未设置时保持原总时与输出；图构建／session 初始化等只按实际操作命名，
不推定厂商排队、actor 初始化或 GPU 计算。[字段与验证](../../../experiments/results/diagnostics/optional_execution_timing_20261014/README.md)
同时说明原图生命周期、图像未知首批／EOF及计时开销。

文本 Ray Data 的 [清理配对修补](../../../experiments/results/diagnostics/optional_execution_timing_20261014/README.md#ray-cleanup-parity)
在计时 ON/OFF 下均保留实际 iterator，独立尝试 close／shutdown；业务首错与清理次错说明保持，
真实清理失败优先于纯计时故障，失败或未确认责任继续拒绝复用。
固定`1d6453d`的[实际Ray2.56.1限定核验](../../../experiments/results/diagnostics/optional_execution_timing_20261014/README.md#ray-cleanup-sdk)
完成四图／8次localhost POST、0模型／0GPU，独立退出及lease检查通过；不将close返回等同于全部actor／GPU退出。

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
常驻入口保留原生Daft runner和Ray连接；当前图、原生actor和HTTP session的实际生命周期分别记录，
不向原生图加入Core或复用已完成结果。合法行间有状态API与当前采用设置见[常驻检查](../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#persistent-fixture)。

[`text/frameworks/native_calls.py`](text/frameworks/native_calls.py)承接LOTUS与DuckDB已生成的完整调用，
由Daft Native或Ray Data图执行并返回完整HTTP状态、原始正文、重复header及HTTP版本。
HTTP错误与网络失败分别交回原算子；响应超量和身份错误直接停止执行。LOTUS原缓存、提示、SDK解析
及阶段等待的采用方式见[LOTUS适配](../semantic_methods/lotus/README.md#daft-native与ray-data执行)。
旧固定Map与SQL读取入口继续保留。
同一`NativeCallExecutor`一次只拥有一个活动批次。已启动的图缺结果、消费线程未结束或图／HTTP池
关闭出错或收到无完整HTTP响应的`transport_error`时，记录`unconfirmed_work=True`并拒绝该对象后续批次；
即使本地收到全部错误结果、线程与连接池已退出，也不能据此确认远端停止。完整HTTP错误响应（如429）仍可复用。
IMLane另持久记录调用者拥有的Coordinator未知状态，跨执行器对象也要求先清理；发送前取消仍可复用。
替换执行器前由调用者核清其运行时，报告保留`remote_http_stopped='unavailable'`，不把消费结束当作模型停止。
厂商生成器在正常EOF的内部关闭中抛错也保留未确认责任，即使全部结果已交付；
`worker_errors`继续记录原错误，后续批次不再进入图。

`NativeGraphOptions`构造只核对正整数及有限大小；所选后端在创建执行器和进入原生图前另做检查。
Daft保持C≥2及`batch_size=1/max_concurrency=C−1`；Ray Data按活动请求检查A×B≤C，
并要求`num_threads > ray_actors`，为原SDK的预处理／后处理task留出CPU。
CPU1／A1会在导入和启动前拒绝；这项检查不改变原图或调度。
IMLane和Ray Core可声明C=1，不借未选用的Daft或Ray Data参数拒绝；实际作者线程数、
Ray逻辑CPU及调用／结果字节额度继续由各自入口核对。这不修改任何已登记运行的额度或配置。

Ray Data完整调用路径的A为HTTP stage actor数，B为每actor的异步批次数，R为`ray_batch_rows`。
[Ray2.56.1 planner](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/_internal/planner/plan_udf_map_op.py)
在导入时读取`RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY`，上游默认B=4；
项目入口在导入前按所选配置设置该变量，默认B=2。导入后修改环境不会改变已加载的常量，
执行前仍须与声明值一致。关闭报告分别保存`async_batches_setting`与`async_batches_effective`；
未读取或不匹配时，活跃HTTP上界写`null`，不借配置推断执行已经发生。
[固定HTTP stage](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/batch/stages/http_request_stage.py)
逐个await批内的惰性`session.post`。当前stage的actor `max_concurrency=1`时，
`active_http_upper_bound=A×B`，R增加单批保留行数与串行工作量；
`request_row_envelope=A×B×R`只记录HTTP stage的批内行包络，R不再占用HTTP活动名额。
完整输入另受有限批次行数和payload字节约束；两个入口均在读到首个越界项时拒绝，尚未创建图。
`NativeCallExecutor`沿用4096行／32MiB输入默认上限，32MiB结果阈值约束累计已消费的完整响应。
`batch_storage`分列实际输入、输入上限、累计消费阈值及所有单响应allowance之和；后者不作新的准入条件。
厂商图可以在消费前保留结果，base64、JSON与SDK还有副本，因此这些字节数和行包络均不保证整个图的RSS。
prepared字典入口默认同样限制4096行／32MiB payload；固定Map查询按既有source额度加每行固定请求模板开销
推导独立payload额度，不把raw source字节等同于完整请求字节。响应上限仍由其session factory负责。
合同修正、最小原生API差异与实际核验见[文本接入检查](../../../experiments/results/diagnostics/native_text_capacity_20261015/README.md)。
活跃上界不是已测峰值。
实际`observed_peak_http`复用查询入口的统一代理记录，由同一monotonic时钟上的
上游发送至完整正文读完区间计算，原件为`http-trace.jsonl`；不从C或参数乘积补造。
数据量、source blocks与原生背压可以使实测峰值更小；该HTTP指标不表示GPU内核并发。
无真实图验证的配置／批次循环替身检查，只说明调用与报告行为。

Daft默认与详细计时共用已有清理helper：执行首错保持，iterator关闭次错附注；decorator、表达式或iterator
建立失败也释放已登记的本地control。该对象留存修补不表示发现了GPU泄漏。

IMLane继续使用已有作者桥。`reuse_bridge=False`默认逐batch写`input.jsonl`并创建桥进程；
显式`True`复用同一执行器内的桥session。公共[`_lotus_rows`](../experiments/postgresql/supplier_adapter_query.py)
默认每查询创建并关闭IMLane执行器；LOTUS常驻owner显式选择`reuse_bridge=True`时，借用owner持有的
既有`NativeCallExecutor`及桥session，查询接点独立，桥在owner退出时关闭。有限期限与未知责任保护保持。
作者CPython／Ray两模式及外部Coordinator的拥有与退出责任不变，无需另造session接口。

Daft已有按事件循环复用客户端、各调用保留期限、关闭失败与未知工作不可直接复用的检查。
这些责任检查与“请求充分时是否持续供给”分别验证；`peak≤C`只说明未超额度，
完整响应消费及准备等待也可能减少活动请求。低成本替身检查不据此改写固定Daft配置。
[独立Daft检查](../../tests/baselines/test_native_daft_activity.py)提供24条预先准备的完整请求、
连续消费和服务端活动时序，另核同一control在两个live loop上的客户端拥有与关闭。
实际图用例需要固定0.7.21及显式`SEMLOOM_NATIVE_CALL_TEST=1`，无需Ray；缺库时保留跳过。

Ray stage看到的`status=200`表示完整调用结果载体，实际状态、原始正文及传输错误仍由原算子SDK解释。
`max_retries=0`，原图的task重试与actor重启也关闭；这条适配没有保留Ray HTTP stage
对模型原始状态的全部错误处理行为。固定方法的执行比较与各系统自有方法的完整任务比较分别报告，
已经准备的HTTP调用不能代替读取、准备和消费的完整数据流对照。

[`text/controls/ray_core_calls.py`](text/controls/ray_core_calls.py)另用Ray Core原生actor和`ray.wait`，
固定数量、每actor单调用的静态分片拓扑明确登记为`diagnostic_reference`。
原生actor队列拥有串行执行，Ray拥有actor调度；没有SemLoom策略或Python HTTP请求池。
`NativeCallExecutor("ray", ...)`选择该参照，`"ray-data"`仍选择官方HTTP Processor图。
actor数不能超过配置及实际Ray逻辑CPU数量；取消只终止本次创建的actor，服务端HTTP状态仍需独立核实。

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
