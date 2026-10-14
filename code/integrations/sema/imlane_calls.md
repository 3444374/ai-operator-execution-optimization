# IMLane完整调用适配

本切片将原算子方法的完整请求交给IMLane作者执行器，返回完整HTTP响应。
[公共适配器](../../src/baselines/text/frameworks/imlane_calls.py)复用
`650f65c8`交付的[Sema DBEnd客户端](imlane_http.cpp)和
[原HTTP函数](imlane_http_udf.py)，不新增作者调度器或重建作者系统。
作者源码固定为`072f8257db80569e42c9f5727d839587df529652`，需要已审计的patched构建。
原桥配置及构建入口见[目录说明](README.md#imlane薄客户端)。

## 调用与配置

`IMLaneCallExecutor(config, options, *, max_batch_rows=None, max_batch_bytes=None, max_pending_calls=None, ...)`
的`backend`为`imlane`。`IMLaneCallConfig`必须显式给出：

| 字段 | 作用 |
|---|---|
| `runtime` | `cpython`或`ray`，均为作者执行器；Ray模式单独报告，不记为Ray Data |
| `bridge_config_path` | 仓库外原桥JSON配置的绝对路径 |
| `coordinator_pid`, `coordinator_start_ticks` | 调用方拥有的Linux Coordinator身份 |
| `author_build_identity` | 已核验作者build-identity.json的绝对路径 |
| `n_executors` | 实际启动Coordinator的声明数，默认2 |
| `max_batch_rows` | 同一完整调用批次的显式行容量；省略时128，已验证新桥配置可选2000 |
| `max_input_payload_bytes` | 全批次原始payload容量；省略时32MiB |
| `max_input_wire_bytes` | UTF-8 JSON lines及换行容量；省略时48MiB |
| `max_item_payload_bytes` | 单项payload容量；省略时8MiB |
| `max_pending_calls` | 桥内待交付数量；省略时128，不表示作者执行并发 |
| `max_batch_response_bytes` | 全批次完整响应累计容量；省略时32MiB |
| `reuse_bridge` | 默认`False`；显式`True`选择下述固定函数的常驻桥候选，作者CPython／Ray资格分别待验证 |

六项均由`IMLaneCallConfig.batch_limits`校验并传到既有DBEnd桥；公开单查询、常驻和MovieQuery的配置文件
读取同一组字段。省略构造参数时使用配置字段，显式参数可选择合法额度；公共`NativeCallExecutor`将自身
行数／payload上限与配置值取较小值，其他四项继续采用配置。`imlane_runtime_identity`记录六项实际配置。
输入预检拒绝时先清除上一批结束报告，不能把上一批`clean=True`用于本次失败。
本地47项回归通过、1项缺实际库条件跳过，只证明配置／协议fixture行为，不替代作者库、模型或性能核验。
SemBench的MovieQuery、数据／运行时预检和实际LOTUS执行器读取同一配置，不按128行拆开原Q1。
新配置须指向已验证桥二进制，摘要为`b201be8891493a2885ff9faae6d755bfe1eb7c20e10bdb2cda3f5f25c56cf5c6`。
该摘要对应已验证的逐批桥；`reuse_bridge=True`须另建本次候选二进制及独立配置，不能给旧二进制添加会话参数。
Coordinator PID/start ticks、端口、namespace和产物目录由新的拥有者重新建立，不能复用已结束的证据配置。

适配器在发送前核对PID/start ticks、实际二进制摘要、作者提交、运行模式、端口、
实例名称和executor声明。它要求调用方先启动Coordinator，不启动模型或Ray服务。
`iter_batch(calls, *, cancelled=None, timeout_s=120)`返回`NativeCallOutcome`迭代器；
`NativeCall`保留原方法的POST正文、重复请求头、超时和响应容量。
响应保留HTTP status、重复响应头、原始body bytes和HTTP版本，解析继续属于原方法。
重复源行允许出现，独立`call_id`必须唯一。

`IMLaneBatchLimits`由公共适配器传到Sema DBEnd适配器和C++参数，三层核对同一配置。
默认128行保留；显式2000行已在作者CPython和作者Ray完成原SemBench Q1的软件检查。
配置最多声明4096行，4096行实库执行尚未验证。有限额度如下：

| 项目 | 最大可声明值 |
|---|---|
| 全批次行数 | 4096；本次验证2000 |
| 全批次payload | 32MiB |
| UTF-8 JSON lines及换行 | 48MiB |
| 单项payload | 8MiB |
| 已投递未交付调用 | 128 |
| 累计完整响应及错误诊断编码字节 | 32MiB |

超额在作者投递前拒绝，不截断输入。单项完整响应额度仍由`NativeCall`指定；本次Q1为1MiB。
待交付额度只限制桥持有的future和结果引用，作者任务线程池、Lane和执行器继续拥有执行。
同一查询只创建一个DBEnd上下文；每交付一个结果就补入下一调用，不等待整组完成。
128行来源是本项目`5ee1b90c3`添加的C++快照检查，另由Python适配器检查；
这是旧调用桥的接收范围，不是作者系统自然行容量的证明。
新桥源码在结果检查、完整stdout输出及已完成future释放后立即清除对应`calls[i]`，
输入/输出表不再因全批次调用列表而一直保留到EOF；新扩容桥已仅链接现有作者库编译并执行。
旧97af50c5与新2cca502d编译来源分别保留，实际运行配置仍须核对新二进制摘要。
这减少已交付结果的留存，不保证待交付结果、Arrow/IPC复制和全部并发输出的聚合32MiB RSS。
原Q1的2000份完整调用在两种作者模式各交付一次，payload及复原原endpoint后的wire摘要与既有测量一致；
原LOTUS `sem_filter`消费所有响应后，原Q1返回5行。旧128行前置拒绝记录继续保留。
MovieQuery独立预检已贯通`IMLaneCallConfig.max_batch_rows`：显式2000配置允许完整Q1，旧配置仍保留128行拒绝；不能用旧二进制接收新参数。
本次只支持软件调用容量与生命周期，真实模型、质量、性能和全部4096行能力尚未验证。

原桥空stdin、线程参数0可读取实际META。2026-10-12的两个软件模式均返回
settings `native_executors=4`、`native_task_threads=6`；实际Coordinator声明2个executor，
两模式的HTTP轨迹均触及2个worker。配置线程固定6，`NativeGraphOptions(concurrency=8)`。
settings数量不表示物理executor数；未来配置仍须重新探测，不复用这次PID或端口。

## 作者循环与结束责任

[薄HTTP函数](imlane_complete_call.py)调用原HTTP函数后恢复作者原event loop，
使后续作者异步Arrow函数仍能执行。HTTP失败保持transport error，容量、输入身份和作者UDF异常
回送execution_error，由调用方恢复已知内置类别、保留未知类别的原诊断；
底层关闭报告另记录`bridge_started`和`author_submission_possible`。已创建的桥即使没有退出码，
只要完成未确认，就保留同Coordinator的持久未确认标记并拒绝新对象；创建前失败不声称作者工作已开始。
四态合成检查覆盖创建前失败、正常退出、非零退出和已创建但停止未确认，保留原始原因及清理注记；
它是报告保护的回归，不是历史泄漏证据。任意UDF异常与全部原方法错误类型的逐项一致性仍待核对。薄函数不改prompt/parser，
不重试，不生成虚假的HTTP成功状态。

`last_close_report`记录`clean`、`graph_consumer_stopped`、`prepared`、`received`、
作者META和进程身份。正常路径要求所有响应、EOF及子进程零退出码；
此时仅据作者DBEnd源码报告完成批次并发送RestoreLane。
它不会把调用方退出解释为作者Coordinator或远端HTTP已停止。

取消、提前停止消费或异常退出后，原桥只能停止本次DBEnd子进程。
已经发送的作者工作、Lane与远端HTTP继续标为未确认，报告要求调用方清理。
仓库外work_root保存按实例/PID/start ticks命名的uncertain记录；
同作用域的新适配器拒绝继续提交，直到owner核清资源。记录不是Coordinator互斥租约。
HTTP超时可正常回送一条错误值；即使DBEnd已排空，远端HTTP停止情况仍为unavailable。

当前原桥未向作者IPC传入非空查询Job ID；调用方出现身份及源行身份仍保留。
因此本切片不能证明作者查询间隔离、多查询公平性或独立取消。

<a id="bridge-lifecycle"></a>

## 桥生命周期与阶段测量候选

本次只改变LOTUS／IMLane适配的初始化寿命，并增加阶段观测；不改变作者调度、HTTP函数、
逐行独立请求、物理Arrow批次或HTTP client寿命。既有同步默认与可选async的实库资格保留，
本次常驻候选的作者CPython、作者Ray及完整桥编译另列pending，不能继承旧路径资格或据此做性能排名。

来源核对固定作者`072f8257`原始源码：
[C++示例](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/demo/cpp_example/dbend_cpp_example.cpp)
在同一context上Setup／Register一次，再连续执行三次两行表；
`DBEndContext::ExecuteAsyncFuncPush`进入作者TaskThreadPool，future返回只说明宿主已接收任务，
不代表已经拿到Lane。项目有限pending窗口是宿主选择，不声称复现论文Algorithm 2的全部机制。
`ExecuteFunction`在输出转换后销毁共享内存，再异步发送RestoreLane；成功归还没有ACK。
作者Coordinator和执行器等待归还后才复用Lane。本次继续调用这些接口，不重写作者执行器。

默认`reuse_bridge=False`仍每个非空批次写私有`input.jsonl`、新建桥／context、等待EOF与零退出码。
可选实现位于[会话消费器](../../src/execution_provider/adapters/imlane_session.py)，
仍逐批写同一完整调用文件，串行提交已完成预检的批次，只复用固定进程／context／函数注册。
每批独立32位十六进制ID；stdin只传最多1024字节的控制帧，文件只接受
`batch-<同ID>/input.jsonl`，拒绝符号链接、非普通文件和超量输入。同一会话最多4096批，
六项原容量仍逐批生效。以后若比较文件与管道传输，应作为独立消融，不与本次复用混在一起。

启动握手确认META、六项容量及协议版本；结果包含batch ID和原始完整outcome，
保留重复源行而拒绝重复出现身份、错行及旧批结果。`BATCH_END`须核对全部结果、字节数、
future／复制计数及批次序号后才允许下一批；它只证明本地作者回调返回、输出复制和交付完成，
不是Coordinator排空或远端HTTP取消的确认。固定函数文件／二进制摘要每批重查，不能通过重复注册实现热更新。
正常完整429响应可消费并复用；transport／执行错误、取消、提前结束消费、桥崩溃或协议错误会停止桥并隔离会话，
不自动重启或回退。公共适配器保留同Coordinator的仓外uncertain记录；杀桥不表示远端HTTP停止。
消费者仍停在yield时，公开`close()`／context退出也会先记录未知工作，不能等旧迭代器回收后再阻止新拥有者。
输出仍通过IPC复制获得调用方所有权，防止共享Lane归还后覆盖既有结果。

计时采用单调时钟，在内存累计后随结束记录一次回送，不增加逐事件写盘：

| 实际观测 | 口径 |
|---|---|
| Python `input_preflight_ns`, `call_encoding_ns`, `input_file_write_ns` | 预检、完整调用编码、文件写入与flush／fsync；预检本身也会编码，分别记录两次成本 |
| `popen_ns`, `process_to_metadata_ns`／`process_to_ready_ns` | Popen返回与从创建开始到首次META／会话握手；包含子进程启动、准备和context初始化，不是纯进程启动时间 |
| C++ `input_read_parse_ns`, `input_arrow_ns` | 文件读取／JSON核对与逐行Arrow表构造；前者剔除本地Arrow构造计时 |
| `context_setup_ns`, `function_register_ns` | DBEnd构造／Setup与固定函数注册；会话首批携带一次，随后为0 |
| `author_execute_inclusive_ns` | 各调用ExecuteFunction累计，包括作者调度、IPC、等待、下列输出复制和RestoreLane发送；并发调用会重叠 |
| `output_copy_encode_ns`, `output_copy_decode_ns` | 共享输出序列化到自有IPC缓冲，再读取为自有Arrow表；包含局部形状／容量核查 |
| `output_wire_ns`, `stdout_write_ns`, Python `outcome_decode_ns` | 结果身份／wire核对及封装、RESULT帧写出／flush、调用方解码；stdout项不含统计帧自身 |
| Python `consumer_wait_ns`, `cleanup_ns`, `batch_wall_ns` | generator交付后直到恢复的消费等待、清理与完整调用方批次；消费等待不等于纯parser时间 |
| C++ `batch_wall_ns` | 逐批桥含输入读取与初始化；会话从本批控制帧验证后开始，不含首批携带的会话初始化时间 |

生命周期记录包括bridge PID、batch ID、context／注册／作者调用／输出复制次数及已发送批次数。
作者内部IPC与模型执行的独立耗时未采集，因为既有API没有对应钩子；不能把ExecuteFunction差分
当成精确模型或IPC时间，也不能把上述重叠累计项相加成查询完成时间（JCT）。
编码容量不是聚合RSS上限，Bridge存活、回调完成和远端请求停止分别解释。
本机[检查记录](../../../experiments/results/diagnostics/imlane_bridge_lifecycle_20261014/README.md)
只支持协议和复制行为，实库窗口由主协调另行安排；原生LOTUS／SemLoom单卡验证可以独立继续。

## 已完成的软件检查

2026-10-12新增[2000行核验与全部失败记录](../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md#imlane-2000-capacity)。
备份七文件原样提交`2cca502d`，在已发布`1e969356`上只cherry-pick这些差异得到`7bb10980`；
组合源码的两作者模式错误、取消和清理后的下一查询另以12次POST复验。
本机容量/协议/分类13项通过；组合20项和首错/cookie4项通过、无跳过。
服务器4559次、本机4次，共4563/5000次localhost POST；作者测试从首次启动计时783.993/900秒。
自有173组PID/start ticks、14个实例、进程及共享内存清理核查通过，现有作者二进制未变，模型/GPU0。
小于诊断文本的32字节额度会由C++交付检查返回ValueError；256字节用例实际验证作者UDF错误类别及原诊断。
错误或已发送取消后保持uncertain记录，核清并结束自有Coordinator/Ray后才用新实例验证下一查询。

[协议测试](../../tests/baselines/test_imlane_calls.py)的8项在作者运行环境全部通过，
包含真实Arrow、完整回送、出现身份、发送前取消、快照超额拒绝、消费中止和跨对象uncertain记录。
新增的日志脱敏与拒绝覆盖检查在本机通过；它只改检查程序的落盘，不改适配器或HTTP函数。
[有界检查程序](verify_imlane_calls.py)执行真实作者CPython与作者Ray两种模式：

- 同一6份完整HTTP调用，覆盖200/401/429、gzip、非JSON正文、重复头及原始正文摘要；
- 同一实际LOTUS 1.2.4 formatter、LM、sem_map、SDK响应解析及4行完整消费；
- HTTP之后原作者105行Arrow函数回归、HTTP超时、已发后取消及owner资源清理。

成功轮actual02两模式各12次POST，共24次，无模型/GPU/重试；
作者Coordinator及独立2CPU/0GPU/128MiB Ray均正常结束，无已观测自有存活进程或共享内存。
先前actual01的CPython通过、Ray短路径问题失败及其12次POST仍保留，未计为双模式通过。
原生产profile要求10CPU，本次软件fixture批准8CPU，原失败预检保留；
独立CPU8 fixture profile通过只支持该无模型软件检查。

检查程序需要已有作者构建和其原verify helper、原桥二进制、独立LOTUS解释器、
已核版本的LOTUS原生方法适配源码及本地tokenizer；不安装依赖，不下载或加载模型权重。
环境预检及资源确认按[运行时说明](../../../deploy/runtime/README.md)执行。
所有配置、原始成功/失败记录和资源身份保存在仓库外；本次检查未运行真实模型、
六方法公平性能、真实GPU/设备对照或PostgreSQL端到端。
