# IMLane完整调用适配

本切片将原算子方法的完整请求交给IMLane作者执行器，返回完整HTTP响应。
[公共适配器](../../src/baselines/text/frameworks/imlane_calls.py)复用
`650f65c8`交付的[Sema DBEnd客户端](imlane_http.cpp)和
[原HTTP函数](imlane_http_udf.py)，不新增作者调度器或重建作者系统。
作者源码固定为`072f8257db80569e42c9f5727d839587df529652`，需要已审计的patched构建。
原桥配置及构建入口见[目录说明](README.md#imlane薄客户端)。

## 调用与配置

`IMLaneCallExecutor(config, options, *, max_batch_rows=128, max_batch_bytes=32*1024**2, max_pending_calls=128)`
的`backend`为`imlane`。`IMLaneCallConfig`必须显式给出：

| 字段 | 作用 |
|---|---|
| `runtime` | `cpython`或`ray`，均为作者执行器；Ray模式单独报告，不记为Ray Data |
| `bridge_config_path` | 仓库外原桥JSON配置的绝对路径 |
| `coordinator_pid`, `coordinator_start_ticks` | 调用方拥有的Linux Coordinator身份 |
| `author_build_identity` | 已核验作者build-identity.json的绝对路径 |
| `n_executors` | 实际启动Coordinator的声明数，默认2 |

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
独立MovieQuery预检仍写128行，发布者须结合新桥配置接续该入口；不能用旧二进制接收新参数。
本次只支持软件调用容量与生命周期，真实模型、质量、性能和全部4096行能力尚未验证。

原桥空stdin、线程参数0可读取实际META。2026-10-12的两个软件模式均返回
settings `native_executors=4`、`native_task_threads=6`；实际Coordinator声明2个executor，
两模式的HTTP轨迹均触及2个worker。配置线程固定6，`NativeGraphOptions(concurrency=8)`。
settings数量不表示物理executor数；未来配置仍须重新探测，不复用这次PID或端口。

## 作者循环与结束责任

[薄HTTP函数](imlane_complete_call.py)调用原HTTP函数后恢复作者原event loop，
使后续作者异步Arrow函数仍能执行。HTTP失败保持transport error，容量、输入身份和作者UDF异常
回送execution_error，由调用方恢复已知内置类别、保留未知类别的原诊断；
任意UDF异常与全部原方法错误类型的逐项一致性仍待核对。薄函数不改prompt/parser，
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
