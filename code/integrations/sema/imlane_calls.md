# IMLane完整调用适配

本切片将原算子方法的完整请求交给IMLane作者执行器，返回完整HTTP响应。
[公共适配器](../../src/baselines/text/frameworks/imlane_calls.py)复用
`650f65c8`交付的[Sema DBEnd客户端](imlane_http.cpp)和
[原HTTP函数](imlane_http_udf.py)，不新增作者调度器或重建作者系统。
作者源码固定为`072f8257db80569e42c9f5727d839587df529652`，需要已审计的patched构建。
原桥配置及构建入口见[目录说明](README.md#imlane薄客户端)。

## 调用与配置

`IMLaneCallExecutor(config, options, *, max_batch_rows=128, max_batch_bytes=32*1024**2)`
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

有效快照上限为128行、32MiB输入；请求更大的配置仍如实暴露上述有效上限。
超额拒绝，不分段，不切换执行器。完整结果在调用方检查同一字节额度；原C++桥在此检查前
已经持有Arrow输出，尚无聚合32MiB驻留输出保证。
128行来源是本项目`5ee1b90c3`添加的C++快照检查，另由Python适配器检查；
这是当前调用桥的接收范围，不是作者系统自然行容量的证明。
新桥源码在结果检查、完整stdout输出及已完成future释放后立即清除对应`calls[i]`，
输入/输出表不再因全批次调用列表而一直保留到EOF；保存的97af50c5桥编译成功，源码摘要与当前桥一致。
编译记录见[发布组合记录](../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md#native-release-composition)；新二进制尚未用于作者runtime或2000行资格检查，实际运行配置仍须单独核验。
这减少已交付结果的留存，不保证待交付结果、Arrow/IPC复制和全部并发输出的聚合32MiB RSS。
当前原SemBench Q1的2000行及512行目标仍为unsupported，LOTUS入口先核对实际executor行容量。
后续最小方案需先核作者DBEnd投递/消费接口的自然持续能力，以及待交付总量和输入/IPC暂存责任，
再核实际2000行；不能仅提高128常量，也不能静默加入整块完成后才供应下一块的屏障。

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
