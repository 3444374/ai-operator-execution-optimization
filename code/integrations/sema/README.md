# Sema后置原生执行检查

本目录保存Daft／Ray Data的实际库检查和IMLane完整HTTP调用的薄客户端。
原Sema仍拥有SQL、提示、解析及请求池；调用从HTTP抵达后交接，
实现与阶段定义见[Sema服务](../../src/execution_provider/adapters/sema_request_service.md)，
总设计见[语义系统方案](../../../experiments/plans/语义系统对照.md#sema-native-executors)。

## Daft和Ray Data

[verify_native.py](verify_native.py)执行有限完整请求及服务回送检查，不调用真实模型。
先按[运行时说明](../../../deploy/runtime/README.md)对独立driver环境检查`core,text`；
程序要求Daft0.7.21或Ray2.56.1，不安装依赖，不接管已存在的Ray会话。
Ray使用8CPU、零GPU、128MiB对象存储，拥有自己的短临时目录；只关闭本次启动的运行设施。

```sh
PYTHONPATH=code <driver-python> code/integrations/sema/verify_native.py \
  --backend daft --artifact-root /path/to/new-private-daft-check
PYTHONPATH=code <driver-python> code/integrations/sema/verify_native.py \
  --backend ray-data --ray-temp /path/to/short-ray-dir \
  --artifact-root /path/to/new-private-ray-check
```

`--scope graph`只检查原生图，`--scope service`只检查后置HTTP服务，默认两组均运行。
记录完整响应、出现身份、实际HTTP峰值、库源码摘要、取消和清理；短case未达到声明峰值时不称饱和。

## IMLane薄客户端

新增[桥生命周期与阶段测量候选](imlane_calls.md#bridge-lifecycle)，显式`reuse_bridge=True`才选择常驻context。
仍保留逐调用HTTP client和逐批私有输入文件，默认旧路径保持；新会话的两作者模式实库资格单独验证。

<a id="imlane-session-verifier"></a>

### 默认／复用的软件资格验证器

[verify_imlane_session.py](verify_imlane_session.py)复用既有`AuthorScope`与HTTP fixture，
每个窗口只选择一个作者运行时，顺序检查默认及复用路径；不上传、不编译、不安装或加载模型。
健康三批共用同一个执行器，核对完整请求／响应、重复源行、乱序、429、batch ID及结束栅栏，
持有首批输出并在后续批次后复核。transport、超时、消费中止、发送后取消、桥崩溃及SDK parser中止
各用独立Coordinator／namespace；保留首错与unknown记录，并拒绝同身份的新拥有者。
故障后的强制清理仍保留原始`passed=False`，只有自有进程、线程和共享内存退出均确认才记录隔离完成。

[独立SDK客户端](imlane_session_sdk_client.py)使用既有LOTUS解释器，经过公开执行器工厂、原LOTUS Map与
`MovieQueryResult.execute`的原Q1/Q2/Q3完整消费；关系是有限合成`software_fixture`，不替代原2000行任务或模型质量。
原batch adapter会将部分OpenAI解析错误作为结果值；parser中止探针显式在消费回调调用同一原SDK parser，
以检查异常退出后的隔离，不改正常查询错误语义。
整份源锁在作者启动前核对；只读SemBench Git checkout可以作为输入，配置及结果必须在Git仓库外。
LOTUS解释器保留venv路径，不把符号链接解析成基础Python。

执行前由主协调分配并持有共享host lease、八个CPU槽和串行窗口，完成独立环境预检及新桥构建。
实际host额度为16核，affinity可见128核不构成额外预算；与Ray／Daft重负载窗口串行。
验证器持续检查lease摘要、进程树RSS及本namespace共享内存；每格最多128 POST，
RSS8GiB／SHM2GiB，作者Ray2CPU／0GPU／128MiB对象存储。
本轮用`--wall-limit-s 0`取消健康运行的总时长计时器、cell deadline及elapsed上限，
健康SDK子进程的总运行等待也不设timeout；启动、parser、显式故障及既有单调用／单批接口的超时保持。
省略该参数仍保留默认限时：每格最多600秒、单作者运行时两格最多1200秒，含60秒清理预留。
lease变化、资源超量或启用的时间窗口到期停止新工作，只清理核对身份的自有进程。

```sh
taskset -c <assigned-eight-CPU-IDs> <author-python> -B code/integrations/sema/verify_imlane_session.py \
  --execute --runtime cpython --path-mode both \
  --author-root <checked-author-build> --bridge-binary <new-checked-bridge> \
  --lotus-python <existing-LOTUS-venv-python> --tokenizer <existing-local-tokenizer> \
  --checkout <pinned-read-only-SemBench-checkout> --source-manifest <candidate-source-lock> \
  --lease-file <held-shared-host-lease> --lease-sha256 <current-lease-SHA256> \
  --cpu-ids <same-eight-comma-separated-CPU-IDs> --cpu-quota-cores 16 \
  --output <new-private-output> --wall-limit-s 0
```

作者Ray在另一个已分配窗口使用`--runtime ray`。当前只有[本机替身与故障注入检查](../../../experiments/results/diagnostics/imlane_bridge_lifecycle_20261014/README.md#local-verifier)，
作者两模式／原SDK／新桥的实库资格仍pending；下方旧验证器的main不作为新常驻方案的资格入口。

[原生async任务函数](imlane_async_complete_call.py)作为独立候选，在作者循环上直接`await _request(call)`。
CPython通过作者`run_until_complete`执行，作者Ray通过原`AsyncActorWrapper`执行；两模式使用同一函数。
每次仍只有一份完整NativeCall，每调用session由既有kernel关闭，cookie与错误诊断采用发布组合的共享实现。
不缓存跨调用HTTP client，不新建循环、Runner、并发池或修改作者源码；既有同步UDF默认保持。
新函数只能在新的Coordinator/实例配置中选择，核对实际worker的任务源码摘要，避免作者module/alias缓存沿用旧函数。

本机65项有限回归通过、无跳过、HTTP/模型/GPU0；Arrow和session使用明确替身。
固定a54e1456的[实库记录](../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md#imlane-native-async-http)
在CPython首个307 cookie检查失败：实际aiohttp3.14.5调用raw-header更新接口，共享cookie代理继承的该方法为空操作。
153/384 POST、4.531/600秒；自有进程/共享内存及HTTP连接已清理，作者Ray和完整Q1未启动。
该失败轮共享native_calls.py按任务范围保持；原时间窗已结束，未重置或借用旧预算。
修后固定77cc1ed0基于发布者d004e720，采用其6629846a共享cookie修复与只读SemBench源码路径修复。
五个async实现/测试文件与已复审候选完全相同；本机67项有限回归通过，无跳过、实际HTTP/模型/GPU0。
两作者模式[新生命周期与原Q1](../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md#imlane-native-async-followup)
均通过：347/384 POST、53.543/600秒；完整Q1两模式各2000调用、共4000/4096 POST、90.019/900秒。
最终143组进程身份及10个实例清理通过，源码/作者库摘要未变；完整CLI/观测代理/官方质量评价与Q2/Q3另列未运行。
源码支持依据是固定作者`PyExecutor::ExecuteFunction`的async分支、Ray执行器的`AsyncActorWrapper`分支，
以及既有[async Arrow用例](../imlane/fixture_udf.py)；这些依据不代替新任务函数的实库检查。
[有限验证脚本](verify_imlane_async_calls.py)采用已直接批准的软件预算：CPU16–23、两作者模式、
计划最多348 POST、上限384 POST/600秒，只访问localhost并保留错误/取消后的资源核查。
脚本不上传、不编译或加载模型；需已有作者构建与桥二进制和独立输出目录。
执行前核对交付的`--source-manifest`，包括共享HTTP实现和调用方源码；worker回报新任务及共享HTTP源码摘要。
使用无字节码缓存的独立源码目录；任务函数逐项比较已加载HTTP函数体与源码编译结果，记录实际请求代码摘要。
逐批提交前预留最多POST次数；异常额外POST立即停止已核对身份的自有作者进程，强制清理不计通过。
时间信号停止新工作并保留90秒清理时间；每项清理独立尝试，检查自有PID/start-ticks、线程及共享内存。
所有正常响应消费后核查HTTP连接关闭，302/307的method、完整请求摘要及两执行器HTTP峰值分别检查。
本地[验证器故障注入](../../tests/baselines/test_imlane_async_verifier.py)覆盖清理异常、信号、PID复用和POST预算。
执行前按runtime手册保存`imlane-runtime`预检并确认CPU可用，外层使用有限timeout；原容量预算已封存。

```sh
taskset -c 16-23 timeout --signal=TERM --kill-after=3 597s \
  <author-python> -B code/integrations/sema/verify_imlane_async_calls.py --execute \
  --author-root <checked-author-build> --bridge-binary <existing-bridge> \
  --source-manifest <delivered-candidate-source-lock> \
  --output <new-private-output> --wall-limit-s 600 --post-limit 384
```

仅上述生命周期检查通过后，用独立15分钟/4096 POST预算检查两模式各完整2000行Q1。
[原Q1客户端](imlane_async_q1_client.py)核对原raw/CSV、完整payload及规范化wire摘要后才取得提交额度，
保持原LOTUS查询、parser和最后5条结果；各模式一个DBEnd上下文，预计4000 POST，模型/GPU0。
从公开`MovieQueryResult.execute`进入，经配置加载、`MovieQuery`容量检查和`NativeCallExecutor`工厂，
核查实际`IMLaneCallExecutor`/`SemaIMLaneExecutor`及桥、UDF、Coordinator身份。
完整CLI常驻owner/观测代理和官方质量评价另列未运行；此轮只运行公开查询API以保留4096次POST总额度，Q2/Q3未加入。
私有`q1-spec`提供已有LOTUS解释器、tokenizer、原查询源码/数据路径及既有摘要，不下载资产。

```sh
taskset -c 16-23 timeout --signal=TERM --kill-after=3 897s \
  <author-python> -B code/integrations/sema/verify_imlane_async_calls.py --execute --scope q1 \
  --author-root <checked-author-build> --bridge-binary <existing-bridge> \
  --source-manifest <same-delivered-candidate-source-lock> \
  --lifecycle-evidence <passed-lifecycle-summary> --q1-spec <checked-original-q1-spec> \
  --output <new-private-q1-output> --wall-limit-s 900 --post-limit 4096
```

该候选在作者循环上执行完整请求，session仍逐调用；不声称keepalive或性能收益。

作者实现采用[固定提交](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)。
共享作者构建由独立实施任务提供，本分支只链接Sema调用方，不复制作者库、依赖或虚拟环境。
需要已有构建目录中的`source/`、`prefix-patched/`与`artifacts/build-patched/build-identity.json`。
[build_imlane_http.py](build_imlane_http.py)核对来源、源码和二进制摘要，再链接
[C++ DBEnd客户端](imlane_http.cpp)，不会重建作者系统或覆盖既有客户端。

```sh
<imlane-python> code/integrations/sema/build_imlane_http.py \
  --root /path/to/checked-imlane-build \
  --output /path/to/new-private-bin/imlane_http
```

客户端通过作者任务线程池调用`ExecuteFunction`，输出复制到本次调用方持有的Arrow内存后回送。
[HTTP函数](imlane_http_udf.py)每次只执行一份原始完整调用，不合并/拆分提示，使用同步入口内的
`asyncio.run`完成HTTP，避免依赖当前Python3.12异步UDF事件循环问题；作者Lane和执行器仍管理执行。
作者库的Arrow16.1.0运行环境与查询driver的Arrow24保持隔离，HTTP函数所在解释器须支持aiohttp。

仓库外`--imlane-config`需声明`author_commit`、`bridge_binary`/`bridge_sha256`、
`udf_path`/`udf_sha256`、`gateway_port`、`work_root`、`namespace`及`native_task_threads`。
可选`env`只接受LD_LIBRARY_PATH、PATH、PYTHONPATH、TMPDIR和两项线程环境变量。
Coordinator须先在同一实例名称和loopback端口就绪；查询只拥有DBEnd子进程。
空输入与线程参数0可读取实际原生线程数，非空执行前必须与配置匹配，并且不超过声明请求容量。
取消后的远端工作写unavailable，客户端异常后禁止继续复用；正常结束需要全部响应、EOF和零退出码。

阶段、请求/查询P99和原始证据由[共同查询入口](../../src/experiments/postgresql/README.md#native-adapter-query)记录。
检查结果进入[同一主报告](../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#sema-native-executors)，
无模型检查不用于评价模型性能或Sema原池前调度。

## 三种原方法的IMLane模式

[完整调用接口](imlane_calls.md)复用上述DBEnd桥，明确区分作者CPython与作者Ray执行器。
公共`NativeCallExecutor('imlane', imlane_config=IMLaneCallConfig(...))`直接交接完整批次，
默认128行；显式2000行配置已通过两作者模式的原SemBench Q1软件检查。
三层共用有限行数、payload/wire、单项、待交付及累计响应额度，超额拒绝。
每查询一个DBEnd上下文，交付后补入下一调用并释放该输入/输出引用；聚合RSS没有32MiB保证。
LOTUS和DuckDB返回原SDK／C++解析，Sema保持原SQL及请求池后的有限HTTP快照。
统一CLI使用`--imlane-cpython-config`、`--imlane-ray-config`指定仓库外配置；
`--options`需覆盖探得的作者任务线程数，本次为6，检查配置使用并发额度8、作者执行器2。

[公共方法检查](verify_imlane_methods.py)在两种mode中检查真实LOTUS、DuckDB和Sema的完整消费及连续LOTUS查询。
它需要已有作者构建、原桥、已编译的`0.4.14-native3`扩展、固定Sema作者二进制和独立LOTUS解释器，
只访问localhost软件响应，不安装依赖或加载模型。来源和全部失败见
[本轮记录](../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md)。
独立单卡及双卡模型轮分别检查LOTUS六条路径，各轮两种IMLane模式各24真实POST、实际2个worker通过；
作者Ray另有2个actor ID，Coordinator／Ray正常退出并核对共享内存0。Sema／DuckDB模型尚未运行，不能借用LOTUS结果。
错误或提前停止消费可能在DBEnd返回EOF前关闭调用方；同一Coordinator出现未确认记录后禁止新对象继续提交。
调用者核对并结束自己的Coordinator／Ray资源后，使用新的进程身份与实例名称开始下一次检查。
