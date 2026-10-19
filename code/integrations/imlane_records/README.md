# IMLane 多行记录桥

[通用全局级联接点](cascade.README.md)消费原 `CascadeStage`，固定角色／阶段／查询并保留逐发生项结果；本机 CPU、作者 CPython、模型及 GPU 资格见[分列报告](../../../experiments/results/diagnostics/imlane_cascade_cpu_20261019/README.md)。

本目录保留无模型 v1 记录桥，并新增[普通 Filter v2 的 CPU 接线](#普通-filter-v2cpu-资格作者实机待验)。作者 DBEnd（数据库调用入口）、Coordinator（协调进程）、Lane
（共享内存交换通道）和 CPython executor（Python 执行进程）继续拥有执行与调度。
冻结 `0190e8cd` 的 v2 原方法、source、调用账本与 CPU 子进程通过[作者 CPython 无模型资格](../../../experiments/results/diagnostics/imlane_filter_author_cpu_20261019/README.md)；当前兼容／有限链修订的来源与待验项在下节分列。真实模型／GPU 和作者 Ray 仍待验，不接 Ray Data 或 SemLoom 模型池。
无模型 v1 桥从 `78f05783` 选择性接入，没有恢复旧 IMLane／Sema HTTP 执行树。

## 当前 Filter 合同与有限链接点

作者无模型报告固定在 `0190e8cd`，由 `9f1a817f` 交付；下述兼容修订和有限链只具有新的
本机 CPU／替身检查，不能重绑定为原作者资格，也没有真实模型、GPU 或作者有限链运行资格。
原 C++ v2 桥字节保持；新 Python 方法和关闭路径需要在新的源码归档和 owner 下核验。
`7cd08b3a` 有68项完整CPU检查；后续 `174ee853` 的两项丢账反例与63项 NoPopen 子集通过。
完整72项曾因自动审批服务容量错误未执行；恢复后在固定174完成72／72、零skip和15个已观察owned退出。
后续共享传输修复以marker字段存在停止继续提交，保留唯一有效终态／部分usage和实际raw回复；最新组合116项零skip、35个已观察owned退出，见[分层复验](../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#engine-result-receipts-cpu-20261019)。
新 wire 的作者实库／模型资格尚未启动，不能以旧68项或原作者归档补齐。

- Filter caller 先调用 [`bind_source(source, method)`](filter_workload.py)，执行 source preflight，
  取得中性 `RecordScan` 并冻结原 LOTUS 列序。worker 严格接收已绑定配置和 scan，拒绝缺列；
  Movie Q3 的 instruction／predicate／schema 继续由 Movie task 与其 runner 绑定。
  传输对象改为 `identity_json` 和 `payload_json` 两个字符串，payload 只含 scan 声明的文本列，
  不再要求 `reviewText`。raw Filter 的 payload 拒绝身份字段；有限链的业务同名列留在嵌套 values 中。
- [`RecordsResults`](records_host.py) 只复用 v2 有界提交／收集和关闭控制；作者继续拥有任务调度，
  pending 仍最多两块，没有新增模型池、线程池或 stage 执行器。
- [`chain_host.configuration`](chain_host.py) 在 owner 分配前校验单模型部署、有限预算、原方法 wire、
  source occurrence 与期限。caller 的 `LotusRecordChain` 自动绑定，worker 的 `from_dict` 严格接收。
  [`ChainWorker`](chain_udf.py) 只调用共同 `lotus_chain_batch.FusedChainBatch.run_rows()`；唯一 stage loop
  仍是 `LotusRecordChain.run_batch`，模型子进程复用 `ChainEngineProcess` 的逐调用 generation。
  `ChainResults` 把完整批 envelope 交给共同 `ChainBatchResults`，分别保留模型调用账本与最终关系。
  FME、MF、Map→Map→Filter、Filter→Filter→Map 共享此路径；Extract 仅末端，拒多部署、Join、分支和全局聚合。
- 一个 IMLane 物理 block 使用一条 transport occurrence，`batch_json` 字符串携带 1..16 个
  `RecordOccurrence`；整批只执行一次，不为每个输入重复运行 stage loop 或重复记账。
  `row_id/source_position` 是 source occurrence 身份，业务 index 可重复；业务 `row_id/call_id`
  保留在 `values` 中。原字节上限仍独立生效；logical JSON／Arrow bytes 不代表 RSS 或显存。
- Filter 和链共用解释器固定角色槽。alias 不能再建角色，模型构造互斥且失败不自动重试；
  同一查询块间复用一个 engine，两个查询使用独立 owner／engine／取消目录。

shared EngineProcess 的关闭预算仍为 API reply 11 秒与进程清理 4 秒。carrier 单独保留 20 秒，
不以 query deadline 截短；StopBudget 在这段 cleanup 内只登记信号，不提前抛错中断关闭。
Filter／chain 作者 root 的等待同样为 20 秒；历史 echo 默认 8 秒不变。
已启动模型必须同时确认 nested `engine_api`、实际子进程退出和空 cleanup errors，不能只凭 exit 标 closed。
GPU 始终单独核验；故障或无法核验仍保留 unknown 与 owner 责任，已有 usage／audit 不因关闭失败清空。
active model cancellation、未观察到的后代和实际 GPU 退出不能从 CPU 替身推导。

Filter 层收到条数、顺序或单项关联错误时，仍保留完整且唯一关联的实际终态；重复或矛盾身份仅留作
未关联 raw 回执，只有未确认调用补 unknown。host 先交付有效回执、停止补块并排空已接受块，再抛原错误。
私有 `filter-audit-*.json`、wire raw 与 close 的 `unconsumed_call_results` 保留已知 usage，不重发，也不升级既有 unknown。
本修复只覆盖 IMLane 实际收到的返回；共享 EngineProcess 在上游条数／关联检查失败时整批转 unknown 的既有边界仍待处理。

方法环境保留作者 Python3.12／Arrow16.1 ABI，driver 与模型解释器各自独立。
[`preflight_method`](method_environment.py) 在 owner 启动前核对两解释器 ABI 及固定 Filter／链源；
`ChainScope` 必须显式提供方法解释器，不回落到缺少方法依赖的作者解释器。
模型 child 使用共享 EngineProcess 的新 CODE `PYTHONPATH`，清除作者 `LD_LIBRARY_PATH`、
`PYTHONHOME` 和 `VIRTUAL_ENV`，显式使用 `model_python`；不向模型环境注入方法副本的 Arrow16 site。
本机 fresh CPU child 检查只验证这条装配机制，固定目标 vLLM0.25.1／无 LOTUS 可见性仍需当前 runtime preflight。

新 local-model 资格配置必须显式给出 `owner_resource_limits`，例如本轮独占单元的
`{"cpu_cores":8,"rss_bytes":34359738368,"shm_bytes":2147483648}`；RSS 是整个已观察 owned 家族之和。
root 的外层 child 需先受其选定的最多 8 CPU affinity 约束，入口只读核验并拒绝更宽范围；
采样超 RSS／SHM 限额停止 owned scope。历史 echo 未提供该字段时仍用原 8 GiB RSS／2 GiB SHM，
不修改共享 cgroup。host RAM inventory 不能替代本单元额度或容器限制。

首个真实模型单元待 root：原 Movie 前 15 行／2 eligible，两查询各 `max_model_calls=2`，
总永久预算 4，失败／unknown 不退、不自动重跑、不暖身；实际模型全部由 root 调度。
每次 owner 关闭后独立检查 GPU，再允许下一个查询；原 `0190` CPU 归档和旧模型账本保持原身份。

## 接入方式

[适配头文件](record_dbend_adapter.hpp)提供 `Session.begin_query`、`submit`、`ready_blocks`、
`collect`、`end_query`。每个 physical block（传输块）可含多行记录；一次 `begin_query` 内可以
连续提交两个块再收集，收集完成一个后即可补下一个。只在本次算法查询结束时调用 `end_query`。
这些调用直接使用作者 `ExecuteAsyncFuncPush`，任务体调用 `ExecuteFunction`，未新增线程池或调度器。

[可执行桥](imlane_records.cpp)将轻量 JSON 命令编码为上述 Arrow Table。
启动参数是 `imlane_records PORT UDF_PATH FUNCTION`；只连接 `127.0.0.1`。
[Python 客户端](client.py)接收现有 supervisor 启动的二进制进程和 `abort_owner` 回调，串行处理控制命令。
它不启动 Coordinator 或模型。上层可以直接使用 C++ 接口，也可以用该客户端，无需经过旧 HTTP 执行器。

```python
bridge = RecordsBridge(process, abort_owner=stop_and_verify_owned_processes)
bridge.begin(query_id)
bridge.submit("block-a", first_records)
bridge.submit("block-b", second_records)
for block_id in bridge.wait_ready(timeout_s=remaining_query_seconds):
    values = bridge.collect(block_id)
# 收集所有块、向原算法交付结果后：
bridge.end()
bridge.close()
```

输入列固定为 `call_id:string, row_id:int64, query_id:string, stage_id:string, record_json:string`。
每个 `record_json` 为非空对象，仅含字符串列值；图片只传本地路径，宿主不读取或展开图片。
当前 echo 仅接受 `text` 列。未来方法 UDF 必须按固定部署检查自己的合法输入列，评价标签不进入记录。
输出列固定为 `call_id:string, row_id:int64, outcome_json:string`。
输出顺序与输入顺序一致；乱序、缺行、重复或错配均拒绝，不按行数猜测关联关系。
允许同一 `row_id` 重复，`call_id` 在整个查询的全部块内必须唯一，查询结束后才清除该登记。
`stage_id` 每块一致；跨块可以转换算法阶段，发生项登记仍保留。

默认 v1 的 `outcome_json` 固定九项：`query_id, stage_id, status, result, error_type, error,
model_entry_attempted, completion_known, worker`。成功为 `status=ok`、对象型 `result`、两个错误字段为空、
`completion_known=true`；`result._occurrence_id/_position` 必须匹配该行。错误为 `status=error`、
`result=null`、非空错误类型及信息。两个完成性字段必须是布尔值，`worker.worker_pid` 必须为正整数。
任何 worker 错误都停止该 owner 的复用，不将错误解释为语义 False。

独立头文件默认每块最多16行、2个 pending block（已提交未收集块）、每查询4096行、每会话64个查询；
输入／输出 Arrow 缓冲区分别最多64 KiB／1 MiB。二进制使用同样默认值；输入命令最多128 KiB，
客户端响应帧最多8 MiB。已收集块不占 pending 名额；发生项／块名登记保持到查询结束。
容量是宿主内存管理参数，不表示作者模型或 GPU 并发能力。

## 输出内存与错误资源责任

输出在 `TableConverter::ConvertFromExchange` 内、作者释放 Lane 之前，经过 Arrow IPC
（跨进程列式数据格式）序列化再读取，获得调用方独立持有的内存。复制前检查 schema、非空、发生项、
结果结构以及实际 Arrow 缓冲区字节数；复制后保留旧结果不依赖 Lane。
可执行桥额外保留最近一个已交付的 C++ Table 及其值快照，在后续结果返回时逐值重读核对；
最多保留一个块，便于实际检查 Lane 复用后的内存有效性。提交的输入 Table 在任务完成前保持不可变。

固定作者版本的 `HandleExecuteFunc` 在转换器抛出异常时会跳过共享内存对象删除与 `RestoreLane`。
本桥使用独立的 `ExchangeResult` 输出类型；转换器保存 `exception_ptr`，不将异常抛出该回调。
作者继续执行原有清理并发送 `RestoreLane`，future 完成后才重新抛出保存的错误。
这项工程决定保留所有正确性检查，本轮不需要新增作者清理补丁，不改变旧转换器。
实际库版本、来源摘要和采用位置由[检查记录](../../../experiments/results/diagnostics/imlane_records_echo_20261016/README.md)维护。

`future.task.get()` 只消费一次：先写入已消费状态，再 get 和检查；重复 ready/collect 返回明确错误。
future 成功仅说明作者回调返回且原 restore 发送路径已执行；作者 API 没有提供本桥可用的逐次归还确认。
本桥不把 future 结果当作资源全部归还的证明。后续同一 Lane 可再次完成任务和 owner 最终清理分别提供额外证据。
转换前后的作者异常、worker 错误或无法确认的提交都会使本桥终态，交给已有进程监督处理。

裸 Python 异常可能不 post 输出 semaphore（信号量），future 因而不就绪。
`wait_ready(timeout_s)` 给等待过程设置绝对期限，连同单次 poll 的响应时间一起计算；到期执行 `abort_owner`。
调用方还必须保留从查询开始计算的共同期限，不能在补块时无限刷新整个查询的期限。
`abort_owner` 必须终止并核验该桥、Coordinator、executor 与本 namespace 下的全部后代／共享内存；
回调失败时客户端保留可重试的关闭责任。无法完成原生任务时，强制清理与作者正常退出分别记录。

## 构建与本轮检查计划

以作者固定 `072f8257db80569e42c9f5727d839587df529652` 加现有 fixture 补丁的既有构建为前提，
保留 Python3.12／Arrow16.1 ABI（编译接口匹配），不使用查询 driver 的 Arrow24 来重建作者库。
目录、二进制和日志均在仓库外；不覆盖既有构建。

```bash
<author-python> code/integrations/imlane_records/build.py --root <checked-author-root> --output <new-bin>/imlane_records
<author-python> code/integrations/imlane_records/build.py --root <checked-author-root> --output <new-bin>/records_checks --checks
<new-bin>/records_checks
<author-python> code/integrations/imlane_records/verify.py --author-root <checked-author-root> \
  --bridge-binary <new-bin>/imlane_records --output <new-run> --artifact-root <existing-host-artifact-root>
```

验证器使用本目录的[owner helper](owner.py)，选择性提取 `e9a54e3e` 已有 `AuthorScope` 的
进程身份、namespace、停止与资源核验；Linux start-ticks 读取复用项目已有 procfs helper。
它不导入旧执行器、HTTP bridge 或旧验证树。host lease（宿主互斥使用记录）仍使用现有
`runner_lease` API，但显式设置 `recover_stale=False`；发现已有记录时保留原件并拒绝接管。
全部 scope 的已知进程、namespace SHM 和日志／采样线程均确认结清后才显式释放本次 lease。
PID 出生身份或状态无法核验时保留 `unknown`，不将读失败当成退出，不删除该 scope 的 SHM，
也不标记 closed；最终报告记 `host_lease.status=held_unresolved` 并保留 lease 原件以便 owner 重试。
创建进程后首次出生身份读取失败也保留该 root 登记，不能因 known 集合仍空而宣称结清。
查询本身失败而资源已确认结清时仍可释放自己的 lease；其他 owner 替换的 lease 不会被删除。
桥的 stderr 经管道交给脱敏 writer，`redact_text` 后才写入文件，writer 也纳入线程关闭核验。
每次只启动一个作者 CPython executor；无模型、无HTTP服务、GPU可见范围为空，不启动 Ray。
有限预算：7个独立scope、8个CPU、8 GiB RSS（进程常驻内存）、2 GiB共享内存；
外层命令期限180秒，TERM后最多20秒强制退出；单次native等待3秒、控制请求5秒。
正常用例有两块在途、跨块重复callID拒绝、重复来源、跨查询同名发生项隔离、同worker复用和旧输出保留；
故障用例包括乱序、缺失、错误结构、显式worker错误、过大输出和裸Python异常。
任一意外失败或资源核验失败停止余下scope，保留日志，不降低检查条件。
首轮7项通过后，独审补充的C++ Table保留与raw异常日志核对只重跑 `--case infer_records --case raw_exception`，
另使用新输出目录，累计最多9个scope；不重复已通过且未受影响的五项输出故障检查。
文档核对再发现首两轮的相同来源位置带有不同的文本后缀；为覆盖完全相同内容的不同发生项，
只增加 `--case infer_records` 一轮，累计最多10个scope，沿用全部原检查和相同资源上限。

上述历史 v1 echo 阶段不做模型加载、GPU装配或LOTUS方法安装。后续作者Ray需要显式 `num_gpus` 和部署环境传播；
每解释器固定一个模型角色，alias不隔离模块状态；模型close成功后才能标为closed；
`ray.kill`、atexit或future错误均不能单独证明模型后代已退出。这些待主链后续独立验证。

## 主线集成检查

本节保留首次主线集成的 CPU 记录；后续新 helper／v2 二进制的作者实测单列于[新报告](../../../experiments/results/diagnostics/imlane_filter_author_cpu_20261019/README.md)，不回填旧资格。

以下命令只运行本地 Python 控制 fixture、Arrow echo 与 owner 身份替身，不启动作者组件：

```bash
<project-python> -B -m unittest discover -s code/tests/integrations -p test_imlane_records.py -v
<project-python> -B code/integrations/imlane_records/verify.py --help
```

原 9 项行为检查加 13 项集成检查在 Python3.12.6／Arrow24 下通过；其中 4 项 echo 检查需要
Arrow，其余不依赖作者库。新增检查覆盖无旧树独立导入、不可得 start-ticks、PID 复用拒绝、
外来 SHM 名称、未知身份保留 SHM／lease 与关闭重试、lease 的确认后释放、stale／foreign lease
拒绝及 stderr 落盘前脱敏。第一轮 15 项的原始记录另行保留。此轮未重编译 C++、未进入作者环境，也未重跑
原 10 个作者 scope；原资格摘要与新增集成 patch 摘要分别保存在
[qualification.json](../../../experiments/results/diagnostics/imlane_records_echo_20261016/qualification.json)。
完整 CLI 可由已有 `lotus_records_supervisor.supervise` 在外层作有限进程监督；本桥的 Linux
namespace／SHM 核验仍由 owner helper 负责。这不是本地模型或 GPU 生命周期资格。

## 普通 Filter 方法接入交接

以下交接记录对应集成基点 `c0be45dd`；后续本职责分支的实现与资格见[普通 Filter v2](#普通-filter-v2cpu-资格作者实机待验)。
交接要求先复用现有 `FilterMethod`／`LotusFilterMethod`、`GenerationSpec` 和隔离的 `EngineSpec`，
再接原 parser、调用账本与 SemBench evaluator。主链的字符串 `row_id` 需要到本桥 `int64`
的可逆映射，同时保留原 `row_id`、`source_position`、`call_index` 与完整 `call_id/query_id/stage_id`；
不把重复来源合并，也不将评价标签放入 `record_json`。五列 Arrow 传输和作者 DBEnd 的调度归属保持。

模型准备／执行的解释器 owner 必须先通过无模型 fixture 验证：作者 CPython／Arrow16.1 ABI
不能与查询 driver 的 Arrow24 混装；模型环境保持无 LOTUS，每解释器固定一个模型角色。
逐行模型终态需保留原 messages 摘要、实际 token IDs、usage、finish reason、engine UID 和
已知／未知提交状态。当前 `collect_ready` 遇错误拒绝整块、错误 `result=null`，还不能保留块内
已经完成的模型调用；正式接入前须增加明确版本的结果／错误交接，先记下已知 usage，再交付错误，
未确认项保持 unknown。块数、Arrow 字节限额不能替代应用请求预算。

完成身份、部分结果、取消和关闭的 CPU 检查后，先用新 owner helper 重跑作者 CPython 无模型
scope，再独立准备双查询模型资格；保留原 10 个 scope 的历史来源。作者 Ray 另核 `num_gpus=1`、
环境传播及自身模型 owner，不能借用其他路径的 Ray pool 或 GPU 退出结论。模型进程的 PID 出生身份、
全部后代、namespace／SHM 与独立 GPU 清点均结清后才能关闭 owner／释放自己的 lease。
本轮只完成交接，没有进入作者环境、增加模型单元或修改桥协议。

<a id="普通-filter-v2cpu-资格作者实机待验"></a>

## 普通 Filter v2：CPU 与作者无模型资格

职责分支 `feat/imlane-local-filter` 从 `integration/data-execution@c0be45dd` 建立。
当前实现接入原普通 Movie Q3 方法，未修改公共 CLI、公共 records 合同、共享 engine、
作者 Coordinator／Lane／执行器或旧 HTTP 配置。[本机 CPU 快照](../../../experiments/results/diagnostics/imlane_filter_cpu_20261019/README.md)
与[作者 CPython 实测](../../../experiments/results/diagnostics/imlane_filter_author_cpu_20261019/README.md)单列，原 echo 的 10 个作者 scope 保持原源码及运行身份。

| 计划与实施要求 | 当前落点 | 验收 |
|---|---|---|
| [架构 §3.2](../../../experiments/plans/系统架构.md#lotus-data-execution)：后端拥有派发，原方法拥有格式和解析 | [Filter UDF](filter_udf.py) 调用既有 `LotusFilterMethod`，宿主将原始 engine rows 交给既有 `run_movie_query` | 原 formatter、parser、usage、COUNT 与原 SemBench evaluator 的 CPU 接线 |
| [Filter 计划](../../../experiments/plans/数据组织.md#lotus-filter-data-execution)：逐发生项关联、不传标签 | [可逆映射](filter_workload.py)，wire `row_id` 为有界发生项序号；原字符串身份置于轻量 JSON 元字段 | 重复来源不同 call ID、Unicode／空值样式文本、错配／缺失／重复／乱序与跨查询拒绝 |
| 同计划：坏后缀不能抹掉已发送前缀，失败及未知项保留 | [宿主结果迭代器](filter_host.py) 最多两块 pending，发现失败后停止供给、有限排空已接纳块；逐项交账本后由原 consumer 报错 | 块内成功→带 usage 的失败→成功、跨块已接纳终态、坏后缀、unknown 与不补零 |
| [模型所有权](../../../experiments/plans/系统架构.md#model-engine-execution)：跨环境只传值，角色固定，失败 owner 隔离 | 选择性复用现有 `daft_engine_process.EngineProcess`；[IMLane 启动装配](model_process.py) 清除作者 ABI 环境，显式保留 owner 分配的单 GPU；解释器共享一个固定配置槽 | alias 不能另开模型角色；关闭失败不先标 closed；构造失败不冒称从未创建模型进程；CPU 子进程与后代退出 |
| README 原交接：先 CPU，再作者 CPython 无模型，再双查询模型；作者 Ray 单列 | [有限资格入口](verify_filter.py)，复用已有 scope／lease；固定输入、每查询永久预留及输出目录禁止覆盖 | 本机 CPU、新 C++、隔离 Arrow16 方法副本及作者 CPython 无模型往返已通过；真实模型及作者 Ray 待验 |

五列输入和三列输出不变。默认 `imlane.records.v1` 继续严格拒绝 worker error 整块；
只有启动二进制时显式追加 `imlane.records.v2`，client 也选择同版本，才交付 v2。
v2 在原九字段 outcome 上增加 `protocol`，失败 `result` 仍携带 `kind=filter` 与完整
`engine_result`，或 `kind=close` 与关闭报告。模型 `succeeded/failed/unknown`、提交证据、
原 messages SHA256、token IDs、usage、finish reason、engine UID 原值透传；桥结构或身份损坏
仍使 owner 终态并交监督清理。v2 的查询 wire 容量为 4096 个模型发生项加一个显式关闭记录，
关闭记录不占模型预算；输入、输出及命令的原字节上限保持。

输出仍在 `ConvertFromExchange` 内校验、经 Arrow IPC 独立复制，并在返回作者清理路径后才收集。
无模型 echo 的异常安全策略未改变：converter 捕获错误而不向作者抛出，保留资源责任；
本轮未新增作者资源清理补丁，也未删除旧校验。新 C++ 检查已在作者构建环境编译并执行，v2 已知失败 usage、独立输出与默认 v1 协商门均通过。

独立方法副本保持作者 Python3.12／Arrow16.1，并包含公开 `imxx==0.0.1` 启动包及
Ray2.56.1 的基础依赖。作者 CPython 的 `Coordinator::Shutdown` 也会导入 Ray 并调用
`shutdown()`；本次只满足该关闭依赖，实测未初始化 Ray。两个缺依赖失败及资源结清原件保留。

每个查询 owner 持有一个私有 engine，在该查询多块间复用；关闭控制记录须在 pending 结清后
经同一作者 Lane 执行。两个查询使用独立 owner、engine 和取消目录；本版不声明跨查询同 engine
复用资格。这是首个普通 Filter 接入的工程范围选择，与计划允许的查询私有资源一致；
若后续研究跨查询模型复用，须另核实例、期限与总预算，不能借此版推导。
失败模型初始化留下未知句柄或关闭报告不确认时，继续保留 owner 责任，使用 namespace supervisor
收尾；API close、进程退出、SHM 与 GPU 清点分别记录。

资格入口只消费仓库外 `imlane.filter.run.v1` JSON 配置，至少包含：

```json
{
  "schema": "imlane.filter.run.v1",
  "mode": "no-model",
  "author_root": "/absolute/checked-author-root",
  "method_python": "/absolute/isolated-arrow16-method/bin/python",
  "bridge_binary": "/absolute/new-v2-build/imlane_records",
  "artifact_root": "/absolute/owned-artifacts",
  "source_manifest": "/absolute/movie-records/manifest.json",
  "sembench_checkout": "/absolute/fixed-SemBench",
  "preflight_report": "/absolute/current-preflight.json",
  "method": {"model_name": "openai/local-fixture"},
  "engine": {
    "model_path": "/absolute/local-model",
    "model_python": "/absolute/model-venv/bin/python",
    "driver_python": "/absolute/driver-venv/bin/python",
    "max_model_calls": 8
  },
  "generation": {},
  "resources": {"prepare_batch_rows": 4, "model_batch_rows": 4, "max_active_batches": 2},
  "query_ids": ["qualification-q1", "qualification-q2"],
  "max_total_calls": 16,
  "model_environment": {}
}
```

```bash
PYTHONPATH=code <driver-python> -m integrations.imlane_records.verify_filter \
  --config <private-config.json> --output <new-private-run-directory>
```

`no-model` 明确使用测试 UDF／CPU 子进程，GPU mask 为空，不加载权重。
`local-model` 由集成 owner 排期，在配置中显式提供其单 GPU mask，核原作者 Python3.12／Arrow16.1、
独立方法副本的固定 LOTUS 源码、无 LOTUS 的 vLLM0.25.1 模型环境及绑定源码的新桥二进制。
开始前持久化所有查询的永久预算，失败／unknown 不退款、不重发；本入口不能自动重启或复用旧输出。
清理后单独查询 GPU compute 进程和显存回落；观察失败或资源 unknown 保留本次 lease。
CPU 结果不认证该模型装配。首轮既有通道缺失的记录保留；恢复后的指定通道已完成作者无模型资格并交回窗口，真实模型／GPU 与作者 Ray 由集成 owner 后续独立排期。
