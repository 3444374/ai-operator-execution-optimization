# Production Core 的异步本地模型 actor：CPU 接线与固定模型结果

后续固定 `82e685b3` 的真实模型结果见[迟到供给与取消检查](#model-late-cancel-20261019)：
late通过，cancel因缺少对应原生abort意图记录保留失败；不把物理退出改作请求终态确认。

本包将已有生产 `SessionEngine` 接到已有有界异步私有模型进程，交付可注入的
`AsyncRecordsActorOwner`。最终源码固定为 `a66648d53934eddad0bbe60ed865246068f68ee2`，
依赖集成者的 `90fe33995ab72ed9e3911ae65c7b9ad6a414e944`（含 `8e7f921a` Core hooks）；
职责分支为 `work/async-records-wiring`。初始基线 `b447fc37` 已包含 `bc92f7ce` 关闭异常修复，未重复实现。

**结论限于 CPU：130 项测试及 40 个子测试通过，实际 Ray actor／CPU 子进程 IPC 另 1 项通过，
两名独立审查者最终 PASS。真实 AsyncLLM、GPU／后代退出、完整查询持续供给和性能仍待专项资格。**
此前同步 Core 的模型资格及独立 Async 组件资格均保留各自固定源码，不转授本包。

## 文档条款、实际调用链与缺口

依据[模型引擎实施路线](../../../plans/数据组织.md#model-engine-roadmap)、
[查询边界](../../../plans/数据组织.md#records-query-boundaries)、
[架构所有权](../../../plans/系统架构.md#model-engine-execution)和
[增量执行](../../../plans/增量执行.md)核对。历史“拟增”描述与后续固定来源按各自日期解释，
本包未发现需改变方向的规范冲突。

| 文档要求 | 基线实际实现与缺口 | 本包落点与验收 |
|---|---|---|
| 复用生产 Core，数据执行持有本地模型 | 通用 `SessionEngine`、`BoundedAsyncBackend`、任务容量与 lease 已有；records 的普通 Core 同步接点已在分支实现，static reference 是另一条独立路径 | 复用原 Core 与 transport，注入新 owner；组合测试断言真实 `SessionEngine`、原额度及引用归零 |
| 请求未结束时继续接纳迟到输入 | 同步 `LLM.chat` 等整批返回；独立 Async 组件已有，actor／Core 尚未消费 | 第一调用保持 active 时第二、第三调用实际经 IPC 完成；不复制调度框架 |
| 原准备、结果和发生项身份不变 | 原 formatter／parser、7 字段身份及 `CallLedger` 已有 | prepare ObjectRef 直接传 actor；每次以完整 `(session_id, sequence)` 区分 Core 工作，同 query／call 身份可跨 session 重用 |
| 输入、结果、控制容量分别有界 | 原 Core 额度不覆盖全部对象和 driver 结果；Async IPC 已有限额 | actor 再保留至 ack；count 与预留 bytes 双上限；command／input 暂满仅等待未接纳交接，不重发 accepted 调用 |
| 满负载仍能取消／关闭 | 单线程同步 actor 会让控制方法排队 | 线程 actor 并发为 `max_inflight + 2`，为 abort／close 留余量；生产资源仍预留 1 CPU／1 GPU，禁止自动任务重试／actor 重启 |
| 取消不等于模型终态 | 原 Core cancel 只改变本地任务状态，组件已有真实 abort 协议 | 保留取消意图直至能入控制队列，接收实际迟到终态；unknown 不释放 actor／IPC 额度，不伪造 usage |
| 连续查询隔离 | transport 的确认 RPC 可迟于 Core known terminal；query_id 不是生命周期 key | 新 session 在期限内等待旧 ack；独立取消 epoch 清旧提前取消记录；旧 key 不能取消新调用 |
| owner 结清且报告真实范围 | 原 owner hook 和进程关闭异常修复已有 | 复用同一启动失败句柄／关闭 RPC；异步收尾窗口 15 秒，查询总期限仍取 min；只确认 API／直接子进程／IPC，GPU 与后代字段为 null |
| 静态及原生参照保持身份 | static reference 不经过生产 Core，原生系统有自己的调度 | 回归同步 owner／Core／static；本包未修改原生后端、默认 selector、方法或 parser |
| 全查询边界与原算法屏障 | driver 仍累计 raw，全部 Filter 终态后解析／COUNT；lease 先于 runner 累计释放 | 不把逐请求返回解释成后继算子已推进；级联全局校准及 Extract 关系屏障保持原合同 |

实际调用链为：`SemLoomRecordsCore.evaluate → _CoreRows → SessionEngine → BoundedAsyncBackend →
_RecordTransport._execute → AsyncRecordsActorOwner.submit_core → Ray AsyncCoreModelActor.run_core →
AsyncEngineProcess → 私有子进程`。原 prepare worker 输出经 ObjectRef 交接，driver 不读取准备结果。
子进程返回完整终态，actor 校验并保留；transport 收到且核验后发 ack，原 Core 负责完成与 lease 释放。
这是本地数据执行接点，无 HTTP gateway。CPU 子进程使用明确的固定响应 fixture。

## 最小集成接口与文件归属

本包只增加[actor／owner](../../../../code/src/execution_provider/adapters/lotus_records_async_actor.py)与
[actor 合同测试](../../../../code/tests/execution_provider/test_lotus_records_async_actor.py)、
[生产 Core 组合测试](../../../../code/tests/execution_provider/test_lotus_records_async_core.py)、
[实际 Ray CPU 测试](../../../../code/tests/execution_provider/test_lotus_records_async_actor_libraries.py)。
共享 `records.py`、`vllm_engine.py`、`lotus_records_movie.py` 和其他后端均未并发编辑。

```python
actors = AsyncRecordsActorOwner(method, engine, generation, resources, limits=limits)
owner = SemLoomRecordsCore(method, engine, generation, resources, actor_owner=actors)
```

prepare／model batch 必须为 1；`limits.max_inflight` 必须匹配 Core 的 `max_active_batches`，
result slots 至少覆盖该并发额度，实际 actor 留存还受 result bytes 限制。
父 actor 使用 driver Python，私有子进程使用 model Python。`submit_core`／`cancel_core`／
`acknowledge_core` 使用同一完整 Core key；元数据为 `independent_final_results`，只消费完整结果。
15 秒 hook 依赖 `90fe3399`，不改变同步 owner 的 10 秒 RPC／11 秒响应窗口。
原查询 close 总期限可缩短等待，此时返回 unknown 并保留原句柄供续清理。

公开 selector／CLI 由集成者的 local-engine worker 接入，本包未新增公开开关；
静态参照及原生 baseline 维持原配置。没有新增 HSE 策略、跨查询并发、图像或多卡能力。

## 冻结验证与原始输出

使用已有 Python 3.12.6 环境，Ray 2.56.1、Arrow 24.0.0、pytest 9.1.1；无安装、模型推理或服务器访问。
各次源码文件在测试前后逐字摘要一致，JSON 保存源码 SHA、命令、版本及日志压缩前后 SHA。
日志为落盘前脱敏与路径归一化后的原始 stdout，无损 gzip 保存；pytest 秒数只表示测试耗时。

| 最终运行 | 结果与证据 |
|---|---|
| Core／actor／IPC 及同步回归 | [cpu_contracts_20261019T093251Z.json](cpu_contracts_20261019T093251Z.json)：130 passed、40 subtests，13.89 秒；[原输出](raw/cpu_contracts_20261019T093251Z.txt.gz) |
| 实际 Ray CPU actor／IPC | [real_ray_cpu_20261019T093254Z.json](real_ray_cpu_20261019T093254Z.json)：1 passed，8.13 秒；[原输出](raw/real_ray_cpu_20261019T093254Z.txt.gz) |

第一组使用真实生产 Core、真实子进程和 sockets；Ray／Daft／formatter 是明确的组合 fixture。
第二组使用实际本地 Ray 线程 actor，逻辑 GPU 明确为 0，覆盖迟到完成、旧取消隔离、正常关闭及
仅杀自有 actor 后 CPU child 因 EOF 退出；实际 Ray 和生产 Core 的新组合并未在同一项测试运行。
该测试不认证生产 GPU 预留、原 LOTUS 方法差分或 vLLM。

核心回归可从仓库根使用已有环境运行：

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code python -m pytest -q -p no:cacheprovider code/tests/execution_provider/test_lotus_records_async_actor.py code/tests/execution_provider/test_lotus_records_async_core.py code/tests/execution_provider/test_lotus_records_actor_owner.py code/tests/execution_provider/test_lotus_records_core.py code/tests/execution_provider/test_lotus_records_static.py code/tests/scheduling/test_async_backend.py code/tests/serving/test_async_engine_process.py code/tests/serving/test_async_engine_close_faults.py code/tests/serving/test_vllm_async_engine.py
RUN_ASYNC_RECORDS_CPU_LIBRARIES=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code python -m pytest -q -p no:cacheprovider code/tests/execution_provider/test_lotus_records_async_actor_libraries.py
```

保留的前序冻结运行是修复过程证据，不与最终结果合并为重复样本：
[a69 CPU 127／40](cpu_contracts_20261019T092333Z.json)、[a69 Ray 1](real_ray_cpu_20261019T092357Z.json)、
[81 CPU 129／40](cpu_contracts_20261019T092947Z.json)、[81 Ray 1](real_ray_cpu_20261019T092951Z.json)。
其未推送旧提交因依赖升级而原样重放；[历史源码恢复映射](historical_sources.json)将每个源码摘要
映射到最终分支可达的 Git 文件，不要求取得未发布旧 SHA。
更早小范围开发运行仅保留工具输出和评审记录，未伪造独立原始日志。

## 独立审查、修复与交接

[reviews.json](reviews.json)保存两名独立审查者的来源、固定 SHA、发现、复核及测试范围。
初审发现暂时背压被误报失败、控制队列满后丢取消意图、unknown 后忙轮询；复审发现异步 ack
跨 session 竞态与旧提前取消挤占新 session。以上均补合同测试修复。
两名审查者在 `81feb546` 确认无剩余 P1／P2，之后对最终 `a66648d5` 的 15 秒 hook 窄复核均 PASS；
各自独立运行 38 项 CPU 测试，Standards 另有 14 个子测试。两人均未改文件或运行 GPU。

集成依赖和本包四个代码提交分别为：
`90fe3399` → `d5019b98`（actor／测试）→ `a9ca93c9`（背压／取消／忙轮询）→
`44228bcd`（session／ack）→ `a66648d5`（关闭窗口）。证据单独提交，交集成者选择性整合。
按独占归属，集成者串行同步 `code/INFRA_STATUS.md`、adapters／diagnostics README、
`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md` 与 `PROJECT_LOG.md`；本包不覆盖这些共享入口。

未采集项：真实 AsyncLLM 结果／logprobs／模型质量、GPU 与全部后代物理退出、全查询 JCT／吞吐／RSS／KV 峰值。
原因是本单元仅获配 CPU fixture，GPU 由专用会话排程。没有从测试耗时、准备并发或固定响应推导性能收益；
后续资格还须按原方法、模型与完整查询预算核对公共入口和静态／原生参照。


## Root 集成、公开入口与关闭回执

职责包以 `70e93911` 原证据接纳；后续集成单列于 [root_integration_cpu.json](root_integration_cpu.json)。
显式 `semloom-local-model-core-async` 现构造同一 `AsyncRecordsActorOwner` 并注入原 Core，
普通同步默认保持。配置不传 `async_limits` 时由 owner 按 Core 窗口生成；显式配置必须匹配窗口，
报告保存实际有效值。CLI 的 13 项 factory／配置检查与旧 Movie 查询回归分列于共享组合检查，
没有把工厂替身当成真实 AsyncLLM 查询。

独立五个实际 CPU scope 把真实 Core／Ray／Daft／LOTUS 准备和三通道 CPU child 接在一起，
覆盖迟到供给、逆序完成、慢消费、取消／unknown、API 关闭失败和实际 owned actor 死亡；
每 scope 15 个 observed owned 身份退出，共 75 个不重复身份。物理引擎为明确 CPU 固定响应。
这些 scope 绑定原 `a66648d5`，不冒充后续补丁全矩阵重测。

第六个实际 return-gate 反例发现：actor 已生成 known 11／1／12 usage，
在 Ray 返回交付前 close，旧查询报告漏掉该回执。原普通 RED 保留；`0e515ba6` 让 actor
携完整 `(session_id, sequence)` 返回有限回执，transport 只按已登记 expected identity／bytes
校验补账，避免按 query_id 猜归属。六项纯反例与旧 Core／actor 共 56 项／10 subtests 通过；
另核 keyed close 回执和 effective limits。相同实际 RED 用例修后 GREEN：known usage 原样入
`last_query_report`，原 InterruptedError、query／overall unknown、held／ref 各 1 保持。
15 个 observed owned 身份退出、外层无清理信号，不把已知 usage 当作 Core 终态或 GPU 退出。

固定 `0e515ba6` 的无 Popen 构造探针另核四组有效默认／拒绝／模式／关窗：
Core cleanup20、actor RPC15／响应16、IPC15；不构造引擎、socket 或 Ray runtime。
源 SHA 前后核同，实际 GPU、后代退出和性能仍未验证。


### 原 parser／COUNT／SemBench 的实际 CPU 闭环

测试 `553e8971` 选择性接纳为 `25ff5169`，只增加 fixture 的默认 False 开关与一个新用例，
旧六项用例方法 AST 保持，未重跑。生产源固定 `0e515ba6` 的 13 个文件；测试在运行后提交，
其字节 SHA 与机器报告相同，不以运行时 HEAD 代替完整测试来源。

真实 Core／Ray／Daft／LOTUS 准备及 Async actor／三通道 CPU child 后，
`run_movie_query` 实际调用原 LOTUS parser、LM stats、COUNT 和固定 SemBench evaluator。
两次人工固定 True 响应得到 COUNT2，ledger 与 LM physical／virtual 均为22／2／24 tokens，
原评价误差0、2TP。Core额度及refs归零，API closed；外层17个 observed owned身份退出，无清理信号。
[同一机器摘要](root_integration_cpu.json)保存四原件SHA、13生产源与测试SHA，
模型调用0、GPU仍None；不把这个软件输入或固定token计数当模型质量、模板或性能资格。

<a id="model-late-cancel-20261019"></a>
## 固定源码的迟到供给与取消模型检查

`async-core-82e685b3-20261019-r1` 在模型创建前因helper无法JSON序列化真实
`BatchEncoding`失败：模型调用0、新账本未创建。原失败保留；helper仅取原`input_ids`平面
非负整数列表做长度／摘要，tokenizer调用、模板、生成配置及411项生产源码不变。
20项真实Transformers对象回归和双独审通过后，执行新的r2运行，不重跑已成功的其他模型单元。

`async-core-82e685b3-20261019-r2` 于2026-10-19 12:11:49.475463–12:14:50.843634 UTC运行，
耗时181.368171秒、控制器退出1。Qwen2.5-7B-Instruct／vLLM0.25.1／单4090，
两个独立永久unit为late 3次、cancel 1次；同一总额4已全部领取，无退款、转移或自动重试。
r2由私有资格脚本读取公开Async selector配置并调用生产`_model_worker`工厂路径；没有执行顶层CLI进程，命令行入口的模型覆盖另验。

| 单元 | 实际请求及终态 | 已知输入／输出 tokens | 判定 |
|---|---|---:|---|
| late | 3条succeeded | 2990／12 | passed |
| cancel | 1条unknown，原查询InterruptedError | 2402／1 | failed |

late的第三条输入及原生接纳发生在第一请求的原生未完成区间内；对应原`AsyncLLM.add_request`
已返回和`generate`最终输出记录，只证明前端接纳时序，不证明GPU硬件重叠或性能收益。
cancel已原生接纳并记录取消意图，随后收到`finish_reason=abort`与production unknown；
缺少所要求的`native_abort_requested`，原失败`matching native abort intent is missing`保留。
该观察事件只由`WitnessEngine.abort`记录，直接shutdown不经过该观察点；现有证据无法确定
实际经过哪条cancel／close路径，不能据此断言原生abort从未发生或取消资格通过。

known usage全部进入原query账本；close后Core仍持有一个任务、一个输入引用及对应容量。
原生模型API关闭字面true、actor engine关闭字面false分列，逻辑unknown不因物理退出改变。
独立确认73个已观察出生身份退出、GPU1MiB且无计算进程、共享内存空、RPC活跃连接0；
六个非活动目录项保留，自有正常lease释放，监督器无清理信号或错误。八旧账本与作者源码／二进制保持。

配置签名、必要原始时序、调用／usage及原件校验值见[紧凑回执](model_qualification.json)。
完整126成员／10,658,407字节原件含r1失败，留在仓库外，SHA256为
`e7a9f8b291852b28d1a3435cab4cfe6dc5c7cbb3d880766974d92aab5651e5d8`；
向实验维护者提供运行ID和该摘要可获取原归档及完整成员索引。取消后续只读诊断按独立修复处理，
不重复late或IMLane已通过单元；跨阶段持续供给、完整工作负载、PostgreSQL和性能仍未覆盖。
