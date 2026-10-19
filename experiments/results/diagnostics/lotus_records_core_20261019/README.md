# Movie Q3 records 的 production Core 接入

本职责分支从 `integration/data-execution@c0be45dd` 独立建设普通 Movie Q3
records→本地模型→原 LOTUS 解释的 Core adapter。只验证接入和资源责任，
不改语义方法、调度算法或其他后端，不运行服务器 GPU。

## 文档要求、现有实现与验收映射

以下映射先于实现建立；来源是本提交基线的文档与源码。

| 文档条款 | 已有实现 | 缺口与本切片验收 |
|---|---|---|
| [架构 §3.2](../../../plans/系统架构.md#lotus-data-execution)：Daft 扫描／普通关系处理，SemLoom 以 Ray Core／Actor 执行；原方法保留 | `lotus_records_static.py` 扫描／过滤／投影后直连准备和模型 actor；`lotus_records_movie.py` 调原 parser、COUNT 与原 evaluator | 新 adapter 的任务必须实际进入 `SessionEngine`；固定响应与同生命周期静态参照核对 occurrence、原始结果及 usage；解析和评价继续由公共 runner 拥有 |
| [模型实例归属](../../../plans/系统架构.md#model-engine-execution)：实例跨批，输入保留至终态，取消不等于模型结束 | 静态 actor 使用隔离解释器、1 CPU／1 GPU、TP1／非 Ray 内部 executor；关闭保留 unknown 和重试句柄 | 复用已有模型内核与 owner；Core 保留 unknown 的输入／结果／请求责任；取消、迟到结果、失败关闭与重试、两次查询实例复用分别测试 |
| [数据组织的实施路线](../../../plans/数据组织.md#model-engine-roadmap)：选择已有 Core 或显式引用接点，不强扩 HTTP bytes | `native_tasks.py` 接完整 HTTP 请求；新 records 路径绕过它；Core `OfferedTask.payload` 仍仅 bytes | 不修改公共 bytes 类型；原始记录规范编码进入 Core，准备 ObjectRef 直达模型，driver 仅接有尺寸上限的终态 bytes；测试无 HTTP、无 driver 获取 prepared payload |
| [增量操作与资源责任](../../../plans/增量执行.md#session-3-操作状态责任变化)：单控制线程、有限 offer、accepted-prefix、lease／release | `SessionEngine` 与 `BoundedAsyncBackend` 已实现；静态参照没有调用它们 | 复用其任务推进、容量与结果释放；Ray submit／结果等待留在异步传输线程。测试慢提交不堵控制线程、慢消费限制留存、释放后的继续输入及非法后缀 |
| [增量后端](../../../plans/增量执行.md#session-4-后端与进展机制)：有限非阻塞操作、取消只提请求、未知工作不返还容量 | 现有异步 backend 将未分类错误报为 `Uncertain` 并保留占用 | adapter 不取消等待后伪造终态；错误／取消／总期限覆盖准备、提交与清理，保留成功前缀；unknown 禁止复用 owner |
| [工程参照 §8.7–8.8](../../../plans/系统架构.md#company-engineering-reference)：方法与执行分开、昂贵实例复用、查询状态独立 | 已有 records 切片记录了固定公开 LOTUS／Ray／vLLM 与 pgml 取舍 | 保留既有方法和配置；只复用自有源码，不读取或复制公司代码，不安装 pgml；身份、空输入与连续查询不串状态 |

文档中“下一单元／待实现”按其写作日期解释；`c0be45dd` 源码及
[既有分层报告](../lotus_records_local_model_20261016/README.md)已包含普通 Q3 的静态／原生小切片。
这属于计划与实施状态的时间差，不把已有静态执行解释为 production Core。
架构提到 `AsyncLLM` 的持续供给属于后续资格；本切片沿已有同步 `LLM.chat`，不据此声明跨调用持续供给。

## 范围与待执行检查

首版单 GPU、单查询顺序复用，显式要求准备批和模型批均为一行；`max_active_batches`
表示 Core 中最多在途的单行任务，模型内部批与 GPU 利用率另作观察。
工作量以每行一次模型调用计数，不声称 token／GPU 成本校准。
输入、终态结果、元数据和每步动作均使用有限正值；逻辑 bytes 不称为 RSS 或显存上限。

## 实现与实际检查

新增 [Core adapter](../../../../code/src/execution_provider/adapters/lotus_records_core.py)
及[专属测试](../../../../code/tests/execution_provider/test_lotus_records_core.py)。
它复用 `SessionEngine`、`SessionPolicies`、既有 admission／endpoint router 和
`BoundedAsyncBackend`，没有新调度器或 HTTP gateway。`_ActorOwner` 只选择性复用静态参照的
actor 内核、资源分配和关闭，不调用其 `evaluate()` 或 `_StaticRows`；旧静态文件保持原样。
模型 worker 在返回前编码并检查终态 bytes 上限。结果预留在分配模型前须能容纳完整 call 身份与最宽有限 token 计数的失败收据；
完整结果超限时只返回有界失败收据，保留已知 usage、确认状态与原 call 身份，省略结果正文等大字段。
已确认的超限完成使查询失败并回收计算；unknown 即使超限仍保留 Core 占用与输入引用，不允许下一查询。
坏身份也不会被改写成已确认成功。

成功结果经 Core Delivery／lease 交给 adapter，解码为独立对象后在 `finally` 中 release。
失败 tick 的紧凑调用收据另行保留，防止 Core 丢弃未交付结果时丢掉已知 usage；
此类收据是失败证据，不冒称 Core 成功 Delivery。早停后迟到的已知结果进入
`last_query_report.unconsumed_call_results`，旧查询记录在下次查询前结清。
集成 runner 需要在将缺项标 unknown 前合并这些已知记录；该公共文件由集成 owner 修改。

[verification.json](verification.json)固定源码 SHA、测试入口与脱敏日志 SHA。

| 运行身份 | 实际结果与来源 |
|---|---|
| `system-python-import-r1` | 系统 Python 缺 `pyarrow`，首次模块导入失败，没有执行用例或调用模型；工具输出 `5711d3` 保留 |
| `core-fixture-r1` | 已有隔离 CPU 环境，20 项新 adapter 用例通过；工具输出 `a43adc` |
| `core-fixture-r2` | 补逆序完成／跨线程拒绝／早停迟到收据后，23 项通过；工具输出 `bf1755` |
| `fixture-regression-r1` | 新 adapter、旧静态、Core、异步 backend、旧 native task 共 101 项及 32 subtests 通过，零 skip；0.70 秒仅是测试墙钟 |
| `fixture-regression-r2` | 修复两轴审查发现的超限 usage 和 EOF 取消，26 项专属与旧路径共 104 项／32 subtests 全通过、零 skip；0.61 秒仅是测试墙钟 |

上述使用真实 production Core 与已有异步传输；Ray／Daft／LOTUS／模型均为明确 CPU fixture。
没有安装依赖，使用已有独立 Python 3.12.6 环境。实际 Ray／Daft 图、原 LOTUS 方法及原 SemBench
评价器在新 adapter 组合上的资格仍 pending；不继承旧静态或原生路径的模型结果。
本任务实际模型调用、GPU 分配、服务器命令均为 0。
两轮完整回归的脱敏原始 stdout 已公开保留于 [r1](raw/fixture-regression-r1.log) 与
[r2](raw/fixture-regression-r2.log)，可从仓库同目录取得并按 verification SHA 核验。
初始导入失败及前两轮单独 fixture 只在上述对话工具输出中捕获，未创建独立日志文件；该部分没有可下载原件，不虚构日志 SHA。

## 接口、清理与交接

`SemLoomRecordsCore(method, engine, generation, resources, *, capacity=...)`
提供 `evaluate(source, *, query_id, cancelled=None)` 与 `close(*, timeout=None)`。
`CoreRecordsCapacity` 默认每行 input／result 各 2 MiB、metadata 2048 bytes、每步 16 动作及
10 ms 正轮询间隔；总逻辑存储由 `max_active_batches` 乘单项上限计算。
完整原 call 身份保留在记录与结果中，Core 的有限 TaskInfo 以 call SHA-256 关联，不截断原身份。

首个关闭沿用查询绝对期限；显式 `close(timeout=...)` 只授权新的清理尝试，不重发模型工作。
取消停止后续输入和派发，继续等待已发送同步调用的真正返回；没有假装支持引擎 native abort。
`production_core_connected` 表示已建立 Core，`core_usage` 与模型 API／GPU 退出分别报告；
`ray.kill`、model API shutdown 或 transport 线程退出都不构成 GPU 退出证明。
unknown 保留 owner 句柄与 Core 账本，进程／GPU 实际退出仍由外层监督独立核实。

## 独立审查

两位只读 reviewer 分别从 Standards 与 Spec 审阅 `c0be45dd...378c7da7`，均核对实际计划和源码。
Standards 发现结果超限后的已知 usage 丢失及日志获取说明缺项；Spec 另独立复现 EOF 观察到取消后仍派发一次，
并以 8192 字符响应／1024 bytes 预留复现超限 usage 丢失。原通过测试没有覆盖这两个执行反例。
现增加模型前失败收据容量检查、worker 内有界收据、失败／unknown 分别结算，以及 EOF 后派发前取消／期限复查；
补充 3 项反例并公开两轮脱敏回归日志。修订提交的独立定向复核待记录。

公共 selector／报告接线、真实库／原方法组合和真实模型资格交集成 owner
选择性整合与排程；本分支没有修改公共 `records.py`、`vllm_engine.py` 或 `lotus_records_movie.py`。
级联、`AsyncLLM`、HSE、多查询、图像、多卡与性能评价均不在本切片。
