# 可选执行计时与零模型核验

本次只补实际可见的适配操作计时，默认关闭。基于 `58462a0d` 的独立职责分支，先合入已发布
`ea6c9184` 的公共 runtime／EOF 修补，再追加诊断；没有重写厂商执行图、请求并发、SDK、
Ray 每次 Dataset 执行的生命周期或模型参数。

<a id="ray-cleanup-parity"></a>

## 文本 Ray Data 默认与详细计时的清理修补

此接续修补从 `integration/baselines@da379b2577118d58b890cff3fca6400af7bda8f2` 单独建立
`fix/native-ray-cleanup-parity`。初始远端仍为该提交，附件所指 `native_calls.py` 的 blob
`821e46eec29520d41885e5759052ef179822c930` 与实际源码一致；附件转录与替身记录只作线索。
本次从仓库导入 `_ray_rows`，使用现有厂商 API 替身，不运行实际 Ray／Daft／IMLane。
发布前远端已推进到 `4db1aec25d5f9437c4fa9b7784395af955658520`；只读核对新增图像工作，
本题文本清理与公共计时源码未变。本分支仍基于指定提交，共同入口文档的新记录交集成owner保留双方内容。

执行先报 ValueError、close 再报 OSError、shutdown 报 RuntimeError 时，修前 OFF 返回 OSError
且未调用该层 shutdown；ON 保留 ValueError 并尝试两项，但只给首错附 OSError 说明。
公开 Iterable 的替身形态还复现了实际 iterator 的 close 未调用。
[固定 Ray 2.56.1 源码](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/data/iterator.py)
表明 `iter_rows()` 返回 `_IterableFromIterator`，其 `__iter__` 才创建行生成器。
因此适配层保存 `iter(result.iter_rows())`，计时 ON/OFF 都复用现有 helper，逐项尝试
iterator close 与已知 executor shutdown；未新增调度器、并发或行数包络设置。

规则为：进入适配层的业务／显式取消首错优先，全部清理次错留在异常 notes；没有业务首错时，
第一个真实清理错误可见。正常提前关闭的 GeneratorExit 不遮住真实清理失败；纯计时故障优先级更低，
writer 独立失败仍保留。清理中一次失败即使后续重复 shutdown 成功，查询报告仍保留
`unconfirmed_work` 并拒绝复用；远端 HTTP 停止仍为 unavailable。

独立子代理审阅新增两个反例：caller parser 首错原先只收到 worker close 类型，遗漏 shutdown 次错；
执行和 close 复用同一异常对象时，新增 notes 复制可能反复扩充自身列表。两项修前共四个子断言失败，
原输出保留后修正：向 caller 交接已有次错说明，复制前取 tuple 快照，避免复制自身 notes。
共享 helper 的 Daft detailed 提前关闭／首错行为也已核对，未修改厂商或 IMLane 实现。

[定向测试](../../../../code/tests/baselines/test_native_ray_cleanup_parity.py)覆盖执行／close／shutdown
组合、两种 iterable 形态、EOF、提前关闭、显式取消、iterator 打开失败、writer 起止故障、
次错交接、共享异常对象以及未确认责任／复用拒绝。
最终五模块58项：57通过、1项因本机缺Ray而跳过导入检查，HTTP／模型／GPU0。
生产源码 SHA-256 为 `ef7f7294a15e5463a8a8c6f89a9465e39f7f2c14cf80a7ba9952ed31955fa936`。
较早修订的共享计时补查42项中41项直接通过，一项六路径fixture因沙箱禁止localhost监听而报六个子错误；
仅重验该项后通过，六路径各ON/OFF两行，服务端断言合计24次localhost POST，模型／GPU0。
该较早修订与最终定向来源分列，不相加为最终源码资格，也未重复原性能矩阵。

[紧凑核验归档](ray-cleanup-parity.json.gz)保存实际函数修前／修后观测、各次源码摘要、
定向失败／回归、共享计时的首次监听失败与单项重验、独立审查和结果来源。
较早测试采样、入口误用等本地过程原件按运行身份与摘要保留于仓库外，不改写旧实验失败或旧性能原值。

最终实际 Ray 2.56.1 软件资格仍 pending。本机现有虚拟环境是 Ray 2.56.0，缺所需 Daft／aiohttp，
不为本次混装依赖或借其版本宣称实库通过。供统一排队的最小无模型计划是：复用已核验的
Ray 2.56.1／Arrow环境，部署独立固定源码包并取得共享lease；沿既有图参数，OFF/ON 各核对正常EOF与提前停止，
共四图、每图两行、最多8次localhost固定响应POST，4 CPU／0 GPU；单图60秒、含退出累计300秒。
在实际 iterator close 与 shutdown 后注入预定失败，核对首错／次错与未知责任；独立确认本次拥有的
consumer、HTTP handler、Ray进程退出再释放lease。超时、超POST上限、结果身份变化或未知清理即停止并保留证据。
此计划尚未执行，不授予模型、GPU或性能资格。

## 来源与问题

指定 `4f392c3652803c7786e56245c0bbc18a3e6e9bd6` 的
[Q2 主报告](../native_sembench_model_dual_supplement_20261012/README.md#retained-stage-analysis)
及其计时归档、复算器是既有证据。该提交的公开发送 helper／计数组件与本任务起点逐字相同。
模型 owner 另交接 192 次已有本地 initialize 回放、八查询 HTTP 分解及实际 helper 源码摘要；
本任务复用这些样本，不重跑模型或改写旧原值。

旧八查询中较慢的连续查询主要增加在首次 HTTP 前；进程 bootstrap 与查询区间的关系是观测线索，
不能证明对应 actor 已提前创建，也不能据此排除账本初始化成本。原生 runtime 预备通用 worker
保持，查询开始应早于本次图构建及专属对象创建；不为统一数字而把所有进程启动时间加进每个查询。

实际私有 `initialize`／`finish` 与本任务起点的对应函数 AST 相同，私有 `_history`／
`initialize_counts` 也相同。私有 `reserve`／`activate` 含不同窗口的预算及截止设置，不能直接用公开
helper 的整文件摘要替代。新增诊断按符号和文件分别登记来源；私有模型配置、请求正文、真实环境文件
及原 SQLite 文件不提交。

## 开关、字段与使用

在原执行命令前设置 `SEMLOOM_DETAILED_TIMING_DIR` 即显式启用；不设置时保留原基础总时、
结果及默认输出字段，不创建诊断 writer，也不新增采时。所有输出仍计入实际运行成本，没有估计扣除。

```sh
SEMLOOM_DETAILED_TIMING_DIR=/path/to/private/new-timing \
SEMLOOM_TIMING_RUN_ID=unique-run \
PYTHONPATH=code python code/scripts/baselines/run_native_adapter_query.py <原参数>
```

上例的脚本参数按[现有入口](../../../../code/scripts/README.md#native-adapter-query)填写。
同一开关适用于公共单查询、常驻查询、SemBench CLI 及图像 CLI，不授予模型请求额度。
`SEMLOOM_TIMING_RUN_ID` 可省略，此时只保存输出目录的摘要，不保存真实目录名。

每行使用 `semloom.execution_timing.v1`：`event`、`operation`、`monotonic_ns`、`span_id`、
`parent_span_id`、`run_id`、`query_id`／原 `unit_id` 对应值、`request_id`／`call_id`、
`row_id`／`stage_id`、实际可得的 `owner_id`／`actor_id`、`pid`、`process_instance`、
`clock_domain`／`clock_verified`、`status`／`error_type`。未取得的身份不编造。

事件复用现有 `BufferedEvents`：有界队列及待写字节、批量后台写出，结束时统一关闭。
成功关闭才发布同名 `.closed.json`，需核对 `accepted_events == written_events`、
`pending_bytes == 0` 以及实际 JSONL 行数；只有进程退出 0 不足以证明诊断完整。
顶层 CLI／owner 显式关闭，写出失败使成功运行报错；已有执行异常保持第一错误并追加诊断说明。
worker 的退出回调只作补充，缺少关闭文件的进程必须按未完整采集处理。
直接使用内部 helper 的调用方须在自身最外层资源清理后调用 `close_timing(error=原异常或None)`。

## 各路径实际覆盖

| 路径 | 新增或复用的实际操作 | 尚不可见的内容 |
|---|---|---|
| NativeLOTUS | owner／LM 准备、原方法和 SDK、共同查询提交至消费、网关发送／收头／完整正文／写出、清理 | LiteLLM 私有线程排队的独立起止 |
| Daft Native | 以上共同层、实际图构建、iterator API 调用、完整迭代、图清理 | 厂商内部排队；iterator API 返回不代表 worker 全部就绪 |
| Ray Data | 以上共同层、原图 API、HTTP session 初始化、可得 actor 身份、完整迭代及原 EOF 清理 | actor 构造内部及 scheduler 排队；session 初始化不是 actor 初始化 |
| SemLoom | 共同查询层、实际执行器创建／drain、已有 Core／transport 事件与时钟身份 | 未公开的服务／GPU 内部阶段 |
| IMLane CPython／Ray | 共同层、适配器建立／start／实际批迭代／close；复用现有 caller `timing_ns` | 作者内部未采集阶段，跨 C++／Python 时钟相减 |
| 现有图像入口 | 各 arm 初始化、source 图构建、完整消费及 audit 返回、warmup／measurement 与清理 | 外层没有绝对锚的首批及原始 EOF，明确 `unknown` |

图像的新开关独立于原 `--detailed-stage-timing`；没有新增 `torch.cuda.synchronize`。
两条 project 模式及既有图像 arm 保留原 `total_s`／`first_output_s`。
消费 span 包含调用者在 `yield` 后停留的时间，不能当作纯执行器计算。
HTTP 写出完成表示本地服务器 `write_eof` 完成，不代表客户端已接收或解析。

账本新增 `identity_check`、历史 GROUP BY 与折叠、`initialize_counts`、`begin_immediate_call`、
事务体、具体 SELECT／INSERT／核计数、原 context 提交或回滚、`finish` 更新及 `counts` GROUP BY。
BEGIN 调用耗时包含取锁及 SQLite 执行，不能称为纯锁等待。事务体还包含身份、截止、额度及摘要处理，
不能整体称为 SQL 时间。提交失败的 context 退出可能包含 SQLite 内部回滚，单独标记。
锁内仅捕获局部整数时间点，事务退出后才编码／入队；不新增请求级 fsync 或放宽预算。

不同进程只有经验证的相同 `clock_domain` 才能相减；否则保留各自局部时长及 `unknown`。
嵌套和并发 span 分别解释，不相加成查询总时。只见开始的 span 保留未完成，不补造终点。

## 本地验证与开销

账本的 [完整配对原值](ledger-local.json) 保留 0／2520 行、正反开关顺序各 120 次 reserve＋finish，
HTTP／模型／GPU 均为 0。开启每轮 3388 事件，关闭 0 事件；计数、返回状态、正文摘要一致。
Mac／Python 3.12／SQLite 3.45.3 的四组开关差异有噪声，开销保留，不外推服务器或 GPU。
2520 行历史核验约 396–402 微秒、0 行约 8–9 微秒，说明分段可识别扫描成本，不能解释旧模型漂移占比。
初版探针未清除上一 case 的开关，8 个样本按 `excluded_scope_error` 保留；修正后 8 个样本另列。

六路径开关 fixture 通过真实 `run_supplier_query` 和方法分发，检查相同请求／结果、基础总时公式及新事件。
LOTUS／SDK 与厂商图使用受控替身；SemLoom 使用本地 Core；IMLane 两模式使用完整响应子进程协议，
没有把它们写成实际厂商／作者模型或 GPU 资格。详细命令、全部失败／跳过及最终源码摘要随核验文件保存。

[软件核验归档](software-verification.json.gz)记录 31 模块、259 项方法：238 通过、21 项预期跳过，
无失败或错误，该次前后源码摘要稳定。随后四项新增错误优先级回归与受影响共同层合计 54 项通过；
两组有重叠，不能相加。账本另外 25 项通过；网关／图像定向 29 项中 27 通过、2 项预期跳过；
探针 20 项无启动测试通过。完整失败记录和原命令保留在仓库外，不把修正前样本丢弃。
[独立交叉审核](independent-reviews.json.gz)按实际源码摘要分别记录共同层、图适配、账本、图像／网关及探针；
最新共同层 54 项补验覆盖最后的网关错误优先级修复。

## 0／2520 行原 Ray 图的有限验证

只用关闭原账本的只读副本抽取前 2520 个序号／phase／kind，不复制请求 URL、正文或原 inode 标记。
新建四个独立完整 schema 的诊断账本，顺序为 0→2520、2520→0；每图两个固定 localhost 响应，
总计最多 8 次 HTTP，模型／GPU 为 0。每份是独立 new-owner，保留原 Ray 图、并发及自然 EOF／shutdown；
不切换真实 budget，不将此安排改成 baseline 每查询重启。
三列投影不保留原请求列的存储宽度，因此只验证历史行数、计数、分段与生命周期，不能替代原账本存储成本回放。

准备／seed、owner startup、进程 bootstrap、查询开始前后、图 API、HTTP session、完整消费与清理分别记录。
该四图只验证历史规模及新增计时，不能据此解释旧 owner 的通用 worker 预就绪差异。
实际复用已安装 Ray 2.56.1／Arrow 24.0.0、Python 3.12.3、aiohttp 3.14.3、SQLite 3.45.3，
没有安装、下载或启动模型。只读 `core,text` 预检通过，自动选择现有通用 Linux NVIDIA profile；
只用 8 个 CPU 亲和槽、Ray 声明 4 CPU／0 GPU，宿主 cgroup CPU 配额为 16。
390 份公共源文件上传前完成敏感检查，服务器逐文件核对摘要；使用隔离新目录与新的 runtime env。
源码清单 SHA 为 `f8cfc50e42ea511f29408a158301867a4f7607bc193d757c30d779e96cab65af`，
源包没有覆盖模型 owner 或实库 owner 的包；后者的 `ea6c9184` 资格与本次计时源码分别登记。

原子取得空闲共享 lease 后，只执行一次四图协议，92.683 秒完成，恰好 8 次 HTTP。
图迭代上限 60 秒、单 owner 90 秒、全协议 180 秒；第 9 次 HTTP、原件摘要变化、计数／响应错误、
未知清理或超时即停止。本次没有触发这些停止条件，执行后释放 lease。

| 顺序／历史行数 | owner 的 Ray init 秒 | 查询至首次 fixture 收到请求秒 | 查询至结果可用秒 | 查询至原生 EOF 秒 |
|---|---:|---:|---:|---:|
| 正向／0 | 10.956409 | 9.424645 | 9.504716 | 9.515080 |
| 正向／2520 | 10.147222 | 9.238526 | 9.303355 | 9.313671 |
| 反向／2520 | 10.093645 | 8.676730 | 8.740284 | 8.750640 |
| 反向／0 | 10.366724 | 9.004034 | 9.077036 | 9.087417 |

这些原值是短 CPU／localhost fixture 的操作时长，不是模型性能或历史规模效应的估计。
owner init 在查询外；查询开始早于本次图构建。首次 fixture 收到请求不等同客户端开始发出。
四图分别核验两条返回身份／响应、账本仅增加两行、计数与历史一致、原 EOF 清理通过；
另按四份实际 `case.json` 的唯一 Ray 目录检查，无存活的非 zombie 残留进程。

每图捕获 15 个进程的计时文件，driver 关闭标记与行数均通过核对；每图另有 12 个 worker 缺关闭标记，
共 48 个按 `unknown/incomplete` 保留，不以进程退出推定尾部事件完整，也不补造终点。
Ray scheduler 队列与 actor 首次就绪时刻仍不可见；bootstrap PID 不升级为 actor 身份。
bootstrap 从 Python 启动 hook 入口开始，不包含操作系统创建进程或更早的解释器工作；
启用时还包含诊断模块加载、writer 设置等成本，不能整体算作原账本初始化，也没有扣除这些成本。
配置 actor 池为 2，不代表两条请求一定使用两个 actor：正向 0 行用两个，其余三图由一个实际 HTTP actor 处理两条。
核验按唯一 call ID 和真实 actor 身份关联，没有改变原生调度来强求 actor 利用数量；初版核验器的错误假设也保留更正记录。

完整[服务器原值与事件](ray-fixture.json.gz)及[独立复核](server-verification.json.gz)保存来源、计数、
每进程覆盖和缺失。外层启动前曾把预检成功值写成 `ready` 而实际是 `ok`，断言在 lease／Ray／HTTP 前停止；
修正后才执行上述唯一四图。外层残留补查最初查错 `config.json`，随后用实际四份 `case.json` 单独核验；
两项准备错误及补查原件均保留，没有追加 fixture 请求。

可用[公共探针](../../../../code/scripts/benchmarks/probe_optional_ray_timing.py)默认打印计划；
在同样通过环境与 lease 检查的机器显式指定 `--execute --output <新私有目录>` 才运行。
`--handoff` 会按实际私有包做本地来源映射；源包部署可改用 `--source-mapping-json <已核验摘要>`。
只传摘要时明确 `private_helpers_rechecked_here=false`，不伪称服务器重新读取了私有原包。

<a id="integration-acceptance"></a>

## 集成分支验收

从 `58462a0d` 普通合入固定 `bbbd8a1b` 及其 `ea6c9184` 依赖，接续只补图像两类诊断故障路径。
[集成核验记录](integration-verification.json)保存390份源码摘要比对及公开事件复算；1796事件、
828对已观察span、12份verified close与48份未知worker尾部一致，没有重新启动服务器。
`bbbd8a1b` 输入的38模块独立进程回归共334项：312通过、22个明确依赖／opt-in跳过、0失败。
这组不与上方不同来源或有重叠的259／54项相加。首轮sandbox禁止localhost监听的失败保留，
仅解除本地fixture监听限制后重验，没有修改源码、安装依赖或新增服务器／模型／GPU执行。

独立审查新增反例：工厂结束计时写出失败发生在caller保存handle前；清理先遇纯诊断错误时，
后续真实worker／Ray清理错误也必须保留正确优先级。修前新增8项组合有3失败、1错误；
现于工厂返回后、计时边界退出前登记owner handle，并提升真实清理错误，原执行异常／取消保持。
修后定向57项中56通过、1真实Ray退出检查因需共享窗口明确跳过；与334项重叠，不相加。
这两个修补只有本地故障／旧生命周期资格，没有新增实际Ray／CUDA完成或性能证明。
服务器390输入仍绑定`bbbd8a1b`且逐文件未变；图像脚本及其回归不在该输入包内，
不能借该实际Ray运行认领这两处图像修补的实测。

### Ray 清理配对的集成接纳

固定 `1d6453d31b9e59123148d22c85f57086c8f7ccad` 普通接纳在 `4f0d05cf` 后，文本
`native_calls.py` 逐字保持 `ef7f7294`；原57／58项final与较早`d6d172f1`共享计时检查分开保留。
[固定Ray2.56.1接口](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/iterator.py)的
`iter_rows/iter_batches`返回可迭代包装；相邻LOTUS图片接点补一行`iter(...)`持有实际generator，
新增包装对象错误回归在修前复现close遗漏，修后显式close及graph shutdown均尝试。
未改图、batching、并发、源先行／FIFO或HSE默认派发；HSE源关闭文件仍同`91803fe8`。

[融合版核查](ray-cleanup-integration.json)为10模块131项：130通过、1固定Ray导入条件跳过；
本机六路径固定HTTP回复24次，实际SDK／服务器／模型／GPU0。跨前阶段用例重叠，不能相加。
原execution／cancel首错与真实清理错误优先级、caller parser的嵌套次错及同异常对象notes防自复制
均保留。真实Ray四图8回复的软件资格仍待独立窗口，不借旧源码实库或GPU成绩。

Standards／Spec两轴冻结终审均0必修；终审后仅填写此核验结果，生产与测试保持冻结树内容。
