# Sema 请求服务接入

本切片服务于 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
Sema 仍拥有 SQL、数据供给、提示、联合提示、原生请求池、限速、解析和结果行关联；接点是 `llm_url` 后的服务。
当前顺序为Sema原生SQL与方法 → 原生请求池 → HTTP正文抵达并读完 → SemLoom任务接纳与外部执行 → 原Sema解析。
SemLoom只能组织已经抵达的完整请求，不能跳过原池、取得尚未发送的任务或接管数据库扫描与算子推进；这不是Sema数据库执行器替换。
`request_sequence`与Core任务编号对应本服务的一次HTTP调用，不是Sema源行身份；`native_ready_ns`表示本次CLI与输入准备完成，不是模型任务在原池前就绪。
实现状态与模型观察由[整合报告](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)记录；本文说明接口、观测和退出处理。
[最新成本诊断](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#adapter-cost-diagnosis)已检查组级复用的实际资源与有限模型样本。
[修复来源的有限模型观察](https://github.com/3444374/ai-operator-execution-optimization/blob/e8051fef151f00349581b8521d1709fb89a54612/experiments/results/postgresql/native_adapter_integration_20261009/README.md#repair-model)属于其实际整合源码和配置，不能据此认定同容量性能或原生请求池前观测已验证。

## 作者产物与采用决定

[固定作者提交](https://github.com/BITQiKangK/SemaSystem/tree/3f2c7182bdaa26c1e8925f486585da25337e687e)公开树只有 README、LICENSE、附录与 `Sema.zip`；归档只有二进制及 macOS 元数据，没有执行器源码或所述实验脚本。

| 项目 | 固定公开产物核对 |
|---|---|
| 版本 | `v0.0.1 8c2d3bd2` |
| 归档 SHA-256，文件完整性摘要 | `e00fc4d1c4e9c99df2ebf647e957ee6943392d2fc74dd22d958d44a71f56632f` |
| 二进制 SHA-256 | `15a534667a668a152524a8756fe5d66c0fa9c9822e73d26029e1a71693a56ec3` |
| 服务设置 | `llm_url`、`llm_model`、`llm_api_key` |
| 原生执行设置 | `threads`、`llm_rate_limit`、`llm_max_burst_seconds`、`semantic_batch_size` |
| 重试 | 135 项非空 SQL 设置未见关闭项；真实二进制探针曾重复发送 |
| 文档差异 | `semantic_voting_rounds` 被固定二进制拒绝 |

原请求含作者 system/user 消息、`temperature=0.0`、`chat_template_kwargs.enable_thinking=false` 及 JSON schema 字符串要求，不含 `max_tokens`。
批量大小 1 对八行发送八次调用，大小 4 将四行联合为一份提示、发送两次；均由作者解析器恢复八行。
服务把每份完整正文作为一个任务，保持提示、请求和响应，不自行拆分、合并或修补非法标签。

<a id="prepool-feasibility"></a>
## 原请求池之前的接入可行性

2026-10-12只读核对。作者main仍为上述固定提交，唯一公开分支为main，没有tag或release；完整树仍只有四项文件。
本地`Sema.zip`与归档摘要一致，只有45,920,168字节的Linux ELF可执行文件及276字节macOS元数据，没有源码、头文件或SDK。
重读已保存的二进制探针，135项非空设置未发现已核实的池前任务回调；本轮不重新运行二进制、查询或模型。
来源、发行包和旧设置文件摘要由[可行性记录](../../../../experiments/results/postgresql/native_adapter_integration_20261009/verification.json)的`sema_prepool_feasibility`保存。

**当前公开产物没有找到可用小改动实现的池前交接。** 这是公开接口和现有探针支持的工程判断，不推断二进制内绝不存在未公开入口。

| 候选路线 | 判断及本次采用决定 |
|---|---|
| `llm_url`或HTTP代理 | 已可用，但原请求池已经产生并发送请求；只能继续作为后置服务接入 |
| `threads`、`worker_threads`、限速设置 | 可作为原路径的物理配置，不导出待执行任务，也不替换原池；线程数不能当HTTP并发数 |
| `semantic_batch_size` | 改变联合提示与模型调用集合，属于算子方法设置；不以修改它制造更多候选 |
| 通用DuckDB扩展装载 | 旧设置确有装载项，但没有已核实的Sema语义请求执行回调；缺少匹配fork头文件、任务类型和调用位置，不能据装载能力认定可小改替换原池 |
| 二进制注入或拦截HTTP发送 | HTTP拦截仍可能在原worker内；要拦截入池动作需识别未公开内部对象与调用方式，工程超出本次范围，不实施 |
| 捕获完整请求后单独重放 | 可以隔离Core请求执行，但改变原生供给和查询生命周期；仅可称请求回放诊断，不能当Sema原生SQL执行或池前适配，本次不实现 |

以后若作者提供源码或稳定回调，优先寻找**原提示、联合提示与方法选择完成后，原限流和入池动作之前**的批任务提交位置。
需要完整请求、查询/阶段/调用身份、完成回送和取消/结束信号；回调若只在原worker内发送HTTP时调用，仍不能达到池前交接。
保留原解析和行关联，仅增加可选提交分流并让默认路径继续使用原池；获得实际源码后才能估计改动，不先建设另一套Sema算子或协议。

保留三种主要比较：原生直达衡量完整系统；原样薄转发衡量接入成本；后置简单有界转发与同核心静态SemLoom成对解释后置执行的增量。
Daft／Ray与Arrow／Ray分别登记；同方法、完整请求、实际调用/输出token、质量、CPU/内存及各自容量覆盖的阶段均核对。
完整查询用实际SELECT提交至结果全部消费；准备/启动另列或按原运行口径报告，不从旧耗时扣除推导新指标。
服务阶段从HTTP抵达到完整写出，模型HTTP另计；原池前请求端到端P99与源行对应继续为unavailable，不能把服务P99改名补值。
后置比较仍受原生供给影响，不能解释为替换数据库执行器的收益；无原池干扰的组织归因优先使用已有LOTUS／DuckDB池前接点。

## 六条路径与接口

| 路径 | 接入与执行 |
|---|---|
| `sema-native-direct` | `prepare_sema_projection(service=None)`，作者二进制直达模型服务 |
| `sema-method-semloom-request-service` | `SemaSemLoomService`，公共任务接口、现有核心、Daft 数据准备及 Ray HTTP worker |
| `sema-native-daft-request-service` | `SemaNativeService`，已经抵达的请求进入Daft0.7.21原生执行图，不创建Core |
| `sema-native-ray-data-request-service` | 同一接收/回送，批内执行由Ray2.56.1 HTTP Processor拥有，不创建Core |
| `sema-native-imlane-request-service` | 同一接收/回送，完整调用进入固定作者DBEnd与CPython Coordinator；使用`--imlane-cpython-config` |
| `sema-native-imlane-ray-request-service` | 同一完整调用进入作者Ray `AsyncActorWrapper`；使用`--imlane-ray-config`，与Ray Data分开登记 |

`sema-native-transparent`是原样转发诊断，`sema-imlane-request-service`保留旧桥配置兼容入口。
六路径均执行原Sema SQL与解析。公开二进制没有可用的池前提交回调，也没有可审阅的优化器、缓存和内部统计源码；
外部适配不能替换或声称复现这些内部机制。当前公开查询固定`semantic_batch_size=1`；联合提示的既有实证另按原设置解释。

Daft、Ray Data及两条IMLane路径使用[原生请求交接](sema_native.py)和[完整调用执行器](../../baselines/text/frameworks/native_calls.py)。
每次取当前已经抵达的有限FIFO快照，不等待SQL结束或更多请求，原生图只执行快照内完整调用。
单个快照默认最多128项、32MiB，使用`SemaNativeLimits`显式声明行数、payload、待写完整响应与图内累计完整响应额度。
前一快照退出后才能启动下一快照是当前adapter的供给形式，不是Daft／Ray库的固有等待要求；
快照构建及图内结果等待计入真实查询，不称全查询充分流水。不排序长度、不增加SemLoom策略或提高作者线程制造供给。

取得名额后的请求、执行中调用和待写响应共享持有上限，直到HTTP写出或异常退出才归还。
正文在读取前取得独立的`intake_requests`／`intake_bytes`额度，读取、接纳及完整响应共用一次请求期限。
入站重复header传入透明转发与原生调用；SemLoom任务只携带完整正文，模型凭据仍由固定配置提供，任意逐调用header转发尚未支持。
响应状态、重复header、原始正文及其中的usage/logprobs保留；公开观察代理对Sema也选择完整HTTP模式。

单查询与常驻入口均接受`--sema-service-config` JSON，或在factory传`SemaServiceConfig`：
`request_bytes`、`response_bytes`、`intake_requests`、`intake_bytes`、`snapshot_rows`、
`snapshot_payload_bytes`、`pending_response_bytes`、`cumulative_response_bytes`、`query_response_bytes`。
可选`listen_port`只绑定loopback，0保持系统分配端口；指定端口在打开服务前校验。
省略配置保持旧有效值；`snapshot_rows`默认取声明持有量，显式值须不超过该量且最多128。
`cumulative_response_bytes`按每图累计完整编码响应；`pending_response_bytes`保留到HTTP写完。
服务另按正文统计全查询累计量，`query_response_bytes`默认`max_requests × response_bytes`，
记录实际返回量及已接纳量；native直达没有这些外部服务额度。transport与vendor内部临时buffer另受各自额度管理，
这些编码/正文计数不保证整个进程RSS。IMLane作者库六项额度来自其独立调用配置，实际限制取各层较小值。

IMLane通过[薄适配](sema_imlane.py)执行完整HTTP调用，不使用算术fixture代替。
单查询与常驻入口的`--imlane-config`读取仓库外配置，核对作者提交、DBEnd程序/UDF摘要、
loopback端口、私有输出目录、实例名称和实际原生任务线程数。
Coordinator与其Python/Ray运行环境由调用方事先准备；查询只拥有自身DBEnd子进程，不停止共享服务。
取消和超时会终止本次子进程，未确认的远端HTTP仍写unavailable，不补造已取消或自动重试。
报告分别记录桥已创建、提交可能性、本地停止和作者模式；`returncode=None`不能证明未启动。
原生图缺少可靠停止报告或存在未知响应后，禁止同一executor跨服务继续使用；一份声明为clean的报告不足以掩盖缺失结果。
HTTPX共用传输的cookie使用每调用独立jar，保留同一调用内部的重定向规则及连接复用。
SemLoom pump首错通过原cause、等待者及退出传播；未知Core责任或未归还资源阻止后续借用。

各阶段从[服务时钟分析](../../experiments/postgresql/sema_native_query.py)取得：抵达至正文读取、
接纳等待、入队、快照等待、图启动、原生执行至完整响应收到、响应收到至HTTP写出。
原生执行区间包括图执行、模型HTTP和完成收集；其中重叠部分不拆为纯模型计算。
单请求服务耗时从抵达到写出，原池前任务端到端仍不可观测；整查询从实际SELECT提交到SQL结果全消费。
库/服务初始化和原生输入准备另外报告；请求/查询P99都保存样本数、原值和是否仅为样本最大值。
独立服务检查验证原始非成功响应交回；统一实验入口的质量审计遇到HTTP或输出错误会停止整查询，
因此该实验入口的失败处理不能作为任意原生SQL错误政策完全一致的证明。

[sema_service.py](sema_service.py)复用[原有 CLI 生命周期](../../baselines/text/products/sema.py)：先导入并核对 CSV，再提交同一 SELECT，读完 SQL 结果及完成标记。
[sema_semloom.py](sema_semloom.py)复用 `prepare_native_task`、`NativeTaskSession`、`decode_full_response` 和 `ray_map_factory`；数据准备默认 `daft`，可显式选择既有 `arrow` 直接分批，响应模式为 `full`，不另建方法驱动或调度核心。
SemLoom分支不创建透明转发的HTTP客户端；完成响应只解码一次，原始编码结果仍由Core持有到HTTP写出完成后归还。
执行核心 Core 在服务输入输出（I/O）线程上建立和操作，跨线程取消使用公共信号；持有任务数 `max_held_tasks` 与活动请求数 `max_active_requests` 分别传入现有核心。
pump 有即刻工作时让出事件循环并继续推进，其他情况使用既有短轮询。满额收取完成事件后，Core 提示再检查一次，下一次空收取仍允许等待。
观测通过 `call_soon_threadsafe` 在 owner 的下一次循环记录。通知等待与当场记录的联合候选在短模型对照中更慢，默认恢复原流程；受测源码和负结果保留在[执行成本报告](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#execution-cost-repair)。
延后记录动作或必需字段读取报错时，通过现有首错与取消逻辑停止查询，记录阶段为`observation_record`，原异常保存在首错的`__cause__`。HTTP已成功返回或SQL已读完时，服务退出仍传播这项记录首错，整查询不能因此报告通过。
已有更早的HTTP／查询错误继续保留；附加记录错误和取消／清理错误按原清理记录保存。取消只发公共session信号，未确认的远端请求仍由Core保留，慢HTTP写出完成前不归还结果使用权。
当前 work 描述每份完整请求为一个 `work_units`，即请求数表征，不代表已校准的 token 工作量。
`NativeTaskSession`的Job由外部查询runner创建，不来自Sema查询计划；组级复用只延长外部Core／worker的寿命，不改变接点位置。
HTTP 调用者保留尚未接纳的正文，读取前取得独立入口持有数／总字节预留；Core 的任务额度不能单独代表全部前端正文留存。
原 `prepare_projection`、`run_projection` 与实验默认调用方式保持。

## 观测与计时解释

分层诊断显式使用`local_diagnostic=True, physical=None`，复用相同服务、Core、接纳锁与完整响应，
HTTP传输采用现有`FullResponseTransport`，发送前检查与原错误返回保持；默认仍要求Ray物理配置。
本地`http_started/http_finished`记录到同一服务行，其时刻来自同进程单调时钟，不补造Ray事件。
简单服务可选`held_request_capacity`与原连接额度一起限制已接纳请求／响应，直到HTTP写出或异常退出才归还；
默认0保持原透明转发。诊断装配的真实短测与适用范围见[分层结果](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#sema-layer-comparison)，运行要求见[分层方案](../../../../experiments/plans/语义系统对照.md#sema-layer-comparison)。
服务从HTTP handler进入的`proxy_arrived_ns`确定一次`request_deadline_ns`，覆盖入口等待、正文读取、校验／解析、名额或接纳等待、完整响应及HTTP写出。
转发、SemLoom和native快照都沿用该时刻，后续不重新取得完整超时时间；响应使用权到写出或异常退出才归还，外层SQL期限保持。
连接建立、HTTP头抵达前的等待不属于这个handler期限。正常、上游回复及合成404／410／413／502都在原截止时刻前完成应用写出，不重新给予timeout。
期限已过或写出不能按时完成时关闭连接并结束协议处理；服务保留原首错（期限错误仍记录502），客户端可能收到断连而非新的HTTP错误正文。

| 现有记录 | 实际作用 |
|---|---|
| `proxy_arrived_ns` → `body_read_ns` | 服务处理入口至完整 HTTP 正文读取 |
| `body_read_ns` → `core_accepted_ns` | 正文校验、任务准备及等待接纳；尚未单列纯容量等待起止 |
| `core_task_sequence` / `request_sequence` | 本服务的任务与 HTTP 出现对应；相同正文的两个出现仍是两个调用 |
| `core_events` | 当前核心及传输事件，包含准备、提交和返回；`observed_ns` 是观察回调时刻 |
| `payload_stage` / `object_put` | Daft／Arrow 准备和 Ray 对象写入；可能按共享批次记录，不能逐行重复计费 |
| SemLoom `forward_started_ns` | Ray RPC，即远程调用入口；发生在远端 worker 的 HTTP 操作之前 |
| `worker_started_ns` → `worker_ended_ns` | worker 入口至完整执行响应返回，含 payload 取值与 HTTP，不是纯模型时间 |
| `model_returned_observed_ns` | driver 收到远程完成；本地单调时钟 |
| `model_returned_ns` / `model_clock_shared` | 仅在核验共享时钟后使用 worker 完成时刻，否则为不可观测 |
| `core_delivery_ns` | pump 取得与当前 HTTP 调用对应的 Core delivery 时刻 |
| `response_ready_ns` | pump 即将设置当前调用的响应 future 时刻，可能是异常 |
| `forward_resumed_ns` | HTTP handler 等待响应成功恢复，校验正文大小和准备返回之前 |
| `response_written_ns` | 服务完整响应写出后归还租用，不证明作者已解析 |
| `submitted_ns` → `finished_ns` | Python 包装实际 SELECT 提交至 SQL 结果和完成标记读完 |
| `native_task_ready_ns` / 原生 HTTP 到源行对应 | `unavailable`；作者请求池前时刻与可信行身份没有已核实的接点 |

本地时刻使用 `time.monotonic_ns()`；Ray 通过 Linux boot ID、time namespace 和时钟实现的摘要核对共享时钟，并检查事件先后关系。
同一时钟下可直接计算对应区间；时钟不同只使用各自内部耗时及本地接收时刻，不跨机器相减。
新增三个时刻仅来自 SemLoom 服务对应的实际动作；未取得时刻保留 `null` 或缺项（不可观测），原生透明路径不补造时刻。原件在退出时写入，请求中不增加同步磁盘写入。
RPC 至本地接收包含 Ray 等待、worker 执行及返回；worker 区间嵌套其中，准备阶段也可能重叠，不能相加或从完整查询时间扣除。
作者线程数、Core 活动额度、实际 HTTP 在途与模型服务执行序列分别记录。
Sema 作者 SQL 进程可以跨查询复用，默认 Core 和 actor 逐查询创建；可选 `group-diagnostic` 在同一控制线程复用设施，任务流、HTTP入口、错误和观察逐查询独立。
该owner只允许顺序查询复用，已有服务未退出时拒绝再接入；并发查询接入仍需独立验证每查询Job、取消和结果责任。
查询期间的准备进入释放至结果消费结束（EOF）的完整时间，组级初始化另列，实际 SELECT 提交至读完结果直接记录。
观察包含回调、JSON 和持久记录开销；[分层诊断](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#sema-layer-comparison)记录接纳、推进、sink及线程CPU，尚未独立拆出JSON和持久记录函数的CPU，不从完整查询时间（JCT）扣除这些累计值。

[作者 README](https://github.com/BITQiKangK/SemaSystem/blob/3f2c7182bdaa26c1e8925f486585da25337e687e/README.md)提供 trace 日志及 `EXPLAIN ANALYZE` 的 token／profile 入口，但没有逐调用池前就绪与行对应的公开字段定义。
这些日志的实际字段、memory/file 保存和时钟语义尚未在固定二进制核实；未取得可信协议前保留上述缺项。
日志到达 Python 的时刻只表示接收，算子总时间不能代替逐请求就绪；stdout 日志还可能混入现有 CSV 消费路径，不默认启用。

## 错误与退出

首个 HTTP 错误保留原状态、原始正文和响应头，写入仓库外 `.first-error.json`，正文以 base64 保存；随后停止发送、取消自有作者进程并拒绝重试转发。
直达路径没有中间服务，不能观察首次模型 HTTP 错误或替作者关闭重试，继续保留有限 SQL 超时和待核对的错误协议。
每份请求仅在选定发送前位置登记一次；`forwarded_posts` 是发送前预留，实际模型接收需独立收据核对。
已经发送或未知的远端工作继续由原核心回收；完成响应的租用留到 HTTP 写入结束，调用者离开后的迟到结果也须归还。

退出依次尝试 HTTP runner、Core、实际创建的 HTTP client 和事件循环；任一步报错仍处理后续动作，保留首次异常及每项动作／类型／单调时刻。
runner 清理报错时使用 [aiohttp 公开接口](https://docs.aiohttp.org/en/stable/web_reference.html#aiohttp.web.BaseRunner)停止剩余 site 和连接；响应持有者未退出则保留 Core 和循环，监听停止不能确认则保留 runner、端口并报告未完成。
取消或线程等待超时继续记录实际线程与循环状态；线程结束不替代监听停止证明。
已有查询或 HTTP 首错继续作为主要原因；退出期间才初次到达的 HTTP 首错补充传播，此前已报告的首错保持原处理；没有主要错误时传播首次清理异常。
`summary.cleanup_failures` 与 `.cleanup.json` 保存退出失败，`first_cleanup_error` 指向首次异常；记录写入失败追加保存，不覆盖查询错误。

## 验证与历史诊断

[固定历史版本](https://github.com/3444374/ai-operator-execution-optimization/blob/7663ab525f0133769dc37114e4059c7d65c396f2/code/src/execution_provider/adapters/sema_request_service.md)保留作者产物检查、全部 CPU 对照值和退出反例；完整原件、失败、来源与恢复摘要继续由 `sema.json` 交接定位。
容量修复让正文只准备一次，提交序号随实际接纳更新；仅实际归还、取消或终止错误唤醒等待者，避免原每次推进反复准备与校验。
接纳锁只允许下一位HTTP调用者向公共Core重试，避免一次归还唤醒全部调用者并重复检查容量和元数据。
已接纳调用在锁外等待完整响应，模型与结果额度不变；等待接纳仍计入原请求期限。取消、超时和错误退出会归还锁。
只有持锁调用者等待容量并重试；其他调用者仍持有已读取正文和任务描述，等待接纳锁。
该锁属于当前查询的HTTP服务，结果归还不取得这把锁；锁的持有期只覆盖接纳，不包括模型执行或HTTP写出。
`SemaServiceLimits`追加可选`intake_requests=128`和`intake_bytes=33554432`，原四参数构造与`request_bytes`／`response_bytes`保留。
三条服务共用入口预留，供给已有占用时先等待额度再读正文；等待计入同一期限。native的持有槽和单snapshot32MiB限制仍独立，不向其vendor图注入Core额度。
已知长度读取前预留`2 * Content-Length + 1`字节；未知长度、chunked或解码长度未知时预留`2 * request_bytes + 1`，为读取buffer／最终bytes转换和1字节超限探测留空间。
逐块检查后完成正文，预留缩到实际正文长度。单份超出`request_bytes`或其读取预留永远不能容纳于`intake_bytes`时返回413，保留首错并停止查询，不截断正文或重试。
入口每个HTTP出现只登记一次，覆盖解析、接纳锁、已接纳handler及响应写出。Core接纳仍取得自己的payload／结果责任；这两个作用域有重叠，不相加为物理内存测量。
native将同一入口登记交给待处理／活动snapshot共同持有；handler退出和bridge结束后，还须取得`graph_consumer_stopped is True`才归还。
报告None、缺字段、字段None、非布尔值，或`clean=False`而无明确True都保留登记并报告图消费停止未确认；无异常与bridge返回不能代替该证明。
取消中止正文读取与尚未读取的入口等待者；aiohttp不再在拒绝后额外读取剩余正文。正常排空后`summary.intake_requests`／`intake_bytes`归零，峰值字段保留实际预留最大值。
这些字节计量服务持有的正文及读取临时表示，JSON／Python对象开销、aiohttp预解析缓冲、socket、vendor图复制、Core远端未知与首错响应另有责任，不代表RSS总上限。
相近请求使用队首先接纳；异质字节需求可能阻挡后面可接纳的请求，尚未完成对应性能评价。
接纳次序也不表示SQL行序或多查询公平；这些分别由结果身份及Core的Job／flow处理，不将查询内锁扩大为全局调度。
这项工程修订的模拟计数与后续真实核对记录在[接纳重试报告](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#sema-admission-retry)。
作者二进制及实际 Daft／Ray 的 CPU 诊断共 32 查询／2,176 次替身 POST，模型为 0；含带探针与不带探针对照，分别保存，不推断同容量系统加速。
[退出测试](../../../tests/execution_provider/test_sema_service_cleanup.py)十四项继续覆盖 runner、多项关闭失败、等待超时、记录写入、初始化、取消、监听及迟到 HTTP 错误；四个实际 aiohttp／公共 Core 反例原件也保留。
真实级联、多模型、Join、并行阶段展开、原生执行器替换及原生池前时刻仍为 `pending`。
