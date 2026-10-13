# 固定发布版的真实 SDK 与原生图软件核查

最新补充：[同步IMLane原Q2公开入口](#imlane-sync-public-q2-software)在固定3bd53587通过两作者模式，240逻辑／480传输POST；真实模型尚未运行。历史三个窗口继续按各自范围解释。

本记录保留已发布4566f1fc在既有服务器、CPU8–15上的有限软件验证。没有安装依赖、重建作者库或运行模型／GPU；IMLane及2000行矩阵未重复。
自动机器模板的CPU项仍为8<10，相关既有依赖项通过；不宣称完整机器模板或模型环境资格。

## 第一窗口：部分通过，整轮未通过

原窗口上限600秒、256次localhost POST。监督从首次测试进程启动计时，实际结束363.192秒，累计154次POST，未超额度。
首个脚本因common模块重名在用例执行前退出，POST0；失败保存，纠正后沿用原始开始时刻与计数器，没有重置窗口。
共执行17项：14通过、1项失败（两个backend子用例失败）、2项用例错误，另有1项类初始化错误；0跳过。

| 检查 | 实际结果与范围 |
|---|---|
| 共享SDK | 三项固定OpenAI／LiteLLM解释差分通过；输入是保存响应及参考MockTransport，真实SDK运行，未发送HTTP。正常usage／logprobs／可选字段、错误状态／坏JSON／缺内容／timeout、一次客户端连续解释均核对。 |
| LOTUS＋Daft／Ray Data | 四项通过，64次真实fixture POST；原方法正常、cache／统计、异常类型、完整响应、两阶段等待与提示保持，非法参数／分派前取消为零发送。 |
| 原生图 | 重复身份／完整HTTP错误、字节超额及取消／期限通过。提前关闭的到达计数断言分别4→5和23→24失败；缺少发送与关闭的同域时间戳，不能据此认定新请求是在关闭后发出，更不能认定历史泄漏。 |
| 真实客户端 | keepalive四次请求复用同一连接通过；真实关闭后注入OSError，首个字节错误及清理诊断保持。cookie脚本漏开follow_redirects，解析空307响应失败，尚未取得该项资格。 |
| 观察模式 | 发送前预算拒绝通过；模式差分因concurrency2不能承接两actor×两async batch在分派前拒绝，新增POST0，尚未取得差分资格。 |
| Ray Core参照 | 原方法／cache／SDK正常通过，12POST；后续类由两种导入名重复初始化Daft而失败，actor CPU／终止检查未运行，非六条主路径资格。 |

监督及独立只读复核均确认登记的82个进程身份无存活残留、无CPU8–15以外的已登记进程，GPU compute列表为空。
这些数据只支持对应组件及软件fixture范围；没有实际模型质量或性能结论，performance仍保留响应转换及日志开销。

机器事实和分项结果见[verification.json](verification.json)，首轮失败、日志、收据与验证脚本见[observations.tar.gz](observations.tar.gz)。

## 第二窗口：三组有限复核通过

父任务另行划定600秒／256次POST的新窗口，继续CPU8–15、模型／GPU0；首轮原件和计数保持独立。
先明确cookie重定向选项、有效Ray参数，并用发送／关闭时间戳区分在途请求与关闭后新开始。
原生无法同步确认的责任继续保存为未知，提前关闭不得改写为clean，不更改厂商调度。
观察模式使用原Movie Q2/Q3与官方评价、八行明确software_fixture，三条原生路径分别连续两查询；不将fixture标签核对写成真实模型质量。

公开CLI零POST预检暴露只读SemBench Git checkout被私有产物检查拒绝。修补仅为checkout来源路径关闭该私有路径检查，
其固定commit／tracked内容核验仍由verify_checkout执行；模型配置、清单、CSV、输出与运行配置继续按原私有路径规则检查。
本地30项受影响回归全部通过、0跳过。固定修补源码6629846a的四次公开CLI数据／运行时预检均通过、POST0；
预检使用原2000行CSV与固定清单，未执行完整公开CLI查询。

task-10独立async交付02a75cc4的作者生命周期在153POST后因307缺cookie停止，作者Ray及Q1未启动；该候选仍未合格且未默认启用。
本服务器aiohttp3.14.3与其3.14.5的ClientSession都经`update_cookies_from_headers`处理原始header，代理jar原继承DummyCookieJar的空实现。
现只增加向本次ContextVar默认jar的同名委派，保留旧mapping入口、IP来源cookie拒绝、同请求重定向及并发／跨请求隔离。
依据来自固定安装源码及[aiohttp3.14.5客户端](https://github.com/aio-libs/aiohttp/blob/v3.14.5/aiohttp/client.py#L931)和[cookie接口](https://github.com/aio-libs/aiohttp/blob/v3.14.5/aiohttp/cookiejar.py#L992)。
41项本机受影响回归通过、0跳过，两轴有限复审均无必修。
实际3.14.5的307／302与两loop回归通过：每loop两并发调用及后续独立调用，开始无cookie、重定向只携带本调用cookie，结束池关闭，跨loop客户端不同。
此项18POST、6GET，未启动IMLane作者过程或重复作者矩阵。

三组共3项通过、0跳过，监督从测试进程首次启动计时104.662/600秒。
HTTPServer收据计数101POST（cookie18、提前关闭11、Movie fixture72）；aiohttp.web代理未纳入该计数器，另由12单元中72条唯一网关到达和正文摘要核对。
总计173/256 localhost POST，符合预先保守上限174；原始监督文件的`posts=101`只表示fixture计数，不能读作全部POST。
代理发送前共用72名额账本，十二单元各扣6次且已关闭，再次扣记被拒绝；不存在把首轮额度重置或转借的操作。

| 跟进 | 实际结果与范围 |
|---|---|
| 提前关闭 | 实际Daft／Ray Data各六调用，分别到达5／6。透明aiohttp发送回调保存与close同域时间戳，关闭返回后新request_start为0、活动请求最终0。Daft池关闭为true；Ray池状态及远端HTTP停止仍unavailable，两者clean仍false。未更改厂商调度。 |
| LOTUS模式 | qualification／performance×原臂／Daft／Ray Data×原Q2／Q3，12单元、每单元6个完整调用。三条路径每种模式连续两查询，owner／LM保持；模式间正文多重集、原查询结果与官方评价一致。原performance完整预期值明确unavailable，替换臂仍通过实际值核查。 |
| 官方评价 | 八行software_fixture中每单元1误报／1漏报，Q2保留LIMIT与重复，Q3保留COUNT5；原evaluator与全出现分类审计均保存。不以替身响应声称真实模型质量。 |
| 清理 | 监督登记68个PID／创建时间，CPU均8–15，无超出记录；结束及独立只读复核均无存活残留，GPU compute为空。 |

1003份运行源码／测试文件与固定6629846a逐摘要核对，验证脚本也与上传摘要一致。
第二窗口机器事实、十二单元结果和准确计数见[followup-window02.json](followup-window02.json)，预检前后、原始日志、发送时间戳、账本和请求见[window02-observations.tar.gz](window02-observations.tar.gz)。
压缩包265成员及101个解码POST正文已独立扫描、无隐私违规；私有路径与外部IP已脱敏，首轮档案保持。
逐成员原始／公开摘要见[window02-members-sha256.json](window02-members-sha256.json)；源码及运行侧车里的原摘要属于脱敏前字节，公开成员另按脱敏后摘要复核。

仍待：完整公开SemBench CLI执行、最新发布源码的作者IMLane生命周期／完整Q1、真实模型Q1/Q2/Q3质量与成本、正式交错测量，以及非主路径Ray Core CPU／终止诊断。
本轮没有性能排名；响应转换、日志与查询内观察仍计入时间，没有估算扣除。默认qualification与同步IMLane保持，async候选02a75cc4仍未合格。

## 第三窗口：四路径公开CLI原Q2／Q3通过

本窗口另获用户本人授权：固定d004e720、CPU8–15、模型／GPU0；上限900秒、1280逻辑调用、2560传输POST。
准备时发现最初1280传输额度不足，未启动旧额度；明确按两跳调整后才执行。
最初上传／预检被自动审批拒绝，远端命令未执行；用户本人确认限定范围后重试获准。没有绕过审批或途中改限额。

两次实际公开入口均以`python -m src.experiments.postgresql.sembench_query_cli --config <private-config> --execute`运行并退出0。
主矩阵原LOTUS、Daft Native、Ray Data、SemLoom四路径各Q2／Q3，共8查询960逻辑调用；原LOTUS的performance Q2另120调用。
原关系2000行、CSV／清单摘要及原SemBench源码保持，每查询按原WHERE选120行；不修改查询、输入规模或标签。
回应由消息内容摘要确定True／False的本地HTTP fixture提供，未把标签交给回应器。
固定SDK、原LOTUS parser／evaluator、Daft／Ray／SemLoom真实执行；软件回应与实际依赖分别标注。

| 覆盖 | Q2 | Q3 | 模式与配置 |
|---|---|---|---|
| 原LOTUS | 120调用通过 | 120调用通过 | qualification；另performance Q2配对120调用 |
| Daft Native | 120调用通过 | 120调用通过 | qualification，原图及公共配置保持 |
| Ray Data | 120调用通过 | 120调用通过 | qualification，两actor／两async batch／batch1 |
| SemLoom | 120调用通过 | 120调用通过 | qualification，workers2／batch2／held8／active4；结束Core用量和Job均0 |

同查询的四路径正文值多重集、120个独立出现身份、原解析决定、关系结果与官方评价一致；原Q2的两模式也一致。
每个主owner连跑两查询、owner／LM身份保持；九个单元各扣120名额并已关闭。
`call_id=lotus-filter`为方法标签，出现身份使用row_id＋stage_id＋stage_ordinal＋call_id组合，不把标签当逐调用唯一ID。
performance没有构造预期正文，120个预期摘要为null且审计unavailable；真实代理正文摘要继续保存和配对，未把缺项写成相等。

质量错误继续计入：Q2为TP9／FP54／TN52／FN5，LIMIT5返回的五项均非原gold集合，官方F1=0；
Q3为TP6／FP52／TN54／FN8，原COUNT返回58、gold14，官方绝对误差44。解析非法和缺项均0。
这些值仅支持软件差分和评价接线，不是模型准确率或模型性能结论。

双计数为1080 gateway＋1080 fixture＝2160/2560传输POST，对应1080/1280逻辑调用；发送前扣记、实际网关到达与fixture收据逐摘要复核。
计时从第一次零POST预检前的保守时间开始，含预检和准备间隔359.252/900秒，执行时未重置；两CLI分别82.304与13.689秒，不据替身耗时推导模型收益。
45个登记PID／创建时间仅在CPU8–15，监督及独立只读复核均无存活残留，fixture线程停止、未结回应0、GPU compute空。
未运行IMLane作者、Q1、模型或新async候选，task-10的CPU16–23／作者过程保持独立。

详细覆盖、配置与原评价见[public-cli-window03.json](public-cli-window03.json)，269成员日志／预检／账本／收据见[window03-observations.tar.gz](window03-observations.tar.gz)，逐成员原始／公开摘要见[window03-members-sha256.json](window03-members-sha256.json)。
完整原始提示保存在私有原件；公开件省略4320段消息内容并保留摘要，避免复制原评论文本。原摘要属于脱敏前字节，公开成员另用公开摘要核对；隐私扫描0违规。
此前两个窗口原件与失败未改。第二窗口早停仅证明close起止之后无新request_start及活动请求最终0；没有逐请求远端停止确认，clean=false／remote_http_stopped=unavailable保持。

剩余模型小轮见[同任务单双卡方案](../../../plans/语义系统对照.md#sembench-native-model-next)；先用已验证同步接法，不等待async新功能，尚未启动模型。


<a id="imlane-sync-public-q2-software"></a>
## 同步IMLane原Q2公开入口软件窗口

固定已发布3bd53587，单独300秒／256逻辑调用／512传输POST窗口。实际两次公开
`python -m src.experiments.postgresql.sembench_query_cli --config <private-config> --execute`
均退出0，作者CPython及原生Ray各原Q2完整120调用；原2000行关系、WHERE、LIMIT5、提示、SDK parser和官方evaluator保持。
从外层监督启动、早于作者启动与首次公开预检计时，共49.861秒；未触发停止信号或强制清理。

| 路径 | 实际逻辑调用 | 完整查询／就绪后查询 | 观察身份 |
|---|---:|---|---|
| 同步IMLane CPython | 120 | 4.367733／4.265819秒 | 两个原二进制执行器，与Coordinator父进程、executor参数、PID／start ticks及bootstrap关联 |
| 同步IMLane作者Ray | 120 | 4.509083／4.404728秒 | 两个原同步ActorWrapper，按准确进程命令、PID／start ticks及bootstrap关联 |

这些是qualification软件替身时间，不作性能排名。两模式完整请求值多重集、120个组合出现身份、解析决定、LIMIT结果和评价一致。
声明HTTP上限6，原作者META为executors4／threads6，实际各两名工作进程；不将META或桥的pending128读作物理并发。
发送前账本及真实收据核对：gateway240＋fixture240＝480传输POST，对应240逻辑调用；6个实际发送进程的bootstrap源码摘要和有效hook保持，未确认返回0。
fixture仅根据消息摘要生成True／False，无标签输入；TP9／FP54／TN52／FN5、LIMIT5的官方F1=0保留为替身错误，不声称真实模型质量。

为核对前述四路径，只在离线请求副本中将其软件模型名改为本窗口模型名，其他完整JSON值、决定、结果及评价均相等。
两个窗口实际执行的原正文与原摘要各自保留；此处比较不是六路径同时运行或同一模型性能证据。

实际核1001个公开Git源码文件、缓存模型12文件、2123个vLLM源码文件、原作者及现有容量桥摘要；十CPU runtime只读检查通过，模型未启动。
最初上传因自动审批要求明确材料与目的地而被拒绝，未执行；补公开GitHub可读、逐Git blob匹配和隐私证据后，同一上传机制获准。
首次只读资产核验误选扩容前旧桥而失败，原件保留；随后改用已有已验证b201be88容量桥，与当前C++源码1e27ee6e匹配，没有新编译、安装或作者库修改。
自有源码副本仅追加指向同一私有计数入口的sitecustomize辅助链接，让作者原Ray UDF目录能加载计数；1001已发布文件和作者调度／函数正文未改。

监督登记31个进程身份仅CPU8–15，结束及独立只读复核均无自有残留。fixture活动连接／请求线程为0，GPU compute列表为空，模型／GPU0。
原始导出首份漏选bootstrap目录，保留后补齐导出，未重跑窗口或增加请求。
[验证记录](imlane-sync-q2-verification.json)、[232成员脱敏原件](imlane-sync-q2-observations.tar.gz)、
[逐成员双摘要与省略记录](imlane-sync-q2-members-sha256.json)及[离线核查脚本](recompute_imlane_sync_q2.py)支持复核。
公开件省略1440段消息正文、保留原内容摘要；240个编码请求也同样省略，720个解码payload独立隐私检查为0违规。
原正文／wire摘要属于脱敏前字节，公开省略件使用成员公开摘要，不用省略正文重新计算原请求摘要。

此次软件窗口补齐同步六路径原Q2的最低公开入口前置项，不恢复旧额度、不重复作者容量或异常矩阵。
同步完整Q1／Q3 CLI、可选async完整CLI及模型质量／交错测量仍按[现有计划](../../../plans/语义系统对照.md#sembench-native-model-next)分别记录；两项历史Q1适配器前置拒绝保持，作者原系统容量不由旧128配置推断。
