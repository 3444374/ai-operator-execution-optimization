# 三种原方法的原生执行接入

<a id="native-adapter-completion"></a>
## 公共Ray启动、正常EOF关闭与实库补验

2026-10-14从实际远端`integration/baselines@1b90c1ea39e12c9fff2e2265aae003e25aa9d618`接续。
该版本已包含`0be63a19`及整合`d1f5ef74`，Daft／Ray图、LOTUS SDK与公共native查询入口
相对旧接入交付无源码变化；IMLane和图像来源保持各自owner归属。
只读审查及零模型复现另发现两处公共适配缺口：

- 准备调用的Ray启动恒使用SQL reader的`QueryConfig`，误拒绝Ray Core／SemLoom的C1、CPU1，
  也将SQL reader校验带到准备调用。现用只含地址、CPU、object-store和导入异步批次数的
  `NativeRayRuntimeConfig`；所选Ray Data仍在启动前核A×B×R≤C，原SQL reader检查保持。
- 全部结果已交付后，厂商生成器正常EOF的内部关闭异常登记为`graph_consume`，旧复用判定却只识别
  显式`graph_close`。现任意worker错误均保留未确认责任，下一批零图进入；原异常和关闭诊断继续保留，
  完整HTTP错误响应仍由原SDK解释，正常成功与发送前取消的复用行为保持。

两项修补仅涉及项目适配层；原生执行器、图、UDF、批处理、并发和原方法未改。
修前同一整合版本的本地组合75通过／1缺Ray导入检查跳过；修后受影响组合Ran90，89通过／1同项跳过，0失败。
runtime专项9项、EOF责任专项24项与最终组合重叠，不相加。两个实现由另一审查者交叉核对，均无待修项。
12个原生图／HTTP kernel／SDK／查询runtime符号或文件与旧交付摘要一致；
[机器核验](native-adapter-completion-verification.json)及[脱敏检查原件](native-adapter-completion-evidence.tar.gz)
保留修前失败、修后结果、全部成员摘要和独立审核。本机Python3.12.6、aiohttp3.14.1、httpx0.28.1；
该本地阶段未安装依赖；当时低CPU新配置的实际厂商图执行及实库EOF注入未覆盖，补验见下文。
既有[5项实库活动检查](#native-http-activity)继续绑定`81a38e52`及120localhost POST，
[单卡原Q1／Q2八查询证据](https://github.com/3444374/ai-operator-execution-optimization/commit/4f392c3652803c7786e56245c0bbc18a3e6e9bd6)
仍绑定模型owner的原来源：8480行请求／SDK响应关联通过，原解析重放零差异，Ray Q1／Q2峰值4。
既有实库与模型证据仅只读复核；本轮另做本地替身和localhost HTTP回归，
该本地阶段未新增远端服务器、厂商图、模型或GPU执行，也没有新性能结论。

### 低CPU与实库EOF补验

随后按三个独立时钟、账本和源码包执行有限localhost软件检查，使用既有Python3.12.3、
Ray2.56.1、Daft0.7.21、Arrow24.0.0；没有依赖安装、模型或GPU启动。
源码基于`ea6c9184a1d30639a87432f32381bab40067353e`，第三包另含下述两行项目适配校验。
每包和测试修订的完整SHA-256见[机器记录](native-adapter-completion-sdk-verification.json)，
原日志、完整响应、账本只读导出、失败、资源采样和复算入口见[公开原件](native-adapter-completion-sdk-evidence.tar.gz)。

| 独立窗口 | 实际用例结果 | 逻辑／两跳物理POST | 秒／采样合计RSS峰值字节 | 最终登记进程身份 |
|---|---|---:|---:|---:|
| 首次 | Core CPU1、SemLoom Arrow CPU1、Daft EOF通过；Ray EOF工具断言失败，Data CPU1未启动 | 8／16 | 73.446／2798657536 | 40全部退出 |
| 受影响补验 | Ray EOF通过；Data CPU1在HTTP前超时 | 4／8 | 112.521／3751063552 | 45全部退出 |
| CPU声明修补后 | Data CPU1提前拒绝；CPU2／A1／B1／R1／source blocks2通过 | 2／4 | 45.417／2805182464 | 17全部退出 |

七次用例尝试为五次通过、一次断言失败、一次错误，0跳过。五个目标用例按各自固定来源通过，
不称三个源码包为同一完整通过轮。全部14逻辑／14代理／14fixture／28物理POST，
429／500状态、二进制完整正文、重复header顺序、HTTP版本及call_id／row_id关联均通过。
这里的重复header检查针对响应；请求侧X-Input在既有native映射中仅保留最后值，
SemLoom使用自身默认header，不增加请求重复header保留资格。
三账本共预留20、实际发送14，失败单位中6个未发送额度保持已关闭预留，未退款或改写旧账本。
每窗口持有同一共享lease，quota16、固定8核亲和和4GiB采样RSS停止条件；最终0强制清理、
无自有PID／出生身份残留、lease已释放。采样合计RSS不表示PSS或未采样瞬时内存上界。

首次Ray工具假定`Dataset.iter_rows()`的公开Iterable本身有close；该版本实际须由`iter()`取得厂商生成器。
工具修正后在自然EOF、实际生成器关闭后注入一次错误；Daft亦在实际iterator关闭后注入。
两路径均先交付完整结果，再保留原关闭错误和不可复用状态；同一执行器下一批零图／零HTTP，
另建执行器的正常两调用通过。厂商图、UDF、HTTP kernel与原方法不变。

Data CPU1原日志显示固定HTTP actor占1CPU，原SDK预处理task仍需1CPU，因而零HTTP超时。
该失败的`clean=False`、消费线程未确认停止及`consumer_stop RuntimeError`原样保留；
controller退出后的45身份清理是另一项事实。项目只在`validate_backend('ray-data')`增加
`num_threads > ray_actors`，在导入、环境修改和runtime启动前拒绝这类声明。
Core／SemLoom CPU1保留；CPU2正向实际图通过，另有12项定向本地检查通过、0跳过。
第三包179份生产源码与EA字节一致，另1份为这两行候选校验；12个原生图／kernel符号、
4份原支持文件及剔除该校验方法后的模块AST保持。独立源码、计划与结果审查无待修项。

CPU2查询末尾CPU2已全额可用，仍有内部actor存活，`object_store_memory`可用额度比声明少34,297字节；
driver关闭后的17身份全部退出，不将查询末尾的CPU释放说成所有厂商资源已关闭。
本轮没有新PG载体、模型质量、性能或高并发结论；现有A2／B2／R1／blocks2、stage max_concurrency1
工作点及旧HTTP峰值4继续按原证据解释，模型冷／热配对由原模型owner负责。

<a id="native-completion-integration"></a>
### 公共计时整合后的定向验收

从`integration/baselines@58e1532888ad7da11bf99854803dac1d94a1c611`普通合入
`ed1867eb696848b68c571d77bd5caf07768ca86d`，只在生产适配层追加上述两行所选Ray Data检查。
现有默认关闭计时、runtime／EOF及图像错误优先级修补保持；不修改main或模型owner的运行源码。
整合版8模块Ran111，98通过／13缺实际Ray或未启用实库检查而跳过，0失败；覆盖低CPU启动拒绝、
Core／SemLoom CPU1、原图参数、EOF不可复用及计时开关契约。该本地组合与此前专项存在重叠，不相加。
独立读取三个源码包和三个结果包核对原摘要；各包181份清单逐SHA通过，第三包仅候选适配两行不同。
公开158成员的完整响应、身份与账本离线复算和机器记录逐字段一致，三份来源与7次尝试继续分列。
两轴独立审查无必修项；整合版未复跑服务器、模型或GPU，不将原`ea6c9184`及候选的实库资格改标为计时整合版。

<a id="native-http-activity"></a>
## 原生参数与HTTP活动核查

2026-10-14基于`integration/baselines@8862c813`补充[参数口径](../../../../code/src/baselines/README.md)：
Ray Data分别记录声明B、导入B、行数包络及活跃HTTP上界，实际峰值沿用统一代理的逐请求区间。
公共配置构造不再把Daft C≥2及Ray包络检查施加到IMLane／Ray Core；选中后端的额度检查保留。
原实现4组不合理拒绝已复现，修后IMLane／Ray Core C1、Daft C2及Ray Data C1的对应检查通过；
IMLane只核公共接入及作者调用桥替身，不增加作者实库C1资格。
受影响本地检查合计79项，74通过、5缺实际库或未启用实际图检查而跳过。
两项新增Ray批次循环检查单次28localhost POST，1／4／4峰值随批次数而非批内行数变化；
该循环为明确替身，不作为Ray actor调度实测。首次沙箱socket错误与随后获准检查分别保留。
[Daft独立检查](../../../../code/tests/baselines/test_native_daft_activity.py)新增双live loop客户端责任回归，
该本地阶段跳过实际Daft的24调用活动用例、Ray三种图配置及新进程导入检查；随后实库结果见下文。
本地阶段未占服务器／GPU或调用模型；Daft batch1／C−1、Ray官方图与各运行预算保持。
原件运行标识为`native-concurrency-local-20261014`，由本任务保管，可按该标识向项目维护者取得
`native-concurrency-local-20261014.tar.gz`；SHA-256为`62d1acead648b095018a043c388c3dcc09ca5bb9a6b619e0d0af4979a8d82668`。
存档的`checks.json`记录6份实现／测试源码摘要及逐成员字节／摘要：`shared-configuration-final.log`对应57通过／1跳过，
`http-and-lifecycle-final.log`对应16通过，`daft-ray-activity-final.log`对应1通过／4跳过；三组不重叠。
四份`http-fixtures/`记录28POST及峰值，首次socket错误、跨后端错误复现和两项活动重验的日志另列，重复运行不相加。

随后固定执行源码`81a38e522f7923ba1db5bcc2d1e9ac63e2d1f3ab`，运行上述5项实际库检查，全部通过、0跳过。
使用既有Python3.12.3、Daft0.7.21、Ray2.56.1、Arrow24.0.0及aiohttp3.14.3，无安装或环境修改。
新进程导入核对上游B默认4／环境覆盖2；实际Ray图导入B=2，max_concurrency=1、source_blocks=8、声明C=16。

| Ray actors A | batch_rows R | 行数包络 A×B×R | 活跃HTTP上界 A×B | 服务端实测峰值 | POST |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 2 | 2 | 2 | 32 |
| 1 | 4 | 8 | 2 | 1 | 32 |
| 2 | 4 | 16 | 4 | 2 | 32 |

本次固定图中增加批内行数没有同比增加活跃请求，也未使所有配置达到HTTP上界；保守行数容量检查保留。
Daft C4／batch1／原max_concurrency3完成24调用，20个200与4个429保持完整响应、重复请求头和独立行身份。
输入在迭代前全部准备，消费没有故意等待；服务端活动出现0／1／2／3／4，峰值4，仍有请求间空闲。
查询所持一个HTTP客户端池已关闭；峰值不代表持续供给饱和，也不据此定位作者调度限制。
上述峰值覆盖localhost handler开始至完整响应写完；Ray保存锁内计数，逐请求时间曲线及批次池关闭计数未采集，
不能把它们当统一代理区间、GPU并发或完整语义查询性能；取消／到期／SDK解析实库检查未扩为本轮资格。

运行标识`native-http-libraries-20261014-81a38e52`，03:06:12.839444 UTC开始，36.840秒内完成，实际120POST、模型／GPU0。
cgroup配额16核，执行固定8核、4GiB采样RSS上限；300秒保留为软件故障保护，不是服务器租用截止。
346份资源样本的RSS加总峰值3,913,281,536字节，含共享页面且可能漏短峰；未发现CPU亲和性超额。
独立核对26组PID／出生时间均已退出，Ray进程与共同lease为空，无强制清理或清理错误；输入及固定版本前后相同。
[机器事实与逐成员映射](native-http-library-verification.json)定位[脱敏原件](native-http-library-evidence.tar.gz)，
26成员、21,045字节、SHA-256 `9d04aa7d19a4ac01d70e61c279b50aab7b85bb1161bdabafb0c7d4ddff0132d0`。
配置、全部fixture、资源样本、用例日志与前后核查保留；批准输入包及23成员私有原件由维护者按运行标识提供，
其摘要和恢复来源均在同一机器事实中，不重复保存同份源码摘要清单。

<a id="duckdb-paths-20261013"></a>
## DuckDB六路径源码、实库与公开SQL检查

本次基于已发布`a0f3fbd393a6bf0879a2c0a973f727c737d00389`复用原DuckDB当前向量接点。
[所属矩阵与检查](../../../../code/integrations/duckdb_ai/README.md#六路径的当前支持与本次检查)登记六个实际方法入口，
三组专属源码／测试互斥推进；原生baseline不增加SemLoom策略或人为提高并发。
公共容量`9f8519d9`、未知工作复用保护`45899253`及完整结果数相等但无完整HTTP响应的保护`69f3e93b`独立发布。
Sema的HTTPX cookie`aaa88e28`、native4工厂白名单`3b465f3f`和运行时报告引用`cc8b09bc`按来源采用。

本地最后126项中94通过、32项明确需要实际编译库或runtime而跳过；公共容量47通过／1跳过，
公共传输保护37通过／18实际库条件跳过。服务器检查使用真实固定库、原SQL及确定性HTTP响应，模型／GPU调用均为0。
重新构建的`0.4.14-native4`二进制SHA256为
`65cd160849d93fba7108733fe7df7a765ecff522ea86bd5af96b5a68f54a76c0`，
DuckDB 1.5.4、Daft 0.7.21、Ray 2.56.1固定；作者IMLane来源`072f8257db80569e42c9f5727d839587df529652`，
使用既有Python 3.12.3／Arrow 16.1.0作者构建和容量桥，未安装依赖或下载模型。
固定上游四文件与补丁后摘要、编译身份保留；共享DuckDB SDK只读使用，前后状态相同。

| 实际路径 | 原SQL／适配检查 | 公开常驻CLI |
|---|---|---|
| DuckDB原生 | 原社区扩展与native4的原完整请求、默认执行与解析对照包含在下列SQL检查内 | 3／3查询通过 |
| 原生Daft | `native-daft04` 11／11通过，2147 POST | 3／3查询通过 |
| Ray Data | `native-raydata02` 10通过／1 fixture失败；`native-raydata-earliest03`仅原失败1项通过，合计11个唯一检查 | 3／3查询通过 |
| SemLoom | `semloom03` 4／4通过，37 POST | 3／3查询通过 |
| IMLane作者CPython | 正常3项＋传输错误／致命解析／取消各1项，6项通过；各异常组使用新Coordinator，2106 POST | 3／3查询通过 |
| IMLane作者Ray | 同样6项通过，独立作者Ray与新Coordinator，2105 POST | 3／3查询通过 |

六个CLI组各使用同一连接完成8、8、129行，18查询共870 POST；所有查询的owner与连接身份非空且保持，
完成序号为1／2／3，结果缓存关闭，变化输入、重复文本独立出现、UTF-8及原解析结果核对通过。
SemLoom执行身份与Ray会话、Ray Data会话保持；IMLane作者Ray身份归其调用者拥有的runtime，未冒用Ray Data身份。
Daft与Ray Data原SQL还覆盖2051行自然跨2048＋3向量、完整正文／状态、NULL、usage、长度截断、
429及非JSON、gzip／重定向原行为、准备失败合法前缀和最早输入错误。
失败SQL的usage记账数量或时刻存在原生与适配路径差异，原观察保留，不能只据最终异常相同宣称失败过程完全相同。

原完整请求准备仍位于批次接点之前；当前向量函数返回屏障、按输入序回填等待与有界乱序暂存仍存在。
既有查询Job跨向量复用测试使用编译扩展＋实际Core＋注入完成fixture，其层次与本次物理SDK检查分开。
本次未修改NativeCall的上游读取需求说明，也未把SemLoom调度引入原生baseline。
所有结果收到但传输没有完整远端响应仍持续标记未知；同对象及同作者Coordinator的新对象拒绝复用。
已知完整429不会被当作未知。只证明消费或本地池停止时，`remote_http_stopped`继续为`unavailable`；
只有fixture拥有者确认归零后才更换测试对象，生产对象没有额外恢复捷径。

失败原件保留：初始LOAD遇旧运行时C++库ABI错误，使用既有编译器库解决；Daft初轮1项断言误读通用C ABI SQL错误，
改为核对桥的原异常后完整复验；Ray Data首个本地地址与SDK规范地址不同，固定SDK节点地址后启动。
Ray Data最早错误fixture原先要求并行调度，现保留第一行原429并记录实际响应顺序，单项复验通过。
SemLoom初轮因验证入口直接初始化Daft后公共manager再次初始化报`MODEL_UNAVAILABLE`，
改用既有`configure_daft_runner`单一拥有者后4项通过，线程数仍8；合法响应窗口增加24字节不是该错误的根因。
作者Ray初轮私有临时父目录缺失、0 POST，修私有harness后通过；06:35截止前未启动的准备也保留为未运行。
这些修订不重置累计预算，也不把不同源码的Ray Data 10＋1写成新完整套件11／11。

授权窗口05:18–06:18 UTC、接续至06:35共用20,000 POST额度；最后独立限定复验实际从
06:42:56.9878168开始，只运行原Ray Data失败1项和SemLoom 4项，分别8与37 POST，45／50。
累计11,608／20,000 fixture POST包含失败；CPU 0–7、私有端口和本次进程于
06:46:59.154913 UTC核对无残留／无监听，早于06:47截止。未改其他路径的runtime、SSH master或公共主机策略。

逐次来源、状态与计数见[机器事实](duckdb_paths_20261013.json)。私有完整证据包`duckdb-paths-evidence-final.tar.gz`
共852成员、1,653,762字节，SHA256
`c13792dfb6de54befd9a87791294a08361e7292ceea523d93853ac168cbb656d`；
按交接校验值获取，运行成员为`artifacts/<运行标识>/`，成功、失败、原源码、日志和清理记录均保留。
Git仅保存脱敏事实，不公开私有runtime配置、地址或凭据。
本次建立当前向量的六路径软件接入资格；真实模型质量／GPU与匹配性能、跨向量流水、级联／Join、
作者并发查询公平性及未知远端工作独立取消仍未建立，不借下方旧LOTUS成绩作新基线。

<a id="imlane-native-async-followup"></a>
## 共享cookie修后两作者async与完整Q1检查

固定源码`77cc1ed0f2fcd74c84f76752315ddc30798b6441`基于发布者
`d004e720239b7902a1b3c5b735eb6f5fa8ad7d33`；采用其6629846a共享cookie和只读SemBench源码路径修补。
五个async实现/测试文件与已独立复审的a54e1456逐字一致，作者源码/调度及同步默认保持。
旧[153 POST失败记录](#imlane-native-async-http)与其原件完整保留；本次两个窗口单独记账，未续旧时间窗。
本机67项有限回归通过、无跳过、实际HTTP/模型/GPU0；本机Arrow/session替身不代替下面的实际库证据。

| 新软件窗口 | 作者模式与实际POST | 总POST额度 | 外层实际启动至清理完成 |
|---|---|---:|---:|
| 生命周期 | CPython174，作者Ray173，两模式通过 | 347/384 | 53.543/600秒 |
| 原Q1完整2000行 | CPython2000，作者Ray2000，两模式通过 | 4000/4096 | 90.019/900秒 |

两窗口分别从UTC`2026-10-12T15:35:56.868232`和`15:38:54.208393`实际启动，
停止新运行的期限分别为`15:45:56.868232`、`15:53:54.208393`。
新窗口合计4347 POST、2 GET，模型/GPU0；旧153 POST另册，总async历史4500 POST，不挪用已封存容量预算。
启动前的CPU忙度检查失败记录保留，未产生作者窗口；最终样本满足同一低于10%的空闲条件。
仅使用既有作者库/桥，没有安装、编译、重建或访问真实模型。

生命周期覆盖两模式正常8/8/129行、重复文本的独立出现身份、乱序完整响应和慢消费，
401/429、重复响应头、gzip原字节、非JSON及length结果，307/302、独立与并发cookie，
超时、容量执行错误、同Coordinator新对象拒绝、已发送取消/提前关闭及结束自有实例后的下一正常查询。
正常145个出现身份在每模式两名作者worker上保持各自线程/循环；Ray核对实际两actor身份。
取消或错误时按原记录保持远端工作unavailable和需清理状态，没有因cookie修补清空未知责任。

完整Q1从真实`MovieQueryResult.execute`进入，经配置加载、MovieQuery容量检查、公共
`NativeCallExecutor`工厂和实际`IMLaneCallExecutor`/`SemaIMLaneExecutor`；声明与实际行容量均为2000。
每模式一个2000行DBEnd查询批次，全部2000个调用身份及完整状态/响应头/HTTP版本/正文摘要匹配后，
原LOTUS1.2.4 parser和查询才各返回5条结果；没有截断评论或提前取5条。
两模式各2000个trace核对执行中的任务/共享HTTP源码、稳定线程/循环与Ray actor；未每128行重启。
payload仍为1,500,488字节、最大902字节，摘要`ec6948a91625073ac3d85e901dd51f823b4d0b164415e68a66de8737f8448fd6`；
规范化原endpoint后的wire为2,649,072字节，摘要`d85a25ca894b6f89ffbd471f7fca8edcbb992a7b4d0123a7ac748994a45f4125`。
实际fixture stdin每模式2,657,072字节，交付编码响应1,108,000字节；实际待交付峰值128。
该128是槽位额度；原生META4/6及声明请求额度8不代表物理worker或HTTP峰值，本次HTTP峰值观察为2。
payload/响应32MiB等编码容量仍不等于聚合RSS保证。

最终独立核查143组PID/start ticks与10个实例：自有及同运行token进程、共享内存均为0，
HTTP连接关闭，没有强制清理或清理错误；21个源码、12个作者源码及3个作者二进制摘要未变，SSH master保持。
原SemBench Git命令保留macOS `._pack`索引告警；命令返回码、checkout提交及已提交查询源码摘要检查通过，未改原checkout。
本地证据导出路径拼接错误已修正，生产源码和实库运行保持；原件可按交接中的校验值获取。

机器事实见[imlane_async_followup_verification.json](imlane_async_followup_verification.json)，
96成员观测见[imlane_async_followup_observations.tar.gz](imlane_async_followup_observations.tar.gz)，
SHA256为`195f21817b8e3bffd3922c2c20aa79458f4002814cd4262239848e65a48473ef`。
保留一次完整Q1 wire，两个模式及冗余输入副本以原件摘要映射；私有runtime配置与无独立核验价值的调试副本留在原件。
可用[离线复算](recompute_imlane_async.py)核对POST、完整payload/wire、两模式身份/循环和清理，不发HTTP。

本次是实际库＋localhost语义响应fixture的公开查询API软件检查。
完整CLI常驻owner/观测代理、官方Movie质量评价、新async的Q2/Q3与单独128行批次、4096行全批及共享Coordinator并行查询隔离未运行。
旧同步128行容量证据仍保留；真实模型/GPU、质量、连接复用、匹配性能与RSS结论未建立。
所需接续仅为发布者整合固定源码与证据；完整CLI/评价或Q2/Q3若需要，另外登记有限软件预算。

<a id="imlane-async-publisher-integration"></a>
### 发布者整合与本地组合核验

交付05758e3f接到已发布0f91ae1b，五个async实现/测试文件逐字保持；共享HTTP、公开CLI及原作者机制未改。
同步默认保持，新Coordinator配置显式选择async UDF并核对源码摘要；已发布四非IMLane路径的第三窗口原件与评价不改。
两轴有限复审必修/缺失证据均0；离线复算确认347＋4000 POST及143组身份清理，未重跑已合格作者组件矩阵。

本地组合95个独立用例最终通过、0跳过：首轮88通过、7项被沙箱拒绝localhost bind，失败原件保留；
只对这7项用本机临时fixture重验，7通过、110/128发送前POST、7.525/60秒，无远端服务器/模型/GPU。
取消用例的BrokenPipe线程日志保留；现有停止转发、资源与线程清理断言通过，未据此扩大远端停止结论。
本机Arrow替身与实际作者实库观测分开；135个归档成员及2152个解码请求均隐私0违规。

原交付事实和追加发布者核验分别在同一[机器记录](imlane_async_followup_verification.json)保存。
剩余[公开CLI小窗口](../../../plans/语义系统对照.md#sembench-async-public-entry-next)与
[同步优先的单/双卡模型提案](../../../plans/语义系统对照.md#sembench-native-model-next)以本次整合源码为共同起点；
前者未启动、各自需新预算，后者未运行模型，不等可选async的补充结果。

<a id="imlane-native-async-http"></a>
## IMLane原生async HTTP实库失败记录

以下保留旧a54e1456失败运行的状态与计划；当前资格以[修后接续](#imlane-native-async-followup)为准。

固定源码`a54e1456f1eafa88c202d9042b03cbe4831faf24`基于发布者
`4566f1fc3839cb6149ebb1cf7d76a64c3d4cf04a`，保留已发布容量、未知责任保护和LOTUS观测实现。
同一可选任务直接`await _request(call)`，CPython/Ray各使用作者原async分支；每调用session继续由既有kernel关闭。
同步默认、共享`native_calls.py`和作者源码未修改。源码核对及本机65项有限回归通过、无跳过，
本机Arrow/session使用明确替身、实际HTTP/模型/GPU0；独立规范与需求复审均无源码必修项。
这些检查不代表实库HTTP生命周期通过。

实际作者CPython在同一Coordinator完成8、8、129行正常批次，以及6项完整HTTP值检查：
401/429状态、重复响应头、gzip原字节、非JSON正文和length结果均保留。
152个任务trace记录145个正常出现身份及后续HTTP调用；离线核对两名作者worker各保持同一线程和循环，
执行中的共享HTTP代码与交付源码匹配。后续完整`run_mode`检查未到达，不将部分观测写成整模式通过。

首个307请求收到`Set-Cookie: route_cookie=first; Path=/`，同一连接的后续POST却没有cookie，
脚本在预期`route_cookie=first`处出现`AssertionError`。原报告没有断言文本或traceback；
定位依据是固定脚本和末批请求/响应记录。安装的aiohttp3.14.5使用
`ClientSession._cookie_jar.update_cookies_from_headers`更新响应cookie；共享`CallCookieJar`
只转发`update_cookies`，继承的`DummyCookieJar.update_cookies_from_headers`为空操作。
实际库源码与原始收发记录共同支持这个兼容缺陷，不把问题归因作者async能力或行容量。
本任务按已指定范围保留共享源码，未在async函数内添加绕过办法。

| 本次软件核验 | 状态 | 实际POST | 时间 |
|---|---|---:|---:|
| CPython生命周期 | 307 cookie检查失败，模式未通过 | 153/384 | 4.531/600秒（外层从实际启动计时） |
| 作者Ray生命周期 | 未启动 | 0 | 未启动 |
| 两模式各完整2000行原Q1 | 生命周期未通过，未启动 | 0/4096 | 独立900秒预算未启动 |

生命周期实际启动UTC为`2026-10-12T14:11:52.446759`，停止新运行的期限为
`2026-10-12T14:21:52.446759`，原时间窗已结束；没有重跑、重置或借用旧容量预算。
GET0、模型/GPU0；只链接既有作者库和容量桥，未编译、重建或安装。
正常响应后HTTP峰值观察为2，结束时活动请求0、连接已关闭；此值只描述本次fixture。
监督记录10组自有PID/start ticks，另复核外层启动身份；最终自有进程、同运行token进程和共享内存均为0，
没有强制清理或清理错误，作者源码/二进制及21个交付文件摘要未变，既有SSH master保持。
期限之后只读收集和复核没有作者执行、HTTP或模型/GPU活动。

机器事实与未运行清单见[imlane_async_verification.json](imlane_async_verification.json)；
39成员脱敏观测见[imlane_async_observations.tar.gz](imlane_async_observations.tar.gz)，包含实际请求/响应、
worker trace、各批关闭、清理、源码清单、实际cookie接口和本机/复审记录。
归档SHA256为`0542c42942aaa3601631da91b498aa8af117aa308c20c530607cd1469e96912c`。
私有原件摘要及逐成员公开映射保留；实际runtime配置不进入公开归档，原件由交接artifact获取。

剩余CPython重定向/独立/并发cookie、超时、错误取消和下一查询，以及全部作者Ray生命周期均待验证。
完整Q1候选已准备真实配置加载、MovieQuery容量检查、公共工厂及`MovieQueryResult.execute`入口，
当前没有实库通过证据；完整CLI常驻owner/观测代理、官方质量评价与Q2/Q3也未运行。
最小接续由共享HTTP源码责任方补真实raw-header更新的逐调用转发，核验已安装aiohttp和受影响旧调用方，
保留本次失败；再登记新固定源码与运行额度，先完成两作者模式生命周期，之后才启动原Q1的4000次完整调用。
没有连接复用、真实语义质量、RSS或性能收益结论。


<a id="imlane-2000-capacity"></a>
## IMLane原Q1完整2000行软件检查

备份补丁SHA256为`01be5a5627b3165f4f486e17c249e4186c22d2c5bcd21647e9625a54023632ad`，七文件原样提交
`2cca502df43e1401284b33ac14862ba945c7c55d`；在已发布`1e96935610069d17430c6fb75fe37a75659aa84e`
上只cherry-pick这七文件得到`7bb10980032e984f8f60ccaed4732c39bc168a1c`，逐文件摘要相同。
现有作者源码仍为`072f8257db80569e42c9f5727d839587df529652`；仅链接既有patched库，不重建作者系统。
桥C++源码SHA256为`1e27ee6e7d7a370c0235582564c3941bdbeb15a14259a4e608a03c8b7f1e7984`，
二进制SHA256为`b201be8891493a2885ff9faae6d755bfe1eb7c20e10bdb2cda3f5f25c56cf5c6`，编译10.927/120秒。

公共IMLane、Sema DBEnd及C++共用`IMLaneBatchLimits`：本次全批2000行、payload32MiB、
wire48MiB（UTF-8 JSON lines含换行）、单项payload8MiB、待交付128项、累计响应32MiB；
Q1单项完整HTTP响应额度1MiB。每查询一个DBEnd上下文，在交付后补入下一调用并释放该输入/输出引用。
作者Lane、Coordinator、CPython与作者Ray继续拥有执行；没有每128行重建进程或整组完成后才继续供给。
这些编码字节额度不保证聚合RSS。接口与旧128来源见[IMLane说明](../../../../code/integrations/sema/imlane_calls.md)。

原SemBench Q1全量2000调用的payload为1,500,488字节，原stdin为2,649,072字节，最大payload902字节、
最大原wire行1527字节。实际fixture端口使stdin为2,657,072字节；复原原endpoint后wire摘要
`d85a25ca894b6f89ffbd471f7fca8edcbb992a7b4d0123a7ac748994a45f4125`完全匹配，payload摘要
`ec6948a91625073ac3d85e901dd51f823b4d0b164415e68a66de8737f8448fd6`也完全匹配。
CPython和作者Ray各只有一份2000行DBEnd输入，2000个独立出现身份均收到完整响应；
原LOTUS1.2.4 `sem_filter`完成全部消费后，原Q1才返回5行。没有截断原评论或提前选5条。

两模式通过128/129行、相同完整请求的独立身份、乱序、慢消费、2001行及单项/累计payload/wire超限拒绝。
两项待交付额度的实际观察证明第3条已到达HTTP时，慢第1条仍未完成；作者线程/META保持。
256字节响应额度实际触发作者UDF ValueError并恢复原诊断；已发送取消保留未知远端状态和uncertain记录，
同实例的新对象拒绝提交。核清并结束自有Coordinator/Ray后，用新实例验证下一正常查询。
`1e969356`的首错/notes/cookie修复未与七文件重叠；组合源码在两模式另以12次POST复验错误、取消及下一查询。

| 软件运行 | 本次服务器POST | 累计POST | 从首次启动至该次结束（秒） | 结果 |
|---|---:|---:|---:|---|
| 首次 | 2259 | 2259 | 46.793 | CPython完整Q1通过；持续供给观察的脚本断言失败 |
| 第二次 | 9 | 2268 | 214.875 | CPython持续供给通过；32字节诊断预期断言失败 |
| 第三次 | 2279 | 4547 | 475.878 | 两模式全部目标检查通过，已完成的CPython Q1未重跑 |
| 组合源码补测 | 12 | 4559 | 783.993 | 两模式错误/取消/下一查询通过，完整Q1未重跑 |

全部服务器操作使用CPU16–23。首次启动11:01:41.629 UTC，原900秒截止11:16:41.629 UTC始终保持；
最后一次已于首次启动后783.993秒结束。本机原容量/协议/分类13项及组合24项通过、无跳过；
组合keepalive产生4次POST（本机控制台最初的0注记已更正）。服务器4559＋本机4＝4563/5000，模型/GPU0。
最终独立核查173组PID/start ticks及14个实例，自有存活进程和共享内存为0；三个既有作者二进制摘要均未变，SSH master保持。

保留编译监督报告缺失：监督脚本将自身timeout父进程纳入清理，编译本身退出0；独立资源复核通过，
脚本在作者测试前修正，未重跑编译。首轮HTTP到达时刻受作者队列影响，不能代替桥补入时刻；
两项额度用例另外实际检查持续供给。32字节用例也限制错误诊断，C++交付检查正确返回ValueError，
脚本却要求UDF错误字段；保留该失败，再以能承接诊断的256字节额度核验原作者错误类别和内容。
生产代码和原Q1未为通过这些检查改写。

机器事实见[imlane_capacity_verification.json](imlane_capacity_verification.json)，84成员脱敏观测见
[imlane_capacity_observations.tar.gz](imlane_capacity_observations.tar.gz)，可用[离线复算](recompute_imlane_capacity.py)核对全部POST、
4000个Q1身份、持续供给、失败来源与清理。原件和旧前置拒绝保持原保存位置。
该独立交付当时仍有MovieQuery的128行预检；发布者随后贯通显式容量并补未知责任保护，见[发布接续](#imlane-publisher-integration)。
未运行真实模型/GPU、质量或性能比较、DuckDB/Sema2000行、全部4096行与共享Coordinator查询隔离。
完整Q1实库检查属于2cca502d；组合七文件相同，组合异常路径已核，不称完整Q1在组合源码重跑。

<a id="imlane-publisher-integration"></a>
## 扩容发布接续与未知责任保护

发布者在1e969356后整合容量7bb10980和证据5820dc37，归档摘要及离线复算通过，旧失败与两项旧Q1前置拒绝保持。
新`IMLaneCallConfig.max_batch_rows`省略时128；明确设置2000并选择已验证新桥时，MovieQuery、数据／运行时预检、
公共工厂和实际IMLane执行器使用同一容量。Q1仍以原完整2000行交付，Q2/Q3仍由原taken_3选择，不改Filter、LIMIT、COUNT或原评价。
2001行、错误模式、非法容量及旧128配置明确拒绝；预检在模型加载、预算分配及执行器创建之前检查行数。
二进制、UDF、Coordinator和namespace继续按原机制核验；须建立新拥有的配置和进程身份，已结束的实验配置不能复用。

配置文件在仓库外，保留已有runtime、bridge_config_path、coordinator_pid、coordinator_start_ticks、author_build_identity及n_executors，
追加`"max_batch_rows": 2000`。桥配置必须指向已验证二进制及其SHA256 `b201be8891493a2885ff9faae6d755bfe1eb7c20e10bdb2cda3f5f25c56cf5c6`；
当前没有活跃作者配置，此处交付代码与配置接法，不声称已经重启服务或重新执行完整CLI矩阵。

用户提供的源码合成反例指出：returncode为空可能是桥尚未创建，也可能是已创建但停止未确认。
底层现在明确记录bridge_started／author_submission_possible，公共包装按启动事实保留未知责任，创建前失败不写作者未确认标记。
正常退出、非零退出、已创建却没有退出码的四态回归，以及同Coordinator新对象拒绝／原始原因和清理注记均通过。
附件只是合成反例，未据此认定历史服务器发生泄漏。

本次本机119项中105通过、14因缺LOTUS／OpenAI／LiteLLM依赖跳过，0失败／错误；30项目标检查全部通过、0跳过。
新服务器／POST／模型／GPU运行0，没有重复已合格大矩阵或使用已结束的900秒时间窗。
C++和作者HTTP函数保持task-10的已验证摘要；新配置／保护的本机来源及命令见[发布核验](sembench_imlane_integration_verification.json)，
输出见[本机回归](sembench_imlane_integration_tests.txt)。当前源码尚未在实际作者环境重新运行完整Q1，实际2000行资格保持2cca502d来源。
固定源码候选59b86fd2的独立标准与需求复审均无代码必修；标准审查指出的两处旧文档状态已修正并只读复核，剩余必修0。

性能接法独立推进：IMLane逐调用事件循环／session成本、LOTUS原生额外prepare_call观测、Daft／Ray真实池寿命仍需各自检查。
Sema当前是已抵达快照上的执行服务，前快照消费结束后才交出下一份；这一等待须计入适配成本，不表示厂商完整持续执行能力。

<a id="native-release-composition"></a>
## 发布组合与本机回归

本节保存截至1e969356的发布组合事实；后续扩容资格见上述独立记录，原Q1前置拒绝仍保留。

2026-10-12恢复本机执行后，以已发布`8fc28d97`在独立检出继续现有native分支。
独立数据集配置测试`e51f4206`已整合为`6faa05ea`，12项本机回归通过、0跳过并已推送核验。
前四块`c47304d1/c8484a2b/47adb0f1/c4f7f157`先与SemBench组合，14个Python源／测试文件逐项匹配固定候选；
首轮本机60项中49通过、11因缺LOTUS依赖跳过，已独立发布并核验远端`9c1e1f22`。
前四块规范与需求独立复审各0项必修；组合保留准备／响应回调、重复行身份、首错和附加清理错误。
随后整合`5e0390ec/97af50c5`两块，规范复审发现1项事实同步，需求复审发现独立HTTP关闭可能覆盖首错和请求内cookie重定向行为改变两项必修。
已保留原异常及脱敏的附加清理诊断，并让每条完整请求独享默认cookie jar；连接客户端仍复用，同一请求重定向保留cookie，并行请求不互相继承。
数据集测试辅助参数随Ray请求数量核查调整为有效声明，并使用已进入的processor session替身，原429／transport／行身份检查保持。
修前16项复现为2失败／4错误，原输出保留；修后100项中86通过、14因缺LOTUS／OpenAI／LiteLLM依赖跳过，0失败／错误。
14项跳过未取得本机验证资格；实际厂商图、keepalive HTTP fixture及作者runtime未在本次运行。
修后规范与需求分别复核，剩余必修各0；新增源码和最终回归摘要逐项一致，隐私／新增链接／文字／diff检查通过。
已保存实际SDK验证仍属`c4f7f157`：20通过、0跳过、12localhost POST、模型/GPU0，源包摘要核验通过。
本次本机新服务器、HTTP POST、模型及GPU运行均0；不占用服务器CPU16–23。
保存的`97af50c5`桥编译退出0，源包与该发布组合C++逐字匹配，源码SHA256及已记录的二进制SHA256进入机器事实。
作者系统未重新构建，新桥二进制未用于作者runtime或2000行检查；这些资格仍pending。
原Q1 2000行的两条IMLane适配器前置拒绝及历史记录保留，当前128行是适配器接受范围。
机器事实见[integration_verification.json](integration_verification.json)，首四块输出见[integration_first4.txt](integration_first4.txt)，修前复现见[integration_late_before.txt](integration_late_before.txt)，最终输出见[integration_final.txt](integration_final.txt)。

<a id="lotus-text-sdk-preflight"></a>
## 集成文本整批预检的实际SDK资格

执行源码固定为`1afa8e325278077a846180997e2db75275631a89`；406个公开源码／检查文件在服务器逐SHA核验，
验证夹具单独记录。复用LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0、pandas2.3.3和既有driver，未安装依赖。
六项最小受影响检查全部通过，0错误／失败／跳过，实际13个localhost POST，模型／GPU0：

- 第五条单请求超限、第五条累计输入超限和固定模型不符，各在本地及typed session_factory入口检查；零flow、零发送、Core用量归零。
- 原SDK三条文本与默认分块／显式预检／factory三组各三条配对，完整请求正文、重复值、Unicode、输出和usage保持，共12POST；原SDK正文／logprobs检查另1POST，代理码点拒绝0POST。

factory使用同进程的实际Core，仅验证工厂接口与typed容量，未建立远程Core／IPC资格；未运行图文profile、Daft/Ray图、模型质量或性能比较。
fixture模型没有定价，不据此报告实际成本。首次自动双卡profile要求10CPU，而进程固定CPU24–27共4槽，
preflight因此失败且SDK未启动、POST0；保留该15.442秒失败。改用仓库已有4槽`generic_linux_nvidia`显式profile后依赖检查通过，
原北京时间19:39:30.441起、19:44:30.441截止与300秒／32POST额度保持，接续没有重置时钟或退款。
窗口含首次失败与接续等待共215.242秒；独立检查在19:44:29.509完成，距原T0为299.068秒。
7个PID＋birth无存活、共同host lease不存在、GPU空，持久发送与fixture收到均13；采样RSS加总峰值1079078912字节，低于4GiB上限。
该加总包含共享页面且可能漏采短峰，不能解释为唯一物理内存。未重复旧SDK全套或扩大性能矩阵。

[资格聚合与来源](lotus-text-preflight-sdk-verification.json)及[紧凑原件](lotus-text-preflight-sdk-evidence.tar.gz)
保存全部用例、逐发送／收取、资源样本、显式profile原因、首次失败和独立释放记录；归档42,124字节，
SHA-256为`2c53f03e73ba74b1a0428970c5adc6dd12c9d379ba6633da9ad16e8d0e11cd6f`。
原始私有归档另保留，其SHA及成员脱敏映射在上述证据中；历史SDK／真实模型结果按原源码与计时继续解释。

<a id="execution-repairs"></a>
## 响应与生命周期修补

2026-10-12从`7829bb45`在独立worktree修补原生消费线程的提前关闭诊断。
线程停止/join后记录执行及关闭错误，附加关闭异常对象保存在`last_cleanup_errors`；
查询第一异常保留，显式close没有第一异常时传播关闭失败。
5项本机真实消费循环／图替身回归通过，HTTP／模型0；原图、远端不确定状态和旧模型原值保持。
共享SDK解释已接入五条替换批次路径，原LOTUS保持；11项Core/准备/取消控制测试与2项共享入口检查通过。
固定SDK响应差分2项及既有SDK清理11项因本机缺依赖跳过；没有安装或下载。
IMLane容量/身份和作者UDF异常另以执行错误结构化返回，正常传输错误保持；5项Arrow/kernel替身分类回归通过。
其中ValueError不会进入SDK连接错误解释，原类别/首因诊断保留；尚未运行真实作者过程或重新构建。
实际Daft／Ray异常路径、固定SDK资格、客户端复用及共同容量仍待分别核查，不称完整公平等价。
共享解释继续改为每批次惰性创建一个SDK客户端，五条替换批次复用同一机制；
不再逐响应重建客户端，仍调用固定SDK转换，重放没有实际HTTP请求。
11项准备/清理控制检查及3项入口/关闭检查通过，3项实际SDK检查本机缺依赖跳过；
新增实际SDK用例覆盖错误后继续解释、不同输出及重复值、客户端身份复用和释放。
指定服务器CPU16–23第一次只读采样最高忙10%，因此该次没有启动验证；后续先重新检查空闲状态。
用户批准具体服务器源码上传后，固定`c4f7f15742d3473274b75c42556c2d75be423060`
通过20项真实SDK/清理检查、无跳过，12次localhost POST、模型/GPU0，15.739秒；
fixture服务器关闭、自有进程观察为0。来源摘要见[修补核验](repair_verification.json)。
实际Daft0.7.21零HTTP探针4行证明查询EOF后异步loop仍运行，支持检查query/loop持有连接。
原生HTTP kernel现按查询及其Daft loop持有客户端；Ray Data遵循原HTTP stage的session_factory批次寿命。
连接器不增加请求准入额度；每次请求保留自己的期限，cookie只在同一请求及其重定向中沿用，不跨独立调用。
6项生命周期检查通过，其中4次真实localhost调用使用同一TCP连接，含不同输出和重复输入；
5项提前关闭回归及2项Ray参数替身检查通过。实际新池的厂商图完整消费/取消资格仍待有限复验。
Ray默认batch_rows=1时每HTTP stage批次只有一个请求，不声称跨批次或跨查询池复用。
报告同时保存声明C与actors/async_batches/batch_rows/source_blocks；C不等于实际请求峰值。
Ray配置上限按actors×async_batches×batch_rows核查，实际执行和供给继续由原native图拥有。
共同`LotusCallLimits`明确请求/响应字节、批次行数/累计字节和request/batch期限；
native LOTUS完整响应默认从8MiB改为1MiB，与普通Engine接收范围对应。
6项本机检查通过，包含IMLane式2000行目标准备前失败、小Engine显式profile失败、最终SDK解释超过期限；
保留SemLoom有限准备及各native图的物理准备，配置相同不表示所有内部时序或实际并发相同。
IMLane128来源核到项目`5ee1b90c3`的C++快照检查及Python检查，不是作者系统自然容量。
桥源码完整stdout输出后释放完成调用`calls[i]`持有的输入/输出；保存的新桥编译成功且源码摘要匹配，作者runtime尚未运行。
32MiB只限定当前声明的输入/已交付结果编码量，不保证C++待交付输出、Arrow/IPC复制及聚合RSS。
该历史128行桥的512/2000目标未支持；新增完整2000行软件资格见上述独立记录，作者并发保持。

本轮在独立分支`codex/native-execution-adapters`整合原LOTUS、DuckDB AI与Sema的方法接点。
旧主及三个原子任务工作树保持只读，原始成功／失败与私有环境继续保留。
执行目标与范围见[方案](../../../plans/语义系统对照.md#native-executor-adapters)，
紧凑事实见[verification.json](verification.json)。本轮软件调用只访问localhost响应fixture；
单GPU短验证另列，不把软件fixture当模型质量或性能结果。

## 方法与执行身份

| 方法 | Daft | Ray Data | IMLane CPython／作者Ray |
|---|---|---|---|
| LOTUS | 原LM未缓存完整批次→Daft Native→原SDK／parser | 同方法，Ray Data HTTP Processor拥有执行图 | 同方法，DBEnd／作者Coordinator拥有执行，两个模式分别登记 |
| DuckDB AI | 当前SQL向量→Daft→原C++ FinishCompletion | 同一完整批次ABI与原解析 | `native3`明确登记`imlane`；两个作者模式使用同一ABI |
| Sema | 原SQL／请求池之后的有限HTTP快照 | 同一后置服务与原作者解析 | 保持原SQL／请求池，两个作者模式显式核验PID与二进制 |

各方法保留自己的提示、生成参数、解析、调用出现身份和完整响应。
Sema的HTTP序号只表示抵达服务的请求，不代表数据库源行；原池前就绪仍不可观测。
Ray Core静态actor拓扑继续作为`diagnostic_reference`单列；IMLane作者Ray使用作者执行器，名称与Ray Data分开。
配置与完整调用说明见[IMLane接口](../../../../code/integrations/sema/imlane_calls.md)，
共同单查询／常驻入口见[CLI说明](../../../../code/scripts/README.md#native-adapter-query)。
IMLane默认128行；显式2000行新桥已取得上述软件资格，超额拒绝；聚合RSS仍没有32MiB保证。
作者IPC的Job ID仍为空，跨查询公平、独立取消、任意UDF异常及全部传输异常类型一致性尚未证明。

## 软件与构建事实

- 原LOTUS／网关交付`f9921475`：本机和服务器各45项检查通过，完整字节、重复头、压缩、状态、缓存及SDK回送按对应来源保留。
- 原DuckDB交付`f4de379a`／`5e95852d`：50项旧路径、19项转换与ABI检查，三个后端真实SQL代表用例；失败轮及首次输入错误修正保留。
- 作者IMLane构建交付`fdd6a227`：两个模式630行／132次Arrow调用，来源与未检查项见[构建记录](../../diagnostics/imlane_build_20261012/README.md)。
- 共同四条LOTUS路径、错误观察和混合owner检查先完成38次软件POST。
- Daft同driver反例可复现`Cannot set runner more than once`。中立初始化入口修复后，实际Daft物化及原LOTUS／Daft／Ray Data／SemLoom同driver组合通过，整组54次软件POST，模型0。
- 新公共IMLane联调两模式各完成LOTUS／DuckDB／Sema四行完整消费，并各完成两次LOTUS常驻查询。成功轮48次软件POST，43.808秒，模型／GPU／重试0。

IMLane两个模式均使用CPU72–79、声明2个作者执行器；空stdin探得settings执行器4、任务线程6，配置并发额度8。
实际各触及2个worker，Ray记录2个非空actor ID；Coordinator及独立Ray正常退出码0、未强制，已观测自有存活进程及共享内存0。
DuckDB只增加`imlane`名称选择，匹配二进制为`0.4.14-native3`，摘要见紧凑事实；固定1.5.4头文件下构建约34.851秒。
原作者构建、桥、kernel和旧DuckDB构建保持原件。

失败轮完整保留：首轮21次POST，Sema错误使快照结束未确认，后续复用正确被拒绝；
第二／三轮11／14次POST因检查程序重复使用保护文件名而停止，改为独立记录后成功轮48次。
累计94次软件POST，小于本次128上限。归档解包首次拒绝Ray绝对符号链接，保留原包并只恢复普通文件／目录，没有重跑fixture。
首次DuckDB构建前版本检查失败，核对旧build的固定头文件路径后在新build目录完成，原失败保留。
本机初轮127项只有一个旧错误文字期待失败，24项未启用；修正后受影响5项通过。修后完整127项通过，24项跳过。
固定稳定性`9bb6c415`与原生工作源码在独立副本组合，247项通过、29项跳过；
首轮仅测试辅助导入路径错误，原输出保留，修运行路径后完成全组。
同一组合源码再完成三方法／常驻的双mode32次真实软件POST，37.327秒，两个Coordinator／Ray正常退出且自有存活／共享内存0。
组合保留稳定性全部实现摘要，原生新增文件逐项匹配；本次相关软件累计126次，小于128上限。

## 较早源码的单GPU失败轮

固定较早源码下启动同一Qwen2.5-7B服务，原LOTUS、Daft、Ray Data各完成一条8行warmup，共24次真实POST。
SemLoom在HTTP之前的Daft重复初始化失败，测量查询0；该反例随后由软件修复核对，该轮修后未重跑模型。
已占用32个模型名额，实际POST24；启动期限到达后服务退出，无自有存活或GPU计算进程。
该轮24次实际POST／32个占用名额独立保留，不归入下述新一轮，也不退还失败查询的名额。

<a id="lotus-six-model"></a>
## 六条LOTUS路径的单GPU真实模型检查

新独立授权轮`lotus-six-model01/attempt02`全部通过：原LOTUS、Daft、Ray Data、SemLoom、
IMLane CPython及IMLane作者Ray各执行一条8行预热和两条8行测量，合计18条查询／144次真实POST。
网关、SQLite请求记账和vLLM访问日志均为144，已结束记账单元18个；错误、超时及重试均为0。
本轮最多192名额，占用144，剩48未使用；独立软件检查额度64没有消耗。
此前24实际POST／32占用名额保留，因此两轮累计实际168、占用176，没有合并统计样本。
测试driver耗时76.203秒，supervisor覆盖driver启动及退出的区间79.361秒，第二次模型就绪48.099秒。

执行源码是本地组合提交`9c2fdefa`，父提交为原生`fd8c00bc`与稳定性`9bb6c415`；
生产文件与已回归组合逐项一致。使用同一物理RTX4090 GPU0、单服务、张量并行1、流水线并行1，
缓存Qwen2.5-7B-Instruct的全部权重／tokenizer文件逐项SHA256固定。
vLLM0.25.1、BF16、context4096、max-num-seqs16、batched-token2048、显存比例0.8，
eager执行，前缀缓存关闭。生成参数为16输出token上限、temperature0、top_p1；完整请求、最终LOTUS parser相同，正常返回／消费通过，中间响应解释仍有差异。
六条路径的driver均限CPU72–79，模型服务限CPU80–87；执行图配置并发额度8、线程8，实际并行与首次图成本分别保留。

输入是[完整记录](model_source.json)中的8条人工构造文本，包含重复出现、换行、中文与标点。
它不是SQuAD或SemBench测试集；要求返回`ok`只检查完整调用、原解析、行关联与消费。
全部18查询的请求值多重集合一致，每次消费8行且原解析结果为8个`ok`；重复文本仍保持两个独立行ID。
这支持本次真实模型接线，不支持任务质量、服务饱和或总体性能排名。

下表查询时间从每次driver入口到原parser、结果消费、记录EOF及该查询清理完成；可复用group／作者进程启动另列。
每条路径测量查询样本数2，请求样本数16；预热单列，不进入测量统计。单位秒。

| 路径 | 预热查询 | 测量查询1 | 测量查询2 | 查询P50 | 查询P95／P99／max |
|---|---:|---:|---:|---:|---:|
| 原LOTUS | 0.685973 | 0.254204 | 0.213361 | 0.213361 | 0.254204 |
| Daft | 3.425198 | 0.223028 | 0.231553 | 0.223028 | 0.231553 |
| Ray Data | 15.186813 | 5.439133 | 1.438763 | 1.438763 | 5.439133 |
| SemLoom | 0.291085 | 0.181207 | 0.177223 | 0.177223 | 0.181207 |
| IMLane CPython | 0.526106 | 0.331219 | 0.384607 | 0.331219 | 0.384607 |
| IMLane作者Ray | 1.375768 | 0.372150 | 0.382981 | 0.372150 | 0.382981 |

请求时间独立采自同一网关进程的单调时钟。网关区间为handler进入至完整响应写出结束；
模型HTTP区间为实际上游headers发送至完整响应体读取，含网络与服务排队，不能当作GPU纯计算。
下表每项n=16、单位秒；最近秩P95与P99在本次样本中都等于最大值。

| 路径 | 网关P50 | 网关P95／P99／max | 模型HTTP P50 | 模型HTTP P95／P99／max |
|---|---:|---:|---:|---:|
| 原LOTUS | 0.112805 | 0.132864 | 0.107354 | 0.117802 |
| Daft | 0.110623 | 0.118906 | 0.102197 | 0.109197 |
| Ray Data | 0.067228 | 0.078621 | 0.064258 | 0.077028 |
| SemLoom | 0.103110 | 0.113720 | 0.097331 | 0.102508 |
| IMLane CPython | 0.063437 | 0.074942 | 0.061678 | 0.071538 |
| IMLane作者Ray | 0.060198 | 0.068122 | 0.057590 | 0.065907 |

原LM在批次完成时共同发布各调用结果，其ready至整批返回区间另存，不能将8个相同区间当作独立HTTP返回时间。
六个group按固定顺序分块执行；Ray Data两条测量查询差异较大，首次图成本、进程启动及服务历史均可能影响小样本。
最近秩P99在n=2／16下只是样本最大值，不是稳定总体尾延迟。
全部原值在[144条请求](model_requests.csv)与[18条查询](model_queries.csv)，
全部分段／吞吐／n／错误数在[机器结果](model_results.json)。
[离线复算](recompute_model_metrics.py)仅读取CSV，不启动任何服务：

```bash
python experiments/results/postgresql/native_executor_baselines_20261012/recompute_model_metrics.py --check
```

IMLane两模式各实际24次调用、2个worker；作者Ray另有2个非空actor ID，均使用同一DBEnd桥与薄UDF。
空输入探得settings执行器4／线程6，声明执行器2、图额度8；settings4不作为实际执行器数。
Coordinator及独立Ray退出码0、未强制，自有存活进程、共享内存、未确认标记均0。
模型API服务退出码0；按记录的自有PID／start ticks核对无存活进程，GPU计算进程归零。
完整资源身份和原件摘要见[来源记录](model_source.json)。

本轮首次启动在模型就绪后、本地连接断开时因supervisor写stdout发生`BrokenPipeError`，driver未启动，POST／分配均0；
API服务退出0、GPU进程归零。改为自有脱离连接的supervisor和持久日志后，在原15分钟启动期限内完成第二次启动，
没有重置期限或重试查询；首次失败与两次服务日志同包保存。
原始归档`final-evidence.tar.gz`的SHA256为`b5b92075e09bfee3d68ff4e95af1a9c47c2f6a2dde8ebf97f08716f2ff60a623`，
保存在接续任务仓库外`evidence/lotus-six-model01/`，成员以`lotus-six-model01/attempt02/`定位。
归档包含SQLite记账、完整protocols、HTTP／worker观测与失败原件；公开CSV不筛除慢样本。

<a id="lotus-two-gpu-next"></a>
## 同机双GPU的后续方案

用户已授权继续双GPU工程检查；软件资格与随后真实模型轮均通过，原安排见[当前方案](../../../plans/语义系统对照.md#lotus-two-gpu)，实测进入下一节。
上一版单双卡重测、到达轮询及55分钟安排已由双卡一轮、完整请求值固定分配、15分钟启动／10分钟测试替代。
[固定请求路由](../../../../code/src/experiments/fixed_request_routes.py)不读服务负载，不改变正文、原parser或并发额度；
所有六条路径共用一个端点，源行0–3映射GPU0，4–7映射GPU1，重复行1／2同副本，[固定清单](dual_route_manifest.json)保留各行关联。
路由不从HTTP序号制造源行身份，模型前先用既有单卡完整请求生成不可变摘要清单。

本机旧网关与新路由30项通过／39软件POST，服务器新路由6项通过／11POST；
实际六条LOTUS路径再各消费两行软件响应，共12POST，两副本各6，原SDK／parser及退出检查通过。
累计62次软件POST、小于64上限，模型／GPU／隐式重试均0。
完整gzip字节、重复头、远端HTTP错误、超时及caller取消都保留一次发送；
caller关闭时不宣称远端停止，受控fixture由自己的owner结束后才记清理完成。
真实双卡计划144POST、独立上限288，不为花完额度扩大样本，旧轮剩48保持未用。

<a id="lotus-dual-model"></a>
## 六条LOTUS路径的双GPU真实模型检查

唯一双卡轮`lotus-dual01/models`全部通过：同机2×TP1／PP1、两个独立Qwen2.5-7B服务，
六条路径共18查询／144真实模型POST，错误、超时、隐式重试均0。
每次8行完整消费、原parser返回8个`ok`，完整请求值多重集合与既有单卡轮逐项一致，
两个副本每查询4／4，各累计72POST／144输出token；测量请求各48，预热各24。
内层网关、公共固定路由及SQLite记账均为144，两份vLLM访问日志各72，已结束记账单元18。
新独立上限288占用144、剩144未用；旧单卡剩48未用，不转入本轮。
三轮累计实际模型312POST／占用320名额，较早失败及两次单卡启动记录保持原件。

固定组合源码`c0f420f8`保留稳定性`9bb6c415`并合入原生`7829bb45`，仅项目日志发生合并冲突、两份记录均保留。
模型源码归档SHA256为`c45fa20abd747d475fe715e9c5b9801e8d1a32b9fe1d6b0f25977be248dbadf1`；
路由实现与服务器软件资格逐字一致，原方法／公共native_calls／供应商入口没有为本轮新增调度策略。
权重、tokenizer、生成、原方法及C8／线程8与单卡保持；每卡vLLM配置也相同，CUDA服务隔离并实际核对GPU UUID。

API PID189412／189413、实际engine PID189623／189612分别在GPU0／GPU1；
其实际CPU亲和为80–87／88–95。两个服务各就绪约46.128秒，含环境／模型文件预检的共同启动区间77.708秒，
保持一次15分钟期限；模型supervisor测试78.467秒、driver区间75.199秒，小于10分钟。
查询driver／公共路由使用72–79，CPU8–15未占用。两个模型API退出码0，已观测自有存活／GPU计算进程0。
作者IMLane两模式各实际24调用、2worker，Ray另有2个actor ID；Coordinator／Ray退出0、未强制，共享内存和未确认标记0。
各PID／start ticks、实际设备和服务参数由[机器结果](dual_model_results.json)定位。

每条路径仍为8预热＋16测量，n=2完整查询／16请求。下表查询单位秒，双卡预热单列，
单卡原值取同一8行输入的已测轮；吞吐比为`双卡16/两次查询耗时之和 ÷ 单卡16/两次查询耗时之和`。
双卡多了公共透明路由一跳，服务启动历史与固定路径次序也不同；比值只作有限诊断，不能报可靠加速。

| 路径 | 双卡预热查询 | 单卡两次测量 | 双卡两次测量 | 消费吞吐比（双／单） |
|---|---:|---:|---:|---:|
| 原LOTUS | 0.748612 | 0.254204／0.213361 | 0.180697／0.482898 | 0.705 |
| Daft | 2.795650 | 0.223028／0.231553 | 0.192738／0.186022 | 1.200 |
| Ray Data | 14.414726 | 5.439133／1.438763 | 5.629190／1.837113 | 0.921 |
| SemLoom | 0.279135 | 0.181207／0.177223 | 0.164347／0.186899 | 1.020 |
| IMLane CPython | 0.512712 | 0.331219／0.384607 | 0.329087／0.371936 | 1.021 |
| IMLane作者Ray | 1.284567 | 0.372150／0.382981 | 0.367844／0.393930 | 0.991 |

请求时间采用独立单调时钟区间。内层网关包含公共路由与模型HTTP往返；模型HTTP采用公共路由的实际上游headers发送至完整响应体读取。
下表n均16，单位秒；最近秩P95／P99均等于本次最大值，不是稳定总体尾延迟。

| 路径 | 内层网关P50 | 内层P95／P99／max | 实际模型HTTP P50 | 模型P95／P99／max |
|---|---:|---:|---:|---:|
| 原LOTUS | 0.094460 | 0.418599 | 0.076766 | 0.082842 |
| Daft | 0.081307 | 0.097283 | 0.066549 | 0.082304 |
| Ray Data | 0.067435 | 0.075549 | 0.061952 | 0.070680 |
| SemLoom | 0.082056 | 0.096746 | 0.061780 | 0.078596 |
| IMLane CPython | 0.061598 | 0.073379 | 0.055746 | 0.069972 |
| IMLane作者Ray | 0.054760 | 0.072843 | 0.049669 | 0.066457 |

首次可复用group启动单独记录：原LOTUS8.164秒、Daft0.428、Ray Data10.699、SemLoom11.997、
IMLane CPython0.275、作者Ray0.289；IMLane作者服务另需0.558／5.563秒。
这些不进入两次查询测量；每次查询包括原parser／消费／记录EOF及该查询清理，
全部公共路由响应写出终点早于记录的查询终点，18／18逐项核对。

GPU利用采样请求间隔0.25秒，加上每次nvidia-smi命令耗时。模型测试区间每GPU各241样本：
GPU0样本平均3.759%、最大100%、非零14个；GPU1平均3.942%、最大94%、非零16个。
该区间含group／graph启动和请求间等待，并非连续GPU计算或饱和测量；短任务可能落在采样之间。
各查询样本数保留，n=0表示没采到样本，不能写成利用率0。
采样窗口采用同一host的CLOCK_MONOTONIC，不跨worker时钟相减制造模型计算时间。

全部原值包括[公共路由144请求](dual_requests.csv)、[内层144请求](dual_inner_requests.csv)、
[18查询](dual_queries.csv)及[482条设备采样](dual_gpu_samples.csv)。
[机器结果](dual_model_results.json)保存每卡请求分布、n／分位、group启动、全部单→双比值及退出。
复算不访问任何服务：

```bash
python experiments/results/postgresql/native_executor_baselines_20261012/recompute_model_metrics.py --dual --check
```

原件包`final-evidence.tar.gz`保存在接续任务仓库外`evidence/lotus-dual01/`，
SHA256为`190d41994ef9dfc70fb7ea268e4bfbb14088121816958de73ddab3ddaa2cac80`。
按成员`lotus-dual01/models/`恢复两服务日志／身份、SQLite记账、完整协议、HTTP／worker及GPU采样，
`lotus-dual01/software/`恢复软件资格；同包保留两份source archive和精确执行程序。
所有新慢样本保留，本轮没有模型失败重跑；旧轮失败独立保存。

<a id="lotus-known-audit-limits"></a>
### 已确认的成本与错误资格差异

独立只读审计核对`e571d4be`及`7829bb45`的相关核心；本轮`c0f420f8`未改这些文件。
完整正常请求／最终LOTUS parser／关联／消费证据继续有效，中间响应解释不能表述为“原SDK全部相同”：
[原生响应记录](../../../../code/src/semantic_methods/lotus/native.py)存在逐响应HTTPX／OpenAI客户端和LiteLLM本地重放成本，
[替换路径转换](../../../../code/src/semantic_methods/lotus/sdk.py)采用直接转换；非2xx与畸形JSON的类型／字段尚未通过同SDK完整对拍。
正常usage／logprobs已有测试，不能扩为全部错误路径等价，也不能把较短查询时间全部归为执行调度收益。

[native_calls](../../../../code/src/baselines/text/frameworks/native_calls.py)每call创建／关闭aiohttp session，
IMLane逐call事件循环与SemLoom客户端复用不同；原生完整响应默认8MiB、SemLoom默认1MiB，未在本轮统一。
当前输入8行、正常响应远低于共同1MiB范围，仍然是有效接线证据；扩大规模前需显式共同容量及期限。
IMLane128行／32MiB输入、C++聚合驻留输出无32MiB保证继续保留，不能以新双卡成功宣称512行能力或内存峰值资格。
提前close的消费线程附加异常可能未进入最终关闭报告，这是已复现的诊断遗漏；不能据此声明真实厂商泄漏或远端已停止。
这些修补已由独立owner按用户批准在另一工作树推进；本轮固定旧源码未带入，当前任务没有修改共享核心或重花模型预算。
新公共路由的超时／caller取消检查只覆盖该HTTP入口，不代替上述六路径所有异常生命周期资格。

### 后续正式SemBench测量条件

由独立SemBench owner完成原查询、真实输入、提示／parser、NULL／重复行及答案质量核对，并完成上述响应解释／连接复用／共同容量／期限和关闭诊断的软件修补及对拍后，才能开展公平性能对照；
本轮人工文本与固定`ok`不能替代该资格。固定源码、原Q／输入、模型逐文件身份和服务参数，
在1×TP1与2×TP1都经过同一公共固定路由下重测，保留各原生调度、相同总额度和明确的每卡容量。
预热、冷group／服务启动分别登记；使用预先安排的交错重复及足够样本，并通过真实请求形状的容量／饱和检查，
报告请求与完整查询原值、吞吐、质量、每卡真实调用／利用、错误／超时／未知责任。
正式清单、服务期限、POST额度和停止条件须单独授权；当前余量不自动继承，不增加动态路由、SemLoom专属负载信息或独立Q实现。

## 结束责任与原件恢复

正常复用要求完整响应、EOF和正常DBEnd退出。错误或提前停止消费后的未确认记录按Coordinator PID/start ticks、实例名称和work_root保留，
新对象不得跳过该记录继续提交；调用者核清并结束自己的Coordinator／Ray后才使用新身份。
通用HTTP端点的`remote_http_stopped`保持unavailable，consumer退出不能当作远端请求停止。
执行稳定性与多副本另在`codex/semloom-stability-and-replicas`，原生分支不重复实现这些修补；
固定两分支组合已完成上述回归，原生单独分支不重复携带稳定性修补。完整方法接通不自动证明PG载体或调度收益。

仓库外恢复根分别为`native-executor-20261012`、`native-executor-baselines-20261012`及接续任务的`evidence/`。
先按紧凑事实核source archive和binary摘要，再读取`source-identity.json`、query summaries、原始HTTP／worker trace、SQLite记账及所有失败轮。
IMLane新交付原件另保存在`imlane-call-evidence/`，接续整合原件在`evidence/imlane-entry01/`；配置、进程身份及大日志不进入Git。

<a id="original-movie-completion-integration"></a>

### 原Movie显式任务完成期限的集成兼容

原通过IMLane运行绑定`ab3d80e6789fd64392c6e7e955d17499f327c8e2`与八份私有适配文件，
派生源码身份为`64985b4e30107789816ab84aacd8d8cdf7ddc802b55c335827f6a598f8d8c7dc`；
`614c8e21`只发布报告。原Q1两模式121.205472062／124.406008423秒与失败原件保持，
不能用融合版默认120秒宣称复现该配置。

已接纳13文件最小兼容补丁：原Movie六路径可显式成对空query／owner总期限，默认120／1800不变；
逐批IMLane可透传空批期限，复用桥session仍拒绝。有限SQLite账本保留v1及NOT NULL，
显式空期限另用v2，不迁移旧文件。非原Movie在读源、建输出和领预算前拒绝。
融合版49项本地检查通过（44个兼容／旧路径方法与5个诊断入口方法），只有两次合成localhost回复；
实际SDK／服务器／模型／GPU0。原owner多轮44项记录和测试编写失败另保留，不相加。

空总期限不会安装监督器。后续调用方必须另持有限的HTTP30秒、backend／工作无活动120秒、
启动无活动180秒、清理120秒、RSS8GiB、至多4608逻辑调用／13824传输POST及自有出生身份、
共享lease与退出核验契约；不能重新启动已闭合的历史supervisor或用旧overlay整文件覆盖新计时／关闭修订。
当前兼容与本地资格不授权新模型任务，新控制器／资源与实际SDK资格须在新运行前单独绑定。
