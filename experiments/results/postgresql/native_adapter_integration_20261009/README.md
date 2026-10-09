# 原生方法与SemLoom执行适配：工程接入核对

日期：2026-10-09。研究对象为**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。
本次整合公共任务／完整响应入口、LOTUS方法、DuckDB批次、Sema请求服务及原生Daft／Ray执行参照。
这是固定版本的工程接入证据，实际模型运行与性能判断分别登记。既有原生入口和默认PG路径保留。
总体设计由[语义系统方案](../../../plans/语义系统对照.md#native-semloom-pairs)维护；命令见[脚本入口](../../../../code/scripts/README.md#native-adapter-query)。

## 来源与实现

起点为`cbfd952ba64e2fc0125a813041cd60c02533d4cf`，依赖提交各自保留后引入独立整合分支。
起点原有24份变更属于前次就绪计时和总体设计，本次不重新计作适配器交付。
提交来源、实际运行源码摘要、全部检查与失败对应由[verification.json](verification.json)维护。
所有服务器源码包和原始运行使用新标识，没有覆盖旧源码、旧证据或main。

| 对象 | 实现位置与采用原因 | 保留的原生行为 |
|---|---|---|
| 公共任务 | [NativeTaskSession](../../../../code/src/execution_provider/adapters/native_tasks.py)复用Core的有限提交、accepted-prefix、乱序完成及归还；[完整响应](../../../../code/src/execution_provider/adapters/full_response.py)保留HTTP状态、重复header、原始体及版本 | PG默认结果与错误分类继续使用旧模式；完整响应为显式选择 |
| LOTUS | [批次／短链](../../../../code/src/semantic_methods/lotus/README.md)在完整未缓存调用进入原生限流与LiteLLM池之前选执行器；SDK转换以原生实际HTTP值核对 | DataFrame物化、formatter、模型设置、统计、parser及原整批返回等待保留；成对检查关闭缓存 |
| DuckDB | [公开扩展补丁](../../../../code/integrations/duckdb_ai/README.md)在原生准备完成后、线程池前交出当前vector的完整批次，再逐响应交回原C++解析 | SQL扫描与整vector结果返回保留；SemLoom分支跳过原线程、请求控制、重试和缓存；默认仍原生 |
| Sema | [请求服务](../../../../code/src/execution_provider/adapters/sema_request_service.md)连接公开`llm_url`，保留直达、透明和SemLoom服务三条路径 | 作者二进制SQL、线程、请求池、联合提示、parser和行对应均保留；这是后置服务补充观察 |
| 共同固定Map | [prepared_map.py](../../../../code/src/baselines/text/frameworks/prepared_map.py)在图外准备完整调用，分别进入原生Daft、原生Ray或SemLoom | 原生图负责并发／缓冲；HTTP kernel不提交SemLoom任务；旧SQL reader入口保持 |

实际查询驱动为Daft0.7.21、Ray2.56.1、Arrow24.0.0、LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0、
DuckDB1.5.4。驱动与vLLM环境保持隔离；SQLAlchemy2.1.4只安装至缺依赖的驱动环境，修复后只读预检通过。
DuckDB编译版本为`0.4.14-semloom1`，匹配DuckDB源码`08e34c447bae34eaee3723cac61f2878b6bdf787`及
AI源码`9b7b16a5d5bfa97180b8be48d69bd9a4a4106419`；编译二进制摘要为
`e1f04a82e1a703d00ce14b4401aec57fd0ac393cfcc02d104878892d4ed1e963`。
原社区二进制摘要`719c48c6cfda10a4c3e1cfcea42a528d2b290ef52cde9cbecddd5b4bb3286ac4`继续保留。
Sema v0.0.1作者二进制产物摘要为`15a534667a668a152524a8756fe5d66c0fa9c9822e73d26029e1a71693a56ec3`。

Daft内置prompt未改造为独立方法适配：固定[公开源码](https://raw.githubusercontent.com/Eventual-Inc/Daft/v0.7.21/daft/ai/openai/protocols/prompter.py)
在同一接口完成内容转换、SDK调用、usage记录和结果文本提取，没有独立完整调用／响应导出接口。
安装源码与固定标签的相关表达式、provider和函数文件摘要已核对；原内置prompt继续作为整体系统比较。

## 查询与请求观测

[新入口](../../../../code/src/experiments/postgresql/README.md#native-adapter-query)读取有限外部原文，
每个行出现有唯一`row_id`，相同文本也保持独立调用。单次最多4096行、源64MiB、单条原文64KiB；
两Map方法还声明状态、结果、阶段与同时保留行数。调用模型前核对输入、所选方法、服务身份和资源设置。
模型POST由共同代理逐次持久登记，worker标签只作观察；失败摘要与已用额度不退款。

完整应用从配置／原文准备入口到统一消费结束；就绪查询直接测实际提交到消费结束，包含方法与图构建。
结果记录保存完整行数、字节数和SHA-256，并在解析前保存协议与完整响应；事后质量和清理另列。
请求统计以一条完整调用为单位，由合法任务就绪到原调用者取得完整响应，不以代理到达作为起点。

| 路径 | 起点与返回 | 不可观测或仍保留的等待 |
|---|---|---|
| 共同固定Map | 图外完整调用就绪，原生图／Core交回完整HTTP响应后返回 | 记录准备、提交、HTTP读取、worker和结果归还；无服务内部单调用计算时间 |
| LOTUS单Map／阶段等待链 | 未缓存批次进入请求控制之前；原LM取得完整SDK响应列表 | 整批返回等待保留；SDK值对象的哈希明确区别于HTTP字节哈希 |
| LOTUS逐行两Map | supplier start／resume产生调用；resume取得完整前继HTTP响应 | 每行前继响应至后继合法就绪单列；原真实数据依赖保持 |
| DuckDB＋SemLoom | 完整C ABI批次到达可信回调；完整body马上交回C++ consumer | C++就绪值另存，未证明其steady clock与Python同钟；SQL当前vector等待保留 |
| DuckDB原生／Sema | 没有整合的池前任务就绪和逐行完整响应观察 | 请求端到端不可观测；HTTP阶段、完整查询和后置服务自己的阶段另列 |

[指标代码](../../../../code/src/experiments/postgresql/native_adapter_metrics.py)保留所有调用和查询样本、
最近秩P99／P99.99、样本数及分位落在最大值的标记。不同表示或时钟不能混用；
HTTP往返不等于纯模型计算，逻辑名额不等于GPU利用率，并行／嵌套阶段不相加扣除。
组织等阶段缺少起止时写不可观测。普通分类错误保留在质量分母，非法值、关联或计数错误停止查询。

## 工程验证与失败

供应商10条实际库入口及共同固定Map三条执行参照均通过；本地39项相关检查和私有PostgreSQL18.3的3项旧查询／direct生命周期回归已通过。
各次来源、测试结果、调用数和原始观测由verification.json记录，工程检查真实模型调用为0。
实际Ray与原生库检查使用本机确定性HTTP fixture，CPU取独立4个可用核；与baseline并存期间的耗时不进入性能排名。
SemLoom声明请求4、worker2、batch2、payload窗口2MiB、对象8MiB、Ray对象存储256MiB、GPU0个。
原生Sema的线程数不能视为同样的请求上限：实际8行模型检查中，线程4时HTTP峰值可达8；后置SemLoom服务峰值4。

已保留的整合失败包括：旧服务器checkout缺redactor；驱动缺SQLAlchemy；初始账本装配错误；
payload窗口漏计身份开销；fixture缺SDK必需role；Sema输入子目录未创建；以及PG测试的过长Unix socket、
新连接缺会话hook预载和driver／PG用户不同。各次修订使用新运行标识，没有抹去失败或合入成功样本。
PG编译来自未修改的公共C扩展，私有前缀／control路径和独立cluster中测试；系统PG18.4和全局extension保持。

供应商交付检查另有各自源码身份：公共任务401项调度检查、provider157通过／9跳过与116项PG协议／静态检查；
LOTUS15项实际库及48项受影响回归，另1项真实Daft／Ray检查；
DuckDB13项原生检查及2项真实Daft／Ray SQL检查；Sema53项本地、9项原进程和9项原生场景检查。
这些范围有交叉，不合计成一个独立样本数；原提交与检查原件按来源单列。

## 模型运行与尚未验证项

用户要求先测已有七条baseline；该独立运行已完成175条查询／51,184次POST并清理释放GPU0。
整合任务随后开始独立模型预演，两个运行没有并行模型服务；baseline结果仍由原报告维护。
使用缓存Qwen2.5-7B、相同服务设置、8行原始Movie输入，单查询120秒、整体45分钟、最多512次累计POST，
先正确性再有限重复；首次协议、关联、非法输出、计数、超时或清理错误停止并保留。
原生Ray首次私有调度缺短scratch路径，在0 POST时停止；进度文件写入随后遮住该错误。
续跑的私有命令遗漏原累计账本路径，仍为0 POST；两次停止与零POST服务退出均保留。
完整CLI的8次fixture检查通过后修正调度，原成功Daft8次保留，后续使用新单元身份、原512上限与原结束时间；
占用未发送的8次额度不退款，累计计划392次预留／384次实际POST，最终值以verification.json为准。
该小清单仅核对工程正确性、质量和调用内容，不提供充分工作负载、服务饱和或总体极端尾部结论。

级联、多模型角色、全表校准、跨行Join、并行展开与PG多Map未实现；
Sema原请求池之前的供给和HTTP到原行对应不可观测，不能宣称干净替换作者执行器。
LOTUS短链是外部输入方法继续执行检查，不等同数据库内多算子接入。
性能采用仍需共同实际任务、质量、完整消费、资源和退出的配对证据；默认不因fixture通过而改变。

## 真实模型预演结果

13条入口各完成一次正确性查询及两次有限重复，共39条完成查询／384次POST；其中首次服务8次、第二次服务0次、
最后服务376次。原共享账本累计预留392次，真实登记384次，0 POST准备失败占用的8次继续保留；总上限512没有增加。
账本、代理、逐查询协议和vLLM成功增量一致，服务结束时运行／等待请求均0。
完整源码为`source-integrated07`，源码包SHA-256见verification.json；全部原值与比较见[model-analysis.json](model-analysis.json)。
每条路径复用同8条已使用的Movie原文，3次查询不是24条独立评论；以下是新进程小批量工程观察，不作为系统性能排名。

| 路径 | 完整API秒：三次原值 | 就绪查询秒：三次原值 | 正确行数／8：三次 | HTTP峰值：三次 |
|---|---|---|---|---|
| fixed-map-native-daft | 3.166 / 3.171 / 3.138 | 2.903 / 2.879 / 2.894 | 5 / 5 / 5 | 4 / 4 / 4 |
| fixed-map-native-ray | 28.821 / 28.958 / 28.808 | 18.544 / 18.629 / 18.173 | 5 / 5 / 5 | 4 / 2 / 4 |
| fixed-map-semloom | 17.390 / 17.258 / 17.556 | 3.059 / 3.161 / 2.919 | 5 / 5 / 5 | 4 / 4 / 4 |
| lotus-adapted-native | 9.282 / 9.506 / 8.905 | 0.733 / 0.905 / 0.756 | 5 / 5 / 5 | 4 / 4 / 4 |
| lotus-method-semloom | 54.449 / 56.494 / 55.240 | 31.326 / 33.542 / 32.131 | 5 / 5 / 5 | 4 / 4 / 4 |
| duckdb-adapted-native | 0.788 / 0.879 / 0.870 | 0.209 / 0.219 / 0.209 | 5 / 5 / 5 | 4 / 4 / 4 |
| duckdb-method-semloom | 17.230 / 17.696 / 17.946 | 2.808 / 3.061 / 3.183 | 5 / 5 / 5 | 4 / 4 / 4 |
| sema-native-direct | 0.988 / 0.569 / 0.507 | 0.704 / 0.227 / 0.225 | 6 / 5 / 5 | 8 / 8 / 8 |
| sema-native-transparent | 0.543 / 0.507 / 0.509 | 0.236 / 0.231 / 0.232 | 5 / 5 / 5 | 8 / 8 / 8 |
| sema-method-semloom-request-service | 18.156 / 17.895 / 16.894 | 3.323 / 3.635 / 2.252 | 5 / 5 / 5 | 4 / 4 / 4 |
| lotus-two-map-native-staged | 10.382 / 9.973 / 10.175 | 1.554 / 1.481 / 1.466 | 6 / 6 / 6 | 4 / 4 / 4 |
| lotus-two-map-semloom-staged | 60.165 / 56.055 / 57.256 | 37.378 / 32.256 / 34.893 | 6 / 6 / 5 | 3 / 4 / 4 |
| lotus-two-map-semloom-incremental | 60.227 / 60.017 / 61.838 | 36.994 / 36.956 / 37.820 | 6 / 6 / 6 | 4 / 3 / 4 |

共同固定Map和LOTUS单Map的完整调用身份／内容一致；DuckDB及Sema的实际HTTP JSON值集合也分别一致。
原生C++和Sema没有任务就绪／HTTP到行对应观察，不能从HTTP集合一致推导这些时刻。
原私有`method-comparisons.json`把DuckDB空调用描述列表报成内容不同，其原因不成立；旧文件原样保留，
更正派生分析将调用身份比较记为不可观测，独立HTTP完整值集合比较仍一致。
两Map链的第一阶段值一致，但前继生成文本不同，第二阶段的实际完整调用集合不同；不据此归因纯执行器收益。
Sema首轮直达正确6/8、另两路径5/8；两Map阶段等待SemLoom第三次5/8，其余链结果6/8。
保留所有合法错误及输出差异，不声明质量等价。小批量SemLoom完整API耗时高于对应原生方法，准备成本明显，默认保持。

调用统计每条路径24或48次，完整查询每条3次；最近秩P99／P99.99均落在各自样本最大值。
27条具备调用者起止的查询按原事件逐值复算通过，其他12条保留不可观测说明。
模型内部单调用推理时间仍不可观测，HTTP往返与嵌套／并行准备不相加扣除。
Python初始模块导入在API准备起点之前，进程启动不等于报告的完整API耗时；RSS采样也不覆盖所有准备峰值。

39条模型查询之后，`source-integrated08`补存成功上游响应体的原始字节／状态；LOTUS、DuckDB、Sema各4次fixture、
共12次原始体摘要核对通过，真实模型POST0。这项观测修订不改方法、执行或解析，与模型源码分别登记。
此前原生C++成功体保留完整JSON值，原始体字节不可重建；旧模型证据没有追补或伪装成新观测。
公共完整响应通路和SemLoom返回体、LOTUS SDK完整值及相关原始体按各自实际表示保存。

只读阶段定位：LOTUS额外约30秒集中在首次`payload_next`工作函数内；源码调用是在线程执行器中
推进`iter_payload_batches`，覆盖Arrow／Daft导入、Native runner设置、图构造、首批物化和逐值检查。
原事件已将池等待、工作函数墙钟及事件循环恢复分开；首个工作函数墙钟占主导，不能把它写成纯CPU计算。
actor ready约3.7秒，属于提交前准备，未解释LOTUS约30秒的就绪查询差异。
单Map首轮提交至代理开始上游dispatch29.264秒，该起点至末上游体1.984秒。
后续headers实际发送另记：提交至首个实际上游headers发送29.714秒，首headers至末上游完整体1.534秒，
末上游体至查询结束0.077秒；首次payload工作函数墙钟28.725秒。
因此该轮主要等待在首次数据准备／物化调用，HTTP区间和末尾消费不能解释额外约30秒；更细内部原因尚未观测，未据此修改执行代码。

| 路径 | 首次payload next秒：三次 | actor ready秒：三次 |
|---|---|---|
| fixed-map-semloom | 2.695 / 2.781 / 2.536 | 3.838 / 3.630 / 3.709 |
| lotus-method-semloom | 28.757 / 30.290 / 28.967 | 3.675 / 3.766 / 3.814 |
| duckdb-method-semloom | 2.424 / 2.683 / 2.793 | 3.712 / 3.668 / 3.854 |
| lotus-two-map-semloom-staged | 31.484 / 26.695 / 29.314 | 3.921 / 3.858 / 3.703 |
| lotus-two-map-semloom-incremental | 31.595 / 31.268 / 32.243 | 3.823 / 3.667 / 3.884 |

独立退出核对通过：三次服务的owner／API进程均不存在，自有Ray进程0，localhost模型与私有PG端口关闭，
两张卡均1MiB／0%；仅GPU0参加模型运行，原DuckDB缓存二进制摘要保持。
根目录和数据盘入口ACL按任务修改前的保存值复核恢复，自有目录额外条目移除。
共享实验父目录未记录最初ACL，当前postgres遍历条目保留；未猜测覆盖共享权限，原件与处理说明已保存。

<a id="persistent-fixture"></a>

## 常驻查询与首批payload的工程检查

`resident-source02`以整合提交`2a7ab18a`加13份代码／测试摘要定位，服务器副本在fixture完成后再次逐文件核对。
身份文件SHA-256为`34b2b2f573b91d3c03e07e176a2a1479d1677f1eed4dda6a62afd340d7fb28c9`；
源码增量归档摘要和依赖版本见[persistent-verification.json](persistent-verification.json)。真实模型比较仍依据既有语义系统方案与对应运行原件，本节仅报告测试HTTP服务。

`PersistentAdapterGroup`通过原查询入口持有同一个Core、Ray连接与LOTUS LM，每查询重建任务流、观察和账本单元。
结果消费结束后归还记录并核对Core用量、Job与代理HTTP，最终owner清理所有组件；首次错误停止后续查询，失败清单不复用结果或返还已消耗预算。
启动记录与逐查询记录分开，`persistent-query.json`以原摘要SHA-256关联`summary.json`，分别保存当前原文读取前的release、实际API提交和EOF。
当前输入读取、formatter、图创建、首批物化、解析和消费均继续计入release到EOF；`ready-timing.json`仍保留实际提交到EOF。
同形状预热只运行先前查询，没有提前生成本次测量查询的完整请求或首批数据。

六条实际库路径逐臂使用独立进程；每条连续4、8、4行三次，分别标识资格、预热和测量，共18查询／96次测试POST、真实模型POST0：

| 路径 | 已核对的跨查询对象 | 查询专属对象与当前设置 |
|---|---|---|
| 固定Map原生Daft | Native runner所在进程 | 当前图与单行async batch；每行仍创建HTTP session |
| 固定Map原生Ray | 同一个8CPU、0GPU的Ray连接 | 原HTTP Processor每图创建自身actor，保持原生调度 |
| 固定Map SemLoom | 同一Core、Ray连接与HTTP worker池 | 完整调用、任务流、结果消费和观察按查询建立 |
| LOTUS原生 | 同一个LM，缓存关闭 | 原uncached batch请求池、formatter与SDK解析保持 |
| LOTUS本地SemLoom诊断 | 同一Core、本地完整响应传输与LM | `physical=None`，不含Daft／Ray，不作为普通HTTP或原生LOTUS |
| LOTUS SemLoom Daft／Ray | 同一Core、LM、Ray连接与HTTP worker池 | 每查询新的LOTUS batch观察与任务流，stage索引从0开始 |

每查询POST、完整行数、预算关闭、owner／执行／LM身份以及HTTP查询标识分别核对。原生Daft0.7.21源码文档提供`@daft.cls`在多个行间复用实例的合法API，
但该说明没有证明跨查询实例生命周期；本轮保留既有`@daft.func.batch`和每行session设置，没有用新有状态算子改变原生参照。
原生Ray三次查询前可用逻辑CPU分别8、6、7，来自其自身图actor的异步清理；SemLoom三次均6，因为其自身两个常驻actor各声明1CPU。
这些是一个arm独立8CPU集群中的实际观测，不声称每个时刻所有内部额度相等。共享多个arm的入口保留为部署诊断，额外保存其他常驻池的声明CPU。

首批payload新增嵌套墙钟探针：Arrow／Daft导入、Native runner设置、源Arrow表、图创建和第一次迭代物化；外围线程等待／工作／恢复仍单独记录。
LOTUS Daft／Ray路径的首次测试样本保留如下，时间属于本次CPU／HTTP fixture，不解释为模型推理或更大工作负载收益：

| 当前查询 | Daft导入秒 | 图创建秒 | 首批物化秒 |
|---|---|---|---|
| 4行资格 | 7.379285 | 0.007345 | 26.187243 |
| 8行预热 | 0.000033 | 0.008262 | 0.203183 |
| 4行测量 | 0.000021 | 0.006093 | 0.154244 |

原约30秒模型样本保持原身份。这次首次工作中导入和物化都可见，后两次小样本观察支持继续验证常驻方式；嵌套探针不相加扣除完整查询时间。
本地77项相关检查76通过、1项实际库入口跳过；Linux93项92通过、1项同入口在六条独立fixture中另行执行。
旧DuckDB两条`owner=None`入口分别4次测试POST通过；第一次加载被旧Conda C++运行库拒绝，实际POST0，保留其失败及错误的预计POST字段。
修订检查仅设置既有系统`libstdc++`的独立preload，没有修改模型环境、缓存二进制或查询方法。

Sema直接借用主线程Core至服务I/O线程的尝试被既有控制线程检查拒绝，尝试撤回，私有失败与源码差异保留。
作者SQL进程的重复SELECT／原表替换与新查询封装通过替身检查；服务Core和worker仍按查询创建，不称完全常驻。
DuckDB连接、Sema进程与两Map的新增常驻装配已提供，但这些路径的实际库常驻资格仍pending；旧入口回归不代替新增复用检查。

公开[350成员归档](raw/persistent-fixtures.jsonl.gz)保留六臂全部摘要、计时、结果、Core／worker事件、实际资源、旧接口检查与DuckDB加载失败。
每个成员分别保存私有原件和脱敏副本SHA-256；[persistent_replay.py](persistent_replay.py)复核全部成员、18查询／96 POST、旧DuckDB8 POST及原零POST失败。
原件保留在仓库外，未把fixture时间或三次查询的极端分位解释为真实模型性能。

<a id="persistent-model"></a>

## 常驻真实模型观察

`resident-model02`沿用`resident-source02`，一个模型服务覆盖六臂；每臂独立常驻进程，Ray路径各自使用8CPU、0GPU集群。
驱动固定8个CPU，模型固定另一组16个CPU，GPU0运行缓存Qwen2.5-7B-Instruct，BF16、模型长度4096、128服务序列、8192批token、
显存比例0.8、FCFS与chunked prefill，prefix cache关闭。C4、SemLoom worker2／batch2和原生接法保持，没有在线调参。
按固定顺序依次完成六臂，属于按臂分块的诊断观察，没有随机化跨臂排序；原生Daft的单行client仍为当前固定接法，不称已最佳调优。

每臂8行资格1次，8行和128行各预热2次、测量5次，合计90查询／5,760 POST，每臂960。
8行沿用资格样本，128行为既有scale输入，重复使用相同样本；不作独立质量泛化判断。
本轮保留当前查询读取、formatter、图构造、物化和消费，主指标为release到EOF；实际提交到EOF另存全部原值。
所有query、行ID、结果摘要、质量重算、HTTP正文不变／单次headers发送／retry0及5,760个唯一成功模型响应通过独立核对。
同方法每ordinal的三个执行路径实际HTTP值集合相同；Core、LM、Ray身份跨15查询保持，两个实际SemLoom actor ID仅各出现一个创建池，逐查询资源归还通过。

| 方法／执行路径 | 8行release到EOF中位秒 | 128行中位秒 | 128行每次正确数 |
|---|---:|---:|---:|
| 固定Map原生Daft | 0.231915 | 3.402501 | 108/128 |
| 固定Map原生Ray | 4.991013 | 8.471441 | 108/128 |
| 固定Map SemLoom Daft／Ray | 0.237352 | 3.499192 | 108/128 |
| LOTUS原生 | 0.278643 | 3.999719 | 106/128 |
| LOTUS本地Core诊断 | 1.939153 | 30.935126 | 106/128 |
| LOTUS SemLoom Daft／Ray | 2.754248 | 38.314639 | 106/128 |

所有8行测量均正确5/8，每格各5条完整查询，全部重复、预热、资格、起止和统计秩归[resident-model-analysis.json](resident-model-analysis.json)。
P99／P99.99仍为样本最大值，不能据此推断总体极端尾部。固定Map中SemLoom与原生Daft接近但略慢，比当前原生Ray入口快；
LOTUS两种SemLoom接入本次观察明显慢于原生，默认保持。不同方法不混合为统一执行排名，同输出计数也不表示新的质量贡献。

LOTUS本地Core不含Daft／Ray，128行仍约30.94秒，因此持续等待不能全部归为Ray启动或首次payload。
同规模HTTP覆盖区间中位分别为原生3.892889秒、本地30.621335秒、Daft／Ray37.636577秒，它们仍包含供给及重叠活动。
128行五次测量的每臂640请求中，转发前回调墙钟中位分别0.001356、0.210924、0.228653秒；转发后回调为0.000335、0.000389、0.000383秒。
单请求代理dispatch到完整上游体中位分别0.101298、0.440639、0.256279秒，三臂HTTP峰值均4。
共同保护／观察路径的实际等待并不相同；本轮是完整诊断观察下的常驻实测，低扰动敏感性尚未执行，不能扣回调累计时间构造虚拟JCT。

只读源码核对：原生与SemLoom的batch观察都会逐请求调用`prepare_call`；SemLoom在提交前再次执行参数转换／校验，
返回时另有完整体解码、SDK ModelResponse转换和原批次值序列化。完整第一次ready扫描在两条128行末次查询中仅约0.03秒，
未计到第二次转换或逐项Core工作；本地HTTPX client由同一传输保留，不是每请求重建。
转发前回调包含单锁、完整JSON检查和逐POST SQLite连接／`synchronous=EXTRA`／transaction提交。
Core在有可提交工作时可以立即推进，其他情况按wake与轮询间隔等待；仅从循环源码不能证明背压忙转。
SQLite锁／提交、GIL或CPU调度、HTTP客户端、Core推进、SDK转换及模型排队是待分离的候选原因，不写成已实测根因。
payload内部探针只有墙钟，没有thread／process CPU；完整进程树的1Hz CPU／RSS／PSS和PID创建身份不能替代单函数CPU归因。

两次零请求准备失败单列：默认解释器缺psutil，随后显式账本创建遗漏；原日志、退出、服务计数和清理保留。
服务01没有成功模型查询被重跑；模型02使用同一账本、同一5,760上限和原截止时刻，没有延长。
主模型owner完整时间884.689804秒，即14分44.69秒；prompt token522,669、generation token17,280、cached token0，与协议用量逐值核对。
结束时服务success delta5,760、running／waiting0、owner退出passed、端口关闭，自有模型进程与Ray清理通过；两GPU各1MiB／0%，仅GPU0运行模型。

原私有归档3270文件、9,887,600字节，SHA-256为`b943fb6b0541bc83129a40de9bf5cbb99c99571141babd10e36c779594b5f82e`，独立审核及成员清单保留。
[公开原始观察](raw/resident-model-observations.jsonl.gz)记录每查询时刻、结果、请求内容摘要、完整响应值、全部Core／worker事件、启动身份和1Hz进程树。
重复派生字段与owner中的查询事件去重，请求原文替换为完整值／消息摘要；原件和公开投影分别记录SHA-256及投影方式。
[resident_replay.py](resident_replay.py)复核全部成员、90查询、5,760响应、质量／请求集合／actor复用和全部测量中位数。
旧首次结果、零POST失败及本轮负结果保持原身份；这项观察不替代PG接入、多Job、低扰动、跨分布或总体尾部资格。

## 保存与复核

公开工程证据由verification.json及其归档成员定位，包含合成请求／响应、原始阶段事件和全部运行结果。
[replay.py](replay.py)核对两个公开归档的全部成员摘要、调用时间和查询分布；模型请求正文以完整值摘要及参数代替原文，私有原件保留。
公开运行中本机地址已脱敏，私有原件与公开成员分别核对摘要。
完整构建、源快照、机器配置与失败日志留在仓库外；通过运行标识、成员及SHA-256取得原件，公开报告不链接私有文件。
复核使用原测试入口和固定依赖；源码、事件及输出摘要分别检查，不把测试报告代替原始观测。
