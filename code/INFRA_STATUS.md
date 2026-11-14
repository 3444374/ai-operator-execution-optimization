# SemLoom 实现状态

本页只维护当前能力、可选路径和未验证项。接口与命令见[代码入口](README.md)、
[配置总览](../docs/执行配置与优化开关.md)；逐次运行的版本、完整结果与失败见
[证据台账](../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md)及所属报告。
CPU、软件替身、真实 IPC、真实 Ray、模型功能和性能分别解释，不把旧版本通过数累加为当前整树结果。

[隔离组合报告](../experiments/results/diagnostics/baseline_validated_integration_20261111/README.md)记录 `2e2616a5` 与 `6a31cc38` 的独立组合、限定 CPU 回归及仍缺资产的检查；
候选尚未合入 main。最新组件目标状态来自协调器报告，按原提交分列，不转授组合树模型资格。

Ray 完整文本查询新增[显式原生 processor 接点](src/baselines/README.md#ray-native-prepared-sembench)：
完整预测物化后、关闭前执行原 parser/关系并记录内存 sink；旧 `query_seconds` 不追认为统一口径。
`ac4440328` 已完成 Q10 full2000、Q8 120、Q9 Map256 的原生模型、实际 token、原 sink 和物理清理审计；
共享 `48a6b42d0` 题号绑定的 Ray Q1/Q2/Q3 也已完成 2000/120/120 次真实运行，2240 条终态、0 条 unknown。
Q9 Map 不授予 TopK 资格。当前整合保留原生执行与完整 sink，合并后的源码仅做受影响 CPU 回归，
不把这些冻结来源的成绩改记为整合分支实测，也不称全部 baseline 或全部题的统一计时已完成。

<a id="current-refactor-status"></a>
<a id="refactor-completion-checklist"></a>
## 当前能力与待处理项

### SemBench 原生接入覆盖口径

**整体接入尚未完成。** 原题绑定、后端执行、完整查询生命周期、目标验证和旧调用迁移分别核对；
单个入口或一批软件检查通过，只关闭其实际覆盖的问题。

固定源码的完整查询范围为 Movie 10、MMQA 16、Animals 4、Cars 5、Medical 6、E-Commerce 14。
这些是原 LOTUS 查询函数的范围，不是模型通过数，也不表示其他系统必须实现同一张能力表。
Medical Q2 在原文件直接声明不支持音频；Cars Q2/Q5/Q6/Q7/Q9、Medical Q5/Q6/Q7 和
Animals 其余题号没有对应的 LOTUS 查询实现，不能把这些缺项算成本项目待补的逐题执行器。
Movie Q9 的 Map／TopK 和 exact／approximate 分支仍分别保留策略身份。

### 各系统 runner 收尾状态

下表先区分runner本身、任务接线与目标验收；详细题目范围仍以随后能力表为准。
LOTUS方法系列共用原题定义／原SDK语义，各执行系统保留自己的生命周期，不把同一实现重复统计为多个语义baseline。

| 系统 | 现行runner与已接任务 | 真实验证范围 | 未接／接口限制及旧入口 |
|---|---|---|---|
| LOTUS | 同一原题对象；[resident文本](src/experiments/sembench_text_resident.py)与[外部服务查询](src/experiments/sembench_lotus_query.py)按模型所有权保留profile。文本、视觉、聚合及实际多阶段已接。 | Movie、MMQA、Animals按冻结来源分列；f26fc MMQA Q6a sf25首／同owner重复各25请求原F1均1；a110 Movie Q8原120×2完成同owner首／暖、相对误差4.0431%；Animals Q10 sf64真实64次、原F1=1。完整排名、空Join与非空Extract组件不互授资格。 | 当前未验视觉／多阶段及真实资产另列；多列Join保留原SDK选列限制。旧视觉／聚合两个CLI和worker已迁移删除。 |
| LOTUS＋Ray Data | [native_ray_sembench](src/experiments/native_ray_sembench.py)承接完整原函数、实际多LM批次、视觉、分支／groupby和分层聚合；query级runtime不等于模型复用。 | 051639原Animals Q1 sf64及MMQA Q5单叶已真实运行；4073的Q7 sf64两阶段128终态、City F1=1，原退出失败及恢复清理保留。旧Q2a/Q7质量问题不抹除。 | 其余真实视觉、分层聚合与并发另验。旧专项Q3图已删除；外部HTTP Q3保留，但明确为LOTUS-compatible工程路径。 |
| 独立原生Ray Data | [sembench_native_ray](src/experiments/sembench_native_ray.py)用公开LLM processor执行Movie、MMQA／Medical／Cars／E-Commerce文本及[Animals图像Filter](src/baselines/image/sembench_native_animals.py)Q1/Q3/Q7/Q10，共用独立JSON任务工厂及官方评分；新增Medical／Cars／E-Commerce／MMQA图文与依赖批次见下文，无LOTUS提示／解析。 | Movie Q1/Q8/Q9-map/Q10、MMQA Q1/Q4 sf25及0d0cc Animals Q7 sf64有具名模型回执；MMQA首次外层清理失败后精确恢复，原件保留。606f Medical Q10与8cd Medical Q4完整1200条已有具名模型回执；8cd MMQA Q2b为325→6非空、F1=0；其余新组合不转授，751b Serve Q8原120×2已有同owner／token／结果资格，原外层清理失败恢复仍保留；其他Serve组合另验。 | “统一语义工程对照”，非作者原版；offline仍逐查询新模型owner；同runner新增显式Serve会话，7aa61配置／token门通过但启动hook冲突0生成；callback修复后751b Q8已通过实际SDK／同模型owner首与重复轮；外层清理原失败及恢复分列，其他组合不转授。当前新增图文／待接算法见下文，不借用LOTUS＋Ray模型成绩。外层拥有监督／租约。 |
| LOTUS＋Daft | [native_daft_sembench](src/experiments/native_daft_sembench.py)承接完整文本原题、exact Join和Q5实际多批；每批原生collect／worker隔离，末批在完整sink后关闭。附加内置OpenAIProvider接文本及视觉数组、原Q5层次聚合、Q12链及Q10/Q11原分支Join；既有顺序桥的两题完整软件／ACK已验，原字符串的native编码显式披露。 | 旧单冷／原生SDK记录仍属各冻结来源；视觉接点有真实Daft0.7.21图＋软件API及完整原题／ACK检查；751b Q5 sf25原16行单叶首／同owner重复各1真实请求原F1均1；751b Medical Q4原1200×2同owner均值MAPE1.6547%／.6512%；4f0b MMQA Q4原24×2、F1均.4865，分组19／18行差异保留；自然多层另核输入；6c75 Animals Q7 sf64两批128真实模型F1=1，4793 MMQA Q2b sf25空Join325真实终态／F1=0，Extract未触发。 | 本地vllm字符串profile没有图像carrier；外部服务profile支持可选静态system＋单user，字符串显式转单text part；多轮／分组并发未接，renderer token等价待验。TopK取消动作保持停止。旧专项Q3图／schema已删除。 |
| 独立原生Daft | [sembench_native_daft](src/experiments/sembench_native_daft.py)执行同一独立Movie Filter／Map／exact pair Join及MMQA／Medical／Cars／E-Commerce文本任务工厂，本地文本用vllm-prefix-caching图，图文用显式附加服务；既有worker／ACK推进实际查询批次，无LOTUS SDK／提示／解析。 | Movie Q1/Q8/Q9-map/Q10及MMQA Q1/Q4/Q6a sf25有具名模型／ACK／物理清理回执；Q1最终结果与全量质量分列。606f Medical Q10及8cd Medical Q4首／同owner重复各1200条已验；8cd MMQA Q2b为325→5／8非空、F1均0；e67 MMQA Q7原625 pair×2、同owner两轮F1均0.1481且两项判定不同；其他新组合不转授，606f Q8另有具名回执。 | “统一语义工程对照”，非作者原版；offline仍逐查询新模型owner；同runner已接附加服务两轮profile，具名Movie Q8、Medical Q4/Q10及MMQA Q2b同模型owner回执各自分列；其他组合仍待验。共享运输／关闭组件不使其成为LOTUS语义路径。 |
| IMLane | [native_imlane_sembench](src/experiments/native_imlane_sembench.py)共用原题／SQL／ACK接完整视觉、多阶段、分支及groupby；TINYINT Filter／Join和DOUBLE Map按真实原生profile保留。作者[Movie Q3](src/experiments/native_imlane_model_query.py)另列。 | 034dc Map／Extract、ecbd TopK样本、807224 Q5单叶各有冻结模型证据；a110完整Q7 sf64的SQL／IPC／ACK及128次VLM通过，City F1=1。4073零模型CSV失败、独立Q3的50%相对误差均保留。 | a110仅关闭准确Q7修复资格，不转授其余视觉／并发。VARCHAR-owned为工程变体，非作者聚合／TopK算法；Q3题目合同参考LOTUS兼容语义，非论文未公开UDF。旧Q1Relation已删除。 |
| Kalypso | 同一[KalypsoRunner](src/baselines/text/frameworks/kalypso_native/query_runner.py)与原算子handler接文本Filter／Map／Join／TopK／原SemAgg，另接明确的文本Extract-via-Map工程适配；Movie records/prefix是不同输入与消息合同的同系统profile。 | Join样本、作者full256 TopK及3500 Q5单层各按原来源验收；e67cc MMQA Q1工程Extract首／同owner重复各25请求原F1均1；3500离线资格失败仍保留，不外推多层／并发。 | 原版semantic-query无实际图像carrier／视觉token预算，本项目图像工程适配未实现；文本Extract已显式映射作者SemMap，非原生Extract，e67cc Q1及f53混合序列内Q1已模型验证，其余组合未验。Filter／Map／文本Extract可由同一owner顺序运行，CPU按query分派；Join／TopK／SemAgg仍各自独立unit；f53 Filter25→Map120→工程Extract25已同owner真机完成，F1=.9167／MAPE60.69%／F1=1，不是每题暖轮或单题多阶段。当前纯文本没有虚构两Filter的需要。旧四个专项Driver类及调用已删除，仍用的records/prefix保留。 |

旧入口的[准确恢复来源](../experiments/reproduction/native_baselines/README.md)与原失败保存；
活动任务的服务器排程由测试会话协调，本页不把已交付候选写成已经实机通过。
[a1105两项LOTUS功能回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-a1105-q8-q10-model)已核原件；
Q8 true same-owner repeat保留prefix缓存及实际线程配置，Q10独立service时钟保留启动／预检分列。
Q8的旧v3区间仍含中间relation快照与stage统计，不能从标签或单次首／暖结果推统一公平排名。
当前resident允许按原题需求预留有限余量，各query可非均等分配；全部新鲜额度在模型前核对，模型／永久总上限独立保留。
119个不同软件方法为116通过、3项依赖或资产未绑定跳过，0模型；实际工作量／受控时钟不变，见[额度边界](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-resident-allowance-headroom)。
当前resident F/M/E已改为完整sink后一次汇总统计，并用浅schema快照替代整表数据复制；
必要请求身份／预算与轻量快照仍在内，cold及其他borrowed调用保持，见[计时收尾](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-join-resident-accounting)。

Q2b旧空Extract补列分支已删除；LOTUS服务、Ray／Daft／IMLane保留原缺列错误、实际记账与关闭，六模块86项软件通过。
已核真实Q2b金标候选未缺失；Q5原sf25在32768／8192下只产生单叶，较大官方scale先核实际分块，
见[真实输入／原生边界](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#mmqa-native-empty-and-real-gates)。

文本check现声明实际runner时钟：原生Ray v1启动在source后、close在sink后，LOTUS resident用v3；
旧isolated无独立关闭前边界，保持None。实际打点和历史值不变；完整Q3软件2000→120→COUNT14及
四模块96方法为93通过、3依赖／资产未绑定，见[预检与复现核对](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#query-clock-preflight-and-repro-freeze)。
Ray非末批终态检查只读新增事件，同owner轮次在source前取事件起点；Daft删除重复同步sidecar，
离线Ray不再落盘完整输出副本；Serve后续也删除逐请求文件与token复制，只在既有终态事件保留关联所需原文，
65个不同软件方法最终通过，见[Serve终态记录](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#serve-terminal-response-evidence)。
b3b7的137及70方法通过，合计去重246方法为243通过、3跳过，
仍未整体对齐计时，见[实际审计开销收尾](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-query-audit-work)。

当前剩余问题按共同边界处理，不能用增加题目入口绕开：

| 共同边界 | 当前源码事实与收尾范围 |
|---|---|
| 原题与实际中间关系 | [`SemBenchTextQuery`](src/baselines/text/sembench_text.py) 已有`bind_execution(multiple=True)`实际算子callback与`bind_original_execution()`完整原函数边界；默认单算子合同供未迁移记录图使用。LOTUS＋Ray已改用完整原函数，MMQA Q2b空／非空第二阶段和Movie quick动态批次有原SDK软件差分；其他后端、分支与未绑定原题仍各自待接线或验收，不从公共函数存在取得资格。 |
| 原生执行与生命周期 | LOTUS＋Ray以query级runtime接实际多批native processor，在完整sink后关闭；分组线程的软件交错检查与4073 Q7目标清理分别解释，不声称跨批模型复用。LOTUS＋Daft沿每批隔离worker，末批完整sink后ACK才关闭；外部OpenAI profile不拥有远端模型，不声称SDK client显式关闭。LOTUS＋IMLane复用多批SQL／ACK及一个模型owner，a110 Q7已验，其余分组／并发另验。Kalypso五handler收拢同一runner，当前受支持纯文本没有虚构第二语义算子的需求；真正图像carrier仍缺，文本Extract-via-Map工程适配及F/M/E跨查询顺序复用已接。独立原生Ray／Daft只复用中性执行／观测与任务工具，不借用LOTUS语义或目标资格。各系统真实owner与不同profile保留，不强行合并生命周期。 |
| 完整查询计时 | 从真实源读取到完整内存sink直接打点，冷启动、服务ready后查询和组件时间分列；版本名不自动授予跨系统可比性。LOTUS＋Ray记`lotus_ray.source_to_sink.v1`，包含actor observer同步事件／输出写盘；LOTUS外部服务记`lotus_service.source_to_sink.v1`，HTTP指纹、图像审计hash及事件编码仍在查询内，后台写盘可重叠，远端READY／启动归属未观测；LOTUS＋Daft记`lotus_daft.source_to_sink.v1`、LOTUS＋IMLane完整原题记`lotus_imlane.source_to_sink.v1`、Kalypso五handler及Movie records/prefix记`kalypso.source_to_sink.v1`，各自包含尚未后置的内部I/O及必要生命周期；独立IMLane v2仍含UDF/provider回执。额外外层预检和driver后置审计分开，但内部审计留存／flush尚待闭合，不能靠旧数值相减制造统一成绩。 |
| 验证与数据 | 先以原函数／SDK 差分覆盖共同执行边界，再对有真实输入且后端支持的完整代表查询验模型、结果关联和物理清理。数据缺口、原生接口限制、代码未接线和目标待验各自保留，见下表与所属原报告；不重复已完成模型请求来增加通过数。 |
| 替代与调用迁移 | 旧 Ray／Daft Q3 专项图和旧 IMLane `Q1Relation` 已退出现行调用。LOTUS外部服务的视觉查询与Q5聚合已共用查询生命周期和原题对象，原两个CLI／worker及旧视觉对象已迁移删除，[恢复来源](../experiments/reproduction/native_baselines/README.md#retired-lotus-service-query-runners)保留。HTTP、records、作者算法和不同生命周期对照按真实职责保留；其他替代路径也须迁移完调用再删除。 |

下表随本页 Git 提交版本化，固定 SemBench `c814e380`。LOTUS 方法接点使用1.2.4；
论文环境1.1.3与官方GenericLotusRunner默认approximate分别记录，不推断论文每题policy。
其他系统版本、模型和参数以各冻结运行回执为准。
“已接线”表示存在完整原查询通路；“软件检查”不等于目标模型通过；目标已验只属于对应
提交／输入／环境。不能把旧来源的目标证据授予当前组合，也不据这些格子计算性能覆盖率。

| 任务能力与官方代表 | LOTUS | LOTUS＋Ray 原生 processor | LOTUS＋Daft 原生 AI | IMLane（接点身份见格内） | Kalypso 作者入口 |
|---|---|---|---|---|---|
| 普通文本 Filter／Map／Extract；Movie、MMQA、Medical、Cars Q1/Q4/Q10 及 E-Commerce Q1/Q3/Q5 | 已接线，软件检查；目标按冻结来源分列 | 已接线，软件检查；目标按冻结来源分列 | 已接线，软件检查；目标按冻结来源分列 | 各场景 Filter、Movie 数字 Map 已接；VARCHAR工程变体的原题单Map／Extract已接，ecbd代表SQL软件已验；其余任务／模型另验，原版字符串输出受限 | 按原字段的文本 Filter／Map 已接；五场景 Filter、Movie/E-Commerce Map 有软件检查，新入口作者模型待验；文本Extract-via-Map工程适配已接，e67cc Q1 sf25首／同owner重复各25真请求F1=1；其他组合另验，未透传结构化格式，非作者SemExtract |
| 文本 exact Join；Movie Q5/Q6/Q7、E-Commerce Q7 | 显式 resident 已接线，N×M／原关系软件检查；实际选列另记，1.2.4不保证多列指令全部入模 | 原生 processor 已接，原 SDK／软件图检查；真实模型待验，旧记录图未接 | LOTUS＋原生入口已接，driver packet／原 sink 软件检查；SDK环境与模型另验，旧记录图未接 | 原版 TINYINT prediction 上的 LOTUS Join 已接；8667样本真实SQL／模型已验，完整输入另验，非作者 Join 算法复现 | 原 CP＋Filter 已接；4f12样本作者模型已验，其他来源／输入另验 |
| 文本完整排名；Movie Q9 TopK profile | 原 quick TopK显式resident已接；cb754的完整256真实模型已验，原parser回退保留；其他来源另验 | 原quick完整函数已接，8行28次软件比较／7批共用query owner通过；目标模型与多批物理清理待验 | 未接线 | LOTUS quick over VARCHAR工程变体已接，原题／Arrow／provider软件检查；11f的8行Linux SQL／renderer门及ecbd的8行13次真实比较通过，full256待验，非作者TopK算法 | 作者 insertion／merge full TopK 已接；11eff的8行及原全量256发生项分别通过真实模型，其余来源另验 |
| 图像单 Filter 与两次独立 Filter；Animals Q1/Q3/Q10、Q7 | 原SDK已接线；a110 Q10原sf64真实64次，完整分组／head1原F1=1；其余按冻结来源分列 | 同一完整查询runner已绑定；051639 Q1 sf64真实64次、COUNT3；4073 Q7 sf64真实两批128次、City F1=1且恢复清理已验；其他题／组合另验 | 外部OpenAI profile已接原消息数组；6c75 Q7 sf64两批128真实终态、原City F1=1；Q1软件及其他组合另验 | 同一完整原题runner已接；a110 Q7 sf64真实SQL／IPC／VLM两批128次、City F1=1；其他题另验 | — |
| 单个图像／文本 exact Join；MMQA Q2a/Q7 | 原函数／原生视觉服务已接线，目标按来源分列 | 已迁移完整原函数及query owner，原消息／关系软件差分通过；dde单批目标成绩不转授当前公共边界 | 外部数组carrier已接；原Q2b双图Join有真实Daft图＋软件API证明，Q2a/Q7自身及目标模型未验 | 同一完整原函数的Q2a/Q7软件差分通过，真实SQL／VLM另验 | — |
| 原关系连接后图像 Filter 和最终 LIMIT；Cars Q3/Q8 | 原函数／原生服务／原评分已接线，软件检查；真实数据／VLM待验 | 原函数／实际连接及LIMIT／评分已接，两题软件差分通过；真实数据与Ray VLM待验 | 外部数组carrier已接；原题数据、连接／LIMIT及目标模型未验，本地字符串profile不支持图像 | 同一完整原函数、连接与LIMIT软件差分通过，保留整数scale；真实资产／SQL／VLM另验 | — |
| 原关系连接后单图或双图 Filter；Medical Q3/Q8/Q9 | 原脚本／原生视觉服务／原评分已接线，db3fe Linux52通过；已查作者checkout缺原图，sf20 Q3原连接0行不授予视觉资格，非空输入／processor／VLM待验 | 三题原函数已接，非空／双图及小gold原失败的软件差分通过；真实数据与Ray VLM待验 | 外部数组carrier已接；原Q9双图10次真实Daft图＋软件API消息等价，其他原题／真实资产／模型未验 | 三题单图／双图、LIMIT前完整输入及小gold原失败软件差分通过；双图processor／真实资产／SQL／VLM另验 | — |
| 图像 Filter／Map／Extract、描述／图像 Join 和图像 self-join；E-Commerce Q2/Q4/Q6/Q8/Q9/Q13 | 原函数及原生服务已接线，软件检查；真实资产／VLM待验，Q8多列意图受SDK选列限制 | 完整原函数已接，单算子及非空描述／图像Join、图像self-join软件差分通过；真实资产／Ray VLM待验，SDK选列限制保留 | 外部数组carrier已接；各题完整消息／评分、真实数据／模型未验；Q8原SDK选列限制保留 | 同一完整原函数软件差分通过；Q4重复ID保留完整Extract及原评分异常，非成功评分；真实资产／SQL／VLM另验，SDK选列限制保持 | — |
| 图像 Join 后对真实匹配结果 Extract；MMQA Q2b | 原完整查询已接线；历史补schema空路径及参考Extract组件5次模型分列；当前删除空Extract补列、保留原KeyError，实际Join→非空Extract仍待验 | 完整原函数、原生JSON及query owner已接；临时2图／5表行的10→5和10→0软件原题差分通过；真实多阶段模型／质量／清理待验 | 外部profile已接；4793旧补schema合同的325 Join全False、空sink F1=0、Extract0仍保留；当前原生空Extract缺列错误／关闭已验，真实非空Extract待验 | 完整原函数／QueryLM／原JSON阶段已接，非空4→4软件差分；真实SQL／VLM／质量及物理清理待验 | — |
| 原分层 Aggregate；MMQA Q5 | 同一原生服务runner已接线，真实sf25软件回放按原来源分列 | 原函数／token计数／层次与query owner已接；051639的sf25单叶1次真实请求、F1=1，当前多层／并发模型及物理清理仍待验 | 原完整函数／实际多批层次算法已接；751b sf25自然单叶首／同owner重复各1真实请求F1=1、目标模板token门／物理清理通过；多层模型待验 | 原LOTUS层次over VARCHAR工程变体已接；807224的原sf25单叶31项LinuxCPU／1次真实模型F1=1；当前多批／视觉／物理清理另验，非作者聚合算法 | 原SemAgg同一runner已接；3500原sf25 CPU20／1次真实归约F1=1；私有owner离线资格失败，原失败保留，多层／并发未验，算法不与LOTUS等同 |
| 文本 Filter 后对实际保留行作图像 Extract；E-Commerce Q12 | 原函数及原生服务完整接线；临时数据软件验证，真实数据／VLM待验 | 完整原函数已接，4→3非空Extract及原空路径KeyError软件差分通过；真实数据／Ray VLM待验 | 外部profile已接，原题软件4→3／原空KeyError与HTTP／ACK通过；native字符串→单text part显式转换，目标renderer／token等价、真实素材／VLM另验 | 完整原函数已接，4→3实际survivors及原空错误软件差分；真实资产／SQL／VLM待验 | — |
| 多分支 Filter／Join 及分组 TopK；E-Commerce Q10/Q11/Q14 | 原完整函数已接线，软件原生图检查；多列 Join 受当前SDK选列限制，真实数据／VLM待验 | 原非空分支／Join和原groupby线程已接并作软件差分；各分支空组合与目标待验，多列Join限制保留 | Q10/Q11沿既有顺序完整查询桥已软件验证，19／25实际请求与原结果／评分／末批ACK一致；grouped TopK Q14未接，真实素材／VLM待验 | 原Q10/Q11分支及Q14四个原group线程的软件差分通过，SQL/config scope保持串行、预算连续；真实资产／模型／物理清理待验 | — |

Kalypso图像相关格的“—”表示固定作者semantic-query前端无图像carrier／视觉token预算，按原生能力留空；
不计作待补逐题runner，不为覆盖率另造视觉机制。其文本Extract工程适配与作者原生算子分列。

表中LOTUS＋Ray／Daft使用原LOTUS提示、解析和算法，不因为底层使用原生AI API就再统计为独立语义baseline。
独立原生Ray／Daft另用任务工厂接Movie Filter／Map及Q5–Q7 exact pair Join，不导入LOTUS语义，目标资格按具名冻结来源分列；
同一工厂的MMQA十二道文本题复用Filter／Extract两类，保留原最终Series／表与后置原评分，当前新组合仍只作软件验收。
HTTP Q3保留LOTUS-compatible合同及外部模型所有权，不能重复计为新独立原生语义路径。
Daft外部数组profile也可绑定其余同形态单算子视觉原题，逐题资产／评分与模型仍未验；
本地字符串profile不借用该图像carrier；attached混合字符串→图像已接，目标renderer／模型待验，分组TopK仍未接。
混合接点在干净导出五个完整模块52项软件检查通过、0模型，真实Daft图使用软件API；原始／effective消息身份于fullsink后核，
不把相同文本当作已证明renderer输入等价，见[混合链](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#daft-native-mixed-messages)。
原生IMLane查询按实际任务合同分列；IMLane的
[`native_imlane_model_query`](src/experiments/native_imlane_model_query.py)通过作者prediction SQL／Python UDF执行Movie Q3，
其不导入LOTUS的题目合同仍明确参考1.2.4 non-CoT规则，不称复现作者未公开的论文Q7 UDF。
该独立Q3的Python计时已拆分为`imlane.q3.source_to_sink.v2`，真实读写/交接保留，outer packet身份审计后置；
UDF/provider同步回执与指纹仍在COUNT内，不能标为统一v3；原生v1 marker单列，无marker不授新版。
冻结来源cf220的目标执行、质量与[原件独立复核](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-imlane-q3-cf220)分列，
当前组合未重跑模型，不能据此称与其他路径统一计时或整体runner完成。

数据状态独立记录：Movie／MMQA／Medical／Animals 已有具名冻结输入和运行证据，具体运行仍须
重绑实际文件；E-Commerce 当前只有临时 Parquet／图像软件差分，真实资产／目标运行待核。
未设置可选真实资产环境变量导致的测试 skip 只记“本次未验证”，不能据此记“缺数据”。
“原版阻塞”限于已固定接口或失败证据；“未接线”不能改记成整个原生系统不支持。
表外题目仍按官方实现和实际接点核定，不能因为原题文件可读就算已经接入。
Kalypso同一runner的Filter／Map／显式文本Extract现允许同一unit顺序运行；原生方法／重试／每query预算及v1时钟保持，
原Movie必需数据与其他场景的SHA可共存且全部核验。97个不同软件方法为95通过、2个旧tokenizer门未绑定跳过；
f53真实三个查询／同模型身份及170个终态已验，Map原始标签造成60.69%误差，执行与质量分列；不是单题多阶段或每题暖轮，见[软件接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#kalypso-text-query-sequence)与[实际回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#kalypso-f53-text-sequence-model)。
Kalypso维护的Filter／Map／Join已允许原题选出零发生项，保留零调用预算、空trace、完整原sink及CPU空计划；
来源和80通过／2项tokenizer未绑定的软件检查见[空输入边界](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#kalypso-empty-frontier)。

本批三条聚合／完整查询接线及225个不同软件方法的219通过／6项环境或真实输入未绑定，见[分路径交接](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#aggregate-system-runners)。软件检查不授予模型或整套完成资格。

IMLane原版Filter／exact Join也已移除非空限制：实际零发生项走SQL／ACK，原函数提前返回则不造算子或SQL；
正预算与实际N分开，38项软件验收通过，Linux薄driver须重链、真实空SQL仍待验，见[零发生项](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-zero-occurrences)。

LOTUS＋IMLane的完整原题profile已整合原生多模态载体和逐批采样；110项受影响软件检查全部通过，各批代表原题与真实模型范围分别标注，见[完整查询](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-original-query)。

后续分支／groupby只需补软件覆盖、无生产修改；受影响原题模块8／8通过，原多列SDK限制、线程关系与目标未验边界见[分支验证](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-branches-groupby)。

后续输入类别也无需生产修改；受影响原题模块12／12通过，包含前8项，不重复累加。
单图／双图、源连接、完整候选、LIMIT前输入及原评分失败见[输入类别验证](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-original-input-kinds)，真实SQL／VLM资格仍待验。

4073的Animals Q7服务器原SQL读取在prediction前触发默认2MiB单行限制，0模型调用，不能解释为系统不支持视觉。
现prepared薄driver按实际最大CSV记录字节绑定reader值，9／9本地读取／build检查通过；原失败保留，
随后a110已完成薄driver重链、Linux SQL／IPC／ACK及原sf64两阶段VLM复验；准确冻结版本及原失败见[实际输入边界](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-csv-record-envelope)与[Q7回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#q7-ray-imlane-frozen-model)。

独立原生Ray／Daft已接Movie Filter／Map及Q5–Q7 exact pair Join；项目JSON任务语义不依赖LOTUS，
Filter来源与身份见[报告](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#independent-native-filter-runners)，Map共用接点与原后处理见[后续接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#independent-native-map-runners)。
7fb4的Q8已取得两系统具名冻结原生图／模型／清理资格；其他题及后续组合不转授，见[原件回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q8-7fb4-model)。
这仍是新JSON工程语义对照，不借用旧LOTUS组合成绩，也不由一次冷查询形成性能排名。
Q10复用同一评分Map及原分组／两位舍入，统一任务准备在模型／扣额／source计时前冻结Movies.csv金标；
缺gold提前失败，真实Reviews鲜读／全量Map与分组仍在查询内，见[分组接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#independent-native-q10-runners)。
Q5–Q7使用同一exact pair任务；完整N²后才去等ID／取head10，计数接口在消息构造前检查额度。
这是公开生成API上的项目pair算法，不称作者Join；原空schema／索引与当前软件范围见[Join接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-join-resident-accounting)。
f83b的Q10全2000条、完整105组及原Spearman已实机验收，两个系统有27条预测／19组均值不同；
原单冷计时含启动与不同内部I/O，无同owner暖轮，见[具名Q10回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q10-f83b-model)。
MMQA Q1/Q4文本Extract沿同一两runner执行，Q1全text后才left join／筛Role、保原director Series，
Q4按原24片名谓词筛选／完整逗号展开与groupby，不按24限制实际请求数。只将text入模，原空关系错误保持；
原Series评分桥及当前验证范围见[Extract接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-mmqa-text-extract)。
MMQA Q3a–g／Q6a–c由单一Filter类接全部原source，两列证据入模后投影原title／Airlines列，
保原索引／重复／空值及各变体gold，不套Q4片名条件；源码及软件范围见[Filter家族](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-mmqa-filter-family)。
f596的Q1具名模型已完成各2000终态及原最终head5，但最终F1不能替代全量分类质量；具体首次失败回执未定位，
见[Q1原件范围](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q1-f596-model)。
后续608e20的Q9 Map各256、原last-wins评分与8d071的MMQA Q1 sf25各25真实终态已核；
MMQA原Series索引[8]及后置F1=1不表示全部25条抽取正确，Ray首轮外层清理失败及精确恢复／未知原因保留。
原LOTUS的f26fc MMQA Q6a sf25首／同owner重复各25次请求、原F1均1，结果缓存关／prefix开，
见[具名回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-kalypso-text-model-20261114)；不转授其他LOTUS后端或正式性能比较。
见[Q9 Map](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q9map-608e20-model)与[MMQA Q1](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-mmqa-q1-8d071-model)，不转授当前新族、视觉或warm。
LOTUS＋Daft外部服务profile已移除纯文本的无依据拒绝，复用同一完整查询／原生图／worker和ACK；
普通F／M／E／Join及原Q5单层／多层软件差分范围见[文本接点](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-daft-attached-text)，目标模型／远端tokenizer另验。
Medical原评分器会原地改列名和诊断文本，共享评价边界现只给其副本；原分数保持，实际完整sink不被评分污染。
LOTUS＋Daft空批本身不创建native worker、后置HTTP审计跳过空批的修复仍保留；当前Q2b原生空Extract在最终投影抛缺列错误，不再补schema造成功sink；
五模块53项软件通过，原首轮审计KeyError与已完成空sink保留，模型资格待验，见[修复记录](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#daft-empty-native-successor)。
独立Medical Q1/Q4/Q10由同一任务类与原两系统runner接全量文本，原普通Join／Series／重复加权平均和诊断关系保持；
原gold在模型前冻结，原评分的小写转换不污染完整sink。七模块139项及六条实际driver软件流程通过，非本组合模型资格，
见[Medical家族](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-medical-text-family)。
独立Cars Q1/Q4/Q10和E-Commerce Q1/Q3/Q5复用同一任务工厂及两系统runner，九完整模块159项全通过、0skip／模型。
Cars真实scale文件／原普通Join／Q4的2026常量及投诉权重保留，E-Commerce保全descriptor和原嵌套谓词／SQL评分；
真实Cars／E-Commerce资产与本组合模型资格仍待核，原生不支持与缺数据不混同；E-Commerce Q7现已接同任务的exact pair工程方法，原SDK算法身份不转授。
见[两族接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-cars-ecommerce-text)。
Q7保完整N²请求及原index标签普通回连／字符串ID，111项软件及两driver各9发生项通过，原F1／空结果保留；见[配对接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-ecommerce-exact-pair)，模型与真实资产待验。
Kalypso的文本Extract现在用显式sembench-extract-map工程profile复用作者SemMap和同一系统runner；
MMQA Q1/Q4、Medical Q10、E-Commerce Q3及Cars Q10原完整关系／评分已做软件差分，非逐题执行器。
93项软件通过、2项旧tokenizer门未绑定跳过；图像carrier／视觉token预算在固定作者前端不存在，本项目图像适配仍未实现。
e67cc的MMQA Q1 sf25首／同owner重复各25请求、原F1均1，真实作者CPU规划与资源清理通过；其他文本Extract不转授。
新profile不冒称原生Extract、不透传JSON格式；见[具名模型回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-kalypso-text-model-20261114)及[来源、实现与范围](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#kalypso-text-extract-map)。

同一Ray runner的[Serve v2隔离caller](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-ray-serve-isolated-caller)
在model_python持有原生deployment／processor，driver保留任务／原评分；两轮fresh查询、有限预算及原进程树监督不变。
必要IPC留在独立v2查询内，caller／部署启动另列；当场model actor与实际EngineCore的PID／birth缺失或空查询均不授予暖轮。
初始55项软件范围保留。7aa61服务器配置／120条完整模板token门通过，但真实启动因observer与原生worker hook冲突失败，0次生成；
现用原生模型初始化callback替代自定义worker hook，70项受影响软件检查通过；751b Q8已有240次真实终态、同owner暖轮／token资格，原外层清理失败恢复分列；
其他组合待验，见[新回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-serve-q8-751b-model)；
见[失败与修复](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#ray-serve-native-hook-repair)，不借offline或Daft具名成绩。
独立Daft同runner的[attached-session](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-daft-attached-session)复用builtin OpenAI graph／既有worker和ACK；
两轮fresh查询共用永久HTTP预算，客户端worker换代而外部服务不由driver关闭，缺新鲜模型进程身份时warm=None。
初始四模块120项软件范围保留；606f Movie Q8后续具名同模型owner两轮已核，其余组合不转授。当前原生静态system／消息layout限制按批次保留。
独立Ray的Animals Q1/Q3/Q7/Q10沿同一任务／runner，真实图像处理保留，Q7两遍完整source共2N请求。
现官方输入身份仍为原sf64 manifest；0d0cc独立JSON Q7已有128次真实终态／F1=.8，其他题不转授；Daft本地字符串profile明确拒绝图片消息，
见[图像接线](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-animals-json-filter)。
606f Medical Q10原生Ray冷1200条、Daft同模型owner两轮各1200条，原F1及七项回答差异保留；只授该文本Extract组合，见[回执](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#medical-q10-606f-model)。
具名[Q4](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-mmqa-q4-8d071-model)及[Q7／Q6a](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-q7-q6a-0d0cc-model)模型回执按原版本保留质量、失败与单冷口径，不代表当前Serve模型复用或全部家族通过。

两系统现以同一任务工厂／query_batches闭合多阶段：Animals Q7为N／N完整Filter，
新增Medical Q3/Q8/Q9、Cars Q3/Q8、E-Commerce Q2/Q4/Q6/Q8/Q9/Q10/Q11/Q12/Q13、MMQA Q2a/Q2b/Q7；
每系统仍一个runner，依赖批次不新增执行器，完整关系结果后才最终ACK／关闭。
212项整合及后续61项受影响软件检查通过，真实数据／VLM与本组合资格另验；
E-Commerce Q10/Q11后续已按独立Filter分支及真实Join依赖接入；135项相关软件回归与六条实际driver软件查询通过，配对在展开消息前检查实际额度。Q14及其他未接算法仍pending，软件与原生生成接口不等于作者算法。
见[实现／缺口](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-query-batches-visual-families)。
LOTUS＋Daft Q2b的4793回执为325 Join、空结果F1=0、Extract未触发；不是独立原生Daft结果。
LOTUS＋Daft6c75 Animals Q7与独立JSON Daft606f Q8同owner回执保持各自身份、质量和计时，
见[模型原件](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#daft-6c75-4793-606f-model)。

8cd独立JSON MMQA Q2b已真实非空执行，Ray325→6、Daft首／同owner重复325→5／8，原F1均0；
所有五个金标配对均被原JSON答复判False，输入／答复／完整sink回放一致；见[非空执行与质量分析](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-json-q2b-8cd-nonempty)。
这不转授LOTUS系列的原语义非空链；8cd Medical Q4三轮各1200终态、原均值误差6.5%–6.9%，
Ray原清理失败及恢复分列，见[完整Join／均值](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#native-medical-q4-8cd-model)。

LOTUS原服务及LOTUS＋Ray／Daft现按已知必要工作准入，不再把maximum_model_calls当最低门槛；后继实际调用仍受永久预算约束。
Q12实际4→3可用7次额度；MMQA Q2b空Join仅消耗实际候选额度，原生投影错误仍保留。旧139项按当时合同记录；当前原生错误及关闭另验。
LOTUS＋Daft Q10/Q11已由现有顺序桥完整软件验证，覆盖表更正为已接／目标待验，不另写执行器；
见[原题预算与Daft分支验证](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#original-query-minimum-budget)。

IMLane原题runner已移除旧Q1专用schema和默认task／固定25次分支，现行Map／Extract使用显式共享task；
预算为覆盖实际N的有限上界，允许余量但不增加调用。受影响软件验证与准确旧恢复树见
[配置收尾](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-shared-task-profile)，旧服务器配置及模型资格保持原版本。

Kalypso现行runner的独立时钟已被公共读取器识别，真实起止及旧receipt不改写；受影响76项为74通过／2项真实tokenizer未绑定。
同次固定源码核对澄清了纯文本与真正图像／Extract多阶段的接点边界，见[共同边界](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#kalypso-query-scope-clock)，没有为此新增逐题或阶段框架。

剩余工作按共同边界闭合：已接线原函数处理真实中间关系，额外预检之外的完整查询计时按版本
记录，再用真实资产及代表查询验收。完整 SemBench 入口及已核活动服务器绑定走共享路径。
旧 Ray Q3 专项图已退出维护，原题 Q1–Q4 差分覆盖同一 prepared processor；
原生默认重试／caller runtime 的历史身份从[准确来源](../experiments/reproduction/native_baselines/README.md#retired-ray-q3-native-binding)恢复，
没有把不同生命周期做成等价别名。Daft 专项 worker 也已移除旧 Q3 schema／执行图，
其结果、关闭失败和部分构造失败用例迁到现行 prepared 路径；独立 HTTP profile 保留。
Animals 的 Ray 零模型捕获／processor门也已按原题元数据消费一遍或两遍完整输入，移除资格工具
写死Q7两遍的限制；四题7图软件样本与受影响28项检查通过。冻结60c875随后通过Linux实际processor门
及Q1官方sf64的64次真实调用，count=3、原误差0；冷source→sink包含原生启动，其他单遍题不转授。
现有四题随后共用v3起点，输入／像素／源码预检及LOTUS客户端构造在真实manifest扫描前，
原生启动和必要阶段间清理仍在完整sink区间内；35项不同本地软件检查通过、0模型，
78d随后通过Linux35及原sf64四题完整软件捕获，共320个软件发生项、0模型／0Ray runtime；
两个预检前harness失败保留。新时钟真实模型仍待验，历史模型记录不回填，原入口至sink宽时钟独立保留。
原生LOTUS服务观察已按SDK输入顺序返回的completion ID关联HTTP尝试，解决重复输入不同答复的
逐发生项观测缺口；本机56通过／1资产未绑定跳过，冻结40c477的Linux57项全部通过，0模型。
旧Q2b的325次Join、空Extract、F1=0及103发生项原答关联缺口保留；不回填旧日志或授予非空第二阶段GPU资格。
非空Extract的参考阶段准备已复用原查询／SDK，官方正例只作显式软件Join夹具；捕获后即停止，
不产生查询结果／质量，不代替真实Join。11f参考组件随后完成5次真实Extract，原JSON及发生项关联通过；
首轮harness配对断言错误与旧完整查询失败保留，不能据组件结果声称真实Join→Extract通过。
IMLane prepared VARCHAR现将原call_id交给provider本地回执，RPC仍由原batch ID与位置关联，
没有新增模型请求字段；取消Q1消息内容必须互异的接线限制。
共享审计只按occurrence与原batch位置核对。冻结417457的当前kernel已通过真实VARCHAR SQL／IPC／ACK，
相同消息不同call_id与不同软件答复保持；0模型。该轮82项中诊断单项失败，3d9fb8测试屏障修正后
Linux单项1／1通过，旧失败保留；没有修改DSO、C++ driver、消息、生成参数或旧模型结果。
[TopK新接点](integrations/imlane_duckdb/README.md#lotus-quick-topk-与-varchar-prediction)
复用同一模型owner／VARCHAR prediction，每批原生SQL关闭计入完整查询；原quick算法、原评分及
失败留证由软件检查覆盖；11f的8行派生输入通过7批真实SQL／IPC及renderer门，0模型、非完整256；
ecbd随后完成同一8行发生项13次真实比较／完整原sink和原答回放，资源清理通过，非full256或作者TopK算法复现。
VARCHAR工程变体的单Map／Extract已使用同一原题任务接口，替代Q1专属关系类；旧Q1配置仅格式兼容。
实际N与原JSON约束由任务决定，输入／参考token检查前置；原查询真实源读取与完整sink仍在查询内。
四场景五个原题的临时输入／原SDK完整关系差分及受影响12项软件检查通过、0模型；
不转授所有新任务或模型资格，不把该单算子路径说成混合图已接通。ecbd的12项Linux检查和原Movie Q8／MMQA Q4
的120／24个真实SQL软件答复随后通过；Map私有参考harness首轮错误保留，不改生产代码绕过。
034dc随后完成同一Map120／Extract24的144次真实请求，unknown／重试0，原答／完整sink及物理清理已验；
Map相对误差与Extract原F1=0.4865保持，不据执行完整宣称质量达标或转授其他任务。
其他已就绪项继续排队；不为每个格子另写执行器，不只积累软件通过数。
已核对的普通文本题主要是单个语义调用；E-Commerce Q12 的后续 Extract 明确包含图像，
已由完整原生 LOTUS 服务入口运行，不能当纯文本或假定其他后端已能执行它。

宿主资源边界不再仅凭 Python 进程存在判定冲突：原生任务、GPU 使用及不完整清单仍拒绝，
未分类 Python 保留观测，且阻止仅凭时间窗口归属残留信号量并清理。
`933157666` 已通过目标 22 项 CPU 检查和实际零模型 stage，外层与阶段边界使用同一策略，
资源／租约释放通过；[资格与迁移范围](src/baselines/image/ray_animals_q7/README.md#evidence-and-timing)
保留旧失败、不声明 CPU 无干扰或新 Ray Q7 模型资格。仍须显式迁移活动外层 owner 的 loader，
不能只更新 stage。已冻结 helper 仍拥有租约和清理职责，不按旧文件名删除。

共享题目准备层的原生 exact Join 现在绑定各自左右表和原指令字段，预算为 L×R；
quick TopK 绑定原指令引用列，保持 K=N、原 pivot 和 N(N−1)/2 比较上界。
`4e1f8a44` 已通过目标 26 项软件／原 SDK 检查，不授予新题目或模型性能资格。
Ray Q7 `d8308aac` 从冻结源读取 N、使用 2N 预算，保持两次原生生命周期和第一阶段必要释放；
目标原关系 N=1/7/13、capture、128 项 token/pixel 及 hook 门通过，模型为 0。
[同轮原件恢复说明](src/baselines/image/ray_animals_q7/README.md#evidence-and-timing)
保留收尾复制 socket／跟随链接造成的旧 failed；独立恢复不把它改成通过。
三候选的实际组合另通过 69 项受影响本机 CPU 回归，0 跳过、0 失败、0 模型调用；
执行和测试文件保留各已验候选的准确字节，不将组合检查改记成新的 Linux／GPU 成绩。

IMLane Filter 不再用历史输入行数作为配置白名单；完整原 occurrence 数仍须等于显式冻结预算。
现有 `freeze-config` 零模型入口从原题准备生成同 schema 的 SHA、实际 occurrence 数和首次观察的
原始列顺序；正式 Filter 复用共享 prepare 消费冻结顺序，不再重复 formatter 接线。
Filter 后端按原操作形态验收，不再另列题号／字段分支或 Medical 固定 scale；上下文和回执
使用本次实际生成上限。任务层继续拥有原题与后处理，原生 TINYINT 驱动、DSO、调度和 ACK 不变。
运输字段改为源索引／模型输入 cells，完整 messages/token 与原关系单独核验；新增源码不继承
历史 Linux SQL、GPU 或性能资格。零输入仍受现有正预算/driver 合同限制，明确留待支持。
生成配置不是 GPU 运行授权；本地原源码／SDK／tokenizer 检查不转授 Linux SQL 或模型资格。
prepared Q1/Filter 的薄 driver 构建身份补齐源文件及编入的 ACK header，旧记录保留历史资格，
当前重用需重链薄 driver 补齐身份。详见[边界说明](integrations/imlane_duckdb/README.md#filter-源规模与构建身份)。
这不改变原生算法或配置声明的采样、源列顺序、DSO 和运行额度；实际 cold/ready-source 时钟仍分列，
原任务形态检查在模型前、fresh source 在 ready 后。`a5353fc25` 已完成目标零模型
Q1 SQL 25、Filter Q3a 25／Movie Q2 120 的完整 sink、ACK 和退出，以及新旧构建记录门检；
新 freeze 对同 seed 原 formatter、旧冻结配置对历史参考的全消息／token／源位置分别相等。
两份既有 DSO 未重编，未运行新模型。source 检查后的外层未知 Python 拒绝及未归因租约消失
仍保留；后续只续未运行的两项比较并清洁退出，详见同一边界说明，不将该失败抹平。

通用 Filter `016a83529` 已另行通过目标 Movie Q2 完整 120 项原生软件 SQL、Arrow/IPC、
原 sink/ACK 和外层资源清理，未重编 DSO、模型调用为 0；其余任务仍按各自精确源码证据判断。
IMLane Q9 Map `0e80e7fa` 的原版 DSO 软件 SQL 256 项和独立重复 ID 样本 6 项通过，
完整消息、关系、sink/ACK/退出已核；运行 token 为软件占位，不能转授模型 token 或性能资格。
Kalypso Q9 Map `0afa54134` 在原 `05024` 环境通过 N=256／上限 512 与样本 N=6／上限 12
的 tokenizer/planner/request 门检，未执行模型。首次缺 Git 身份的部署失败保留，修正部署后通过。
这五项限定目标门检与一项失败共 363 份原件已校验，不代表所有题或整合树的新 GPU 实测。
三项合并后的 64 项受影响 CPU 回归全部通过、0 跳过；19 个执行／测试文件逐字匹配上述
已验来源。整合仅合并说明中的资格边界，没有新增模型调用，也没有把软件 token 认作真实 token。

| 部分 | 当前实现 | 未完成或未验证 |
|---|---|---|
| 文本语义与查询 | Filter／Map／Extract 保留原提示、解析、顺序和关系屏障；原生系统各自拥有执行与调度 | records 的 Join／TopK 未接入；完整查询与质量按各路径报告判断 |
| 普通与有限链执行 | 普通 Filter、融合批和逐记录查询各有 owner；接纳、poll、ACK、取消和结果交付使用现有实现 | 不同组合的真实模型与最终清理尚未全部覆盖 |
| 容量与存储 | 请求额度、在途 work、输入／结果字节、驻留与未消费结果分别记账；省略值从声明解析 | 没有自动取得物理 RAM 或完整查询内存的能力；逻辑额度不是硬件容量证明 |
| 模型与期限 | vLLM 可选参数省略时交引擎解析；整查询默认不设总期限，显式值保留；启动、通信与关闭分别计时 | 真实在途到期／abort 和重复关闭仍须匹配后端验证 |
| 准备与反馈 | 无 B、rolling、交错／事件驱动准备及显式请求反馈可按支持条件组合 | 动态 B／C 联合控制未实现；功能检查不证明吞吐改善 |
| 常驻查询 | 模型 owner 可复用；内部反馈逐查询绑定，用户显式配置保持 | 同一 owner 连续 Extract 的真实 SDK／模型验证仍见对应报告，不外推到任意并发查询 |
| 基础多 Job | Session 保留固定份额、普通轮转和统一资源责任；共享策略通过显式研究适配接入 | 已有模型五请求中一条 unknown 的失败保留；不据此声明完整多查询组合通过 |
| 图像 | 文件图像、LOTUS 检索及已有阶段执行分别维护；公共 CLIP 组件不依赖 PG 数据工具 | 软件替身不证明真实 Ray／模型进程退出，增量图像与阶段执行不互相替代 |
| 观测 | 查询时钟、事件、模型复用、资源采样和额度由各自模块维护 | 事件重叠、API 断开和引用释放不等于物理并发、进程退出或性能收益 |
| 原生对照 | LOTUS、Ray、Daft、IMLane、Kalypso 保留各自原生图、作者服务或方法职责 | 输入、生成、缓存、生命周期和实际版本匹配后才能比较；不转授其他路径的验证结果 |

Daft 原生 AI 的[完整原题查询入口](src/baselines/README.md#daft-native-prepared-sembench)
已从历史私有接线整理到正式源码，保留分离依赖和原生执行器，并将完整原 sink 与模型关闭分开计时。
`ffbc352ac` 已完成 Q3a25/Q4 24 的真实单冷资格；最初的 main 整合保留相同 Daft 执行代码，
原整合22项CPU回归通过；后续sink计时修正的19项检查与Q1/Q3a/Q4完整软件回放见[报告](../experiments/results/diagnostics/daft_native_semantics_20261110/README.md)。旧真实时长含部分审计，不能追认为修正后的统一口径。
Movie Q1/Q2/Q3 已开放正式单算子查询绑定，复用同一 Daft native AI、原 parser 与完整 sink；
`48a6b42d0` 已完成 2000/120/120 次真实单冷及逐条审计，完整 Filter 后的 head(5)／COUNT、
原提示、重复 occurrence 和全阴性 schema 由[原查询回归](tests/experiments/test_sembench_text_original.py)保留。
其余题按各自来源判断；同 owner 暖轮仍未取得资格，不把旧版本真实记录称为整合候选实测。

LOTUS 的[显式 resident 文本入口](src/baselines/README.md#lotus-native-resident-sembench)复用原生stage、
模型owner及既有生命周期，默认isolated保持；当前源码/输入/模型绑定通过配置声明。
124个不同CPU检查及Q3a50/Q4 48条完整软件冷暖回放通过，e097目标环境导入/绑定及CPU监督通过；准确范围按[报告](../experiments/results/diagnostics/lotus_mmqa_q3a_native_20261110/README.md)分列；
已完成的Q3a50次真实冷暖仍绑定ee625/fd06，不自动成为新正式候选的模型结果。
完成后的 session 报告按实际大小读取，配置上限与符号链接／非有限数拒绝保持；
`50eae2e04` 已完成 Movie Q10 冷暖各2000次及离线审计。当前合并不重跑模型。
Movie Q9 的[显式作者 TopK 候选](src/baselines/README.md#lotus-movie-q9-topk)复用 resident owner，
保留原 quick、K=N 与比较预算，和 Q9 Map 分开验证；`c9cadb1df` 的 8 occurrence 样本实际用了 20 次比较，
未超过 28 次上限，不授予完整 256 项排序资格。

Movie Q5/Q6/Q7 的[共享原生 Join 候选](src/baselines/README.md#lotus-movie-exact-join)复用上述 resident，
每个原始有序 pair 独立关联和扣预算，原 parser、完整 Join 与后置筛选/限制不变。
[首批 CPU 报告](../experiments/results/diagnostics/lotus_movie_join_native_20261111/README.md)
只覆盖每题 8×8 软件答复回放及 sink 前后边界；没有新的模型、GPU 或完整 256×256 资格。

LOTUS 原视觉查询与 MMQA Q5 聚合在完整原关系返回时记录内存 sink；维护版 v3 把本地 LM／tokenizer、
源码校验／模块绑定和输入身份预检放在起点前，真实数据／图像读取、原生连续算子和完整后处理仍计入。
Animals 的额外像素身份审计在 sink 后复核，不取代原 ImageArray 解码；具名官方 sf64 身份仍固定64条，
软件 fixture 不再受64条上限。原 SDK、HTTP 软件答复与受控时钟检查覆盖两次独立 Filter、Join→Extract、
真实中间关系和聚合分层；Q5 原 sf25 资产使用软件 tokenizer／生成回放，不证明目标模型的分层请求数。
目标环境验证待完成；旧 `query_seconds` 和历史 v2 时长保持。

[Kalypso 原生客户端](src/baselines/text/frameworks/kalypso_native/README.md#内存结果边界)新增完整原关系
就绪时间，维护版公共报告从显式 `source_started_ns` 到内存 sink 计算 `source_to_sink_seconds`；
旧 caller 起点另列，不回填历史值。trace 审计、结果编码、
评价和证据持久化在该终点后。旧 `answer_ready_s` 保留，缺少新接点的路径明确留空。
共享接点的 29 项 CPU 检查与公共时钟／生命周期的 42 项检查通过；后置失败仍保留 sink 并报告失败。
现有 Q10 Map 与 Q3b–g 私有入口已给出新副本迁移补丁；4000 条 Map 保存答复和 12×25 条 MMQA
归档响应的完整结果／评价回放相等。Q8/Q9 Map、Q3a/Q6a–c、Medical Q1/Q4 的迁移又通过 16 轮保存
响应回放与 18 个失败边界检查。原 Map 最坏 2N、Filter N 的实际请求额度保持。作者 `05024f0` 的
Linux 原环境已通过零模型 native import／request object 门检，CUDA 未初始化；此前失败来自门检
导入次序，按既有服务入口修正后通过，不是作者依赖缺失。各题目标 sink／清理按上述隔离组合报告和准确原件分别核对，
该门检也不授予作者原版结构化 Extract 资格。Q9 本批独立全输入 hash 未知，原 occurrence／必要 CSV／结果相等，
不能以其他输入身份补齐该未知项。这些 CPU 结果不是新 GPU 或性能资格。

[Kalypso Movie Q9 Map 维护接线](src/baselines/text/frameworks/kalypso_native/README.md#movie-q9-map-维护版接线)
已进入原 query/server/CPU CLI，保留完整 256 发生项、原评分／排序／评价和作者 length→8192
重试，物理上界为每题 512；不是 TopK。新增本地完整保存输出／token／时钟回放通过，但不继承
旧模型成绩。204faa 的目标 CPU 预检暴露作者环境无 pandas 的边界错误，修正版把原查询准备
放回 driver Python，作者 CPU CLI 仅消费摘要绑定的完整输入；不安装或混用环境。协调器已报告 `2e2616a5` 的 Q9 目标冷暖及逐条审计通过，
同源 Q1–Q4 冷暖共4720次也由协调器报告完成，但 Filter runtime 完整token数组未采集。
这些结果不转记当前组合源码，逐题来源见上述报告。当前输入完整 SHA 独立绑定，
不补写旧 Q9 缺少 full-source hash 的历史未知项。

文本预检按原查询实际产生的算子检查后端能力，不再按 Movie Q5/Q6/Q7 题号判断 Join。
普通 Filter／Map／Extract、显式 LOTUS resident 的 Join／TopK、Ray 原生 processor 和独立 Daft 原生
exact Join 按各入口分列；prepared 请求入口对 TopK／Aggregate 等不支持算子在读取 stage 前明确拒绝。
这不等于任意多算子查询已经接通，
原项目的查询／系统职责及与本地执行比较的区别见[接入职责](src/baselines/README.md#sembench-原项目与本地接入职责)。
共享普通算子绑定新增 Map `suffix`、Filter／Map 的 instruction keyword 及 Extract 的
`input_cols/output_cols` keyword，保留同一准备、parser 和关系恢复；真实 SDK 的消息与结果差分
覆盖命名列、嵌套对象、重复 index 及碰撞字段。E-Commerce Q1／Q3／Q5 现补齐原 Parquet 读取、原查询
及 TOML gold SQL／官方 F1 或 adjusted Rand index 评价，继续使用相同后端接点；本地临时数据的原 SDK、
记录链与 prepared 差分已覆盖完整关系和失败边界，真实资产／目标模型未验。
Cars Q1／Q4／Q10 也已绑定原 CSV、原函数及原评分，共用同一 Filter／Extract 和后端接点；
full_data 标签只供评价，既有 sample 的尺度只选择原路径，不充当模型调用数。
软件对照保留重复投诉及原 Q4 加权查询／去重车辆金标的差异，真实 Cars 资产与目标环境待验。
IMLane Cars Q1/Q4、E-Commerce Q1 复用同一原版 TINYINT Filter、owner 和完整 sink ACK，
仅增加原场景文件绑定；原 formatter/tokenizer/Arrow kernel、重复发生项和空结果软件检查通过。
Cars scale 只选原 CSV，不等于模型行数；Extract/Map/Join 的尚未接线状态不由 Filter 扩展而改变。
Cars 新增[原CSV准备](src/baselines/README.md#cars-original-csv-preparation)：已有四表元数据可直接复用
官方分层抽样／投影和完整标签，不把图像／音频 payload 下载作为文本查询准备前提。
临时表与真实CLI已有软件检查；真实元数据、媒体和各题目标资格仍分别核定，不生成替代金标。
Cars Q3／Q8 也复用原生视觉服务，保留完整图像发生项及最后 LIMIT；原 car_id／image_id 标签筛选与
评分独立于计时，原图像路径不在执行时改写。固定 LOTUS tree 缺 Q2/Q5/Q6/Q7/Q9 查询文件，
该官方实现缺口不等于其他系统不支持；不补造音频查询来宣称覆盖。
共享原生 Join 接收多列 `:left/:right` 引用及 positional／keyword 指令，E-Commerce Q7 复用
同一 resident、原价格选择、N×N self-join、ID 拼接与 F1，没有单独执行器或候选预筛。
IMLane 也通过独立 `text_join` schema 将同一原消息和完整 pair 发生项交原版 TINYINT prediction；
复用既有 owner/SQL/ACK，由原 SDK 恢复 Join／原题 sink，原始答复和实际选列在 sink 后审计。
原版 DSO/kernel/C++ 未改。冻结 `8667d526c` 已通过[真实 Linux SQL 三门](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#imlane-boolean-join-sql)：
完整64条软件答复、8×22中间关系及 sink ACK，缺行／重复 ID 在任何 AI query／provider 请求前拒绝；
复用既有 patched 构建的 `52bf5763` DSO，0模型，资源与租约闭合。随后同一冻结来源完成64次
真实请求、完整8×22关系／空sink及逐条消息／token核对，原F1=0；单样本资格不外推全量，零候选仍是薄 driver 接线限制。
已核当前 LOTUS1.2.4 accessor 实际每侧只选一个 Series；此前多列软件检查证明与原 SDK 一致，
不证明全部声明字段进入模型。现在 `native_join_inputs` 记录真实选列／遗漏字段，保留原生行为；
完整多列指令载荷在该原生接口中不能表示，不能仅记作待跑模型。四个独立进程的hash seed检查还确认
实际所选字段会变化；截至上游`aa81339c`，Join与列解析文件仍与1.2.4逐字相同，升级这两处不能解决。
固定源码与软件观测见[原生选列限制](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md#lotus-native-join-projection)。
上游1.1.3也有每侧一个Series及同类选列限制；不能把版本回退当成修复。Ray／Daft exact Join 复用
同一原 SDK 消息／parser／关系和原题，保留该限制；原消息准备及 sink 重建的适配成本实际计时，
后置消息核对／选列披露不进 sink。调用预算从真实 L×R 取得，未开放 TopK 或混合算子图。
Movie 的 exact 是显式比较 profile，官方 runner 默认 approximate，两者及未逐题核实的论文策略不混同。
四个文本场景现于任务准备阶段绑定官方函数，实际查询仍重新扫描数据；源码 Git 校验和模块加载
不再每次嵌入执行区间。原题、原参数及已有独立调用入口保持，原 SDK／prepared／resident 软件回归通过；
这不扩展单算子桥的多算子能力，也不转授新的目标模型或完整公平计时资格。

共享 Movie source 已移除三列投影，完整保留官方读取的 CSV 关系；只有算子引用字段进入模型消息。
原始11列样本的 Join 中间关系恢复22列，未引用元数据不再丢失。受影响131项软件检查为128通过、
3跳过；Daft／Ray 各64条历史真实答复经原完整 CSV 回放，消息、本地冻结 token 数组、完整中间关系、
最终 sink／评价相同，新增模型调用0。旧 `c39d96347` 模型运行仍属于三列投影来源，耗时不回填到
修正路径。冻结 `4f12b556e` 随后通过 Linux 完整源关系门及原作者 Movie Q5 样本8×8的64次
真实请求、完整22列中间关系／原 sink／评价重放和物理清理／租约释放；原件已恢复并核验。
原生8 token Filter与判定保持；实际消息摘要、CPU完整token数组已核，GPU实际token数组、usage、
价格与kernel计数未观测。资格只属于该来源／样本，不转授全量、其他后端或后续TopK组合树。

LOTUS resident／视觉／MMQA聚合、旧Ray prepared／Animals、Daft prepared、IMLane Filter和Kalypso各路径
按其来源记录外层source/sink版本；部分仍保留`sembench.source_to_sink.v3`字段，但不能据此推断内部审计全部排除。
当前完整LOTUS＋Ray入口已改独立v1，原生IMLane Q3为独立v2，具体残余见上方能力表。共享文本身份在起点前及sink后核对，
完整查询真实读取及最终关系处理仍计入；旧宽时钟和 v2 不回填。LOTUS 可显式
`model_initialization=before_queries`，调用原 owner 初始化后执行 `ready_first/ready_repeat`，没有额外模型预热；
默认仍在首次查询内启动。已就绪路径记录实际就绪观测，Ray／Daft 仍是包含启动的原生单冷路径。
维护版 IMLane Filter 与 Kalypso Movie Filter／Map 也已移出额外预检：前者在起点前准备源码、
审计 token、预算及上下文检查，区间内保留执行消息、native SQL 和原完整 sink；后者前置
manifest／源码绑定，仍新鲜读取原 Reviews CSV、生成作者必需输入并执行原关系。
原版DOUBLE Q9 Map的源／消息物化与额外preflight也已分开，完整sink立即发布全局ready，
保留原phase-entry含启动宽区间；034dc的48项Linux和原版256项软件SQL／提前READY通过，
首轮SQL后私有类型断言失败及离线原结果复核分别保留；0模型，DOUBLE新时钟真实模型仍待验。
Kalypso另保留caller→sink，不改旧receipt或估算扣除。各v3目标范围按冻结来源分列；
其余专用／私有入口的旧时钟按
[时钟范围](src/baselines/README.md#完整查询时钟与仍未统一的路径)分列。
服务器冻结 `d55e9229` 的112项CPU检查通过；IMLane Q3a完成25次请求、原F1为0.9167，
5.4116秒保留为含文件校验的旧区间。Kalypso在prompt目录冲突处失败，请求0；这是运行绑定问题，
不代表原生不支持。Join的8行64对只完成CPU及真实renderer核对；E-Commerce真实数据在已查位置缺失，
多列原题和全量目标运行仍未验证。上述结果不转授本批v3或性能排名资格。

Kalypso Movie Q8/Q9/Q10 现由同一个 Map 接点绑定原题，保留作者提示、动态输出上限、
length→8192 重试、原 parser 和完整关系后处理。Q10 另绑定原 Movies 评价文件；调用数从原题选择取得，
不设固定样本量。候选 `e5ea4842f` 的既有 48 项 CPU 检查与 4000 条保存答复回放为软件证据；
交接后的隔离组合另通过 22 项零模型回归，覆盖三题的小规模原查询／评价及 CLI 交接。
冻结候选 `e5ea4842f` 已由独立服务器会话通过 Linux 作者 SDK／planner／request-object 及完整 token
检查，合计2376条，模型调用0、未初始化 CUDA。
当前分支随后修改了共享查询及 Map 字段绑定，整合后的目标环境验证仍为 pending，不转授该候选或旧 GPU 结果；
查询终点仍是完整内存关系，审计、评价、编码、持久化和关闭位于该终点之后。

同一 Kalypso Map driver 现在按原题引用字段绑定，不限定 Movie 题号／`reviewText`／数字结果；
新的 `sembench-map` spec 直接指定原 scenario/query，旧 Movie 名称仍使用它。E-Commerce Q5 的
完整多字段描述、原分类列与 ARI 已有本机原查询／SDK／作者模板软件检查；结合 Movie/owner/计时
回归共 56 项不同检查通过，0 跳过、0 模型。真实 E-Commerce 数据未齐，作者 planner／模型待验；
Extract 的结构化输出、连续算子及其他未接线能力不因此获得支持。
来源、输入表示与边界见[Map 字段绑定](src/baselines/text/frameworks/kalypso_native/README.md#按原题字段复用-map)。

Kalypso 的 `sembench-filter` 复用相同原题／字段输入和 CPU 交接，只有一个 Filter driver；
Movie、MMQA、Medical、Cars、E-Commerce 原题均由作者普通 Filter 判定，再回到原关系及评分。
原 `max_tokens=8`、不含 false 即通过、无 retry、作者调度保持，不冒用 LOTUS parser；N 个实际
occurrence 需要 N 次有限预算。五类来源九个原题、全否与 sink 后失败、本机原作者方法体和预算
边界检查通过；结合上述 Map、owner 与计时共 63 项不同软件检查通过，0 跳过、0 模型。
旧 Movie／私有入口的结果保留各自身份；新入口的作者 planner／模型仍待验。
见[Filter 字段绑定](src/baselines/text/frameworks/kalypso_native/README.md#按原题字段复用-filter)。

Kalypso `sembench-join` 把原左右表交给作者 `CartesianProduct → SemFilter`，一个 Join driver
复用现行 predicate/source/sink 生命周期；预算为全部 L×R，保留作者8 token、原判定和调度。
只补原 CP 未传递的配对 `idx` 元数据及原 `:left/:right` 指令字段，不改候选展开／准入。
原 SDK 只恢复关系，不再调用模型；Kalypso 模型输入包含全部声明字段，不借用 LOTUS SDK 的单列投影。
Movie Q5/Q6/Q7 与 E-Commerce Q7 的原结果／评分、2×3重复输入和原 planner／本地真实 tokenizer
软件门检通过，连同受影响旧路径共68项不同检查，0 skip、0真实模型。新作者环境／模型仍 pending；
这不代表任意连续算子或 E-Commerce 多分支查询已在 Kalypso 完成。
见[原生 Join 绑定](src/baselines/text/frameworks/kalypso_native/README.md#原生-join-组合与配对身份)。

Kalypso 客户端另移除与原关系规模无关的固定32MiB响应上限，完整读取后仍做原有数量／身份检查。
33MiB本地回归先复现旧失败再通过，原HTTP/JSON/sink与失败边界通过；结合上述接线共69项不同
软件检查最终通过，0真实模型。外部 owner 预算、超时与资源监督不变，旧源码结果不回填。

Kalypso 的[原生完整排名](src/baselines/text/frameworks/kalypso_native/README.md#原生完整排名-topk)
复用维护 CLI、原查询和字段交接，把完整 source 一次交作者 `SemTopK(k=N)`；保留其 insertion／merge、
5 token A/B 判定、blocking plan 与原调度，不把它命名为 LOTUS quick 算法。预算为完整比较上界，
实际调用另记；空／单行不虚构请求。原字段、重复发生项、7／8行算法切换、完整原结果与评分、
原 planner／本地 tokenizer context 门及失败路径有软件检查。冻结 `11eff9cd7` 已通过 Linux5项软件
及原作者0／1／8行CPU门（8行56个有向比较完整token），模型调用0、清理通过，原件已核。
同一冻结来源又完成原CSV样本8行的16次真实比较（上界28），原排名／分数／评价重放、实际消息及
generate API输入token数组关联、清理／租约均通过；质量按原结果保留，不代表全256行或其他后端。
另以原2000×11 CSV实际选出的256发生项独立通过65280方向CPU门和1508次实际原生比较；
重复review ID保留，完整原rank-to-score／评价、输入token关联和清理通过。
全量资格及原生质量见[验收报告](../experiments/results/diagnostics/sembench_native_integration_20261112/README.md)，不转授其他系统或组合树。
实际output token数组、价格和纯GPU kernel计数仍未观测；API usage不当作GPU实际工作量。
连同受影响 Map／Filter／Join、owner、HTTP／时钟及原题共114项不同本机检查为111通过、
3跳过（独立Daft SDK及未设置的MMQA／Medical可选资产变量），0真实模型。

[IMLane VARCHAR 资格入口](integrations/imlane_duckdb/README.md)明确区分作者 `072f8257`、可选
schema-child 和 varchar-owned 工程变体。连同[完整原关系交接](src/experiments/native_imlane_relation.py)，
原接线的 29 项不同 CPU 检查及 14 份保存 sink 关系回放通过。作者原版 Linux 零模型 15 例实际为
4 过、11 失败：NULL、offset、长字符串借用和 PROCESS 的根 schema 错配已复现；THREAD 字符串
SQL 实际返回 0 行，不能仅归因于字符串内容。新 varchar-owned 候选处理 child schema、有效位、
offset 及字符串复制；`0f562cf0` 已完成真实 Linux DSO／converter／PROCESS／THREAD 15/15 零模型
检查，该工程变体的 THREAD strings／null／offset 各 3 行、empty 0 行、ownership 1 行均正确，
不改记作者原版的失败。新增[MMQA Q1 接线](integrations/imlane_duckdb/README.md#mmqa-q1-工程版接线)
复用该 DSO、原 JSON prompt/parser、完整关系 sink 和既有 owner；25 条保存答复的完整消息/token、
关系／评价、25/26 物理预算及双时钟 CPU 检查通过。协调器已报告 `73ee255e` 的 Q1 目标25次及完整 sink 完成，结果仍绑定原提交。
schema-child 仅保留独立候选身份，尚无该变体的目标资格。原版 TINYINT Filter／DOUBLE Map 的已有
真实结果不受影响；不能用工程变体或 Python fixture 宣称原版 Extract 已支持。新交接要求实际 DuckDB/result
等待完整 sink ACK 后退出；逐题目标状态见上述报告，Q3e 外层 guard 失败保留。联合 150 行的六题批次只有一个批次时钟，
不能拆成六个独立查询时长。


[IMLane Medical Q1](integrations/imlane_duckdb/README.md#medical-q1-原版-filter-接线)已最小接入同一
TINYINT Filter／owner／ACK，原 patient_id Series、parser 和 evaluator 不变。完整历史
cold/warm 各1200保存答复经真实 CPU Arrow／Unix IPC 回放，历史 CPU 完整token数组逐条相等；
原 GPU 仅长度不升级资格。冻结源码 `70a153810cbe901ae3d4ee24c9af5cbe9c966c5c` 已独立完成
Linux 原DSO/SQL门及一次冷1200，实际完整消息/token逐条一致；结果40行，precision=0.5、
recall=0.4、F1=0.4444444444444445，hardcap1200、warmup0、retry0。原件与审计定位见
[Medical Q1说明](integrations/imlane_duckdb/README.md#medical-q1-原版-filter-接线)。
这不授予新组合目标资格；Q9维护CLI的独立Linux零模型门与原件本机核验已通过；
当前组合未作整体目标／GPU执行。

[IMLane Medical Q4](integrations/imlane_duckdb/README.md#medical-q4-原版-filter-接线) 复用原版
TINYINT 单题入口，1200 个发生项进入完整原 join/filter/mean 内存 sink 后才 ACK；回执通过
batch position 与本地 occurrence ID 关联 43 个重复消息组，不改变模型请求。历史 cold/warm
2400 条保存答复的真实 Arrow／Unix IPC、原完整消息、CPU token 数组、关系／评价与物理预算
回放通过，后置失败不隐藏，cold 与 ready 起点独立保留。77 项受影响 CPU 测试通过；协调器另报告 `6a31cc38` 的冷轮1200次及完整 sink
通过，来源与当前组合 CPU 分列，不改记历史质量／时长。

[IMLane Movie Q1–Q4](integrations/imlane_duckdb/README.md#movie-q1q4-原版-filter-接线) 已接入现有
单题 TINYINT owner／provider／完整 sink ACK：Q1 2000 次，Q2–Q4 每题 120 次，首个目标门
只选 Q2。76 项局部 CPU 测试与 2600 条 Movie 保存回复回放通过；Q3 的 120 条来源是旧
`168bc` project resident bridge，不是作者默认。共享 Medical 2400 条保存回复回归通过。
各题保留原 source、prompt/parser、head/count/ratio/evaluator 和独立 cold／ready 实际时钟；
上述 CPU 回放不授予目标资格；`5c69f037` 的 Q1–Q4 后续均已独立完成目标 SQL 与真实冷轮。

Movie Filter 的 `9507` 目标 Q2 尝试在旧 native CLI 的25/1200白名单处失败，generate=0、
无 sink，资源已清理。现修复为必填正 int64 与实际 count／DISTINCT 一致，无枚举或默认25；
当前 driver 源 SHA 在模型启动前校验，必须重链薄 driver，DSO/kernel不变。生产参数函数
实际 C++ 编译执行和旧driver前置拒绝检查通过；新增软件服务的真实子进程／Unix IPC 120条
完整 token／逐发生项答复本机通过，最终80项 CPU 测试通过。`5c69f037` 的 Q2 已完成原版 DSO
新薄 driver 的 Linux 全120行／缺行／重复ID三门，以及真实冷120次、完整原 sink、逐发生项
运行时 token 和资源清理。后续 Q1 全2000条、Q3/Q4 各120条也分别完成原生 SQL、真实冷轮
及完整 token／发生项核对；Q3/Q4 的原评价误差保留，不写成全题质量通过。单一测试入口现按
原配置绑定逐题数量，仅两份已完成测试回执按自身大小读取；生产与全局读限未改。
来源、旧失败、质量结果与单冷轮边界见上述模块说明，不扩展为稳定性能排名。

[Ray Animals Filter](src/baselines/image/ray_animals_q7/README.md)现让 Q1/Q3/Q10 复用同一单遍 worker，
Q7 复用两遍 worker；原题及原评价决定结果，不新写题目执行器。N=1/7/13 的原消息、parser、
完整结果／评价、单遍错误收尾与旧 Q7 生命周期已有 CPU 检查；单遍的目标 SDK／GPU 待验。
Q7 仍保留第一 stage 的完整关闭，最终原 City
关系就绪后才 ACK 并关闭最后 runtime。原 19 项 CPU、真实本地子进程交接及 128 条旧答复身份／原 parser
回放通过；`909f842c4` 又通过目标 64 张原图／128 条保存答复的完整 City／evaluator 回放，并在独立
冷轮完成 128 条真实终态、原 City P/R/F1 = 1 和物理清理。此前因本任务未登记的 Python 观察进程
被 guard 阻断的 64 条失败轮单独保留；改用 shell 观察，未放宽 guard，也不拼接两轮成绩。
后续失败诊断与 EXIT 状态修正仅有 CPU 资格，不把 `909` 的模型结果改记到新 HEAD。
不能从公共 runtime ACK 推断内部模型 actor 驻留或 kernel 时间。

<a id="pr52-g1-qualified-scope"></a>
<a id="record-local-sections-c827"></a>
<a id="record-local-completion-progression"></a>
## 查询路径

- 默认 fused 与可选 `record_local_sections` 使用不同编排；后者可显式选择同一异步后端上的完成推进。
- 原生 baseline 不能注入 Core 调度；完整查询保留原关系处理、评价与工作量。
- 容量、结果关联及逐行方法的接口见[执行适配](src/execution_provider/adapters/README.md)、
  [LOTUS 方法](src/semantic_methods/lotus/README.md)与[原生对照](src/baselines/README.md)。

<a id="records-runtime-modules-cpu"></a>
<a id="records-config-refactor-cpu"></a>
<a id="records-model-context-cpu"></a>
<a id="resource-allocation-candidate"></a>
<a id="pressure-sample-cpu-fix"></a>
<a id="finite-query-token-candidate"></a>
## 容量、上下文与反馈

C／R／work 与存储预算的具体取值和支持组合由[容量类型](src/execution_provider/adapters/records_capacity.py)及
[配置总览](../docs/执行配置与优化开关.md)维护。未消费结果、unknown 和晚到请求继续占据其实际资源责任。
模型 ready 返回的真实上下文用于长度核对，不能以配置省略推断上下文无限。
宿主内存与 cgroup 指标是观测，不等于资源授予。

<a id="prepared-request-batching"></a>
<a id="async-record-token-batches"></a>
<a id="batch-supply-feedback"></a>
<a id="finite-query-successor-candidate"></a>
## 可选组织与提交

| 选项 | 使用条件与说明 |
|---|---|
| 跨读取块 token 组批 | 由实际组织器处理完整请求，不切分单行提示；同步与异步链分别核对配置 |
| token 在途额度 | 仅在已支持路径显式启用；不能按其他算子的检查推断 Extract 支持 |
| 固定 B 的批反馈 | 调整已支持的批并发，保留原批大小和容量声明；不等于动态调整 B |
| 后继优先与等待老化 | 使用既有方法 driver 和 ready 时钟；没有竞争的运行不能证明选择器收益 |

具体参数、默认与组合要求从[执行适配](src/execution_provider/adapters/README.md)和
[配置总览](../docs/执行配置与优化开关.md)核对，不从旧候选记录复制。

<a id="model-query-reuse-scope"></a>
## 模型复用与多查询

模型常驻、仅 runtime 复用和原生单线程 session 是不同运行方式，分别记录生命周期与查询时间。
records resident 的顺序复用不能直接解释为共享 owner 上完整查询并发。
单查询也可能包含多个 flow，Job／flow 注册、容量、结果与取消责任都应保留。
研究接入与普通 Session 的职责见[多任务说明](src/research/multitask/README.md)。

<a id="record-results-migration"></a>
<a id="shared-chain-ipc-scope"></a>
<a id="本地目录与依赖整理尚未发布"></a>
<a id="候选交接状态2026-10-27"></a>
<a id="优化与观测进展2026-10-27"></a>
<a id="current-runtime-cleanup-composition"></a>
## 模块与清理责任

[模块职责](README.md#核心文件职责)区分方法状态、查询 owner、传输、同步／异步引擎、结果关联与观测。
清理沿原持有者执行；未完成 RPC、未消费结果和 unknown 不因调用返回或窗口改变而提前释放。
实际共享链与 IPC 检查从[CPU 指南](tests/records_cpu.md)及[主报告](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)定位。

<a id="current-image-status"></a>
## 图像与未接入能力

当前文件图像与 LOTUS 检索按各自输入表示和方法核对。阶段执行与指标累计分工维护，
不将准备进程、工作量或资源采样的软件检查扩展成多卡或真实模型能力。
跨模型任意依赖图、自动物理 RAM 授予和完整消费阶段内存计量仍未实现。

<a id="legacy-request-retirement"></a>
<a id="http-call-configuration-cpu"></a>
<a id="physical-probe-policy-cpu"></a>
## 不再支持的入口

旧 SemLoom fixed HTTP、PG 实验入口及旧请求反馈配置已退出当前运行路径。
现在的 `resource_feedback` 与 `batch_feedback` 分别按现有解析器使用，不能恢复旧具名 profile 或提示缓存。
需要回查旧实验时使用[已有复现说明](../experiments/reproduction/native_baselines/README.md)，不添加兼容执行路线。

## 证据与仍需验证的事项

| 问题 | 事实入口与适用范围 |
|---|---|
| 当前重构及取消／unknown／资源清理 | [重构主报告](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)记录逐批准确源码和未解决项；不同版本不合并为一次通过 |
| 常驻 Extract 反馈 | [3 项 CPU 检查](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#extract-resident-feedback-20261109)含真实 CPU IPC，Ray 与语义使用替身；模型复验单列 |
| Ray 原生进程退出 | [三类关闭路径](../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ray-close-three-paths)分别核对 Core RPC、自有 UDF 与官方 Processor；驱动断开不证明物理退出 |
| 原生 Movie Q3 | [原生 Q3 报告](../experiments/results/diagnostics/native_sembench_q3_20261106/README.md)保留输出差异、质量与资源结果；不据此给新组合性能排名 |
| 旧关闭故障与负结果 | 原 1596 第二项关闭失败及模型 unknown 等仍按主报告保存；后来检查通过不解释旧故障原因 |

没有运行回执的能力保留为 pending。正式模型比较使用对应计划、实际环境和准确版本，不能从本页推断已运行。

### Movie Q9 原DOUBLE Map维护入口

[唯一CLI](src/experiments/native_imlane_movie_scores.py)已收口原Q9薄driver/UDF、source/audit与完整内存sink接线；
单cold256永久预算，原DSO不变。机器资源控制器与冻结数据仍在仓库外。
当前迁移的资格范围及复现配置见[集成说明](integrations/imlane_duckdb/README.md)。
本机软件IPC/原256消息-token/关系回放与目标模型运行分开记录。`4c08b57b` 维护CLI已独立
通过Linux完整SQL256／零模型及实际加载原52bf DSO检查，1173份原件已回收核验；
其运行token是软件placeholder，不是新GPU token资格。当前组合未作整体目标／GPU执行。
