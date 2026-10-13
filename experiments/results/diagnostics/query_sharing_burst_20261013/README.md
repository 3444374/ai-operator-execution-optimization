# 已封口小Job共享机会：无模型机制与反例

初始实验只解释真实SessionEngine加两个串行替身服务，模型、GPU、HTTP和PG查询均为0；新增桥接阶段见下节。
研究问题和真实前端预演只维护在[同题M4方案](../../../plans/数据组织.md#query-sharing-burst)。
LOTUS/Sema/DuckDB原生多查询及真实模型性能均pending；FCFS诊断不代表这些系统。

## 来源与证据

- 固定base：`9bb6c4154a3fa321c49a84f45eaeb6c7e12b129c`，隔离分支`exp/semloom-query-sharing`，未merge native/main；初始实验及规则阶段已推至`126b591f`。
- [原始证据](raw/probe.json.gz)：126次七场景/六臂/三次确定性重复、两次静态校准、42次满资源/保留资源solo，
  共170次运行/8308次合成请求；工作定义和四份运行源码SHA在原件中，另保留288个探索cell。
- [summary.json](summary.json)由[独立分析器](../../../../code/src/experiments/query_sharing_analysis.py)复算，含全部重复、每查询三种JCT反事实、
  最差短查询、长查询与吞吐比；源码入口为[probe](../../../../code/src/experiments/query_sharing_probe.py)。
- [checks.json](raw/checks.json)、[checks.log](raw/checks.log)记录无模型回归、早期失败和不可用依赖。

新版观测、封口及输入/结果消费在所有内部臂相同；候选与强静态adapter/K/router相同。
旧单查询与旧适配器测量未用于改善计算。初始核验时native对象尚未取得；规则同步fetch后已取得`62445a7e`/`3bd53587`，
但native与stability仍互不包含，本原型未合入native代码。共同集成和服务器匹配实验需由主协调。

## 固定条件

每行一个完整独立任务；长短分别注册Job并按各自release到达，有界offer/lease释放。
全局在途请求最多4；每Job16个持有任务、1024输入字节、128结果字节。
每endpoint一个不可抢占串行worker，相同LeastQueued router；真实服务时间只属于后端/离线审计，不传给在线选择器。
长Job独立校准K=2/4都是32模拟秒，强静态选最小饱和admission=2；候选主臂同为2，全局上限仍为4。
最少在途估计的工作守恒、共享RR和FCFS用4；候选4另列已承诺工作消融。该校准不为真实模型选择K。
候选只优先已封口且剩余估计≤4的Job；被连续跳过的有资格Job累计工作量达到4后先获得选择机会。
未知/混合工作单位回到RR；取消通知不代表停止证明，不提前释放远端名额。

JCT终点为同步合成consumer释放最后一个正确结果；真实前端要计客户端EOF、读源和专属准备。
最长无提交时段仅在仍有未提交必要工作时计算；最长无完成服务另报，不能混称墙钟饥饿保证。

## 观察与反例

下文10%/5%/2%是已执行无模型工程探测的原分类值，未建立应用SLO或真实重复噪声依据；
原JSON分类字段保留，仅解释该协议，不作为后续实验的资格或方法取舍门槛。负结果与无改善同样保留。

下表三次结果一致，单位是模拟秒。确定性一致不代表真实性能统计显著。

| 场景 | 工作守恒短JCT | 强静态短JCT | 候选短JCT | 强静态→候选长JCT | 强静态→候选整组JCT |
|---|---:|---:|---:|---:|---:|
| 64行长Job，4行短Job在0.5s到达 | 4.5 | 4.5 | 2.5 | 34→34 | 34→34 |
| 没有竞争 | 2 | 2 | 2 | 32→32 | 42→42 |
| 两Job都16行 | 15.5 | 15.5 | 15.5 | 15→13 | 16→16 |
| 两个20s不可抢占任务占用 | 20.5 | 20.5 | 20.5 | 20→20 | 21→21 |
| 短Job三行均实际5s、估计1 | 12.5 | 11.5 | 10.5 | 10→11 | 12→11 |
| 短Job实际1、估计20 | 8.5 | 4.5 | 4.5 | 34→34 | 34→34 |
| 8个短Job连续到达，最差JCT | 16.5 | 25.5 | 9.5 | 80→80 | 80→80 |

主场景短Job较两个合理控制均少44.444%，长与吞吐不变，是本模型里的采用信号。
连续短到达也满足本次三项要求；其他五个场景未同时满足短Job改善10%、长Job退化≤5%、吞吐降低≤2%。
估计偏小时长Job较强静态慢10%、较工作守恒慢22.222%，即使整组更快也拒绝。
288-cell探索找到四个长Job退化超5%的cell，全部cell保留，候选阈值未根据枚举调优。

候选K=4主场景短JCT为3.5，慢于同策略K=2的2.5；既有承诺工作削弱优先级效果。
估计偏差场景该消融整组13秒、强静态12秒，吞吐少7.692%；多短Job场景该消融也伤害部分短Job。
这些反例支持静态回退，不能预设SemLoom普遍赢。

独立审计从提交到服务终态重建名额，检查到达、恰好一次、endpoint无重叠、实际时长、请求/work峰值、
每Job存储和最后归零，不只相信runner汇总。模型输出质量、SQL权限/快照、真实CPU控制成本和客户端尾延迟均pending。

## 检查与后续

规则原提交`d8d3e095`、`abda3404`已分别吸收为`c14d2f20`、`22596647`；
仅同步11个规则/历史诊断相关文件，日志冲突保留两方各自记录，原四份研究实现SHA不变。
新增受影响验证为历史E2E/代价15项及本原型15项，共30项通过；原463项记录仍归其原运行。

本机432项scheduling、6项probe/审计、19项native task、6项真实本地UDS gateway，共463项通过。
gateway初次sandbox未创建socket；只读诊断确认`listener.bind`被拒，原6项随后在获准本地socket范围通过。
早期新测试用错`seal`和`CleanupReport.events`名称，修正后通过，失败身份保留在checks。
`test_scheduling_daft_ray_contract`因本机缺少`pyarrow`未导入，不计通过；未混装依赖。
当前本机解释器无LOTUS/LiteLLM/OpenAI/DuckDB，未冒充真实前端资格。
用户授权后只读复用已核验SSH master检查一次GPU状态：两张4090未列出模型进程，未启动模型/GPU任务；
这不是新校准或窗口分配。后续先共同源码与前端桥接、策略关闭的新强静态，再由主协调有限模型预演。

从仓库根生成新输出，不覆盖原件：

```sh
python code/scripts/benchmarks/query_sharing_probe.py --output /path/to/private/fresh-probe.json.gz
PYTHONPATH=code python -m src.experiments.query_sharing_analysis \
  --input /path/to/private/fresh-probe.json.gz --output /path/to/private/fresh-summary.json
```

不把host elapsed当模拟服务时间；完整资源、决策及重复事件均在gzip，summary只是一份可再生聚合。

<a id="native-query-bridge"></a>
## 双前端到单Core owner：本地桥接阶段

[bridge_checks.json](raw/bridge_checks.json)保存这次独立运行与源码摘要；63项中50通过，13项实际LOTUS库检查因本机缺库跳过。
新增11项桥测试用真实Core与替身I/O，其中两个确实spawn独立Python进程；其余以进程内客户演练相同RPC协议。
同adapter RR/候选保持独立Job/session，覆盖query身份、外Job lease、部分输入封口、慢消费者、取消后在途额度、
进程异常停止与EOF负例、畸形帧、8MiB上限。LOTUS可选factory另有旧consumer路径行为检查。

EOF但进程仍活着、还持有delivery时，`close`确认失败，结果保持计费；另一Job仍完成。
进程sentinel确认停止后遗留lease归还；取消已接收的两个计算保留额度直到替身服务返回终态。
这些是生命周期/接入正确性，不能计为调度收益。RPC复制和新的owner轮询都须计入后续静态/候选两臂。
未运行真实LOTUS双进程或任何HTTP、SQL、GPU、模型；源码校验、264行请求对拍及客户端JCT仍pending。
现有服务器解释器只做包元数据核实，不安装依赖；已分配CPU16–17的零模型实库窗口等待baseline计时释放。

<a id="lotus-pair-preparation"></a>
## LOTUS配对入口准备

[实库入口](../../../../code/src/experiments/query_sharing_lotus.py)和同题M4已收敛到两独立Map进程，
native请求池保持每LM2，内部三个控制共用新桥、同全局K、Job封口和请求数观测。
K由独立long64两次K=2/4校准选定；主长256/短8四臂三轮交错及无竞争/取消负例的fixture上限4240POST。
源/结果/关联与service槽由独立审计复算，错误原件写新输出，不能沿用旧单查询成绩。

入口准备阶段新增18项中17通过/1实库双进程用例因缺LOTUS跳过；本机4个HTTP POST只属于两槽服务单元用例。
源码摘要与原运行见[lotus_preparation_checks.json](raw/lotus_preparation_checks.json)。
尚未运行真实LOTUS双前端；这份检查不是实库成绩、SemBench评价、SQL或真实模型验证。

补充审计[lotus_audit_checks.json](raw/lotus_audit_checks.json)核对完整响应摘要、n/生成字段及LM实际usage/cache。
8项中7通过/1实库跳过；较严参数校验先拒绝旧测试的max_tokens变动，导致一次错误消息预期失败，
已改为完整消息差异用例并单测参数拒绝；失败与通过原件分别保留。这两次本地运行共8个fixture POST。
前端退出、RSS与累计4240上限继续由本runner自己控制；实际库资格仍pending。

<a id="lotus-sdk-repair"></a>
## 已释放窗口的实库预检与首次失败

主协调确认baseline清理释放后，轻量核查CPU16–17无新受限进程、GPU无计算进程，再上传已提交`a64ce942`快照。
现有解释器Python3.12.3，实际版本LOTUS1.2.4/LiteLLM1.95.0/OpenAI2.50.0/pyarrow24.0.0/pandas2.3.3/HTTPX0.28.1；
固定SDK源码摘要、Arrow表和Pandas frame检查通过。早期目录盘点报pandas3.0.5，不能代替此次解释器实测；未安装或修改公共依赖。

首次实库19项18通过/1审计错误；三组32+4 Map结果已返回，但没有导出HTTP事件，所以不授予完整配对资格。
固定LiteLLM将请求的`max_tokens=16`转换成`max_completion_tokens=16`，旧审计误要求字面字段；
零HTTP的实际`prepare_call`诊断确认这点，审计改为核对有效输出上限并继续对拍完整正文，不改供应商请求。
Linux另暴露reader EOF与关闭流程同时关闭Pipe fd的竞态；新增单独关闭互斥和真实fd并发用例，属于普通正确性修补。
本地21项20通过/1实库跳过；修后完整实库验证仍pending，失败/预检与源码摘要归[repair checks](raw/lotus_sdk_repair_checks.json)。
首次实库仅保存日志，没有HTTP事件原件；后续小配对将在审计前导出事件，即使失败也保留，不把这一缺口抹掉。

`1dab41bc`复测的12项Linux桥检查通过，fd竞态未再出现；三组32+4真实LOTUS进程返回且完整事件已导出，
共享observer的`http_started/http_finished`缺少Core `usage`导致审计KeyError，suite仍保留退出1。
修订只区分HTTP和Core容量观测；[108请求原件](raw/lotus_smoke_1dab41bc.json.gz)由新审计零HTTP复算，
[reanalysis](raw/lotus_smoke_reanalysis_1dab41bc.json)确认完整消息/生成/输出/LM usage、两query/Job/session/进程、两服务槽及最终归零。
三臂真实重叠、RSS峰值约2.14GB；这是实库桥接正确性资格，小样本JCT不作为模型收益。主长256/短8协议尚待执行。

<a id="lotus-main-fixture"></a>
## 长256／短8：已完成四臂，预算使完整协议未完成

运行源码`9d3e4228676086a50355d48c06d419c617bfa44f`；[完整原件](raw/lotus_protocol_9d3e4228.json.gz)、
[独立复算](raw/lotus_protocol_summary_9d3e4228.json)、[控制器](raw/lotus_protocol_controller_9d3e4228.json)和
[释放证明](raw/lotus_release_proof_9d3e4228.json)分别保留。代码父仍为stability来源；本结果不重新绑定后续共同基点。
本窗口始于05:20:39 UTC，截止05:50:39；修补后只用剩余152秒运行一次四臂，单次重复写入manifest，未暗称三轮完成。
05:50:10 driver结束；第二个无竞争cell在frontend READY之前超时，0调用、最终Core/Job/lease归零，整协议退出1。
顶层原件错误类型因cleanup替换异常显示NoneType；失败cell准确保存TimeoutError，复算明确这一诊断缺口，原件不改。

已完成四个独立long64校准、四个long256/short8比较和一个native无竞争，共9个完整cell／1384个fixture POST。
在校准中K=2／4的long JCT中位数1.2507／0.9356秒，因此内部三臂均选K=4，候选没有用短query结果选点。
四比较臂各264次完整请求；正文、生成上限、输出、LM usage/cache、行身份、独立进程/Job/session及服务槽由复算确认。
它们均实际重叠，两服务槽峰值2，RSS峰值≤2.25GB，模型/GPU/SQL均0。

| 单次主场景 | 短query JCT秒 | 长query／整组JCT秒 | parent CPU秒 |
|---|---:|---:|---:|
| LOTUS native shared，保留每LM2请求池 | 0.5780 | 5.2324 | 0.6018 |
| 新桥最少在途的工作守恒，K=4 | 0.2665 | 3.6045 | 1.7523 |
| 新桥修后静态RR，K=4 | 0.2547 | 3.5617 | 1.7025 |
| 新桥已封口小Job选择，K=4 | 0.1955 | 3.6136 | 1.7544 |

候选较同桥static短JCT少23.25%，长JCT多1.46%、整组吞吐少1.44%；较工作守恒短JCT少26.65%，
长多0.25%、吞吐少0.25%。只有一个样本，没有噪声/SLO依据，不据此宣布采用、稳定非劣或双GPU模型收益。
native差距同时含原LM/SDK请求池与新执行路径的成本，不能全部归到机会选择；parent CPU含Core、fixture和编排，不能称纯调度CPU。
实际语义入口是两个独立进程的`frame.sem_map`，不是SQL；文本是公开合成fixture标识，未执行SemBench真实评价集。

完整无竞争static/candidate、真实LOTUS取消、连续短到达和重复均未完成；已有Linux12项桥及旧Core负例继续引用，不重复大范围复测。
窗口累计按首次失败112预留＋已审计smoke108＋本协议1384保守计1604／4240，0模型调用；失败和已完成cell均保留。
这是固定必要调用集合的执行/生命周期研究，不改变合法求值范围或LIMIT语义，原Q2 LIMIT5成绩不作为这里的全体质量依据。
后续共同集成/模型预算及剩余资格只维护在[M4](../../../plans/数据组织.md#query-sharing-burst)，阶段源码与旧证据分别保留。

<a id="common-typed-limits"></a>
## 共同基点与远程typed limits兼容

`28093954`含原stability祖先；其Core session.py/session_jobs.py相对原9bb无公共变更，已普通merge为`768f59e5`，
父为本线`5ae23d0e`与共同`28093954`，双方源/文档/旧原件保留。共同CSV原CRLF保持，按cr-at-eol检查，未改原结果。
本地共同接续45项42通过/3SDK跳过；新增typed-limit最小补丁后48项45通过/3SDK跳过。
`describe_limits`是owner的只读配置操作，未创建Job/接受输入；无Engine的frontend要求factory与typed SessionLimits，
拒绝缺失/伪类型及local override，flow打开后复核item容量。共同SDK replay及默认local路径保留。
此处本地阶段后实库兼容曾pending；下节记录完成的剩余资格。旧9d的23.25%不是当前版本收益，新增窗口继承总4240余额。
公共两文件补丁`e4677b4c`直接以共同`28093954`为父，已在研究分支无树变merge保留，可独立交给其他owner。
仅剩余检查入口拒绝扩到360新POST/600秒以上或把旧1604计数清零；本地10项检查通过。
同组此前一次受文件系统sandbox的loopback bind限制（9项8过/1环境错误），授权环境复验通过；未改变HTTP服务。
运行时fixed K=4只表示新版本兼容配置，未重新校准或运行旧主比较；旧失败原件及诊断保留。

<a id="lotus-remaining-qualification"></a>
## 新共同版本的剩余反例与释放

运行源码`01c28df1f0b15fee9d9b48808809a8fd88baa79a`，含共同`28093954`及独立公共接口`e4677b4c`。
既有解释器和固定SDK未改；两独立进程分别执行Map，Core仍由唯一owner管理。
运行前已向整合和副本owner登记06:48:30–06:58:30 UTC；实际启动06:49:54.911754，
仍以登记的06:58:30为硬截止，内部预算505秒且预留20秒清理，不重新开启十分钟。
实际06:51:00.956162退出0，driver约65.55秒，无watchdog触发；06:52:07.651939独立确认释放。

[完整原件](raw/lotus_remaining_01c28df1.json.gz)保留全部请求、响应、Core事件、前端版本与结果；
[离线复算](raw/lotus_remaining_audit_01c28df1.json)由[审计入口](../../../../code/scripts/analysis/audit_query_sharing_lotus.py)重建行关联和计算责任，
[运行摘要](raw/lotus_remaining_checks_01c28df1.json)、[控制器](raw/lotus_remaining_controller_01c28df1.json)及
[释放证明](raw/lotus_remaining_release_01c28df1.json)记录实际时钟、来源、预算与清理。
只补原未完成项：work-conserving、strong-static、candidate的long64/short8无竞争各72POST，
static及candidate取消分别45、44POST，共305新增；旧保守1604继续计入1909/4240，余2331。
GPU、真实模型与SQL均0，服务并发峰值2，聚合driver RSS峰值2,223,280,128字节，小于4GiB。

三个无竞争cell都实际无重叠，全部64+8行完整对应；两取消cell短query均完整8行、64 tokens、无cache命中。
取消长query返回定义的RuntimeError；static取消瞬间长query仍有2个在途，candidate长query在途为0，
两者全局C/W均4。逐事件重放确认每个已提交计算名额保留到其终态，最终submitted与terminal集合相同、全部归零；
未观察到取消后新的long提交。候选本cell没有覆盖“取消长query在途工作”，不能借global4冒称覆盖。
完整响应、生成上限、necessary-call/usage、独立PID/Job/session及结果lease由同源请求审计与额外责任复放分别核验。
四个篡改反例（提前退款、重复终态、取消影响另一query行/HTTP）均被离线审计拒绝，新增网络和模型调用0。

最终5个Core闭合，Job/session/held/input/result/compute/lease均0；driver及10个前端PID不存在，
CPU16–17无受限相关进程，fixture随driver退出，独立GPU检查为空。释放receipt已交整合与副本owner。
当前资格支持共同SDK response replay和typed remote factory的实际库兼容、无竞争与取消隔离，
不支持新版本调度性能比较、真实双GPU模型效果、SQL并发或任意持续多Map服务。
旧9d的单次23.25%只归旧源码；完整三轮比较、连续短到达、新版本同参校准及真实模型仍未完成。

复算不启动服务：

```bash
python3 code/scripts/analysis/audit_query_sharing_lotus.py \
  --input experiments/results/diagnostics/query_sharing_burst_20261013/raw/lotus_remaining_01c28df1.json.gz \
  --output /tmp/lotus-remaining-new-audit.json
```

<a id="lotus-work-descriptor"></a>
## 单Map可选工作描述的本地接线

源码依据：`planning.work.WorkDescriptor`已有单位/来源/不确定性字段，`prepare_native_task`已有`work`参数，
DuckDB executor已接`describe_work`；LOTUS的`on_prepared`只观察，原来每次固定走默认请求数。
新增可选`describe_work(index, prepared_call)`，不设置仍每请求1；同一PreparedLotusCall与原完整payload传给观察、任务和SDK解析。
typed描述直接进入TaskInfo，Core仍是work单位和C/W容量的权威；未新建估计平台或改Job封口、路由、HTTP。

[本地检查](raw/lotus_work_descriptor_local_20261013.json)记录53项50通过/3实际SDK缺库跳过。
覆盖默认1、异质1/3进入Core、lower/upper/来源保留、相同规范调用、回压suffix只估计一次、远程无Engine factory、
回调异常和非typed返回、单位/单项容量拒绝、回调后取消/期限检查及现有响应/清理和native task路径。
这些是mock SDK与真实Core的接口资格，实际LOTUS新回调、真实模型、批次总量提示及持续短query仍未验证。
服务器负载与新增HTTP/模型/GPU均0，旧1909/4240额度不变，replica真实模型窗口优先。
接口与后续有限协议集中在[M4](../../../plans/数据组织.md#query-sharing-costs)，本阶段不把单Map桥扩成任意Filter→Map。
成本接线自有提交`e3d50689`在桥修复`0517ba9f`之后；公共两文件独立补丁`556977cb`直接父为`e4677b4c`，
研究分支无树变merge保留，两公共API阶段与桥修复分别可审阅。

<a id="bridge-review-repairs"></a>
## 当前桥接源码的三项审查修复

审查线索基于01c；复核本线`507572ee`仍存在三项问题，未将附件方法当成当前事实。
成本回调的未发布改动先保留，本阶段只提交桥、runner和对应回归，不启动服务器。
[本地证据](raw/lotus_bridge_review_local_20261013.json)保存旧源码失败、修后检查及日志校验值。

1. 实际RemoteNativeTaskSession通过继承Pipe送入真实Core：16个不同600000字节任务截到13，
   无效metadata的0/REJECTED被旧前端改成BACKPRESSURE。现仅Core全接纳所传prefix时标记未传suffix；
   拒绝、部分接纳和原backpressure原因保留。合法13+3的重试在Core/backend恰好提交/完成0–15各一次。
2. 直接加载507源码并用真实Core、无新RPC的20项工作验证：8步预算后仍ready，旧owner在8、16项处再次阻塞RPC。
   新owner最多连续4个有界Core tick，每步非阻塞检查RPC；预算内不多等，40项反例在32项处按上限让出I/O。
   没有永久空队列忙等，也没有更改Core预算、admission/router或C/W。此项是普通等待开销修补。
3. 在实际run_pair方法注入setup/join/control/bridge-close/snapshot组合失败，旧finally遮首错并漏收记录。
   现每步独立收集phase/type，保留第一异常为PairFailure原因；请求、HTTP错误、退出码和parent CPU继续采集。
   清理单独失败同样标记failed，snapshot未知保持None，不宣称释放；启动中的两端Pipe也独立关闭。

修后32项全部通过（18桥、4清理、10审计/预算），只用4个本机loopback模拟请求；服务器、模型/GPU/SQL负载0。
首轮状态回归的额外清理失败及一次测试重复close错误都保留；修正测试的自有flow teardown后18项桥回归通过。
机会bypass是补偿触发阈值，单次估计工作可跨过它，不是8work的绝对等待、token公平或墙钟保障。
历史9d/01c原件和1909/4240累计不变；这些正确性修补不支持新版本模型性能，下一RR/候选必须同桥同版。


<a id="matched-remaining-work"></a>
## 同信息剩余工作控制与有限反证

接续基点为`5e0ff640d57adfc892b629ecf3bd5f607aa60801`，工作树接管时干净。
独立接口补丁`556977cbeef5f96437e4813939156bc000333dbf`的直接父为`e4677b4c`；
远端研究分支在接管时为同一5e0ff640，补丁已随无树变merge发布，未重新实现或提交。
[独立补丁检查](raw/lotus_work_descriptor_independent_20261013.json)在临时提取的556源码上得到50通过/3实库缺依赖跳过，
两文件SHA与此前接口证据一致，不替代实际LOTUS新回调或模型资格。

本次新增`RemainingWorkPriority`及可选相同补偿，与候选复用一套机会记账。
无可比较的已知总量时清空补偿并RR；有已知总量时最小估计优先、tie按原Job/flow RR，未知Job可由补偿获得机会。
这也明确了旧候选在全部总量未知时的回退行为；默认RR和关闭工作观测保持。
补偿只计有资格时选中Job的最大排队估计，失败选择同样计入；单次选择可跨阈值，不是token份额、墙钟等待上限或服务抢占。

[完整运行](raw/matched_work_probe_20261013.json.gz)保存5条件×4臂×3确定性重复，共60个运行、3588次模拟提交；
[增强独立复算](raw/matched_work_audit_20261013.json)核对完整臂/重复身份、相同输入/阈值/容量、逐项关联、两串行槽、C/W与最终归零。
运行时源码由[初始三文件补丁](raw/matched_work_source_initial_20261013.patch.gz)相对5e0ff640重建，文件SHA在原件；
后续增强审计有自己的`audit_source_sha256`，没有改写运行原件。
全部内部臂C=K=2、W=2×本条件最大声明单项work、每Job16项/1024输入字节/128结果字节，同Core/router/输入与观察。
阈值在各条件按一个短Job的声明总work预先固定；Core仍只知道已接受工作，长Job全部输入接纳后才封口，未接批次总量提示。
代理估计来自明确fixture字段；真实服务时长只交后端，未向选择器暴露。HTTP/模型/GPU/PG均0。

下表列出短Job中最差JCT（模拟秒）。三个确定性重复的完整轨迹相同，全部逐Job与长Job数字保留在原件/复算。

| 条件 | RR | 剩余work优先 | 剩余work＋补偿 | 候选 | RR长/组JCT | 其余三臂长/组JCT |
|---|---:|---:|---:|---:|---:|---:|
| 等成本long64/short8 | 8.5 | 4.5 | 4.5 | 4.5 | 36/36 | 36/36 |
| 声明4/1，服务2/1 | 12.5 | 5.5 | 5.5 | 5.5 | 69/69 | 68/68 |
| 声明4/1，服务1/4 | 20.5 | 16.5 | 16.5 | 16.5 | 48/48 | 48/48 |
| long64＋8个独立short2 | 6.5 | 1.5 | 4.5 | 4.5 | 40/40 | 40/40 |
| 两个20秒长请求先占槽 | 20.5 | 20.5 | 20.5 | 20.5 | 20/21 | 20/21 |

候选与带相同补偿的剩余work控制在全部15个条件/重复配对中，选择、服务、容量、查询终态与owner tick轨迹完全相同；
本切片没有独立调度增量。有限短到达中，无补偿控制最差短Job1.5、补偿/候选4.5，补偿对短Job有代价。
不可抢占占用时四臂均无短Job收益。该结果只覆盖本表；全部已知大Job超过候选阈值时两规则仍可不同，已有行为回归覆盖。
这些负结果与旧估计错误导致长Job受损的原件并列保留，不以RR改善推导方法新颖性或真实服务效果。

计时从每个`Query.release_s`预定义到达开始，准入前等待不扣除；`finish_s`在必要结果关联校验、消费释放及Job关闭后记录。
虚拟时钟只推进后端与到达事件，没有计入真实SDK/解析/消费CPU耗时；这是模拟JCT，不是完整模型查询墙钟。
独占slowdown未测（本次没有新增solo臂），查询P99未评价，确定性重复不代表随机不确定性；不新增长Job5%门槛。
真实查询仍须以预定义释放到必要解析/消费完成为终点，独占参照匹配同C/W、服务实例和资源边界。

[75项本地回归首轮](raw/matched_work_local_checks_20261013.json)为73通过、1实际SDK缺库跳过、1沙箱禁止localhost监听；
受阻单项经获准的[4次loopback请求复核](raw/matched_work_bind_recheck_20261013.json)通过，首轮失败保留，共74项独立通过/1跳过。
现有桥的截帧拒绝、四tick推进和首错清理回归保持；没有扩大服务器或模型矩阵。
实际SDK单Map新回调资格现见[接续软件检查](#work-sdk-qualification)；生产长度代理、私有共享桥同信息四臂、
可信单Map总量及持续短frontend仍pending。
Linux窗口按多模态独立释放后串行协调；新资格使用独立run_id/额度，不借旧1909/4240。
共同状态、台账和README由baseline owner统一同步，本报告与同题M4维护本研究事实。

复算（只读原件、0网络）：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code python3 -m src.experiments.query_sharing_analysis \
  --input experiments/results/diagnostics/query_sharing_burst_20261013/raw/matched_work_probe_20261013.json.gz \
  --output /tmp/query-sharing-matched-work-new-audit.json
```

未来重跑使用新的输出路径；`query_sharing_probe.py --protocol matched-work`选择本协议，默认`legacy`保持历史入口。


<a id="work-sdk-qualification"></a>
## 实际SDK的单Map工作描述资格

固定`21003676945e57368317421c3d2af484649415d8`，从Git获取全新源码，未复制私有输入。
[完整脱敏证据](raw/work_sdk_21003676_20261013.json.gz)包含源码SHA、机器preflight、SDK版本/源哈希、全部规范请求/typed描述/结果、
测试日志、RSS序列、控制器和独立释放检查；私有原件的SHA单独记录。
`core,text,semantic-benchmarks`自动profile预检通过；未安装依赖、下载模型或覆盖profile。
使用已有共同host-exclusive lease，没有按replica私有ARTIFACT_ROOT建立平行锁。

实际LOTUS SDK与本地真实Core的单Map新检查、既有3项SDK replay检查，共4项通过/0跳过，unittest计时9.359秒。
同一4行Map的默认与显式工作描述分别使用独立Core，C=2/W=128/held=2，单进程串行LOTUS settings；
8次内存后端回复，不调用真实模型或HTTP传输。两个模式的完整规范payload与结果逐项相同，所有消费、Job和Core责任归零。
默认每项1；显式描述按准备时完整text消息UTF-8字节B及有效输出cap16计算`ceil(B/128)+16`：
B分别201/1476/796/200，point18/28/23/18、lower2/12/7/2、upper与point相同。
来源签名绑定完整非message生成配置；metadata及上下界完整到达Core，回调各项仅一次。
这些区间只描述该未校准代理公式中的输出长度范围，不是GPU耗时、token换算率或统计置信区间。
资格进程从外层封锁全部socket连接；3项replay中的HTTP/超时/坏JSON错误均是预定义内存响应对拍，日志原样保留。

窗口T0为09:05:58.455558 UTC，硬截止09:15:58.455558，实际09:08:23.926902结束，共145.471344秒；
源码获取及预检完整计入窗口，Git准备流量与资格网络分别记录，资格网络POST/模型/GPU均0，旧1909/4240不动。
RSS采样峰值1,066,516,480字节（约0.993GiB），低于4GiB采样停止限；不把离散采样写成连续内存上界。
预检先观察机器可用CPU；资格进程启动时设置CPU16–17，未采集thread级亲和性/逐核CPU违规审计，不据此作性能资源配平结论。
09:10:22.030018 UTC使用既有driver独立核对8个PID+birth身份，无存活；共同lease不存在、GPU计算列表为空。
源文件的5个SHA与本机冻结文件一致；之后不再占用服务器。

该资格覆盖实际SDK、完整单Map和本地Core成本接线，尚未证明私有双前端桥的代理估值共享、完整多阶段SQL并发或GPU性能。
真实模型四臂仍须同一修后桥、同信息及同C/W/路由/服务；本次没有执行1432请求模型方案，也没有因RR改善忽略补偿等价的负结果。
