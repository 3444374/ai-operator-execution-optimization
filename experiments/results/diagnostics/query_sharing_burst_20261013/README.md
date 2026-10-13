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
新版实库兼容仍pending；旧9d的23.25%不是当前版本收益，新增窗口只补原未完成反例并继承总4240余额。
公共两文件补丁`e4677b4c`直接以共同`28093954`为父，已在研究分支无树变merge保留，可独立交给其他owner。
仅剩余检查入口拒绝扩到360新POST/600秒以上或把旧1604计数清零；本地10项检查通过。
同组此前一次受文件系统sandbox的loopback bind限制（9项8过/1环境错误），授权环境复验通过；未改变HTTP服务。
运行时fixed K=4只表示新版本兼容配置，未重新校准或运行旧主比较；旧失败原件及诊断保留。
