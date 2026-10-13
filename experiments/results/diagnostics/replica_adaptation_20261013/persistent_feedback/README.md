# 持久反馈与两因素的软件配对

本机localhost替身，模型／GPU／服务器负载0。按[先行计划](../../../../plans/查询调优.md#replica-persistent)，
123批、8156个原请求在68.798／120秒内完成，持久账本8156项、未闭unit0、所有execution owner清理确认。
[原值](raw.json.gz)、[收据](receipt.json)、[逐批日志](run.txt)、[离线核对](audit.json)保留。
复算入口为[只读脚本](../../../../../code/scripts/benchmarks/summarize_replica_persistent.py)，支持提供私有原账本检查durable尝试／闭合；未提供账本明确为unavailable。
实际测量源码为`e3532cfb04ee3413ba823c25fcf4580033e6be68`加[冻结overlay](source-overlay.patch.gz)，不是后续边界修订的源码。
receipt原四个文件摘要逐项与快照匹配；HTTP槽／服务／回复／metrics实现来自该base的`replica_adaptation_probe.py`。
overlay现无损gzip归档；解压SHA与原字节相同，artifact SHA另列audit。原patch的diff空白前缀使Git staged风格检查报警，
曾未阻止第二阶段提交；后续压缩保留精确源身份并通过检查，原字节可从归档恢复，未修改测试源码。

每case独立RR选K2/4/8，固定／online初始都从同一所选批次的transport忙时并集、成功work、时长／完成数和并发点校准。
每臂一个execution，依次开新的NativeTaskSession／Job；先16请求预热，再三次交错测量，完成数和滤波状态跨query保留。
五臂：原RR固定供给、online固定供给、原RR加压力供给、online加同一压力供给、冻结rate固定供给。
raw `signatures.adaptation`是公共tuning；供给两臂按实际overlay覆写`adapt_supply=True`，其余False。
全局硬C/W为独立选K，各副本4/4、服务fixture执行槽2，work=1 request count、soft target=None，支持／缓存／cadence50ms。

完整消费与lease释放的中位秒数如下；每格三个原值在raw和audit，短样本不作显著性、P99或真实模型排名。

| case | 选K | RR固定 | online固定 | RR供给 | online供给 | 冻结rate |
|---|---:|---:|---:|---:|---:|---:|
| change | 8 | 1.420304 | 1.435158 | 1.569864 | 1.560251 | 1.424344 |
| homogeneous | 8 | .219046 | .225241 | .248110 | .242951 | .217693 |
| no_ready | 8 | .740822 | .747811 | .727563 | .741979 | .725462 |
| client | 2 | .657711 | .637427 | .650444 | .651028 | .638808 |
| tail | 8 | .089539 | .083010 | .069920 | .080625 | .062226 |

A初始10ms/work，change中query+.2至+.7秒为60ms，之后恢复10ms，B为20ms；旧在服务请求保持原时长。
256行已跨多个有效窗口：online三个测量中，A的真实新rate更新分别为[3,7,14]／[3,7,14]／[3,7,16]
（初始／下降段／恢复段），不把每个重复`feedback_updated`日志都计一次新样本。
保留服务变化→支持采样→随后原路由→服务→完整响应的时间顺序；旧route日志没有直接绑定实际读取sample，
RR与冻结臂记录反馈但并不采用后续在线rate，不能将这些顺序全部解释为该样本造成某个目标决策。
恢复段被实际覆盖，仍不能从owner吞吐直接宣称物理容量恢复已准确识别。

持久数值状态也不等于始终新鲜：15个online首测中6个、15个online+供给首测中7个超过stale=2s，
首任务实际走未知rate回退；冻结臂持续发布校准视图。交错空闲使同先验的**初始来源**相同，却不保证每个首请求
具有相同信息年龄。逐cell首决策的样本、支持年龄、route reasons及rate/down/recovery/hold/fallback计数在audit；
本轮不能单独证明在线更新优于公平且始终新鲜的冻结对照。K8已是本轮最高被测点，未证明物理饱和或最优静态。

实际接线：合成完整请求→逐query NativeTaskSession/Job→同一个Core硬全局C/W→admission→
Core逐endpoint在途快照→原RR或合格rate least-work→对应HTTP fixture→完整响应检查→Core权威终态→lease释放。
压力RR只在原轮转前过滤软目标已满候选；反馈不释放Core容量，开户／取消／失败及未知责任保持原路径。
已有RR跳满补位依然解释大部分可用资源利用；change／同速供给有退化，不升级默认。

策略耗时只包决策，不能代表全部观察开销。change中位每次决策RR3.24μs、online25.95μs；
相应整个query诊断写入约94.2／87.7ms、完成反馈约29.9／25.9ms，反馈数已包含自己的诊断部分，不能相加。
同步／buffer各三次64请求：诊断写入中位18.634→5.073ms；完整replay中位.325201→.315503秒，
buffer原值中仍有.354783秒反例，不能称稳定收益。最终flush单列，发送前SQLite持久额度和Core lease均未省略，
未从JCT减估计成本。cold两个无先验128请求另存raw，不并入五因素配对。

独立审查发现原驱动开户／准备在清理try之前、只有新POST截止，失败保护可能超出窗口；本次实际成功在原120秒内，
缺口及原源码如实保留。后续修订将创建与准备纳入清理，failed owner禁止复用；新批／POST与批次等待共用T0+105秒，
清理T0+115秒、进程监督T0+120秒，失败收据区分完整cell计数与持久预留尝试，并记录实际配置和全部源码。
[故障和旧语义检查](guard-tests.txt)验证准备／开户零HTTP失败、绝对批次截止后读完唯一原调用和保留预算。
这些后续源码修复不追溯改标上述8156测量。精确记录route实际typed sample后的补查见下方。

软件两因素没有整体净收益；no-ready／自然尾的小差异和client的供给上限不能外推为GPU欠供给识别。
默认RR、原820模型负结果、SQL／LOTUS evaluator／PG／共享准备与跨owner硬总容量的资格边界保持。

## 精确反馈关联的独立小对子

固定已推送`ad4b2b556d8e226007d5363a0432a5bc11a538c1`，receipt五个源码SHA逐项匹配该提交，
不复用前轮实际源码overlay。11批／1808原localhost POST，在10.897／30秒内完成，
durable预留1808、未闭unit0，全部owner清理；服务器／模型／GPU0。
[原值](pair-raw.json.gz)、[源码／资源／计数收据](pair-receipt.json)、[日志](pair-run.txt)、[只读复算](pair-audit.json)保留。
同一独立RR校准K8、同先验、两持久owner各16预热／三次256请求，次序[A,B]／[B,A]／[B,A]，
沿用10→60→10ms A、20ms B和50ms支持门槛；没有更改硬C/W、重复原请求或制造额外服务负载。

RR三次1.325202／1.300482／1.314527秒，中位1.314527；online三次1.331303／1.319155／1.326550，
中位1.326550（约慢0.91%），未取得净收益。这是新源码的独立对子，不并入旧五因素排名或宣称保护修复加速。
三个online首路由均有新鲜合格rate，过期数0/3。A每次独特rate更新为[3,7,12]（初始／下降／恢复段），
与旧矩阵一样按发布计数去重；恢复有多个有效窗口，不将每次结束的重复观察当新样本。

下降和恢复各三个回执共六条：服务变化后的完成→新supported反馈→随后原route实际读取的typed反馈→
对应原请求进入目标服务→完整响应。实际读取的A样本时刻不早于已发布的新反馈，reason为qualified least-work，
所有原出现及byte／body／规范化／容量／lease检查保持。此证据闭合本机反馈与决策的时间关联，
没有证明该次目标必然优于RR、物理容量准确识别或整体模型收益。低压力不单独解释欠供给，GPU活动仍未用于本机fixture。
