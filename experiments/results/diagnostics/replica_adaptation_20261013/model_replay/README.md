# 固定原Q2请求子集模型诊断

执行源码 `82bd400ba3225a5b7ed737c40bb4758764dfda35`，源请求执行 `3bd5358706679b639e790585347133cd8d60b836`。
这是固定24完整请求的回放batch，不执行原SQL、LOTUS formatter/parser或官方evaluator；原Q2每次120调用。
[计划](../../../../plans/查询调优.md#replica-adaptation)与[机器预算](../../../../plans/replica_adaptation_model.json)保持单/双、四臂、三交错重复。
820请求包括576评价、144校准、96预热、4协议；无失败模型调用、隐藏重试或额外探测POST。

原T0为07:45:29 UTC，07:59:02.827结束，813.827/1080秒；模型加载82.349/300秒。
两个准备失败均0POST、0预留：额外preflight T0+120秒错误、继承driver2核亲和性；修订后严格沿原T0／原账本／原截止继续未执行阶段，失败不改为成功。
本地payload复制曾被自动审批拒绝，后直接在服务器复用同SHA原件，未导出该本地载荷；subset SHA为`fdd6d7c2415b29168506a5e78d1342d83060ab503d47172df43d617913eb95a8`。
模型调用开始前最后冻结上述SHA，四臂没有在运行中变更。原件metadata归档SHA为`ba7d3aa11e313ae98d7160c1963d4d063edc2907ef0d7a309a8468a11a8935cf`，642082字节；载荷／响应正文留在私有原件。

## 结果与强静态参照

四臂均同Core、完整响应HTTP、每副本硬C/W4/4及全局已选K；neutral work每调用1。
RR跳过满副本；固定rate和在线rate共用同一existing completion accumulator的transport起止clock和成功work计量。
固定冻结独立校准整批rate；在线保留窗口／EWMA更新。所有臂soft work为None，背景metrics250ms、GPU1s及Core/策略记录口径一致，观察成本计入JCT。
主指标是已准备payload首次offer到全部24完整响应校验、消费和lease释放；source64加载／task assembly另列，不能当原SQL任务到达计时。
固定24任务的有效吞吐24/JCT只是完成时间的换算，不计为第二个独立收益；均分请求、单请求更短或低队列都不能单独证明完整任务加速。

| 拓扑 | 策略 | K | 三次完整消费JCT秒 | 中位秒 |
|---|---|---:|---|---:|
| single | round_robin | 4 | 0.869572 / 0.908288 / 0.870890 | 0.870890 |
| single | fixed_rate | 4 | 0.819858 / 0.894698 / 0.893502 | 0.893502 |
| single | adaptive_route | 4 | 0.891051 / 0.818739 / 0.897084 | 0.891051 |
| single | adaptive_supply | 4 | 0.886750 / 0.887259 / 0.892964 | 0.887259 |
| dual | round_robin | 8 | 0.478848 / 0.471218 / 0.499414 | 0.478848 |
| dual | fixed_rate | 8 | 0.483806 / 0.490771 / 0.474944 | 0.483806 |
| dual | adaptive_route | 8 | 0.479413 / 0.509223 / 0.505861 | 0.505861 |
| dual | adaptive_supply | 8 | 0.487913 / 0.500571 / 0.475285 | 0.487913 |

两拓扑的静态选点分别K4／K8，均为本轮1/2/4、2/4/8中独立24校准请求的最小JCT点；[全部校准](calibration.json)。
单服务固定／在线／供给中位均未超RR；双服务在线0.505861比RR0.478848慢，供给0.487913也未超RR。
双服务每一评价臂每次均12/12；各副本硬4请求填满后，静态已利用两个副本的合法余量。本轮没有需要在线异构路由才能获得的净收益。
策略函数成本单次约10–188微秒，全部原值及driver CPU、assembly见[摘要](summary.json)。该数值不含observer函数外的记录成本，JCT／process CPU包含这些成本。

## 供给、GPU与队列的联合观察

单服务评价跨度约10.76秒内，GPU0的10个1Hz样本平均利用率91.3%、内存控制器活动82.4%、功耗373.89W，显存约19276MiB；GPU1无本轮驻留，利用率0、显存1MiB。
双服务评价跨度约6.08秒内，每卡6个1Hz样本；GPU0／1利用率平均77.17%／100%，内存控制器活动60.17%／89.67%，功耗336.36／316.02W，显存19308／19240MiB。
这些是整个交错评价段的粗采样，含臂间空隙；单个0.47–0.91秒batch常短于GPU采样周期，不能据此精确比较逐臂GPU峰值或解释小差异。
内存活动百分比不是GB/s，也不能凭它诊断内存带宽瓶颈；MFU缺可靠模型FLOPs／实际执行量／精度峰值联合口径，**未测**。

服务样本running最高每副本4、waiting最高0，KV最高单卡0.00794、双卡0.00816。
同时，Core评价在途峰值单4／双8，供给目标全程4／8；D没有降压或恢复动作，故本轮不能证明降压收益或欠供给修复。
低waiting与高GPU活动、持续完成并存，直接把低waiting当容量闲置会误读此轮。Core待提交／硬容量下可提交、在途、策略与服务／GPU样本的时间线均在[数值原件](raw.json.gz)。
低GPU活动也可能来自减少重复工作；本轮每出现完整POST及token usage一致，没有这类工作量削减，不能把其他系统的低活动一概当供给不足。
无待提交的自然尾部和API/模型加载间隙不当成错误节流。完整消费JCT包含Core背压等待及响应消费／lease释放，没有把上游等待藏到计时外。

## 语义、计数与释放

40单元、820完整响应逐出现检查，576评价的content digest／finish reason／token usage全相同，校准／预热／协议的重复出现也一致。
两拓扑各评价288响应均36360 prompt tokens／1152 completion tokens；只按已收到的usage计数，不从request-count work推算实际token或FLOPs。
持久shared_requests820与API成功POST A616+B204=820相等；40登记单元全部关闭，未关闭0。
私有初次计数回执把shared_units历史登记数误命名为open_shared_units；离线审计用登记LEFT JOIN关闭标记确认0，未将40历史记录当40存活。
本轮复用缓存Qwen目录；新增deployment digest只标识serving参数声明，不是权重内容SHA，未重新做完整权重hash审计。各臂共享同一缓存目录／服务实例，真实process／argv／package／独立GPU绑定核验单列。
逐批Core held/active归零、真实一设备replica identity检查通过；07:59:02回执和[08:01:56独立释放](../model-independent-release.txt)均确认所有三个监督身份退出及GPU compute空。

[审计入口](../../../../../code/scripts/benchmarks/summarize_replica_model_replay.py)核对原件manifest、账本／API计数、逐出现响应、硬上限、身份和清理。
公开数值原件移除真实row/call IDs、GPU UUID和载荷／响应正文；不执行原SQL或语义评价，因此一致性不能代替官方质量资格。
查询每臂n3，服务直方图还受250ms跨臂采样边界影响；服务请求P99与完整query P99均未取得稳定资格，不作SLO保证。
默认RR保持。GPU物理饱和容量、更多/异构真实副本、外部客户和跨query共享总容量，以及欠供给／尾风险闭环仍待独立验证。
