# SemLoom单流增量session：近期详细设计

更新日期：2026-09-08
状态：`current / bounded-http-smoke-verified / pg-and-batch-migration-pending`
受众：执行核心实施者。所属工作为[主设计B1–B2](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence)。
本文件唯一定义首个增量核心的操作/所有权/状态；不定义PG wire或多Job策略。

原设计依据代码66887463。单流增量核心已在2d2dee35基础上实现，并以受控后端及有界异步HTTP小规模真实验证；
旧同步入口保持原行为，生产模型协议适配、完整legacy包装及PG桥接仍未迁移。
实现与逐项测试见[验证记录](../results/scheduling/incremental_session_20260908/README.md)。

§13补充预声明能力与逐行方法续体；[受控及真实验证](../results/scheduling/method_framework_20260908/README.md)
通过44项本地/325项Linux测试和两行共4次真实生成请求。具体优化算法、总方法状态计费及PG桥接仍待实现。

## 1. 范围、选型与旧实现复用

首版采用**单线程驱动、非阻塞step、单Job单流**的session，允许有界多在途和逆序完成。测试使用可控
时钟与异步事件fixture；不使用后台线程包装同步drive来伪装增量，不增加PG连接、模型服务或新调度算法。
一个Engine暂只允许一个存活session；Engine寿命长于session，负责跨关闭保留的未知远端占用。
Engine构造时固定服务总limits和Backend/policies/clock/sink；open的session limits不得超出它，
更换session不能重置Engine总额度。各项预留同时检查session限额和Engine剩余额度。

| 现有资产与已核对事实 | 复用/迁移决定 |
|---|---|
| `scheduler.py:SubmissionAdapter`有submit、wait_one、poll_one | 复用后端概念；wait_one不能进入非阻塞advance，新增适配必须满足下述有界后台操作合同 |
| AdmissionPolicy、PoolRouter、EndpointRouter、SharedCreditPolicy | 保留策略接口/实现，调用顺序和相同fixture下决定先做B1表征，不复制策略 |
| `SynchronousScheduler.run`内部拥有拉源/等待/派发/排空循环 | 将状态与单步推进逐步提取；旧run保留，确认兼容后才包装新session |
| `_ConcurrentEnvelopeSource`是队列容量1的daemon producer | B2不使用；由外部producer显式offer，阻塞取数不属于Core |
| `SubmissionExecutionLedger`在record后删除在途，保留completions/events及顺序信息 | 在途关联逻辑可提取；不能把全历史list/order/seen集合原样放入长期流式核心 |
| `SchedulerResult`返回整批completions和事件 | 保持legacy调用方输出；全量收集由legacy runner承担，不据此声称整个旧run内存有界 |
| `BoundedReadyWindow`、现有work descriptor与credit代码 | 复用单项容量判定与work单位；首版FIFO，不实现第二套packing或fairness |
| `PayloadEnvelope`允许泛型payload | B2仅接受下述不可变有界payload；Arrow/Ray handle等由实际Adapter另定义寿命和计量，不盲目deepcopy |

B1还须逐一确认策略/credit调用是否阻塞；B2只接本地非阻塞实现，网络credit或无法满足有界调用的实现
继续留在legacy，不能藏入advance或用无界线程队列掩盖。

B1需把上述静态结论转成确定性表征：正常派发/完成顺序、容量与route选择、错误、shared-credit释放、
旧run返回字段逐项对照。未知远端失败的保守处理可能有意不同于旧submit异常即release，必须列为
新接口行为，不能因“兼容”继续错误退还额度。未覆盖的旧策略继续走原run，不同时宣称已迁移。

## 2. 公开对象与输入所有权

文件落点为`scheduling/core/session.py`（对象与单步驱动）和`session_capacity.py`（唯一账本），值对象在`session_contract.py`，既有策略适配在`session_policy.py`；
若实现时已有模块能完整承接则不强制建新文件。后端桥接保留在执行Adapter，不引入供应商类型到Core。

| 拟议对象 | 首版内容与责任 |
|---|---|
| SessionSpec | 单个可信job/flow、固定方法/模型能力引用、策略配置；在session内不可变，不逐任务复制完整配置 |
| SessionLimits | 最大hold任务数、input bytes、reserved result bytes、active requests/work、单项/单次offer上限、每任务/完成元数据bytes上限、step动作数、无进展期限；均显式正值 |
| OfferedTask | 连续uint64 sequence、不可变规范payload bytes、正estimated_work、最大result bytes与有界元数据；不包含SQL或PG指针 |
| TaskKey | (Engine生成且不重用的session_id, sequence)，不是payload digest；sequence溢出前永久拒绝，不回绕 |
| OfferResult | accepted_prefix_count、ACCEPTED/BACKPRESSURE/REJECTED及原因、wait generation；返回前明确全部责任转移 |
| Delivery | TaskKey、终态、只读结果/有界错误、lease_id；仅在lease释放前允许使用；不是SQL已交付证明 |
| AdvanceResult | deliveries、OPEN/DRAINING/FINISHED/CANCELLED/FAILED、has_immediate_work、blocked_reason、wake generation、next_deadline |
| CloseReport | CLOSED或WAITING_FOR_RELEASE，未确认远端任务数/占用与脱敏原因；不包含业务payload |

首版不接收可变payload对象。offer成功前调用方持有全部责任；成功后Core持有接受前缀的不可变引用和
计费责任，调用方可以保留只读引用但不得把该任务再次提交。未接受后缀仍归调用方。不可变bytes允许
共享底层存储而不强制再复制；mutable producer必须先生成有上限的快照并为转换峰值负责。
Core验证payload计费bytes而非信任调用方自报；元数据不计入示例的payload预算，但每任务/完成
元数据上限乘held_tasks给出独立上界，不能无限附加字典字段。不把logical bytes称为RSS。

offer只接收有限、可检查长度的批对象，不接收生成器或任意迭代源；空批合法，返回0/ACCEPTED。
sequence严格从0递增，只有接受前缀推进next_sequence。每次offer先校验整批条数、序号/类型和单项
永久上限；存在永久非法项则REJECTED且接受0，不触发派发。有效批再按多维容量选最大前缀，允许接受0。
全批接纳返回ACCEPTED；只接纳部分或零项均返回BACKPRESSURE，并以accepted_prefix_count为唯一责任转移数量。
单项超过空session/Engine的静态配置上限（不是当时剩余容量）为REJECTED，不能让producer永远等待。
接受前缀的账本/引用提交必须原子，提交前再次检查取消标记；准备失败或已取消时回滚本次预留，
抛错不得隐藏已接受任务。

## 3. 操作—状态—责任变化

```text
engine.open(spec, session_limits) -> session
engine.reap(max_events) -> CleanupReport  # 有无存活session均可回收残余；不发放Delivery
session.offer(tasks) -> OfferResult
session.advance(max_deliveries) -> AdvanceResult
session.seal()
session.cancel(reason)
session.release(lease_ids)
session.close() -> CloseReport
```

| 操作 | 允许状态与结果 | 所有权变化 |
|---|---|---|
| offer | 仅OPEN；DRAINING/终态明确拒绝 | 仅接受前缀进入QUEUED；不在offer内派发或等待 |
| advance | OPEN/DRAINING及尚需本地回收的终态；不阻塞I/O | 收完成、处理取消/超时、产生lease、尝试派发，最多step_action_limit次 |
| seal | OPEN→DRAINING，重复seal无副作用；不自动close | 不再接纳输入，在途与完成结果仍由Core持有 |
| cancel | OPEN/DRAINING→CANCELLED；重复不重复取消/释放 | 停止新派发，丢未派发/未借出结果；已派发按后端终态处理，已借出结果不可物理撤回 |
| release | 任意未完全关闭状态；重复已发放lease释放无副作用，未来/他session lease报错 | 确认消费者不再使用，释放结果预算及对应task hold；不重新发布结果 |
| close | 先执行本地cancel；有未release的lease返回WAITING_FOR_RELEASE | 无lease时释放session局部资源，未知占用移交Engine；不等待远端无限结束 |

内部任务状态为`QUEUED → INFLIGHT → READY → LEASED → RELEASED`。取消可将QUEUED/READY转DISCARDED；
INFLIGHT在确认完成/取消后释放计算占用，无法确认则进入UNCERTAIN。每个接受任务有一次内部最终处置，
至多发放一次Delivery；取消丢弃不要求逐项交付。取消确认与完成的资源结算只有§4的一处入口。
任务执行失败使整个session FAILED：不再发布待交付成功结果、不派发新任务，清理规则同取消；先前已经
交给调用方的Delivery无法撤回，调用方不得因此把整个作业记为成功。控制面FAILED保留稳定首个错误。

FINISHED仅在seal后，没有queued/inflight/ready/leased或uncertain责任时成立；输入未seal即使为空仍为OPEN。
CANCELLED/FAILED是逻辑终态，不表示资源已全清；它们可以返回尚需release/远端确认的资源摘要。
正常close在FINISHED时无额外取消效果。WAITING_FOR_RELEASE可在release后变为CLOSED；首次CLOSED
报告是关闭时快照，重复close返回该快照，后续残余变化从Engine清理报告读取。

Delivery的所有权转移点在advance成功返回。先准备返回对象，再在最后无外部调用的短段内将READY
转为LEASED并连续分配编号；此后不调用Backend、policy或sink。准备/提交过程中抛错须回滚本次
尚未返回的lease，终止时将其作为未交付结果清理；不能留下消费者从未拿到编号的LEASED任务。
以前调用已返回的lease不撤回。错误清理保持首个错误，并继续回收，不能因观测失败而跳过资源处理。

取消请求由单一控制线程处理；其它线程只能投递取消标记并唤醒，不能直接修改账本。advance先检查取消，
每次派发前再检查；刚越过派发点的请求按INFLIGHT处理，不承诺从未到达模型。

## 4. 后端与进展机制

Engine.reap只轮询/消费有限后端终态并回收，不派发或交付用户结果；session.advance复用同一事件处理器。
事件先按Engine登记的(handle, TaskKey)归属路由，再进入活动session或关闭后的drain ledger。
有活动session时仍轮询旧残余；A的迟到完成只回收A，不交给B，也不因不属于B而误报B协议失败。
真正未知或身份冲突的事件属于Engine后端协议错误：停止新派发、保留无法确认的责任，不重复结算。
无存活session时服务循环仍调用reap或等待其wake句柄，所有账本变更都在同一控制线程。

Core内部后端Interface选用三种结果明确的有界操作：

- `try_submit(task, endpoint)`：NOT_ACCEPTED（确定没有派发）、ACCEPTED(handle)、UNKNOWN(handle或有限诊断)。
  未分类异常按UNKNOWN处理。NOT_ACCEPTED保留本地queued任务和预算，等可重试条件；不暗增attempt。
  UNKNOWN使当前session FAILED并停止正常派发，该任务转UNCERTAIN，不重新提交；无handle时以
  TaskKey保留责任，不把缺少回收标识解释为未执行。
- `poll(handles, max_events)`：非阻塞，返回有限权威终态事件，是唯一计算额度结算入口。
  Adapter将取消确认与已排队完成合并成一次终态；Core收到重复终态仍报协议失败。
- `request_cancel(handle)`：返回取消请求/确认信息，不直接结算；已确认终止必须由Adapter在后续poll
  发布唯一终态。缺取消能力时报告无法确认，不能伪造成功，也不能因此立即退还credit。

这些是执行接纳语义，不是模型业务重试。直接同步HTTP/可能阻塞的Ray submit不得未经适配就调用它们。
B2只用受控后端；真实Adapter必须另行证明其入队、事件返回、线程/队列和payload缓冲有界。
模型计算在外部，但后端实际接纳前，Core已经预留计算额度；预留成功不代表模型已执行。
配置已有SharedCreditPolicy时，计算lease引用其try_acquire结果，由同一任务记录负责release；
不另算一份独立公平credit。NOT_ACCEPTED确认后归还计算lease，UNKNOWN不归还。
Input/result预算仍是不同资源维度；finish_job必须等待Engine中的该组残余责任清理，不能在session.close时抢先调用。

advance单步按：取消/到期检查 → poll有限事件 → 校验终态并更新额度 →
按既有admission/route策略尝试派发 → 准备最多指定数Delivery → 最后提交并返回交付。
任何前置步骤使session FAILED时，本次不发放成功Delivery。未完成的操作留到下一步；不会等待producer输入或后端完成。
同一session一次只允许一个控制线程调用，错误的重入在任何接纳前拒绝。

本地仍有可做动作但step动作数已用完时，返回has_immediate_work=true，外层公平地再次推进而不等事件。
只有没有立即工作才返回`NEED_INPUT / WAIT_CAPACITY / WAIT_BACKEND / WAIT_RELEASE`和wake generation/最近期限。
max_deliveries必须为正；NOT_ACCEPTED不算立即可重试工作，等待容量通知或配置的有限轮询期限。
Engine提供Condition式唤醒句柄：等待方在同一锁下检查generation未变再等待；输入、完成、release、
取消或服务可用事件增加generation，防止丢唤醒。时钟注入monotonic；外层runner等待至事件或最近期限，
不能持续忙轮询。非可中断Backend不是靠这一句柄自动变成可中断的。
通知型Adapter必须在完成可被poll读取后增加generation；纯poll型Adapter必须提供有限正的轮询间隔，
next_deadline包含下一次poll时刻。不能等待一个没有事件生产者的Condition。

| 等待对象 | B2计时规则 |
|---|---|
| OPEN且未持有任务 | 正常输入空闲，不启用执行无进展计时；输入空闲超时留给producer策略 |
| 已接受但未派发任务 | 从接纳起计容量等待期限；成功派发结束该项等待 |
| 在途任务 | 从实际接纳起计后端完成期限；到期是策略超时，转FAILED并保留未知占用，不声称后端死锁 |
| 已交付但未release | 从交付起计消费者期限；到期诊断消费者等待，转FAILED但仍需release |
| READY或步长耗尽 | 本地可推进，不进入等待；READY保留到下一步交付 |

首版上述三种任务等待使用同一显式配置时长，但按任务/阶段分别计时和诊断；另一个任务取得进展
不能重置老任务的期限。空poll、重复advance、NOT_ACCEPTED或日志事件均不重置计时。
到期不跳过队首或增加容量；停止新派发后继续有界清理。Adapter取消确认等待沿用该项在途期限，
已到期仍无终态则留Engine残余；没有伪造“已释放”的清理期限。

## 5. 单一资源账本与完成lease

对本session及Engine持有的残余任务都计算：

```text
held_tasks <= task_limit
held_input_bytes <= input_limit
reserved_result_bytes = sum(task.max_result_bytes for queued/inflight/uncertain/ready/leased)
reserved_result_bytes <= result_limit
active_requests + uncertain_requests <= request_limit
active_work + uncertain_work <= work_limit
```

accepted阶段预留input和最坏result预算；dispatch前才占计算request/work。queued取消释放全部预留；
确认的完成释放计算和已不再被后端借用的input，但结果预算保守保留到release，不在完成时一起归零。
返回结果超过预留上限即协议失败；后端产生的结果在进入Core缓冲前就必须受尺寸限制，不先接收无限值再检查。

READY按完成顺序交付，Core不为输入顺序无限缓存。LEASED仍计Core预算，调用方处理/复制完成后显式release，
因此慢消费者自然阻止新offer。调用方另有自身存储预算；拷贝期间两边分别计费。release不代表数据库提交。
lease_id为(session_id, lease_ordinal)，每个session从0连续发放，只有成功交付才推进上界。
检查session归属后，低于上界且不在活动lease表中的编号才是幂等旧release；未来/其他session编号拒绝。
不保存无限已释放集合，失败返回不能制造编号空洞。

在途表、ready/leased表都受held_tasks限制；使用next_sequence及当前活动表识别非法/重复完成，不保留
全部历史payload、task map或事件数组。sink接口本身必须非阻塞且有界；不可直接接同步文件/网络I/O。
满队列/失败使session停止新执行并记录稳定诊断，清理不再依赖该sink成功；不能事后检测永久阻塞。
旧SchedulerResult的全量收集只存在legacy调用方。

UNCERTAIN的request/work以及后端仍借用的input/结果预留转入Engine的有界drain ledger，不能在close后
退还并允许新session超额提交。handle后续终态可以完成回收；没有可信终态时保留至管理员确认服务结束后
重建Engine，不按固定睡眠猜测完成。close报告残余而不阻塞，不声称Engine内存或所有后端线程已经归零。
Engine重启不等于远端工作结束；恢复前必须核对服务状态，不在本切片实现持久化恢复或自动重试。

## 6. 正常、中断与兼容时序

正常：producer生成不可变任务 → offer校验/预留/提交接受前缀 → advance派发 → 后端完成事件 →
advance释放计算并给Delivery → 消费者验证/处理 → finally release → seal且排空后FINISHED → close。
配置/模型引用在open时固定，任务不重新构造部署配置；输入未结束前即可交付完成。

中断：控制线程收到cancel或consumer异常 → 停止offer/pull/派发 → release所有已借结果（包括处理失败的）
→ 未派发任务丢弃 → 尝试取消在途 → 已确认终态回收，未知留Engine → close返回清理摘要。
Core取消不自动等于PG全查询取消；该映射属于B4。首次PG桥接前已有同步节点仍通过断开各自连接收尾。

旧run迁移顺序：先保存B1输入/输出/策略调用表征；提取可共享的策略/在途关联；实现单流session；
最后给已通过对照的旧run提供producer/driver/collector包装。新的incremental caller不经过legacy源线程
或全历史collector。时间指标、策略调用时机、异常分类与返回字段若不能保持，单列兼容差异，不直接替换全部runner。

## 7. 首批可判定测试

数值是测试参数，不是生产推荐。设置hold_tasks=2、input_limit=8B、result_limit=8B、active_requests=1、
work_limit=2；每项payload=4B、result_bound=4B、work=1。metadata单独使用固定有限测试上限。

| 反例 | 必须结果 |
|---|---|
| offer三项seq0/1/2 | 接受2，seq2仍归producer；再offer seq2返回0/BACKPRESSURE，没有派发 |
| 第一项完成但未release | active可归零，但hold/result仍占；不能因计算结束就继续接受seq2 |
| release第一项，再offer seq2 | 接受1；旧payload/task不残留全历史list |
| 9B单项或错误seq在批中 | 整批REJECTED、接受0，原next_sequence及额度不变 |
| 尚未seal，只有一项已完成交付/release | 状态OPEN/NEED_INPUT而非FINISHED |
| seal后排空 | FINISHED；seal重复无副作用；新offer明确拒绝 |
| 容量2、seq1先于seq0完成 | Delivery次序1、0；TaskKey正确、各一次；不是重复payload去重 |
| 相同payload、不同sequence | 两项独立接纳与结果，任务数2；再次提交已接受seq拒绝 |
| 重复后端完成、未知handle/错TaskKey | FAILED，不再次释放credit或交付结果 |
| cancel时分别有queued/inflight/ready/leased | queued/ready丢弃、已发请求仅尝试取消、leased等待release；无新派发 |
| close有leased结果 | WAITING_FOR_RELEASE；release后close完成，无提前释放或悬空引用 |
| 未确认请求后close并open新session | Engine旧额度仍占；新提交受剩余容量约束，不重置成全空闲 |
| completion到达与准备wait交错 | generation避免丢唤醒；无事件时阻塞或到期，不能CPU忙循环 |
| 无限producer持续完成/release | 活动表/元数据窗口有界，与总处理条数无关；不只看最终进程退出 |
| 准备Delivery后后端、返回对象构造或sink失败 | 本次无消费者不可见lease；旧lease仍可释放，close不会永远等待本次返回 |
| cancel确认与已排队completion交错 | Adapter只发布一个权威终态，计算额度只结算一次 |
| step耗尽但仍有READY/可派发任务；后端只有poll | 前者立即再推进；后者按有限期限轮询，不永久睡眠或忙循环 |
| A关闭、B打开后A迟到完成 | 仅回收A残余，B状态/结果关联正确；UNKNOWN无handle仍保留额度 |
| 空OPEN、持续NOT_ACCEPTED、未release、其它任务持续完成 | 分别按等待对象计时，不误判输入空闲或靠无关动作刷新老任务期限 |
| 任意有限二进制payload、不同result_bound | Core不解析文本，不假定固定结果长度；跨session lease拒绝 |

B1实际表征与受控B2结果见验证记录；只对已测路径声明支持。代码实施前先审查接口实现是否满足单步不阻塞和账本原子性；真实后端/PG资格分别验证。

## 8. B4桥接的分层前提

| 子切片 | 最小依赖 | 明确不宣称 |
|---|---|---|
| B4a 单查询单算子窗口1 | 已定义对应算子语义/本地绑定；B2接纳/lease/取消/Engine残余账本；版本化消息映射 | 不需要完整B3公平算法，不支持组合共享预算 |
| B4b 同一算子有界多在途 | B4a；PG拥有输入/重排/完成预算；producer以部分前缀与release驱动 | 单流预算可覆盖此时整条AI查询，但不是多节点总量控制 |
| B4c 多节点/多查询共享 | B3中的组/流归属、查询共享账本、可信份额、组取消和进展检查；再接相应PG组合 | 不由B2单流成功推定多Job公平或查询整体内存有界 |

跨进程不直接传递Python lease对象；B4设计必须明确接纳/完成确认、消息丢失时的责任和关闭排空，
同步wire的close不是这些操作的别名。B4详细wire规格在该切片开始前另行建立，不为B2预造全部协议字段。

## 9. 首版选择与扩展位置

以下是工程选择，不是新研究结论；实施只提供已有需求使用的最小内部函数，不先建设插件平台。

| 首版选择 | 可替换位置与必须保留的责任 |
|---|---|
| FIFO | 从有界只读ready window选任务与账本/派发分开；策略不改状态或自存无限队列，Core校验选择合法性 |
| 单项提交 | 单成员工作单元reference；任务到提交及完成映射集中在Adapter/关联记录，不在多个账本处假设任务数等于请求数 |
| offer时预留最坏结果 | 保守reference；长期要求是产生输出前已有受控完成空间。派发前预留或分块交付须另证有界性 |
| bytes载荷 | 首个可计量表示。未来引用/张量须定义版本、真实尺寸、所有者和释放；metadata小handle不能掩盖所引用大对象 |
| 一个Engine一个活动session | B2验证范围；B3在同一服务资源域扩多流，不能每查询复制全部服务容量 |
| 有限任务、一次终态结果 | 图像有限输入可扩类型/编码；视频先以方法定义的有限片段分解汇总，持续单任务多输出需另定事件与分块lease协议 |

encoded、decoded/prepared与结果可能同时存活，阶段适配负责转换峰值的预留/转移，不能让每种策略
自行估计内存。estimated_work只限定估计工作量，不是已证明的GPU显存上限。当前没有图像/视频
适配资格，二进制fixture只检验中立接口。组织batch不自动授权合并多行prompt或改变单行请求语义。

## 10. 2026-09-08实施记录

基线2d2dee35。复核scheduler.run、SubmissionExecutionLedger、BoundedReadyWindow、
StaticAdmissionController与RoundRobinEndpointRouter：现有策略继续复用；旧账本保留历史和阻塞
wait_one，不直接进入新advance。单项bytes适配构造已有BatchRequest，保留中性work_units。
新增单流账本只持有活动责任；旧run不在这一步替换，先运行既有调度表征并核对相同fixture的路由。
参照主设计§8.7的执行核心共享原则及§1来源表；本步为自有工程决策，不复制公司代码，不涉及
模型客户端或SQL函数属性。测试使用确定性本地后端和注入时钟，不启动真实模型或PG服务。
验收覆盖接受前缀、逆序完成、release背压、错误/取消唯一结算、跨session残余、唤醒和阶段期限。


### 已实现范围与继续工作

本轮公开实现是SessionEngine/SchedulingSession及不可变输入/结果对象；最多一次有限poll，随后
终态、派发与交付共享step_actions预算，扫描/计费只遍历受held_tasks限制的活动表。
没有跨调用的全历史结果/事件集合；完成按实际终态到达次序交付，调用方决定是否另外重排。
输入规范化在接纳之前；数据组织属于同一执行上下文中的策略阶段，Backend承担实际执行。
首版只选择已接纳的单任务，传递不变bytes和中性work，不改变prompt或结果解析。
SessionSpec指定operator/work_unit，转换已有BatchRequest只供旧策略读取，不把payload送给策略。

复用限定：静态admission、已有RoundRobin路由和无历史事件的本地FIFO credit已验证。
credit新增incremental_safe与forget_finished_job；只有权威终态清空所有责任后才finish并退休历史。
旧finish_job继续保留原统计，旧SynchronousScheduler/SubmissionExecutionLedger仍由runtime/execution使用，
本轮不删除有效消费者依赖，也不把旧run全历史collector带入新核心。未知submit异常保留占用，
与旧run异常即release是有意不同；新接口不承诺旧阻塞等待统计或全量输出形式。

后续工作：验证真实非阻塞Backend的队列/缓冲/取消与唤醒；在表征覆盖对应策略后迁移旧driver与collector。
尚未迁移的动态/远程credit、pool选择、数据组织batch等不因结构可传入而获得非阻塞资格。
PG消息版本、accepted-prefix和结果重排按B4另做；多个Job的公平策略按B3另做。
取消原因目前只用于调用者控制，核心不保留任意调用者文本；稳定错误代码避免将payload带入报告。


## 11. 为数据组织与优化执行保留的职责（2026-09-08修订）

session参与提交，是持续运行的执行上下文，不是与“数据执行”并列的外围工具。
长期顺序为：PG/producer产生已确定任务 → session接纳并拥有输入 → 有界数据组织/准备 →
提交与路由 → Backend → 成员完成关联与资源回收 → 消费者release。
优化策略改变执行选择；接纳、所有权、取消和一次结算规则不能由策略另行维护。

当前FIFO已经从advance循环提取为SessionPolicies.choose_task的默认实现。选择器只看到有限的
不可变TaskCandidate视图，返回一个已接纳TaskKey；Core检查其合法性并独占账本变更。
反序选择测试证明顺序可替换而额度/身份不变，非法选择在派发前失败。
这只完成任务选择接口，不声称任意batch形成或多阶段执行已接通。

| 对象 | 当前实现 | 后续批处理接入的责任 |
|---|---|---|
| 语义任务TaskKey | 每项固定sequence、payload、结果预留；完成/lease归属稳定 | 即使合批，也保留每个成员的身份、NULL/error和结果归属；不得把整个批次伪装成一个语义任务 |
| 物理提交 | BackendTask为显式单成员reference；每TaskRecord目前持一个handle/计算预留 | 引入有界提交记录，持member TaskKey列表、成员结果对应及一次request/work lease；同一批终态只结算一次计算额度 |
| 数据组织策略 | 默认FIFO选择单个已接纳任务；不自行提交 | 在同一session中选择成员/准备表示；不在外面增加拥有独立额度的第二提交器 |
| 阶段缓冲 | 当前只有不可变输入和最坏结果预留 | 接入encoded/decoded/prepared时先向Engine预算申请阶段lease，计转换时两种表示重叠峰值；不能藏在metadata handle里 |
| 既有物理资产 | BatchRequest/WorkDescriptor供已有策略读取 | 优先适配planning/blocks.py:StageBlockDescriptor与runtime/stage_broker.py:BoundedStageBroker；任务成员和阶段lease映射由执行适配负责 |
| 资源所有权 | SessionCapacity管理当前任务与关闭后的残余；本地FIFO credit管理计算许可 | 同一资源只由一个账本授予额度，StageBroker需要消费Engine授予的预算或迁移该维度，不能并列各持完整容量 |

上述StageBlockDescriptor和BoundedStageBroker已按当前源码只读核对：前者包含row_ids、表示、
物理/逻辑/ready bytes及分阶段work，后者有encoded→prepare→ready→model状态和提前预留。
它们不等于新session已经支持这些路径，也不应被新写的batch框架复制。
本轮不实现没有实际消费者的多成员提交对象；打开批处理前必须先实现member/result映射、
request与task分别计数、部分失败及取消后的整批/成员归属测试。当前单成员计数仅为reference，
文档和API不得把其等价关系外推为长期要求。

旧同步入口仍有消费者，保留到旧driver可通过同一增量核心得到相同输出/统计时再迁移；
迁移完成后删除被替代的运行循环与历史收集重复实现，保留历史证据和恢复身份。

最终验证：本地33项新增测试、Linux314项调度测试通过，616项源码哈希一致；零模型请求、Raylet残留0。

## 12. 有界异步传输与真实核心验证

用户明确要求增量session也进行真实模型测试后，增加runtime/async_backend.py作为受控协程传输。
它的单个I/O线程运行真正的异步请求，不包装同步drive；try_submit/poll不等待网络、不拥有调度策略。
传输槽数有明确上限，完成未poll前也占槽；网络异常保留UNKNOWN，request_cancel不伪造远端取消确认。
其调用方必须在读取响应时限制字节；生产模型适配和完整PG桥接仍需对应协议检查。

独立真实核心诊断使用5次请求的新AttemptLedger，不改此前12次PG组合预算：先offer3项接受2，
实际并发派发2项，完成后不release时第三项仍背压；release后接受并完成第三项；另一个session
提交第4项后取消/close，新session提交第5项，旧完成只回收旧责任，不能成为新session的Delivery。
取消仅验证本地停止交付与等权威响应再结算，不声称vLLM已经接受远端abort。
模型/版本与前次同一已核验7B资产，单GPU独立localhost服务、最多2个HTTP请求；输入为公开合成
规范请求bytes，响应逐块限64KiB，单项生成上限64 tokens，取消项128；无重试、无正式性能比较。
全部请求经新SessionEngine和BoundedAsyncBackend，记录TaskKey、接纳/交付/释放、实际usage与终态；
正常结束要求任务/额度归零、协程线程退出、模型进程与端口退出和GPU空闲。任一失败保留独立尝试，不自动扩大预算。

实际验证：本地37项核心/传输测试、Linux318项调度回归通过；新核心5次真实请求通过，峰值HTTP并发2。
[结果记录](../results/scheduling/incremental_real_20260908/README.md)保留配置/身份、预算、结果、状态与清理。


## 13. 多阶段方法接入准备（工程决策，2026-09-08）

本轮只完善接入接口，不实现embedding评分、阈值校准、级联算法或PG异步协议。
沿用主架构§8.7、§8.8已记录的公司语义Module/carrier分工和pgml模型能力封装比较；
保留自有SessionEngine，不引入公司实现。公开LOTUS approximation cascades说明用于核对
“代理判断后可继续请求主模型”的变化需求；尚未复制LOTUS源码或声称1.2.4行为等价。
当前公开LICENSE为Apache-2.0；后续源码适配须固定实际commit、保留license/NOTICE及修改说明，
并用该版本输入输出进行parity测试。

源码落点：session_contract预声明任务profile，session对整个offer批次验证profile并为每项绑定
有效能力；semantic_methods提供只产生请求/最终结果的有界逐行方法续体。算子方法决定阶段，
执行核心继续拥有提交、背压和回收。方法不持有backend、全局模型客户端或独立提交循环。
默认profile保持原路径；不同阶段的work须由适配器换算成同一session work_unit，不能混加原始单位。
方法续体保存的bytes及阶段数有界；外部驱动仍须限制活跃行数、合计方法状态和最终结果占用。
输入EOF不等于session可seal：必须等所有方法不再产生任务。请求接受后才绑定TaskKey，
阶段结果由驱动交给对应方法并释放lease；不能在持有唯一结果槽时等待下一阶段。

验证使用零请求提前完成、一阶段及两阶段合成方法，复用真实SessionEngine和受控backend，
检查profile切换、整批拒绝、关联、续体大小、阶段上限和单槽回收；不建立模型质量结论。

用户随后授权本轮启动真实模型。追加独立4次POST预算，合成两行各执行draft→revise两个生成阶段；
两种profile映射同一已核验7B模型，第二阶段输入包含第一阶段真实响应。单任务/单HTTP槽，响应上限
64KiB，输入上限8KiB，状态64bytes，最多两阶段，每次生成64tokens，temperature=0；不额外warm-up或重试。
复用已验证的控制器/HTTP诊断适配器，使用新的源哈希、preflight和AttemptLedger；不使用旧请求预算。
检查阶段profile、TaskKey、原始响应、lease释放前背压、读完输入仍可继续阶段任务、最终行结果和资源归零。
失败即保留本次记录并停止，不扩大预算；控制器负责退出I/O线程与自有模型进程、检查端口关闭和GPU空闲。
这只验证新框架真实接通，不涉及embedding、近似质量、性能比较或PG异步。

第一次真实准备在0次POST时被advance的delivery bound校验拒绝：诊断脚本仍传2，held_tasks已改为1。
保留失败控制器/日志，修正为advance(1)后在新的准备目录验证；复用同一持久4次预算，
不增加模型请求许可，不改变生产代码或验收标准。

## 14. 后续接线与旧入口迁移顺序（待实现）

结合本轮源码审查确认：SessionPolicies仍构造row_count=1、空prefix_key的BatchRequest，
Engine仍限一个活动session；数据组织尚未接入这条路径。方法续体只是逐行顺序接入点，
不能据此声称支持跨行校准、并行阶段、物理批次或多Job调度。下一步按以下顺序形成实际消费者，
不预先增加空registry、万能DAG或第二个资源账本。

1. **任务与物理批次分开表达。** 复用现有WorkDescriptor、batching/token-budget/service-quantum
   以及§11列出的阶段描述。方法产生稳定TaskKey和已确定语义的工作；组织器从有界就绪窗口选择成员，
   产生包含成员身份、能力、表示及work信息的物理提交。prefix/locality信息来自可验证的typed描述，
   不能从opaque payload猜测，也不能继续用空值假装已支持。一个批次到底对应一个HTTP请求还是
   多个backend请求须由适配器明确声明；request额度按实际物理提交计，不再与task数混为一谈。
2. **先贯通单流组织与回收。** 用真实消费者连接“方法→组织器→物理提交→session/backend→成员结果”。
   验证至少两个成员、逆序完成、成员部分失败、结果映射、取消及迟到终态；每个成员只交付一次，
   每笔request额度只结算一次。对不能批量执行的backend保留显式单成员回退及对应计费。
3. **表示转换共用预算。** 接入encoded/prepared/张量表示前定义申请、转换重叠峰值、释放与取消归属，
   BoundedStageBroker消费Engine授予的预算，不能持有另一份完整容量。方法状态、最终结果也要纳入
   驱动层总预算。GPU显存能力按后端实际可观测/可控制程度声明，不能把估计work标量说成显存保证。
4. **再扩展同一Engine的多session。** 在共同资源域中保存各流就绪状态及份额，接纳/选择跨流进行；
   先验证总量不超限、一个流取消不影响其他流、旧任务不向新流交付，再进行公平策略研究。
   不以复制多个Engine的完整服务额度代替共享调度。
5. **迁移后退出旧编排。** 先从scheduler.py抽出已被两条入口使用的公共策略Protocol，保留原import
   兼容；选取一个现有runner通过新核心运行，对比输出、错误、取消、统计及资源行为。逐个迁移调用方，
   直到旧同步循环无直接执行消费者且兼容回归通过，再删除旧循环。最终保留的同步入口只驱动同一
   增量核心；原始实验、失败证据及可恢复版本继续保留。

跨行校准应由有界样本/方法上下文承接，不塞入单行续体；并行阶段需要显式依赖及fan-in协议。
这两项先有具体方法消费者再落实，避免为未来任意算法预建通用工作流系统。
