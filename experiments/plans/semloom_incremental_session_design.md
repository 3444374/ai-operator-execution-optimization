# SemLoom单流增量session：近期详细设计

更新日期：2026-09-07
状态：`current / design-specified / implementation-pending`
受众：执行核心实施者。所属工作为[主设计B1–B2](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence)。
本文件唯一定义首个增量核心的操作/所有权/状态；不定义PG wire或多Job策略。

依据代码`66887463`与主设计`440f7cea`。已完成下列静态源码复核；动态行为表征、原型、实现与验证
均未执行。接口名为拟议名称，不是现有Python API。核心可以先用fixture研发，不等待SQL组合。

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

初始文件落点拟为`scheduling/core/session.py`（对象与单步驱动）和`session_capacity.py`（唯一账本）；
若实现时已有模块能完整承接则不强制建新文件。后端桥接保留在执行Adapter，不引入供应商类型到Core。

| 拟议对象 | 首版内容与责任 |
|---|---|
| SessionSpec | 单个可信job/flow、固定方法/模型能力引用、策略配置；在session内不可变，不逐任务复制完整配置 |
| SessionLimits | 最大hold任务数、input bytes、reserved result bytes、active requests/work、单项/单次offer上限、每任务/完成元数据bytes上限、step动作数、无进展期限；均显式正值 |
| OfferedTask | 连续uint64 sequence、不可变规范payload bytes、正estimated_work、最大result bytes与有界元数据；不包含SQL或PG指针 |
| TaskKey | (Engine生成且不重用的session_id, sequence)，不是payload digest；sequence溢出前永久拒绝，不回绕 |
| OfferResult | accepted_prefix_count、ACCEPTED/BACKPRESSURE/REJECTED及原因、wait generation；返回前明确全部责任转移 |
| Delivery | TaskKey、终态、只读结果/有界错误、lease_id；仅在lease释放前允许使用；不是SQL已交付证明 |
| AdvanceResult | deliveries、OPEN/DRAINING/FINISHED/CANCELLED/FAILED、blocked_reason、wake generation、next_deadline |
| CloseReport | CLOSED或WAITING_FOR_RELEASE，未确认远端任务数/占用与脱敏原因；不包含业务payload |

首版不接收可变payload对象。offer成功前调用方持有全部责任；成功后Core持有接受前缀的不可变引用和
计费责任，调用方可以保留只读引用但不得把该任务再次提交。未接受后缀仍归调用方。不可变bytes允许
共享底层存储而不强制再复制；mutable producer必须先生成有上限的快照并为转换峰值负责。
Core验证payload计费bytes而非信任调用方自报；元数据不计入示例的payload预算，但每任务/完成
元数据上限乘held_tasks给出独立上界，不能无限附加字典字段。不把logical bytes称为RSS。

sequence严格从0递增，只有接受前缀推进next_sequence。每次offer先校验整批条数、序号/类型和单项
永久上限；存在永久非法项则REJECTED且接受0，不触发派发。有效批再按多维容量选最大前缀，允许接受0。
临时结果/输入空间不足为BACKPRESSURE；单项超过空session/Engine的静态配置上限（不是当时剩余容量）为REJECTED，不能让producer永远等待。
接受前缀的账本/引用提交必须原子，提交前再次检查取消标记；准备失败或已取消时回滚本次预留，
抛错不得隐藏已接受任务。

## 3. 操作—状态—责任变化

```text
engine.open(spec, session_limits) -> session
engine.reap(max_events) -> CleanupReport  # 无存活session时仍由服务驱动回收残余
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
INFLIGHT在确认完成/取消后释放计算占用，无法确认则进入UNCERTAIN。每个接受任务只发布一次逻辑终态。
任务执行失败使整个session FAILED：不再发布待交付成功结果、不派发新任务，清理规则同取消；先前已经
交给调用方的Delivery无法撤回，调用方不得因此把整个作业记为成功。控制面FAILED保留稳定首个错误。

FINISHED仅在seal后，没有queued/inflight/ready/leased或uncertain责任时成立；输入未seal即使为空仍为OPEN。
CANCELLED/FAILED是逻辑终态，不表示资源已全清；它们可以返回尚需release/远端确认的资源摘要。
正常close在FINISHED时无额外取消效果。close重复调用返回相同终态摘要，Engine仍计未知占用。

取消请求由单一控制线程处理；其它线程只能投递取消标记并唤醒，不能直接修改账本。advance先检查取消，
每次派发前再检查；刚越过派发点的请求按INFLIGHT处理，不承诺从未到达模型。

## 4. 后端与进展机制

Engine.reap只轮询/消费有限后端终态并回收，不派发或交付用户结果；session.advance复用同一事件处理器。
无存活session时服务循环仍调用reap或等待其wake句柄，所有账本变更都在同一控制线程。

Core内部后端Interface选用三种结果明确的有界操作：

- `try_submit(task, endpoint)`：NOT_ACCEPTED（确定没有派发）、ACCEPTED(handle)、UNKNOWN(handle或有限诊断)。
  未分类异常按UNKNOWN处理。NOT_ACCEPTED保留本地queued任务和预算，等可重试条件；不暗增attempt。
- `poll(handles, max_events)`：非阻塞，返回有限终态事件；未知handle、错TaskKey、重复终态报协议失败。
- `request_cancel(handle)`：返回已确认终止或无法确认；缺取消能力时返回无法确认，不能伪造成功。

这些是执行接纳语义，不是模型业务重试。直接同步HTTP/可能阻塞的Ray submit不得未经适配就调用它们。
B2只用受控后端；真实Adapter必须另行证明其入队、事件返回、线程/队列和payload缓冲有界。
模型计算在外部，但后端实际接纳前，Core已经预留计算额度；预留成功不代表模型已执行。
配置已有SharedCreditPolicy时，计算lease引用其try_acquire结果，由同一任务记录负责release；
不另算一份独立公平credit。NOT_ACCEPTED确认后归还计算lease，UNKNOWN不归还。
Input/result预算仍是不同资源维度；finish_job必须等待Engine中的该组残余责任清理，不能在session.close时抢先调用。

advance单步按：取消/到期检查 → poll有限事件 → 校验终态并更新额度 → 发放最多指定数Delivery →
按既有admission/route策略派发队首任务。未完成的操作留到下一步；不会等待producer输入或后端完成。
同一session一次只允许一个控制线程调用，错误的重入在任何接纳前拒绝。

无可执行动作时返回`NEED_INPUT / WAIT_CAPACITY / WAIT_BACKEND / WAIT_RELEASE`和wake generation/最近期限。
Engine提供Condition式唤醒句柄：等待方在同一锁下检查generation未变再等待；输入、完成、release、
取消或服务可用事件增加generation，防止丢唤醒。时钟注入monotonic；外层runner等待至事件或最近期限，
不能持续忙轮询。非可中断Backend不是靠这一句柄自动变成可中断的。
等待达到配置无进展期限时转FAILED并保留诊断，不静默跳过队首任务，也不提高容量继续运行。

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
lease_id使用单调计数，不保存无限已释放集合；同session旧lease重复release为幂等，超过已发放上界为非法。

在途表、ready/leased表都受held_tasks限制；使用next_sequence及当前活动表识别非法/重复完成，不保留
全部历史payload、task map或事件数组。结束事件/样本通过有界sink或外部日志消费；sink阻塞/失败要有明确
错误，不能让observability后台无限积压。旧SchedulerResult的全量收集只存在legacy调用方。

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

B1的实际表征与B2结果进入独立验证记录，并与旧实现差异逐项核对；本设计只规定预期，不声称这些测试
已经运行。代码实施前先审查接口实现是否满足单步不阻塞和账本原子性；真实后端/PG资格分别验证。

## 8. B4桥接的分层前提

| 子切片 | 最小依赖 | 明确不宣称 |
|---|---|---|
| B4a 单查询单算子窗口1 | 已定义对应算子语义/本地绑定；B2接纳/lease/取消/Engine残余账本；版本化消息映射 | 不需要完整B3公平算法，不支持组合共享预算 |
| B4b 同一算子有界多在途 | B4a；PG拥有输入/重排/完成预算；producer以部分前缀与release驱动 | 单流预算可覆盖此时整条AI查询，但不是多节点总量控制 |
| B4c 多节点/多查询共享 | B3中的组/流归属、查询共享账本、可信份额、组取消和进展检查；再接相应PG组合 | 不由B2单流成功推定多Job公平或查询整体内存有界 |

跨进程不直接传递Python lease对象；B4设计必须明确接纳/完成确认、消息丢失时的责任和关闭排空，
同步wire的close不是这些操作的别名。B4详细wire规格在该切片开始前另行建立，不为B2预造全部协议字段。
