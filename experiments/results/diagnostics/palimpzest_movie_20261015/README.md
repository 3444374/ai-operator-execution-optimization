# Palimpzest Movie 原生接线与固定计划边界

本切片先建立原 SemBench Movie Q3 完整过滤／count，再保留 Q2 LIMIT5，
使用 PZ 原并行策略和 Generator，未改上游源码。接口与运行方法见
[PZ 模块](../../../../code/src/semantic_methods/palimpzest/README.md)；
[本节计划](../../../plans/语义系统对照.md#palimpzest-native-and-fixed-plan)与[公开原值](qualification.json)分开保存。

固定 SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2`，
PZ 专属 tag `0.8.2.sem_agg` 对应 `6efc19841c49d727ce42f5c2dd55e7433601d2d9`。
使用显式 `QueryProcessorConfig`，避免旧 `Dataset.run` 丢弃 kwargs；
只提取原查询方法并捕获其终端 run，不使用 generic runner 的 Gemini 回退。
旧 vLLM enum 与精确本地服务模型的绑定、原模型卡与实际费用分别记录。

## 软件资格

本机 Python3.12 独立环境中运行真正固定 PZ／LiteLLM／OpenAI 库，
使用 8 行合成数据和 localhost HTTP fixture，无模型推理、无 GPU 工作。
PZ0.8.2、LiteLLM1.95.0、OpenAI2.50.0、pandas2.3.3、NumPy2.0.2、Arrow20.0.0；
Pydantic2.14.1、HTTPX0.28.1 实际版本另记。依赖解析、安装、freeze 与 preflight 原件仓外保留。

| 查询 | 实际 fixture POST／完成 | 原生实际配置 | 关系结果 |
|---|---:|---|---|
| Q3 | 6／6 | parallel，workers4，join4，batch_size=null | COUNT=5 |
| Q2 | 6／6 | parallel，workers4，join4，factory 将 batch_size 夹至5 | 5行，保留重复 reviewId |

Q2 的结果上限不是请求上限。两条查询的桥均恢复、活动 Generator 归零，
错误及解析默认均为0；这不单独证明远端 GPU 已停止。
Q3 刻意让一次假阳性和一次假阴性相互抵消，COUNT 误差为0，逐行审计仍分别记下 FP1、FN1。
失败／解析默认与真实 negative 分开，缺 usage 为 unavailable，原成本卡默认零不解释为免费。

固定计划 CPU 检查另用原库完成：原 prompt/parser/usage、completion／parse 默认、missing-usage 原异常、
parallel 的完整 count、禁止重优化和计划变化拒绝。它们使用内存 fixture completion，不产生 HTTP 或模型请求。
首轮 sandbox socket 拒绝与缺 cdlib 的失败保留；首次测试临时目录已清理，不能声称恢复其原始响应。
其后受影响项在补依赖后使用新的输出目录重跑；unittest 总耗时不作为查询性能。

独审发现并修正两项接入合同问题：响应模型身份失配现为致命合同错误，并阻止之后的新提交；
另将 metadata GET 纳入新目录的失败记录。新增真库反例中，两行错模型仅发1次 POST，
普通503两行仍发2次并保留上游默认；新增3次 fixture POST、模型0，未重跑上项12次或改写其来源。
metadata 错误／失联保留配置与失败摘要，busy lease 不创建输出。

上述12次 fixture 原件的运行标识为 `pz-native-cpu-20261015-r2`，新增3次反例为
`pz-native-stage1-fixes-r1-identity-guard`。两组原件及原库 CPU 首次失败／复验日志
保存在仓外 `pz-stage1-software-evidence.tar.gz`（30成员，22142字节，SHA-256
`2eb01852b3c966f3991c42886e26bac5688a2d88fe4474844dd3a56ec4185990`）；
可按运行标识与校验值向维护者索取，包内清单列出逐成员字节数与摘要。
原12次资格 JSON 的 SHA-256 为公开原值中的 `original_qualification_sha256`；
保全不补回已缺失的首次临时响应，也不升级为当前发布源码的模型或后端资格。

## 一次原生 Movie Q3 模型资格

执行源码 `7ac292e0a174dd52701cfb1a8675b918403c3f7a`，复用已核2000行输入（CSV摘要 `ad5def7d`、清单内容 `e753baa5`），
原普通谓词选出120行；使用现有 Qwen2.5-7B-Instruct／vLLM0.25.1、单4090、float16，PZ原 parallel／workers64／join64。
真实请求120／完成120，无warm-up、HTTP／解析失败或无效预测。COUNT=7，真值14，官方相对误差0.5；
完整过滤审计为TP7、FN7、FP0、TN106。`passed`表示本次执行合同完整，不表示质量等价；
模型／标签判定差异保留，不靠重试、改提示或追加模型消除。usage为38,325输入／7,666输出tokens，美元价格不可用。

原查询构造／run至关系返回为8.875966797秒，之后质量评价与证据写出另计；
runner输入与依赖核对11.952560934秒、服务准备87.284129010秒分别记录，不能相加后仍称查询时间。
原计划写prefix cache开启，本次服务实际显式关闭；[原值](native-model.json)保存此配置偏差，不能当同缓存设置的性能对照。
本轮未保存所选完整物理计划对象／指纹，不能充作固定计划后端比较的原生控制。
单查询不提供可靠query P99／P99.99或系统性能排名。

独立服务owner核对退出码0、自有残余进程及GPU计算进程均为空、GPU回1MiB、共享lease正常释放，
始终未启用stale recovery；不从桥恢复推定设备停止。vLLM退出日志有一项semaphore清理警告，原件保留，
这里只确认实际观察到的进程／GPU边界。完整原件34文件、压缩109,793字节，SHA256见原值，全部留仓外。

<a id="fixed-plan-cpu-backends"></a>
## 固定计划交接与真实 CPU 后端资格

本次实现从选定物理计划之后、原逐行线程池之前交接合法就绪工作；
查询私有副本保留 PZ 原执行函数体、DAG／future 队列和非模型算子，原 Generator 在同一查询线程中继续。
原 LiteLLM／OpenAI／HTTPX 准备完整 SDK 请求后暂停，由所选后端执行；
完整状态、响应头及正文回到原 SDK／parser／stats／DataRecordSet。
未修改上游 PZ／SemBench，也未把 PZ 算子嵌入 LOTUS。greenlet3.5.6 只在同线程切换，
新增 wire／stack 上限约束适配器保留值，不构成全查询 RSS 上限或跨后端并发控制器。

固定计划入口记录源数据／schema／UDF、提示／解析、模型、依赖、关闭的客户端缓存及外部服务／缓存声明；
每臂重建独立算子、Generator、source 和原策略，不再优化。模型绑定与 metadata 使用独立冻结副本，
改写公开配置不能改变发送目标。部署预算与后端 worker／背压分列，当前只允许显式 fixed profile；
自动调度不等于已核实在线调优。原生旧 PZ 零幸存者可能返回无列空关系，该行为保留并标 compatibility-invalid，不补造 COUNT=0。

首版 `fixed-local-nonredirect.v1` 在原 SDK 准备前明确关闭 redirect，保留此 profile 的完整请求。
3xx 保留 SDK／PZ 原错误默认和失败账，再停止新派发；不借此声称已覆盖 SDK 默认 redirect 行为。
零 SDK send、第二次 send、模型／关联失配、超出准备额度和未知远端责任均拒绝继续派发；
普通 provider／parser 异常仍保留原 False／None，单独记 invalid，缺 usage／价格继续为 unavailable。
已完成及取消 Future 释放新增请求和 greenlet 引用，原 PZ 暂留 Future 时不虚减仍被引用的 wire 额度。

实际 Linux CPU 软件资格使用真正固定 PZ／SDK 和后端库、3行确定性 HTTP fixture：
两行具有重复业务ID，一行由普通谓词排除；选定计划包含原 LLMFilter 与结构 COUNT。
这是 Q3 形状的软件检查，尚非原2000行 SemBench 方法／真实模型的逐臂对比。
执行底稿为7ac292e0加逐文件校验的 Stage2 overlay；底稿单独不含本次适配，完整摘要由[软件资格原值](software-qualification.json)保存。

| 路径 | 实际 fixture POST | 原关系结果与退出 |
|---|---:|---|
| native-adapted | 2 | COUNT=1，原 SDK／parser、调用账和自有退出通过 |
| Daft Native | 2 | COUNT=1，原生完整调用图、consumer／HTTP池退出通过 |
| Ray Data | 2 | COUNT=1，原生批次／worker、关联及自有 Ray 退出通过 |
| 作者 IMLane CP | 2 | COUNT=1，n4，DBEnd／worker trace、Coordinator／SHM正常退出 |
| 作者 IMLane Ray | 2 | COUNT=1，n4，独立4CPU／0GPU Ray与作者进程正常退出 |
| SemLoom＋Daft／Ray | 2 | 原 PZ／SDK＋实际物理后端组合COUNT=1，Job／session／存储／传输退出通过 |

IMLane 复用固定作者072f8257的既有已审 patched build；本次未改作者源码、环境或重编译。
Coordinator 的n4及实际创建4个lane与DBEnd本地META的executors6／host threads8分列，
后者不代表实际作者worker数。Ray日志保留2条 SchedulingCancelled，涉及的actor与实际完成请求的2个worker不同；
该阶段只证明2-call结果与退出；其后的告警核验与4个lane健康补轮见下节，不能回填本轮资格。

另一个实际 Core／Daft／Ray 检查为5次 fixture POST：下游在上游尚未结束时派发，
gzip429与重复响应头保留，命名池身份／复用和发送前拒绝0POST均通过；
3次查询的 Job、存储与传输归零，正常lease释放；该独立检查配置Ray逻辑2CPU／0GPU，未设driver的CPU affinity。
gzip响应字节在运行时断言，未另存响应原件；独审未声称重新解码该响应。`physical=None` 仍仅为本地诊断。
有限 vendor 图可按就绪快照消费，SemLoom 则保持一份查询 Job 与开放算子流直到方法结束；
允许合理原生批次和内部生命周期不同，未从软件检查推导性能优劣。

Ray Data 首次未设置 import-time 异步批数，声明2而实际默认4，守卫在0POST拒绝；
首轮 Daft 子结果已通过，整轮因随后的 Ray 失败仍为 failed。只补底层错误观察的第二次 Ray 仍0POST失败；
随后在新进程导入前显式设置2，实际值2并通过，未改生产代码、未重跑已通过 Daft。
全部失败与成功原件、作者来源、源码摘要及正常 lease／PID身份退出分别保留仓外；
进程被强制回收不能算正常资格。本阶段实际共17次fixture POST，真实模型／GPU工作为0；其中12次为六条PZ接入、5次为独立Core检查。
fixture usage不是模型费用或吞吐证据；有限原件按run_id／SHA由项目artifact owner取回，不提交冗余debug包。

首版仍拒绝 LIMIT、join、distinct、语义聚合、多字段 follow-up 和未核多调用形状。
PZ 原生模型轮未保存完整所选 plan，不能回填为固定计划控制；
原2000行任务的固定计划模型轮按下节分列R3六完成臂／SemLoom发送前失败与后续独立R4；不是同完整身份的七臂比较。

<a id="imlane-four-lane-health"></a>
## IMLane 四 lane 健康补轮

后续检查使用干净的固定 `adde01a660473dde626d6d04169e52487d879c74`，保留同一作者构建与原调度。
CP／Ray各连续两批、每批4次真实localhost HTTP，共16次fixture POST。
每批实际4个不同worker、4个lane恢复且HTTP峰值4；屏障在第4个请求到达前不返回响应，关联与结果断言全部通过。
选中原件保存请求／trace及已审脚本的逐条响应断言结果，未另存响应字节用于独立重新解码。
Ray每批另核4个不同actor，两批复用同一组worker／actor，DBEnd仍按原默认每批注册；
作者未发出actor到lane的逐点映射，实际actor集合与lane恢复分别核对。

旧2条取消告警的源码／GCS链支持重复同名注册的新actor失去引用：作者`emplace`保留既有条目，
未保留的新handle被销毁。本补轮仍有7条同类创建取消告警，7个精确ID均有GCS无引用销毁记录，
与实际完成请求的actor无重合；告警原样保留，不称已消除或推广到所有注册错误。
两种runtime的自有进程／SHM、fixture线程和桥均正常退出，父owner无残余或unknown，正常共享lease已释放、未启用stale recovery。
[原值与归档身份](imlane-health.json)保留来源与边界；本补轮模型调用和GPU工作均为0，不替代PZ原任务模型资格。

<a id="fixed-plan-model-rounds"></a>
## 原任务固定计划模型轮

首轮 `pz-q3-adde-fixed-r1` 使用干净adde与已冻结仓外控制器，在首个original-native臂的metadata GET处失败。
原观察器在判断请求类型前拒绝`stream=True`，而原`fetch_model_metadata`使用流式GET；尚未进入Generator。
账本实际消费、服务日志模型POST和Generator调用均为0，已分配的840额度未退款或复用；后续新运行仍受累计840次实际POST上限约束。
服务准备86.113515530秒已实际初始化GPU，不能把0模型POST写成0GPU工作。本轮不提供模型或质量资格。
自有责任关闭、服务退出码0、无自有残余及GPU计算进程、GPU回1MiB和正常lease释放均已核，未启用stale recovery。
15份原件及清单留仓外，归档SHA256为`e69932ec0b359865fa677157b2bef068e2bb00a9dc6bb1bf2134e16864384298`；
按运行标识与摘要向项目artifact owner获取。第二轮仅修仓外GET观察范围，固定adde及已知关闭错误分支的边界保持。

新运行`pz-q3-adde-fixed-r2`通过GET后，在完整metadata身份比较处再次停止；真实POST／Generator／账本消费仍为0，
服务准备50.121419687秒，正常自有退出与lease释放完成。安装的vLLM源码每次GET新建model的`created`和permission的随机`id`／`created`，
整个响应的哈希会随这些观测值变化。第二次GET原正文未保留，不能称已实测逐字段差异；安装源码和原始失败分别保留。
本轮未生成原关系结果或PlanStats，COUNT与质量均不可用。[各轮原值](fixed-model.json)分列原件摘要与预算；
后续`bebf682b`保留初始完整metadata计划锚，重复GET仅规范化上述三类动态值，并保存逐次原正文及完整／稳定摘要；
该修补已通过CPU检查与双轴独审，模型资格另记，不回填adde资格，不自动重复模型查询。

后续集成`0bdfaae27fc7ea7cb293165d2567aff738c447ed`另修`response.close`与`after_send`独立尝试及原错误优先；
该修补不属于本轮adde或bebf资格。bebf继承adde的关闭失败分支，该分支仍未验证；发生关闭或观察错误即停止后续臂，由独立服务owner核物理退出。

<a id="fixed-plan-model-r3"></a>
### R3：六臂完成，SemLoom发送前失败

新运行`pz-q3-bebf-fixed-r3`固定`bebf682b136b983257c70944aa0cb07000c976ac`，复用原2000行输入及一次选定的扫描→普通过滤→LLMFilter→结构COUNT计划。
每臂fresh状态，原生控制为原`ParallelExecutionStrategy.execute_plan`；Qwen2.5-7B／vLLM0.25.1／单4090／prefix cache关闭，无模型预热、重试或选择阶段模型调用。
前六臂各120次真实POST200，SemLoom首次发送前失败，整轮仍为failed；累计实际消费720／840，余120未退款、复用或自动补跑。

| 路径 | 实际POST | COUNT | 输出tokens | query秒 | application秒 |
|---|---:|---:|---:|---:|---:|
| PZ原生固定计划 | 120 | 7 | 7,685 | 13.304 | 13.791 |
| native-adapted | 120 | 7 | 7,695 | 13.321 | 13.867 |
| Daft | 120 | 7 | 7,665 | 15.832 | 16.381 |
| Ray Data | 120 | 7 | 7,655 | 60.086 | 69.980 |
| 作者IMLane CP | 120 | 7 | 7,675 | 37.998 | 40.699 |
| 作者IMLane Ray | 120 | 7 | 7,667 | 38.310 | 46.285 |
| SemLoom＋Daft／Ray | 0 | 不可用 | 不可用 | — | — |

六臂120行的原source-position／解析状态／过滤bool矩阵逐项相同，TP7／FN7／FP0／TN106／invalid0；真值14、官方相对误差0.5。
完整请求正文的多重集合相同（120次、119种值），重复调用仍保留；原始生成文本并不相同，不能从相同过滤矩阵外推一般质量等价。
每臂输入38,325 tokens，六臂合计229,950输入／46,042输出；实际美元价格不可用，未把原成本卡默认零当免费。
八份原始metadata GET的正文与完整摘要均不同，仅三类已核动态值变化，版本化稳定摘要一致，初始完整计划锚保持。

query包含固定计划／metadata核对、原PZ执行与质量评价；application再含fresh、runtime／gateway准备、关闭与记账，范围重叠，不能相加。
服务准备52.128秒、共享准备11.612秒另计；HTTP实际峰值依次20／20／20／4／4／4，直接SDK与gateway上游观察范围分别保留，不表示GPU序列占用。
作者两臂各核4个实际worker，Ray另核4actor；Ray Data实际actor ID及逐池关闭计数未保存，标unavailable，不能用声明值补齐。
本轮作者Ray另保留coordinator的5条取消告警，GCS涉及8个唯一取消actor（含前5个），均与完成调用的4actor不同，逐ID有无引用销毁记录；精确C++引用释放点不可用。
收口阶段另4条INFO级KillActor RPC失败涉及完成actor，日志原样保留；不称告警消除，最终进程／设备退出独立核对。
单臂一次查询／120请求，顶部1%约1.2个观测，不提供稳定P99或性能排名。

SemLoom事件流在Daft payload的`runner_configure`处失败：64个已准备Generator取消、56个尚未准备，模型POST为0，未产生原COUNT／PlanStats或模型usage。
失败query 2.142秒／application 15.744秒仅为失败阶段时间。Core Job／额度归零、物理transport及调用方Ray关闭；父owner另核服务退出0、自有残余与GPU计算进程为空、GPU回1MiB、正常lease释放，未启用stale recovery。
197份原件（185份与12份作者补件）、六份完整PlanStats及精确清单均留仓外；[原值与SHA](fixed-model.json)保留全部失败及补件身份。
独立3815项原件一致性检查通过，只覆盖六完成臂与失败责任关闭；原全七臂验证器的失败保留，不授SemLoom、全轮或性能资格。
只读调用链显示，前置Daft完整调用直接取得默认runner，SemLoom payload随后经共享helper配置runner；重复初始化是待核候选原因，原内部vendor异常未保留。
后续固定bebf的两个独立进程使用真实Daft0.7.21作生命周期诊断：先取默认runner再配置出现重复设置异常，先配置后重复配置复用同一对象；85项独立检查通过，未创建HTTP客户端／服务或调用模型，GPU为0。
该诊断不补回R3内部原错，`hypothesis_supported`仍为null，不能认定原R3的具体因果。

<a id="daft-semloom-cpu-composition"></a>
### 后续CPU：同进程Daft到SemLoom

修补源码`62018855791dbee5c7a2c933361b21e58451496e`在Daft臂执行前使用现有进程级配置helper，保留配置冲突拒绝；19项测试／28项子检查及双轴独审通过。
源码与完整固定身份区别于bebf，不借用其模型资格。实际运行`pz-daft-semloom-synthetic-cpu-r3`在同一Python进程先Daft后SemLoom，调用生产`execute_fixed_movie`及原PZ／SDK，使用Daft0.7.21、Ray2.56.1和4CPU／0GPU。
三行软件fixture保留两个重复业务ID及一行普通谓词排除；一次选计划、各臂fresh。两臂各2次localhost合成HTTP、COUNT1、TP1／TN1、invalid0，usage各22输入／6输出tokens；请求多重集合相同、共享native runner为同一对象，合成usage不作模型费用。
Core Job／session／存储／physical transport、Daft consumer／HTTP池、Ray与fixture线程均正常关闭，独立owner核自有进程退出及正常lease释放，未启用stale recovery。
此前CPUr1因版本检查先导入环境PZ，在固定source核对处0POST失败；CPUr2完成Daft2POST后，checker误用`TP`字段失败，均保留failed。此次通过轮4POST、本CPU组合阶段累计6POST，模型／GPU均为0。
91份原件、104,497字节归档及独立原件审计的身份见[原值](fixed-model.json)。该CPU阶段模型仍为720／840、剩余120，原模型R3保持failed；CPU结果不授模型、完整七臂或性能资格，C0bdf的关闭错误修补也不由本次正常退出代替。

<a id="fixed-plan-model-r4"></a>
### R4：source620的独立SemLoom模型补轮

新运行`pz-q3-semloom-resume-r4`固定62018855，仅执行SemLoom；原2000行／120语义出现、Qwen2.5-7B／vLLM0.25.1／单4090及关闭prefix cache的配置保留，无模型预热、重试或选择阶段模型调用。driver与Ray为4CPU／0GPU，模型服务使用该GPU。
完整物理计划指纹仍为`99935b70…`，但两处固定入口源码变化、fixed manifest新增的adapter源码绑定（Daft helper字节未改）及新metadata初始完整锚使full identity由`dfa59e39…`变为`aba2bee1…`；稳定metadata摘要相同，不称同完整身份七臂。

| 路径 | 实际POST200 | COUNT | 输入／输出tokens | query秒 | application秒 |
|---|---:|---:|---:|---:|---:|
| SemLoom＋Daft payload／Ray HTTP worker | 120 | 7 | 38,325／7,717 | 15.110 | 29.739 |

原COUNT关系为7、真值14、官方相对误差0.5；120行TP7／FN7／FP0／TN106／invalid0，source-position／解析／过滤矩阵与R3六臂相同。
请求值及已记录wire hash的多重集合均为120次／119种，重复次数保留并与六臂相同；raw输出逐source分别有10／8／9／8／9／7处不同，不推定质量等价。实际美元费用不可用，原成本卡零估值不作免费。
query与application沿用上节重叠口径；服务准备49.091秒、共享准备11.510秒另列。额外一次零模型选择的准备10.262秒、优化0.015122秒已包含于共享准备，不能重复相加。

原日志记录两个实际Ray actor就绪，120个完成事件没有逐call actor_id，不能分摊为每actor60次。
Ray日志保留14条WARNING及2条INFO级KillActor RPC失败，含2条quick-exit与6条plasma EOF；其中部分EOF在工作前出现，时间与重复分别核对，不作请求失败数或无告警声明。实际actor关闭原因为`ray.kill`，不借用旧IMLane的无引用解释。
pending_calls／registered_jobs计数、Core usage及object bytes归零、physical transport closed可核；远端HTTP池关闭状态为null／unavailable，私有Job／session／storage终态未记录，不补0。
外层owner另核query／service退出0、自有残余为空、parent已退出、GPU回1MiB及正常lease释放，未启用stale recovery；该物理见证与调用方close返回分开。

88份常规原件含Ray日志与外部parent退出见证，935,173字节归档及精确摘要见[原值](fixed-model.json)。离线reader首轮因比较完整quality对象而出现形状错误，失败审计保留；修正版经独立窄审后，1682项原件检查全通过，支持该单次SemLoom模型补轮；未因此重跑模型或新增POST。
R3 failed与六完成臂720原账不改；本轮新增120后累计840／840、余0。CPU通过轮4／阶段累计6fixture另计；不授同完整身份七臂、一般质量等价、稳定尾分位或性能排名，也不借C0bdf关闭错误分支资格。

### baseline 合并的来源边界

[软件资格原值](software-qualification.json)的25份源码摘要仍绑定 `adde01a6`，没有改写为后续集成源码。
合并时保留 Animals 原生请求显式容量与普通 PG／wire 默认1MiB，再验证完整调用模式下的工厂合同，
才重绑 PZ 的两份工厂源码 guard；其余三份受控依赖及拒绝未知源码的行为保持。
同时修复 native-adapted 响应关闭异常跳过发送收尾的问题：关闭和观察分别尝试，原请求／取消错误优先，
次错保留；纯观察失败仍终止，未知发送结果仍禁止复用。
本机定向 HTTPX／Core／有限 localhost 回归验证这些整合行为；实际 PZ／后端软件资格、17次 fixture
和原生120次模型调用仍归各自冻结来源，不升级为合并源码的真实库或逐臂模型资格。

上次接纳固定 `2c175f4d` 的 metadata 身份补丁和部分模型结果时，保留上述首错／关闭修补及来源守卫，
当时尚未包含 Daft 初始化修补。该次融合复用固定原 PZ／SDK 的受影响 CPU／有限 localhost 回归
为118通过、1项显式 nonredirect 条件跳过，另95个通过子测试；与先前检查重叠，不累加。
该次离线原件重算核对六完成臂720POST和当时剩余120额度；没有新模型／GPU调用，不授第七臂或整轮资格。

本次接纳 `62018855` 与 `2210068e`，此前C关闭／首错修补及完整调用来源守卫保持不变。
融合后的受影响原PZ／SDK CPU／有限localhost回归为123通过、1项显式nonredirect条件跳过，另103个通过子测试；
与前述检查重叠，不累加。C离线重算R4的1682项检查通过，未连接网络或新增模型／GPU运行。
该复核保留source620单次补轮及旧R3的各自身份，不把原模型资格移至合并源码，也不替代关闭错误分支资格。

## 当前边界

`freeze_best_plan` 只覆盖无 validator 的最终计划；完整系统采样和 validation 不得被省略。
只替换 completion 接点时，PZ 继续拥有 DAG。源数据、配置、请求摘要和服务／缓存状态须与计划指纹共同核对。
原生完整2000行Movie、固定bebf的六完成臂与source620独立SemLoom补轮分列；R3 failed不改，累计模型840／840。R4仅支持该单次补轮，不合称同完整身份的完整七臂比较。
本记录不支持逐臂排名、质量等价、性能收益或可靠尾分位。

附件审查针对 PZ `807ed301`／1.5.3，本切片继续保留 SemBench 所需的旧 tag。
固定后端接入现按 A（选定计划后、原线程池前）和 B（最终 SDK 请求准备后）两处边界实现；
现有仅 completion 的桥保留为原生／诊断证据，不授予完整后端替换资格。
首版适配仅接受已核单模型 LLMFilter、单字段 ONE_TO_ONE LLMConvertBonded 及普通扫描／谓词／投影／结构 count；
LIMIT、join、distinct、语义聚合或未核多调用形状明确 unsupported。计划中的六臂另加 native-adapted 控制，
默认 work=1 只表示请求计数；软件与实际模型资格按本页分列，不外推未核算子或性能。

<a id="query-timing-owner-regression"></a>
## 查询计时、公开入口与异常关闭回归（2026-10-16）

本次以 `integration/baselines@7e59e5a1` 为基点，按新审查附件局部修补，未改原 PZ
查询、优化器、Generator、SDK parser 或 evaluator。原 `query_seconds`／fresh 合计仍含评价；
新增结果就绪与外层完整查询时间，评价独立记录。现有公开脚本贯通七种固定计划 arm，
主计时从每查询 fresh、执行器及 `SemLoomProgress` 创建之前开始；一次选计划与外部服务启动分列。
配置、边界和 owner 责任以[模块说明](../../../../code/src/semantic_methods/palimpzest/README.md#fixed-public-runner)为准。

`progress.finish()` 异常后继续尝试安全栈清理、SDK 关闭和最终报告，保留首因。
独审额外复现 reader 已结束但 native sender future 仍使用 SDK 的情形；现在发送结束前保持 SDK 打开。
外层读取 IMLane 已有外部清理标记；未确认工作或资源关闭失败保留 owner 与 lease，
不误记完成或自动恢复。`_send/after_send` 的既有实现保持。

[紧凑回归记录](lifecycle-regression.json)绑定改动源码摘要和分批日志：实验／入口 56 个、
语义层 74 个，去重共 **130 个用例通过**。使用既有独立 PZ／SDK 环境与显式 Darwin CPU profile；
PZ 能力组通过，通用 core 组缺 Ray／Daft／psycopg，未混装依赖。最终公开入口实际运行
original-native、native-adapted 两个两行合成查询，共 4 次 localhost POST，均 COUNT2；
请求多重集合、原 SDK、实际预算及正常 owner／临时 lease 关闭断言通过。另一批同一 fixture 的
4 次 POST 分列在实验回归日志中，不是新模型工作。

测试首轮的 unittest 辅助方法命名／mock 报告错误、目标测试导入顺序及 sandbox localhost 拒绝均保留，
修正后只复验受影响项；无模型／GPU 请求，未取得共享服务器 lease。
Standards 与 Spec 两轴独审提出的首因、sender、IMLane 责任和失败摘要问题均修复并复核通过。
测试源码随本次 Git 提交保留，摘要见上述 JSON；脱敏日志以运行标识
`pz-query-lifecycle-cpu-20261016` 留仓外，可按批次运行标识和校验值向项目维护者索取
`<批次run_id>.redacted.txt`。临时 unittest 响应已由测试清理，不声称可恢复原响应。

七臂配置只补软件接线检查，本轮未重跑真实 Daft／Ray／IMLane 或 SemLoom 物理矩阵，
不授模型、性能或 GPU 资格；上述历史模型、失败与费用／计时原值保持原来源。
