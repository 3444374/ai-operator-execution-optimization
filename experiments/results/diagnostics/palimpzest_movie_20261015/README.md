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
本轮只证明2-call结果与退出，告警原因及4个lane满载健康仍未核，不称无告警或满载资格。

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
原2000行任务的固定计划 original-native／native-adapted／四后端模型、质量／费用／总时比较仍待执行。

## 当前边界

`freeze_best_plan` 只覆盖无 validator 的最终计划；完整系统采样和 validation 不得被省略。
只替换 completion 接点时，PZ 继续拥有 DAG。源数据、配置、请求摘要和服务／缓存状态须与计划指纹共同核对。
原生完整2000行Movie的本次模型质量已实测；待执行工作交接的最小CPU软件资格已按上节分列；PZ＋SemLoom／IMLane／Daft／Ray Data的原任务模型资格仍待执行。
本记录不支持六臂排名、质量等价、性能收益或可靠尾分位。

附件审查针对 PZ `807ed301`／1.5.3，本切片继续保留 SemBench 所需的旧 tag。
固定后端接入现按 A（选定计划后、原线程池前）和 B（最终 SDK 请求准备后）两处边界实现；
现有仅 completion 的桥保留为原生／诊断证据，不授予完整后端替换资格。
首版适配仅接受已核单模型 LLMFilter、单字段 ONE_TO_ONE LLMConvertBonded 及普通扫描／谓词／投影／结构 count；
LIMIT、join、distinct、语义聚合或未核多调用形状明确 unsupported。计划中的六臂另加 native-adapted 控制，
默认 work=1 只表示请求计数；软件与实际模型资格按本页分列，不外推未核算子或性能。
