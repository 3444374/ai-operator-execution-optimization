# HSE可选设备组选择检查

从正确性切片A `d307bb9265a32e516052f3bea7d8206f8906c288`独立实现。
默认`model_dispatch=None`保留源先行、CPU准备、model派发、最小空闲actor及`hse_static`输出；
不携入历史扫描B或ready优先C，不改image source、CLI、普通流水及原生baseline。
本项属于AI执行层的可选工程实现，不要求PG接入。

## 实现与复用

| 已有机制 | 本次采用方式 |
|---|---|
| [EndpointSnapshot与BatchRequest](../../../../code/src/scheduling/core/models.py)、[LeastWorkEndpointRouter](../../../../code/src/scheduling/endpoint_routing/policies.py) | 仅所有当前可准入候选都有有效同单位速率时，复用预计完成时间选择；图像使用pixels/s，token速率为空 |
| [SessionCapacity](../../../../code/src/scheduling/core/session_capacity.py)、[模型副本容量](../../../../code/src/execution_provider/adapters/model_replicas.py) | 沿用从live记录派生占用和声明容量的做法；不实例化另一份credit账本或HTTP副本传输 |
| [共享容量更新](../../../../code/src/scheduling/submission_control/shared_credit.py) | 下调只影响后续准入，已有调用仍计费；不撤销lease |
| [已有服务观察器](../../../../code/src/scheduling/runtime/observations.py) | 其累计token样本不能直接解释CLIP完成块；本次使用最近一次有效完成的work/service_s，不新增EWMA |
| 驻留切片`42186dbed22c9c70cf0d22285afe4698c97bc714`的`input_residency.estimate(descriptor,destination,now_s)` | 直接接受原回调签名。该实现返回None；没有verified prepared内容映射，选择侧不读tensor、散列payload或推断缓存命中 |

[StageModelDispatcher](../../../../code/src/scheduling/runtime/stage_dispatch.py)分别保存声明的actor/group额度、
可到期的容量反馈和完成速率。占用每次由HSE live `_ModelCall`派生；preparing/ready的预计work不是model-active占用。
actor/node/accelerator观察与声明group标签分别保存；[actor内helper](../../../../code/src/modalities/image/hse_dispatch.py)
使用[Ray 2.56.1公共runtime context源码](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/runtime_context.py)中的接口。
本轮单节点CPU Ray实采10个model角色的runtime actor ID，accelerator ID为空；
这些身份不证明物理GPU独立或可用HBM。

[broker.peek_model](../../../../code/src/scheduling/runtime/stage_broker.py)只读同一FIFO头。
先确定可容纳的actor，再取原model lease；暂时不足不requeue、不增加retry。
明确到期时刻可在无pending或另有慢pending时唤醒循环。
大head仅可在大actor执行时，仍等待该actor；小尾块不会越过head。

只有`ray.get`、broker完成和输出audit都成功才学习速率。
速率缺失、到期、单位或版本不符时，在全部当前eligible actor间按确认完成的采样尝试序列轮换；
零时长或非法速率不会饿死有有效样本的actor。每次运行清空速率，dispatcher拒绝跨pool复用。
`service_s`包含actor内host copy、H2D、forward、D2H及已有完成确认，不称纯CUDA吞吐。

## 检查及确定缺陷

机器事实、逐项用例、源码摘要和失败记录摘要见[checks.json](checks.json)。
本树受影响组合187通过／2项驻留切片缺失跳过；包含原A的29项、新策略34项及实际Core／旧路由／共享容量检查。
另在独立临时组合中加载固定`42186dbe`七个成员，2项驻留接线通过：
cache OFF/ON都执行4次完整forward和8行唯一输出；ON模拟转换为2次，OFF为4次。
这些次数来自Torch/Ray替身，不能解释为实际设备复制或性能提升。
设备fence失败时，HSE保留该actor的96字节／8pixels及payload引用，输入cache另保留96字节／1个unknown引用；
另一actor确认后只结清自己的阶段责任，缓存仍由输入owner持有。
第二独立审查以内存导入同一七个blob复核2/2通过；未修改或合并驻留分支。

新增代码审查期间修复两项确定缺陷：反馈清理被中断会跳过fence／unknown发布；
仅轮换missing-rate子集会让持续零service的actor饿死有效actor。
前者修前退出130、修后最小反例通过；后者的unit及HSE反例通过。
早期HSE反例的恒定fixture时钟触发`total_s=0`错误，已修测试时钟并保留原日志；不是生产计时修改。
最终两名独立审查者未发现本分支必须修复项。

## 接线与尚未验证的行为

调用方为每个已创建GPU actor提供`StageActorCapacity`，为其声明group提供`StageExecutionGroupCapacity`，
然后给原`run_semloom_ray_hse_pipeline`增加`model_dispatch=StageModelDispatcher(actors, groups)`。
声明requests/work是本run的逻辑额度，当前serial actor仍最多一项在途。
与驻留切片组合时可直接传`placement_estimator=input_residency.estimate`；借用池显式创建其`ReusableClipTensorActor`，
常规worker builder保持原类。现有回调只有unknown，位置成本排序仍pending；unsupported或异常回调只记诊断。

潜在开销包括每次可选选择的live占用汇总、路由视图构造和本地到期观察；净收益未经测量。
声明额度不代表全部RSS、pinned或device内存，服务速率也不代表GPU空闲计算能力。
原`081b3c17`研发树未包含literal重复handle`(g,g)`拒绝，曾可能将其计为两个index；
独立修补与本次整合见下文。额度仍针对注册slot/group，分别反序列化的Ray别名和物理GPU身份不在该拒绝范围。

新策略的单节点CPU实际Ray软件检查已完成；Torch／CUDA、异构物理设备及GPU性能仍未运行，
不借用旧47f或驻留单卡结果。软件检查持有共同host lease，使用4个Ray CPU名额、0GPU、
512MiB object store及CPU模型fixture；60秒仅作单操作／无进展诊断期限，健康运行不设总墙钟截止。
真实异构设备比较另需匹配的独立设备及强static／原生对照。

复现本地新策略：`PYTHONPATH=code python3 -m unittest tests.scheduling.test_stage_dispatch tests.modalities.image.test_hse_device_dispatch`。
驻留组合用例在[专属测试](../../../../code/tests/modalities/image/test_hse_residency_dispatch.py)，仅在对应切片存在时执行。

实际Ray软件用例见[CPU fixture测试](../../../../code/tests/modalities/image/test_hse_device_dispatch_ray.py)，
复用`ed209238`发布的五项默认路径检查，另检查完成反馈、共享group容量、两种到期唤醒、零时长和异常责任。
共11项，仅在已排期的共同lease下设置`SEMLOOM_RUN_HSE_RAY=1`执行；未设置时明确跳过。
测试actor使用CPU数组与启动／错误握手；实际执行11/11通过，Torch、真实模型、GPU和P99／P99.99均未测。

## 实际Ray软件检查

固定发布源码`4c7efa79074ae564db1dfb6831bbe3a73063d5b1`，29个生产成员与`081b3c17`相同；
34个包成员逐字及SHA核对。实际Ray2.56.1、Arrow16.1.0，driver为Python3.12.3、NumPy1.26.4、psutil7.0.0。
沿用已有独立driver，无安装或base环境修改。逐项结果、包摘要和原件摘要见[ray_checks.json](ray_checks.json)。

五项默认static检查与六项可选策略检查均通过，0 failure／error／skip／诊断错误。
共享group允许一项在途时，broker虽允许两项，实测model peak仍为1；有效完成后才进入least-work选择。
容量反馈到期在无model pending或另一model仍pending时均能推进，两个case的lease retry保持0。
零service按`0,1,0,1`轮换，只保留actor1有效样本；NaN结果成功返回driver，完成额度结清后被audit拒绝且不学习速率。
模型异常与一侧fence失败时只保留该owner的真实ObjectRef、48逻辑字节／4pixels；
另一owner经serial fence确认后归还自己的额度，unknown池再次调用被拒绝。
流水driver的get类型不含prepared payload；14项可选model调用收到只读CPU数组，不能据此计算物理复制量。

34个actor的PID／启动时间全部纳入49个owned进程记录。逐case清理及独立postcheck确认
owned进程、控制器、Ray head均退出，lease释放，无强制清理。
首轮`41639493`保留为10通过／1 fixture失败：NaN先于`dataclasses.replace`注入，actor构造校验提前拒绝。
仅修测试注入顺序后完整重跑11项；两轮原件保留，不把首轮记作通过。
本轮确认软件执行与归还行为；逻辑容量不提供RSS、pinned或device上限，也不产生模型性能或尾延迟结论。

<a id="integration-acceptance"></a>
## 与公共正确性修补的整合验收

以单取unknown waiting修复后的`f6daa6bc`为基点，普通合入独立literal guard `f7881106`，
再普通合入`fd1fc250`的可选实现与证据；B历史扫描、C ready优先及驻留实现均未合入。
`model_dispatch=None`保持默认source先行、FIFO和static选择，CLI、输入、worker／model默认实现保持。
literal重复Python handle在读source、绑定dispatcher及任何actor访问前拒绝，新增回归分别覆盖默认与显式派发。
不同Python handle的Ray别名或物理GPU归属仍不由此证明。

最终20模块Ran262，249通过／13明确跳过（11实际Ray窗口未启用、2缺独立驻留切片）。
因实测包与现行工厂源码不同，另核5个依赖模块Ran70，69通过／1实际工厂退出窗口未启用；
两组测试身份无重叠，共332项318通过／14跳过。前述2项literal专项与该组合重叠，不另相加。
覆盖A的unknown／引用／serial fence、共享group和到期推进、有效完成反馈、原Core／LeastWork／共享容量，
以及现有工厂启动失败、计时错误优先级和unknown waiting／queue控制。两轴独立终审无必修项。

交付`fd1fc250`的34成员与固定实测`4c7efa79`逐字核对一致；当前融合版29成员仍一致，
其余5份分别为既有工厂生命周期修补、literal guard及3份unknown queue正确性修补。
来源与逐文件差异见[整合机器记录](integration_checks.json)，原[checks.json](checks.json)和
[ray_checks.json](ray_checks.json)保持。两轮原始250成员包摘要均核对且保留，首次10通过／1失败不改写。
本次只做本地fixture与原件离线核验，没有重启服务器、模型或GPU；旧11项实际Ray资格仍归`4c7efa79`，
不自动变为该融合版资格。Torch／CUDA、物理多卡、驻留组合与性能结论继续待验证。
