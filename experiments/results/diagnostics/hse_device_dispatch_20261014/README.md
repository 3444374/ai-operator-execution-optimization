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
本轮没有实际Ray身份采集，逻辑accelerator ID也不证明物理卡独立或可用HBM。

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
继承A的literal重复handle`(g,g)`仍可能被计为两个index；其拒绝检查已交集成owner，未在本分支重复修改。
因此当前额度针对注册slot/group；实际不同actor身份的完整入口检查待组合该独立修补。

新策略的实际Ray／Torch／CUDA、异构物理设备及GPU性能均未运行，也不借用旧47f或驻留单卡结果。
最小实际Ray软件检查需求：共同host lease、4CPU、0GPU、512MiB object store，使用CPU模型替身；
60秒仅作单操作／无进展诊断期限，健康运行不设总墙钟截止，由主任务在baseline后排期。
真实异构设备比较另需匹配的独立设备及强static／原生对照。

复现本地新策略：`PYTHONPATH=code python3 -m unittest tests.scheduling.test_stage_dispatch tests.modalities.image.test_hse_device_dispatch`。
驻留组合用例在[专属测试](../../../../code/tests/modalities/image/test_hse_residency_dispatch.py)，仅在对应切片存在时执行。
