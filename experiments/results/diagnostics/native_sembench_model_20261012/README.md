# 原SemBench Q2真实模型小轮

固定源码 `3bd5358706679b639e790585347133cd8d60b836`，已缓存Qwen2.5-7B-Instruct、vLLM0.25.1与两张RTX4090。原Movie CSV／2000行清单、WHERE产生的120个出现、原提示、SDK、Filter解析、LIMIT5和原评价器保持。此处是原SemBench外部关系和六路径执行观察；不据此声称已接入PostgreSQL真实语义算子。

实际完成质量6槽、单卡18槽和双卡11槽：共7680次真实模型调用／23040次三跳POST。双卡第12槽同步IMLane作者Ray的公开CLI启动确认发生JSONDecodeError，后7槽未运行；接续程序退出1，不标整轮通过。原质量完成后等待父技术决定超时的退出1也单独保留。不存在模型重跑或预算重置。

首次API为2026-10-12 19:34:52 UTC，原硬截止20:59:52 UTC。实际最终清理距离首次API2964.56秒，启动累计140.11／900秒；各分项仍按原768／4608／4608逻辑及三跳硬额记账，不转借剩余份额。此前软件验证另记，不加入本轮模型调用。

## 任务质量与原gold

原SemBench提交c814e380、CSV摘要ad5def7d、清单内容摘要e753baa5已核验，原CSV字段与标签逐行对应；Q2的120出现有14正例，原gold SQL实际返回14行，原查询及评价器源码摘要保留。

六个质量入口和全部58个已完成的预热／测量查询，其完整请求值、决策、返回集合及质量一致。每次原LIMIT5官方precision／recall／F1均1.0；完整120行分类真阳性7、假阳性0、真阴性106、假阴性7，另算召回0.5、F1为2/3。LIMIT5分数只适用于原返回集合，不代表全表过滤满分或模型可靠性充分。不改标签、答案或提示来抬高评价。

质量段185.48秒，独立服务建立46.16秒；每入口仅一次无预热的qualification查询。其时长原值见质量JSON，不能用这6个冷观察排性能名次。

## 单卡完整与双卡部分测量

每槽实际公开CLI为单臂Q2、warmups1／repeats1／max_attempts240；原N路径performance模式的预期完整值unavailable保留，实际HTTP值及120调用继续核对。共有29个测量查询和29个预热查询，各自120调用；单卡六路径测量n3，双卡J为n1，其余为n2。

表中“完整／ready”是每次查询的全部原值，秒。ready从原SQL/API提交到统一消费EOF，完整时间另含准备；全槽冷启动、就绪、两次查询和清理时间另存。观察成本均保留，不作估算扣除。

| 拓扑／路径 | 查询n | 请求n | 完整／ready原值（秒） | 请求P50／P95／P99／max（秒） | ready查询吞吐（调用/秒） |
|---|---:|---:|---|---|---:|
| 单卡 N LOTUS | 3 | 360 | 4.567/4.463；5.466/5.376；7.068/6.984 | 0.139/0.188/0.222/0.254 | 21.40 |
| 单卡 D Daft Native | 3 | 360 | 4.663/4.555；5.194/5.111；7.421/7.327 | 0.144/0.214/0.236/0.253 | 21.19 |
| 单卡 R Ray Data | 3 | 360 | 12.718/12.621；10.405/10.317；10.550/10.467 | 0.134/0.164/0.188/0.210 | 10.78 |
| 单卡 S SemLoom | 3 | 360 | 5.201/5.104；5.464/5.374；6.894/6.811 | 0.142/0.185/0.208/0.231 | 20.82 |
| 单卡 I 同步IMLane CPython | 3 | 360 | 8.250/8.165；8.469/8.368；8.902/8.806 | 0.123/0.135/0.138/0.141 | 14.21 |
| 单卡 J 同步IMLane作者Ray | 3 | 360 | 8.375/8.293；8.542/8.458；9.589/9.468 | 0.121/0.134/0.138/0.149 | 13.73 |
| 双卡部分 J 同步IMLane作者Ray | 1 | 120 | 9.202/9.096 | 0.122/0.138/0.143/0.145 | 13.19 |
| 双卡部分 I 同步IMLane CPython | 2 | 240 | 8.813/8.712；9.523/9.439 | 0.125/0.143/0.149/0.209 | 13.22 |
| 双卡部分 S SemLoom | 2 | 240 | 7.745/7.644；8.946/8.859 | 0.178/0.244/0.273/0.330 | 14.54 |
| 双卡部分 R Ray Data | 2 | 240 | 10.466/10.368；10.683/10.601 | 0.123/0.140/0.152/0.163 | 11.45 |
| 双卡部分 D Daft Native | 2 | 240 | 7.444/7.359；8.564/8.461 | 0.194/0.255/0.264/0.276 | 15.17 |
| 双卡部分 N LOTUS | 2 | 240 | 7.739/7.659；8.336/8.232 | 0.177/0.237/0.305/0.315 | 15.10 |

请求时段为owner网关进入到完整响应EOF，包括网关、路由、网络、排队与模型服务；不是纯GPU计算。分位数用线性插值。JSON同时保留查询／请求n、P50、P95、P99、max、全部重复值和错误数；查询只有1–3个样本，尤其查询P99仅描述性，不作显著性、稳定排名或最优配置结论。双卡缺项不能据单卡n3自动补齐。

固定顺序为单卡N D R S I J／J I S R D N／R S I J N D，双卡逐轮反向。3轮不是完整六轮位置均衡。双卡已完成第一轮及第二轮N D R S I；第二轮J与第三轮D N J I S R尚未运行。

## GPU工作、资源与清理

质量服务实际API历史0→720；单卡新服务0→4320；双卡两个独立新服务分别0→1320。每查询的vLLM请求计数增量与120调用对应，双卡固定全值摘要路由为每副本60；每个副本的实际API／engine PID、start ticks、CPU分配与GPU UUID均保存。双卡最后值是最后成功查询的metrics快照，异常清理前没有额外最终生命周期GET，未伪造最终快照。

720质量与6960预热／测量响应usage合计prompt955264／generation30720 token，与原服务计数对应，两个GPU确实处理模型调用。以下采样覆盖分项查询与槽位之间的空闲，采样间隔约0.25秒；均值不表示纯kernel时长或MFU。

| 分项／GPU | 样本数 | utilization均值／峰值% | 显存峰值MiB |
|---|---:|---:|---:|
| single/gpu0 | 2212 | 31.99/100 | 19518 |
| single/gpu1 | 2212 | 0.00/0 | 1 |
| dual/gpu0 | 1434 | 22.00/100 | 19464 |
| dual/gpu1 | 1434 | 21.31/100 | 19464 |

模型配置为BF16、context4096、max-num-seqs16、batched-token2048、显存比例0.8、eager、前缀缓存关闭，独立副本张量／流水线并行均1。生成保持原temperature0、top_p1、max_completion_tokens8、stop换行、非流式、cachefalse、retry0。

N/D/R/S沿C4／线程8，R actors2／async2／batch1，S workers2／batch2／held8／active4。I/J沿C6、声明两执行者、实际作者META为native_executors4／native_task_threads6，逐实例核两个实际原执行者PID。不同原生控制不称同形调度。driver／作者CPU72–81，GPU0 API82–89、GPU1 API90–97；未使用task-10 CPU16–23。

每查询的原生RSS采样、共享内存与作者清理、EOF／首行／iterator清理、外层进程身份均随原件保存；RSS求和不是唯一物理内存。无独立逐查询CPU utilization探针，不补造CPU利用率。此前只读主机背景CPU观测也不代表测量期间完全空闲。

接续累计登记1951个PID／出生身份，最终残留0、CPU分配违规0、GPU计算进程为空。异常后的本人API和待释放CLI由登记身份清理；作者Ray实例自身清理通过。失败处CLI PID297104的bootstrap已等待释放，但release文件、driver-bootstrap确认和execute完成记录均不存在；该槽0模型调用。原件把失败定位在父控制程序的CLI启动确认读取，非原模型答案解析；非原子marker写入造成瞬时空JSON读取是源码与现场支持的推断，控制器未保存该异常的逐帧traceback，不能把此推断写成已完整复现。

原质量等待超时后，父已确认gold适用性；接续只运行尚未启动的单／双段，同一SQLite和原首次时钟SHA837b0b49保持，携带既耗启动时间。旧超时和原退出1保留；同一逻辑路由端点重建后的进程身份、服务新建与间隔均记录，不能称整轮连续稳态。

## 原件与独立重核

- [质量核验JSON](quality-verification.json)、[质量原件公开包](https://github.com/3444374/ai-operator-execution-optimization/blob/08e02fd00336164f940546e2fbb73e3f4773de4c/experiments/results/diagnostics/native_sembench_model_20261012/quality-observations.tar.gz)、[成员摘要](https://github.com/3444374/ai-operator-execution-optimization/blob/08e02fd00336164f940546e2fbb73e3f4773de4c/experiments/results/diagnostics/native_sembench_model_20261012/quality-members-sha256.json)、[质量重核脚本](recompute_quality.py)。
- [单卡与双卡部分核验JSON](performance-verification.json)、[完整观测公开包](https://github.com/3444374/ai-operator-execution-optimization/blob/08e02fd00336164f940546e2fbb73e3f4773de4c/experiments/results/diagnostics/native_sembench_model_20261012/performance-observations.tar.gz)、[成员摘要](https://github.com/3444374/ai-operator-execution-optimization/blob/08e02fd00336164f940546e2fbb73e3f4773de4c/experiments/results/diagnostics/native_sembench_model_20261012/performance-members-sha256.json)、[性能重核脚本](recompute_performance.py)。
- 所有Benchmark消息内容改为原SHA／字节数；编码请求同样处理，完整JSON模型响应保留。二进制SDK wire保留私有原件和摘要，公开不提供不可完整隐私扫描的二进制内容。480个质量成员／960个解码载荷，2815个性能成员／9120个解码载荷已检查，隐私命中0；原完整私有包和补充router trace下载保持，不重跑生成证据。

首次性能导出的router trace因18.18MB超过单文件16MB导出限额而明确未导出，随后只读补取完整原件及摘要并并入公开包；原不足导出记录保持。公开源码1001个文件与固定3bd的Git blob逐字节核对，实际使用七个软件helper原摘要不变。

运行本目录两个recompute脚本只用stdlib，不连网络／模型／GPU。重核公开成员、三跳计数、原120任务、决策／质量、单卡全部n3及双卡实际n1/n2、usage与服务计数，返回记录必须包含7个双卡未运行槽和原退出失败。

本轮不包含Q1/Q3真实模型、async入口、同Coordinator并发查询或原始IMLane扩容。历史两项Q1 2000行适配器前置拒绝记录继续保留，不据此说作者原生系统只支持128行。
