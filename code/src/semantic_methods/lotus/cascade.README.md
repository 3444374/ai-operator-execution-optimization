# 原 LOTUS 全局 helper 级联

本切片按[数据执行计划](../../../../experiments/plans/数据组织.md#lotus-filter-data-execution)和
[系统架构](../../../../experiments/plans/系统架构.md)推进，本轮代码基点为
`integration/data-execution@90fe3399`，初始 `b447fc37` 的开发在交付前接续现有公开 actor-owner hook。
普通 Filter、有限链及历史模型资格分别保留。

[cascade.py](cascade.py)以独立 globals 绑定固定 LOTUS 1.2.4 的
`SemFilterDataframe.__call__.__wrapped__` 和 `learn_filter_cascade_thresholds` 原代码对象，
只替换记录展开与模型评价接点。已安装文件、模块全局函数和 LM 方法保持原样。
`validate_cascade_source()` 校验原 formatter、parser、LM、cache、settings、types、
`sem_filter.py` 和 `cascade_utils.py` 摘要；通用接口 profile 为 `lotus-helper-cascade-records-v2`。

算法来源为 [LOTUS 固定提交的 Filter](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_filter.py)、
[校准／采样／阈值](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/cascade_utils.py)
和 [LM 评分／记账](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py)。
这里复用原算法。架构 §8.7–8.8 的语义与执行职责分离决定沿用；本轮未访问或复制公司材料。

## 方法与全局阶段

`run_cascade(..., instruction=..., query_id=..., max_rows=..., max_calls=...)` 要求显式指令。
输入仅含 `row_id`、`source_position` 和 main／helper 指令引用列的并集；这些 payload 列必须是文本。
两个角色可引用不同列，main 可引用多列。driver 分别解析并冻结列序，dispatch 再核对原 accessor
实际展开的顺序；worker 使用 `CascadeStage.input_columns`，不按自己的 hash seed 重新选序。
通用方法返回逐 occurrence 决策及调用证据，报告 schema 为 `semloom.lotus_helper_cascade.v2`。

全部 helper 评价完成后，原 LM 从各生成位置的 top logprobs 中寻找首个同时含精确 `True`／`False`
的集合，并归一得到正例概率。完整日志始终缺少这对候选时，保留原库按生成 token 有无 `True`
得到 1／0 的行为。缺日志、非法生成 token IDs 或非有限分数会报错。

原函数对全部分数执行一次分位数校准，指定固定阈值时也执行。学习模式再由原 `importance_sampling`
有放回抽样，main 评价全部样本，原函数按 precision／recall 目标、失败概率及修正权重学习阈值。
严格处于两个阈值之间的位置送 main 复核；等于阈值由 helper 接受，阈值相等时正例优先。
样本答案不替代最终复核；总评价次数是 `N + S + L`，原 large-model resolved 统计仅计 `L`。
这些是真实全局屏障，物理 batch 不重新校准或学习阈值。

`CascadeStage` 交给后端的内容为阶段、角色、模型名、原格式化后的 instruction、parser default、
system prompt、输出标签、有效生成参数、冻结输入列，以及带完整独立身份的原文本 records。
后端返回相同身份的 terminal engine rows，物理顺序可以变化。重复文本与重复样本保留独立 call ID。
`preflight(main_lm, helper_lm, effective, *, method, helper_method, max_possible_calls)` 接收两个绑定方法；
输入源只按 main method 校验任务，`source.scan_spec(method)` 须覆盖两角色列并集。
`check_lifecycle()` 检查取消与期限，最终报告构造后仍再次检查。

支持 helper-LM、无 examples／CoT、DEFAULT serialization 和关闭结果缓存。分配前检查两个 tokenizer
的单 token 标签、生成参数和有限调用上界 `2N + S`；helper 使用原有效 `logprobs=True`、默认
`top_logprobs=10`，显式 top 值保留。main 的样本和复核使用 `logprobs=False`。
学习模式要求至少一个样本和 `cascade_IS_weight<1`，避免不可严格 JSON 序列化的无限修正权重。
`EngineSpec.max_model_calls` 约束单次引擎寿命；查询总上界另由 `max_calls` 约束。

两个兼容修正单列：重置 pandas 位置索引但保留外部 occurrence，避免原 `.loc[sample_indices]`
对稀疏／重复索引标签的错配；空关系返回空决策、零模型调用。原库空集 percentile 会抛错。

## 原生执行与本地引擎

| 后端 | 阶段执行 | 调度与容量归属 |
|---|---|---|
| [Ray Data](../../baselines/text/frameworks/ray_data_lotus_cascade.py) | `RecordScan` CSV／等值谓词／投影、重复 draw 展开、CPU formatter actor、独立模型 actor | Ray Data 2.56.1 拥有 batch、actor 和 backpressure，关闭自动重试／重启 |
| [Daft Native](../../baselines/text/frameworks/daft_lotus_cascade.py) | 原生扫描、batch expansion／explode、独立 preparation 与 inference class process | Daft 0.7.21 拥有批次和进程调度；显式逻辑输入字节限额不冒充 RSS 上界 |
| [SemLoom Core](../../execution_provider/adapters/lotus_cascade_core.py) | 独立 stage CSV／provider occurrence alias、已有 Daft raw scan／Ray actors、本地引擎前恢复 canonical cascade ID | 复用生产 `SessionEngine`、`BoundedAsyncBackend`、credits 和原 transport；准备／模型 batch 当前为 1 |

Core 的现有普通 Filter carrier 会重建普通身份。新增适配器把每次抽样映射为独立 provider occurrence，
模型调用前恢复级联身份，回执同时保留 `provider_call_id`。它额外检查原输入值、写入有界 stage CSV，
随后原生扫描该 CSV；全部物化、别名、重复扫描和初始化成本计入该配置，不声称优化收益。
stage 全关系持有量也不属于 Core 每项 credit／逻辑字节或 RSS 限制。

三个后端分别拥有物理执行，不共享线程池、队列或调度器。每阶段使用自己的有限本地 `EngineProcess`；
它只负责 JSON IPC 和私有模型进程，不拥有组批策略。当前部署使用不同显式模型路径，逐阶段串行加载
helper、main sample、main fallback；main 可能加载两次。扫描、加载、等待、IPC、关闭均属于查询成本。
Ray／Core 当前要求单个本地 Ray 节点，相关路径在 driver／worker 可见；GPU 配置为单卡 TP1。

[清理实现](../../baselines/text/frameworks/lotus_cascade_process.py)按已登记 PID／create time 监督私有进程。
stop 写失败、坏报告或一个 PID 观察失败不能跳过其他登记进程；晚到回执与缓存 OS 退出证明合并，
新增登记子进程会使旧证明失效。失败时仍保存已验证 usage，已交付的 unknown 不被晚到回执改写。
构造失败、关闭异常与取消分别保留错误；清理未知会阻止下一阶段。Core 在私有模型 OS 退出确认后
可终止失败的 IPC wrapper actor，查询与 API／GPU 状态仍保持 unknown。
引擎 API 关闭、已观察 OS 进程退出和实际 GPU 退出分别报告，仍需外层监督处理原生调用卡住及未观察后代。

[Movie Q3 适配](../../experiments/lotus_cascade_movie.py)保留原谓词、COUNT、classification audit 和
SemBench evaluator；`positive_review_cnt` 与 provider 使用标记只在任务适配层发布。
source 首次预检／读 occurrence 在计时前，后端扫描、准备、初始化、必要屏障、关闭、解析和 COUNT 计入查询，
evaluator 另计。公共 CLI／结果入口及共享 status／证据台账由唯一集成 owner 串行更新。

## 条款、代码与验证

| 计划条款 | 代码 | 验证 |
|---|---|---|
| 原全局控制、采样、目标与边界 | `run_cascade` 原代码对象绑定 | [原库差分](../../../tests/semantic_methods/test_lotus_cascade.py)：独立原 SDK 响应，固定 seed／阈值、校准、重复样本、fallback、parser、usage limit |
| 非 Q3／多列／独立 helper 指令 | 冻结 `input_columns` 与显式 instruction | 同上：逐消息／kwargs、随机抽样、stats、decisions、usage 与原 accessor 比较；worker 冻结反序检查 |
| 原生 Ray 阶段 | `RayCascadeBackend` | [Ray CPU 检查](../../../tests/baselines/text/test_ray_data_lotus_cascade.py)：实际 CSV／数值谓词／三阶段 worker，多列 main、独立 helper，与原控制器比较 |
| 原生 Daft 阶段 | `DaftCascadeBackend` | [Daft 检查](../../../tests/baselines/text/test_daft_lotus_cascade.py)：实际原生图／私有 CPU 进程，非 Q3 和 Q3、重复输入、失败 usage、空集、关闭阻断 |
| 生产 Core 与独立调用身份 | `CoreCascadeBackend`／stage alias | [Core 合同](../../../tests/execution_provider/test_lotus_cascade_core.py)使用真实 SessionEngine、模拟 actor API；[实际组合](../../../tests/execution_provider/test_lotus_cascade_core_libraries.py)使用真实 Ray／Daft／LOTUS 和私有 CPU 进程 |
| 部分失败／取消／已知成本／资源回收 | `_StageCalls`、各后端 close、`StageEngineCleanup` | 回执命名空间／关联校验，stop-I/O、坏报告、多 PID 隔离、晚到回执、构造／关闭错误、unknown 和 stage gate 定向检查 |
| Q3 COUNT 与原 evaluator | `run_movie_cascade_query` | [查询检查](../../../tests/experiments/test_lotus_cascade_movie.py)，普通 records 的原查询回归另保留 |

本机 Python 3.12.6、LOTUS 1.2.4、Ray 2.56.1、Daft 0.7.21、Arrow 24.0.0，显式 Darwin CPU profile
的 `core,text,semantic-benchmarks` 只读能力检查通过；没有安装或替换依赖。原始输出、初轮失败及修后
复验保留在仓库外 `evidence-backends`；运行时强制本地 LiteLLM cost map，固定响应不产生模型费用。

交付检查：`method-q3-records-regression` 收集 132 项，130 通过、2 项原生 opt-in 另测；
`ray-delivery-cpu` 12 项通过，22 个已观察所属进程退出；Daft 7 个实际库场景及修后 8 项合同通过。
Core 修后 17 项合同通过，在当前公开 actor-owner hook 上的实际组合覆盖两个查询、六阶段、44 次
synthetic evaluations，30 个已观察所属进程退出；相应旧 Core／actor-owner 回归也通过。
两个独立代理分别审查方法等价性和资源／错误清理，已报问题修复后复核。

**源码：** 通用方法及 Ray／Daft／Core 阶段实现，独立方法与资源审查；不包含 Async／Extract 重做。
**真实库：** 原 LOTUS、SemBench evaluator 和原生后端 CPU 固定响应检查，推理／性能结论不由此取得。
**真实模型：pending。** b447 的本地引擎源码已支持生成 token IDs、逐 token／top candidates、usage 和
finish reason；仍需实际模型核对 tokenizer、一次模板、日志内容、两个显式角色及阶段间 GPU 退出。
本轮没有启动服务器模型或 GPU，没有使用 HTTP gateway，也没有把失败降为普通 Filter。

IMLane 尚缺级联身份和角色／阶段 wire、全局 driver 接线、helper logprobs 校验，以及固定角色 owner
的串行新 scope 和关闭资格。其现有 v2 carrier／stage 字段可以复用，最小方法接点仍为上述
`preflight`、`evaluate(CascadeStage)`、`check_lifecycle`；普通 Filter 或作者 CPU 检查不转授级联资格。
