# Movie Q3 的原 LOTUS 全局 helper 级联

本切片按 `main@9b74f00d` 的[数据执行计划](../../../../experiments/plans/数据组织.md#lotus-filter-data-execution)推进，
代码从 `integration/data-execution@c0be45dd` 独立开发。普通 Q3 的既有真实模型结果不转授本实现。

[cascade.py](cascade.py)以独立 globals 绑定固定 LOTUS 1.2.4 的
`SemFilterDataframe.__call__.__wrapped__` 和 `learn_filter_cascade_thresholds` 原代码对象。
只替换记录展开与模型评价接点；已安装文件、模块全局函数和 LM 方法保持原样。
`validate_cascade_source()` 校验实际 formatter、parser、LM、cache、settings、types、
`sem_filter.py` 和 `cascade_utils.py` 的摘要，使用独立 profile `lotus-helper-cascade-records-v1`。

算法来源为 [LOTUS 固定提交的 Filter](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_filter.py)、
[校准／采样／阈值](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/cascade_utils.py)
和 [LM 评分／记账](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py)。
此处复用原算法，不主张新增级联方法。架构 §8.7–8.8 中算子语义与执行职责分离、复用模型调用设施的决定沿用；未重新访问公司材料，未复制其实现。

## 全局依赖与调用身份

全部 helper 评价完成后，原 LM 从每个生成位置的 top logprobs 中寻找首个同时含精确 `True`／`False` 的集合，
归一得到正例概率。完整日志中始终缺少这对候选时，保留原库按生成 token 有无 `True` 得到 1／0 的行为。
缺日志、非法 token IDs 或非有限分数会报错，不产生虚构分数。

原函数随后对全部分数进行一次分位数校准；即使指定固定阈值也执行这一步。
需要学习阈值时，原 `importance_sampling` 有放回抽取位置，原 main 评价全部样本，再由原阈值函数计算目标。
只有严格处于两个阈值之间的位置送 main 复核；等于阈值由 helper 接受，两个阈值相等时正例优先。
样本答案不替代最终复核；总评价次数为 `N + S + L`，原统计里的 large-model resolved 仅计 `L`。

`CascadeStage` 交给 `evaluate(stage)` 的内容只有：

- `name`：`helper_score`、`threshold_sample` 或 `fallback`；`model_role` 为 helper／main。
- 固定模型名、格式化后的 instruction、parser default、system prompt、输出标签及完整有效生成参数。
- `records`：原文本与 query／role／stage／row occurrence／source position／call ordinal／call ID。

后端选择自己的批次与资源，返回带相同完整身份的 terminal engine rows。
回传顺序可以变化；重复文本及重复抽样按独立 call ID 还原。全局控制只有一份，不能按物理批重新采样或学习。
后端可提供 `preflight(...)` 与 `check_lifecycle()`；查询在最终报告构造后再次检查取消和期限。
各阶段完整成功响应先交原 LM 记账，再走原 choice/parser；失败和 unknown 的已知 usage 另存逐调用记录。

首版仅支持普通 Movie Q3 的文本 `reviewText`、helper-LM、无 examples／CoT、DEFAULT serialization 和关闭结果缓存。
调用前同时检查主／helper 单 token 标签、生成参数白名单、正整数容量与有限调用上界 `2N + S`。
helper 的 `logprobs=True` 使用原 kwargs 合并及 `top_logprobs=10` 默认，显式 top 值保留。
主模型的样本和复核为 `logprobs=False`。学习模式要求至少一个样本，且 `cascade_IS_weight<1`，以保留可严格序列化的有限修正权重。
不支持的形态在模型分配前明确拒绝。

两个兼容修正单列：入口重置 pandas 位置索引但保留外部 occurrence，避免原 `.loc[sample_indices]` 对稀疏／重复标签的错配；
空关系直接 COUNT 0、零调用。原库空集 percentile 会抛错，这两项不宣称逐缺陷等价。

## Ray Data 与本地模型接点

[Ray 实现](../../baselines/text/frameworks/ray_data_lotus_cascade.py)每阶段原生扫描 CSV、过滤 `id=taken_3`、
投影、展开抽样发生项、worker 原 formatter、GPU callable 和紧凑结果消费。
Ray Data 2.56.1 拥有批次、actor 和调度；源码显式关闭扫描／准备／模型重试与 actor 重启。
没有共享物理线程池、统一调度队列或 SemLoom Core。Daft／IMLane／SemLoom 可分别实现相同阶段接口；本提交不提供它们的级联执行资格。

首版限单 Ray 节点、driver 与 worker 共享本地文件路径、单 GPU／TP1。
helper、main 校准、main 复核按阶段依次创建模型。main 可能加载两次，数据也会重扫，不能从耗时中扣除这些成本。
不同角色必须显式指定不同模型路径；原 LM 标签编码还须与部署 tokenizer 文件对应。

GPU callable 复用现有 `EngineProcess` 的有限 JSON IPC 和私有模型子进程寿命；该设施不拥有批次策略或模型调度。
driver 预先持有每阶段 control 目录，[清理实现](../../baselines/text/frameworks/lotus_cascade_process.py)
在结束 actor 前通知 stop，并按已登记 PID／create time 观察及回收进程。
构造中的 executor、stop 文件失败和受限进程枚举分别处理；未知清理会阻止下一角色。
报告区分引擎 API 关闭、已观察 OS 进程退出与尚未验证的 GPU 退出；仍需要外层进程监督覆盖卡住的原生调用和未观察后代。

[Movie 查询函数](../../experiments/lotus_cascade_movie.py)保留原谓词、COUNT 和 SemBench evaluator。
初始 source 预检／读 occurrence 在查询计时前；后端各次扫描、准备、初始化、IPC、必要等待、关闭、解析与 COUNT 计入查询。
evaluator 另计，评价报错也保留已经完成的模型调用及 usage。本提交未改 owner 管理的公共 CLI／结果入口。

## 计划条款、代码与测试

| 计划要求 | 实现 | 验证 |
|---|---|---|
| 原全局控制器、采样及目标 | `run_cascade` 的原代码对象绑定 | [方法差分](../../../tests/semantic_methods/test_lotus_cascade.py)：直接对原 accessor，独立构造原 SDK 固定响应 |
| 全局校准与物理批分离 | 原校准／采样／阈值函数 | 批大小 1／2／4／25 结果、阈值及调用账本相同 |
| helper 接受与 main 复核 | 原 accessor 路由及 merge | 固定／学习阈值、等值、空／非空复核、重复抽样与消息／kwargs 差分 |
| parser、来源及有效参数 | `validate_cascade_source`、`CascadePreparer`、原 LM/parser | 主／helper 指令、system prompt、default、显式 top、usage limit、token 元数据拒绝 |
| 每次评价独立身份和 usage | `_StageCalls`、独立 call ID | 重排、重复／缺失／变型身份、部分失败、known／unknown usage、严格 JSON |
| Ray 原生准备和重复样本 | 原生 CSV→flat_map→两类 callable | [Ray／清理检查](../../../tests/baselines/text/test_ray_data_lotus_cascade.py)：真实 Ray CPU 图、原 LOTUS worker 准备、固定响应，无模型 |
| 取消／错误与资源回收 | driver control、`StageEngineCleanup`、生命周期复核 | 真实私有 sleep session 回收，延迟 executor、stop 写失败、最终关闭后取消、unknown 保留 |
| Q3 结果与原评价 | `run_movie_cascade_query` | [查询检查](../../../tests/experiments/test_lotus_cascade_movie.py)：原 SemBench evaluator，评价错误保留完整账本 |

本机 Python 3.12.6、LOTUS 1.2.4、LiteLLM 1.95.0、OpenAI 2.50.0、NumPy 1.26.4、pandas 2.3.3、Arrow 24.0.0、Ray 2.56.1。
显式 Darwin CPU profile 的只读能力检查通过；默认 GPU profile 在本机无匹配的失败也保留。未安装或改变已有依赖。

本批原库方法 21 项、SemBench 查询 2 项、Ray／进程 7 项通过，均零模型请求。
两个独立审查分别复核方法等价性、资源及错误处理，发现项已修复。
重复 close 的报告发布使用私有临时文件与原子替换，幂等回归及独立复核通过。
完整输出留仓库外；运行标识包括 `cascade-cpu-final`、`movie-evaluator-cpu`、`ray-process-cpu-delivery-final`。
初次 unittest 入口错误、受限进程表导致的 supervisor unknown、首个 Ray fixture 得到空复核而预期三阶段的失败均保留；之后的复验各有独立标识。

**源码状态：** 原控制流及新 records／Ray 阶段实现完成，使用独立扩展身份。
**真实库状态：** 原 LOTUS／SemBench evaluator／Ray Data 的 CPU 固定响应验证通过，不能据此主张推理或性能收益。
**真实模型状态：pending。** 原职责基点的公共 `GenerationSpec` 拒绝 logprobs，不能执行真实 helper。
集成线现已提供逐token content数组及严格关闭确认；21项原方法、2项原评价及7项原生／进程CPU检查在组合源码重新通过，
25个已观察owned进程退出、零模型／GPU。来源和原件SHA见[集成记录](../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/verification.json)的 `global_cascade_integration_cpu_20261019`。
仍需核对实际生成 token／top candidates、tokenizer、一次模板、usage、finish reason、不同模型角色及阶段间 GPU 退出后，才取得模型资格。
GPU 按 owner 的 Daft 排期串行安排；本批没有启动服务器模型、HTTP gateway 或 GPU 请求，也没有将失败降为普通 Filter。
