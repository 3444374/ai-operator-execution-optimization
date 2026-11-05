# LOTUS 方法与执行接点

这里调用已安装 LOTUS 的提示构造、解析、有限方法与结果合同；现有资格环境为 LOTUS 1.2.4。
SemLoom 执行适配直接依赖本层，
不从 baseline 获得生产实现；公共 Core 不选择 Movie、字段或标签默认。
`FilterMethod(model_name, instruction)` 的 instruction 由调用方显式绑定。

## 当前入口和所有权

- [engine.py](engine.py)维护单 Filter 的原生 formatter／parser 与 LM 记账。
- [record_types.py](record_types.py)维护方法／阶段及记录身份的序列化合同；
  [record_method.py](record_method.py)调用单阶段 LOTUS formatter／parser；
  [record_chain.py](record_chain.py)维护有限链推进、失败快照及逐记录交接。
- [record_relation.py](record_relation.py)维护 Extract 的完整关系赋值与最终 DataFrame 组装。
  旧 `record_chain` 类型与函数导入直接指向同一实现，JSON 标签和字段保持。
- [record_results.py](record_results.py)拥有调用／父关联、coverage、失败和 unknown 合同；不拥有模型或调度。
- `execution_provider/adapters/lotus_records_core.py` 与有限链适配属于 SemLoom，负责供给、推进和背压。
- `baselines/text/frameworks/` 中原生 Ray／Daft 图负责自己的 worker、批次与调度；不注入 Core credit。
- 原生 LOTUS Movie 对照在[lotus_native_local.py](../../baselines/text/frameworks/lotus_native_local.py)：
  原算子、LM、缓存逻辑和 LiteLLM batch_completion 保留，只通过公共 provider 把模型调用接到现有本地 IPC。

共享的 `observe_filter_rows` 在[engine.py](engine.py)，CLIP batch workers 在
[图像公共层](../../modalities/image/clip_batch_workers.py)，模型进程和清理在 `serving/backends/`。
这些共享组件不拥有 baseline 图或 SemLoom 策略。

旧 `LotusBatchExecutor`／`LotusNativeBatchExecutor` 完整 HTTP 前端及 PG／HTTP 实验入口已退役，
源码、接线和历史测试从[精确归档](../../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)恢复。
`sdk.py` 的仍用方法／响应工具与有研究价值的独立模块保留；不把历史 HTTP 接点说成当前 records 模型连接。
Q2b 独立视觉对照尚未用本次 Movie 文本 provider 替换；当前代码和历史证据分别解释。

查询结果以实际 `result_ready` 为边界；原评价、API 返回、owner 关闭及模型复用成本另记。
新原生 LOTUS 非 HTTP 路径只有 CPU 资格，完整官方 Q123 和真实模型尚待其依赖与实际环境检查。

## 来源记录与运行合同

[source.py](source.py)记录实际安装的 LOTUS、LiteLLM、OpenAI 版本及可读取源码的摘要。
旧 `validate_*_source` 导入继续返回来源报告，但不再将固定版本或源码字节相等作为执行门槛；
缺失的元数据或源码明确列为不可用。`lotus_commit`／图像的 `commit` 为未知值，
`reference_lotus_commit` 只标识既有资格所用的参考实现，不能冒充当前安装包的 Git 身份。
现有历史结果及其固定版本要求保持原身份；本次变化不证明其他发行版已经兼容。

普通 formatter、方法绑定和 SDK 请求准备不读取这些来源摘要。实际调用继续使用 LOTUS 原实现；
输入列、DEFAULT 序列化、适用的缓存／重试政策、生成选项、请求／结果身份、用量和资源边界继续校验。
Filter cascade 仍使用原 accessor 和阈值学习器的私有全局绑定，保留后端准备原始记录的接点。
这处代码对象耦合仍在：直接换成公开 DataFrame 调用会把完整提示展开移到 driver，
不能算等价简化。同进程并发 cascade 的随机数与 LM 记账隔离尚未验证。

<a id="finite-chain-result-index"></a>
## 有限链共享结果关联索引

公共 `FusedChainBatch._infer` 使用七字段完整身份索引，
供 Core fused、Ray Data、Daft 的正常有限链路径共同调用，不是 SemLoom 策略独占优化。
普通 SemBench Movie 单 Filter records／session 和 Core `record_local_sections` 不经过此 worker，
不能把该实现当作这些入口的优化或成绩。

标准 list／dict 及 str／int 身份结果只建一次临时索引；重复、缺失、外来及乱序结果保留原处理，
声明顺序、raw 顺序、父调用、deepcopy 隔离、known usage 和 unknown 不变。
原有非标准身份／容器比较路径保留，不添加协议放行或静默补成功。
这是通常哈希条件下的关联 CPU 优化，不保证所有输入严格线性；临时索引增加 O(n) Python 容器。
模型调用、组批、屏障、审计写入、取消及结果租约未改变；[审计引用释放](#completed-chain-audit-lifetime)仍是独立且默认关闭的选项。

[固定及随机差分](../../../tests/execution_provider/test_chain_result_index.py)包含100组固定种子案例、
真实哈希碰撞、执行中取消与部分结果，关联操作计数与原循环作 RED／GREEN 对照。
完整链、结果合同及迁移回归为 CPU 证据；没有新模型、RSS 或完整查询性能验证。

<a id="filter-execution-boundary"></a>
## 原生 CLIP 图像检索

`image_source.py`记录实际LOTUS来源；`image_identity.py`绑定模型参数、processor内容与版本。
`image_rm.py`把完整图像批交给原执行owner，保留RGB／源字节内容、行ID、shape／finite及独立矩阵所有权；
`image_workflow.py`保留原生FAISS建库、重载、完整检索结果和settings恢复。
`image_backend.py`提供固定单worker的Direct、SemLoom static／静态HSE和Ray fused／staged；
[Daft原生UDF](../../baselines/image/frameworks/daft_clip.py)保留自己的模型批次与生命周期。

[公开命令](../../../scripts/README.md#原生-lotus-clip-查询)包括文件manifest、模型资格producer及完整查询。
`--observe-model-reuse`显式保存真实构造身份与整数调用计数，默认关闭；已导出的观测可回收，返回矩阵独立拥有。
可选resource配置只开放static／Ray／Daft单卡、CPU1和外置TF32关闭环境，模型启动前拒绝HSEauto及多卡。
HSE仅带必要身份／取消／unknown基础，新dispatcher／路由未纳入。
[本批来源与检查](../../../../experiments/results/diagnostics/main_lotus_clip_20261015/README.md)明确区分当前CPU接入、
固定d2历史证据与main570新增四路径八臂40行严格单卡实机分列；默认数值失败、HSE环境缺项及性能边界保留。

<a id="completed-chain-audit-lifetime"></a>
## 有限链 worker 的可选审计引用释放

在 `SemLoomRecordChainCore(..., release_completed_audit=True)` 和
`ray_data_lotus_chain.evaluate(..., release_completed_audit=True)`／
`daft_lotus_chain.evaluate(..., release_completed_audit=True)` 提供相同 Python 选项，默认 `False`。
三条路径共用一个 `FusedChainBatch`；选项会传入每个完整关系阶段，不改变原生组批、调度、模型调用或重试。
这是有限链 API 接线，普通 SemBench Movie 的单 Filter records／session CLI 不经过该 worker，
没有给那些入口增加无效参数。Core 的 `record_local_sections` 不持有该 worker 的审计副本，显式拒绝开启此项。

成功 batch 的完整 envelope 编码后，仅当无方法／审计清理错误、已尝试的 engine close 确认为
`closed` 且无内层错误，同时尚未观察到取消或 deadline 过期，才撤掉 worker 自己的 `_audit_calls` 引用。
未执行 close 的中间成功 batch 可释放该副本，模型仍按原生命周期运行。编码失败、unknown／脏 close、
取消或过期均保留；已编码结果、磁盘恢复审计、消费者账本、身份、usage 和 Core 结果租约保持原责任。
通过重新绑定空列表释放自身引用，不清空其他消费者持有的列表。

CPU 接线覆盖 Core 完整链及 Extract 阶段、Ray／Daft 构图到共享 worker 的参数传递和异常保留。
原生图测试使用CPU构图替身，未证明真实Ray／Daft模型图或性能；同步IPC组件的Linux结果及当前整合边界见[状态说明](../../../INFRA_STATUS.md#shared-chain-ipc-scope)。
仅减少 Python 引用持有，不证明 RSS、操作系统物理内存或 GPU KV 下降。

<a id="record-local-sections"></a>
## 同模型有限计划的可选逐记录执行

同一份 `LotusRecordChain` 可以显式交给 Core 的 `record_local_sections` 模式。
已有普通 Filter／Map／Extract 可依赖合法地排序和重复，不按链名、Q编号或预测结果选择策略，
也不自由重排算子。F/M共享原方法对象、formatter/parser及计量；其真实结果只归并一次。
Extract仍用完整关系：真实EOF、所有前段结果与调用责任检查后执行原parser及组装，再续后段。
重复用户index、动态列和父调用按原关系方法处理，不能把Extract直接套入逐行F/M程序。

默认fused、原LOTUS方法、baseline原生路径不变。第一版一个查询使用一个模型部署，
同部署角色变化可形成新段；另一模型部署会在启动前报错，不做猜测路由。
这是有限顺序计划的执行选项，不表示Join、任意DAG、跨查询模型复用或所有组合已实机验证。
接口与资源说明见[执行owner](../../execution_provider/adapters/README.md#optional-single-model-finite-record-plans)，
资格范围见[代码状态](../../../INFRA_STATUS.md#record-local-sections-c827)。

同一逐记录异步后端可显式选择逐记录完成推进，或按固定输入组逐阶段等待后提交后继；
原`StageSpec`、`ChainRun`／`MethodRun`、格式器和解析保持。组内过滤终结、尾组和取消不产生不存在的后继。
这只改变提交放行时刻，不表示native请求已经按token组批；跨查询模型借用尚未实现。
接口、范围及与原fused完整路径的区别见[同异步后端推进策略](../../execution_provider/adapters/README.md#optional-completion-progression-on-the-same-async-backend)。
