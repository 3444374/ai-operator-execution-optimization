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
  内部 worker、执行适配、对照图和脚本已按所属模块导入这些合同／方法，旧路径留作公共兼容入口。
- [record_results.py](record_results.py)拥有调用／父关联、coverage、失败和 unknown 合同；不拥有模型或调度。
- `execution_provider/adapters/lotus_records_core.py` 与有限链适配属于 SemLoom，负责供给、推进和背压。
- `baselines/text/frameworks/` 中原生 Ray／Daft 图负责自己的 worker、批次与调度；不注入 Core credit。
- 原生 LOTUS Movie 对照在[lotus_native_local.py](../../baselines/text/frameworks/lotus_native_local.py)：
  原算子、LM、缓存逻辑和 LiteLLM batch_completion 保留，只通过公共 provider 把模型调用接到现有本地 IPC。

共享的 `observe_filter_rows` 在[engine.py](engine.py)，CLIP batch workers 在
[图像公共层](../../modalities/image/clip_batch_workers.py)，模型进程和清理在 `serving/backends/`。
这些共享组件不拥有 baseline 图或 SemLoom 策略。

`RecordOccurrence.from_dict` 由同一构造器校验并复制 values、source_refs、index_updates，不再先对完整字典重复深拷贝。
返回的 describe 与调用方可变输入仍相互独立，检查见[记录持有与组合回归](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-structure-and-bridge-capacity)。

`ChainRun`、内部记录段及待采用结果的已确认类型快照直接用 `dataclasses.replace` 调用同一构造器，
不再为复制先生成完整 describe 字典。三个可变字段仍独立复制，外部字典和历史恢复继续用 from_dict。
[同选择隔离检查](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#feedback-duties-and-record-copies)不提供RSS或查询性能结论。

旧 `LotusBatchExecutor`／`LotusNativeBatchExecutor` 完整 HTTP 前端及 PG／HTTP 实验入口已退役，
源码、接线和历史测试从[精确归档](../../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)恢复。
[sdk.py](sdk.py)只为历史复算记录已安装 SDK 的来源，保留 text 与 image-message 两种 profile。
孤立的 HTTP 请求准备／响应重放与固定两 Map 原型已退出当前目录，
准确源码和原投影读取位置见[恢复说明](../../../../experiments/reproduction/native_baselines/README.md#retired-lotus-http-methods)。
Q2b 独立视觉对照尚未用本次 Movie 文本 provider 替换；当前代码和历史证据分别解释。

查询结果以实际 `result_ready` 为边界；原评价、API 返回、owner 关闭及模型复用成本另记。
新原生 LOTUS 非 HTTP 路径只有 CPU 资格，完整官方 Q123 和真实模型尚待其依赖与实际环境检查。

## 来源记录与运行合同

[source.py](source.py)记录实际安装的 LOTUS、LiteLLM、OpenAI 版本及可读取源码的摘要。
旧 `validate_*_source` 导入继续返回来源报告，但不再将固定版本或源码字节相等作为执行门槛；
缺失的元数据或源码明确列为不可用。`lotus_commit`／图像的 `commit` 为未知值，
`reference_lotus_commit` 只标识既有资格所用的参考实现，不能冒充当前安装包的 Git 身份。
现有历史结果及其固定版本要求保持原身份；本次变化不证明其他发行版已经兼容。

普通 formatter 和方法绑定不读取这些来源摘要。实际调用继续使用 LOTUS 原实现，
输入列、DEFAULT 序列化、适用的缓存／重试政策、生成选项、请求／结果身份、用量和资源交接继续检查。
Filter cascade 仍使用原 accessor 和阈值学习器的私有全局绑定，保留后端准备原始记录的接点。
这处代码对象耦合仍在：直接换成公开 DataFrame 调用会把完整提示展开移到 driver，
不能算等价简化。同进程并发 cascade 的随机数与 LM 记账隔离尚未验证。

[cascade 差分检查](../../../tests/semantic_methods/test_lotus_cascade.py)用确定性 CPU 响应核对原生
accessor 与 records 接点的完整提示、固定／学习阈值、重复抽样记录、fallback、决策与 LM 用量。
helper 元数据无效时仍保留已知用量并让查询失败。阶段解释复用首次验证的响应，
只为 helper 附加 logprobs，避免重复构造普通响应；未改原生校准、抽样和阈值算法。
这些检查不执行模型，也不提供真实 helper 质量、服务器性能或其他发行版的资格。

## 排查与回归记录

下列记录保留可复核的触发条件、原因和处理，供后续兼容性与性能排查使用；依据为改动前源码及本批 CPU 检查。

| 问题与触发条件 | 原因与处理 | 必要检查及其范围 |
|---|---|---|
| 旧接点在安装版本或源码字节与固定列表不等时，于方法绑定／准备前拒绝执行，即使差异不影响所用语义。 | 实验来源相等被用作普通运行的接纳条件。现由 `source.py` 记录实际来源，执行方法继续检查真实输入、请求和结果。旧 HTTP SDK 执行实现随后退役。 | [来源记录检查](../../../tests/semantic_methods/test_lotus_source.py)核对版本元数据、源码及缺失元数据只进入报告，formatter 与有限链不读取来源摘要；未安装验证其他版本。 |
| 旧 cascade 的每个成功响应在阶段解释中转换为原生响应两次。 | `dispatch` 为记账先转换，随后 `cascade_response` 再转换一次。现在复用首次验证的响应，仅为 helper 附加 token 元数据。 | [原生差分](../../../tests/semantic_methods/test_lotus_cascade.py)核对固定／学习阈值、重复抽样、fallback、提示、决策和用量，并覆盖 helper 元数据失败后的记账。没有查询耗时或性能收益实测。 |

本地消费端首次检查另出现进程查看权限错误，启用该权限后的同范围重跑通过；这是环境限制，
原失败与重跑分列保留，不将其写成算法或清理协议缺陷。状态入口见[实现状态](../../../INFRA_STATUS.md)。

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
三条路径共用[无调度 worker](../../serving/backends/lotus_chain_worker.py)的原生计算和审计生命周期；
Core 的 prepared batching／复用由其执行子类另行实现。
选项会传入每个完整关系阶段，不改变原生组批、调度、模型调用或重试。
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


`records.py` 的 `RecordsResources` 默认保留 R=None，由实际消费者解析已有有限额度；
它不探测硬件，也不把 None 当无限。普通异步支持 Core／IPC／服务声明，其他有限入口缺来源时在启动前拒绝。
不可变资源值在构造时检查，`describe()` 只复制其字段；[当前范围](../../../INFRA_STATUS.md)区分原生 baseline、SDK 和诊断桥。
