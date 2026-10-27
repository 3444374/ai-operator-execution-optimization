# LOTUS 完整请求执行接点

本模块保留 LOTUS 1.2.4 的原始 `LM`、算子提示、缓存判断、SDK 响应解析和后处理，
只在完整 uncached batch 的执行接点选择原 SDK、Daft、Ray Data 或 SemLoom。
`sdk.validate_source()` 核对实际安装源码和版本；不把可选 PG 兼容 profile 当成原 LOTUS 查询。

main已接纳请求／检索接点及PR52的records、有限链和本地模型接点；PR56另归位共享结果模块。实现、45d各入口小实机资格与尚未完成项见[当前状态](../../../INFRA_STATUS.md#pr52-g1-qualified-scope)，各来源的资格不自动互授。

已接纳records本地闭包的`FilterMethod(model_name, instruction)`要求调用方显式提供非空instruction；
序列化配置同样拒绝缺失或空值，不选择Movie／字段／标签默认。调用方先按自己的列绑定方法，再交执行owner。
Movie原提示由`baselines/text/movie_tasks.py`显式提供，公共Core不导入benchmark配置；CPU接线与各轮45d小实机资格分别解释，不转授未测任务或新源码组合。

## 接口与所有权

共享的[records结果合同](record_results.py)承载发生项、调用身份、父关联、coverage与失败/unknown审计，由Core、Ray Data和Daft消费者共用，不拥有模型或调度。旧`src.baselines.text.frameworks.lotus_chain_results`保留同一对象的薄重导出。新pickle使用新模块路径，旧pickle全局名仍可读取；worker须绑定同一候选源码，不能混用缺少新模块的旧worker。迁移与CPU边界见[代码状态](../../../INFRA_STATUS.md#record-results-migration)。

- `batch.lotus_executor(lm, executor)` 在作用域内替换 uncached 执行接点，退出时恢复；
  `LotusBatchExecutor` 将完整请求提交给已有 native task API，并将完整响应交回原 SDK。
- `native.LotusNativeBatchExecutor` 消费 Daft／Ray Data 自有图的完整响应。
  vendor graph 拥有并行与缓冲；适配层不向这些 baseline 注入 SemLoom 的任务选择或 credit。
- `limits.LotusCallLimits` 声明逐请求、整批字节与 deadline。native graph 在发送前准备并核对整批；
  Movie runner 显式设置 `prevalidate_batch=True`，也在发送前预检整批。
  直接使用 `LotusBatchExecutor` 的默认分窗口路径时，无效后缀被拒绝前，合法前缀可能已经执行。
  单个请求不截断或补发；SemLoom 默认逐请求输入仍为 1 MiB。图文 profile 强制整批预检。
- `maps.staged_two_map` 保留原阶段间整批等待；`driver.iter_two_map_rows` 是显式增量两阶段库接口，
  保留逐行关联和方法状态。这两条路径分别测试，下面的 Movie runner 不增加两阶段查询形状。

HTTP 状态、重复响应头、压缩 body、错误和 usage 进入原 SDK，适配器不把 HTTP 错误改成合法空结果。
取消与本地 iterator 关闭不证明远端模型停止；未确认的工作保留原 ownership，执行 owner 负责 drain。
完整响应字节预算不等于进程 RSS 上限。接口和完整响应基础见[执行层说明](../../execution_provider/adapters/README.md)。

<a id="filter-execution-boundary"></a>
## 当前 Filter 边界与目标入口

当前接点接收完整 uncached messages；`PreparedLotusCall`／`NativeCall` 承载完整 HTTP 请求，
native 路径先构造整批再交给后端，SemLoom 仍在调用方准备请求。固定 LOTUS 的
[`SemFilterDataframe`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_filter.py)
在 LM 接点之前调用 `df2multimodal_info()`，图片展开仍在 driver；当前不能称为后端完整准备流水。
固定 [SemBench Movie Q1–Q3](https://github.com/SemBench/SemBench/blob/c814e3807e72d4cf876b852b17e77f3cc94575c2/src/scenario/movie/runner/lotus_runner/lotus_runner.py)
没有传 `cascade_args`，现有资格不表示已开启级联或全部 LOTUS 优化。

`lotus_executor()` 已按 LM 实例挂载，helper／main 可分别复用；SDK 已支持 logprobs，LOTUS 已有作用域设置。
现有限制在观测与预算对整批逐行一次的假设，见[Filter 接线与验证计划](../../../../experiments/plans/数据组织.md#lotus-filter-data-execution)。
记录到准备、调用与结果的文本接点已随PR52进入main；同模型有限计划的可选执行见下节。[后续设计](../../../../experiments/plans/系统架构.md#lotus-data-execution)中的媒体准备和跨模型接合仍须分别实现和验证。旧请求路径保留既有接入范围作参照和选择性复用。
首闭环直接与原生 LOTUS 核对，不以旧 adapter 为唯一正确性定义，也不要求经过本模块的完整调用执行器。

`sdk.validate_source()` 的默认 `text` profile 当前不核 `sem_filter.py`；`IMAGE_TEXT_PROFILE` 才追加该文件校验。
后续文本级联也需补相应来源资格，records 扩展须保留原提交、补丁与独立校验身份；不能覆盖旧摘要后仍称原源码未改。
当前 `NativeCallExecutor` 下层也先收齐 calls，`LotusResponseReplay` 持有活动解析与客户端状态；旧内核／图段仅可选提取，新链的输入、执行和状态所有权按[接入计划](../../../../experiments/plans/数据组织.md#lotus-filter-data-execution)独立验证。
`prepare_call()`／完整 HTTP 接点准备和传输模型服务请求，未生成 GPU 输入或持有生成式模型引擎。
[第二层目标](../../../../experiments/plans/系统架构.md#model-engine-execution)将常驻 LLM／VLM 纳入 worker；已有 `ClipTensorActor.embed()` 真正完成 tensor 上卡、图像特征计算与 CPU 结果返回，
但属下节 CLIP 检索路径，不能认证 Animals VLM Filter。records文本／本地引擎接点已接纳；[路线映射](../../../../experiments/plans/数据组织.md#model-engine-roadmap)中的图文及其他扩展不因此取得实现或模型资格。

## 原版 Movie 查询

[公开入口](../../../scripts/experiments/run_sembench_lotus_queries.py)使用
[四臂配置](../../../../deploy/autodl/sembench_lotus_queries.example.json)：

| 配置臂 | 执行 owner |
|---|---|
| `lotus-adapted-native` | LOTUS／LiteLLM 原 request pool |
| `lotus-method-native-daft` | Daft Native batch graph |
| `lotus-method-native-ray-data` | Ray Data 官方 HTTP processor graph |
| `lotus-method-semloom` | SemLoom Core／Ray workers |

输入绑定 SemBench `c814e380` 的选定 Movie 关系、manifest、原 CSV 和独立 occurrence ID；
Q1／Q2 的原 `LIMIT 5`、Q3 的 `COUNT` 和原 evaluator 保持。
标签仅供预检与查询结束后的评价，不进入模型输入。重试与结果缓存关闭。
每个臂的持久 owner 可处理多个独立查询；每条查询重新绑定输入、预算和 trace，
只有查询 trace 成功写入并确认归档后才回收网关对应观测。LM／Core 复用不复用模型答案。

默认 `qualification` 模式核对准备请求值与实际 HTTP；显式 `performance` 模式保留实际 HTTP 和
原 SDK 调用，准备前不可得的调用值标为 unavailable。两者的实际观测开销都计入查询，不作估计扣除。
调用开始至消费者 EOF 与后续清理分别报告；owner 启停另列。
示例使用有限 query／owner deadline。仅原 Movie 可显式同时置为 null，且调用方必须提供有限的
故障、取消与清理监督；该配置本身不会安装 supervisor，逐请求 deadline 仍有限。

依赖检查、环境变量和三种 CLI 动作见[脚本入口](../../../scripts/README.md#lotus-原版-movie-四臂)。
本批来源、检查和未验证范围统一记录在[main 文本接入资格](../../../../experiments/results/diagnostics/main_lotus_text_20261015/README.md)。
[固定main原查询实测](../../../../experiments/results/diagnostics/main_queries_20261015/README.md)记录原Movie与Animals的完整模型请求、质量不足和单次计时；
可选服务资源配置的后续源码资格见[同题报告](../../../../experiments/results/diagnostics/main_service_auto_20261015/README.md)。
图像 RM／index／search 的本批工程接点见下节；PZ／IMLane继续按各自来源解释，软件 fixture不代表真实模型质量或性能收益。

## 原版 Animals 图文查询

[Animals Q1 入口](../../../scripts/experiments/run_sembench_animals.py)保留 SemBench 的 ImageArray、sem_filter、COUNT 和原 evaluator；
`lotus-image-text-v1` 核对原 PNG data URI、消息结构及固定 LOTUS 图文源码。四臂名称为
`lotus-native`、`daft`、`ray-data`、`semloom`，执行 owner 与上表一致。非法图文后缀在创建 Core flow 或原生执行图前失败。

原图不缩放、拆分或转为 CLIP 输入；模型 processor 的内部处理另行固定。SemLoom 使用显式 native
请求／总输入与 Ray window／object 字节额度，原 PG／wire 默认不变。CLI、正常 lease 和外层故障监督要求见
[脚本说明](../../../scripts/README.md#animals-原版图文-q1)；源码闭包与实际 CPU 来源见
[Animals 接入资格](../../../../experiments/results/diagnostics/main_lotus_animals_20261015/README.md)。


## 原生 CLIP 图像检索

`image_source.py`核对原LOTUS1.2.4；`image_identity.py`绑定模型参数、processor内容与版本。
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

本地候选在 `SemLoomRecordChainCore(..., release_completed_audit=True)` 和
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
原生图测试使用 CPU 构图替身，未证明目标系统的模型、跨进程或性能资格；
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
