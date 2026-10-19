# LOTUS 方法与执行接点

## Movie Q3 records 最小切片

本分支新增独立的[普通 Filter records 接口](records.py)、[worker 准备／原结果解释](engine.py)，
以及[查询入口](../../../scripts/README.md#movie-q3-records-本地模型接点)。
Ray Data 原生图、[Daft Native 记录图](../../baselines/text/frameworks/daft_lotus_records.README.md)与 Daft＋Ray Core 的 SemLoom 静态参照共享原 LOTUS 方法，后者尚未接生产 Core。
Daft 拥有 process UDF 与私有模型子进程，完整结果 JSON 透传身份、未知／失败及已知usage；冻结f526的两查询／4次真实vLLM调用另有独立资格，新通用源码仍按CPU范围解释。
新链消费共同离线投影、建立发生项身份并核验过的无标签 `records.csv`，worker 准备 messages，模型 worker 负责一次模板；不经过下方旧完整请求执行器或 HTTP replay。
固定 LOTUS／SemBench 的真实 Python 3.12 差分和部分 Ray／Daft CPU 接线见[资格记录](../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md)。
原生参照、Ray Data与静态参照的Qwen／单4090小切片已分别验证；static两查询同engine、隔离call IDs，原提示／token／usage／parser／COUNT与原评价一致。首次Native runner及Daft关闭失败保留，修正来源单列。普通Ray／Daft／静态／Core共用中性scan的CPU复用已验证；[冻结普通Core](../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#core-model-qualification-20261019)另完成同引擎两query／四次真实调用。后续源码、有限链／级联模型、图像与持续供给仍分别待验，旧request资格保持独立。

`LotusFilterMethod.prepare_rows()` 是 worker 本地方法接口；`FilterMethod` 为通用普通文本配置，
默认参数兼容 Q3。Movie source／runner 独立严格绑定 Q3 instruction、default、True/False tokens、
system prompt、原 predicate 与 CSV schema；方法层继续拒绝 JSON 模式及 helper logprobs／repetition override。
[共享引擎](../../serving/backends/README.md)另支持逐调用完整 `GenerationSpec` 与 `json_object`，供后续 Extract 选择；
默认参数不变，每次调用独立构造采样参数。这项接口只有 CPU 合同资格，不继承上方固定源码的模型结果。
模型 terminal rows 必须保留全部 call 身份、状态及真实 usage，不能以最终 COUNT 重建工作量。
共同端收齐整个 Filter 的 terminal rows 后才原解析／COUNT；成功调用的异常格式按原 parser 处理，failed／unknown 或非法终态不转为语义判定。
`run_movie_query()` 的 lazy `execute` 工厂在 source／版本／LM policy 检查后才调用；失败的 `query_report` 保留逐调用账本。
输入上限和清理／unknown 的实际保证从上述资格记录核对。

本地引擎结果另保留规范消息 SHA256 与实际 prompt／output token IDs，供一次模板及 usage 核验。
可选 `EngineSpec.max_model_calls` 限制单个引擎 lifetime 的应用请求数，按行在 chat 前预留，失败／unknown 不退；默认 `None` 保持原行为。
它不是跨实例总预算；资格 runner 仍须先持久预留每个 unit 的上限，并证明一次性 owner／无重启／无重复消费。

## 普通文本方法与 source 扫描合同

[`FilterMethod`](records.py) 是唯一普通文本 Filter 配置；非空 instruction 可引用不同文本列。
[`record_chain.TextFilterMethod`](record_chain.py) 直接 alias 同一类型，旧 serialized
`method_type="TextFilterMethod"` 仍可读取，重新输出统一为 `"FilterMethod"`，不维护第二套方法协议。
普通标签仍限 True/False；原 LOTUS `parse_cols()` 检查实际引用且拒绝无列 instruction，准备时逐列验文本。
单 Filter 与记录链 Filter 共用原 `df2multimodal_info()`／`filter_formatter()` 准备逻辑，
raw records 保留调用方的完整身份，默认 Q3 call IDs 字节不变。Map 生成的文本列可交同一 Filter 方法消费；
模型、原 parser／usage 与每阶段 generation 仍分别拥有身份。

固定 LOTUS 1.2.4 的 `parse_cols()` 返回 `list(set(...))`，不同进程的列序可能不同。
caller 必须先调用 `engine.bind_filter_method(method)`，将原解析顺序冻结到同一
`FilterMethod.resolved_input_columns` 字段；后端在分配资源前绑定并序列化，worker 只核引用集合
和输入文本列，按收到的顺序调用原 formatter。已绑定方法再次绑定保持原顺序，不按 worker
本地 hash 顺序重排，也不依赖统一 `PYTHONHASHSEED`。同一方法的跨后端比对应共享 caller 绑定后的值。

`MapMethod` 使用同一可选列序字段，`StageSpec.bind()` 按 caller 原解析顺序绑定 Filter／Map；
Extract 保留自身显式 `input_columns` 顺序。普通 `LotusRecordChain(stages, query_id=...)` 构造
会在 caller 自动绑定，再由 `describe()` 生成 wire 值；worker 使用 `LotusRecordChain.from_dict()`
严格接收，缺失绑定或引用集合不符即失败，不在接收端补绑定。
直接构造 worker 的 `LotusFilterMethod`／`LotusRecordMethod` 同样要求已绑定值。
`make_lm()` 只创建 accounting/config 对象，允许尚未绑定的 caller 配置并检查原引用，不负责准备消息。

显式 Extract 关系屏障有[Core 同 worker 与原生分段布局](../../../../experiments/results/diagnostics/lotus_extract_relation_20261019/README.md)：
Ray／Daft 各段仍由 native graph 执行，完整 EOF／覆盖／成功后才关系广播；每个非空段独立引擎，
成本与每实例调用上限分列。默认非末端 guard 保留，不增加通用 DAG。
最终 `records_to_frame(records, columns=..., source_index=source_frame.index)` 可保留原 Index 的
dtype／name 和空切片类型；消费者需保存原 metadata，无参数时保持旧推断行为。

旧配置缺少 `resolved_input_columns` 时仍读为 `None`，caller 绑定默认 Q3 后固定为
`("reviewText",)`；旧 Q3 消息和 call IDs 保持。`dataclasses.asdict()` 的新输出会新增该字段
（未绑定为 `None`），属于增加可选字段的配置兼容，不承诺新配置 JSON 与历史归档的字节 SHA 相同。
旧冻结运行不修改；未绑定的旧 wire 需要经过 caller 绑定后才可交给新 worker。

source 保留 `csv_path`、`preflight(method)`、`expected(query_id)`，并通过
`scan_spec(method) -> RecordScan` 声明不可变的原生扫描值：

- `csv_schema`：有序 `(列名, "string" | "int64")`，必须包含 `row_id:string` 与 `source_position:int64`；
- `payload_columns`：唯一的文本投影列，不得覆盖任何 call identity 字段；
- `predicate`：可选 `(列名, 值)`，仅表示单个等值条件，值严格匹配列类型。

配置支持 `describe()`／`from_dict()` 的 JSON 往返，不承载通用 SQL 或执行图。
source 拥有 schema、任务语义、来源和发生项预检；后端消费扫描值并使用自身的 scan/filter/select。
`MovieRecordSource.scan_spec()` 每次先预检 Q3，再返回原五列 schema、`reviewText` 投影和 `id == taken_3`。
通用列、旧 Q3、序列化 alias 和 Map→Filter 的 CPU 合同不扩展既有真实模型资格；冻结模型运行仍绑定其原源码。

## 完整请求级接点

本模块保留 LOTUS 1.2.4 的原始 `LM`、算子提示、缓存判断、SDK 响应解析和后处理，
只在完整 uncached batch 的执行接点选择原 SDK、Daft、Ray Data 或 SemLoom。
`sdk.validate_source()` 核对实际安装源码和版本；不把可选 PG 兼容 profile 当成原 LOTUS 查询。

本页描述 main 已接纳的请求／检索接点；独立 records、有限链及本地模型分支的实现与资格统一见[实施来源](../../../../experiments/plans/数据组织.md#model-engine-roadmap)，不表示已纳入本模块。

## 接口与所有权

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
记录／媒体引用到准备、调用与结果的[新主链](../../../../experiments/plans/系统架构.md#lotus-data-execution)在独立分支推进，尚未纳入 main；旧请求路径冻结接入范围作参照和选择性复用。
首闭环直接与原生 LOTUS 核对，不以旧 adapter 为唯一正确性定义，也不要求经过本模块的完整调用执行器。

`sdk.validate_source()` 的默认 `text` profile 当前不核 `sem_filter.py`；`IMAGE_TEXT_PROFILE` 才追加该文件校验。
后续文本级联也需补相应来源资格，records 扩展须保留原提交、补丁与独立校验身份；不能覆盖旧摘要后仍称原源码未改。
当前 `NativeCallExecutor` 下层也先收齐 calls，`LotusResponseReplay` 持有活动解析与客户端状态；旧内核／图段仅可选提取，新链的输入、执行和状态所有权按[接入计划](../../../../experiments/plans/数据组织.md#lotus-filter-data-execution)独立验证。
`prepare_call()`／完整 HTTP 接点准备和传输模型服务请求，未生成 GPU 输入或持有生成式模型引擎。
[第二层目标](../../../../experiments/plans/系统架构.md#model-engine-execution)将常驻 LLM／VLM 纳入 worker；已有 `ClipTensorActor.embed()` 真正完成 tensor 上卡、图像特征计算与 CPU 结果返回，
但属下节 CLIP 检索路径，不能认证 Animals VLM Filter。[路线映射](../../../../experiments/plans/数据组织.md#model-engine-roadmap)中的普通 Q3 接点现有上方分支实现；真实模型小样资格沿上方报告的各次固定源码解释，Daft GPU 与其余未覆盖范围仍待验。

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
