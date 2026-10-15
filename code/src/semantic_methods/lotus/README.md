# LOTUS方法与SemLoom执行适配

## 原始 Animals 图文 Filter

固定7d83的`lotus-image-text-v1`完整消息profile现已窄接至当前SDK／`LotusBatchExecutor`／`LotusNativeBatchExecutor`；默认纯文本行为、当前可空batch deadline及remote session_factory/session_limits均保留。图像profile在任何flow／native graph前校验完整批次的模型、字节和期限，保留原生PNG data URI及完整SDK响应解释。[Animals入口](../../../../experiments/results/diagnostics/sembench_animals_20261015/README.md)执行原ImageArray→Filter→COUNT；不是CLIP RM。Core入口保留默认1MiB；原图大请求须显式选择native单项及总输入预算，不能隐式放大。`--arm semloom`复用同一`LotusBatchExecutor`完整请求／响应路径，实际资格由同题报告逐次记录。


## 原生 CLIP 图像检索

固定融合源码 `2afcaf47` 的[最终十格资格](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#final-integrated-qualification)
已经完成六路径64图组合与四格已有索引的完整数值／查询检查；单次资格不提供性能排名或稳定尾分位。

普通入口自动采用已验证的后端输入：Direct／Daft／Ray 接 RGB PIL，数组转换在查询内；
SemLoom static／HSE 复用已核对的原图字节。源记录缺失、图片变换或像素变化时，按当前 RGB PIL
无损 PNG 编码，固定压缩等级1；不会回读旧 JPEG 代替当前像素。所有图片先整批检查后再提交。
必要 fallback 保留，旧输入选择／压缩调参开关已移除；历次命令与实测来源保留在
[原记录](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md)。
HSE 的 encoded 容量与块上限仍可显式设置，超限沿用原错误，实际字节减少不会自动扩大容量。

历史PNG参数与候选命令按[固定来源记录](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#png-candidate)解释；当前入口不再接受旧调参开关。HSE逻辑容量只约束broker持有块，RM列表、复制及RSS／显存不由此形成硬上限。

固定 `bdae61bb` 的[基础修正](../../../../experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#foundation-restoration-20261015)
由真实 Slow/Fast processor 发布解码器、版本、配置摘要及输出规格；工厂 ready、HSE descriptor 与 worker 完成核验一致，错误身份在 source／解码前拒绝。
Slow 仍以 PIL RGB 解码，Fast 保留 torchvision 路径，像素处理数学不变；不是向量错误修正的结论。
真实工厂自动提供 transform-v2，encoded descriptor 的 work-v2 区分 decoder／执行变体／完整 transform。
未升级的合法自定义池仍可使用原 transform-v1 默认，不能据此认领实际身份核验；直接 work helper 未给新身份仍保留 work-v1。
旧签名及旧实测保留原来源，新旧校准不可混用。`OrderedMatrix` 预分配及 RM 左侧 scatter 保留校验、顺序和自有输出复制。
图像 RM 调用／向量与 executor 观测仅在对应 JSON／NPZ 成功写出后明确释放，未导出审计与后续追加记录保留，累计编号与行数不重置。

`image_workflow.py`保留 LOTUS1.2.4 的`sem_index`、`load_sem_index`、`sem_search`与原生
`FaissVS(Flat, inner product)`。图库固定原始 RangeIndex，业务ID另列；输入使用一维object列中的
RGB PIL对象。该版本的`ImageArray(...).tolist()`返回路径字符串，stock RM不会把list解码为图片，
因此路径示例不足以证明图像编码。在线PIL查询会编码一张图片，文本`bird`查询只编码文本。

图像实现LOTUS作者规定的抽象`RM._embed()`扩展点，保留原`RM.__call__()`与查询转换；
文本则在固定源码上替换实例内部`_process_uncached_messages`，不是官方稳定执行器插件。
项目自己的`image_rm.ImageBatchExecutor`提供`semantics`、`embed_batch(doc_ids: tuple[str,...], images: Sequence[bytes | Image.Image])`
与`close()`。输入表示在executor创建时声明，RM按稳定ID恢复顺序，检查完整float32 `[N,512]`向量、
finite值与单位范数。原生建索引需要完整矩阵，显式限制图库至5000行；这不是流式PG执行。
Direct复用既有慢CLIP预处理和tensor actor，SemLoom复用原driver/HSE，Ray Data保留原生图与调度；
`--arm daft_native`接入[独立Daft后端](../../baselines/image/frameworks/daft_clip.py)，
保留其原生图片类型、stateful batch UDF与生命周期，不复制执行代码。外部工厂也可使用同一协议。
原生算子与作者源码均不修改，固定来源、资格及精简证据见[本轮记录](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md)。

[资格与查询CLI](../../../scripts/experiments/run_lotus_image_retrieval.py)将建库、已有索引查询、
组合workflow、CLI准备和结果写出分别记录；`model_startup_s` 不证明全部模型构造都在应用区间外，详见[字段口径](../../../../experiments/plans/reference/对照规范.md#measurement-field-scopes)。源码中Ray Data每次RM调用新建CPU/GPU callable池图；其他内置路径
在workflow对象内持有模型／worker池。该生命周期声明不等于实测加载／销毁次数；早期`2afcaf47`资格未采集这些次数，后续`485c86a8`的实际计数见下方连续查询观测，不能直接用这些样本排名。非stock臂的可选文本RM位于CPU、stock文本在CUDA；数值通过不表示文本性能可比，耗时另列。
所选Ray图内模型重建是合法原生配置，全部成本计入完整查询，不因此排除主测，也不标成跨查询模型常驻。
默认只读低开销单调时钟；`SEMLOOM_DETAILED_TIMING_DIR`借用已集成的可选计时模块，缺模块时明确失败。
原生缓存关闭；串行入口拒绝同进程第二个owner，尚未资格跨查询并发。失败查询与RM调用保留时间、状态和阶段。

先用[模型身份CLI](../../../scripts/experiments/qualify_lotus_clip.py)核对固定ST与cached OpenAI CLIP的实际
参数内容、有效PIL像素张量、tokenizer与完整向量；序列化文件哈希不同不等于参数不同。
当前只公布float32，索引sidecar绑定有序像素、模型资格与原生文件哈希。小样本不输出P99或P99.99。
已有索引查询复用同一qualification identity文件与原始图库顺序；新生成的资格文件不自动替换旧sidecar。
本任务是CLIP检索，不是VLM `sem_filter/sem_map`；`sem_sim_join`的子集索引和缺邻居行为尚未纳入资格。

五条替换批次共用`sdk.LotusResponseReplay`，每批次惰性创建一个客户端并复用，
在已有消费线程执行固定SDK解释，完整响应重放不产生实际HTTP。关闭失败保留第一异常及其他失败的类型注记。
原LOTUS调用仍使用原执行入口；实际SDK/厂商验证进度见[修补记录](../../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md#execution-repairs)。

本目录实现LOTUS单模型文本Map的成对执行入口，以及外部输入的两Map逐行继续执行。
总体设计见[语义系统方案](../../../../experiments/plans/语义系统对照.md#native-semloom-pairs)，
公共执行与完整响应见[执行适配说明](../../execution_provider/adapters/README.md)。
路径名称为`LOTUS native`与`LOTUS method + SemLoom`；两Map属于LOTUS方法适配。
全局实验入口、状态和真实模型检查由整合任务接续，本目录没有接入PG多Map。

## 固定来源与采用决定

源码依据为LOTUS1.2.4提交`b1a85fd7a66fabed8a1585d44d7597d592b4433f`，
实际库验证使用LiteLLM1.95.0、OpenAI SDK2.50.0。`sdk.validate_source`核对包版本及
LM、Map、formatter、postprocessor、缓存、设置、列引用和计费模块的SHA-256，
以及LiteLLM普通OpenAI文本转换和参数工具的源码摘要。
新增适配只调用这些库；没有复制或修改LOTUS源码。该提交的
[LICENSE正文](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/LICENSE)
为Apache-2.0，而`pyproject.toml`的分类仍写MIT，按正文记录来源。

| 已核对的源码行为 | 采用与保留 |
|---|---|
| [LM.__call__](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py)先查LM缓存，再把全部miss交给`_process_uncached_messages` | 在该实例的方法入口选择执行器；原生默认仍调用LOTUS/LiteLLM。SemLoom分支获得全部miss，随后才进入自己的有限offer |
| LM原生分支拥有RPM/TPM等待、`batch_completion`线程池与批次提交 | SemLoom分支直接使用公共SessionEngine及Daft/Ray传输；响应回到消费线程后调用`completion`作本地SDK重放，未再发送HTTP或进入LOTUS请求池 |
| LM在批次返回后更新physical/virtual usage、LM缓存，并按原位置合并命中与miss，提取choice/logprobs | 这些源码继续执行；乱序完成按miss序号复位。完整HTTP响应由适配保存，原生ModelResponse继续交给LM |
| [DataFrame.sem_map](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py)拥有算子缓存、整表格式化、方法提示、postprocessor及结果列 | 单Map全部保留；批次接入不会自动增加跨算子推进。结果全列表返回前的等待仍存在 |
| `df2multimodal_info`、`map_formatter`和`map_postprocess`可以处理普通文本行 | 两Map复用这些函数，由MethodDriver保留行身份、状态与结果。第二条指令必须引用第一条输出列；没有方法算法重写 |

上述接点选择是工程决定。方法所需的全表校准、联合提示、模型角色选择和跨行依赖不能借此提前执行。
首版两Map仅处理独立文本行、默认parser和一个模型；算子缓存按完整DataFrame取键，
因此逐行路径明确拒绝启用缓存，成对的分阶段等待路径同样要求缓存关闭。
单Map继续支持原LM缓存与算子缓存。自定义postprocessor仍可在原单Map的整批返回后执行，
逐行路径明确拒绝它；流式响应、工具调用、结构化生成、其他供应商或特殊模型转换均明确拒绝。
级联、多模型、全表依赖、Join和并行展开保持`pending`。

## 调用、响应与所有权

`sdk.prepare_call`读取LM已经选择的完整消息及有效参数，直接调用LiteLLM的
`get_optional_params`与OpenAI provider转换。它按原非流式调用移除`stream`，并按OpenAI SDK处理
空`extra_body`。不会擅自把`max_completion_tokens`改成`max_tokens`。
原始文本必须可以UTF-8编码；Python代理码点在JSON复制前拒绝，包括能被JSON复制合并的代理对。
整批预检也先拒绝无效suffix，避免已接纳合法前缀后才发现该输入错误。
确定性HTTP验证对拍服务实际收到的正文，覆盖有效model、消息、生成预算、logprobs及Unicode。
API base、key、超时和model必须与传入的`FixedModelConfig`相同；连接配置由调用方在仓库外读取。
成对运行要求显式API base、认证及有限超时、重试为0；未知参数和隐藏的全局OpenAI配置会被拒绝。

`LotusBatchExecutor`只借用公共execution，按公共offer数量与容量提交，不创建执行线程池。
调用方也可传入`session_factory(query_id, operator_id)`提供每批session。
当`execution=None`时须同时提供typed `SessionLimits`，调用字节额度先按该声明检查，
打开session后再核对其实际单项输入／结果容量；有本地execution时容量继续来自其Engine。
默认仍创建`NativeTaskSession`，完整请求准备、SDK响应重放和批次关闭责任保持。
该接口不选择多Job策略；跨进程桥接和调度候选的资格由各自固定研究版本记录。
输入miss最多4096行，原始响应的批次留存默认最多16MiB；这些是编码字节额度，不是RSS测量。
`last_responses`默认只保存最近一次miss批次；`retain_full_responses=False`不保存这份原始响应列表，
`on_response(index, full)`仍在解析前交给调用方保存证据。SDK结果、错误中的完整响应和批次字节额度继续保留。
<a id="whole-text-preflight"></a>
`prevalidate_batch=True`在打开本地flow或调用`session_factory`前逐行准备完整纯文本调用，
检查固定模型／连接配置、单请求字节与期限及累计输入容量。非法后续窗口零flow、零发送；取消及到期覆盖预检。
只保留首条规范调用，其余正文校验后丢弃，执行时在有限块内再次准备；额外准备计入本次批次期限与任务成本。
`on_prepared(index, call)`仍只观察实际执行使用的同一不可变值。默认`False`保持原有限块准备与发送语义。
直接调用`sdk.validate_batch`而不传模型／容量guard时，保留原轻量消息、共同参数及UTF-8检查。
这些入口只支持上述固定版本普通OpenAI纯文本转换，不适用于工具、结构化输出或其他模型转换。
通用拒绝规则来自[固定stage修补](https://github.com/3444374/ai-operator-execution-optimization/blob/7d83c2a625fc37901fd8079bed86bb5bc573e854/experiments/results/diagnostics/lotus_image_text_compatibility_20261013/README.md#stage-review)；
本集成分支只实现文本边界，[本地Core回归](../../../tests/semantic_methods/test_lotus_batch_preflight.py)使用内存后端，无SDK／HTTP／模型。
stage图文profile、准备缓存及其实际Daft/Ray结果仍属原研究版本，不作为本集成HEAD的实库资格。
LOTUS在Core前的整表读取／解码与消息构建尚未获得统一准入，本次只核对已经形成的完整文本调用。
准备、批次观察与执行使用同一绝对期限；准备块内和接纳、推进之前检查取消及到期。
完整响应包含实际HTTP状态、重复header、原始body和HTTP版本；Content-Encoding由同版本HTTPX解码，
原始字节仍保留。HTTP错误保留完整响应并按LM错误路径上抛；解析错误同样保留原始响应。

调用方创建、推进和关闭execution；单Map适配负责自己的Job/session，在观察及SDK转换结束后归还结果lease。
执行或delivery处理的第一异常保持顶层；release或close的附加错误以阶段、类型写入notes，
具体异常对象按批次保存在`last_cleanup_errors`。没有执行错误时首次清理错误传播，后续关闭仍会尝试。
取消及超时停止新增任务，远端未确认工作仍由公共Engine保留，退出不伪造远端停止证明。
`iter_two_map_rows`只读有限外部输入，每行最多两个串行请求。输出带原始`RowIdentity`，按可用顺序返回；
调用方可依原序号复位。方法结果在调用方消费期间仍计入固定MethodBudget，关闭迭代器归还本地状态。
source EOF后继续处理后继，直到所有方法结束才seal。execution的拥有者随后继续reap并关闭backend。
两Map输入或方法的第一异常继续传播；grant、driver／session和Job清理分别尝试，附加异常对象保存到
该异常的`lotus_cleanup_errors`，类型与阶段写入notes。没有原执行错误时传播第一项清理异常。
完整阶段响应以公共响应编码的base64保存到最终值的`raw_responses`，保留usage、logprobs及原始字节。

实验观察对原生与SemLoom统一记录：完整消息及有效参数进入原LM未缓存批次时，任务已经具备方法输入；
该共同时间写入`task_ready`，HTTP编码完成另外记录为`lotus_http_call_prepared`。这与旧版逐条观察转换
结束时记录的时间口径不同，历史记录保持原值，不能合并成同一单请求延迟样本。实际出站请求仍由gateway独立核对。

## 最小入口

调用方使用公共`build_native_execution(model_config, physical=RayMapConfig(...), ...)`创建主执行路径；
`physical=None`只用于明确命名的本地诊断。两者共用任务与完整响应接口。

```python
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor

executor = LotusBatchExecutor(execution, model_config, query_id="query", operator_id="map")
with lotus_executor(lm, executor):
    result = frame.sem_map("Summarize {text}.", return_raw_outputs=True)

# 原生对照：同一原生LM与Map API，由LOTUS/LiteLLM执行。
with lotus_executor(native_lm):
    native_result = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
```

## Daft Native与Ray Data执行

`LotusNativeBatchExecutor`沿同一个未缓存批次入口选择
`NativeCallExecutor("daft"|"ray-data", NativeGraphOptions(...))`。
路径身份为`LOTUS method + Daft Native`和`LOTUS method + Ray Data`；原生LOTUS对照继续使用
`lotus_executor(lm)`。原LM缓存、算子缓存、usage、计费、logprobs、Map提示、postprocessor、
缓存命中与miss合并以及整阶段等待均由固定LOTUS源码继续处理。
单模型非流式普通OpenAI文本配置沿用显式API base、认证、有限超时和零重试要求；其他配置明确拒绝。
原LOTUS的LM与算子缓存保留；额外的全局LiteLLM响应缓存需要另行指定策略，当前新入口拒绝该配置。

上述缓存说明适用于基础LM接点。[原生接点测试](../../../tests/semantic_methods/test_lotus_native.py)
覆盖单Map首次全未命中与重复算子命中，以及直接LM的部分命中／未命中合并；
不等于完整实验观察器的缓存资格。`supplier_adapter_query._LotusBatchObservation`按全部源行核对未缓存批次，
配套入口显式关闭结果缓存；不能仅打开开关便声称支持混合命中的完整实验流程。

公共[完整调用执行](../../baselines/text/frameworks/native_calls.py)只接收已准备的完整HTTP正文、
原行与调用身份、目标、headers和单请求期限。Daft0.7.21的原生异步批次函数及
Ray Data2.56.1的HTTP Processor拥有并行与结果缓冲；它不调用SemLoom Core或方法驱动器。
Daft调用方须在首次创建runner之前显式选择`daft.set_runner_native(num_threads=...)`；
Ray运行时由调用方创建和关闭。运行中的Ray会使Daft默认自动选择Ray runner，因此未明确选择Native时拒绝该身份。
HTTP正文按原字节发送，错误状态、重复响应头、原始压缩正文和HTTP版本完整返回；
网络失败没有伪造的HTTP状态，响应超量与非法关联作为执行错误传播。

连接复用保留每条完整请求的默认cookie行为，重定向在同一请求内沿用cookie；
并行或后续独立请求使用各自的cookie jar，不继承别的调用状态。单独持有的HTTP客户端关闭失败
不会覆盖已有执行或传输错误，附加关闭类型随脱敏诊断保留。修后组合与待验证项见[发布记录](../../../../experiments/results/postgresql/native_executor_baselines_20261012/README.md#native-release-composition)。

`LotusBatchExecutor`与`LotusNativeBatchExecutor`共同使用`sdk.parse_lotus_response`，
以固定LiteLLM/OpenAI SDK解析已返回响应；旧`parse_native_response`保留为兼容入口。
SemLoom借用已分派的PreparedLotusCall直到响应解释完成，不重复准备请求；
原LOTUS仍使用原执行入口。各替换批次采用相同的SDK重放工作，HTTPX客户端仅重放本地响应。
该客户端断言完整请求字段与已准备正文相同、认证相同且仅执行一次解析；没有第二次HTTP发送，
也不会进入统一HTTP观察器。HTTP状态、畸形JSON和缺失content的错误类型由原SDK及LM决定。
确定性检查对拍关闭的localhost端口与实际HTTP读超时的最终异常类型、状态和LM用量。
SDK传输异常的详细子类、逐字消息和所有超时阶段尚未逐项核实；HTTPX的标量超时分别作用于各阶段，
新HTTP kernel使用总时长与连接超时，不能将这两者解释为所有传输时序均相同。
Core报告的未知传输失败仍保留原拥有权与失败状态，本次共享响应解释不伪造成功或提前归还。
IMLane UDF的容量/身份等执行错误另存`execution_error`，不会重放为HTTP连接错误；
已知内置类型与原错误诊断向调用方恢复，未知作者UDF异常保留原类型诊断并以RuntimeError报告。
作者仍收到结构化Arrow结果；正常HTTP/传输wire保持兼容，取消和未确认工作保持原退出责任。
[SDK差分检查](../../../tests/semantic_methods/test_lotus_response_replay.py)使用本地HTTPX MockTransport，
包含usage/logprobs、缺字段、401/429/500、非法JSON和已知timeout元信息；缺固定SDK时显式跳过。
原SDK默认跟随重定向，新入口同样默认开启；同目标302会改用GET、307会保留POST，两种行为已逐项对拍。
显式`follow_redirects=False`用于已有关闭重定向的配对配置；跨目标重定向尚未验证。
`staged_two_map`同样可以选择此执行器，第二个Map仍等待第一个Map整阶段完成。

单一消费线程只供同步调用方读取原生图和触发停止，不是请求池。取消、到期或consumer关闭会停止
继续取结果并关闭本次原生图；已经发送的HTTP请求可能仍在服务端运行。
`last_close_report.remote_http_stopped`明确为`unavailable`，实际替身活动归零由独立服务记录验证。
完整批次输入与返回均有编码字节额度，不将这些额度解释为RSS。
Daft连接按查询和实际异步loop拥有及关闭；Ray Data沿原HTTP stage的批次session_factory复用，
默认batch_rows=1不代表跨批次复用。实际厂商图参数与声明C分别进入关闭报告，C不表示观测并发。
Ray行数包络、活跃HTTP上界、导入生效值及统一代理实测口径见[公共参数说明](../../baselines/README.md)。
没有新增统一请求池或SemLoom准入策略；客户端连接器不再另设并发额度。

两种批次入口接受可选`call_limits=LotusCallLimits(...)`；它声明请求字节、完整HTTP响应字节、
行数、累计输入/响应字节及request/batch期限。显式profile必须能由选定Engine item容量承接，
native入口在准备前核对目标行数与实际executor容量。普通LOTUS替换请求的默认完整响应额度为1MiB。
省略profile时保留已有批次参数，并从实际Engine item容量推导有限请求/响应额度；
自定义小容量诊断仍可运行。SemLoom继续有限准备，native继续采用自己的物理准备方式。
默认分块路径在准备时检查逐条请求和累计输入，不宣称未知整批字节在任何HTTP之前全部核完。
SemLoom显式`prevalidate_batch=True`先按上面的整批文本规则拒绝，再打开flow；原生替换入口保持原路径。
batch期限覆盖准备、消费至EOF和SDK解释，清理时间另记；各替换传输按总时长核查request期限。
原LOTUS SDK仍保留阶段超时语义，本profile不能据此证明六条路径所有超时阶段等价。
IMLane默认128行来自自有调用桥；显式2000行新桥已在作者CPython/Ray通过原Q1的软件检查。
同一完整查询持续投递/交付、三层共用有限容量，配置与旧前置拒绝记录见[IMLane接口](../../../integrations/sema/imlane_calls.md)。
MovieQuery与CLI预检已读取所选显式容量，旧配置保留128行；模型质量、性能与共享Coordinator独立取消尚未验证。
原生消费线程的执行与关闭异常在停止/join后进入`last_close_report.worker_errors`，
关闭异常对象保存在`last_cleanup_errors`；已有查询第一异常继续传播，附加类型进入notes。
提前关闭而没有查询错误时，图关闭失败向调用方传播；`clean=False`与未知远端完成保持。
[控制流回归](../../../tests/baselines/test_native_call_cleanup.py)使用真实消费循环和图替身，未运行厂商库。

`NativeCallExecutor("ray", ...)`增加独立Ray Core执行参照，沿用相同LOTUS批次与SDK解析。
它直接使用Ray2.56.1原生async actor、固定输入分片及`ray.wait`，每个actor保留一个HTTP调用名额，
数量取`NativeGraphOptions.concurrency`，不得超过配置或实际Ray逻辑CPU数量。
Ray拥有actor队列与调度；该静态拓扑属于`diagnostic_reference`，与官方Ray Data图分别登记。
停止或consumer关闭仅对本图actor发出`ray.kill(no_restart=True)`；
`last_close_report`保存创建及终止请求数量，检查独立确认actor状态为DEAD、CPU归还及HTTP活动归零。

两Map使用`LotusMapStage("Summarize {text}.", "summary")`，随后
`LotusMapStage("Classify {summary}.", "label")`。
`staged_two_map(frame, lm, stages)`保持每个原生Map整阶段等待；同一batch选择器也可为它提供SemLoom执行。
`LotusTwoMapMethod(lm, stages, model_config=...)`配合`iter_two_map_rows`去掉行间整阶段等待，
调用方显式提供`MethodLimits`和`MethodCapacity`，其保留空间须能容纳两阶段输入、状态和完整结果。
两条路径都使用原提示和默认parser，输入来自外部授权文本列。

## 验证入口

[SDK与方法检查](../../../tests/semantic_methods/test_lotus_adapter.py)覆盖真实LOTUS/LiteLLM、
原生HTTP正文、usage、费用、logprobs及两Map依赖；
[等待检查](../../../tests/semantic_methods/test_lotus_wait.py)覆盖等待序号和结果留存；
[异常检查](../../../tests/semantic_methods/test_lotus_cleanup.py)覆盖第一错误、附加清理错误和Core资源归还。
[控制检查](../../../tests/semantic_methods/test_lotus_control.py)以真实Core和明确替身覆盖准备中停止、两Map清理及未知远端工作留存。
[Daft/Ray检查](../../../tests/semantic_methods/test_lotus_ray.py)需显式启用`SEMLOOM_LOTUS_RAY_TEST=1`，
并将`SEMLOOM_LOTUS_RAY_TMP`设为仓库外短目录，`SEMLOOM_LOTUS_TEST_ARTIFACT`接收脱敏事件。
这些入口使用localhost响应替身；真实模型结论与实际源码身份由结果报告分别记录。

[完整调用检查](../../../tests/baselines/test_native_calls.py)和
[LOTUS原生执行检查](../../../tests/semantic_methods/test_lotus_native.py)通过
`SEMLOOM_NATIVE_CALL_TEST=1`启用实际Daft/Ray Data；`SEMLOOM_NATIVE_CALL_RAY_TMP`指定仓库外短目录。
在导入Ray Data前显式设置`RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY=2`。
检查包含完整响应、重复输入、缓存／用量、logprobs、HTTP与JSON错误、两Map等待、取消、到期及退出。
`SEMLOOM_NATIVE_CALL_ARTIFACT`指向仓库外新目录时保存脱敏请求、原始响应和HTTP活动记录。

整合只采用公共空释放实现，并引入`60c45593`的异常处理；调用侧候选保留在历史分支。
测量、失败及后续模型复核归[整合结果](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)。
后续修复的[公开核验记录](https://github.com/3444374/ai-operator-execution-optimization/blob/e8051fef151f00349581b8521d1709fb89a54612/experiments/results/postgresql/native_adapter_integration_20261009/adapter-repair-final-verification.json)
登记独立57项／组合59项检查、来源提交、原件摘要及公开恢复入口。
[历史诊断样本](https://github.com/3444374/ai-operator-execution-optimization/blob/60c45593b8d837684c24f6f29375c15af3495f76/code/src/semantic_methods/lotus/README.md#单map等待循环修复)
保留独立分支原数值与CPU设置，来源对应原提交，便于与后续整合样本分别核对。

原生图像执行器的关闭与失败责任见[集成接纳](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#集成接纳与关闭修补)：
同一 executor 互斥批次，关闭先封口，未知工作拒绝复用；部分启动 actor 只保留清理用途。
Ray Data 的 iterator／graph 和 Daft iterator 会在失败或 EOF 尝试关闭，caller-owned runtime
与物理退出证明仍归调用方。两个资格 CLI 保留执行／取消首错及独立证据写入失败。

## 连续查询观测

主测保持长生命周期 LOTUS 进程连续承接完整查询，原生内部生命周期可不同，成本全计入；每图重建本身不失去主测身份，官方服务化另作配置，见[生命周期口径](../../../../experiments/plans/reference/对照规范.md#resident-query-timing)。文本固定版本使用内部`_process_uncached_messages`替换，图像`RM._embed`是作者扩展点，不泛称统一公开插件接口。关闭缓存的图像观察不认证开启缓存配置的通用兼容。

显式`--query-repeats N`在同一native owner／FaissVS内重复原有有序查询；默认1保持原query ID，重复时增加source_query_id、repeat_index与query_ordinal。`native_call_return_elapsed_s`紧接原生`sem_search`返回采样，失败时为null；旧elapsed_s保留输出检查和失败耗时，原生实际加载／建图／传输不会扣除。

当前每次`run_workflow`新建`FaissVS`；本节资格只覆盖同一次调用内的多轮查询。磁盘索引目录或模型endpoint常驻不证明服务级RM／内存索引跨调用复用，版本变更后的重建、加载与失效也未由本切片认证。

`--observe-model-reuse`仅用于短诊断，使用实际模型UUID、PID、Linux进程start及整数计数。stock观测vision/text子模块，其他图片路径观测完整GPU模型调用至CPU NumPy返回；阶段快照与Ray批次标量采用不同取得方式，缺项不推定复用。模型构造每实例写一次私有记录；默认关闭，观测不持有模型强引用，失败／取消后不等待忙碌worker的观测RPC而继续关闭。

### Ray Data 与 Ray Serve 的独立配置

[`RayServeClipExecutor`](../../baselines/image/frameworks/ray_serve_clip.py)提供
`LOTUS + Ray Data + Ray Serve 常驻 CLIP`，沿原 RM、索引与查询方法接入。
普通同步[`map_batches`工作负载函数](../../serving/ray_data_clip_client.py)只持公开 deployment handle，
每个实际批次等待一次完整 NumPy 返回；图由 Data 调度，模型副本、路由与队列由 Serve 管理。
不使用 LLM 流式 Processor，也不引入 SemLoom 调度池。原 Ray 图内模型继续是独立可用配置。

使用既有`--arm external --executor-factory src.baselines.image.frameworks.ray_serve_clip:RayServeClipExecutor`
和仓外`--executor-config`。工厂参数为`model_path`、`processor_path`、`ray_temp_dir`，可选
`device='cuda'`、`dtype='float32'`、`batch_size=32`、`max_batch_rows=5000`、`request_timeout_s=60`。
该 executor 自动声明 RGB PIL 输入。
权重／有效处理器内容摘要和实际软件版本仍由现有 CLI 核对。

固定 Ray2.56.1，使用[普通函数任务与公开参数](https://github.com/ray-project/ray/blob/936f0d7d49d9da8ac1a9f04cc8a89faf2cb3c42a/python/ray/data/dataset.py#L530-L558)、
[可序列化 handle](https://github.com/ray-project/ray/blob/936f0d7d49d9da8ac1a9f04cc8a89faf2cb3c42a/python/ray/serve/handle.py#L331-L342)和
[同步完整返回](https://github.com/ray-project/ray/blob/936f0d7d49d9da8ac1a9f04cc8a89faf2cb3c42a/python/ray/serve/handle.py#L562-L589)。
Data task CPU1／GPU0，单 Serve 副本 CPU1／GPU1，独占 runtime 声明 CPU4／GPU1；
Torch 两线程池均为1。控制器与 driver 实际资源另计，逻辑 CPU 声明不是物理 CPU 硬限制。
worker 可重建自己的 handle/router，只有服务模型拟跨图保留，须以实际 UID／PID／调用计数证明。

查询内 RGB 转换、建图、序列化、排队、RPC、原 slow 处理、FP32 模型与完整恢复均进入 LOTUS 时间；
服务初始化和最终退出另报。每条查询完整消费并关闭自己的迭代器，健康服务仅在 owner 退出时删除。
只删除本次唯一应用并关闭自有 runtime；构造半途失败也执行清理。
Data task 重试为0、`retried_map_errors=False`；Serve 仍可能原生重新路由／重建，不能据此保证推理恰好执行一次。
任何错误、超时或实例身份变化都禁止继续复用；cancel／逻辑删除不代替物理退出核查。
Ray2.56.1正常EOF、early close及开始迭代后的异常由原生end hook关闭图；不再重复关闭。
若取得executor后BatchIterator构造失败，仅在该图明确`_shutdown is False`时补`shutdown(force=True)`；
仍保留unknown及拒复用，由owner最终退出负责服务／runtime，见[真实CPU构造故障检查](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#ray-iterator-construction)。
真实 CPU SDK 与完整 LOTUS 模型资格分别记录，服务化收益与调度机制收益分别论证。

### 输入、计时与实际模型观测

`--query-repeats N`在同一 owner／FaissVS 内执行独立完整查询，默认1；重复输入不额外去重。
`native_call_return_elapsed_s`从原生`sem_search`提交到完整 DataFrame 返回；`elapsed_s`另外保留
输出检查和失败耗时。索引准备、模型服务启动、查询及最终退出分别记录。每次查询内实际发生的
转换、建图、加载、排队、传输、预处理、模型、恢复与 LOTUS 处理全部保留，惰性图必须完整消费。
已有内存索引由同一 FaissVS 保留，答案缓存关闭。首条查询不丢弃，不据短重复声称可靠尾分位。

Ray 普通图内模型默认`--ray-image-pipeline fused`；`staged`保留 CPU 预处理／GPU 模型两段，
两者均从 RGB 数组执行相同 slow 数学，原生 Data 决定批处理与图生命周期。融合减少 actor／张量传输，
分阶段允许跨批准备与模型重叠，不能凭单次时间删除一种。普通 ActorPoolStrategy 只配置本图 pool，
不保证后续独立图的模型常驻；Serve 组合才另行声明模型所有者。

`DaftClipExecutor(input_kind='rgb_pil')`为普通默认，在查询内转 HWC uint8，经公开 cast 进入
`Image[RGB]`；`input_kind='encoded_bytes'`保留给原生编码文件及 decoder 诊断。两种输入共用
stateful UDF、slow `channels_last`、FP32 与原生资源，不预先推定 JPEG decoder 数学相同。
向量经公开 Arrow 缓冲视图校验 float32／宽512／有效区间无 NULL，再完整恢复顺序与输出所有权；
拼接和结果复制仍可能发生，不称零拷贝。CPU准备尚未单独形成新的 Daft 流水配置。

SemLoom 的源缓存绑定同一 PIL 对象、尺寸及当前 RGB 摘要；不可复用项走 lossless PNG fallback。
记录实际源复用／fallback行数、编码 CPU／墙钟和字节。源检查与转换发生在哪层就计入哪层，
不会将公共输入减负写成 SemLoom 专属调度收益。完整矩阵的ID／dtype／finite／norm检查保持。

`--observe-model-reuse`仅用于短诊断，默认关闭。实际模型 UUID／PID／Linux出生身份与整数调用计数
证明已观察到的复用；worker wrapper 或固定副本数不能代替它。构造每实例记录一次，计数随真正调用增长；
失败／取消后不等待忙碌worker的观察RPC，而继续资源处理。纯快照与请求实际执行计数分别解释。

`model_startup_s`只表示显式启动区间；Ray图内模型或Daft首次UDF若延迟加载，成本仍在对应index/query中。
任务和输入输出责任按查询处理，健康长期服务在 owner退出时结束；未确认的远端工作禁止遗忘或复用。
普通输入`17ac2aeb`的八配置与Serve来源`460d3870`的三配置已完成有限完整LOTUS检查；
[实际原值、模型身份与复算](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#native-input-and-serve)
分列来源及样本数，不据短轮推断稳态或调度优势。IMLane图像尚无已交付资格，继续使用公开external factory等待接纳。
已有[连续查询记录](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#resident-query-measurement)
及[Daft RGB记录](../../../../experiments/results/diagnostics/lotus_native_image_retrieval_20261014/README.md#daft-rgb-input)
保留原源码身份、配置、原值与失败，不自动改称当前默认的实测。后续规模与缓存选择见
[同题计划](../../../../experiments/plans/语义系统对照.md#lotus-image-resident-queries)。
