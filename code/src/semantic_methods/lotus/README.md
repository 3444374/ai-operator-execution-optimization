# LOTUS方法与SemLoom执行适配

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
确定性HTTP验证对拍服务实际收到的正文，覆盖有效model、消息、生成预算、logprobs及Unicode。
API base、key、超时和model必须与传入的`FixedModelConfig`相同；连接配置由调用方在仓库外读取。
成对运行要求显式API base、认证及有限超时、重试为0；未知参数和隐藏的全局OpenAI配置会被拒绝。

`LotusBatchExecutor`只借用公共execution，按公共offer数量与容量提交，不创建执行线程池。
输入miss最多4096行，原始响应的批次留存默认最多16MiB；这些是编码字节额度，不是RSS测量。
`last_responses`默认只保存最近一次miss批次；`retain_full_responses=False`不保存这份原始响应列表，
`on_response(index, full)`仍在解析前交给调用方保存证据。SDK结果、错误中的完整响应和批次字节额度继续保留。
`prevalidate_batch=True`先检查整批纯文本消息与共同参数，复用首条规范准备值；其余完整调用按有限块准备，
`on_prepared(index, call)`将同一不可变调用值交给观察器，避免观察与执行重复转换。默认`False`保持直接入口。
这项轻校验依赖上述固定版本普通OpenAI纯文本转换，不适用于工具、结构化输出或其他模型转换。
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

公共[完整调用执行](../../baselines/text/frameworks/native_calls.py)只接收已准备的完整HTTP正文、
原行与调用身份、目标、headers和单请求期限。Daft0.7.21的原生异步批次函数及
Ray Data2.56.1的HTTP Processor拥有并行与结果缓冲；它不调用SemLoom Core或方法驱动器。
Daft调用方须在首次创建runner之前显式选择`daft.set_runner_native(num_threads=...)`；
Ray运行时由调用方创建和关闭。运行中的Ray会使Daft默认自动选择Ray runner，因此未明确选择Native时拒绝该身份。
HTTP正文按原字节发送，错误状态、重复响应头、原始压缩正文和HTTP版本完整返回；
网络失败没有伪造的HTTP状态，响应超量与非法关联作为执行错误传播。

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
没有新增统一请求池或SemLoom准入策略；客户端连接器不再另设并发额度。
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
