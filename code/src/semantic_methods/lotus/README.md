# LOTUS方法与SemLoom执行适配

本目录实现LOTUS单模型文本Map的成对执行入口，以及外部输入的两Map逐行继续执行。
总体设计见[语义系统方案](../../../../experiments/plans/语义系统对照.md#native-semloom-pairs)，
公共执行与完整响应见[执行适配说明](../../execution_provider/adapters/README.md)。
路径名称为`LOTUS native`与`LOTUS method + SemLoom`；两Map属于LOTUS方法适配。
全局实验入口、状态和真实模型检查由整合任务接续，本目录没有接入PG多Map。

## 固定来源与采用决定

源码依据为LOTUS1.2.4提交`b1a85fd7a66fabed8a1585d44d7597d592b4433f`，
实际库验证使用LiteLLM1.95.0、OpenAI SDK2.50.0。`sdk.validate_source`核对包版本及
LM、Map、formatter、postprocessor、缓存、设置、列引用和计费模块的SHA-256。
新增适配只调用这些库；没有复制或修改LOTUS源码。该提交的
[LICENSE正文](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/LICENSE)
为Apache-2.0，而`pyproject.toml`的分类仍写MIT，按正文记录来源。

| 已核对的源码行为 | 采用与保留 |
|---|---|
| [LM.__call__](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py)先查LM缓存，再把全部miss交给`_process_uncached_messages` | 在该实例的方法入口选择执行器；原生默认仍调用LOTUS/LiteLLM。SemLoom分支获得全部miss，随后才进入自己的有限offer |
| LM原生分支拥有RPM/TPM等待、`batch_completion`线程池与批次提交 | SemLoom分支绕过这些物理控制，直接使用公共SessionEngine及Daft/Ray传输。它没有调用单条`completion`，也没有嵌套LOTUS请求池 |
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
`last_responses`只保存最近一次miss批次，`on_response(index, full)`可在解析前交给调用方保存证据。
完整响应包含实际HTTP状态、重复header、原始body和HTTP版本；Content-Encoding由同版本HTTPX解码，
原始字节仍保留。HTTP错误保留完整响应并按LM错误路径上抛；解析错误同样保留原始响应。

调用方创建、推进和关闭execution；单Map适配负责自己的Job/session，始终归还每条结果lease。
取消及超时停止新增任务，远端未确认工作仍由公共Engine保留，退出不伪造远端停止证明。
`iter_two_map_rows`只读有限外部输入，每行最多两个串行请求。输出带原始`RowIdentity`，按可用顺序返回；
调用方可依原序号复位。方法结果在调用方消费期间仍计入固定MethodBudget，关闭迭代器归还本地状态。
source EOF后继续处理后继，直到所有方法结束才seal。execution的拥有者随后继续reap并关闭backend。
完整阶段响应以公共响应编码的base64保存到最终值的`raw_responses`，保留usage、logprobs及原始字节。

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

两Map使用`LotusMapStage("Summarize {text}.", "summary")`，随后
`LotusMapStage("Classify {summary}.", "label")`。
`staged_two_map(frame, lm, stages)`保持每个原生Map整阶段等待；同一batch选择器也可为它提供SemLoom执行。
`LotusTwoMapMethod(lm, stages, model_config=...)`配合`iter_two_map_rows`去掉行间整阶段等待，
调用方显式提供`MethodLimits`和`MethodCapacity`，其保留空间须能容纳两阶段输入、状态和完整结果。
两条路径都使用原提示和默认parser，输入来自外部授权文本列。

## 验证入口

[确定性库测试](../../../tests/semantic_methods/test_lotus_adapter.py)使用真正LOTUS/LiteLLM、
原生HTTP客户端、公共本地诊断执行和MethodDriver；
[Daft/Ray检查](../../../tests/semantic_methods/test_lotus_ray.py)进一步使用真正Daft0.7.21、
Arrow24.0.0和Ray2.56.1，明确启用`SEMLOOM_LOTUS_RAY_TEST=1`。
Ray检查要求仓库外`SEMLOOM_LOTUS_RAY_TMP`为短数据盘目录，以容纳Unix socket路径；
`SEMLOOM_LOTUS_TEST_ARTIFACT`接收脱敏后的原始事件与响应。
测试cluster使用2个CPU、0个GPU、128MiB对象存储；传输窗口约1MiB、对象额度8MiB、活动请求2。
HTTP超时5秒，Core backend等待45秒以覆盖冷Daft准备，单次脚本120秒；改变等待设置的失败原件保留。
来源工作树、依赖提交、实际检查结果和失败保存在本任务的仓库外交接记录。
全部模型响应来自localhost确定性替身，真实模型请求0；此检查不支持质量或性能结论。

2026-10-09验证：15项LOTUS专属检查、33项完整响应/原有传输/MethodDriver回归及1项真正Daft/Ray检查通过。
主传输检查包含24次确定性响应，核对了两阶段完整响应、usage、logprobs、原行关联和逐行后继推进。
测试自己的Ray进程、任务额度与Job均已退出。Mac解释器缺少LOTUS等依赖，16项实库测试明确跳过；
实际通过证据来自指定服务器的既有独立driver环境。此前SDK对拍、Unix socket路径过长和冷准备等待不足的
失败原件保留；全局整合及真实模型质量/性能验证仍由整合任务完成。

## 异常退出修复

2026-10-09在LOTUS独立分支`5ba43329`上复现：请求准备、offer或advance已有错误时，close错误会替换它；
delivery解码、响应观察或SDK解析已有错误时，release及close错误也会替换第一错误。
现在分别捕获本次执行和delivery处理的第一异常，再尝试释放及关闭。附加错误以阶段、类型写入
第一异常的notes，`last_cleanup_errors`逐批次保存`("release"或"close", 异常对象)`元组。
没有执行错误时，第一次清理错误以原对象传播，随后关闭仍会尝试；后续错误作为附注另记。
调用方正在处理旧异常时，本批次的清理错误也须传播，因此使用局部异常捕获判断本次第一错误。

[异常回归](../../../tests/semantic_methods/test_lotus_cleanup.py)新增11项检查，覆盖prepare/offer/advance、
完整响应解码、观察、SDK解析、响应字节额度、连续清理失败、成功返回、后继批次诊断重置，
以及调用方正在处理旧异常的情形。原有完整响应、`last_responses`、SDK usage和缓存token处理保留。
测试核对真实Core的任务额度及Job归还；close故障注入在执行实际关闭动作后抛出，资源归还结果
对应这一受控情形。旧失败、初步修复和最终修复的原件分别保留，没有将初步通过作为最终结果。

独立分支最终57项回归通过；以整合提交`1024fb90`为基础、仅加入异常处理的临时副本59项通过。
后者包含公共空release修复和常驻owner修复，未叠加独立分支原来的调用侧判断。
整合只采用公共空释放实现，另引入`60c45593`的异常处理与11项回归，没有加入调用侧判断。
全部独立检查使用CPU40–47、`CUDA_VISIBLE_DEVICES`为空，新增真实模型请求0。
预检查的软件依赖通过；指定8个CPU少于机器画像默认10个而报告缺项，该记录与CPU用例一起保存。
源码摘要、全部测试输出和独立补丁以仓库外运行标识`lotus-failure-repair-20261009`交接。
全局状态、证据登记及统一源码的模型复测由主会话处理。
