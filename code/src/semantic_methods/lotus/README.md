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

调用方创建、推进和关闭execution；单Map适配负责自己的Job/session，在观察及SDK转换结束后归还结果lease。
执行或delivery处理的第一异常保持顶层；release或close的附加错误以阶段、类型写入notes，
具体异常对象按批次保存在`last_cleanup_errors`。没有执行错误时首次清理错误传播，后续关闭仍会尝试。
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

[SDK与方法检查](../../../tests/semantic_methods/test_lotus_adapter.py)覆盖真实LOTUS/LiteLLM、
原生HTTP正文、usage、费用、logprobs及两Map依赖；
[等待检查](../../../tests/semantic_methods/test_lotus_wait.py)覆盖等待序号和结果留存；
[异常检查](../../../tests/semantic_methods/test_lotus_cleanup.py)覆盖第一错误、附加清理错误和Core资源归还。
[Daft/Ray检查](../../../tests/semantic_methods/test_lotus_ray.py)需显式启用`SEMLOOM_LOTUS_RAY_TEST=1`，
并将`SEMLOOM_LOTUS_RAY_TMP`设为仓库外短目录，`SEMLOOM_LOTUS_TEST_ARTIFACT`接收脱敏事件。
这些入口使用localhost响应替身；真实模型结论与实际源码身份由结果报告分别记录。

本分支保留调用侧空释放判断作为候选；整合采用公共空释放实现，异常处理和等待回归可单独选取。
测量、失败及后续模型复核归[整合结果](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)。
后续修复的[公开核验记录](https://github.com/3444374/ai-operator-execution-optimization/blob/e8051fef151f00349581b8521d1709fb89a54612/experiments/results/postgresql/native_adapter_integration_20261009/adapter-repair-final-verification.json)
登记独立57项／组合59项检查、来源提交、原件摘要及公开恢复入口。
[历史诊断样本](https://github.com/3444374/ai-operator-execution-optimization/blob/60c45593b8d837684c24f6f29375c15af3495f76/code/src/semantic_methods/lotus/README.md#单map等待循环修复)
保留本分支原数值与CPU设置，来源对应原提交，便于与后续整合样本分别核对。
