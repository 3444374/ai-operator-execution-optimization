# 本地生成引擎

[`vllm_engine.py`](vllm_engine.py) 接收已准备的独立文本请求；调用方拥有方法、提示、parser、
结果解释及进程监督。模型环境与 LOTUS driver 环境隔离，内核固定 vLLM 0.25.1。

`VllmEngine(engine, generation)` 在模型分配前验证默认生成配置并构造采样参数。
`run_prepared(rows, *, generation=None, deadline_monotonic=None)` 的 `None` 使用构造默认；
传入 `GenerationSpec` 时整份替换本次配置。每次调用创建独立 `SamplingParams`，不修改默认
`generation`、构造时的 `_sampling` 或其他调用的采样对象；引擎与模型权重继续复用。

`GenerationSpec.response_format` 仅接受 `None` 或 `{"type": "json_object"}`。
默认 `None` 保持普通 Filter 的生成参数；方法层决定哪些算子允许 JSON 模式。
JSON 模式不增加严格 schema、字段校验或结果补写。默认换行 stop 不随格式自动改变；方法若需
完整 JSON 输出，须在选定配置中明确指定 stop 和输出 token 上限。

配置构造、`from_dict()`、`dataclasses.asdict()` 和 `kwargs()` 都无需导入 vLLM。
`kwargs()` 返回普通字典：JSON 模式含 `structured_outputs={"json_object": True}`；仅模型内核
把该字典转换为 `StructuredOutputsParams(json_object=True)` 后交给 `SamplingParams`，与固定
[官方 chat 请求映射](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/openai/chat_completion/protocol.py)
及[采样类型](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/sampling_params.py)一致。

构造配置及每次覆盖都重检数值、支持范围和输出 token 上限。每次调用使用模型分配时的
`max_model_len` 校验输出上限；实际提示长度仍由 vLLM 原模板及输入验证处理，不截断提示。
有效采样参数在预留 `max_model_calls` 之前构造；每行在 chat 前预留一次，失败或 unknown 不退。
空批及已过期调用不预留额度。逐调用覆盖不改变身份、真实 usage/token IDs、失败/取消或清理合同。

`GenerationSpec.logprobs` 仅接受 `None` 或严格整数 `0..20`，拒绝 bool、负数和超限配置，
并直接传入 `SamplingParams.logprobs`。LOTUS helper 的 `logprobs=True, top_logprobs=10`
须由方法接点转换为整数 `10`。模型内核显式固定 vLLM 原默认的 `max_logprobs=20`、
`logprobs_mode="raw_logprobs"`，在分配模型前拒绝超限，不扩大到全词表。
[固定模型配置](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/config/model.py)
及[原始概率计算](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/sample/sampler.py)
定义此处返回的是温度、惩罚等处理前的 log probability。

未请求时 `logprobs_json` 仍为 `"null"`；请求时其值是 OpenAI `content` 数组的 JSON：
`[{"token": ..., "logprob": ..., "bytes": ..., "top_logprobs": [...]}]`，
与生成 token IDs 逐位置对齐。方法/LM 接点构造 `ModelResponse` 时再包装
`logprobs={"content": content_array}`；原生 token ID 到 `Logprob` 的字典不能直接交给 parser。
`0` 保留真实 sampled token 概率而返回空 top 列表。
转换遵循固定版本的
[chat 输出映射](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/openai/chat_completion/serving.py#L1000-L1081)
和[token 解码](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/generate/base/serving.py#L231-L248)：
优先保留 `decoded_token`，缺失文本才调用同一 tokenizer 的 `decode([token_id])`；不改大小写、
空白或特殊 token，不从最终文本重新分词。top 列表按原字典顺序取前 K 项，不重新排序；
真实 `-inf` 或低于 `-9999` 的值按官方 wire 规则限为 `-9999`，其余值不变。
sampled token 的 bytes 在原 `decoded_token=None` 时保持 `null`；top bytes 来自实际返回文本。

请求日志时缺失、错长度、缺 sampled token ID、非法候选或 NaN/正无穷会使该行失败，
保留已知 token IDs/usage 和已预留额度；不走官方缺失值默认 sentinel 分支来补写概率。
该接口提供原始响应数据；LOTUS 原类型、概率归一化、校准器和全局 cascade 控制仍由方法接点拥有。
本接口不增加 cascade、重试或跨引擎总预算。

CPU 合同见 [`test_vllm_engine.py`](../../../tests/serving/test_vllm_engine.py)：采样/模型均为明确替身，
覆盖参数前检、连续不同配置、JSON 转换、概率输出、对象隔离、预算及旧结果/清理行为。
原 LOTUS/LiteLLM 概率消费合同在独立 Python 子进程中读取明确 fake vLLM 结果，避免把 LOTUS
导入内核测试进程。CPU 检查不构成真实模型或生成质量资格，已有模型运行仍绑定各自固定源码。

`result_record(identity, *, engine_uid, start, end, status, submitted, error_type=None)`
建立原终态信封；`result_from_output(identity, output, *, engine_uid, start, end,
generation, get_tokenizer)` 将已关联的单个 RequestOutput 转为同一结果，保留验证失败前
已知的真实 token IDs／usage。同步内核共用这两个纯函数，`_result` 保留原签名薄转发。
调用方继续拥有批关联、容量、期限、取消与进程关闭；转换函数不将 abort 确认为物理终态。
原采样工厂保持未改。[新增测试](../../../tests/serving/test_vllm_result_conversion.py)、
69项CPU及64项固定旧源码逐字段差分的[来源与范围](../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#shared-result-conversion-cpu-20261019)
单列，供后续独立Async组件复用；没有运行新的模型或继承冻结Core模型资格。
