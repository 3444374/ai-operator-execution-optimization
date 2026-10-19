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
logprobs 继续只允许 `None`；本接口不增加 cascade、重试或跨引擎总预算。

CPU 合同见 [`test_vllm_engine.py`](../../../tests/serving/test_vllm_engine.py)：采样/模型均为明确替身，
覆盖参数前检、连续不同配置、JSON 转换、对象隔离、预算及旧结果/清理行为。CPU 检查不构成
真实模型或 JSON 生成质量资格，已有模型运行仍绑定各自固定源码。
