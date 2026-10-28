# Native task execution

This module accepts prepared single-model request bytes through the existing Core.
It does not interpret prompts, change generation parameters, or replace a supplier's parser.
The implementation is adopted from integration source `671a22dd4285319209396ece3572bb89ff5ab74e`;
[main qualification](../../../../experiments/results/diagnostics/main_foundation_20261015/README.md)
records the composed main source and the checks it actually passed.

## Python consumer

`prepare_native_task` produces an `OfferedTask` with call, row and stage identity.
Its default work estimate counts requests; it is not a token or GPU cost estimate.
`build_native_execution(config, physical=RayMapConfig(...))` uses existing Daft payload batches
and Ray HTTP workers. `physical=None` selects the local diagnostic transport.
The builder retains the existing capacity and scheduling policies. Native input defaults to 1 MiB.
`max_request_bytes` may explicitly select at most 8 MiB; a nondefault value requires an explicit
`input_bytes` total at least as large. Ray window, object and optional preparation capacities must
fit that choice before transport creation. PG public factories and wire framing remain at 1 MiB.
The three private assembly factories form one capacity closure;
[boundary and lifecycle qualification](../../../../experiments/results/diagnostics/main_lotus_animals_20261015/README.md)
keeps logical input, prepared suffix, SDK/proxy copies and process RSS distinct.

`NativeTaskSession` registers a query Job and one operator flow. `offer` transfers only its
accepted prefix, leaving the caller responsible for the suffix; offering sends no request.
`advance` returns completion-order deliveries, and `wait` uses Core progress and deadlines.
The consumer decodes a delivery, discards the returned bytes, then releases its lease.
Results remain charged until that release. Empty releases preserve the progress generation;
invalid batches and closed handles remain errors.

`NativeQueryJob` borrows one existing query grant across declared operator flows.
Closing a borrowed session keeps the query grant. The query owner closes the Job after its
consumers stop. `request_cancel` is the cross-thread cancellation signal; the owner thread
performs other Core mutations. Source EOF is not sufficient to seal a method that can still
produce continuation tasks.

Closing a flow does not close the shared backend. Unknown remote work remains charged;
the service owner continues to advance/reap the Engine until it can settle the work.
`execution.close()` returns false while any Job or task remains. Identified Ray payload
failures prove that HTTP was not entered and can settle that task; HTTP transport failures,
worker loss and cancellation alone do not prove remote completion. There is no automatic retry.

## Complete HTTP responses

`FullResponseTransport` carries status, duplicate-preserving header pairs, HTTP version and raw
body bytes in the existing result reservation. The envelope permits at most 8192 encoded header
bytes and 128 pairs; `max_result_bytes` covers both header and body. SDK-specific decompression,
JSON interpretation, usage accounting and error policy belong to the consumer.

`AsyncFixedModelTransport` retains its completion response and HTTP error mapping.
`RayMapConfig.response_mode` defaults to `completion`; the native builder selects `full`.
A separately owned native worker pool must also select `full`. Worker identity includes this
mode, so an incompatible pool cannot be borrowed. Confirmed pre-send failures have no invented
HTTP status and raise `CompletionAdapterError` when decoded.

Both transports reuse an HTTPX client and connection pool. A standard `CookieJar` belongs to
one execute task, including redirects within that request. Concurrent calls and later queries
cannot inherit its cookies. Redirect following remains disabled by default; cancellation
releases that request's cookie context, and the transport owner closes the client.

These are engineering decisions within the existing Core and transport interfaces.
[Native lifecycle tests](../../../tests/execution_provider/test_native_tasks.py),
[HTTPX isolation tests](../../../tests/execution_provider/test_httpx_request_cookies.py),
[complete response tests](../../../tests/execution_provider/test_full_response.py) and
[real CPU Ray tests](../../../tests/execution_provider/test_native_ray_integration.py)
exercise the implementation. Supplier adapters, a complete LOTUS query, GPU benefits and PG
integration on this composed source require their own qualification.

## Optional single-model finite record plans

`SemLoomRecordChainCore(..., execution="record_local_sections")` runs a legal finite
Filter/Map/Extract plan using the shared `ChainRun` and one prepared Async model owner.
`execution="fused"` remains the default. This is a Python API option, not an automatic
selector or a change to a baseline's native execution.

Record-local F/M sections use `LotusChainFIFO` over the existing Core/MethodDriver.
Extract waits for actual source EOF, complete predecessor finals and drained result
responsibility; the original parser and relation assembler then run once before continuation.
One model is started and finally closed per query, including all its sections. Cross-query
reuse, multiple deployments and arbitrary DAGs are not provided by this option.

The caller owns Ray 2.56.1. Preparation/model batch sizes must both be 1; explicit
`max_active_batches` must fit the engine capacity. Formatting/parsing remains in the
method caller and no Filter prepare actor is allocated. Core/MethodBudget logical limits
do not bound whole-query relations, audit memory, process RSS or GPU KV bytes.
The full-query mode defaults to request-count admission (`token_budget=None`).
An explicit existing `ChainTokenBudget` can be passed to the same owner:

```python
owner = SemLoomRecordChainCore(engine, resources, execution="record_local_sections",
                             token_budget=budget)
```

This option supports complete Filter/Map plans with one model deployment and one model role.
Extract and model-role changes are rejected before source consumption or model startup;
their existing count-mode behavior remains available. The budget's engine and exact set of
stage output estimates must match the query. The owner copies its declared configuration.
Every legal continuation is estimated from its actual prepared messages and stage generation,
including columns produced by earlier calls. The existing Core alone holds both the request
limit (`max_active_batches`) and token work limit (`budget.active_tokens`); the latter reserves
estimated complete input plus the stage output cap, rather than actual KV bytes or GPU work.
Expected output is diagnostic and does not reduce that reservation. Confirmed terminals and
the existing producer-retirement protocol settle work; cancellation alone cannot release it.

Source positions are increasing and protocol occurrence IDs unique; user index labels
and business IDs may repeat. A query failure stops continuation and retains earlier facts.
Late receipts may supplement the stopped query's report but cannot rerun a parser or
produce successors. Actual unknown results and known usage remain; producer retirement
and physical GPU exit are distinct checks.

See [whole-query CPU cases](../../../tests/execution_provider/test_lotus_record_local_query.py),
[whole-query token cases](../../../tests/execution_provider/test_lotus_record_local_tokens.py),
[prepared owner cases](../../../tests/execution_provider/test_lotus_prepared_actor_owner.py),
the [shared method API](../../semantic_methods/lotus/README.md#record-local-sections)
and [actual qualification scope](../../../INFRA_STATUS.md#record-local-sections-c827).


## records Core 可选动态接纳

`lotus_records_core.create_records_core` 的固定默认与 sync/async 既有路径保持。
可显式选择 `profile="delivered-work-scan"` 或 `profile="computed-work-local-pairs"`；
名称与原实验参数、首次／复用条件的唯一映射见[配置说明](../../scheduling/runtime/calibration.README.md#原实验配置选项)。
`profile={"mode":"async","max_inflight": C0,"admission": {"kind":"resource_feedback", ...}}`
显式接回原单遍 native-feedback admission；选项来自现有 `ResourceFeedbackConfig`，
不是另一个执行器。`C0` 是起始软窗口，owner 上界为声明的 Core request budget 与
`EngineSpec.max_num_seqs` 的较小值；prepare 数、模型容量和 actor/IPC 容量在启动前确定。
显式 `async_limits.max_inflight/result_slots` 必须覆盖该 owner 上界，不是只覆盖 C0。

`allow_exploration=True` 显式允许原有限探索；不设 admission 时保持固定且不启动 native
反馈采样。`max_inflight="auto"` 仍只是声明容量的 min，不是实测调优。同步 Core 仍只
接收 model batch 1；有限同步批参照 `lotus_records_static` 不冒称 Core 批处理。

普通查询可用同一公共 `scheduling.runtime.calibration.run_admission_query` 接精确配置复用。
Movie 的薄配置槽及计时/资格边界见 [calibration.README](../../scheduling/runtime/calibration.README.md)。
恢复的是两版旧策略本身，历史波动和负结果保留；旧短查询功能实测的边界见[证据台账](../../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621)。
两版原参数的同条件首次／复用模型验收须另列，不能以开关能运行或短查询返回替代。


### 静态参照的显式 session

维护 Movie session 现在可选择 `backend="semloom-static-reference"`、
`profile={"mode":"sync"}`，继续使用原 `RecordsResources` 的 prepare/model batch。
这是 Core 外有限批参照，不是 Core B4；Core 的原单行合同不变。
resident 在查询前调用原模型 owner 的 start，两次完整查询复用模型，query EOF 不关闭模型，
session 结束一次原 close。不同 FilterMethod 指令明确拒绝，不能复用旧 formatter 冒称支持。
准备批、原有限模型批、原 LOTUS parser、身份和未确认关闭责任不变。
本接线已做维护 CLI/CPU first-reuse 与失败边界检查，真实模型/物理退出仍需本候选单独资格。
