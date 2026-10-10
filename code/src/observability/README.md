# Observability cores

This package owns passive measurement and stable profiling schemas. Measurement code must not take
over the execution policy it observes.

- `request_gateway.py` is the five-arm Job-labelled HTTP pass-through. It forwards each body exactly
  once with no admission limit, retry, cache, route choice, or payload rewrite, then records lifecycle
  clocks and endpoint-reported token usage for cross-framework tail/fairness metrics.
- `metrics/` contains reusable metric collectors and summaries.
- `profiling/` contains the PostgreSQL AI operator profiler's stable result schema and helpers.

The gateway is an observation boundary, not a baseline executor or scheduler. Any future queue,
backpressure, batching, or retry belongs in the measured system and must not be added here.

Upstream HTTP connections are pooled by default. Sequential query owners call
`reset_upstream_connections()` before preparing each query, after the preceding query has drained;
an active request makes this operation fail. Within a query, connections remain reusable.
Public aiohttp trace signals record connection creation, reuse, connector waiting and pool generation.
`fresh_upstream_connections=True` is an optional transport diagnostic; it does not replay a failed POST.

`process_resources/` provides Linux FD identity observations, explicit missing values, sampling windows,
stable baselines, operation/error capture and gzip JSONL persistence. A tick is a sequential observation
batch, not an atomic snapshot. PostgreSQL session attribution and threshold policies live under
`src/experiments/postgresql/`; the collector makes no qualification decision.

<a id="运行指标的读法"></a>

## 运行指标的读法

测量字段以[profiler schema](profiling/schema.py)和各采集器为准。报告同时解释请求形状、服务压力、
完整结果、资源与任务质量：prompt/output token、批次行数与字节说明工作量；排队、在途与缓存
说明服务状态；完整查询时间及首token、逐token等待说明用户实际等待；CPU、GPU和能耗说明资源使用。

子阶段计时必须说明起止事件和是否重叠，跨进程计时先核对时钟条件。请求级时延、分片结束时间和
SQL查询完成时间分别保存；采样窗口未捕获短任务时保留采样范围与实际请求记录。

生成任务用答案质量、非空输出、截断与失败解释正确吞吐；embedding任务说明向量维度、数值差异与
相应质量验证。未采集字段记录原因，重复与失败分别保留。研究中的指标选择依据见
[指标调研](../../../docs/research/evaluation_metrics_survey_20260731.md#观察变量选择)。
