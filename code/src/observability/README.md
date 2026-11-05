# Observability cores

This package owns passive measurement and stable profiling schemas. Measurement code must not take
over the execution policy it observes.

- `metrics/` contains reusable metric collectors and summaries.
- `clock.py` owns the shared monotonic-clock identity without loading an execution adapter.
- `execution_timing.py`, `buffered_events.py` and `model_reuse.py` own optional adapter timing,
  bounded event output and weak model-reuse observations. Existing experiment imports share the same
  module objects; enabling flags, schemas and failure handling remain unchanged.
- `httpx_observer.py` observes complete POSTs using a caller-supplied request allowance;
  `request_identity.py` owns task-local observation identity. Durable experiment ledgers remain in
  `src/experiments/`; process-local prepaid claims live in `src/infrastructure/request_budget.py`.
- `records_telemetry.py` provides explicitly started, optional GPU/host/native sampling. Missing fields and observer failures remain separate from business results; no sampler starts on import.
- `profiling/` contains stable result schemas and directly imported profiling helpers.
  `profiling/ray.py` owns public task/actor submission, invocation values and legacy polling.
  `profiling/ray_scheduler.py` assembles endpoint topology, routing/admission options, the existing
  scheduler and result metrics. Policy and execution remain implemented in `src/scheduling/`;
  the two profiler modules share the existing scheduler rather than maintaining another one.
  `profiling/batch_envelopes.py` converts existing Arrow batches into typed request envelopes.
  The old `profiling/replay.py` argument-driven replay adapters have been retired; their
  [exact source and recovery](../../../experiments/reproduction/native_baselines/README.md#retired-profiler-replay)
  remain separate from the maintained SDK. Caller-supplied `replay_envelopes` still work.
  The unused PostgreSQL profiler argument parser has been retired with the old entry;
  its [exact historical source](../../../experiments/reproduction/native_baselines/README.md#retired-profiler-parser)
  preserves the original arguments. Callers supply already constructed admission objects to the SDK.

The former HTTP forwarding gateway and its exclusive loopback tests are retired. Their
[exact recovery source](../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md#retired-http-observation)
remains available with the old experiment code. Shared HTTPX observation, request identity, native
traces and trace-summary analysis keep their existing implementations.

Records `MetricSources` owns one locked cumulative CPU counter baseline. In a visible Linux cgroup,
`AllocatedCPUCounterSource` reads group CPU time and divides its rate by the smaller of process
affinity and visible CPU quotas, preserving fractional CPUs. It also checks usage of capped ancestors
and retains the largest observed percentage, so sibling usage of a shared quota is visible. The source
reports group CPU pressure rather than an actor's CPU time or a per-core maximum. Separate v1 cpuacct
groups cannot establish the same quota membership and remain unknown. Without cgroup scoping, host
counters are restricted to process affinity. Legacy `host_metadata()` callers retain their reader.

Successive calls on different RPC threads share the counter and monotonic clock. First reads, counter
rollback, allocation changes and invalid intervals stay unknown. The feedback report retains interval,
source, effective CPUs and quota; the existing field `host_peak_cpu_pct` carries the qualified CPU
percentage for compatibility. Host RAM remains explicitly host context, with separately named visible
cgroup memory limit and headroom. The latter also checks ancestor usage; host available RAM is never
reported as container headroom. Kernel interfaces follow [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)
and [v1 bandwidth control](https://docs.kernel.org/scheduler/sched-bwc.html). Tests and limits are listed
in the [CPU check record](../../tests/records_cpu.md#resource-allocation-cpu).

With native feedback enabled, `worker_extension_cls` registers
`src.infrastructure.gpu_allocation.SemLoomGpuIdentityWorkerExtension` before engine construction.
Startup calls the named `semloom_gpu_identity` method through ordinary `collective_rpc`; request
arguments and the reply contain primitive values. The original callable request was rejected by the
fixed default encoder and has been replaced. Default serialization settings remain unchanged.
Only an already initialized single CUDA device is read. The private readiness frame carries its UUID
to the actor's existing sampler. `sources.gpu.binding_source` distinguishes `native_worker_receipt`
from `cuda_visibility_without_worker_receipt`; the latter retains the prior explicit UUID mask or
unambiguous single-NVML-device resolution, and does not claim a worker receipt. Numeric multi-device
ambiguity stays unknown. Feedback off registers no extension and adds no native observation RPC.

`metrics/memory_pressure.py` reads memory pressure stall information (PSI, 内核压力停顿统计)
and reclaim/out-of-memory (OOM, 内存耗尽) event
counters from the current process's visible cgroup v2 hierarchy. The first read,
unsupported systems, incomplete ancestors, I/O errors, counter reversals and
membership changes stay unknown. One source owns its locked baseline across RPC
threads; a hierarchy change starts a new source identity. No kernel files are
written and no total/headroom value becomes a storage grant.

`MetricSources` retains the kernel report under `sources.host.memory_pressure`.
Its false value means no observed increments between consecutive reads. This is
neither an absence-of-OOM guarantee nor measured spare capacity. A model actor
adds its actual PID, query identity and capture clock in the existing resource
RPC. The records adapter checks that identity and conservatively maps source age;
optional guard use is separate from these passive readings. Cgroup siblings can
contribute pressure, and allocations outside this hierarchy are not covered.
The [CPU record](../../tests/records_cpu.md#host-memory-pressure) covers the reader,
binding and existing public owners; real Linux/Ray/model validation is pending.

The RPC carries `timeout=1.0`; the current uni executor does not enforce it around synchronous worker
code. The 1.5-second `asyncio.wait_for` bounds local client waiting, not worker completion or physical
termination. Timeout leaves the optional identity unknown; existing model ownership and cleanup apply.
The supported interface is verified against the fixed [worker extension implementation](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/worker/worker_base.py),
[default encoder](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/serial_utils.py) and
[uni executor](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/executor/uniproc_executor.py).
The [CPU source probe](../../tests/serving/test_vllm_identity_protocol.py) runs real MessagePack with
hash-checked source definitions and CPU model substitutes. Full vLLM, real Ray and GPU execution of
this candidate remain pending.

The request-feedback report also keeps `recent_RPC_attempts` for the current bound Core session:
at most 64 fixed-metadata records plus cumulative `RPC_counts` for returned resource attempts,
including evictions and audit errors. The existing snapshot receipts still own the complete RPC ledger.
The same optional `observe_native` callback carries this metadata for rejected and valid RPCs.
Request/current/response session IDs and RPC indices join existing query boundaries; timestamps name
their driver, actor-counter and native clock domains without asserting a cross-host clock mapping.
An unbound actor, absent native frame and rejected CPU interval remain separate facts. This audit
does not retain prompts, input rows or model outputs, refresh a native timestamp, or feed rejected
resources into policy. `recent_samples` and the two physical policy samples still require the
original session/native qualification. Bind resets the new diagnostics; close retains terminal
evidence. Native validation in an audit is the RPC-validation result, not provider/controller adoption;
a subsequent query switch still invokes the original session guard. Reference profiles keep
resources off; omitting feedback remains disabled. Turning the
external observation tool off still permits existing production feedback and its bounded report;
this change starts no extra readers or RPCs.
New diagnostic IDs use the existing 32 lowercase hexadecimal engine identity and uint64
integer ranges (with -1 for an unbound session). Malformed fields become null with fixed
`invalid_fields` names before retention or callbacks; values already unavailable at this boundary
stay unknown/null. No original text or oversized integer is retained in the new diagnostic row.
The [CPU rejection cases](../../tests/execution_provider/test_resource_rpc_rejection_audit.py)
check these boundaries with local Futures, not Ray or model execution.

Native scheduled-work MFU helpers live in `metrics/mfu.py`, `mfu_trace.py` and the explicitly invoked
`mfu_native_bootstrap.py`. They require one verified engine/GPU, complete token/terminal watermarks
and native FLOPs updates. Missing computed-prefill FLOPs also make an estimate unavailable; a final
completion watermark alone cannot restore that numerator. These are analytic scheduled-work estimates,
not hardware instruction utilization or kernel MFU. No model engine is registered or started by adding them.

`metrics.statistics.latency_observations`, `query_costs` and `reuse_payback` read existing receipts only.
Declare actual API/batch sample units; keep failed/unknown observations separate, report small query
samples with median/max, and charge external scan once plus complete query costs. Reporting rules and
GPU/MFU报告按[既有报告核对](../../../experiments/plans/reference/报告核对.md)声明观测窗口与分母。query-ready、完整caller返回、owner关闭分别报告；窗口未封闭的FLOPs尾部标unavailable，不能仅由finished/token终态补作完整分子。

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

<a id="repeat-mean-confidence"></a>

## 重复均值的置信区间

`metrics.statistics.repeat_summary` 返回 `mean ± critical × sample_stdev / sqrt(n)`，
其中 `critical` 对应双侧 95% 区间的 `t(0.975, n-1)`。公式与分位点定义见
[NIST 均值置信区间](https://www.itl.nist.gov/div898/handbook/eda/section3/eda352.htm)和
[t 临界值表](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3672.htm)。
无样本与单次重复继续分别标为 `unavailable:no_formal_repeats` 和 `single_repeat:no_interval`；
单次的上下界占位值不表示存在置信区间。

实现不新增运行依赖：df=1–30 逐项查表，另保留 40、60、120、1000、10000、1000000 六个锚点。
表值向上保留六位小数；缺档取较小 df，超过最大锚点继续取该有限 df 的值。
由于同一上尾分位点随 df 增加而下降，所得半宽相对精确 t 区间保守，所有正整数 df 的额外半宽
小于 1.1%。常量经本机既有 SciPy 1.13.1 `t.ppf(0.975, df)` 和 mpmath 1.3.0
45 位精度不完全 beta 函数反演交叉核对；二者仅用于离线验证，不是运行或测试依赖。
半宽先合并 `critical / sqrt(n)` 再乘样本标准差，避免可表示区间在中间乘除时发生可规避的
浮点溢出或下溢。

以下修前值来自 `277d8f4862c291bc2830cc6622f5a0b4f28b5c90` 的实际函数，误差均相对精确 t 半宽：

| 重复数 n | df | 修前临界值 | 修后保守临界值 | 高精度参考 | 修前半宽误差 |
|---:|---:|---:|---:|---:|---:|
| 3 | 2 | 4.303 | 4.302653 | 4.302652729749464 | +0.0081% |
| 12 | 11 | 2.131 | 2.200986 | 2.200985160091640 | −3.1797% |
| 32 | 31 | 1.96 | 2.042273 | 2.039513446396409 | −3.8986% |

n=3 只有旧表的取整差，未发生跨档；n=12 修后比精确半宽大约 0.000038%，n=32 大约 0.1353%。
原始时间、均值、中位数、标准差及独立的 nearest-rank P99 均不因本修复改变。

发布影响核查固定于上述提交：唯一生产调用者是
[`summarize_formal_repeats.py`](../../scripts/analysis/summarize_formal_repeats.py)。对 Git 对象中的
`experiments/`、`docs/`、`figures/` 及根现行报告搜索其 `ci95_lower`、`ci95_upper`、
`repeat_summaries` 输出字段，并只读扫描 2768 个压缩文件、15105 个归档成员
（4506690994 个解压字节），均无匹配且解压无错误。未确认任何已发布摘要由该调用者产生，
因此没有离线重算或改写历史摘要、原始观测；此结论不覆盖仓库外或后续生成的报告。
历史 `ci95_half_width` 同名统计不代表使用过本函数，例如 2026-07-25 的 flush 报告使用 n=5、
t=2.776，不属于本次跨档错误。

保守仅描述相对 t 临界值的取法；有效覆盖仍依赖独立重复及适用的均值 t 模型。
输入每轮 P99 时，此函数给出每轮 P99 的**均值**区间，不能充当总体 P99 分位数的置信区间，
也不证明尾部样本充分或独立。测试见
[`test_statistics.py`](../../tests/observability/test_statistics.py)，覆盖 df 档位、极小与大样本、
非法 df/非有限观测及实际 CSV 调用者的 warm-up 排除、分组计数和配对退化统计。
另覆盖接近浮点上限的可序列化区间，以及极小非正规数的非零区间。


共用脱敏和私有文件输出直接依赖 `src.infrastructure.redact`、`src.infrastructure.private_artifacts`，
与现有实现使用同一对象；baseline 的原公开导出仍供比较和历史调用使用。
[本次消费者迁移](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#neutral-shared-tool-imports)
只调整导入归属，不增加观测、落盘步骤或执行策略。
