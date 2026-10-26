# 有界递增运行时搜索

`calibration.calibrate/run_calibrated` 提供完整工作负载的测量、选择和缓存。
同模块的 `run_admission_query` 另保存单次执行的局部接纳推荐，详见文末；两类证据不混用。
它不依赖 Movie、gold、模型库或实验目录；调用者提供真实工作负载回调及资源监督。
原显式有限候选列表和固定配置保持兼容。新增 `IncreasingSearch` 是可选搜索策略，
没有将 1/2/4/8 或模型容量 8 当作通用最优范围。

```python
from src.scheduling.runtime.calibration import IncreasingSearch, ScanBudget, run_calibrated

result = run_calibrated(identity,
    [{"max_inflight": 1, "cpu_workers": 2}], execute,
    search=IncreasingSearch("max_inflight",
        capacity_limits={"engine.max_num_seqs": 64, "owner.request_slots": 64}),
    cache_dir=cache_dir, measure=measure, repetitions=3,
    budget=ScanBudget(timeout_s=scan_seconds, max_work=scan_work),
    work_per_sample=full_workload_calls)
```

这里的 64 是示例中声明的容量边界，须由真实引擎及资源配置支持，并非自动检测或默认值。
每点按原工作负载重复至少三次，参数按增长倍数递增，最后可测非二次幂边界。
回调接收实际参数、重复号、总截止时间和单次工作上限；目标执行接收选择后的实际参数。
测量不改变提示、模板、生成参数、任务结果或 baseline 的原生调度。

## 停止与选择

| `search.stop_reason` | 实际含义 |
| --- | --- |
| `plateau` | 边界前，连续至少两个扩展点的观测乐观收益均不超过阈值 |
| `regression` | 新点观测区间已明显慢于此前最佳点，回退到已测健康配置 |
| `capacity_limit` | 命中具名引擎/owner 容量；不能称为平台 |
| `resource_limit` | 命中具名资源边界，或回调资源判定失败；不能称为平台 |
| `work_budget` / `time_budget` | 调用量/时间不足；保留成本并报扫描失败，不运行目标 |
| `measurement_failed` / `unconfirmed_callback` | 测量失败或责任未结清；停止扩展，不自动重试/运行目标 |

每点使用重复耗时的中位数，以及观测到的最小/最大值作为噪声包络。
相对先前最佳点计算 `1 - new_min / best_max`，连续低收益才确认局部平台；
宽波动无法确认平台时继续测到明确限制。该规则是可配置的工程判据，
不是置信区间、全局最优证明，也不排除更远处出现第二个性能峰。
容量/资源边界优先于平台；即使最后一点很平，也不把撞边界标成平台。

选择仍复用原中位数/near-peak 规则：选达到已测峰值指定比例的最小配置。
严重回退不继续探更大点；已知测量失败不会被当作健康回退。
预算不够下一完整点时不会启动半个点；回调自身仍须遵守截止时间和清理合同。
失败报告保留已完成/未知工作及停止原因，不发布成功缓存。

## 缓存与容量边界

缓存绑定平台、模型、生成配置、执行配置、工作负载和测量口径，以及搜索规则、具名边界、
起点、增长倍数、重复数和 near-peak 比例。命中零测量，保留历史扫描成本和停止原因。
实际模型 `EngineSpec.max_num_seqs` 已由 Core 工作负载 adapter 写入执行身份；
更换此值须新身份/缓存和新模型 owner，不能在现有引擎上只改记录字段。

Core 的 `evaluate(..., max_inflight=C)` 可在同一 async owner 内逐查询使用原 SessionEngine
额度；`C` 不超过该 owner 的声明容量。上一查询必须结束且归属结清。
prepare、AsyncLLM、IPC 及 actor 上限仍由 owner 构造时固定，查询不会扩容这些对象。
因此一个预留上界足够的 owner 可顺序探测 C1/2/4/8/16/...，最后执行所选 C，
无需每点加载模型；此 owner 上界与原生模型容量必须如实记录，不能把 API 在途等同 GPU kernel 并发。

维护 Movie CLI 的 `calibration.search` 只转交同一公共策略，并提前展开有限预算/身份槽。
现有 CLI 每个受监督 sample 仍各建 owner；跨点保留模型使用普通公共回调和上述 query 额度能力，
不是已经修改 CLI 的隔离生命周期。两种测量口径/启动成本不能混用缓存或比较。

CPU 合同见 `code/tests/scheduling/test_runtime_plateau_search.py`、
`code/tests/execution_provider/test_core_query_capacity.py` 和维护 CLI 专属回归。
CPU 假耗时只验证控制流/选参，真实 CPU 子进程只验证输入、容量、身份、取消和关闭，
不认证 vLLM/GPU 性能、峰值资源或新的最优并发。

## 普通查询运行策略

`run_query` 是复用上述能力的公共策略接点，不新增调度器或模型执行器。
默认 `mode="query"` 只查精确缓存；命中应用其参数，未命中直接执行调用者明确提供的 `default`。
即使传入 measure callback，普通查询模式也不会调用它。缓存不可读/损坏时不采用其参数，
返回 `unusable_selection` 原因并按明确默认配置执行；业务异常仍原样传播。
低层 Core 工厂/evaluate 继续是显式配置执行，不能把它们说成暗自做了扫描或缓存决策。

```python
from src.scheduling.runtime.calibration import run_query, ScanBudget

# 普通查询：缓存参数或明确默认参数；target 总会真实执行。
result = run_query(identity, candidates, execute,
    default=default_configuration, cache_dir=cache_dir)

# 显式离线：只校准/填参数缓存，不执行最终目标查询。
selection = run_query(identity, candidates, mode="offline",
    default=default_configuration, cache_dir=cache_dir,
    measure=measure, budget=ScanBudget(scan_seconds, scan_work),
    work_per_sample=full_query_work)

# 显式有界在线：扫描和最终一次查询共用声明的时间/工作范围。
result = run_query(identity, candidates, execute_online, mode="online",
    default=default_configuration, cache_dir=cache_dir,
    measure=measure, budget=ScanBudget(request_seconds, remaining_work),
    work_per_sample=full_query_work, target_work=full_query_work,
    request_started_monotonic=caller_request_start)
```

在线 target 接收 `execute_online(configuration, *, deadline_monotonic, work_limit)`，
measure 接收原 deadline/work_limit。工作预算先留出 target 的数量，只将余量交给原 calibrator。
缓存命中不用探索额度但仍执行模型；只有 target 额度且缓存缺失时返回 needs_scan，不暗改成离线长测。
在线任一点失败/unknown/资源检查未通过均停止后续探测，不在可能仍有借用时重跑默认查询。
已知用量、原异常和迟到/超时返回的完整结果保留。调用者须用原资源/取消/监督设施遵守 deadline/work_limit；
这里是协作式界限和调用前后检查，不是新的硬墙钟监控器。

`total_wall_s` 是策略入口至目标回调返回，包含缓存查找、实际调优回调（及回调内开销）、选择和目标；
不是只统计模型时间。提供同一 monotonic clock 的请求起点才有 `request_wait_s`，
它还包含外层已发生的启动/暖机；不提供时为 null，不能把外部耗时伪造为零。
在线 deadline 也从该请求起点算，已花的时间不可重置。工作预算是本次测量与目标的剩余额度，
外层已经完成的暖机调用须由原账本计入总额并从剩余工作预算扣除，不能从时间戳猜出调用数。
外层最终 owner 关闭和真实客户端返回，仍按原完整会话/outer 边界单列。

精确缓存沿用原六组 identity 和完整候选/搜索规则。调用者必须绑定真实平台资源、模型推理配置、
输入字节/方法指令/准备方式及完整查询计时口径；现有 Movie adapter 已冻结相应任务/输入/源码信息。
不会只凭同一硬件就泛化到另一个任务，也没有新增相似度匹配或自动漂移控制。
`performance_tuned` 及参数来源在策略报告中；Core 工厂原字段只表示其自身显式容量声明，
不会因外层选参就伪称该工厂曾主动测量硬件。
# One-pass admission reuse

`run_admission_query(identity, execute, initial_window=..., maximum=..., cache_dir=...)`
uses the same exact six-group identity and atomic cache writer as calibration.
It invokes `execute(window, allow_exploration=...)` once. A matching admission
hint from a measured local stop disables new exploration while retaining pressure protection. A hint
stopped by time/remaining-work limits resumes bounded exploration from the
confirmed value, with `bounded_continuation=True`; a cache hit alone no longer
implies zero exploration. A miss or
explicit `reuse=False` permits bounded one-pass exploration; it does not replay
the query as a scan. Ordinary `run_query` modes remain unchanged.

The callback returns `status`, `reuse_eligible` and `admission_feedback` from the
actual controller. Eligibility means successful complete calls and released
query borrowing; a resident model may remain alive. For Core, pass the selected
window and flag into `owner.evaluate(..., max_inflight=window,
allow_exploration=flag)`, consume once and check its existing call/close receipts.
Each query resets policy state without replacing the model owner.

Hints are named `admission-<identity-and-policy-digest>.json` in the supplied
cache directory. They describe a confirmed same-query window or a successful
initial configuration, never a measured fixed-C full-query benchmark. An
unconfirmed trial is not promoted. A same-value partial hint is updated when
new measurements reach a local stop or record a new incomparable-work cause;
missing feedback cannot certify that
search completed. Other same-value hits retain the original evidence.
An incomparable work mix invalidates that trial, not its safe admission value.
The controller returns to the last confirmed window, discards the old comparison
samples and requires two fresh work-stable windows before another trial. It keeps
the original elapsed/change budgets; persistent changes or a short tail never add
work or wait for extra samples. A trial whose work is incomparable cannot certify
a gain, regression or optimum. The hint retains `incomplete_reason` and
`invalidated_comparisons`; a later matching query may continue within its own
declared budget. This does not guarantee that a variable workload will yield a
comparable local trial or beat an explicitly calibrated fixed configuration.
Strategy `onepass_performance_v3` isolates this policy from earlier hints.
Invalid optional cache data uses the explicit initial value
once; business failure never triggers a retry. Do not include query run IDs in
the workload identity; do include actual source/instruction/sink, model,
hardware, engine capacity and effective policy. Changed conditions get a new
key. There is no automatic similarity grouping or drift detector.

`request_started_monotonic` supplies a same-clock client submission anchor when
owner startup/queueing precede this call. Without it those costs are unobserved.
`total_wall_s` already includes online exploration; do not add that subset again.
Historical hint/calibration evidence is provenance, not a new scan charge.
The caller records answer readiness separately from its complete API-return interval.

## Optional geometric one-pass policy

The independent CPU candidate adds `ResourceFeedbackConfig(search_mode="geometric")`.
The default `local` policy remains reproducible; no global default is changed.
Pass the same effective options to the Core admission profile and
`run_admission_query(..., options=options)`. Its callback accepts
`execute(window, *, allow_exploration, search_hint)` and forwards all three to
`owner.evaluate`. Explicit `allow_exploration=False` always disables trials,
including on a cache miss; pressure protection remains available.

The geometric points double the configured origin and include the declared
capacity endpoint. A regression or flat neighboring point does not exclude
untested larger points. Three valid delivery windows precede a comparison;
the rate is total completed records divided by actual elapsed time, not the
median of rates. Observed min/max bands are descriptive local evidence.
`work_tolerance` is distinct from `min_gain`; equal mean work does not establish
equal request-length distributions or causal comparability. Integer bisection
and global-optimum claims are deliberately absent.

Exact hints store only finite point attempts/outcomes and the confirmed window,
not old rates. Unvisited points run before unresolved retries. Each point is
attempted at most once per query and `max_candidate_attempts` times per exact
identity (default two). The next query builds a fresh reference. Noise and
incomparable work remain unresolved; exhaustion is explicitly censored and
`can_continue=False`, never a proved plateau. `sampled_range_complete` describes
only the finite tested points. Pressure can limit further points; no in-flight
requests are cancelled for admission reduction.

Time, remaining-work and change budgets still apply, including rollback.
Short queries and tails never wait for extra samples or repeat processed rows.
The cache key binds all effective options and declared capacity, alongside the
six identity groups; new source, input, method or performance conditions require
their real identity. There is no cross-workload generalization or drift control.
The old v3 hints cannot be used as geometric completion evidence.

The primary performance measure is actual submission through full API return,
including this query's trials, continuation, result processing and necessary
cleanup. Answer-ready and chosen-window execution-only spans are diagnostics.
Online exploration is already inside the return interval; never subtract it or
add it twice. External offline acquisition is reported separately and counted
once in cumulative from-zero cost, never once per cache hit. Report first,
subsequent and cumulative costs. CPU checks establish data/control behavior,
not a full-query speedup; real-model performance and natural pressure recovery
remain unverified.
