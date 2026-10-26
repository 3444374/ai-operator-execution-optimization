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

## Optional work-aware admission

Use `ResourceFeedbackConfig(search_mode="geometric", work_basis="computed")`
explicitly. The old modes remain available. `run_admission_query` additionally
requires `workload_features` containing `work_unit`, numeric `input_work` and
`output_work` distributions/limits, and `cache={enabled, scope}`. The producer
supplies those cost features; `admission_identity` replaces only the admission
workload group. Task labels, query IDs, source names and exact record counts do
not enter that feature mapping. This does not weaken the separate exact offline
calibration identity, infer workload families, or discover missing features.

The async adapter reads actual vLLM 0.25.1 `RequestOutput.num_cached_tokens` and
reports prompt-minus-cache tokens alongside the unchanged logical usage. Missing
or invalid counts and remote KV-transfer results have no computed-work value.
These are request-associated cost descriptors, not measured FLOPs: attention
length, kernel behavior and preemption can still matter. No MFU/chart hook or
extra per-request RPC is needed. See the pinned
[RequestOutput source](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/outputs.py).

Core accumulates those descriptors only after the existing delivered call-ID
validation. The provider accepts logical/computed/cache totals only for that same
completed prefix. Changed windows still settle existing in-flight work. Trials
compare delivered row rates only when logical input, computed input, cached input
and output work bands match. Incomparable work rolls back to the last safe value;
it is neither measured regression nor pressure. Fresh reference work can reset
outcomes from another work condition, but never the current query's elapsed,
change or once-per-point budgets. Same-condition noise retries stay finite.

Hints retain a work envelope and, when supported, a verified window; no rate or
counter watermark crosses queries. A successful but unmeasured default is tagged
`safe_initial_or_unresolved_progress`, not a tuned winner. Matching completed
hints execute without a new trial. Observed work changes require fresh bounded
validation, starting from the explicit safe origin if a higher cached setting
no longer matches. Short queries/tails do not wait for validation samples.
Prior pressure starts the next query lower and reopens only bounded validation;
it does not become a permanent performance plateau. Historical evidence remains
unchanged. In-flight requests are never cancelled just to reduce admission.

This candidate passed 341 combined CPU tests plus one thin-driver test. Fixtures
replace Ray/Daft/model hardware where stated; Core, formatter and CPU subprocess
transport are real. Actual work alignment, natural pressure, query speed and
payback still need the sole GPU owner's finite validation. All lookup, observation,
trial, useful model execution, result processing, cache writing and necessary
query cleanup stay inside the existing full-return clock. Optional `stage_timing`
also splits `feedback_poll` and `feedback_progress`; these host spans can overlap
other stages and must not be summed as query wall time. About one second in older
runs remains unattributed until corresponding stage receipts are measured.

## Pressure recovery correction

`onepass_work_aware_v2` replaces the first work-aware policy identity. Old v1
hints remain untouched and cannot be accepted as v2 recovery evidence. The
previous CPU suite missed completed-profile pressure recovery; the two original
independent counterexamples remain RED against 9cd and pass in this increment.

Pressure now records the prior/safe windows even for a reused setting, invalidates
higher outcomes for bounded revalidation, and adds a finite doubling path when
the safe value is below the original search origin. It records no invented model
trial. A fresh low-pressure frame can reopen measurement in the same query if
remaining work, original wall/change budget and rollback allowance permit it.
Configured memory protection requires explicitly nonpressured memory feedback.
Missing/stale signals, exhausted budgets, fixed mode and a short tail cannot
restore a high setting. Existing in-flight work is allowed to finish.

Elapsed time, change counts and the per-query visited set are not reset. After a
query, only progress and work descriptors cross the cache; no throughput rate or
counter watermark does. The next query starts from the lower saved setting and
must measure any recovery candidate. Historical pressure evidence is unchanged.
Finite recovery points are stored explicitly, including points below the old
origin; `sampled_range_complete` cannot be reported merely because stale higher
outcomes were once resolved.

Validation reuses the 347 passing combined cases and one thin-driver case. The
subsequent required-memory guard was checked by 61 targeted cases, including one
new case; the union is 348 distinct source tests, not a fresh 348-case full run.
The original independent author's 14 scenarios were replayed unchanged except
for the local source path, all passing. That author replay is not an independent
re-review. GPU/resource-pressure performance remains unverified.


### Unstable-reference trial admission (CPU candidate, work-aware v3)

`work_basis="computed"` now separates permission to try a setting from evidence
for accepting it. Three heterogeneous valid windows can price one adjacent
trial under the existing time/change/work/resource limits. They cannot confirm
a gain or publish a verified setting. The original stability tolerance remains.
An unstable or cache-incomparable candidate returns to the last safe setting.
If B is stable while A was not, one fresh A recheck may use the remaining
original budget; acceptance still requires stable matching work and a measured
gain. A-B-A does not reverse cache warming or establish causal equivalence.

Known old in-flight work is excluded by the existing transition/cooldown path;
no queue drain, request repetition or model restart is added. Measurement points
carry control phase IDs and delivered-prefix ranges. These are driver-observation
phases, not GPU timing or additional semantic stages. Noise after the single
recheck remains unresolved. Pressure recovery, short-tail and fixed modes retain
their existing behavior. The v3 policy key separates older cached progress.

`run_admission_query` still accepts producer-supplied generic workload features.
It does not build an unmetered benchmark token index. A caller constructing these
features must start its full-query clock before that work and pass the original
`request_started_monotonic` anchor. Existing request-construction lengths may be
reused when available; missing initial feature-generation cost stays unknown.
The thin research driver identifies its precomputed input cost as unmeasured.
Normal request work contributes output once; observation, trials, rechecks,
cache updates and necessary cleanup remain in complete API return time. Trial
wall time is a subset, not another amount added to the query.

CPU regressions cover continuously changing work at a non-power capacity, a
real comparable positive case, irreversible cache changes, transition prefixes,
noise, original budgets, pressure, missing signals, short/fixed queries and
production Core with a resident CPU child. Real-model benefit and complete
feature-preparation cost remain unverified. Baseline files and CURRENT are unchanged.


### Local-pair comparison (explicit CPU candidate)

Use `ResourceFeedbackConfig(search_mode="geometric", work_basis="computed",
work_comparison="local_pairs")` in the existing Core admission profile. The
default `envelope` behavior remains available as the legacy control. Policy
identity `onepass_local_pairs_v2` separates cached progress. No new executor,
model lifecycle, baseline path, feedback RPC or per-record decision is added.

At each original progress window, rate is completed records divided by elapsed
time; logical input, uncached input and output are per-record means. Totals must
not stand in for per-record work. A point retains three to six windows with
disjoint delivered-prefix ranges. Pair candidates by work direction and closest
logical input, then uncached input, then output, with chronological ties; never
select pairs using their speed. Each window is used once per comparison.
A candidate must not be lighter on any of these three descriptors to support a
gain. At least two such pairs must all improve and have no opposing evidence.
The reverse relation can support observed regression. One favorable window,
a cache-only lighter workload, overlapping prefixes and mixed rates cannot
certify a winner. No weighted token cost or token-throughput normalization is
fitted. The old 3% workload-equality test is not an acceptance requirement in
this mode; `min_gain` remains an independent configurable engineering margin,
not a statistical guarantee, user SLO or an empirically optimal constant.

Before accepting a candidate, use at most one fresh safe-setting reference
within the same original budget. If before/after references reverse the ranking,
retain uncertainty. The earlier reference is a crosscheck, not another set of
independent candidate samples. A-B-A neither reverses warming nor proves causal
equivalence. Distribution, kernel and unobserved supply changes remain limits
of window means. Do not accept when the available local support is insufficient.

Retain pressure/stale/missing/transition/cancel handling, original elapsed and
change budgets and visited points. Short/tail/fixed inputs do not wait for extra
work. Rates are not cached across queries. Generic feature identity remains
exact; observed descriptor ranges must overlap on reuse, with no Q-name, ID, path
or exact-row-count special cases. Preparation of producer features still costs
time as described above. Full API return includes every observation, candidate,
reference return and result action; nested spans are never added twice.

CPU validation covers production Core/actual CPU socket and 387 source cases,
plus two thin-entry cases. Saved c2 extrema reproduce all three legacy rejection
reasons but cannot reconstruct missing window pairs or a hypothetical winner.
Actual-model benefit remains unverified. The original dynamic policy remains
the main performance control; merely beating 68/c2 is not optimization success.
Historical full-query advantages and API-tail tradeoffs remain recorded.


#### Earlier partial opposition correction (CPU)

An earlier unresolved reference can already contain one valid adverse pair.
That pair must not disappear merely because its phase has fewer than two
matching pairs and therefore no aggregate outcome. Before confirming against
a fresh reference, inspect the earlier opposite-direction pairs as well as
resolved summaries. A contradiction keeps the safe setting and noise/unresolved
progress; it is not promoted to measured regression or a verified winner.
The symmetric gain/regression check uses the existing configured margin. No
window, budget, matching order, threshold or resource handling is changed.

A normal-update regression reproduces the old false C6 acceptance and now keeps
C3. An unresolved earlier reference without adverse evidence still permits
fresh positive support. The new v2 policy separates prior cached acceptance.
112 affected source tests and two thin-entry tests pass. The previous 387-test
run belongs to a38 and did not detect this issue; it is not a full rerun on v2.
Independent re-review and actual-model validation remain pending.
