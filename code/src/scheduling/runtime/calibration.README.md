# 有界递增运行时搜索

`calibration.calibrate/run_calibrated` 仍是唯一测量、选择和缓存入口。
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


## 可选单遍动态接纳与参数提示复用

`run_admission_query` 保留已交付工作量扫描（原 cfdc/79e8），并接回计算工作量／局部配对版本（原 ee39/5007）的完整反馈与 hint 状态。
它与上述 `run_query(mode="query"/"offline"/"online")` 并存：后者调用完整工作负载测量回调，
前者在**本来要完成的查询请求**上调整 admission，不另做一遍查询。此次接回不修改历史选参算法。
默认 Core 仍固定；没有 `profile.admission` 就没有新增反馈采样任务。

### 原实验配置选项

`create_records_core(..., profile=name)`、普通 Movie 的 `core_profile` 和 session 的 `profile`
均接受以下名称；显式字典配置仍可使用，普通 sync/fixed 默认不变。

| 名称 | 对应历史实现 | 工作量与比较方式 |
|---|---|---|
| `delivered-work-scan` | cfdc，实际冻结源码 `79e8f103` | 已交付请求与输入／输出工作量；原范围比较 |
| `computed-work-local-pairs` | ee39，实际冻结源码 `5007e95b` | 实际计算／缓存 token；原局部配对和状态重验证 |

两项都展开为历史正式配置：async、起始8、反馈读取0.1秒、测量窗0.25秒、最少16个样本、
剩余至少64请求、10秒探索上限、最多8次调整、增益及工作量容差各3%、几何搜索、候选最多2次尝试，
并开启原阶段计时。owner 上界仍来自调用方资源与模型容量；复现原实验须显式声明容量64、
CPU准备2、prepare/model batch均1，以及相同模型、生成、输入和缓存条件。名称不修改这些外部声明。
未指定名称的 `ResourceFeedbackConfig` 原默认值没有改成这些实验参数。

原首次／复用条件为同一模型依次执行16条预热、完整首次和完整复用；预热不读写 hint，
首次结束满足完整成功与关闭责任后才保存，复用须显式配置下述 `admission_cache`。
模型常驻本身不等于配置 hint 已保存。历史实现与原样恢复入口见[具名来源](../../../../experiments/reproduction/native_baselines/README.md)。
两版本的固定源码和负结果保留；CPU差分不能替代同条件真实模型验收，不能按期望成绩修改参数或结果。

普通公共调用先以 `create_records_core(..., profile=profile)` 建立唯一 async owner，
再将完整查询消费/解析/汇总和借用释放放进 `run_admission_query` 的回调；
回调中的 `owner.evaluate(..., max_inflight=window, allow_exploration=..., search_hint=...)`
使用同一模型。必须消费 iterator 并核对原关闭/调用账本后才报告 `reuse_eligible=True`，
不能以创建 lazy iterator 代替查询完成。公共层不依赖 Movie、gold 或 experiments。

`ResourceFeedbackConfig` 是 owner 与缓存 policy 的同一份选项。显式
`allow_exploration=True, search_mode="geometric"` 才启用原几何性能试探；
`allow_exploration=False` 可仅保留资源压力保护。缺失/过期 native 反馈不当作零压力。
模型 `max_num_seqs` 和 owner 上界在启动前声明，控制器只调整运行时 admission，
不热改模型容量，不把容量边界叫最优，不取消已经在途的请求来缩窗。

计算型选项使用原 `work_basis="computed", work_comparison="local_pairs"`。
每次真实 RequestOutput 的缓存计数有效且无KV传输时才得到 `prompt_computed_tokens`／`prompt_cached_tokens`；
缺失或非法保持未知，不能假设缓存为0。原交付前缀、工作量匹配、压力后重验证与有限续探一起保留。

Movie 普通 JSON 使用 `core_profile`，session/维护 plan 使用 `profile`；可选
`admission_cache={"identity": six_group_identity, "cache_dir": absolute_directory, "reuse": true}`
只负责薄接线到上述公共 API。identity 仍要求 hardware/model/generation/execution/workload/measurement
六组及 `measurement.work_unit`。调用者须提供真实硬件、权重/tokenizer/软件版本和计时口径；
适配层额外绑定实际 EngineSpec、generation、resources/profile、source manifest SHA 和 method。
交付型沿用该输入身份；计算型缓存另须显式提供 `workload_features`，其结构为
`{"work_unit":"tokens","input_work":{...},"output_work":{...},"cache":{"enabled":true,"scope":"..."}}`。
调用方提供有来源的数值特征并计入获取成本；原 `admission_identity` 用这些精确特征替换 workload 组，
其他五组身份保留。缓存命中后仍用本次实际计算工作量重验证，不复用历史速率。
更换 query ID 不自动失效，不做相似度匹配；旧源码或不同策略的磁盘 hint 不自动迁移。
预热不读取/写入 hint 且不探索。成功短任务可复用安全初值，但不因此称其经过调优。
未结束的搜索 hint 按原有限预算继续，不能把未决状态当全局最佳。

单遍探索的请求正常贡献输出，探索耗时已在 `total_wall_s` / caller 返回中，不能再相加。
`admission_selection.entry_work_wall_s` 覆盖绑定到完整回调和缓存写入、止于 payload 返回前；
真实调用方返回、模型启动/暖机/最终关闭由原生命周期计时单列。历史扫描费只记一次。
缓存写入是查询局部资格，不证明物理 GPU/后代退出；unknown、held、借用未释放或账本不完整
不会发布新 hint。原冻结来源的功能实测与当前代码整合分别判断，见[证据台账](../../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621)；短查询提示复用不证明反馈调节或搜索收益。


维护矩阵的 `base-config.json.shared_resources` 可显式提供本轮已封存的共同资源上限
（`cpu_affinity_slots`、`max_sampled_owned_rss_bytes`、`max_owned_shm_bytes`，及原 host
monitor 所需字段）。未提供时维持旧 source comparison 文件合同；该文件在当前 main 中已不存在，
因此新封包须显式提供，不能搜索历史目录或采用机器相关默认值。原资源监护和责任判断不变。
现有 `run_movie_records_matrix.py` 是唯一 worker/audit 路径，旧 `run_sembench_movie_matrix.py`
仍可转调同一 main；修复了原 main 与绑定检查选用不同脚本名的问题。
