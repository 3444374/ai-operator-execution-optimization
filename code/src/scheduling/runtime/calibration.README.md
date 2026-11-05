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


<a id="resource-request-public-current"></a>
## 当前资源请求接纳

普通 Core 与 rolling token 的 `admission.kind="resource_feedback"` 使用
`ResourceRequestConfig` 和 `ResourceRequestController`，省略 admission 仍走固定路径。
当前可直接解析的配置如下；启用后从一个请求开始，不接受跨查询 `search_hint` 或 `admission_cache`：

```json
{
  "mode": "async",
  "max_inflight": 1,
  "admission": {
    "kind": "resource_feedback",
    "poll_interval_s": 0.25,
    "window_s": 1.0,
    "max_sample_age_s": 1.0,
    "min_samples": 16,
    "measurement_windows": 3,
    "min_gain": 0.03,
    "allow_exploration": true
  }
}
```

这些采样与比较值是工程缺省值，不表示硬件容量或已验证的性能选择。
请求许可只来自实际 owner／Core／IPC／结果预留；未消费结果、unknown、ACK 与取消责任保持。
vLLM ready 只声明实例归属，原生序列／token／上下文参数另由引擎解析；未知值不作为无限容量。
普通默认 R 仍是调用方声明值，没有自动探测硬件额度。公共 async Filter／rolling 工厂的
默认存储现从已有 IPC 字节及单项声明推导，低层直接构造和非 rolling 整批仍保留旧解释。
新校准身份 `records_capacity_policy_v2` 记录实际资源值，不复用旧的 C／R 派生身份。
`require_memory_feedback=true` 现显式使用模型 actor 可见 cgroup v2 的内存停顿／回收事件，缺省仍为 false。
首次读取、缺项、来源变化和不支持的环境保持未知，不据此增长；原生拒绝计数未知时不捏造零。
此反馈不发放 RAM、不限制 RSS，也不保证避免所有分配错误；初始一个请求和原已验证窗口仍可能继续。
驱动、Ray 和可见层级以外的模型进程内存仍需分别管理，不能据此宣称自动容量已完成。
当前策略不接受旧 KV／busy 百分比、`search_mode` 或 `search_policy` 选项，也不保存跨查询推荐。
准确 CPU 与实机未测范围见[接线记录](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#公共工厂与实际资源许可接线)。

以下单遍、压力模式和条件续扫说明只属于显式 `legacy_resource_feedback`、具名历史配置
或直接传入旧 typed 配置对象的路径；它们的数值与历史复现材料继续保留。

## 显式历史单遍接纳与参数提示复用

`run_admission_query` 保留已交付工作量扫描（原 cfdc/79e8），并接回计算工作量／局部配对版本（原 ee39/5007）的完整反馈与 hint 状态。
它与上述 `run_query(mode="query"/"offline"/"online")` 并存：后者调用完整工作负载测量回调，
前者在**本来要完成的查询请求**上调整 admission，不另做一遍查询。历史源码从冻结投影还原；
维护版现在默认不设内部探索期限、剩余条数门槛或变更次数预算，比较工作量与原请求语义保留。
默认 Core 仍固定；没有 `profile.admission` 就没有新增反馈采样任务。

### 原实验配置选项

`create_records_core(..., profile=name)`、普通 Movie 的 `core_profile` 和 session 的 `profile`
均接受以下名称；显式字典配置仍可使用，普通 sync/fixed 默认不变。
两个名称保留 **legacy 实验参数对照**；它们不是新研究
配置的推荐，也不证明这些数值得到资源平台验证。下方字典只选择相同的历史策略。

| 名称 | 对应历史实现 | 工作量与比较方式 |
|---|---|---|
| `delivered-work-scan` | cfdc，实际冻结源码 `79e8f103` | 已交付请求与输入／输出工作量；原范围比较 |
| `computed-work-local-pairs` | ee39，实际冻结源码 `5007e95b` | 实际计算／缓存 token；原局部配对和状态重验证 |

两项都展开为历史正式配置：async、起始8、反馈读取0.1秒、测量窗0.25秒、最少16个样本、
剩余至少64请求、10秒探索上限、最多8次调整、增益及工作量容差各3%、几何搜索、候选最多2次尝试，
并开启原阶段计时。当前 owner 上界来自调用方 request budget，与引擎每轮序列上限分开；复现原实验须显式声明两处容量64、
CPU准备2、prepare/model batch均1，以及相同模型、生成、输入和缓存条件。名称不修改这些外部声明。
未指定名称时，`ResourceFeedbackConfig` 默认 `exploration_s=None, min_remaining=0,
max_changes=None`，不按10秒、32／64条或8次变更停止。采样与比较参数仍是未调优的工程候选。

```python
profile = {"mode": "async", "max_inflight": C0,
           "admission": {"kind": "legacy_resource_feedback", "allow_exploration": True,
                         "search_mode": "geometric"}}
```

现有 `exploration_s`／`max_changes` 可显式给正值；`min_remaining>0` 显式启用旧尾段与
未来收益摊销门槛。它们仅作为 legacy／预算对照，触发表示探索截断，不能作为平台证据。
缺少 pending 反馈时保持未知；没有可提交工作或查询结束、样本不足时不补调用，不等待额外样本。
`exploration_censored`／`incomplete_reason` 保留证据不足，包括从未开启 probe 的短查询。
实验外部 wall／call 预算、取消、实际请求／字节额度、结果槽位与 unknown 责任始终独立保留。

原首次／复用条件为同一模型依次执行16条预热、完整首次和完整复用；预热不读写 hint，
首次结束满足完整成功与关闭责任后才保存，复用须显式配置下述 `admission_cache`。
模型常驻本身不等于配置 hint 已保存。历史实现与原样恢复入口见[具名来源](../../../../experiments/reproduction/native_baselines/README.md)。
两项可选实现均已接main，已有原参数GPU单轮首次／复用；当前接纳与结果边界见[代码状态](../../../INFRA_STATUS.md#优化与观测进展2026-10-27)。
这些原参数GPU观察仍绑定各自固定源码，不转授本次容量／截止规则纠偏的GPU资格。
两版本的固定源码和负结果保留；CPU差分验证显式旧参数及保留比较逻辑，不再要求维护配置／控制器AST与旧版相同。
当前单轮、旧短查询提示复用与历史源码n3分别保留，不相加作重复数或宣称稳定收益；CPU差分不替代真实模型验收，也不能按期望成绩修改参数或结果。

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
`control_seconds` 是 `ResourceFeedbackController.update()` 入口至返回／异常退出的累计
**控制器函数墙钟耗时**，由其单调时钟差得到；它不是 CPU 时间，也不是全部控制链路成本。
外层供给门控、采样、RPC、序列化及其他进程的工作不在该字段中；相关成本仍由完整查询计时承担。
`exploration_wall_s` 是试探状态持续的墙钟区间，其中仍在完成原查询工作，不能当作纯等待或
可直接扣除的损失，也不能从完整查询耗时减去它后声称得到了实测稳态性能。
`admission_selection.entry_work_wall_s` 覆盖绑定到完整回调和缓存写入、止于 payload 返回前；
真实调用方返回、模型启动/暖机/最终关闭由原生命周期计时单列。历史扫描费只记一次。
缓存写入是查询局部资格，不证明物理 GPU/后代退出；unknown、held、借用未释放或账本不完整
不会发布新 hint。原冻结来源的功能实测与当前代码整合分别判断，见[证据台账](../../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621)；短查询提示复用不证明反馈调节或搜索收益。

<a id="pressure-sample-response"></a>
### 压力采样与吞吐测量的时钟

`ResourceFeedbackConfig.pressure_response="sample"`使明确的抢占计数增量、
`memory_pressure=True`在新鲜采样到达后先处理，不等待`window_s`的吞吐测量。
同一个provider接纳的采样带单调递增`sample_sequence`；重复或倒序读取不再次调整额度。
直接调用且不提供该序号时，用采样时刻去重。缺失或过期反馈不作为新压力或资源充裕的依据。
KV占用的两窗确认和性能比较仍走原测量流程；单个高KV读数不变成即时抢占事件。

采样响应模式在停止性能探索后仍按`poll_interval_s`读取，复用既有采样器和单个在途snapshot RPC。
`pressure_response="window"`保留原窗口响应及保护采样节奏；
`delivered-work-scan`、`computed-work-local-pairs`两个具名历史配置显式选择此模式。
压力缩小的是后续接纳额度，已经提交的请求和其字节／结果责任继续保留到原确认流程结束。

rolling目前只有原生queue、KV与抢占信号，尚未接入可用内存的保护源。
因此`memory_floor_bytes>0`在profile解析和直接owner配置时明确拒绝，不将`None`解释为内存充裕。
普通非rolling的现有设备内存读取未改接到此路径。
以上为源码修复与CPU行为验证；真实Ray、模型及完整查询性能尚未验证。

### rolling 请求的上探与条件续扫

显式历史 rolling `admission.kind="legacy_resource_feedback"` 使用
[`AscendingRequestController`](../submission_control/request_scan.py)；解析后的
`search_policy="ascending"` 区分新策略与 `search_policy="reference"` 的旧回访策略。
此段的 reference／ascending 仅属于历史策略；当前请求策略使用上方配置，默认固定路径不安装反馈。
新策略复用现有需求／字节背压、新鲜 native 反馈、窗口采集、工作量配对和几何进度；
有可比收益就在候选档确认并继续上探，不逐档强制 A-B-A，也不在接受后重跑整段参考测量。
平台、回退或不可比／冲突证据才触发局部确认；真实压力仍收缩新请求额度，让已归属成员排空。
近峰选择复用 `select_near_peak`，只纳入有独立等工作量配对的已测档位，不能推断未测档位或全局最优。

resident 的新策略把模型寿命与提示状态分开：完整 query 消费、解释、结果投影成功，且调用、
source、Core credits 和 snapshot RPC 全部结清后，Movie Session 才调用
`PreparedRecordsCore.confirm_query_feedback(completed=True)` 提交内存提示。
直接公共调用也须在其完整结果处理后明确确认；模型 EOF 只暂存，未确认、失败、取消或 unknown
不发布新状态。空查询不以未测默认值覆盖健康提示。
提示绑定实际 owner engine UID、模型／生成／方法、B、原生容量、Core／IPC 边界和完整策略选项；
新 query ID 不单独使其失效。下一 query 在已确认档位测本次工作，未完搜索条件续扫；
已完成选择经新鲜验证后保持并监测；工作、可比性能条件或新资源机会改变才局部再探。
提示可保留明确标为`invalidation_only_not_a_current_query_score`的旧速率区间，只用于判断条件失效；
保持选择期间，失效判据锚定最近一次验证的工作／速率范围，不随每次重叠的小变化滚动替换，避免累计变化被逐次吞掉。
新查询的比较参考、候选评分及结果账本全部重新测量。旧提示没有条件证据时不能直接宣布旧平台已验证。
阶段关闭的 `can_continue=false` 与 `search_can_continue_next_query` 分开，旧阶段永不重开。
此提示不是磁盘 `admission_cache`，也不是跨 owner／跨任务相似度学习。

资源现在直接影响可回退的试探，不以精确请求成本模型为前提。
[`resource_probe_opportunity`](../submission_control/resource_probe.py)联合实际 ready/backlog、满额在途、
本地字节／传输背压和新鲜 native queue／KV／抢占状态，识别普通试探机会；明确的压力优先，
即使额度已为1也会清除旧平台，恢复后重新测量。原性能窗口、显式尾部／时间／尝试预算仍生效，
不增加10秒、64请求等默认停止规则；资源提案与普通候选使用同一资格检查。

ascending 的唯一 snapshot RPC 可附带读取模型 actor 分配设备的 NVML 与其 CPU 分配范围。
默认固定、reference 及探索关闭路径不调用物理读取器，没有新采样线程／模型进程或HTTP探针。
设备优先采用同一模型 worker 的具名只读 UUID 回执；缺失时仅走已有安全 CUDA visibility 映射，不能证明则缺测。序号／engine／session、driver issue→receive包络及
actor内采样耗时用于校验来源和新鲜度，无需假定两个主机的单调时钟绝对值相同。
CPU计数的累计差分基线由采样器实例保存；实例首样本建立基线，后续RPC线程共享该基线，过长／未知测量区间保持unknown。
cgroup可用时按有效CPU配额归一，非cgroup时取分配affinity范围的核峰值；不是全机余量或单进程CPU占用。
CPU测量区间和RPC数据年龄分别受限，不声称整个历史取样区间都落在最后一个性能窗口内。
当前owner是一张GPU；多个设备的均值不能证明每卡空闲，因此不据其均值加大步幅。

当最近两次不同RPC都显示设备及上述CPU分配范围**空闲时间占多数**，同时上述供给／native条件成立时，
用两次较忙的设备／CPU值与native KV占用得到联合空闲份额
`s=min(1-GPU_busy/100, 1-host_peak_CPU_busy/100, 1-KV_usage)`，在有供给的额度区间内取点：
`target=C+max(1,floor((min(R,C+ready_items)-C)*s))`。
`host_peak_CPU_busy`沿用旧名称，实际取上述cgroup／affinity读数；字段名不能解释成全机CPU余量。
这是可回退的**探测位置启发式**，不是把空闲份额换算成可持续请求容量，也不使用`1/GPU_busy`。
余量程度直接改变候选，允许跨多个几何档或插入非几何点；R和真实ready工作夹住提案，
已有pressure ceiling／尝试预算／字节边界仍优先。普通几何探测继续作为无合格物理证据时的回退。
50%是这条加速提案的工程资格判据，不是硬件平台或总体探索停止线；新目标仍须有可比收益才能保留。
两次读取的NVML内部采样区间可能重叠，不称统计独立。物理值缺失或单独GPU busy低不生成此大步目标。

<a id="physical-probe-policy"></a>
`physical_probe_policy="idle_majority"`保留上述双50%判据；当前默认未改。
显式rolling／ascending配置可选`physical_probe_policy="native_only"`，只关闭物理读数产生的大步目标，
继续使用原生反馈与普通几何候选。此比较候选默认关闭，没有新增利用率阈值。
请求上限、KV／抢占／内存压力、传输与消费背压、真实ready供给及工作量比较仍先检查；
没有已测参考时不会仅因资源信号就增加额度。可用的原生KV成本提案仍按原流程处理。
采样通道及资源读数的归属未因这个选项改变，配置进入原有提示身份，不能复用不同策略的查询提示。
此选项仅有CPU反例与adapter接线检查；尚无真实资源或完整查询性能结论。

新机会可唤醒旧选择；相同资源状态的反复轮询不自动反复扫描。
未知／弱覆盖不会清掉上次合格物理条件；同设备联合空闲份额相对其已测试条件超过`min_gain`才触发新的余量条件，
沿用该相对变化容差避免微小波动反复重扫，不把它解释为资源收益预测。
真实观测到忙→闲可再次局部试探。物理条件按其实际请求额度建立监测基准；控制动作改变额度本身不当成外部资源变化，
也不拿旧小额度下更高的空闲份额永久遮住当前额度的余量改善。
未完成提示同样核对工作量及速率变化，native间断不抹掉待验证的旧工作量条件；
失效评分不退还已消耗的候选尝试次数。资源有余量但实测吞吐无收益时保留较小已测档位，
不以“填满利用率”替代查询目标，瓶颈转移须结合各阶段等待分析。

供给条件另记**实际已试探范围**：`ready_items`、已试额度`window`和该次可用区间上界`range_end`。
只在候选真正执行、且实际ready可覆盖该候选时记账；未发出的提案不成为已测证据。
同物理条件下，ready数量和区间上界都超过此前试探记录，且有超出已试额度的新合法目标时，才局部向上复探；
原条件高水位随健康hint保存。C自身变化、ready回落后恢复旧范围、未知覆盖或R已夹满后的数量增长不自动触发重扫。
工作／速率条件失效作废该条件范围，保留已用候选尝试；真实native压力作废旧条件范围，
范围恢复沿用原有压力保护逻辑。已记录的探索时间、控制器墙钟和额度变更历史均保留。
这不是供给预测或输入全表统计；显式尝试／变更预算仍优先，已花费用不因唤醒而退还。

状态由现有参考、在试候选、局部确认和已保持选择推导，不新增执行器或另一套策略框架。
导入hint和同query native间断都进入“参考待验证”；后一种在清空统计窗口前保存已确认额度的条件，
恢复后与已保持平台共用`_resume_reference`，不因查询边界或参考是否暂缺而跳过局部下探。
监测中的选择（含候选证据耗尽）与本query的探索暂停分开：native间断不会把前者变成永久禁止测量。
时间／变更等明确限制仍能截断参考验证；尚未验证的参考不能导出hint，未知native也不变成有效资源。

局部下探目标在资格／预算检查**之前**登记，只表示未完成的验证意图，不附带旧评分或已执行次数。
首次拒绝、在试／确认期间截断、native间断都保留它；合法的比较完成，或新工作条件／真实压力替代它时才清除。
下一健康query重新测量安全参考，重置本query的时间／变更预算后继续下探；候选尝试次数沿原进度保留，不能借query边界退款。
若尝试额度仍不允许重试，目标继续显式阻塞，而不被误报成已完成的平台验证。

报告的`query_exploration_pause`只表示暂停搜索，业务query仍执行；`continuation_state`区分
`query_paused`、`reference_pending`、`local_probe_blocked`、`local_probe_pending`、`monitoring`和`searching`。
`local_probe_blocked_by`说明预算／候选资格等阻塞；`can_continue`保留搜索进度是否尚有候选的含义，
不能用它代替当前暂停／未验证状态，阶段关闭时仍另行禁止继续执行。

| 当前状态／事件 | 新鲜证据及动作 | 后续状态与保留内容 |
| --- | --- | --- |
| 冷启动、无合格提示 | 从1采集原始有效工作 | 参考测量；没有捏造旧速率 |
| 导入健康hint | 在已确认额度验证当前工作／速率 | 不变则保持旧选择或续未完候选；未完局部下探保留目标方向，重新测量，不携带旧评分 |
| native间断 | 中断未确认比较，回到已确认额度；保存其工作／速率条件及待续下探方向 | 参考待验证；保留供给范围及花费，不拿缺帧当空闲 |
| 候选证据耗尽后native间断 | 无真实query预算暂停时，仍收集新鲜安全参考 | 恢复监测；新合法目标按原尝试额度验证，不把耗尽当永久终态 |
| 参考待验证或保持选择，工作／速率改变 | 清除旧评分，采用新鲜参考并局部向下探；C=1则按合法候选向上 | 测量／试探；同query、跨query使用同一转换 |
| 相同工作／速率，新资源条件或ready范围扩展 | 按来源重开合法上探；供给扩展只尝试超出已试范围的目标 | 试探；不退款、不重试已耗尽档位 |
| 相同条件、弱／未知物理覆盖 | 继续监测；真实忙→闲和足够的累计余量变化仍可唤醒 | 保持选择；未知不擦掉历史合格条件 |
| 在试候选有可比收益 | 接受并继续上探 | 新确认额度；不机械回访A |
| 在试候选平台／退化／冲突 | 原有局部确认、回退或未决处理 | 仅用已测可比档位选择近峰较小值；不宣布未知平台 |
| 下探在首次执行／比较／确认时被预算截断 | 保留目标和已花费次数；已验证参考可随健康hint保存方向，未验证则hint保持缺失 | 暂停本query探索；下一query先验证参考，再按新的query预算续做，旧评分不迁移 |
| native压力 | 原有缩容及排空；旧额度速率不套到缩容后的额度 | 安全额度重新测量；所有权和显式预算仍约束 |
| 完整query解释／投影和责任结清 | 显式确认后保存选择、未完进度及供给范围 | 下query先验证；失败／取消／unknown／未确认不提交提示 |

[`propose_kv_window`](../submission_control/resource_step.py)仍可在布局／时钟／完整已准备成本已证实时
提供更具体的前缀建议；现有backend没有这些完整证明时只标该成本估计不可用，不阻断资源观测触发试探。
设备空闲显存不等于可用KV；CPU／显存的精确请求成本、MFU／HBM和硬件性能极限仍未实现或测定。

这条请求反馈只是异构资源调度的一个接点，不能将全部资源压成GPU利用率。
现有[`StageWork`／`WorkDescriptor`／`RuntimeStateSnapshot`](../../planning/work.py)可按阶段描述不同单位的需求、
活动／排队工作和服务率；本路径复用`SupplyState.ready_work`及`local_capacity`，随每次资源提案保留其原单位和阶段。
它们没有校准成CPU核时、设备字节或链路带宽，未知需求不能当零需求。

| 阶段／资源 | 当前决策输入及作用 | 仍缺的证据或动作 |
| --- | --- | --- |
| CPU准备、结果消费 | ready/backlog、准备／完成率、consumer背压；同actor的cgroup／affinity CPU读数参与大步资格 | driver异机CPU、逐阶段核时；不调CPU worker/放置 |
| 模型执行与设备池 | UUID-bound busy及native queue/KV/抢占；试探与压力回退 | MFU/HBM、算子物理成本；不迁移设备或拆分引擎阶段 |
| host／device驻留 | Core行／input/result字节、IPC配额和native KV保护 | 物理可分配容量未知；不把NVML空闲显存换成请求数 |
| IPC／数据搬运／I/O | pending transport admission、已有存储／字节背压阻止放大 | PCIe／网络／磁盘饱和度和阶段传输成本未知；不调链路或I/O并发 |

GPU空闲但准备无供给、CPU忙、传输或消费被阻塞时，不据GPU单项放大请求。
尚未观测到的I/O瓶颈仍可能存在；本启发式只作有界试探，由实际吞吐、等待及压力检验，
不声称已实现DAG跨阶段重叠／分离或全异构资源编排，也不新增并行资源框架。

以上是 CPU 待复审候选；可控资源数据经同一RPC实际改变Core试探额度，真实硬件采样／Ray／模型和性能待验。
旧 `0585e9bf` 同条件 off 配对中，
复用 Q1 固定 RF 为 17.275 秒、旧动态 RD 为 74.621 秒；这是单配对负结果，不能改标新策略成绩。
模拟／CPU 轨迹只验证状态机，不能从旧回访区间相减后报告提速。


维护矩阵的 `base-config.json.shared_resources` 可显式提供本轮已封存的共同资源上限
（`cpu_affinity_slots`、`max_sampled_owned_rss_bytes`、`max_owned_shm_bytes`，及原 host
monitor 所需字段）。未提供时维持旧 source comparison 文件合同；该文件在当前 main 中已不存在，
因此新封包须显式提供，不能搜索历史目录或采用机器相关默认值。原资源监护和责任判断不变。
`run_movie_records_matrix.py` 是唯一维护的 worker/audit CLI；重复别名
`run_sembench_movie_matrix.py` 已退役。历史命令仍可按原提交恢复，新运行使用上述唯一入口。
