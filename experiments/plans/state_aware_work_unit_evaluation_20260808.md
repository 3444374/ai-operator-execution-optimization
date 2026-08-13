# Work Unit、状态感知、动态调度与代价估计的实验计划

日期：2026-08-08（2026-08-09 更新：开题两作业 guaranteed-overlap 已完成；
2026-08-11 更新：新增 SAOR 动态调度设计维护入口；2026-08-12 更新：fixed-envelope
2-Job formal 已运行并经 resolution-aware v2 完整 validation；通用有界优先级 v0.5.1 已完成
本地实现与门禁工具；2026-08-13 更新：按 DRF/Pisces/DRFT、Themis/Tiresias/Pollux、
VTC/DLPM 与 SLO-serving 文献收紧多 Job 评价合同，并完成双轮 bounded-priority GPU
development gate；结果未晋级，新增 ready-set observation 修订任务；同日 bounded-ready
$0.125W_e$ 双轮通过开发门，但审核发现 observation 与 selector 归因混杂，formal 前新增
项目内部 matched-observation attribution gate；原生 baseline 不接入 bounded-ready；完整系统
价值另需同一 2-Job 合同下的 native-system matched comparison；同日 matched-observation
selector 双轮与 single-head→bounded-ready FIFO observation bridge 均已完成，SAOR 只形成
观测非支配折中，`formal_authorized=false`）

> **动态调度算法唯一维护入口**：本文 §5.2。当前状态为
> 旧 fixed-envelope formal `formal-valid / not-promoted`；当前 guarded-debt selector
> `development-observed / formal_authorized=false`，不是已完成方法，
> 也不替代 §5 的简单阈值/滞回控制 baseline。后续算法假设、公式、工程映射、实验门禁和
> 结论状态统一在 §5.2 调整，避免散落到报告、代码注释或结果文档中形成不兼容版本。

算子代价估计是下述 organization 与 admission/routing/multi-job 的共同使能部件：
同一 calibration signature 下输出 stage/service/remaining work、SLO slack 与预测区间。
它不单列为第三项研究内容，但必须先通过 work estimate/decision gate，才允许驱动两项策略。

## 1. 四条同等严格的可证伪链路

1. **Work Unit**：现有 rows/images/batch 是否无法表示可比 AI work？分阶段 work、locality、job/SLO、uncertainty 字段分别由哪个实验现象导出？
2. **状态感知**：相同静态上限下，stage/service/job 状态是否随到达、work mix 和 job 存活集变化？哪些信号必须是 fresh 且绑定 calibration signature？
3. **动态调度**：静态分区或独立 job 并发何时出现空闲份额、全局压力叠加、tail 或公平性问题？有界准入、路由和 shared work credit 是否在同最大 K/work 下超过 frozen-static？
4. **算子代价估计**：简单均值、解析 proxy 或 lookup 是否能稳定选对 organization/active-work/routing 候选？解析 + profile + residual 是否降低 ranking/regret？

四条链路均要记录“已证明 / 条件性支持 / 待验证 / 不能声称”。文本与图像是外部有效性轴，不把 token 机械改名为 frame。

## 2. 成功标准

- correctness、exactly-once、任务质量、feeding-saturation、稳定性与资源门禁全部通过；
- 相对同资源、同上限 frozen-static，至少一个**预注册 headline**（correct throughput、
  SLO goodput、worst-Job P99/JCT 或 empirical service lag）改善约 5%；同时为每个 protected
  metric 分别冻结方向与非劣/SLO 边界，不能事后用“无不可接受退化”替代明确阈值，也不能
  把 5% 机械套给所有指标；
- steady control 不要求动态胜出，但控制开销和回退不得造成约 5% 以上退化；
- 若所有目标指标均未改善，记录动态控制失效边界，不更换弱 baseline。

## 3. 公共实验合同

- immutable workload manifest、相同 source、模型/processor revision、输出语义、随机种子和服务 flags；若 headline 是 database-E2E，再要求相同 sink；
- 1 warm-up + 3 个交错 formal，失败 run 结构化保留；
- 离线容量校准和正式运行使用相同 calibration signature；
- baseline 由对应框架拥有 batching/backpressure/scheduling；项目只做 source、必要的 sink、质量与指标适配；
- project static 与 project dynamic 使用同一最大 request credit、stage work credit、actor/GPU/CPU 资源和 buffer bytes 上限；
- `BoundedReadyWindow` 是 project-owned observation/execution mechanism，不得注入 Daft、Ray Data、vLLM 或产品原生 baseline；只有项目内部 selector 消融共享它；
- 全部指标使用 during-run time series 聚合，不使用单点 idle snapshot。

## 4. 因子分解

### 4.1 Work estimate gate

| 模态 | 对照 | 候选 | 指标 |
|---|---|---|---|
| 文本 | rows、input tokens | input+cap、input+predicted output、calibrated service work | rank、MAPE、pairwise、regret、区间覆盖 |
| 图像 | images、encoded bytes、pixels | prepare work、model work、two-stage vector | stage service rank、regret、区间覆盖 |

只在估计器能稳定排序候选配置后，才允许驱动在线控制；否则只作 tracing 字段。

### 4.2 Organization gate

保持总 work、服务与静态 credit 不变，逐一比较：

1. fixed rows/images；
2. scalar token/frame budget；
3. stage-aware work budget；
4. balance-aware；
5. locality-aware；
6. balance + locality 受控组合。

报告 batch work CV、packing ratio、oversize rows、endpoint/stage work skew、prefix/locality preservation、queue age、throughput、tail、quality 和 energy。

### 4.3 Dynamic gate

动态控制仅在离线校准动作集合内选择，按以下顺序运行：

1. steady underload；
2. steady near-saturation；
3. steady overload guardrail；
4. two-phase low→high 与 high→low；
5. burst arrival；
6. short/long 或 easy/hard work mix；
7. 1/2/4 job equal work；
8. staggered overlap；
9. weighted 3:1；
10. heterogeneous job mix/offset。

每个场景比较：

- frozen-static；
- state-observed but no-op（测量观测开销）；
- admission-only；
- routing-only；
- fair-sharing-only（多 job）；
- 最小联合候选。

禁止一开始把 organization、K、routing、actor 数和资源同时动态化。

## 5. 动态控制器最小设计

第一版只使用：

- offline `safe_min_work`、`safe_max_work`；
- arrival-work EWMA；
- completion-work EWMA；
- oldest queue age / SLO slack；
- stage active/ready work；
- 观测 freshness。

动作：从冻结的 work-credit 候选集合中单步升降；deadband 内保持；最短驻留时间内不重复动作；观测过期、误差异常或 failure 时回退 frozen-static。vLLM waiting/KV 和 GPU util 作为 guardrail/诊断，不单独驱动 AIMD。

### 5.1 当前实现边界与最小接入顺序

| 部件 | 当前代码事实 | 正式数据支持到哪 | 下一项可验证接入 |
|---|---|---|---|
| Work Unit | `planning/work.py` 已定义 staged `WorkDescriptor`；`BatchRequest`、图像 batch 和调度器可消费中性 primary work | fixed-row token tail 与图像阶段画像支持字段设计；正式 runner 目前仍主要构造 legacy scalar work，尚无生产 descriptor builder | 先为文本/图像 adapter 构造带 calibration signature 的 descriptor，并做 legacy/descriptor 等价性门禁 |
| 状态感知 | `RuntimeStateSnapshot`、freshness/signature 检查和 `BoundedStageWorkController` 已有单元测试；现有 runner 已采 endpoint/service/resource trace | high/arrival-limited、容量曲线和原生状态指纹证明信号会变化；stage snapshot 尚未接入正式 runner | 先接 observe-only snapshot 与 trace，测 no-op 开销、缺失/过期回退，再允许控制 |
| 动态调度 | 调度器已实际消费 active work、least-work routing、completion release 和共享 DRR work credit；5s 两 job A/B 已正式运行 | 已证明 shared credit 的效率—隔离—公平权衡；尚未证明 stage-aware dynamic 或 SLO guard 优于 frozen-static | 保持同最大 K/work，按 admission-only、routing-only、fair-sharing-only 顺序做 phase-change/SLO 消融 |
| 代价估计 | CE1–CE5 已实现为离线分析器；CE5 尚未在线驱动 organizer/scheduler | 文本 context-LOO 只支持配置选择的 marginal feasibility | 先把 estimate 作为 descriptor/tracing 字段回放；只有 held-out ranking/regret 过门后才影响在线动作 |

因此当前代码不能表述为“完整状态感知方法已经落地”。最小工程顺序是 descriptor builder →
observe-only snapshot → no-op/fallback gate → 单一控制动作；不先把 cost、organization、routing
和 credit 一次性联动。

### 5.2 SAOR：阶段感知有序释放调度（动态候选，唯一设计维护入口）

#### 5.2.1 状态、定位与一句话定义

| 字段 | 当前冻结值 |
|---|---|
| 工作名称 | SAOR：Stage-Aware Ordered Release（阶段感知有序释放） |
| policy revision | runtime/formal contract `saor-v0.4.6-work-conserving-gate`；resolution-aware audit `saor-v0.4.10-resolution-aware-full`；priority diagnostic `saor-v0.4.9-release-upper-bound`；failed development candidate `saor-v0.5.1-reclaim-barrier`；local observation revision `saor-v0.5.2-bounded-ready-local`；core implementation `saor-core-v0.2`；capacity adapter `saor-v0.2-development/not-promoted` |
| 状态 | 2×4090 fixed-envelope 2-Job formal 已完成 40/40、0 incident、exactly-once；resolution-aware v2 在服务器完整 artifact 上 validation passed、credit mechanism effective 12/12，原 failed 文件保留审计。SAOR 在 credit 臂内 fg 最好但未越过 static；strict-priority 两轮 GPU 短测达到 11,791 tok/s、fg P99 14.27s/SLO 0%，但 formal repeats=0。旧 runtime 的 `slo_weight=0`，不是完整 SLO-aware 方法；dynamic K 为 `parked-conditional`。v0.5.1 单-head gate 未晋级，定位 ready observation gap；v0.5.2 bounded-ready 两轮中只有 $H_B=0.125W_e$ 同时通过 foreground/bulk/efficiency 门，$0.25W_e$ 被 bulk guard 拒绝。该结果只注册候选参数；因 ready-window 与 selector 同时变化，formal 前必须完成项目内部 matched-observation 归因 gate；该机制不注入原生 baseline，定理证明也未完成 |
| vLLM 合同 | 未经修改的 vLLM；主臂显式 `--scheduling-policy fcfs` |
| 内部能力 | continuous batching、chunked prefill、PagedAttention/KV、prefix cache 按冻结配置工作 |
| 外部控制对象 | Job/request 的释放顺序、endpoint 路由、request/work active window |
| 公平定位 | Job 级长期加权服务约束；不声称复现或超过 in-engine VTC token-level bound |
| 理论来源 | constrained stochastic network、MaxWeight、Lyapunov drift-plus-penalty；属于成熟理论迁移 |

一句话定义：

> SAOR 在冻结的安全 request/work envelope 内维护数据库/Daft/Ray 分阶段真实队列与公平/SLO
> 虚拟债务，以单 endpoint 有序 dispatcher 将选中的 Job-head 独立请求持续释放给 vLLM FCFS；
> vLLM 继续负责 iteration/token 级 continuous batching、chunked prefill 和 KV 管理。总容量
> 在主方法中固定；动态对象是 active-set entitlement、idle borrowing/reclaim 和 release order。
> Safe-Capacity Governor 已暂停，不进入当前主方法。

该设计利用 FCFS，而不是与 FCFS 对抗：上游决定合并后的到达流，vLLM 按到达顺序执行；
request 完成即释放 credit 并补位，使 continuous batching 不必等待整个上游 cohort 完成。
项目不分配 CUDA core、不选择下一个 decode token、不改 vLLM scheduler。

#### 5.2.2 对“waiting reservoir”和单一 GPU 利用率判断的修正

`vLLM waiting > 0` 不是目标状态，也不是 continuous batching 生效的必要条件；GPU
utilization 高或低同样不是健康/欠供给的充分条件。GPU utilization 是采样到的设备忙碌症状，
不能单独回答“是否有上游 ready work、请求排在哪里、是否产生有效完成、tail/SLO 是否恶化、
是否达到当前配置的服务平台”。`running` 较高、GPU utilization 较高且 `waiting=0` **只有在**
完成速率接近同协议 ceiling、上游/Ray/vLLM 队列不持续增长、tail 稳定且无 failure 时，才可
判为健康饱和；缺少这些交叉证据时不得下结论。

此前项目经验统一冻结为反例，而不是零散调参记忆：

| 已观察经验 | 单一指标会给出的错误判断 | 对 SAOR 的约束 |
|---|---|---|
| 单点 `gpu_utilization_pct` 曾在实际运行后显示 0，属于采样时刻假象 | “GPU 没有工作” | 禁止使用单点 snapshot；只读 during-run `mean/p50/p95/max`，并与完成 work、running 和功耗交叉验证 |
| shared-vLLM 中 `waiting=0`，但请求已在 Ray actor 侧排队，前台延迟恶化 38.9% | “vLLM 无 waiting，所以可以继续加并发” | 必须同时观察 Ray ready/active work、oldest age、submit/completion rate 和 Job slowdown |
| 每 endpoint 65,536 active work 已达到最大吞吐的 97.80%，继续提高到 98K--131K 吞吐近似持平而 P99/SLO 更差 | “更高并发或更高 GPU busy 一定更好” | 选择最小饱和点；边际吞吐平台且 tail 上升时判为过量 admission |
| request K64 的 offered work 比匹配臂约高 33%，P99 同时恶化 | “持续补位带来的吞吐就是机制收益” | 比较 continuous replenishment 时必须匹配 active work，不能把多喂 work 当算法收益 |
| 图像路径曾表现为 CPU preprocess/driver-Ray submit 混合木桶 | “GPU utilization 低，应继续提高模型侧并发” | 先定位 source/prepare/model 哪一阶段缺 work；上游瓶颈不能靠扩大 vLLM window 修复 |
| KV 饱和、prefix 命中下降时，数据组织策略排名发生反转并伴随 TTFT/tail 恶化 | “GPU 已经很忙，所以运行状态健康” | 高 GPU busy 仍可能是重复 prefill/KV thrash；必须联合 KV、prefix hit、TTFT/TPOT 和 correct goodput |

SAOR 的观测必须形成五层状态指纹：

| 层次 | 必需信号 | 回答的问题 |
|---|---|---|
| workload availability | source/prepare/model `ready_work`、arrival work、freshness/signature | 当前是否真的有可提交工作，瓶颈在哪个阶段 |
| Ray 外部执行 | active request/work、credit occupancy、Ray queue depth/work、oldest age、submit/completion rate | 请求是否在 vLLM 之前形成软拥塞 |
| vLLM 服务内部 | running/waiting、KV usage、queue time、TTFT/TPOT、prefix-cache hit | engine 是否出现排队、KV/prefill 压力或 cache thrash |
| 实际进展 | completion-work EWMA、model/operator/E2E throughput、JCT/P99、SLO goodput、failure | 忙碌是否转化为正确完成和可接受 tail |
| 资源佐证 | GPU utilization `mean/p50/p95/max`、MFU、显存、功耗/能耗 | 设备是否忙、忙碌成本多高；只作交叉证据，不单独触发动作 |

控制状态按联合证据分类：

| 状态 | 联合判据 | SAOR 动作 |
|---|---|---|
| 观测无效 | snapshot stale/signature 不匹配，或只有单点 GPU 值 | 不调节；保持/回退 frozen-static |
| 上游受限 | model ready work 不足，且 source/prepare 等待或服务不足 | 不扩大 vLLM 容量；保留阶段诊断，相关策略在上游单独处理 |
| 模型欠供给 | ready work 充足、active work 低于安全饱和区、完成速率低于同协议 ceiling，且 tail/队列稳定 | 只上调一个离线安全容量档位 |
| 健康饱和 | correct completion/goodput 接近 ceiling，队列和 tail 稳定，无 failure | 保持；不要求 waiting 非零，也不要求 GPU utilization 达到固定阈值 |
| Ray 侧软拥塞 | Ray queue work/oldest age/slowdown 持续上升，而 vLLM waiting 仍低或为 0 | 停止扩容；下调外部 window 或在独立消融中调整 dispatcher/credit |
| vLLM 内部拥塞 | waiting/KV/queue time/TTFT 持续上升，边际吞吐平台或下降，SLO 恶化 | 下调一个安全容量档位；记录是否存在 preemption/KV thrash |
| 信号冲突 | GPU/MFU 与完成速率、队列或 tail 给出相反结论 | 不在线猜测；保持当前档位或 fail-closed，保存 trace 离线诊断 |

所有“持续上升/稳定/接近 ceiling”均按冻结观测窗口、最短驻留时间和 time-series 聚合定义，
不能由一次 scrape 或瞬时峰值触发。GPU utilization 与 MFU 进入模型可以作为解释变量或
guardrail，但第一版 SAOR 不以它们作为单变量阈值控制器。

vLLM `priority` 只作可选消融：将少量公平/SLO 紧迫度等级投影为 request priority，检验在
真实 engine waiting 出现时是否改善 tail。它不是 SAOR 成立条件；其潜在 preemption/recompute
必须单独记录。自定义 `scheduler_cls` 不进入主方法，因为它改变引擎边界且接口不是稳定公共合同。

主要依据：

- vLLM V1 request queue 当前公开的 FCFS/priority 语义：
  <https://docs.vllm.ai/en/stable/api/vllm/v1/core/sched/request_queue/>；
- vLLM chunked prefill 将长 prefill 分块并可与 decode 工作共同调度：
  <https://docs.vllm.ai/en/latest/configuration/optimization/>；
- VTC 是 continuous-batching 引擎内部公平算法，本项目仅保留外部 VTC-style baseline：
  <https://www.usenix.org/conference/osdi24/presentation/sheng>。

#### 5.2.2.1 2026-08-11 phase-change 提前停止结果对状态模型的约束

`experiments/results/phase_change_state_aware_corrected_early_stop_20260811/` 已在
`dae9f7ae9dbbf211887134ce5bddd07bcb0aa81a` 完成报告和紧凑原始数据归档，本节将其作为
已审计的提前停止证据纳入设计约束。

- 在该机器/模型/服务/workload calibration signature 下，A-only 从 K128 到 K160 的每 endpoint
  服务率提升约 7.77%，且 K128 上游 `arrival→submit` P95 约 23 秒；这支持“存在安全增档机会”的
  动机，不支持把 K128/K160 固化为通用动作。
- B=2.5/3.5/4.5 req/s 均未通过预注册降档门。最高压力下只有第二轮 ON 的一个 endpoint
  出现 KV max=0.874，waiting 始终为 0；因此不得把 `KV≥0.85` 或 `waiting>0` 写成 SAOR
  的硬编码降档规则，也不能声称 adaptive 已有效或无效。
- 第二轮 ON 明显比第一轮重，且第二个 OFF 仍保留较高 KV/backlog，说明 phase label 不是
  状态复位信号。$Q_{j,s}$ 与 $R_e$ 必须跨 phase 连续演化；若实验想把两个周期当作等价重复，
  必须另设 `waiting=0 + KV/active-work/backlog 回到预注册基线带` 的 recovery gate。
- 下一轮独立验证保持模型、硬件、输出上限和两档容量不变，只改 phase 结构：使用明确 burst，
  或延长 drain 并等待 recovery gate。未恢复的周期仍可用于检验有记忆的在线控制，但不能当作
  独立同分布重复。

这一结果改变的是可行动作的证据等级和实验初始条件，不改变 DPP/ordered-release 框架。核心
代码只接收带 calibration signature 的容量档位、预测 service/cost 和 fresh state；所有具体
档位、阈值、窗口、endpoint 数与模态 work 换算必须由配置/adapter 显式提供。

#### 5.2.2.2 2026-08-11 capacity-only development gate：为什么没有超过强静态

`experiments/results/saor_capacity_development_20260811/` 在同一 A20/B4.5 合同上完成 frozen
K128、frozen K160、legacy threshold 和 SAOR capacity-only 四臂各一次。4/4 均完成 5,266
请求、0 incident、终态 credit 归零；但只有一次且顺序固定，服务器 worktree 还是基座 commit
加未提交同步代码，因此只作 development evidence。

SAOR 相对 K128 吞吐 +4.36%、group duration −4.18%，低于约 5% 晋级门；相对 K160 只有
+0.52%/−0.52%，相对简单 threshold 则吞吐 −1.46%、duration +1.48%。同时 SAOR 的 Jain
为 0.776，低于 K128/K160/threshold 的 0.805/0.779/0.786；相对 K128，Job B P99 恶化
26.94%。因此当前 **aggregate-state + two-arm + capacity-only** 版本记为 `not-promoted`，
不继续在同一 workload 扫 `V`、risk weight 或更多 K。

这不等于 frozen K160 很差。K160 相对 K128 吞吐 +3.82%、duration −3.68%，两个 Job JCT
分别 −13.54%/−3.58%；代价是 waiting mean/P95/max 从 0.068/0/13 升至 2.023/17/70，KV
P95/max 从 0.826/0.940 升至 0.997/1.000，Job B P99 +23.93%，Jain −3.22%。本轮无 OOM、
failure 或 credit leak，故只能把 K160 定位为**强静态效率点兼 tail/fairness 风险点**，不能
写成“不安全/必然失败”。动态方法的证明义务是相对它形成 Pareto 改善，不是先假定它有害。

失败机理按证据冻结为：

1. workload 是持续高压且没有 recovery gate；四臂 GPU mean 都约 99%，缺少可避免 idle；
2. SAOR 1,876 个 endpoint-state 样本中 1,551 个（82.7%）仍在 K160，threshold 为
   1,632/1,850（88.2%），SAOR 只是“多数时间 upper + 末段振荡”；
3. 当前 runner 只执行 aggregate capacity，没有把 per-Job fairness/SLO debt 或 ordered release
   接入动作，因此不能从完整 DPP 模型推导 Jain 改善；
4. backlog-weighted service 项随队列增长，而 risk proxy 是归一化小数且 `V=1`，量纲使 high
   arm 长期占优；线上还只更新 current-arm EWMA，未获得同状态反事实 service；
5. 外部降档不撤销已获 lease，14 次切换主要出现在约 286--330 s，KV/长 decode 的排空延迟
   使动作错过压力形成期；
6. aggregate waiting/KV 不表达 Job B P99、SLO 或 attained-service lag，故改善 Job A tail
   可以同时伤害 Job B 和 Jain。

这一负结果进一步说明“数学模型写出”不等于“实现已数学验证”：oracle DPP 的 conditional
mean service、即时 action effect、stationary slot 与完整 virtual queue 在本轮都未闭合。后续
只有在独立 burst/recovery workload 上 oracle 相对强静态先显示约 5% 以上且无 tail/fairness
退化时，才允许继续在线 estimator；否则 dynamic capacity 分支直接淘汰。

#### 5.2.2.3 `saor-v0.4`：固定总 K，动态 active-set 份额

详细数学模型、证明骨架、benchmark 和一手依据统一见
`../../research/saor_model_scenario_audit_20260811.md`。本次审计把两个不同问题明确拆开：

1. **SAOR-Release（主方法候选）**：固定总 request/work envelope，只对 per-Job head request
   做 completion-driven ordered release，并用实际 completion 更新 unfinished work、公平和
   SLO debt。动态对象是 backlogged Job 集合内的 entitlement、idle borrowing、completion-time
   reclaim 和 release order；不在线切 K。
2. **Safe-Capacity Governor（`parked-conditional`）**：慢速选择已校准档位。现有实验没有
   证明它相对 K160/最小饱和强静态点有必要性。只有离线 oracle 先证明 Pareto 空间才允许恢复；
   恢复时安全/tail/failure 先形成 hard
   feasible set，再在其中选择预测 goodput 最好的档位；current-arm EWMA 不再被当作未运行臂
   的同状态反事实。降档显式记录
   $D_e=[R_e-K^{work}_{target}]^+$ pipeline debt，debt 清零前不发新 lease，并用 drain envelope
   与 hysteresis 处理不可撤销 work。

该修正保留 MaxWeight/DPP 作为 fixed-envelope oracle theorem 的理论来源，但不把它自动扩张到
未知 service、延迟生效的 capacity governor。安全条件不再作为可被 backlog 抵消的 soft risk
小数；第一版 ordered-release 可令 $V=0$，先验证 queue/fairness/SLO 约束，再单因素加入有明确
单位的能耗或切换 penalty。

容量 benchmark 与公平 benchmark 分轨：多 Job fixed-envelope 是 SAOR 主验证；capacity 动态
暂停。现有 static/shared 只证明固定分区存在浪费和无约束共享存在 fairness/tail 代价，**尚未
排除同一 request K 下 no-project Job scheduler 已足够好**。因此第一项决定性 formal 使用统一
交错 runner 比较：direct no-Job、static partition、project shared FIFO、shared DRR、external
VTC-style 与 SAOR-Release，并加入 project/direct 各自的 bulk/foreground matched solo。
project 五臂共享 request/work envelope；direct 只共享相同 request window、manifest、协议和
vLLM 配置，不声称具有 project work-credit envelope，结果显式记录
`work_envelope_applied=false`。若 direct/FIFO/DRR 已落在相同 Pareto 前沿，淘汰 SAOR，不更换
workload 追正。

该 formal 已于 2026-08-12 运行：40/40 cell、0 incident、exactly-once；`saor_release` 和 FIFO
机制门 3/3，通过定位性均值分别为 12,393/12,103 tok/s，SAOR fg P99 50.3s、FIFO 58.7s；
但 static 以 9,508 tok/s 换得 fg P99 29.2s 和 0% SLO violation。原始 validation 因 DRR/VTC
rep2 `active_set_bulk_only_post_samples=0` fail-closed；两个 repeat 的 Job 完成时刻仅差约
5.8ms/4.8ms。审计器现已把“小于 250ms trace 周期且区间内无样本”冻结为 post-drain
不适用；`ed168d8` 默认 summarizer 已在服务器完整 artifact 上旁路重汇总，完整 validation
passed、四 credit 臂 effective 12/12，并保留原 failed 文件作审计。该修复只纠正观测假阴性，
不改变 static 与 SAOR 的性能排序，因此仍不能发布 winner claim。权威数据见
`../results/saor_active_set_release_formal_20260812_69affc7e/README.md`。

post-formal 第一性原理审计进一步给出 release-only 下界：若前台到达时 bulk 已占满总包络且
没有保护余量，项目又不能抢占已进入 vLLM 的请求，则新前台只能等待 completion 释放 credit；
只改 Job-head score 不可能免费复制 static 的即时隔离。下一候选因此不是继续扫 fairness 权重，
而是有界 lexicographic priority/lag guard；reservation 作为未知到达/预测误差下的鲁棒性消融：有限保护余量、空闲借用、completion reclaim、upper-bound
resource credit 与 hard SLO feasible set。有界 priority/lag guard 未闭环前不扩 4-Job。

决定性场景固定为 `bulk-only → foreground-arrival → overlap → either-job drain`：bulk Job
先借用总 envelope；前台 Job 到达后只在 completion 释放 credit 时回收未来份额，不抢占已
进入 vLLM 的请求；任一 Job 先结束后，剩余 Job 在有 waiting work 时应越过旧等份，否则允许
按实际剩余 work 自然排空。该场景验证的是活跃集变化下的分配，不预设哪一个 Job 先结束，也
不通过 K128/K160 切换制造收益。晋级最小效应按 formal repeat 噪声预注册，不写入算法硬编码
阈值。

工程合同补充：`K^*=(K^{req},K^{work})` 由当前机器、GPU 拓扑、模型/revision/dtype、vLLM
flags、协议与 workload shape 的签名索引。新签名先自动选择硬件 profile，再由操作者启动一次
calibration matrix；选择器冻结“满足 correctness/SLO 且达到已测峰值平台的最小点”并输出带
证据 SHA 的 selection contract。签名不变的后续实验只读合同，不逐 run 手调；正式 runner
若配置与 selection 不一致直接拒绝。`saor_release` 不修改该 K，只有 Job active set 与 release
order 动态变化。

#### 5.2.2.4 `saor-v0.5`：通用接口、2-Job 首验的有界词典序 release

完整推导、release 非饥饿界和反例见
`../../research/saor_model_scenario_audit_20260811.md` §12。本节只冻结后续实现/实验合同。

根因不再表述为“SAOR 的 SLO 权重不够大”：formal 配置强制 `slo_weight=0`，请求剩余 SLO
预算也未从 scheduler 接入 coordinator；当前 soft score 实际优化 entitlement/fairness，不直接
优化 foreground tail。strict-priority 证明未来 release 能救前台，但它会为当前不 fit 的高优先级
Job 留空，并在无 debt cap 时让 bulk 长期欠服务。因此下一版不继续扫软权重，也不先引入
reservation。

通用策略对任意 Job 集 $B_e(n)$ 定义稳定业务优先级 $p_j$、公平权重 $\phi_j$、请求剩余 SLO
预算、completion-corrected actual-work 债务 $F_j$ 和 cap $H_j$。每个 release epoch 只在同时
满足 request/work envelope 的 fitting heads 中按词典序选择：

| 层级 | 动作 | 不能被什么覆盖 |
|---:|---|---|
| 0 | correctness、lifecycle、freshness、request/work fit | 任意吞吐、优先级或公平 score |
| 1a | $F_j\ge H_j$ 且无 recovery lease 在途时，先选 fitting 的最大 $F_j/H_j$；每 Job 至多一个 recovery lease；发出后立即恢复普通选择 | 避免 completion 校正前过量 recovery，也不为 recovery request 整段执行时间保留全局空槽 |
| 1b | debt-critical ready head 暂时不 fit 时，只针对该确定 head 建立 `guard_reclaim_hold`，直到 $D_e^{reclaim}=\max\{0,\overline W_i^{resource}-(K_e^{work}-R_e^{active})\}=0$ | 防止 foreground 小请求反复填充碎片；能 fit 后立即只发一个 recovery lease，不等待整份 quota |
| 2 | 否则选已进入 priority window 的最高 $p_j$；同级按最少剩余 SLO 预算 | SAOR 普通 entitlement score |
| 3 | 无 guard/priority 触发时回退现有 SAOR selector | 只处理剩余普通共享机会 |
| 4 | 普通高优先级 Job 当前无 fitting head 时，继续选择其他 fitting head | 禁止 strict-priority 的 avoidable idle；不覆盖 1b 的硬 guard reclaim |

服务债务仍按实际 completion work 更新：

$$
F_j(n+1)=\left[F_j(n)+\rho_j(n)c_n-\mathbf1\{j=k(n)\}c_n\right]^+,
\qquad
\rho_j(n)=\frac{\phi_j}{\sum_{h\in B_e(n)}\phi_h}.
$$

resource upper bound、ordering point estimate 和 actual fairness work 分账。若共同积压份额和
单 completion actual work 有正下界，则可以界定 debt 到 cap 前允许的 foreign completion 数，
并保证 cap 触发后第一个 fitting release opportunity 给欠服务 Job；但不能界定该请求在 vLLM
内部的完成时间或直接继承 VTC 的 in-engine 2× service-difference bound。若 SLO priority 与 debt guard 同时触发，记录 `constraint_conflict` 并由 debt guard 覆盖，
不以隐式软权重决定牺牲哪个约束。

1b 只能由“debt 到 cap + ready head + 暂时不 fit”触发；unfinished 但无 ready head、普通
priority head 不 fit 均继续选择其他 fitting head。目标 head 大于总 envelope 直接拒绝；若冻结
request/transport timeout 到期仍不能 fit，则 run 记 incident 并 fail-closed，不以任意 hold timeout
静默回退。

接口从第一天支持任意 Job 数和显式 per-Job priority/SLO/debt cap，不按到达次序或 Job 名称推断
foreground；首个实现与 GPU development 只复用冻结 2-Job workload。只测
$H_B/W_e\in\{0.125,0.25\}$ 两点，foreground 的 priority window 取完整 30s SLO，固定其余
参数和总 envelope；不跑长时间 formal，不扩 4-Job。

等权时 bulk debt 按 foreground actual completion work 的 1/2 增长。当前 foreground actual work
约 147.7K、两 endpoint 近似均分，$0.125W_e/0.25W_e$ 粗略在 foreground 单 endpoint 完成约 22%/44%
后触发；0.50K 约到 89% 才触发，过于接近 strict-priority 无限 cap，故退出首轮。

其中 $W_e=65,536$ 是单 endpoint work-credit limit，两个 cap 分别为 8,192/16,384 actual-work
debt units。配置字段的 fraction 乘 `work_limit`，不是 request K；历史 scenario 中的 `0.125K`
只作为旧 ID 保留，不再进入正文记号。

| 首轮门 | 判据 |
|---|---|
| correctness | 0 incident、exactly-once、lifecycle/fit/event ledger 全通过 |
| foreground | P99≤30.7s，SLO violation≤1% |
| efficiency | tokens/s≥9,984，即相对 static 9,508 至少约 +5% |
| bulk protection | SLO violation≤0.723；slowdown 只作诊断，不作硬门 |
| mechanism | `avoidable_idle=0`；guard hold 的 count/total/P95/max 与 reclaim debt 单列；priority/debt tier 均实际触发；每 Job recovery lease≤1；debt-critical fitting head 被 foreign grant 越过次数=0 |
| stability/结论 | 两个短 repeat 方向一致；只决定是否值得注册 formal，不发布 winner claim |

若两个 cap 均不能同时通过 foreground、bulk 和 efficiency 门，则停止密集扫描 cap/权重并审计
约束冲突。当前 $0.125W_e$ 已通过双轮 development gate，但这只冻结候选参数；必须先完成
§5.2.11 的项目内部 matched-observation 归因 gate。reservation 与 point/upper-bound resource work
推迟到该 gate 和 2-Job formal 均闭合以后，不因 development 正结果自动启动。

#### 5.2.3 三种 work 语义：token 组织不取消，但不再一数三用

每行必须保持一个独立完整请求。**禁止**把单行长 prompt 按 token 切成多个互相隔离的请求；
安全的 token 级 prefill 分块由 vLLM chunked prefill 完成。现有 token-budget 若表示“按 token
总量把多行请求组织为 cohort”，则继续保留。

三种 work 明确分离：

1. 数据组织量：

   $$
   W_i^{org}=n_i^{prompt}
   $$

   用于 token-budget、length alignment 和 prefix-affinity；只改变请求集合/顺序，不拆请求内容。

2. admission/resource work：

   $$
   \widehat W_i^{resource}
   =c_0+c_p n_i^{uncached}+c_d\mathbb E[n_i^{output}],
   \qquad
   \overline W_i=\widehat W_i^{resource}+\kappa\sigma_i
   $$

   用于 active-work、容量档位和 endpoint routing。prefix cache 开启时使用 effective/uncached
   prefill work；若解析 proxy 不能在 held-out 上改善配置 ranking/regret，则退回 raw token
   work，不为使用复杂模型而使用复杂模型。

3. 公平记账量：

   $$
   W_i^{fair}=w_p n_i^{prompt}+w_q n_i^{output,actual}
   $$

   用于与 VTC-style baseline 可比的 Job entitlement；完成时以实际输出 token 修正估计。
   capacity 使用物理 resource work，公平使用逻辑 weighted-token work，不能因 prefix cache
   命中而混为同一个计数器。若以后研究硬件资源公平，另报实际服务时间/KV/FLOPs 指标，
   不把它悄悄替换为用户侧公平定义。

图像路径保持同一抽象，只在 modality adapter 中把上述量换成 image/frame、prepare/model
work 与实际完成服务；scheduler 本身不出现 `if text else image`。

#### 5.2.3.1 图像 CPU 木桶的两级 broker 修正

现有图像证据不支持“GPU busy 低就扩大 model active window”。60K×2 matched-resource 正式
结果中，project CPU8→CPU16 从 1038.7 提升到 1666.5 image/s，而 GPU busy mean 仍只有
6.3%→9.6%；同机 dual-GPU forward ceiling 约 19K image/s。host-path screening 又显示
preprocess actor 16→32 的 post-setup 吞吐只提高约 7.3%，但 worker setup 和 first output
明显恶化；source thread 1→4 只提高约 2.4%。因此当前木桶是 CPU prepare 与 driver/Ray
submission 的组合，不是数据库读取、PCIe 或 GPU forward 单点。

当前 project runner 直接执行：

```text
preprocess_ref = cpu_actor.preprocess.remote(encoded)
result_ref = gpu_actor.embed.remote(preprocess_ref)
```

这会把尚未完成的 prepare dependency 预先排入 GPU actor，调度器看不到独立的 ready-tensor
队列，也无法区分“CPU 还没产出”与“GPU 来不及消费”。SAOR 图像扩展必须先改为显式两级
broker：

```text
Daft encoded queue
  → bounded pending-prepare（预启动 Ray CPU actor pool）
  → bounded ready-tensor queue
  → bounded pending-model（常驻 Ray GPU actor）
```

令 $Q_{j,p}$ 为等待 prepare 的 work，$Q_{j,m}$ 为已经 prepare、等待 model 的 work。对单 Job
省略下标后，两级 tandem queue 的动作相关 MaxWeight 项为：

$$
\Psi_{pipe}(a)=
-(Q_p-Q_m)\widehat\mu_p(a)-Q_m\widehat\mu_m(a)
+V\left(c_{tail}(a)+c_{memory}(a)+c_{switch}(a)\right).
$$

这里 $Q_p-Q_m$ 是 differential backlog：encoded backlog 大而 ready tensor 少时提高 prepare
流量；ready tensor 已堆积时自动抑制继续膨胀 tensor，并优先让 model 排空。它比“GPU
utilization 未达阈值就增档”更符合现有反例。

工程采用两个时间尺度：

- **离线/慢时间尺度**：冻结 CPU/GPU actor pool 拓扑、每 actor 线程数和 placement/resource
  request；不在每个控制周期创建/销毁 actor；
- **在线/快时间尺度**：只从校准安全集合选择
  `(prepare_inflight, ready_tensor_work, model_inflight)`，completion 即补位；
- **多 Job**：每个 Job 保持独立的 $Q_{j,p},Q_{j,m}$ 和公平债务，先选 Job-head，再由两级
  broker 执行；共享 ready buffer 必须按 work 而不是 batch 条数记账；
- **内存**：ready tensor 以 bytes/work 设硬上限。224×224 RGB 的 FP32 tensor 约 588 KiB
  （602 kB）/图，
  不能把 batch 数当成跨 dtype/shape 可比较的内存量。

纯策略已新增 `scheduling/runtime/saor_pipeline.py` 的有限安全臂与 differential-backpressure
控制器；它不 import Ray/Daft/CLIP。当前尚未接 image formal runner，因此只算核心代码和单元
测试，不产生图像性能 claim。接线前先把现有 observe-only snapshot 中“同一 submitted batch
同时计入 prepare/model active”的近似替换为真实两级队列。

Ray/Daft 的正确利用方式是让框架负责资源放置和流式执行，让 SAOR 负责项目拥有的应用层
admission：Ray task/actor 显式声明 CPU/GPU/custom resource，必要时 placement group 只做
跨节点共置；Ray Data/Daft Native baseline 仍由其自身 streaming executor 和 backpressure
拥有调度，不注入项目 controller。官方接口依据：

- Ray Data streaming executor 与 operator out queue/backpressure：
  <https://docs.ray.io/en/latest/data/data-internals.html>；
- Ray `map_batches` 的 CPU/GPU、concurrency 和 actor pool：
  <https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html>；
- Ray logical resources 与 placement group：
  <https://docs.ray.io/en/latest/ray-core/scheduling/resources.html>、
  <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html>；
- Daft `@daft.cls` 的 `cpus/gpus/max_concurrency/ray_options`：
  <https://docs.daft.ai/en/stable/api/udf/>。

SAOR 能减少错配、隐藏 overlap 并限制中间态，但**不能凭调度消灭每图约 5 ms 的 decode/
resize/normalize CPU work**。因此图像需求拆成“flow scheduling”和“work reduction”两条正交
消融，后者优先验证以下已有技术，不包装成 SAOR 算法贡献：

1. **带签名的 derived-image cache**：对重复/迭代数据库 AI 查询，建立普通派生表或 lakehouse
   表，主键为 `(content_hash, transform_signature)`；保存 resize/crop 后的 `uint8 CHW`，把
   decode/resize 从热路径移走，normalize/cast 留给 GPU。224×224 RGB uint8 为 150,528 B/图，
   是 FP32 输入 tensor 的四分之一。PostgreSQL 中不建议用 generated column 隐式执行模型依赖
   预处理；使用可审计 refresh 的派生表/物化结果，并显式记录 processor revision。Arrow
   lakehouse 可用 canonical `arrow.fixed_shape_tensor`（底层 FixedSizeList）保存 shape/layout。
2. **GPU preprocessing baseline**：用 NVIDIA DALI `external_source` 接数据库返回的 encoded
   bytes，比较 mixed decoder + resize + crop/normalize 与当前 CPU actor。DALI 官方即以
   CPU preprocessing bottleneck、prefetch/parallel/batch 为目标，但 mixed JPEG decoder 部分
   路径仍可能使用 CPU，且会消耗 GPU 资源，所以必须在 4090 上实测，不能按文档直接假定获益。
3. **CPU 路径继续保留**：当前 `FastClipImagePreprocessor`/torchvision actor 是冷数据和缓存
   miss 的生产 fallback；actor pool 规模离线冻结，在线只做 dispatch/admission。数据库端
   predicate/projection/limit 必须在读取 image bytes 前完成，但已有 source-thread 扫描说明单纯
   增加 PostgreSQL 读取并发不是当前主杠杆。

正式消融固定同一 CLIP processor 语义和 embedding 质量，比较
`raw→CPU`、`derived uint8→GPU normalize`、`raw→DALI mixed→GPU` 的冷/热命中率、JCT、
CPU-core-s/image、GPU-s/image、bytes/image、能耗和检索 recall@k/nDCG。只有 cache hit 可预测且
收益覆盖存储/refresh 成本时，derived cache 才进入主方案；一次性冷扫描不得借用预计算成本。

接口依据：NVIDIA DALI 的 GPU/mixed image pipeline 与 external source
<https://docs.nvidia.com/deeplearning/dali/user-guide/docs/index.html>、
<https://docs.nvidia.com/deeplearning/dali/main-user-guide/docs/operations/nvidia.dali.fn.external_source.html>；
Arrow fixed-shape tensor
<https://arrow.apache.org/docs/format/CanonicalExtensions.html>；PostgreSQL materialized view
<https://www.postgresql.org/docs/current/rules-materializedviews.html>。

#### 5.2.3.2 HSE 异构执行底座与暂缓候选

2026-08-11 的架构迁移审计进一步把两级 broker 扩展成工作名 HSE（Heterogeneous Staged
Execution）的执行合同，完整设计见
`../../research/heterogeneous_ai_dataflow_execution_model_20260811.md`。HSE 不修改 Ray/Daft
调度器，也不增加第三项研究内容；它只把研究内容一的 staged work/data representation 与
研究内容二的 admission/Job 调度接在真实队列上：

```text
encoded Arrow block
  → bounded CPU prepare leases
  → packed ready block（uint8/FP16，按 physical bytes/work 记账）
  → resident GPU actor（actor-local pinned ring / CUDA stream）
  → embedding block / ordered sink
```

2026-08-12 已完成前两步的静态核心：engine-neutral descriptor/lease broker、prepare 时
ready-byte/work 预留、真实 ready snapshot、Ray CPU actor descriptor/tensor 双返回值以及
`project_ray --project-execution-mode hse_static` 接线；旧 `direct_dependency` 默认路径继续作为
同资源对照。安全不变量已完成归纳证明与单元/fake-Ray E2E 检查，但尚无 GPU 性能结果。

后续顺序冻结为：先跑 direct-dependency static vs HSE static gate → 单因素 packed-uint8、
pinned、double-buffer、DALI/cache 消融 → 最后才接 SAOR 多 Job 动态控制。static HSE 未达到
同资源 current project frozen-static 的 `≥95%` 吞吐/JCT 非劣门，不运行动态 HSE 主实验；
它只有相对 frozen-static 出现可重复 `≥5%` 正增量时，才可声称 flow 增量。

prompt 变化感知、exact/semantic 结果复用、数据库级/模型内部增量推理已登记为
`parked-conditional`，当前不实现。它们必须复用 descriptor 的 source/version、transform、
model/tokenizer/processor 和 decoding signature；vLLM APC/prefix affinity 只算已有能力，不能
写成项目实现任意 KV 增量更新。重新激活门为主路径完成后真实 reuse opportunity ≥10%，且
扣除 lookup/build/refresh 后离线 oracle 潜力 ≥5%。

#### 5.2.4 固定控制周期与事件驱动补位

理论模型使用固定长度控制周期 $\Delta$，避免把可变请求完成间隔直接套入普通 slotted
Lyapunov 定理。工程实现分成两个时间尺度：

- **SAOR-Release 慢循环（每个 $\Delta$）**：读取 atomic snapshot，更新队列/虚拟债务，在
  当前冻结 envelope 内生成每个 endpoint 的有序候选流；
- **可选 governor 慢循环（多个 $\Delta$）**：仅在独立 oracle/安全门通过后选择容量档位；
  stale/signature 变化时回退 frozen-static；
- **快循环（每次 completion）**：释放 request/work credit，在本周期已选择的动作与上限内
  立即补一个或多个请求，不等待 cohort 完成；周期中出现 failure/stale signature 时停止补位
  并回退 frozen-static。

每个 endpoint 使用单一 ordered dispatcher 和单调 `release_seq`。正式 trace 同时记录
`release_seq`、HTTP submit time、服务端可见 arrival（若可得）和 completion time。HTTP/Ray
并发造成的小范围重排不伪装成严格 FCFS；理论上将其作为每周期有界近似误差，实验中报告
实际 inversion rate。若 inversion 不可界定，`FCFS ordered release` 的机制声明失败，不能
仅靠理论意图继续声称成立。

#### 5.2.5 系统状态、队列与可行动作

索引：Job $j$、阶段 $s\in\{source,prepare,model,result\}$、endpoint $e$、控制周期 $t$。

- $Q_{j,s}(t)$：Job $j$ 在阶段 $s$ 的 ready work；
- $A_{j,s}(t)$：外部到达或上一阶段完成带来的新 work；
- $S_{j,s}(t)$：本周期实际完成的阶段服务；
- $R_e(t)$：endpoint 已提交未完成的 active resource work；
- $R_j^{active}(t)$：归属于 Job $j$ 的已提交未完成 work；
- $H_e(t)$：running、waiting、KV、TTFT、queue/service rate、freshness/signature；
- $N_e(t)$：endpoint active request 数。

阶段队列：

$$
Q_{j,s}(t+1)=\left[Q_{j,s}(t)-S_{j,s}(t)\right]^+ + A_{j,s}(t).
$$

endpoint outstanding work：

$$
R_e(t+1)=\left[R_e(t)-C_e(t)\right]^+ + X_e(t).
$$

容量档位只来自同 calibration signature 下离线验证的有限集合：

$$
\mathcal K_e=\{(K_{e,k}^{req},K_{e,k}^{work})\}_{k=1}^{m_e}.
$$

请求 $i$ 只有同时满足下式才可释放：

$$
N_e(t)+1\le K_{e,k}^{req},
\qquad
R_e(t)+\overline W_i\le K_{e,k}^{work}.
$$

SAOR-Release 动作 $a(t)\in\mathcal A_K(H_t)$ 在冻结 envelope $K$ 内只包含 Job/request 选择
与必要 endpoint assignment。capacity governor、priority、routing 和 organizer 动态化分别按
独立消融加入，禁止第一版联合搜索所有旋钮。

#### 5.2.6 公平和 SLO 虚拟债务

当前有 ready 或 active work 的 Job 集合：

$$
B(t)=\{j:Q_{j,model}(t)+R_j^{active}(t)>0\}.
$$

权重 $\phi_j>0$，动态理想份额：

$$
\rho_j(t)=\frac{\phi_j}{\sum_{k\in B(t)}\phi_k}.
$$

公平债务：

$$
F_j(t+1)=
\left[
F_j(t)+\rho_j(t)C_{total}(t)-S_j(t)
\right]^+.
$$

只在共同积压期间计债；上游尚未准备好且无 active work 的 Job 不积累债务。Job 新加入或
重新变为 backlogged 时采用 counter-lift/active-set 初始化，防止通过离开再加入重置历史份额。
外部实现只能在 completion 后获得准确 output work，因此属于 delayed accounting；固定
`max_tokens`、timeout 和有界请求大小是证明中“反馈延迟有界”的必要条件。

若 $M_j(t)$ 为本周期 SLO miss 数，$N_j^{done}(t)$ 为完成数，允许 miss rate 为
$\epsilon_j$，则：

$$
Z_j(t+1)=
\left[
Z_j(t)+M_j(t)-\epsilon_jN_j^{done}(t)
\right]^+.
$$

$F_j$ 与 $Z_j$ 是约束债务，不是把公平、SLO、吞吐压成一个无法解释的 Jain/utility 总分。

#### 5.2.7 Drift-plus-penalty 决策

令 $\Theta(t)=\{Q(t),F(t),Z(t)\}$，定义：

$$
L(\Theta(t))=
\frac12\left(
\sum_{j,s}Q_{j,s}^2+
\eta_F\sum_jF_j^2+
\eta_Z\sum_jZ_j^2
\right).
$$

每周期枚举有限可行动作，最小化 Lyapunov drift 上界加运行代价：

$$
a^*(t)=
\arg\min_{a\in\mathcal A(H_t)}
\widehat\Delta(a\mid\Theta(t))+V\widehat g(a,H_t),
$$

其中：

$$
g=-\operatorname{goodput}
+\alpha\operatorname{tail\ risk}
+\beta\operatorname{energy}
+\gamma\operatorname{switch\ cost}.
$$

实现可直接计算下列与动作相关的上界项，不需要通用优化求解器：

$$
\begin{aligned}
\Psi(a)=
&\sum_{j,s}Q_{j,s}\left(A_{j,s}-\widehat S_{j,s}(a)\right)\\
&+\eta_F\sum_jF_j\left(\rho_j\widehat C_{total}(a)-\widehat S_j(a)\right)\\
&+\eta_Z\sum_jZ_j\left(\widehat M_j(a)-\epsilon_j\widehat N_j^{done}(a)\right)\\
&+V\widehat g(a,H_t).
\end{aligned}
$$

安全/tail/failure 条件优先定义 hard feasible set，不能作为会被 backlog 项压过的任意软小数。
prefix/cache locality 只能作为 $\widehat g$ 中有上界的效率奖励，或在得分近似相同的候选间
tie-break；不得无限压过 $F_j/Z_j$ 而造成饥饿。

经典理论依据为 MaxWeight 与 Lyapunov drift-plus-penalty：

- <https://drum.lib.umd.edu/items/571fda52-aefb-4497-9a2d-69d8c7c907b9>；
- <https://ee.usc.edu/stochastic-nets/docs/network-optimization-notes.pdf>。

#### 5.2.8 目标定理、证明义务和不可声称边界

目标不是证明“SAOR 在任意 vLLM 负载下最优”，而是在显式假设下证明队列稳定与长期约束
满足。需要依次完成：

**假设集**：

1. arrival、单请求 work 和单周期 service 有界并具有有限二阶矩；
2. arrival/service 在 calibration signature 内平稳遍历，或每个 phase 内分段平稳；
3. offered load 位于所选安全动作的 capacity region 内部，并存在 $\varepsilon>0$ slack；
4. observation delay、completion feedback delay 和 `release_seq` inversion 有界；
5. work/service 估计误差可转化为每周期 $C$-additive DPP 近似误差；
6. failure/stale signature 时 fail-closed，回退到已经验证的 frozen-static。

**拟证明结论**：若上述假设成立，且每周期动作相对理想 DPP 至多为 $C$-additive，则争取证明

$$
\limsup_{T\rightarrow\infty}
\frac1T\sum_{t<T}\mathbb E[g(t)]
\le g^*+\frac{B+C}{V},
$$

以及真实/虚拟队列强稳定、平均 backlog 为 $O(V)$。由公平与 SLO 虚拟队列稳定推出共同积压
Job 的长期加权服务约束和长期 miss-rate 约束。

**必须先证明或实测的桥接项**：

- 不同容量档位下 $\widehat\mu_e(a,H)$ 对真实完成 work 的 ranking 与残差界；
- service process 在一个 calibration signature 内是否足以稳定，phase change 是否能及时检测；
- completion-only actual-token 修正的最大反馈延迟；
- 单 dispatcher 的实际 arrival inversion 是否有界；
- approximate action 的误差能否写成与队列长度无关的常数 $C$。

上述桥接项未完成前，只能称“基于 DPP 的设计候选”，不能声称已经拥有 $O(1/V)$、$O(V)$、
公平份额或 SLO 定理。即使完成这些定理，也不能继承 VTC 的 in-engine 2× tight token service
difference bound。

#### 5.2.8.1 什么才算“完成定理证明”

仅写出 Lyapunov 函数、引用 MaxWeight 或跑出实验曲线，都不算完成证明。项目中“定理证明已
完成”必须同时具备以下可审计对象：

1. **定理陈述闭合**：随机过程、capacity region、稳定性定义、控制周期、可行动作、所有常数
   和期望所依赖的条件全部定义；结论必须量化适用范围，不能写“通常稳定”；
2. **假设可枚举**：arrival/service 有界性、平稳或分段平稳、$\varepsilon$ slack、反馈延迟、
   work 上界、失败回退逐项列出，正文不得再偷偷使用未声明假设；
3. **逐步推导**：从 queue recursion 展开 quadratic drift，给出有限常数 $B$，证明每一步
   不等式，再与存在 $\varepsilon$ slack 的 stationary randomized policy 比较；
4. **近似误差闭合**：明确线上选择相对理想 MaxWeight/DPP 是 exact、$C$-additive、
   $\alpha$-approximate 还是 high-probability robust，并按对应形式重写定理；
5. **虚拟队列推论闭合**：单独证明 $F_j/Z_j$ 稳定如何推出长期 weighted-service 与 SLO
   约束；不能从普通物理队列稳定直接跳到公平；
6. **边界和反例**：说明 overload、signature 漂移、无界 output、无界 inversion、Job 离开重入
   时哪个结论失效；
7. **独立复核材料**：proof appendix 给出全部 lemma/theorem，不以代码或实验替代缺失推导。

定理本身完成与系统适用性验证是两道门。数学证明可以在抽象模型内成立；但要把它写成 SAOR
系统结论，还必须用 trace/实验验证真实实现满足 service ranking、delay/inversion、bounded
work 和 action-feasibility 等桥接假设。实验不能“证明定理”，只能证伪或支持假设适用。

#### 5.2.8.2 对估计误差论证的致命缺口与修正路线

上一版拟把任意 bounded service-estimation error 直接写成与队列无关的 $C$-additive DPP
误差，这不严谨。MaxWeight 得分含 $Q\widehat\mu$；即使
$|\widehat\mu-\mu|\le\delta$，当 $Q$ 无界时得分误差仍可达 $Q\delta$，一般不能成为常数
$C$。在该项闭合前不得套用 $(B+C)/V$ 结论。

可接受的证明路线只有三类：

1. **oracle/exact 主定理**：先在已知 conditional mean service 的有限动作模型下给出 exact
   MaxWeight/DPP 定理，明确它只覆盖抽象 oracle policy；
2. **$\alpha$-approximate MaxWeight**：证明线上动作的真实 weighted service 至少为最优值的
   $\alpha$ 倍，则只对相应收缩后的 capacity region 声明稳定；需要用校准区间和 held-out
   ranking 证明 $alpha$ 下界；
3. **有界物理 buffer + robust error**：若工程硬上限给出 $Q\le Q_{max}$，可把误差界写成
   与 $Q_{max}\delta$ 有关的常数，但物理队列“稳定”此时是容量截断的直接结果，必须同时报告
   source backpressure/drop/未接纳 work；仍需另证不受硬截断保护的公平/SLO 虚拟队列。

当前冻结选择是：以路线 1 给出理论基准，以路线 2 作为可发表的实现桥接目标，路线 3 只作
工程安全边界。若 held-out 数据无法给出可复现的 $\alpha$ 或 contracted-region slack，论文
只能保留 oracle theorem + empirical controller，不声称实际 SAOR throughput-optimal。

#### 5.2.9 可执行伪代码与现有模块映射

```text
initialize frozen safe arms, per-Job Q/F/Z, per-endpoint ordered queues

every fixed control interval Δ:
    read one atomic RuntimeStateSnapshot
    if snapshot stale, signature changed, or failure observed:
        select frozen-static fallback; stop dynamic expansion
    aggregate arrivals/completions and update Q/F/Z
    enumerate feasible (capacity arm, Job/request, endpoint) actions
    choose action minimizing the DPP upper-bound surrogate
    publish per-endpoint ordered release queues with monotonically increasing release_seq

on every request completion:
    release request/work credit
    correct estimated work with actual prompt/output work
    update completion counters used by the next fixed interval
    while the published arm has request/work capacity:
        release the next eligible independent request to the endpoint dispatcher
```

工程映射：

| SAOR 概念 | 当前落点 | 状态与下一最小增量 |
|---|---|---|
| staged work/state | `planning/work.py` | descriptor/snapshot 已有；需接入文本 formal runner 的 atomic observe-only trace |
| safe capacity arms | `scheduling/core/control.py` | 中性 `CapacityArm` 已抽离；具体档位只由 calibration config 注入 |
| finite-action DPP | `scheduling/submission_control/saor.py` | 纯策略与公平债务已单测；需用 replay 验证 service ranking/动作 regret |
| completion/exactly-once | `scheduling/core/execution.py` | 通用 ledger 已接原 scheduler；actual-work extractor 由模态 adapter 注入 |
| ordered release | `scheduling/submission_control/{ordered_release,shared_credit,saor}.py` + `scheduling/runtime/shared_credit_ray.py` | fixed-envelope `saor_release` 已在真实 2×4090 formal 运行；SAOR mechanism 3/3，credit 臂内 fg 最好，但 static fg 更强；`slo_weight=0`，不称 SLO-aware 已接通 |
| formal harness | `experiments/shared_vllm/{runner,direct_control,metrics}.py` + `analysis/{audit_saor_formal_readiness,summarize_saor_active_set,summarize_saor_priority_reachability}.py` | 十 scenario 40/40、0 incident、exactly-once；resolution-aware v2 完整 validation passed、effective 12/12；strict-priority 两轮短测完成但不是 formal，不发布 winner claim |
| endpoint state | vLLM/resource time series | atomic freshness/signature gate 待接；waiting/KV/GPU 不单独驱动 |
| cost model | CE1--CE5/WorkDescriptor | 先 replay/observe-only，过 ranking/regret 门后才进入动作 |

第一版算法增量必须保持简单：只实现 `FCFS + admission arm + Job ordered release`。routing、
priority、prefix bonus、SLO debt 分别做后续单因素增量，不在一个提交中同时实现。
单请求动作构造器把 hold 定义为零增量参考，所有 release action 的 service/goodput/tail/energy/
switch 输入都必须是相对 hold 的显式边际预测；任何映射缺失立即拒绝，不能静默补 0。
这是对每个候选分数减去同一个基准 $\Psi(hold)$；因为
$\arg\min_a\Psi(a)=\arg\min_a[\Psi(a)-\Psi(hold)]$，不会改变动作排序。该等价只在同一 snapshot
下基准项对所有候选相同时成立，跨 snapshot 或跨 calibration signature 不能复用边际预测。

#### 5.2.10 公平评价合同

主公平定义：**单租户内共同积压 Job/workload class 的长期加权 attained service**。Job/query
是当前调度单元，request 只是工作量载体；不以原始 TTFT 相等、请求条数相等、静态配额未超限
或单一 Jain 替代。当前 coordinator 按 `job_id` 记账与本范围一致，现有证据应称 intra-tenant
logical Job-stream fairness/service differentiation，不称 tenant fairness。

当前 formal 的 `job_id`、weight、priority 和 SLO class 均由 immutable experiment/application
contract 冻结，不接受客户端在 run 中自行创建额外 Job 身份或修改权重。因此本轮回答“一个
租户内已知多个 workload class 如何共享服务”，不回答 adversarial identity manipulation。

多租户不阻塞当前 formal，也不要求现在修改 runner。未来若进入 scope，在现有 Job scheduler
外增加 `principal_id→workload_class→job_id→request_id` 层次：先聚合 tenant entitlement/debt
与 ready/buffer cap，再在 tenant 内复用当前 Job priority/SLO/borrowing/reclaim。届时才补同一
principal 将相同 work 拆成 1/2/4 Job 的 anti-splitting 门。若仍让所有 `job_id` 平铺竞争，或让
跨租户 strict priority 绕过 tenant floor，则不能声称多租户公平。公平评价与效率/SLO 形成约束
下多目标向量，不压成一个 composite score。

先冻结评价语义，禁止把两种不同问题混成一个“公平分数”：

| 模式 | Job 关系 | 主要目标 | 项目内部算法消融 | 不能用什么代替 |
|---|---|---|---|---|
| equal-share fairness | 同一租户内同权或显式权重的 Job/class | 共同积压 weighted service lag、最坏 Job、work conservation | project bounded-ready + DRR/WFQ、external VTC-style | foreground SLO 或全生命周期 Jain |
| differentiated service | foreground priority 高于 bulk | foreground SLO isolation，同时 bulk starvation/退化有界 | project bounded-ready + strict-priority/EDF；project frozen-static reference | 要求两类延迟相等或只报 aggregate throughput |

当前 long bulk + 5s 后 foreground 实验属于 differentiated service。bulk 的 30s miss rate 在没有
外部应用合同证明该 deadline 合理前只作相对 static 的保护 guard；bulk headline 优先使用
reserved-share JCT、最大正 lag、最长 no-service 和 token/work goodput。另设 equal-share 场景时，
才用相同权重检验 DRR/VTC 式服务公平。

1. 加权服务 Jain。共同积压窗口内令 $x_j=S_j/\phi_j$：

   $$
   J_{service}=\frac{(\sum_jx_j)^2}{|B|\sum_jx_j^2}.
   $$

   只在按权重归一化后的实际服务量上计算；不直接对原始 TTFT/JCT 做 Jain。

2. 理想 GPS 服务滞后：

   $$
   G_j(t)=\int_0^t\rho_j(\tau)dS_{total}(\tau),
   \qquad
   Lag_j(t)=G_j(t)-S_j(t).
   $$

   报告最大正 lag、P95 lag、按 $W_e$ 或 service quantum 归一化的 lag、phase change 后的偿还
   时间、`min/mean` 与 `max/min`。没有定理时只称 empirical service lag，不能称 VTC
   service-difference bound；Jain 接近 1 仍可能隐藏一个 Job 的长时间无服务。

3. 用户体验隔离。每个 Job 必须有三个反事实，而不是只选一个有利分母：

   $$
   R_j^{full}=\frac{JCT_j^{policy,multi}}{JCT_j^{\mathrm{full\text{-}solo}}},\qquad
   R_j^{reserve}=\frac{JCT_j^{policy,multi}}{JCT_j^{\mathrm{reserved\text{-}solo}}},\qquad
   R_j^{sched}=\frac{JCT_j^{policy,multi}}{JCT_j^{static,multi}}.
   $$

   `full` 回答总体干扰，`reserve` 回答策略是否达到同 Job 独占名义份额时的经验性能，
   `sched` 隔离同一竞争条件下 scheduler 的增量。报告 max ratio、progress Jain、TTFT/JCT
   P95/P99 和 SLO miss；Jain 高但所有 Job 都同样慢不能称为好调度。$R^{reserve}\le1$ 只能称
   **reserved-share non-inferiority**，不是 DRF sharing incentive 或 Themis finish-time fairness
   定理，因为资源模型、离散请求和服务干扰假设不同。

   公平与隔离分开报告：公平看共同积压的 entitlement/service lag；隔离用固定 victim 的 matched
   normal→aggressor step/burst 对照，报告 `victim P99 ratio`、`victim goodput loss`、`victim SLO
   violation delta`、最大 waiting age 和 burst 结束后的 recovery time。victim manifest/arrival、
   总资源与服务配置必须不变，只改变 aggressor offered load。`group JCT` 定义为 group barrier/
   start 到最后一个 Job 完成的 wall time（makespan），不新增同义字段，且不得替代 per-Job P99。
   该 noisy-neighbor 对照不扩张当前 matched-observation gate 或首个 2-Job formal；只在它们闭合
   后，从已经登记的单个 held-out on/off/burst 场景中选择并冻结，避免重复发明 workload。

4. 饥饿和工作守恒。报告最长连续 no-service interval、最大 waiting age、未完成请求、以及
   “存在 eligible ready work 且
   健康 endpoint 仍有可用安全容量”的 avoidable-idle ratio。GPU idle 但无 ready work 不算
   调度器不工作守恒。

5. 效率—公平 Pareto。吞吐/goodput、tail、energy 与 service lag 分开报告，不压成一个 headline
   composite score。若 policy 相对 static 使所有 Job JCT 都不增、至少一个严格改善，同时
   aggregate 指标也不退化，可称“**相对该 baseline 和已观测维度的经验性 Pareto 改善**”；
   这不是 DRF 的 Pareto efficiency。Jain 下降但所有 Job JCT 改善时，应表述为“收益更不均”，
   不能只凭 Jain 宣称份额保证失败；最终结论仍由 service lag、保留份额与 SLO 共同决定。

   `slowdown Jain` 不作为新增 headline：对 slowdown 或 reciprocal progress 做 Jain 的结果依赖
   变换，且无法识别“所有 Job 同样慢”。保留现有 actual-work Jain/normalized-progress Jain 作为
   描述量；正式保护使用 max slowdown、worst-Job/class P99/SLO、service lag 与最长 no-service。

6. 数据与可计算性门禁。历史 compact CSV 可计算三个 JCT 反事实、worst Job 和 normalized
   progress，但不能无损重建动态 active set 下的 GPS lag、最长连续 no-service 或偿还时间。
   新 formal 必须保存 request ready/backlogged interval、completion event、actual completed
   work、权重/active-set 变化和 release/submit/complete 时间；缺一项就把对应字段记为
   `unavailable`，禁止从 phase aggregate 插值制造公平轨迹。当前
   `cumulative_service_disparity` 是 empirical 描述量，不冒充理想 GPS lag 或理论上界。

   backlog 起点必须按评价层区分：用户 E2E 等待用 `arrival→completion`；上游调度公平用
   `concrete-ready/credit-registered→completion`。source/Daft 尚未物化、coordinator 不可选择的
   时间不能算作 scheduler 欠下的 service。当前 completion ledger 只在 request 完成时增加 actual
   work，故指标必须命名为 `completion-accounted empirical service lag`；除非获得 per-Job 连续
   token service trace，不能写成 engine 内连续服务曲线。

   request latency 至少按 Job/class 报 P50/P95/P99，并拆分 ready→registered、registered→grant
   （admission wait）、grant→submit、submit→completion；能取得 request-level engine 指标时再报
   per-class TTFT/TPOT/ITL。aggregate vLLM histogram 只能解释模型服务总体状态，不能代替
   foreground/bulk 用户体验。SLO goodput 同时报 request/s 与 token/work/s，避免短请求数量占优。

7. 多资源边界。图像 prepare CPU work、ready bytes 和 GPU model work 先分别作为 stage
   mechanism 指标。只有校准出同时可消费的资源向量、每种容量的归一化单位及 dominant share
   后，才增加 DRF-style headline；当前 scalar token/frame work 与阶段利用率不能称 dominant
   resource fairness。

   bounded-ready 只证明 active request/work envelope 不扩大，不自动证明总缓冲成本相同。每个
   formal cell 必须报告 ready request/work/bytes 的 mean/P95/max、host memory、coordinator CPU、
   registration→grant tail 与随 Job 数的增长。文本 token work 不约束 payload bytes；图像扩展若
   没有 byte-bounded queue，不得声称与 static 具有同一 memory/backpressure envelope。

8. 可行性裁决。三个 JCT 反事实只需现有 matched controls，立即可行；event-level empirical
   lag/starvation 可在不修改 vLLM 的前提下离线重放 completion/backlog ledger，工程上可行，
   但前提是 coordinator 能看到完整 bounded ready set。2026-08-13 bounded-priority gate 已证明
   “只暴露一个 Job head”会造成 observation gap，因此该 gate 也适用于公平指标本身：ready/
   backlogged 语义不完整时，lag 必须 unavailable。当前不合理的目标是直接承诺 VTC 式端到端
   service bound 或 DRF dominant-share 定理——上游不能抢占已进入未修改 vLLM 的请求，输出
   work 未知，内部 continuous batching 还会改变完成顺序。可行的论文承诺是约束下经验评价，
   以及在明示不可抢占/估计误差假设后，对**上游 release/credit 层**证明有限性质；不能把后者
   自动外推为模型服务端到端公平保证。

#### 5.2.11 交叉验证、baseline 与淘汰门

所有正式臂冻结未修改 vLLM 和相同内部配置，但执行所有权严格分层：direct bounded HTTP 是
同协议 saturation ceiling，不是公平策略；Daft Native、Ray Data 和产品路径是**原生系统
baseline**，必须由各系统拥有 batching/backpressure/scheduling，禁止接入项目 coordinator、
credit 或 bounded-ready；project frozen-static 是同栈静态 reference，不冒充原生 baseline。

`BoundedReadyWindow` 只存在于 Project 路径：它把已经 concrete-ready 的有限多个请求暴露给
项目 selector。算法因果比较时，project bounded-ready + global FIFO、DRR/WFQ、external
VTC-style、strict-priority/EDF 与 proposed 必须使用**同一个 bounded ready-window、同一 active
K/W 和同一 ready bytes 上限**。FIFO、DRR/WFQ、external VTC-style 是已有算法思想，但此处
可执行版本由 Project shared-credit coordinator 实现，不是 Daft/Ray Data/upstream vLLM/VTC
artifact 原生实现。no-bounded-ready/single-head 版本是项目内标准算法 controls；接入统一
Project ready-window 的副本只是在候选集相同条件下比较 selector，身份是 matched-observation
controls，不代表 bounded-ready 属于这些算法。strict-priority/EDF 是 SLO 上界 control。这些
Project harness 路径均不进入 vendor-native 系统排名；旧 single-head `saor_release` 只保留为
observation-gap 定位臂。

`saor-v0.5.2` 的 $0.125W_e$ 虽已通过双轮 development gate，但其 observation/execution path
与 selector 同时改变；因此 formal 前的证据与归因分层如下：

1. **项目内部 matched-observation selector gate（当前先运行）**：在冻结 2-Job workload 上做
   1--2 个 rehearsal，至少比较 project bounded-ready + FIFO、DRR/WFQ、strict-priority 与
   proposed；external VTC-style 在 event accounting 可复用时加入。所有 Job 应用同一 ready
   window，只改变选择器。这里的 FIFO/DRR/VTC-style 必须标成已有算法的
   `Project implementation + bounded-ready matched-control`，不能称 vendor-native/system
   baseline。该 gate 只回答 SAOR 是否被简单 selector 击败，
   不能单独证明完整系统相对 Daft/Ray 的价值；
2. **系统级 native matched comparison（下一阶段必做）**：在同一 2-Job immutable workload、
   Job 级 `bulk@0s → foreground@5s` 且 Job 内 eager 的共同原生到达合同、PG source、模型、
   vLLM FCFS 服务签名、协议和 correctness 合同下，分列
   Daft `prompt()` Native、Daft `prompt()` Ray（两者均可执行时）、Ray Data native graph、
   project frozen-static 与 proposed $0.125W_e$。Project 两臂冻结相同 K/W；原生臂保留官方
   batching/backpressure/scheduler，不注入 Project K/W/credit/bounded-ready，但共享相同物理
   CPU/GPU/endpoint 包络并使用预注册的原生 calibration。报告 E2E throughput/MFU、group JCT、
   per-Job JCT、资源与 correctness；逐请求 P99/SLO 只有在对应原生臂提供共同真实时钟时才报告，
   否则显式 `unavailable`。该矩阵禁止在计时前读取 JSONL；PostgreSQL scan/materialization 必须
   位于共同 source→validated-gather 计时边界内，主性能矩阵统一 `writeback=none`。这里只能声称
   完整系统的经验表现；详细冻结规格见
   `../../code_doc/superpowers/specs/2026-08-13-saor-native-system-matched-comparison-design.md`；
   因共同原生到达形态与旧 selector rehearsal 不同，同批基础设施另跑 1--2 次短的 Project
   bounded-ready FIFO/DRR/VTC-style/SAOR 四臂 same-regime sanity block。它只检查 arrival regime
   是否改变 selector 排序，不是 native baseline、不是 selector formal，也不新增参数；
3. **no-bounded-ready control 与 observation 桥接（已完成）**：双轮 6/6 cell、0 incident。
   `frozen-static → single-head + shared FIFO` 使 tok/s +25.96%、group JCT −20.58%，但 fg P99
   +99.17%、fg violation +95.90 pp；`single-head + shared FIFO → bounded-ready + FIFO` 再使
   tok/s +7.30%、fg P99 −33.62%，但 fg violation 仍约 39.7%。这证明共享容量、ready exposure
   和 selector 是三项独立效应；所有 FIFO 臂均为 Project implementation，不是原生系统 baseline。
   若报告完整 SAOR 包相对 DRR/VTC 包，还须纳入 Project no-bounded-ready DRR/VTC-style；
4. **项目 formal**：project frozen-static reference、最强 bounded-ready internal controls 与
   proposed $0.125W_e$ 为核心；direct ceiling 和原生系统另表，不把不同 scheduler owner 混成
   selector 排名。使用 1 warm-up + 3 个 balanced/interleaved repeats，$0.25W_e$ 只保留
   development rejected ablation；
5. proposed 只有相对最强 bounded-ready 简单 internal control 至少改善一个预注册主要指标、其余 protected
   metrics 通过 non-inferiority/SLO margin，才能把 selector 写成独立贡献。若简单策略已在同一
   Pareto 前沿，贡献收敛为 bounded ready-state exposure + 最小 guarded release，或淘汰复杂
   selector；不更换 workload 追正。

历史 Daft/Ray 原生多 Job 数据只有在 manifest SHA/顺序、arrival offset/scale/barrier、模型与
服务签名、PG source/sink 和计时边界、协议/输出 cap、硬件/endpoint、correctness 以及上述指标
schema 全部一致时才能复用；缺一项就重跑，不能把不匹配的历史数字拼进系统级表。

2026-08-13 matched-observation selector gate 已从两个独立 root 完成并归档：12/12 cell、
12,288/12,288 requests、0 incident，completion-accounted registered-ready ledger 对五个
bounded-ready 臂均完整。DRR/VTC-style 双轮均值约 12.90K tok/s、foreground P99
27.23/26.16s、30s SLO violation 0；proposed 为 12.28K tok/s、foreground P99 17.85s、SLO
violation 0。相对 VTC-style，proposed 的 foreground P99 −31.78%、lag P95 −11.67%，但吞吐
−4.81%、bulk JCT +5.15%、longest no-service +22.68%。因此它是两轮观测中的非支配折中点，
不是 selector victory。固定顺序、每臂 n=2，且 selector 级 protected margins 未在看结果前精确
冻结，汇总保持 `selector_victory_decided=false`、`formal_authorized=false`；不事后调阈值授权
formal。即使完整 Project 系统相对原生框架更快，也不能把全部收益归给 guarded-debt selector。
完整报告见 `experiments/results/state_aware_work_unit/saor_matched_ready_selector_rehearsal_20260813/`。

dynamic capacity 保持 `parked-conditional`；若未来恢复，才独立比较 frozen lower/upper、
state-observed no-op、threshold/deadband、governor 和 offline oracle。

验证顺序保持“单一动作先行”，且 fixed-envelope release 与 dynamic capacity 分轨：

1. fixed-envelope `bulk-only → foreground-arrival → overlap → drain`：统一 runner 已接真实
   per-Job completion evidence、direct no-Job、project FIFO 与四个 credit 策略；所有 active-set
   臂先过外生错峰/overlap/exactly-once lifecycle gate；foreground-first 只作结果字段，不筛选
   baseline。只有 credit 臂再过 borrow/reclaim/work-conserving-drain mechanism gate，且
   post-drain 按任一 Job 先退出后的剩余 Job 检查：若仍有 waiting request，则其 endpoint-local
   队首必须被 request slot 或 work slack 至少一项阻挡；若明明装得下仍等待则失败。readiness
   先证明
   foreground 到达前每 endpoint 的 bulk predicted ready work 至少覆盖一个完整 work envelope，
   static/direct 的 mechanism 为 not-applicable，不得误判失败；
2. proposed 同时超过同 ready-window 的 project FIFO 与 DRR/WFQ internal controls 的 Pareto 前沿后，先做当前 2-Job formal，
   再选一个不调参 held-out（reverse/simultaneous arrival、on/off burst 或 prefix-rich 三选一）；
   held-out 通过后才进入 four-Job、3:1 weighted 与图像泛化；
3. 若 FIFO/DRR 已足够，淘汰 SAOR；不运行 dynamic capacity，也不换 workload 追正；
4. 只有 fixed-envelope release 通过后才加入 routing、prefix bonus、priority 和图像 HSE 泛化；
5. dynamic K 仅在被重新激活时先跑独立 finite-horizon oracle，未证明机会则永久停止。

动态候选只有同时满足下列条件才晋级：

- 与 frozen-static 保持同最大 request/work 上限、source/sink、manifest 和服务配置；
- 与 bounded-ready project internal controls 保持同 ready-window、ready bytes、selector 可见 active set 和运行顺序；原生 baseline 不适用该条；
- correctness、exactly-once、质量、feeding-saturation 和稳定性门禁通过；
- 至少一个预注册主要指标改善约 5%；每个 protected metric 必须在 run 前写明方向、
  non-inferiority/SLO margin 和统计汇总方式，并全部满足。通用 5% effect-size gate 与具体保护
  边界是两件事，禁止事后以“无不可接受退化”放宽；
- 相比简单 threshold controller 的 recovery/overshoot/regret 有可复现增量；
- 控制轨迹确实不同，非零动作覆盖足够时长；否则按 effect-range gate 停止；
- cost/状态估计换成 oracle 后仍无收益，则淘汰调度结构；oracle 有收益而在线无收益，则归因于
  estimator/controller，而不是更换 workload 追正。

统计上，所有“不恶化”使用运行前冻结的 non-inferiority/equivalence margin，不以两次均值接近
替代。正式报告列出三次全部值、sample CV、置信区间/配对差异和运行顺序；prefix cache ON 时
必须声明采用 warm-cache steady state 还是 cell reset，并保存每 cell cache counter 起止值。固定
顺序的 development rehearsal 只作方向筛选，即使 cache hit 没有单调漂移也不能代替顺序平衡。

#### 5.2.12 相关工作边界与可写贡献

相关工作不能只围绕 VTC。当前评价与设计边界至少来自四组互补文献：

- **共享资源与数据库多租户**：DRF 定义 sharing incentive、strategy-proofness、envy-free 与
  Pareto efficiency；Pisces 强调全局 weighted fairness、work conservation 与隔离；DRFT 把
  accurate incremental resource usage、share guarantee 和 admission control 带入多租户事务。
  它们约束“份额保证应如何表述”，但本项目当前还没有多资源 dominant-share 定理；
- **作业完成与未知时长**：Themis 用独占/共享 finish-time 比约束训练作业；Tiresias 用
  attained service、JCT 与 starvation promotion 处理未知时长；Pollux 以 useful progress/
  goodput 连接资源分配与效率。它们支持 reserved/full solo 反事实和最坏 Job 评价，但 gang
  placement、训练收敛效率不能直接迁移到不可抢占的 vLLM request release；
- **LLM serving 公平与 SLO**：VTC/DLPM 提供 token/service disparity、work conservation 和
  prefix-locality 冲突；Sarathi-Serve、DistServe、Llumnix、JITServe、SCORPIO 与 ProServe 提供
  SLO goodput、TTFT/TPOT/TBT/P99、输出长度不确定性、priority/isolation 评价。它们大多位于
  serving 内部，本项目只迁移 work accounting、预测/oracle 消融与指标，不把其内部调度结果
  当作同层 executable baseline；
- **上游/程序级调度候选**：EWSJF 的 upstream mixed-workload 排序、Equinox 的预测/双维公平、
  CONCUR 的外部 congestion admission、Agentix 的 program-level attained service/JCT，以及
  BatchGen 将 batch inference 作为独立执行模式，支持把 database Job/batch 而非孤立 request
  设为一等评价对象；它们用于校验上游定位和 baseline，而不自动构成项目创新。

一手入口：

- VTC：<https://www.usenix.org/conference/osdi24/presentation/sheng>；
- DRF：<https://www.usenix.org/conference/nsdi11/dominant-resource-fairness-fair-allocation-multiple-resource-types>；
- Pisces：<https://www.usenix.org/system/files/conference/osdi12/osdi12-final-215.pdf>；
- DRFT：<https://doi.org/10.14778/3742728.3742751>；
- Themis：<https://www.usenix.org/conference/nsdi20/presentation/mahajan>；
- Tiresias：<https://www.usenix.org/conference/nsdi19/presentation/gu>；
- Pollux：<https://www.usenix.org/conference/osdi21/presentation/qiao>；
- DLPM：<https://arxiv.org/abs/2501.14312>（预印本）；
- Agentix：<https://www.usenix.org/conference/nsdi26/presentation/luo>；
- Sarathi-Serve：<https://www.usenix.org/conference/osdi24/presentation/agrawal>；
- DistServe：<https://www.usenix.org/conference/osdi24/presentation/zhong-yinmin>；
- Llumnix：<https://www.usenix.org/conference/osdi24/presentation/sun-biao>；
- JITServe：<https://www.usenix.org/conference/nsdi26/presentation/zhang-wei>；
- SCORPIO：<https://arxiv.org/abs/2505.23022>（预印本）；
- ProServe：<https://arxiv.org/abs/2512.12928>（预印本）；
- BatchGen：<https://www.usenix.org/conference/osdi26/presentation/xu-tairan>；
- EWSJF：<https://arxiv.org/abs/2601.21758>（预印本）；
- Equinox：<https://arxiv.org/abs/2508.16646>（预印本）；
- CONCUR：<https://arxiv.org/abs/2601.22705>（预印本）。

因此不能把 MaxWeight、虚拟队列、weighted-token fairness、上游 adaptive scheduling 或
continuous replenishment 单独声明为新算法贡献。可验证的项目贡献候选收紧为：

> 在不修改 vLLM 的前提下，为数据库/Daft/Ray 上游建立有限、生命周期正确且 byte/work-bounded
> 的 ready-state exposure contract，再将 Job 存活集、priority/SLO 与 completion-accounted
> service debt 映射为固定 active envelope 内的 guarded ordered release，并与 vLLM FCFS
> continuous batching 组合；selector 是否构成独立增量必须通过项目内部 matched-observation
> attribution ablation，
> 状态感知容量选择只在独立 oracle/safety gate 通过时作为可选扩展。

该定位属于 **new setting + mature-method transplantation**。当前 idea 审核结论为
`Accept with Revisions`：工程可行性较高，主要风险是 prior overlap、observation/selector 归因
混杂、范围过大以及理论假设无法在实际 vLLM 服务曲线上闭合。当前论文核心优先收敛为
“bounded ready-state exposure + fixed-envelope guarded ordered release”；公平是约束，token
organization 是输入，多模态是外部有效性验证。若同 observation 的项目简单 selector 已达到同一前沿，
删除不必要的复杂 SAOR 算法叙事，而不是继续加参数。

#### 5.2.13 维护规则与决策日志

本节每次修改按以下字段追加，不删除被实验否定的旧版本：

| 日期 | revision | 变更 | 依据类型 | 状态影响 |
|---|---|---|---|---|
| 2026-08-11 | `saor-v0.1-design` | 首次冻结 FCFS ordered release、固定周期 DPP、三种 work、公平/SLO debt、证明义务与淘汰门 | 现有代码/实验事实 + 文献迁移 + 设计推导 | 保持 `design-candidate` |
| 2026-08-11 | `saor-v0.1.1-design` | 撤销任何以 waiting 或 GPU utilization 单独判断健康/拥塞的表述；加入历史反例、五层状态指纹与联合状态分类 | 历史 GPU/vLLM/Ray/图像实验事实 + 指标合同 | 保持 `design-candidate`；观测模型收紧 |
| 2026-08-11 | `saor-v0.1.2-design` | 纳入 phase-change 提前停止证据：增档动机成立、降档区未建立、phase 不等于状态复位；禁止固化 K/KV/waiting 阈值 | 独立结果分支正式报告 + CSV 审计 | 保持 `design-candidate`；重做 recovery/burst gate |
| 2026-08-11 | `saor-core-v0.1` | 实现中性 capacity、execution ledger、有限动作 DPP、公平债务和 Job-head ordered release；配置与模态适配外置 | 纯 Python unit/architecture tests | 不升阶；runner/replay/真实服务均未接入 |
| 2026-08-11 | `saor-v0.2-development` | 接入文本 shared-vLLM capacity adapter、配对 trace replay、state/action trace 与最大安全臂 validator；四臂真实服务 development gate 未过晋级门 | 6-sample regret replay + 2×4090 one-repeat 4-arm gate | 升为 `trace-validated`；capacity-only 标记 `not-promoted`，完整 SAOR 仍未验证 |
| 2026-08-11 | `saor-v0.3-design` | 将 fixed-envelope ordered release 与 slow Safe-Capacity Governor 分层；安全改为 hard feasible set，加入反事实模型、pipeline debt/hysteresis 和独立 oracle 淘汰门 | capacity-only 负结果 + MaxWeight/DPP、reconfiguration-delay 与 unknown-service 一手工作交叉审计 | SAOR-Release 保持 design-candidate；governor 降为 optional，不能继承 oracle theorem |
| 2026-08-11 | `saor-v0.4-design` | dynamic K 退出主线；固定总 envelope，仅动态调整 active-set entitlement、idle borrowing/reclaim 与 ordered release；新增 direct global FIFO/no-op control 和 Project DRR internal control | capacity-only 未胜 K160 + eager/online 两/四 Job static/shared 方向差异 + fatal-flaw audit | dynamic-K `reject and pivot`；SAOR-Release `accept with revisions`，决定性 controls 未过前不晋级 |
| 2026-08-12 | `saor-v0.4.1-runtime` | 接入 fixed-envelope Ray credit runtime、completion fairness debt、SAOR/shared FIFO 配置与 active-set phase audit；新增 direct merged-arrival no-Job control；K 全部改由签名化 calibration contract 注入 | 代码/单测与既有 calibration infrastructure | 仍为 candidate；无 GPU formal、SLO debt 和 theorem bridge，不晋级 |
| 2026-08-12 | `saor-v0.4.2-formal-ready` | direct no-Job 纳入同一交错 runner；新增 project/direct matched solo、request lifecycle 与 credit mechanism 分层门禁、rehearsal、静态 readiness audit 和 fail-closed formal summary | 单元测试 + 静态合同；未运行服务器 formal | 工程达到可 rehearsal/formal 状态，但仍无 GPU 策略结果；direct 只匹配 request K，不伪称 work-credit 等资源臂 |
| 2026-08-12 | `saor-v0.4.3-server-ready` | 服务器 preflight 发现 512-row manifest 原始 arrival span 约 66,880 s，模板写死 `arrival_time_scale=1.0` 会使 rehearsal 约 18.6 h；改为 workload 合同注入 scale，并由 readiness 自动计算 effective span、设置运行预算门禁 | 服务器真实 immutable manifest + 本地/服务器静态门禁 | 修正运行可行性，不改变算法、K 或证据结论；仍需 GPU rehearsal 后才能启动 formal |
| 2026-08-12 | `saor-v0.4.4-transport-contract` | 第二次 2×4090 rehearsal 前八个 cell 完成，但 `solo_direct_bulk` 出现单请求 `ReadError`；服务端健康且日志为 200，定位为 direct 漏配已在 Ray actor 验证的 idle keepalive 合同。将 expiry 改为两路径共享、可注入和可审计，仍禁用 retry | 真实服务器 rehearsal + endpoint health/log + 既有 tail-drain 复现 | 传输合同修正，不改变 SAOR 算法或性能结论；旧 8/10 rehearsal 判失败，完整新目录 rerun 通过前禁止 formal |
| 2026-08-12 | `saor-v0.4.5-active-set-supply` | 修复后真实 rehearsal 10/10、0 incident、exactly-once 通过，但 0.001 replay 的 5 s 前置阶段每 endpoint 仅约 10K bulk work，无法占满 65,536 envelope；四 credit 臂 pre-borrow 均未发生。公共 lifecycle gate 删除 outcome-dependent 的 foreground-first 条件，新增 per-endpoint pre-foreground work-envelope readiness；相同 manifest 冻结 0.0001 burst scale 复验 | 真实 group/request/credit trace + immutable manifest 离线供给计算 | correctness 通过、mechanism 未通过；不跑 formal。foreground-first 改为结果字段，禁止按性能结果过滤 baseline；新 burst rehearsal 通过前不晋级 |
| 2026-08-12 | `saor-v0.4.6-work-conserving-gate` | 0.0001 burst rehearsal 再次 10/10、0 incident、六臂 overlap、四 credit pre-borrow=95.0%；旧 post gate 却只检查 bulk 且强制剩余 active work >50%，即使 FIFO/DRR 中 bulk 先结束，或 coordinator waiting work 已为 0。改为任一 Job 先退出后的 endpoint-local head-fit 条件，并让 rehearsal runner 自身 fail-closed | 真实 per-request completion + per-endpoint active/request/work/waiting/head-work credit trace + 单元反例 | 修正因果机制判据：包络是不可分 request 的二维上限；只有 waiting head 同时装得进 request/work envelope 却未释放才判非工作守恒。须用新 commit 完整 rerun后才能称 formal-ready |
| 2026-08-12 | `saor-v0.4.7-reservation-candidate` | fixed-envelope 2-Job formal 40/40、0 incident、exactly-once；SAOR/FIFO mechanism 3/3，但总 gate 因 DRR/VTC rep2 无 post-drain 样本 fail-closed。SAOR 在 credit 臂内 fg 最好，仍显著落后 static。第一性原理审计确认无 reservation、不可抢占的 release-only 控制存在到达后回收下界，且 formal 的 `slo_weight=0` | 3-repeat GPU formal + credit/request trace + matched solo + 实现审计 | 状态升至 `formal-evaluated / fail-closed / directional-only`，不晋级 proposed；下一候选收紧为 reservation-backed release，先过 strict-priority 可达性、reserve 曲线和 static 非劣门，未闭环前不跑 4-Job |
| 2026-08-12 | `saor-v0.4.8-resolution-aware` | 将 post-drain 的可检验性绑定到 250 ms trace 周期；完成间隔低于该周期且区间内无样本时记为 `not_applicable`，有窗口/样本时仍 fail-closed 要求工作守恒。legacy compact evidence 仅在 lifecycle、borrow、reclaim 和无 fit violation 全满足时重分类 | 单元反例 + 已归档 `group_runs.csv` 离线回放 | compact mechanism 12/12 effective pass；显式 `full_formal_validation_updated=false`，不改原始 validation、不改变性能结论 |
| 2026-08-12 | `saor-v0.4.9-release-upper-bound` | 增加非抢占 foreground strict-priority：前台 Job 首次注册后只把新释放 credit 给前台，前台生命周期结束后恢复 bulk；priority 与 fairness weight 分离，并进入 group evidence | shared-credit/scheduler/config/runner 单测 + fail-closed readiness/summary 单测 + 两轮 GPU rehearsal | release-only 可达：fg P99 14.27s、SLO 0%，但仅 development diagnostic；下一步给 hard priority 加 bounded window/service-lag guard |
| 2026-08-12 | `saor-v0.4.10-resolution-aware-full` | 默认 formal summarizer 写出 resolution-aware v2、采样周期、完整 validation 更新标志与 legacy 重分类清单；在服务器完整 artifact 上旁路重汇总 | 本地/服务器 6 个真假阴性回归 + source SHA 绑定 | validation passed、credit effective 12/12；原 failed 文件保留审计，性能排序不变，SAOR 仍 not-promoted |
| 2026-08-12 | `saor-v0.5-bounded-priority-design` | 将后继冻结为通用有界词典序 release：显式 per-Job priority/剩余 SLO 预算、completion-corrected actual-work debt cap、单 recovery lease、guard drain/普通 priority fitting-head fallback 与 event-level 机制证据；首轮只做 2-Job 两个 cap | formal/strict-priority GPU 证据 + 实现断点审计 + DRR/VTC/EDF 理论边界 | 仅设计冻结；尚未实现/短测/证明，SAOR 保持 `formal-valid/not-promoted`；reservation 降为通过 guard 后的鲁棒性消融 |
| 2026-08-12 | `saor-v0.5.1-reclaim-barrier` | 把 guard drain 收紧为只面向一个 debt-critical ready head 的 reclaim barrier；recovery 发出后立即解除全局 guard；首轮 cap 改为 0.125K/0.25K，bulk slowdown 降为诊断；实现 selector/coordinator/SLO plumbing/timeout cleanup/Ray lossless ledger/readiness/two-round summary | 设计复核 + 本地受影响套件 291 tests + compile/diff/secret scan | 本地代码与证据工具完成；GPU rehearsal 因服务器关机未运行，状态 `development-unrun/not-formal-registered`；避免无限 hold 与 0.50K 近似无限 cap 的低信息量实验 |
| 2026-08-13 | `multi-job-eval-v1` | 将公平评价从 VTC+Jain 单中心扩展为三个 JCT 反事实、共同积压 service lag、starvation/work conservation、SLO 与约束下 Pareto；区分经验性 baseline-relative 改善和 DRF/Themis/VTC 理论性质 | DRF/Pisces/DRFT、Themis/Tiresias/Pollux、VTC/DLPM、Sarathi/DistServe/Llumnix/Agentix 一手文献 + 现有四 Job compact evidence 可计算性审计 | 不改变 SAOR `formal-valid/not-promoted` 状态；新 formal 增 event-ledger 证明义务，历史四 Job 只重解释、不补造 lag |
| 2026-08-13 | `saor-v0.5.1-ready-set-gap` | 按冻结四臂执行两轮 GPU gate，并用 lossless event + request trace 分解 selector→submit→service | Round 1 clean；Round 2 0.25K debt-recovery=0 被 fail closed；8 个 cell 全部 exactly-once、GPU mean≥95.8% | 两 cap foreground 门均失败，formal 不注册。所有可见 fg head 均获 priority，但单-head pull 使完整 ready backlog 间歇不可见；下一修订先改 observation contract，不扫 cap、不扩 4-Job/reservation |
| 2026-08-13 | `saor-v0.5.2-bounded-ready-local` | 新增独立 `saor_bounded_ready` observation contract：每 Job 预注册已到达的 concrete request 有界 ready set，request 上限由 effective K 派生、work 上限由 endpoint 数×W 派生；新增 ready→register→grant→submit→completion lifecycle；coordinator 在同一无损事件域记录 register/grant request ID+epoch，旧单-head policy 不变 | 本地 targeted unit/architecture tests + compile/diff；尚未运行 GPU development rehearsal | 状态仅 `local-implemented/development-unrun/not-formal-registered`。异常退出只撤销未提交 waiter/lease，已提交 request 的 credit 保留到整组 fail-closed cleanup，避免服务端仍执行时容量超卖；未过双轮 ready-set 门前不做 formal、reservation、4-Job 或动态 K |
| 2026-08-13 | `saor-v0.5.2-bounded-ready-gated` | 首次服务器运行因跨 trace 错误假设 `submit_epoch_s` 而 fail closed；按 `submission_id` 连接 submission 生命周期与 request submit 后，从两个全新 root 重跑冻结四臂 | 2×4090 两轮 development rehearsal；8/8 cell、0 incident；lossless event + group/request/submission/resource evidence | $0.125W_e$ 两轮全过并注册候选参数：12.36K tok/s、fg P99 17.58–18.15s、fg SLO 0%、bulk miss 65.8%–66.6%；$0.25W_e$ 因 bulk miss 74.4%–75.2% 两轮越界拒绝。尚不能把组合收益归因给 selector |
| 2026-08-13 | `saor-v0.5.3-attribution-review` | 审核确认 `saor_bounded_ready` 同时改变 observation/execution path 与 priority/debt selector；修正 cap 记号为 $H_B/W_e$，把 equal-share 与 differentiated-service 公平拆轨，并冻结同 ready-window 的 Project FIFO/DRR/VTC/strict-priority internal controls | 本地代码语义审计 + 双轮 gate + VTC/DLPM、Themis/Pollux、JITServe/SCORPIO、Agentix/BatchGen 文献迁移 | 保持 `development-gated/formal-registration-candidate`，但增加 `matched-observation-attribution-required`：先做 1--2 轮项目内部归因 gate，通过后才启动 1+3 formal；原生 baseline 不接 bounded-ready，不扫 cap、不扩 4-Job/reservation/dynamic K |
| 2026-08-13 | `saor-v0.5.4-matched-ready-observed` | frozen-static 与 bounded-ready FIFO/DRR/VTC-style/strict-priority/guarded-debt 在同 observation 下完成两轮；新增 completion-accounted lag、最长无服务、ready bytes/CPU/memory 重汇总 | 2×4090 双轮 development rehearsal；12/12 cell、0 incident；仓库 compact + 服务器完整 archive | proposed 相对 VTC-style 用 4.81% 吞吐、5.15% bulk JCT 和 22.68% no-service 代价换 31.78% fg P99 与 11.67% lag 改善；记为 observed nondominated tradeoff，不判 selector victory、不授权 formal。当时冻结的 bridge 下一行已完成 |
| 2026-08-13 | `saor-v0.5.5-observation-bridge-observed` | frozen-static/single-head shared FIFO/bounded-ready FIFO 在同 K/W、FIFO、manifest 与服务签名下完成双轮 | 2×4090 双轮 development rehearsal；6/6 cell、0 incident；Project implementation + compact/full archive | shared capacity 解释效率提升与 foreground 隔离损失；bounded-ready 额外提升效率并部分恢复 foreground，但 FIFO 仍约 40% SLO violation。bridge 完成，dynamic 证据收紧为 fixed-envelope Job 份额/ordering；下一步只做 native-system matched comparison |
| 2026-08-14 | `saor-v0.5.6-native-system-matched-local` | 八个唯一物理臂的本地合同/薄编排 + 两层离线 fail-closed 汇总；系统表 5 臂，Project sanity 表 4 臂，共享同一 SAOR run | 本地 synthetic/corruption unit tests；未连接服务器、未运行 GPU/rehearsal/formal | 只完成可执行基础设施。共同 Job release `[0,5]`、Job 内 eager、PostgreSQL source→validated gather 计时；FIFO 全名为 **Project bounded-ready + global FIFO matched-control**；native request tail 不支持时为 `unavailable`。后续只能按 runtime preflight→static readiness→small correctness/local fake rehearsal→review→单独授权 GPU execution 推进 |

状态只允许按以下顺序变化：

```text
design-candidate
→ trace-validated
→ observe-only-passed
→ single-action-gated
→ formal-evaluated
→ frozen-proposed 或 rejected
```

每次升阶必须在本节记录对应代码 revision、结果目录、通过/失败门禁和仍不可声称的结论。
开放问题保持显式：控制周期 $\Delta$、可证明的服务/预测误差界、Job 重入 counter-lift、
实际 arrival inversion、resource-work 系数、priority 是否值得保留，以及图像公平 work 的最终
定义。任何开放项不得在实现中用未记录的默认值静默决定。

#### 5.2.14 SAOR-only 删除门

当前**不能**直接删除其他所有策略后期待项目继续运行。惰性包入口已经保证“只导入 SAOR
核心”不加载 legacy policy，但生产调用仍有显式依赖：profiler 使用 adaptive/PID/static
admission 与 routing，shared-vLLM runner 使用 bounded capacity，图像多 Job runner 和 Ray
runtime 使用 shared credit，replay batching 使用 flush policy，通用执行路径仍使用
`SynchronousScheduler`。其中一部分是实验 baseline，一部分是可复用执行基础设施，不能都当作
“待删除的竞争算法”。

只有依次满足以下门禁，才可建立 SAOR-only 发布 profile：

1. 薄 runtime adapter 已完成文本 aggregate capacity actuation；仍需把 atomic snapshot、
   action builder、ordered publish、Ray completion 和实际 work correction 接成完整端到端
   路径。纯策略仍不得 import Ray/Daft/vLLM。
2. 文本 shared-vLLM 与图像 project runner 都显式选择 `saor` profile，并通过 exactly-once、
   capacity、failure/stale fallback 和配置 provenance 测试。
3. 对 `code/src`、`code/scripts` 做静态依赖扫描；除单独保留的 baseline/reproduction profile 外，
   production SAOR profile 对被删模块引用为零。
4. 在临时副本中实际移走候选 legacy 文件，运行全量 unit、Daft→Ray contract smoke 和一个
   bounded fake/HTTP E2E；不能只以 import SAOR 成功作为“项目可运行”证据。
5. 论文复现实验所需 baseline 默认归档并冻结，不从研究仓库物理删除；若只做部署制品，则由
   packaging allowlist 排除，避免破坏已有结果复现。

因此当前删除判断为 `not-ready`。下一工程动作应是接一个最小 `saor` runtime profile，而不是
先删策略文件再逐个修 ImportError。

#### 5.2.15 SAOR v0.5.1 bounded-priority implementation plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` to execute this plan task-by-task. Run every named red test before production code, keep the bounded-priority selector below 100 executable lines excluding dataclasses, and commit/push after Tasks 2, 5, and 7.

**Goal:** 在不改变 endpoint 总 request/work envelope、不可抢占语义和现有 SAOR fallback 的前提下，
实现“actual-work debt cap → 队首定向 reclaim barrier → SLO priority window → SAOR fallback”，并用
事件级账本消除 250 ms sampling 对 release 机制判定的假阴性。

**Architecture:** 纯函数只对一组 Job-head state 做词典序选择；engine-independent shared-credit
coordinator 持有 completion-corrected debt、单 recovery lease 与 hold episode；scheduler 只把
request arrival epoch 转成剩余 SLO 预算；Ray/client、profiler 和 shared-vLLM runner 只做 typed
transport、配置与证据落盘。首轮 workload 保持 2 Job，通用接口不从 Job 名称、到达次序或
`foreground` 字符串推断业务语义。

**Tech stack:** Python dataclasses/protocols、Ray named actor、`unittest`、CSV/JSON evidence、现有
shared-vLLM runner；不修改 vLLM 内部 scheduler，不新增第三方依赖。

**执行状态（2026-08-13）**：

| Task | 状态 | 证据/边界 |
|---:|---|---|
| 1–2 | ✅ 完成并推送 | 纯 selector、completion-corrected debt、单 recovery lease、队首 reclaim hold；commit `b59ce7e` |
| 3–6 | ✅ 完成并推送 | arrival epoch/剩余 SLO、timeout cancel、Ray lossless ledger、四臂配置/readiness、两轮 fail-closed 汇总；commit `60559d7` |
| 7 | ✅ 完成并推送 | 受影响套件 291 tests passed（仓库内固定临时目录绕过 Windows sandbox temp ACL），selector 89 physical/34 statement lines，compileall/diff/secrets passed；完整 discovery 1,154 tests 中 24 个因本机缺 Ray/Daft 或 Windows 无 POSIX `os.killpg` 报错，故不记 full pass；本机未安装 ruff，不临时装依赖；commit `8600044` |
| 8 | ✅ 双轮 GPU gate 完成、未晋级 | single-head bounded-priority 两 cap 均未过 foreground 门；$0.25W_e$ 第 2 轮机制门 fail-closed，定位为 ready-backlog observation gap；未启动 formal |
| 9 | ✅ bounded-ready 修订与双轮 gate 完成 | $0.125W_e$ 通过开发门，$0.25W_e$ 被 bulk guard 拒绝；后续同窗口 selector attribution 与 FIFO observation bridge 也已完成，SAOR 是观测非支配折中，不是 selector winner |

下方 checkbox 是已经执行完毕的历史复现清单，现统一勾选；真实 GPU 判决仍以 Task 8/9 和对应
results 报告为准。不得把 `formal_registration_candidate`、development 性能变化或观测非支配点
写成 formal/winner。

##### Task 1：先冻结纯选择器的可证伪语义

**Files**

- Modify: `code/tests/scheduling/test_saor.py`
- Modify: `code/src/scheduling/submission_control/saor.py`

- [x] 在 `test_saor.py` 先写以下失败测试：
  1. debt-critical 且 fitting 的 Job 一定越过 priority/soft-score Job；
  2. 最大 $F/H$ 的 ready debt-critical head 不 fit、且没有其它 fitting debt-critical head 时返回
     `guard_reclaim_hold`，`reclaim_debt=head_work-(K_work-active_work)`；
  3. debt-critical Job 已有 recovery lease 时不再发第二张 recovery lease；
  4. 无 ready head 的 debt-critical Job 不触发 hold；
  5. 普通 priority head 不 fit 时允许 fitting fallback，不制造 strict-priority idle；
  6. priority window 内按 `(priority, -remaining_slo_budget)` 选择；窗口外回退原 selector；
  7. debt guard 与 priority 同时触发时 `constraint_conflict=true` 且 debt 层优先；
  8. duplicate Job、非正 cap/window、head 超 envelope、缺 SLO budget 均 fail closed。
- [x] 运行红测：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_saor.py'
  ```

  预期：新增 import/test 因 bounded selector 尚不存在而失败；现有 SAOR 测试仍通过。

- [x] 在 `saor.py` 增加明确的 typed contract，不改变旧 `select_saor_release_job`：

  ```python
  @dataclass(frozen=True)
  class SaorBoundedHeadState:
      release: SaorReleaseState
      priority: int
      remaining_slo_budget_s: float | None
      priority_window_s: float | None
      debt_cap: float | None
      head_work: int
      ready: bool
      recovery_inflight: bool

  @dataclass(frozen=True)
  class SaorBoundedSelection:
      action: Literal["grant", "hold"]
      tier: Literal[
          "debt_recovery",
          "guard_reclaim_hold",
          "slo_priority",
          "saor_fallback",
      ]
      job_id: str | None
      reclaim_debt: int
      constraint_conflict: bool
  ```

  `select_bounded_saor_release(...)` 只做四层词典序，不读 Ray/Daft/vLLM；同层 tie-break 固定为
  debt ratio、priority/remaining budget、原 SAOR score、arrival order、Job ID。`hold` 只允许
  `debt>=cap && ready && !fits && !recovery_inflight` 的具体 head 触发。
- [x] 重跑同一命令，预期全部通过；用下式逐条核对反例，而不是只核对返回 Job：

  $$
  D^{reclaim}=\max\{0,w_{head}-(K^{work}-R^{active})\}.
  $$

##### Task 2：把 completion debt、单 recovery lease 和 hold episode 接入 coordinator

**Files**

- Modify: `code/tests/scheduling/test_shared_credit.py`
- Modify: `code/src/scheduling/submission_control/shared_credit.py`
- Modify: `code/src/scheduling/submission_control/__init__.py`

- [x] 先新增 coordinator 红测：
  1. `saor_bounded_priority` 只接受显式 priority/SLO/window/debt-cap 组合；同一 Job 配置改变即拒绝；
  2. estimated work grant、actual work completion correction后，公平债务严格符合
     $F_j^+=[F_j+\rho_jc-\mathbf 1\{j=k\}c]^+$；
  3. cap 后 fitting bulk head 得到 `debt_recovery`，在它实际 completion 前 recovery in-flight
     最大为 1；
  4. cap 后 non-fitting bulk head 建立一个 hold episode，foreground completion 释放到 fit 后立即
     发一张 recovery lease，并立即恢复普通选择；
  5. unfinished/no-ready bulk 和 non-fitting priority 都不能触发 guard；
  6. 任意 grant 后 `active_requests<=K_req`、`active_work<=K_work`；超 envelope 请求仍在入队前拒绝；
  7. `drain_release_events()` 恰好一次返回事件，第二次为空，避免 observer 重复计数；
  8. completed hold episode 的 duration/reclaim debt/target Job 可审计，未结束 hold 在 snapshot 中可见。
- [x] 运行红测：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_shared_credit.py'
  ```

- [x] 增加 `CreditLease.slo_deadline_s`、`SaorReleaseEvent` 和 coordinator 状态：稳定 per-Job
  priority/window/cap、endpoint-local recovery request key、当前 hold episode、单调 event sequence。
  新 policy 复用既有 `SaorReleaseConfig` 作为第 4 层 fallback，不扩 actor configuration tuple。
- [x] `_grant_bounded_saor_waiters` 每轮只执行一个 pure selection：grant 后写 event 并继续；hold 时
  只开启/维持一个 episode 后返回。相同 target/reclaim 状态的 polling 不重复写 event；当 target
  fit、消失或完成时关闭 episode并写一条带 duration 的 `guard_reclaim_hold` event。
- [x] `release()` 先移除完成的 recovery key，再用 actual work 更新 debt，最后重新 grant；这样
  “一张 recovery 在途”依据真实 completion correction，而不是 grant-time 估计。
- [x] 事件至少包含：`event_seq/event_time_s/endpoint_id/action/tier/selected_job_id/
  selected_request_id/target_job_id/head_work/reclaim_debt/hold_duration_s/constraint_conflict/
  ready_jobs/fitting_jobs/debt_by_job/debt_cap_by_job/recovery_inflight_by_job/active_requests/
  active_work/avoidable_idle/foreign_grant_over_debt_critical`。
- [x] 重跑 Task 1–2 测试并提交推送；commit message：
  `Implement bounded-priority SAOR core`。

##### Task 3：显式传播 arrival epoch 与剩余 SLO 预算

**Files**

- Modify: `code/tests/scheduling/test_scheduler.py`
- Modify: `code/tests/observability/test_postgres_profile_scheduling.py`
- Modify: `code/src/scheduling/core/models.py`
- Modify: `code/src/observability/profiling/replay.py`
- Modify: `code/src/scheduling/core/scheduler.py`
- Modify: `code/src/observability/profiling/ray.py`
- Modify: `code/src/observability/profiling/cli.py`
- Modify: `code/src/observability/profiling/config.py`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py`

- [x] 先写红测锁定时钟语义：`arrival_time_s` 仍是 workload-relative；replay 另写
  `BatchRequest.oldest_arrival_epoch_s`。scheduler 在 epoch clock=110、arrival epoch=100、
  target=30 时传 20 秒 remaining budget；不得计算 `110-relative_arrival`。
- [x] 写参数门禁红测：bounded policy 必须是 request granularity + arrival replay；priority>0 必须
  同时具有正 SLO target/window；cap 必须为正 work；shared-credit acquire timeout 必须为正。
- [x] 运行：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_scheduler.py'
  python -m unittest discover -s code/tests -t code -p 'test_postgres_profile_scheduling.py'
  ```

- [x] 给 `BatchRequest` 增加向后兼容的 `oldest_arrival_epoch_s: float | None = None`。arrival replay
  在已经计算 `intended_arrival_epochs` 的同一处用 `dataclasses.replace` 写入 envelope；request
  granularity 一行一 deadline，batch/quantum 只保存最老 arrival epoch。
- [x] `SynchronousScheduler` 增加 `job_slo_target_s/job_priority_window_s/job_fairness_debt_cap/
  shared_credit_acquire_timeout_s`。第一次 enqueue 前计算：

  ```python
  remaining_slo_budget_s = (
      self.job_slo_target_s
      - max(0.0, self.epoch_clock() - request.oldest_arrival_epoch_s)
  )
  ```

  remaining budget 允许为负；缺 arrival epoch 时有 priority window 的策略 fail closed。credit wait
  达到现有 transport/request timeout 时抛 `TimeoutError`，交给 runner 记录 incident，不加静默
  `max_hold` 回退。
- [x] profiler 新增 runner-owned flags：`--shared-credit-job-slo-ms`、
  `--shared-credit-priority-window-ms`、`--shared-credit-job-debt-cap-work`；bounded policy 自动把
  `--completion-request-timeout-s` 用作 acquire timeout。旧 policy 的默认值均为 disabled。
- [x] 重跑上述测试，预期 remaining budget、negative slack、timeout、legacy compatibility 全通过。

##### Task 4：Ray transport 与事件账本必须无采样假阴性

**Files**

- Modify: `code/tests/scheduling/test_shared_credit_ray.py`
- Modify: `code/tests/experiments/test_shared_vllm_experiment.py`
- Modify: `code/src/scheduling/runtime/shared_credit_ray.py`
- Modify: `code/src/experiments/shared_vllm/runtime.py`
- Modify: `code/src/experiments/shared_vllm/runner.py`
- Modify: `code/src/experiments/shared_vllm/metrics.py`

- [x] 先写 fake-Ray/runner 红测：bounded coordinator 配置能被旧 3-tuple、现 4-tuple兼容检查；
  `drain_release_events` 跨 actor/client 保序且不重复；runner 成功与失败路径都保存 events CSV。
- [x] 增加真假阴性回归：两个 grant/hold 事件间隔 5 ms，而 observer 仍每 250 ms sample，事件门必须
  正确计数；缺 event 文件不得用 credit snapshot 推测 priority/debt tier 已触发。
- [x] 运行：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_shared_credit_ray.py'
  python -m unittest discover -s code/tests -t code -p 'test_shared_vllm_experiment.py'
  ```

- [x] actor/client 暴露 `drain_release_events(endpoint_id)`；`_RayCreditObserver` 分别提供 sampled
  snapshot 与 lossless drained events。runner 在每次 sample、正常结束、异常落盘前三处 drain，
  写 `traces/<run_stem>.release_events.csv`；schema/version 与路径进入 group record/failure JSON。
- [x] `bounded_saor_event_summary` 只从 event ledger 计算：tier counts、hold count/total/P95/max、
  max reclaim debt、constraint conflicts、max recovery in-flight、avoidable idle、foreign-over-critical。
  250 ms credits trace 继续用于 phase/资源图，不再作为新机制是否触发的真值源。
- [x] 重跑测试，预期 5 ms 回归通过、事件第二次 drain 为空、失败 run 仍保留最后事件。

##### Task 5：配置冻结为显式 per-Job 语义和两档 cap

**Files**

- Modify: `code/tests/experiments/test_saor_shared_vllm_config.py`
- Modify: `code/tests/experiments/test_saor_formal_tools.py`
- Modify: `code/src/experiments/shared_vllm/config.py`
- Modify: `code/scripts/analysis/audit_saor_formal_readiness.py`
- Create: `deploy/autodl/saor_bounded_priority.example.json`
- Modify: `deploy/autodl/README.md`
- Modify: `deploy/README.md`
- Modify: `PROJECT_INDEX.md`

- [x] 先写配置红测：`saor_bounded_priority` 支持任意 Job 数的等长数组
  `priorities/slo_targets_s/priority_windows_s/debt_cap_fractions`；禁止由 offset/Job 名推断；
  priority>0 缺 SLO/window、cap fraction 不在 `(0,1]`、数组长度不等、bounded policy 缺
  `saor_release_control` 均拒绝。
- [x] 写 command 红测，2 Job 应精确生成：bulk priority=0、无 priority window、debt cap 分别为
  `8192/16384` work；foreground priority=1、SLO/window=30 s、无 debt cap；coordinator policy 为
  `saor_bounded_priority`。
- [x] 运行红测：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_saor_shared_vllm_config.py'
  python -m unittest discover -s code/tests -t code -p 'test_saor_formal_tools.py'
  ```

- [x] 新模板固定四臂且不加参数：`static_partition`、现 `saor_release`、
  `saor_bounded_priority_0125k`、`saor_bounded_priority_025k`。两档只改变 bulk
  `debt_cap_fractions=[0.125,null]` / `[0.25,null]`；两档均为
  `priorities=[0,1]`、`slo_targets_s=[null,30]`、`priority_windows_s=[null,30]`。
- [x] readiness 新增 `bounded_priority_development` profile，复用同一 immutable manifests、
  offset、K/W、model/protocol/calibration SHA，且检查 `request_slo_ms=30000` 与 foreground policy
  SLO 一致。该 profile 只准 `--rehearsal`；不把 1-run development 写成 formal。
- [x] 重跑 Task 3–5 测试，运行 template readiness，预期 `status=passed`、scenario_count=4；提交
  推送，commit message：`Wire bounded-priority SAOR evidence path`。

##### Task 6：实现两轮 development gate 汇总，不混淆结果与机制

**Files**

- Create: `code/scripts/analysis/summarize_saor_bounded_priority_gate.py`
- Modify: `code/tests/experiments/test_saor_formal_tools.py`
- Modify: `code/scripts/README.md`

- [x] 先用两个人工 matrix root 写红测；汇总器接受两次 `--matrix-root`，每个 root 必须是 clean
  rehearsal、四臂各一 run、相同 config fingerprint/commit/service signature。
- [x] 锁定硬门，不允许用 slowdown 误杀：

  | 维度 | 每个 cap 的判据 |
  |---|---|
  | correctness | 两轮均 0 incident、exactly-once、lifecycle/metrics/resources 通过 |
  | foreground | 两轮均 P99≤30.7 s、SLO violation≤0.01 |
  | efficiency | 两轮均 tokens/s≥9,984 |
  | bulk protection | 两轮均 SLO violation≤0.723；slowdown 只输出诊断 |
  | mechanism | priority/debt tier 均至少触发；avoidable idle=0；foreign-over-critical=0；max recovery in-flight≤1；event ledger 完整 |
  | stability | 两轮 gate 方向一致；输出 mean、全部单次值和 sample CV（可计算时） |

- [x] 单独写 false-negative 测试：event ledger 已证明 tier 触发时，即使 snapshot trace 没采到中间
  状态也应 pass；event ledger 缺失/sequence gap/duplicate 则 fail closed。
- [x] 输出 `gate_summary.csv`、`mechanism_summary.csv`、`validation.json`；结论字段只允许
  `formal_registration_candidate`、`diagnostic_only` 或 `constraint_conflict_stop`，不能输出 winner。
- [x] 运行：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_saor_formal_tools.py'
  ```

##### Task 7：全链验证、文档与安全提交

**Files**

- Modify: `code/INFRA_STATUS.md`
- Modify: `code/README.md`
- Modify: `learning/experiment_walkthrough.md`
- Modify: `experiments/plans/state_aware_work_unit_evaluation_20260808.md`
- Modify: `experiments/plans/experiment_status_and_gaps.md`
- Modify: `experiments/plans/README.md`
- Modify: `PROJECT_OUTLINE.md`
- Modify: `overview/current_direction_and_plan.md`
- Modify: `PROJECT_LOG.md`

- [x] 更新学习讲解，明确：release-only 非抢占下界、actual-work debt、为什么 barrier 只针对 ready
  head、为什么 recovery 只允许一张、priority/SLO 与 fairness 的词典序冲突、事件账本如何消除
  sampling false negative。
- [x] 运行受影响套件：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_saor.py'
  python -m unittest discover -s code/tests -t code -p 'test_shared_credit.py'
  python -m unittest discover -s code/tests -t code -p 'test_scheduler.py'
  python -m unittest discover -s code/tests -t code -p 'test_shared_credit_ray.py'
  python -m unittest discover -s code/tests -t code -p 'test_postgres_profile_scheduling.py'
  python -m unittest discover -s code/tests -t code -p 'test_saor_shared_vllm_config.py'
  python -m unittest discover -s code/tests -t code -p 'test_shared_vllm_experiment.py'
  python -m unittest discover -s code/tests -t code -p 'test_saor_formal_tools.py'
  python -m compileall -q code/src code/scripts code/tests
  ```

- [x] 再运行完整 discovery；本机缺 Ray/Daft/psycopg 等环境错误必须逐项列出，不能把部分套件写成
  full pass：

  ```powershell
  python -m unittest discover -s code/tests -t code -p 'test_*.py'
  ```

- [x] 检查 diff、文档入口与策略实现行数，再扫 secrets：

  ```powershell
  git diff --check
  python code/scripts/environment/scan_git_secrets.py --all
  ```

- [x] 提交推送，commit message：`Complete bounded-priority SAOR development gate`。commit 前确认
  无服务器 host、用户名、口令、runtime env、raw workload 或输出 artifact 进入 Git。

##### Task 8：远端只跑两轮 rehearsal，不启动长 formal（2026-08-13 已完成，未晋级）

**Files**

- Created after fail-closed run:
  `experiments/results/state_aware_work_unit/saor_bounded_priority_gate_20260813/README.md`
- Created after fail-closed run:
  `experiments/results/state_aware_work_unit/saor_bounded_priority_gate_20260813/raw/`
- Modify after interpretation: `experiments/results/README.md`
- Modify after interpretation: `experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`
- Modify after interpretation: `experiments/plans/experiment_status_and_gaps.md`
- Modify after interpretation: `PROJECT_LOG.md`

- [x] 远端先同步到 Task 7/入口修正 commit；使用仓库外 runtime env，保存只读机器报告：

  ```bash
  cd /root/autodl-tmp/ai-operator
  SAOR_REVISION=$(git rev-parse --short HEAD)
  set -a
  source /root/autodl-tmp/ai-operator-runtime.env
  source /root/autodl-tmp/runtime/saor-active-set-formal.env
  set +a
  PYTHONPATH=code /root/miniconda3/bin/python \
    code/scripts/environment/manage_environment.py check \
    --groups core,text,analysis \
    --json-out "/root/autodl-tmp/experiment-artifacts/saor_bounded_priority_preflight_${SAOR_REVISION}.json"
  ```

- [x] 按 `deploy/autodl/README.md` 只读检查 endpoint/PG/Ray/runner lease/GPU；Ray stale 已先停
  Ray 再只删除 `/tmp/ray/ray_current_cluster`。任何另一 runner、非空 vLLM waiting、endpoint
  配置漂移或 preflight failure 都停止，不安装依赖、不重启健康服务追结果。
- [x] 运行静态 readiness，随后用两个全新且预先确认不存在的输出目录各跑一次 `--rehearsal`：

  ```bash
  PYTHONPATH=code /root/miniconda3/bin/python \
    code/scripts/analysis/audit_saor_formal_readiness.py \
    --profile bounded_priority_development \
    --config deploy/autodl/saor_bounded_priority.example.json \
    --output /root/autodl-tmp/experiment-artifacts/saor_bounded_priority_readiness_20260812.json

  PYTHONPATH=code /root/miniconda3/bin/python \
    code/scripts/experiments/run_shared_vllm_experiment.py \
    --config deploy/autodl/saor_bounded_priority.example.json \
    --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
    --python-executable /root/miniconda3/bin/python \
    --output-dir /root/autodl-tmp/experiment-artifacts/saor_bounded_priority_gate_20260812_r1 \
    --health-url http://127.0.0.1:8000/health \
    --metrics-urls "$MODEL_METRICS_URLS" \
    --ray-address "$RAY_ADDRESS" \
    --rehearsal

  PYTHONPATH=code /root/miniconda3/bin/python \
    code/scripts/experiments/run_shared_vllm_experiment.py \
    --config deploy/autodl/saor_bounded_priority.example.json \
    --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
    --python-executable /root/miniconda3/bin/python \
    --output-dir /root/autodl-tmp/experiment-artifacts/saor_bounded_priority_gate_20260812_r2 \
    --health-url http://127.0.0.1:8000/health \
    --metrics-urls "$MODEL_METRICS_URLS" \
    --ray-address "$RAY_ADDRESS" \
    --rehearsal

  PYTHONPATH=code /root/miniconda3/bin/python \
    code/scripts/analysis/summarize_saor_bounded_priority_gate.py \
    --matrix-root /root/autodl-tmp/experiment-artifacts/saor_bounded_priority_gate_20260812_r1 \
    --matrix-root /root/autodl-tmp/experiment-artifacts/saor_bounded_priority_gate_20260812_r2 \
    --output-dir /root/autodl-tmp/experiment-artifacts/saor_bounded_priority_gate_20260812_summary
  ```

- [x] 用新汇总器同时消费 r1/r2；Round 2 非 clean 被汇总器拒绝，结论 `diagnostic_only`：
  1. 任一 cap 通过全部门 → 只注册后续 formal 候选，本轮停止；
  2. 两 cap 均 foreground 过、bulk/efficiency 失败 → 记 priority/fairness constraint conflict，停止
     cap 密扫；
  3. 两 cap 均 foreground 失败 → 回到 non-preemptive residual-work 下界，不能加大 SLO 软权重；
  4. 机制门失败 → 只诊断代码/供给/事件合同，不解释性能。
- [x] 结果 README 按项目七步结构记录设置、设计、合规、全表数据、事实/推断/不能声称、课题含义、
  下一步。短测明确标 `development rehearsal`；提交结果文档前再次跑 secret scanner。按用户要求
  不同步 Wiki。

**Task 8 事实判决**：0.125K 两轮机制可达但 fg P99/SLO 均失败；0.25K 第 2 轮
debt-recovery=0 且 fg 门失败。GPU/throughput/correctness 门排除欠供给。事件与 request trace
显示每个已注册 foreground head 都被优先选择，但 per-Job scheduler 同步等待 acquire 后才注册
下一 head，导致 coordinator 在相邻请求间看不到实际 ready backlog 并向 bulk fallback。按预注册
规则不启动 formal、不补第三轮、不扫描额外 cap。

##### Task 9：ready-set observation 修订（双轮 GPU gate 已完成）

- [x] 把观测状态拆为 source/Daft 尚未产出、scheduler concrete-ready、coordinator registered、
  granted active 与 vLLM running/waiting。submission trace schema 6 增加
  `ready_epoch_s/credit_registered_epoch_s/credit_granted_epoch_s`，并分别计算
  ready→register、credit wait、grant→submit；服务开始仍来自 request/service trace。

| 状态 | 所有者与当前证据 | freshness/有效区间 |
|---|---|---|
| source/Daft 尚未产出 | PostgreSQL/Daft source iterator；当前不由 coordinator 推断 | arrival/flush 事件时间；只表示尚未成为具体 request，不能计入 ready fairness backlog |
| scheduler concrete-ready | 每 Job `BoundedReadyWindow`；`ready_epoch_s` + ready count/work 峰值 | 进入窗口至 grant/取消；只含已经到达且携带完整 payload/estimated work 的 request |
| coordinator registered | named Ray credit actor per-endpoint Job queue；`credit_registered_epoch_s` | register 至 grant/取消；这是 selector 的真实候选集与公平共同积压起点 |
| granted active | coordinator active lease；`credit_granted_epoch_s` 与 lossless release event | grant 至 completion/release；grant→submit 单列，不能冒充已进入模型服务 |
| Ray/vLLM service | submission/request trace + vLLM time-series running/waiting/KV/TTFT | submit/service-start/completion 与 during-run 聚合；不以单次 GPU/vLLM snapshot 代替前四层 |
- [x] 采用独立 policy `saor_bounded_ready` 的 bounded concrete waiter pre-registration；旧
  `saor_bounded_priority` 保持单-head 行为作回归对照。窗口 request 上限从当前 Job 的有效 K
  派生，work 上限从 endpoint 数×共享 W 派生，不新增 workload 行数/offset/queue-size 旋钮。
- [x] 本地门已覆盖多候选先注册后提交、完整 request 不拆分、exactly-once/source-order、有限
  request/work、timeout/cancel、异常 lease cleanup、新旧 policy 区分、runner/config/static audit 和
  双轮汇总 profile；`max_ready_requests_seen/max_ready_work_seen` 记录实际窗口峰值。
- [x] release-event schema 2 对 coordinator register/grant 同时记录 request ID 与
  `event_epoch_s`；runner 先用各 Job submission trace 验证 concrete-ready lifecycle 完整，再在
  actor 的同一时钟域内按 request ID 配对 foreground register→grant。新机制门要求至少一个
  actor-side wait interval，且区间内 foreign `saor_fallback` grant=0；request join 不完整、时序
  错误或 event epoch 缺失均 fail closed。GPU 仍需验证该区间在真实 workload 中非空。
- [x] 复用独立 `deploy/autodl/saor_bounded_ready.example.json` 做两个全新 development rehearsal
  root，再以 `--profile bounded_ready` 汇总。首次 root 在审计阶段暴露跨 trace
  `submit_epoch_s` 假设并 fail closed；按 `submission_id` 连接后用新 commit/新目录完成两轮。
  $0.125W_e$ 两轮通过全部门，冻结候选参数；$0.25W_e$ 两轮 bulk miss 越界，停止。matched-
  observation 归因 gate 前不启动 formal；reservation、4-Job 与动态 K 继续阻塞。

**Task 9 事实判决**：bounded concrete-ready observation 修复了单-head 可见性断点。$0.125W_e$
相对原 SAOR release 两轮均值吞吐近似持平（−0.03%），foreground P99 从 55.28s 降到
17.87s，foreground SLO violation 从 89.2% 降到 0；bulk SLO violation 从 46.5% 升到 66.2%，
仍低于 72.3% 预注册上限。$0.25W_e$ 的 bulk miss 两轮均越界，不进入 proposed candidate。
该短测只支持候选参数冻结；由于 observation 与 selector 同时变化，不构成 selector 归因、
formal 胜出或公平定理。

##### Plan self-review（写码前必须再核对）

| 风险 | 本计划的约束 |
|---|---|
| 把 relative arrival 当 epoch | 新增独立 `oldest_arrival_epoch_s`，用 deterministic clock test 锁定 |
| guard 退化为 strict priority 无限留空 | 仅 debt-critical ready head 可触发；fit 后只发一张 recovery 即解除全局 hold |
| completion correction 前连续补 recovery | per Job/endpoint 最多一张 recovery in-flight，actual completion 后才清除 |
| 250 ms trace 造成机制假阴性 | 机制真值来自 lossless coordinator event ledger；snapshot 只做状态/资源图 |
| 为追结果在线改 cap/窗口 | 模板只含 $0.125W_e/0.25W_e$，窗口固定 30 s，两个全新 rehearsal root 后停止 |
| observation 改善误归因给 selector | formal 前所有 project selector internal controls 共享相同 bounded ready-window、active K/W 与 ready bytes；原生 baseline 保持自身调度 |
| slowdown 误作 bulk hard gate | bulk 只用 request-level SLO violation≤0.723；slowdown 单列诊断 |
| 旧 SAOR/strict-priority 被静默改写 | 新 policy 名与新 selector；旧 policy 和测试保持不变，作为对照 |
| 短测被写成 formal/winner | readiness/profile/summary/README 四处固定 development-only claim boundary |


## 6. 图像正式矩阵

### 6.1 强 baseline

1. bounded direct ceiling；
2. Daft built-in AI function；
3. Ray Data native API graph；
4. project typed Ray actor frozen-static；
5. project state-aware proposed（只在前四项统一合同通过后进入排名）。

统一 PostgreSQL BYTEA source、CLIP model/processor/dtype/L2 normalization、CPU/GPU reservation、到 gather 完成的 operator-E2E 边界、task quality 和 failure accounting。pgvector sink 只运行小规模 exactly-once/检索质量闭环，不进入性能主排名。vLLM pooling 只有在相同模型/语义可部署且通过 correctness gate 后才进入，否则保持 blocked 并说明原因。

### 6.2 图像 workload 形态

- uniform resized control；
- encoded-size/decode-cost controlled skew；
- easy→hard/hard→easy phase shift；
- burst arrival；
- two-job staggered 与 weighted mix。

图像 work descriptor 至少记录 encoded bytes、original dimensions、prepare-work estimate、model-work estimate、tensor bytes、locality/shape bucket 和 uncertainty。

### 6.3 图像指标

- correct embeddings/s、operator-E2E、first output、P50/P95/P99 batch/row JCT；
- CPU preprocess busy/queue、ready-tensor queue work、GPU queue/service/busy、H2D、buffer bytes；
- GPU utilization/MFU（若公式适用）、power/energy、J per 1k embeddings；
- exactly-once、retry/timeout/OOM、embedding finite/norm/digest；
- Recall@K、MRR 或 nDCG（使用冻结 ground truth）；
- multi-job JCT、SLO goodput、Jain fairness、isolation 和 borrowed work。

## 7. 开题使用边界

开题需要四条证据链都有动机现象和可执行对照，但不要求论文方法已经全面胜出。原生单 job 1+3 已完成：Daft Native/Ray 在当前官方 graph 下稳定 overqueue，Ray Data 当前冻结路径稳定 underfeed；它只证明问题形态，不证明项目方法胜出。2026-08-09 的 5s guaranteed-overlap 又完成了 Daft Native/Ray、Ray Data 的 short/long 独立 job 观察，以及项目 `static_partition` vs `shared_work` 的同上限因果 A/B。该最小文本缺口已经关闭，开题前不再扩大矩阵。

现有 token-work、active-work、图像 stage profile/matched-resource、1/2/4-job 和 429-run cost decision-quality 直接复用。图像 cost held-out 只在已有 profile 无法组成决策对照时才新跑；phase-change、3:1 weighted、第二硬件和更多 workload 不阻塞本轮开题证据闭环。

### 7.1 开题最小矩阵的冻结规模

| 组 | 冻结 workload/shape | arms/scenarios | 重复 | 目的 |
|---|---|---|---|---|
| 原生单 job（已完成） | ShareGPT controlled-skew held-out Chat manifest；同 model/service/output cap | bounded Chat、Daft Native、Daft Ray、Ray Data | 每臂 1+3，平衡交错 | 已冻结 underfeed/minimum-saturation/overqueue 三类外部状态；见 `opening_text_native_single_job_formal_20260808/` |
| 原生两 job 观察（已完成） | 两个 512 行 short/long job；offset=5 s；互斥且 endpoint-work-balanced manifest | Daft Native、Daft Ray、Ray Data 各自启动两个独立 job；不注入项目 credit。bounded 多进程 client 因可复现 CLOSE_WAIT 生命周期问题排除，单 job C128 仅作容量参照 | 每臂 1+3 | 三臂均产生真实 overlap，short JCT 相对各自 single +82.42%/+104.84%/+32.76%；只作外部竞争观察 |
| 两作业（已完成） | 两个512行short/long job；5s stagger；互斥manifest-selected doc_id集合 | online replay与eager两种arrival regime；各自使用full/half single、static partition、shared work-credit/fair queue匹配对照 | 每场景1+3 group runs | online下quota-only≈0、shared提高总吞吐但伤short/Jain；eager下quota-only short JCT +59.00%，shared相对static使short JCT −48.94%、总吞吐+31.85%、Jain 0.894→0.972；冻结为arrival-regime dependence，weighted留论文阶段 |

#### 四作业干扰补充合同（2026-08-09 用户确认）

四作业补充实验固定为 `short@0s → {long1,long2,long3}@5s`。四个 512-row
manifest 必须 doc_id 互斥、两 endpoint 等行；已冻结 short manifest 的 endpoint
prompt-work skew 为 3.58%，原生门禁按不重排同一输入的原则冻结为 ≤4%。三个 long
从 short 之外的行按 prompt-token work 贪心平衡，避免将输入工作量差异误判为调度
不公平。它是两作业结论的并发扩展，不替换两作业的最小因果证据。

对每个系统都先运行 `short/long1/long2/long3` 各自的 full-pool 单 Job 控制，再运行
四 Job 并发。Project 额外运行四个 reserved-quarter-pool 单 Job 控制，然后比较：

1. `single_full → static_4job`：总影响（静态配额 + 服务竞争）；
2. `single_full → single_quarter`：纯静态配额影响；
3. `single_quarter → static_4job`：匹配本地上限后的真实竞争影响；
4. `single_full → shared_4job`：共享 work-credit 下的总干扰；
5. `static_4job → shared_4job`：同全局 K/W 上限的调度策略因果对比。

结果解释再统一映射到 §5.2.10 的三个反事实：`policy/full-solo`、
`policy/reserved-quarter-solo`、`policy/static-multi`。历史 compact 结果只补这三类 JCT 比率；
共同积压 GPS lag、最长连续 no-service 和偿还时间必须来自新 formal 的无损 completion/backlog
ledger，不能从下述三段聚合事后推算。

逐 Job 必须报告 JCT、P95/P99（native adapter 无可靠 request timestamp 时明确不可用）、
actual work、work/s、相对自身 single-full slowdown、开始/结束和与其它 Job 的重叠时长。
三个 long 还要报告 slowdown/JCT 的 max-min、CV、最慢 Job、pairwise overlap 和完成顺序。
组级报告总 tokens/s、描述性的 Jain fairness、max/min service、GPU util、MFU、running/waiting/KV、
能耗、exactly-once；Project 按 `short-only / four-job overlap / long-only drain` 三段重算
完成 work rate 与服务状态。只有四个 Job 全部完成、实际 overlap>0、manifest 和资源
合同一致的 formal run 才进入比较。每场景 1 warm-up + 3 formal；短于60秒的单 Job
cell 仅作匹配 slowdown 的诊断基线，不作框架容量排名。

两作业必须使用冻结的 short/long manifest 直接过滤互斥 doc_id，source offset 固定为 0。
项目 A/B 按原始 `arrival_time_s` 做 request-level replay；原生 Daft/Ray Data 观察只按
0/5 s 对齐 Job 启动，Job 启动后完整 manifest 交给框架拥有的 graph，不声称逐行 replay。
两条轨道都报告各 Job 实际 predicted/observed work，但绝对 JCT/吞吐不得跨轨排名。
原生框架观察不得命名为 `static_partition`；只有项目 A/B 可计算
`borrowed_work_seconds`。该最小矩阵不声称 3:1 weighted fairness 已验证。
Daft Native、Daft Ray 和 Ray Data 均保持 vendor-owned graph：不为正式结果扫描
workload 重排、长度均衡、项目 credit、跨 Job 路由或调度参数。Ray Data 使用冻结的
官方 API 配置 `batch_size=16, concurrency=8/endpoint`；外层 shard 进程等待上限延长至
2400 s 仅保证 512-row 原生运行能够自然结束，不改变框架内部执行策略。

本轮到达方向严格为 `Short@0s → Long@5s`，回答“后到 long 是否影响已运行 short”。
所有进入干扰结论的 arm 都必须满足 measured overlap > 0；旧 15 s Daft Native 中 short
自然完成后 long 才到达，因而只能保留为 arrival observation。`Long→Short` 回答的是
“繁忙 long 背景下新到 short 的 SLO”，是不同的论文阶段补充场景，不能用本轮结果代替，
也不构成本轮开题最小因果链的缺口。

### 7.2 开题后项目性能诊断的两条独立轨道

当前项目 single-short 约 71 s、Daft Native single-short 约 11 s 不构成性能差距结论：
前者是 arrival-limited request replay，后者是 eager-manifest graph。若开题后需要判断
项目代码是否还需优化，先按以下顺序执行，不把诊断加入开题 blocker。

**轨道 A：同 replay 在线诊断。** 只比较 bounded HTTP replay control 与 project
frozen-static replay；使用同一 short manifest、endpoint pinning、Chat 语义、output cap、
temperature、`arrival_time_scale` 和逐请求到达时间。除 Job JCT 外，必须分解并报告：

- `arrival_span_s`：首个到达到最后到达；
- `post_last_arrival_drain_s`：最后到达到最后完成；
- completion lag P50/P95/P99；
- available/admitted/completed work、running/waiting/KV、queue/service 与 MFU。

arrival-limited cell 不套用离线 feeding-saturation ≥95% 门禁，改用 arrival fidelity、
exactly-once、同 offered load 和 matched-replay no-regression。若 project 的 drain/P99
相对 bounded 差异均小于约 5%，立即停止，不改调度行为；若显著更差，再一次只隔离
source/organizer、flush、Ray actor 或 routing 中一个因素，禁止先放大 K/W 或联合调参。

**轨道 B：离线饱和容量。** 所有 rows 在 `t=0` 可见，不做 arrival replay；在同一
Chat manifest/model/service/output 合同下，分别运行 bounded control、Daft Native、
Daft Ray、Ray Data 和 project frozen-static。workload 先扩到每个 formal ≥60 s，
各框架拥有自身 scheduling；correctness、feeding-saturation 和稳定性门通过后才允许
容量排名。该轨只回答平台容量，不与轨道 A 的在线 JCT 混表。

最小诊断已经完成：`deploy/autodl/opening_project_short_all_at_t0_diagnostic.example.json`
的Project 512-row 1+3将arrival span压到微秒级；T3/service throughput/MFU与现有
Daft Native同边界只差约2.5%–2.7%。它回答的是“此前71.24s是否来自模型请求路径”，
不是≥60s容量排名，因此无需人为扩short，也不触发K256/K512或项目代码调优。

现有 `Short@0s → Long@5s` 继续回答“活跃 Job 集变化是否产生效率—隔离—公平权衡”，
不承担单系统最优容量排名。online full/half single-short近似相等，而eager full→half
使short JCT +59.00%，说明quota效应依赖arrival regime；两者必须分轨保留。当前
all-at-t0诊断已经排除“项目模型请求路径慢6.4×”，因此不扫K256/K512。

### 7.3 图像四作业：原生 formal 与 Project observe-only formal 已完成

图像多作业不复制文本的 5 s offset。既有单作业结果表明 Ray Data/project 的图像作业
远快于 Daft built-in；若仍用 5 s，前台 short 很可能已经结束，实验无法回答“后到作业
怎样影响正在运行的 short”。因此候选 manifest 固定为 COCO PostgreSQL 中互不重叠的
`short=2,000` 与 `long1/2/3=3,000` 行，三个 long 在 `t=0.5 s` 同时到达。正式结果只有在
每次 run 的实际时间戳证明 short 与三个 long 均有正 overlap 时才有效。64-row gate 只验
correctness；offset 由既有 single-job JCT 推导，并在首次 formal 前用一次不进入结论的
full-size rehearsal 验证。若 rehearsal 无 overlap，必须废弃候选 manifest、重新冻结一次，
不能在看过正式结果后扫描 offset。

原生观察矩阵由 Daft built-in `decode_image→embed_image` 和 Ray Data native
`SQL datasource→map_batches` 各自拥有调度。每个系统运行 short/long1/long2/long3 四个
single-full 控制，再运行四个独立应用并发；外层只提供同一 PostgreSQL 数据切片、共享 Ray
资源池、ready barrier 和 0/0.5 s 启动时序，不注入项目 credit、active-work、路由或
负载均衡。项目矩阵使用同一个 manifest，运行四个 single-full、冻结
`fourjob_static_partition` 与 `fourjob_proposed`。`proposed` 是稳定的实验角色，不绑定
具体算法名：当前实现版本由 `policy_revision` 记录，后续接入状态感知、动态 credit 或
动态路由时保持 manifest、六个 scenario、模型、资源上限与指标 schema 不变，只更换项目
实现和 revision，并重新运行 project 矩阵。

冻结资源/语义：2×RTX 4090、同一 CLIP model/processor revision、L2 normalized 512-d
embedding、batch 64、4 source shards、Ray cluster 总物理资源不变；project 为 16 CPU
preprocess actor、2 GPU actor、全局 active-batch 上限 32、每 Job source queue 2 batch。
static 四作业每 Job 固定 8 active batches且不借用；proposed 使用同一个全局 32 上限。
Daft/Ray Data 的公开资源参数保持既有正式单作业合同，不为多作业结果重新调参，也不把
它们命名为 static partition。

64-row 首次试跑发现：若四个 Ray Data 独立 graph 都使用固定 16-CPU/2-GPU actor pool，
32 CPU 会先被 64 个 preprocess actor 占满，8 个需要 CPU+GPU 的 predictor actor 全部
pending，GPU 保持 0%，构成跨应用资源碎片化死锁。该失败 gate 原样保留，不删证据。
可完成性修正只启用 Ray Data 官方 `ActorPoolStrategy(min_size=1,max_size=冻结单作业上限)`；
四个 graph 仍由 Ray Data/Ray scheduler 原生调度，不加入项目 quota、credit、router 或
跨 Job 管理。single/four-job 全部 Ray Data arms 使用同一 autoscaling contract，避免
只为并发 cell 特制配置；固定 pool 失败只作诊断，不进入完成作业的 slowdown 表。

每个 Job 必须保存：arrival、actual start、first source batch、source done、first submit、
first output、completion/JCT、images/s、single→four-job slowdown、source/queue/completion
时间分解、prepare/H2D/forward P95 与总量、encoded/tensor/device/output bytes、exactly-once
与输出 norm。三个 long 额外报告 JCT/slowdown max-min、CV、完成顺序和 Jain fairness。
组级保存 GPU util 时序、显存、功耗/能耗、经校准 FLOPs 计算的 estimated MFU、CPU busy、
host memory/network/disk、Ray available CPU/GPU、`/dev/shm` 峰值；project 另保存逐事件
ready/active-by-job trace。Daft/Ray Data 隐藏阶段无法可靠归因的字段留空，不补造。

冻结执行顺序为：环境/数据库/模型只读 preflight → 停止文本 vLLM 并清理 stale Ray →
生成候选 manifest 并记录 SHA256 → 64-row correctness/capability gate → 一次 overlap rehearsal
并封存 manifest → 启动
共享 32-CPU/2-GPU Ray → 原生 1 warm-up + 3 balanced formal → project 1+3 → fail-closed
汇总。远端 64-row gate 已验证行数、schema、doc-id/encoded-byte digest、exactly-once
和采集闭环；Ray Data 首次固定 actor pool 的资源碎片化失败证据保留，改用其官方
autoscaling ActorPool 后与 Daft built-in、project static/proposed gate 均通过。候选
2K+3×3K manifest 的一次 full-size rehearsal 也通过：Daft、Ray Data、project static、
project proposed 的 short/long overlap 分别约 19.62/20.43/6.19/2.27 s，证明 0.5 s offset
能测到真实并发。后续 Daft built-in/Ray Data 原生矩阵已完成 40/40 runs、30 formal group；
Project staged descriptor + observe-only snapshot 矩阵也已完成 24/24 group、99K formal rows
exactly-once，3,114 个 snapshot 全部 fresh、构建均值 0.141 ms，static/proposed-role group JCT
仅差 0.98%。因此当前只证明原生执行图内的多 Job 干扰和 Project 观测接入，不构成跨框架绝对
排名或 state-aware 策略收益；真正动态实现仍须相对同次 frozen-static 重跑 Project 矩阵。

未来只重测 project 的复用门禁：native 结果的 manifest SHA、model/processor、输出语义、
硬件/资源、batch/source-shard、计时边界和 metric schema 全部与新 project run 一致；否则
native 证据失效并重跑对应系统。若只是 `policy_revision` 和项目提交逻辑变化，native 不重跑；
project static/proposed 同次交错重跑，避免把日期漂移误判成算法收益。任何 proposed 结论都
必须相对同次 frozen-static，而不是只与旧 native 绝对 JCT 比较。

### 7.4 DuckDB 文本四作业：产品原生 gate 已通过，尚未启动正式实验

DuckDB 只进入 bounded-output SQuAD 轨，不使用 ShareGPT fixed-cap 语义失败的输入。冻结
四份 doc-id 互斥、双 endpoint prompt-work skew 通过门禁的 manifest；分别运行四个单
Job 控制和 `short@0 → 3×long@offset` 四个独立 DuckDB connection/process。DuckDB AI
extension 自己拥有执行和并发，固定已有产品合同的 per-endpoint concurrency 32，不注入
project work credit、路由或重新分区。必须记录每 Job barrier JCT、first/last completion、
output-length/quality error、服务 token counter、running/waiting/KV、GPU/MFU/energy、组级
Jain 与 single→four-job slowdown；缺少可靠逐请求时间戳时不伪造 P95/P99。

DuckDB 当前仍只做 manifest/config/capability gate，不跑 formal；图像 formal 状态以 §7.3 为准。64-row
候选先被 endpoint-work skew 4.68% 门禁正确拒绝，未放宽冻结 4% 合同；重新生成的 128-row
gate 通过，四个 Job 共 512 rows 全部 non-empty、0 error、exactly-once，short 从 0 s
运行至 3.448 s，三个 long 在 0.5 s 到达，实测 short/long overlap 2.948 s，终态服务
running/waiting 均归零。该单次结果明确为 `comparison_admission=not_rankable`。
它回答的是
产品原生多个独立查询竞争时的现象，不是 DuckDB 内部拥有跨查询全局 fair scheduler 的
证明，也不与 Chat 原生框架做绝对排名。项目文本动态策略以后仍使用同一批 manifest 和
到达合同单独重测；若 workload/service 资源签名未变，DuckDB 原生结果可复用。

### 7.5 现成 benchmark 的复用边界：组合合同，不更换四作业 runner

当前没有一个现成套件同时覆盖数据库 source、Daft/Ray Data/DuckDB 原生 graph、独立 Job
生命周期、共享 vLLM/GPU、项目 static/dynamic 和逐 Job fairness。正式方法因此采用四层
组合 benchmark，而不是把自写 runner 误称为新的通用行业标准：

1. **服务容量/到达模型**：复用 `vllm bench serve` 的 ShareGPT/custom dataset、Poisson/
   Gamma burst、request-rate、max-concurrency 与 ramp-up；它只作 serving ceiling/control，
   不代表多 Job 数据流。远端 vLLM 0.25.1 实测没有新版文档中的 `probe-request-rate`，不开
   升级或自写仿 probe 客户端；现有独立 Job runner 已覆盖更完整的干扰观测。
2. **单租户多 Job 评价与未来多租户边界**：当前组合 DRF/Pisces/DRFT 可迁移的
   share/isolation/work-conservation、
   Themis/Tiresias 的 full/reserved finish-time 与 attained service，以及 VTC/DLPM 的 actual
   token-work、共同积压 service disparity 和 locality。项目 runner 已保存 normalized
   service/Jain/描述性 disparity；还需完整 ready/backlogged ledger 才能算 empirical GPS lag/
   starvation。VTC 位于服务内部，本项目不修改 vLLM，故只迁移指标和 counter 思路，不把
   VTC artifact 当同层系统 baseline，也不称已获得 VTC bound。
3. **多模态 workload/native graph**：复用 Daft/Ray Data 官方 image/document/audio/video
   benchmark 的 workload 形态与 vendor-owned graph；本项目只增加 single controls、独立
   graph 并发、ready/start barrier、统一 source/质量与状态采集。
4. **数据库 AI 定位**：SQuAD/ShareGPT/COCO 继续作为当前冻结工作负载；SemBench/LOTUS 可
   在论文阶段补 operator 语义覆盖，但不替换长短异质、多 Job 干扰主矩阵。

跨模态统一派生指标为 `normalized_progress_i = isolated matched JCT_i / concurrent JCT_i`，
并报告其 Jain、max-min disparity、max/min 与 worst Job。文本现有四 Job 正式数据已能事后
重算；图像和 DuckDB 只有在各自四个 single controls 与 formal 完成后才计算。该指标用于
消除 short/long 固有工作量差异，但不能替代 aggregate throughput/MFU、actual-work Jain、
JCT/P99/SLO 与能耗；高 normalized Jain 也可能只是“大家同样慢”。

### 7.6 大众多 Job benchmark 的接入合同：VTC 因果套件 + BurstGPT 真实 trace

现有 1-short+3-long 四 Job 不是废弃的自定义负载，而是最小因果控制组；公开 benchmark
作为第二条泛化轨补充。两轨回答不同问题，不合并成一个 headline：
baseline 的层级身份与原生性总规则仍以 [`baseline_reference.md`](baseline_reference.md)
为准，本节冻结可执行的公开多 Job workload/metric 合同。

| 轨道 | 冻结 workload | 回答的问题 | 允许进入的 arms |
|---|---|---|---|
| 因果控制 | 现有 single controls + `short@0 -> 3*long@offset` | 谁影响谁、idle borrowing 和 long-long interference 从何发生 | 原生 Daft/Ray Data/DuckDB observation；project frozen-static/shared-work |
| VTC-compatible synthetic | VTC `on_off_overload`、`overload-multi` | active/inactive phase 下是否 work-conserving；8 client 规模下 service fairness 是否稳定 | direct FCFS/control；project frozen-static；project shared-work；可选 external VTC-style counter |
| BurstGPT trace | BurstGPT v2.0 固定窗口，保留 timestamp/session/token lengths | 在真实 burst、conversation session 和长尾 work 下结论是否泛化 | direct/project faithful timed replay；Daft Native/Daft Ray/Ray Data native eager trace-shape observation，不注入项目 credit |

#### 7.6.1 VTC-compatible synthetic：只选两个公开 suite

固定上游 artifact `Ying1123/VTC-artifact@192c2e2014c69c8c6c699d7113c3822e4db632e6`
（Apache-2.0），保存原始 `exp_suite.py` SHA 和转换后 manifest SHA。首轮只复用：

1. `on_off_overload`：2 clients、到达率 `[2,3] req/s`、每 60 s 切换 client-0
   active/inactive、256 input + 256 output、原 artifact 600 s。它是“状态变化与闲置份额”
   的标准化验证；本机可按预注册比例统一缩短，但至少保留两个完整 on/off 周期且总测量
   不少于 180 s。
2. `overload-multi`：8 clients、到达率 `[0.4,0.4,0.4,0.6,0.6,0.6,0.6,0.6]`
   req/s、256 input + 256 output、原 artifact 360 s。它验证 client 数扩展与累计服务差，
   不再额外发明 16/32 Job 扫描。

转换器把 `adapter_dir` 仅解释为 `job_id/client_id`，把 `req_time` 映射为相对到达时刻；
prompt 使用冻结 ShareGPT/SQuAD 内容池按目标 token 长度做 deterministic nearest-length
匹配，禁止把 artifact 的 `"Hello " * token_len` 当数据库语义输入。目标 output length
只用于 workload 构造/估计；计费和公平指标使用模型实际完成的 prompt/output token，预测值
在 completion 时校正。

VTC 官方 artifact 把公平 scheduler 实现在 S-LoRA/continuous-batching 服务内部，且论文
真实 trace 的原文件已遗失、仓库中的 `real_trace.pkl` 来自不同时间段。因此：

- 不把 artifact 直接跑出的 S-LoRA 数字与本项目 vLLM 上游路径做绝对性能排名；
- 不称本项目 `shared_work` 为“复现 VTC”；
- 若实现 `external_vtc_counter`，必须标为 **VTC-style upstream baseline**：按最小累计
  actual token service 选 Job、work-conserving、completion 校正，但不修改 vLLM 内部调度；
- 真实到达泛化改由 BurstGPT v2.0 承担，不使用 VTC replacement real trace 作主证据。

#### 7.6.2 BurstGPT trace：真实 arrival，不把 Session ID 直接等同用户

冻结 `HPMLL/BurstGPT` release v2.0 commit
`7eb2c4f8350f8a6985272386f5c14af1f678b299`（dataset CC-BY-4.0），只保存原文件 SHA、
许可、筛选规则和转换后的 manifest；原始大文件不进 Git。使用 v2.0 的
`BurstGPT_without_fails_3.csv`，因为 Session ID 与 elapsed time 是 `BurstGPT_3` 新增字段；
它同时包含 timestamp、model、request/response tokens 和 log type。首轮选一个连续、
以 conversation log 为主的窗口，按本机 bounded ceiling 用单一 `time_scale` 压缩到
60--180 s；scale 先由 control
校准后冻结，所有 faithful-timed arms 共用。`Session ID` 表示会话而非稳定用户，故只作为
不可拆分的 session 原子：按首次出现顺序做 deterministic round-robin，冻结为 4 条 logical
application streams；不称真实 tenant/user，也不按事后 work 人工平衡。API log 缺 session
时不随机造租户，首轮直接排除并记录计数。

BurstGPT 不包含原始 prompt 文本。按其官方示例，用冻结 ShareGPT prompt pool 做
deterministic nearest-length matching；记录匹配前后 prompt token 误差 P50/P95/max。若
output cap 使目标 response length 无法表达，则该行在转换门禁被拒绝，不能静默截断后仍称
faithful replay。首个公开 trace formal 包含 direct capacity/control 与 project
frozen-static/shared-work 的 faithful timed replay；另补 Daft Native、Daft Ray、Ray Data
三条原生 `eager_trace_shape` 观察轨。DuckDB 不进入 BurstGPT：其 bounded-output 产品轨继续
使用 SQuAD 多 Job，不能把 output-cap 不兼容的 trace 强塞进产品 baseline。

Daft/Ray Data 原生 graph 不暴露逐请求 timed-arrival 调度合同，因此每个 logical stream
作为独立官方 application/process，在同一 ready barrier 后 eager 执行自己的 immutable
manifest；保留 prompt、token shape、stream 划分和共享 vLLM，但不声称 faithful timestamp
replay。每个原生 arm 必须跑 4 个 isolated single controls 与一次 4-application concurrent，
只比较该系统内部的 single-to-multi slowdown、normalized progress、吞吐/MFU、waiting/KV
和公平性；不得用其 absolute JCT 与 faithful-timed direct/project 排名。Daft/Ray Data 不跑
VTC on/off 与 8-client suite，因为给官方 graph 注入外部 per-client pause/credit 会改变其
原生 scheduler ownership。

#### 7.6.3 统一指标与通过门禁

每条公开 benchmark run 除现有 E2E/GPU/MFU/能耗指标外，必须输出：per-job arrived/
completed/failed requests、actual prompt/output/weighted service、JCT/TTFT/P99/SLO goodput、
三个 JCT 反事实、solo-normalized progress、描述性 Jain、持续 backlogged window 的
empirical GPS lag/max-min disparity、最长连续 no-service、偿还时间、idle time、borrowed work
和 endpoint running/waiting/KV。service lag/disparity 只在至少两个 Job **同时持续
backlogged** 的窗口计算，不能把未到达或已 drain 的 Job 计入分母制造“公平”；没有证明时
不称 VTC 式 service-difference bound。

准备阶段通过标准：转换 deterministic；Job/manifest doc-id 互斥；faithful-timed arms 的
arrival 重放误差 P95 <= 50 ms；token-length 匹配误差有审计；exactly-once；同一资源/
模型/服务签名。所有 arms 复用同一内容/stream manifest；faithful-timed arms 再共用同一
time scale，eager arms 保存移除 timing 后的 derived-manifest SHA。正式阶段保持 1 warmup
+ 3 balanced formal；任一 correctness、feeding 或适用的 arrival-fidelity 门禁失败，只
保留 diagnostic，不继续调参追正。

图像侧没有可直接称为“VTC/BurstGPT 图像多 Job benchmark”的公开套件。图像继续复用
官方 Daft/Ray Data image workload 和现有四 Job wrapper，并迁移上述 active/inactive、
actual stage-work 和 service-disparity 定义；只能称 **VTC-compatible evaluation contract**，
不能称官方 VTC 图像 benchmark。
