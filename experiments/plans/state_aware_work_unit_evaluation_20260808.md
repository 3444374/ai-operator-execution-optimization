# Work Unit、状态感知、动态调度与代价估计的实验计划

日期：2026-08-08（2026-08-09 更新：开题两作业 guaranteed-overlap 已完成）

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
- 相对同资源、同上限 frozen-static，correct throughput、SLO goodput、P99/JCT 或 fairness 至少一项改善约 5%，且其他关键指标和 failure 不出现不可接受退化；
- steady control 不要求动态胜出，但控制开销和回退不得造成约 5% 以上退化；
- 若所有目标指标均未改善，记录动态控制失效边界，不更换弱 baseline。

## 3. 公共实验合同

- immutable workload manifest、相同 source、模型/processor revision、输出语义、随机种子和服务 flags；若 headline 是 database-E2E，再要求相同 sink；
- 1 warm-up + 3 个交错 formal，失败 run 结构化保留；
- 离线容量校准和正式运行使用相同 calibration signature；
- baseline 由对应框架拥有 batching/backpressure/scheduling；项目只做 source、必要的 sink、质量与指标适配；
- project static 与 project dynamic 使用同一最大 request credit、stage work credit、actor/GPU/CPU 资源和 buffer bytes 上限；
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

逐 Job 必须报告 JCT、P95/P99（native adapter 无可靠 request timestamp 时明确不可用）、
actual work、work/s、相对自身 single-full slowdown、开始/结束和与其它 Job 的重叠时长。
三个 long 还要报告 slowdown/JCT 的 max-min、CV、最慢 Job、pairwise overlap 和完成顺序。
组级报告总 tokens/s、Jain fairness、max/min service、GPU util、MFU、running/waiting/KV、
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

### 7.3 图像四作业：gate/rehearsal 已通过，尚未启动正式实验

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

执行顺序固定为：环境/数据库/模型只读 preflight → 停止文本 vLLM 并清理 stale Ray →
生成候选 manifest 并记录 SHA256 → 64-row correctness/capability gate → 一次 overlap rehearsal
并封存 manifest → 启动
共享 32-CPU/2-GPU Ray → 原生 1 warm-up + 3 balanced formal → project 1+3 → fail-closed
汇总。远端 64-row gate 已验证行数、schema、doc-id/encoded-byte digest、exactly-once
和采集闭环；Ray Data 首次固定 actor pool 的资源碎片化失败证据保留，改用其官方
autoscaling ActorPool 后与 Daft built-in、project static/proposed gate 均通过。候选
2K+3×3K manifest 的一次 full-size rehearsal 也通过：Daft、Ray Data、project static、
project proposed 的 short/long overlap 分别约 19.62/20.43/6.19/2.27 s，证明 0.5 s offset
能测到真实并发。该 rehearsal 只有一次、`proposed` 仍是当前占位实现，所有绝对值和臂间
差异都只用于流程诊断，**没有启动 formal，也不构成策略收益结论**。

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

DuckDB 与图像矩阵同样先只做 manifest/config/capability gate，不跑 formal。64-row
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
2. **多租户公平定义**：复用 VTC 的 actual token-work accounting、work-conserving、
   cumulative service disparity 和 per-client completion；项目 runner 已保存 normalized
   service/Jain/disparity。VTC 位于服务内部，本项目不修改 vLLM，故只迁移指标和 counter
   思路，不把 VTC artifact 当同层系统 baseline。
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
solo-normalized progress、Jain、持续 backlogged window 的 cumulative service
max-min/mean disparity、idle time、borrowed work 和 endpoint running/waiting/KV。VTC 式
service disparity 只在至少两个 Job **同时持续 backlogged** 的窗口计算，不能把未到达或已
drain 的 Job 计入分母制造“公平”。

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
