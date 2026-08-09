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

### 7.3 图像四作业：已冻结准备合同，尚未启动正式实验

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
汇总。当前只完成 runner、配置和 preflight 准备，**没有启动 formal**。远端 preflight
已确认 core/image Python 与 2×4090 profile；原始 COCO ZIP 不在本机，但数据库中已有
正式导入表，首次 gate 仍须验证行数、schema、doc-id digest 和 encoded-byte digest。
当前 GPU 被文本 vLLM 占用，正式图像 gate 前必须先释放，不能与文本服务共跑。

未来只重测 project 的复用门禁：native 结果的 manifest SHA、model/processor、输出语义、
硬件/资源、batch/source-shard、计时边界和 metric schema 全部与新 project run 一致；否则
native 证据失效并重跑对应系统。若只是 `policy_revision` 和项目提交逻辑变化，native 不重跑；
project static/proposed 同次交错重跑，避免把日期漂移误判成算法收益。任何 proposed 结论都
必须相对同次 frozen-static，而不是只与旧 native 绝对 JCT 比较。

### 7.4 DuckDB 文本四作业：产品原生观察准备合同

DuckDB 只进入 bounded-output SQuAD 轨，不使用 ShareGPT fixed-cap 语义失败的输入。冻结
四份 doc-id 互斥、双 endpoint prompt-work skew 通过门禁的 manifest；分别运行四个单
Job 控制和 `short@0 → 3×long@offset` 四个独立 DuckDB connection/process。DuckDB AI
extension 自己拥有执行和并发，固定已有产品合同的 per-endpoint concurrency 32，不注入
project work credit、路由或重新分区。必须记录每 Job barrier JCT、first/last completion、
output-length/quality error、服务 token counter、running/waiting/KV、GPU/MFU/energy、组级
Jain 与 single→four-job slowdown；缺少可靠逐请求时间戳时不伪造 P95/P99。

DuckDB 与图像矩阵同样先只做 manifest/config/capability gate，不跑 formal。它回答的是
产品原生多个独立查询竞争时的现象，不是 DuckDB 内部拥有跨查询全局 fair scheduler 的
证明，也不与 Chat 原生框架做绝对排名。项目文本动态策略以后仍使用同一批 manifest 和
到达合同单独重测；若 workload/service 资源签名未变，DuckDB 原生结果可复用。
