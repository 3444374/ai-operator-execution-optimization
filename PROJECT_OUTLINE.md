# 项目大纲

更新时间：2026-08-13

本文件是项目方向、研究内容、证据等级和近期执行顺序的权威总纲。实验细节以对应结果目录的 README/CSV/JSON 为准；文献入口见 `research/knowledge_hub.md`；开题材料必须服从 `opening/claim_matrix.md`。

## 1. 题目与研究对象

题目冻结为：

> 数据库 AI 负载的执行优化与调度研究

统一研究对象是数据库触发后的 AI 数据执行层：

```text
Database
  -> AI Data Execution Layer
       -> research content 1: work-unit construction and organization
       -> research content 2: state-aware admission, routing and multi-job
       -> shared cost estimator
            -> stage/service/remaining work
            -> SLO slack and uncertainty
  -> Model Service / GPU Executor
  -> Database / Vector Sink
```

对外口径：数据库内置 AI 算子的外部分布式数据处理执行链路优化。

Daft、Ray、vLLM、PostgreSQL、pgvector 和 CLIP 是实现与验证平台，不是贡献名称。项目不修改数据库内核、vLLM continuous batching、Ray 调度器、模型结构或 GPU kernel，也不回到传统 GPU 查询算子。

## 2. 研究内容

### 2.1 研究内容一：workload-aware work-unit 构造

研究数据库记录如何组成发送给模型服务的 staged work unit。核心接口是 `WorkDescriptor`：source/prepare/model/result work、locality key、deadline/SLO、不确定区间和 calibration signature，而不是固定行数或把 token 机械改名为 frame。候选机制包括 sequential budget、length alignment、prefix-aware grouping 和受控 best-fit。重点刻画两个冲突：

- work balance：减少 batch 与 endpoint 之间的计算量偏差；
- locality：保留 prefix、frame 或数据局部性，避免因重排序破坏缓存与流水线效率。

评价 packing、work skew、prefix group ratio、cache hit、吞吐、TTFT、尾延迟、能耗与任务质量。策略排序必须绑定机器、模型、endpoint/KV regime 和 workload，不宣称全局最优。

### 2.2 研究内容二：容量感知的提交、路由与多作业调度

以 endpoint-shared request/work credit 表达在途工作量，在 completion 时精确释放并连续补位；在固定资源和上限下研究：

- 最小饱和 active work 与过载边界；
- request-level replenishment；
- endpoint routing、idle borrowing 与故障迁移；
- 多 job fair queue、JCT、tail、SLO 和公平性。

原生 baseline 必须保留被测系统自己的 batching/backpressure/scheduler；项目 frozen-static 是
同栈静态参照，不冒充原生 baseline。现有证据已将动态 K 标记为 `parked-conditional`；主候选
固定总 envelope，只动态决定活跃 Job 间的份额借用、completion-time 回收与 release order。
项目 selector 必须在相同 bounded-ready observation 下超过 FIFO、DRR/VTC-style 等项目内部
消融；吞吐接近时继续评价 tail/SLO/fairness，均无改善则淘汰 SAOR，不更换 workload 追正。

多 Job 不由单一 VTC/Jain 指标判定。每个 Job 同时报 `multi/full-solo`（总体干扰）、
`multi/reserved-solo`（经验性保留份额非劣）和 `policy-multi/static-multi`（同竞争调度增量）；
共同积压窗口另报 weighted actual service、empirical GPS lag、最长连续无服务和 avoidable idle，
用户层报 worst-Job JCT/P99/SLO。评价采用明确保护约束下的多目标/Pareto，不压成 composite。
baseline-relative empirical Pareto improvement 不等于 DRF Pareto efficiency；Jain 只表示均匀度，
不能单独证明或否定 share guarantee。

### 2.3 共同使能组件：算子代价估计

首版采用解析 work 特征、profile 校准和 residual correction，预测文本/图像的 stage work、operator service、JCT、remaining work 与 SLO slack。它同时服务于 active-work 初始化、`WorkDescriptor`/组织预算、路由、提交与多 job，不单列为第三项研究内容。

评价 MAE/MAPE 之外的候选配置 ranking、pairwise accuracy、selection regret、最坏 context 与预测区间；平均误差好不能替代决策质量。

### 2.4 多模态泛化

文本 `AI_COMPLETE` 是主要方法场景；图像 `AI_EMBED/AI_CLASSIFY` 是正文泛化验证。公共策略只消费 staged estimated work、credit、fresh state 和 completion event：文本 adapter 输出 source/tokenize/prompt-output/result work，图像 adapter 输出 encoded/prepare/tensor-model/result work；Organizer、Scheduler、Tracing 和配置逻辑保持一致。不适用某模态的能力必须显式声明。

图像路径新增 HSE（Heterogeneous Staged Execution）作为执行底座候选：Daft/Ray 继续拥有
数据引擎、资源放置和任务执行，typed data plane 管理 encoded/prepared/device/result block，
SAOR 只控制项目侧 Job-head admission 与 byte/work-bounded 中间态。HSE 连接研究内容一和二，
不单列为第三项贡献；static HSE 未超过冻结 project static 前，不评价动态 HSE。

## 3. 系统与实验边界

```text
PostgreSQL source
  -> Daft DataFrame / Arrow
  -> Shared Cost Estimator + WorkDescriptor + Organizer
  -> Ray actor admission / shared credit / routing
  -> text: vLLM generation
     image: typed CLIP GPU actor
  -> unified PostgreSQL / pgvector sink
```

- 写回采用 PostgreSQL + pgvector、COPY + deferred index，属于统一 correctness/E2E guardrail，不是独立研究内容。
- 正式 baseline 必须由被测系统拥有执行与调度；项目只做 source、sink、质量审计和指标适配。
- 自写 actor pool、credit、inflight/backpressure 或 Daft UDF 只能按清晰 provenance 标为项目方法或 diagnostic reference。
- `BoundedReadyWindow` 属于项目方法：不得注入 Daft、Ray Data、vLLM 或数据库产品的原生 baseline；只在项目内部 selector 归因消融中保持一致。
- 模型/数据下载不等于数据库 workload 已导入；必须继续执行 importer 和 schema/行数/exactly-once 门禁。
- 性能参数绑定“机器 + 模型/服务配置 + 协议 + workload 分布/规模”签名，签名变化重新校准；
  同一签名只校准一次并复用冻结合同。K 不是逐实验手调，也不在 SAOR formal 中在线变化。

## 4. 研究问题与因果设计

四个研究问题：

1. 固定资源下达到近饱和吞吐所需的最小 active work 是多少，过载怎样影响 tail 与能耗？
2. 相同 work 下怎样组织记录，balance 与 locality 何时冲突？
3. 多 Job 活跃集、arrival 或 work mix 改变时，固定总 K 内的 idle borrowing、reclaim 和
   state-aware ordered release 能否相对 global FIFO/static/DRR 改善 worst-Job JCT/tail/SLO？
4. 多 job 共享 endpoint pool 时，怎样在 work conservation 与 weighted service lag/fairness
   之间形成可验证的 Pareto 改善？

两项策略先独立搜索冻结静态点并分别消融，再把独立最优拼接，与小规模联合 grid 对比。联合显著优于拼接说明需要联合调优；两者接近说明可分层优化。任何结果都不改变研究对象，但会改变方法适用边界。

正式实验统一要求：immutable manifest、相同 source、相同完整结果语义、相同服务 flags、固定随机种子、warmup 与交错 formal repeats；结果保存请求、submission、资源时序和版本。调度主实验以完整结果 gather 的 correct throughput/JCT 为 headline；仅 database-E2E 护栏要求相同 sink 与 sink readback，另报告 service throughput、质量、failure 类型和资源门禁。

## 5. 当前证据等级

### 5.1 已证明

- 固定行数不是稳定 work 代理：固定 16 行批次的 work 最小/最大中位数为 474/6,793 token，相差 14.3 倍。
- 同一静态上限不是运行状态：W65K 下 high offered load 的运行内峰值 active work/MFU 约为 100%/35%，arrival-limited 约为 29%/7%；前者不是时间平均 active work。
- 当前双 4090/Qwen/vLLM 签名下，65,536 active work/endpoint 达最大已测吞吐均值的 97.80%；下一档只增 0.92%，继续增压会恶化 P99。
- 复杂动态控制不天然优于强静态点：AIMD/PID/EWMA、adaptive flush、service quantum 与多 actor 多数未过约 5% 晋级门槛。
- 数据组织策略排名受 serving regime 影响：双 endpoint 大 KV 池下策略范围约 12%；四 endpoint 小 KV 池饱和时分化约 27% 且排名反转，重排序可使 prefix hit 降至 0.06–0.07。
- 图像 matched-resource 静态执行结构有可重复收益：主报告冻结约 13%–15% operator-JCT 改善；旧 45.7% 资源不匹配，不再使用。
- 多 Job 干扰已从两作业扩展到受控 `short@0s → 3 long@5s`：Project full/quarter
  single 将 quota 与竞争分离；三条原生路径全部 Job 均出现轨内退化。Project shared 相对
  static 总吞吐 +8.68%，四个 Job JCT 分别 −72.23%/−8.28%/−20.24%/−52.66%，因此在
  效率/JCT 子向量上是相对 static 的经验性 Pareto 改善；但 raw-work Jain 0.960→0.923、
  long 收益/稳定性不均，且 long1/2 相对 quarter-solo 仍慢 29%/14%。这支撑 idle borrowing
  与 fairness/SLO guard 同时存在的研究问题，不证明 dynamic 普遍胜出、份额保证或理论
  Pareto efficiency。

### 5.2 条件性

- Hybrid 代价估计在 429 个 formal 观测、20 context × 4 candidate 的 context-LOO 中取得 pooled regret 1.67%、macro 2.90%、pairwise 0.808、max regret 14.72%。最大 regret 距 15% 门仅 0.28 个百分点，属于 marginal pass。
- prefix-affinity routing 在四 endpoint/小 KV 池条件下出现约 5.9% 增量，但 endpoint consolidation 与饱和深度尚未完全隔离，不外推为普遍有效。

### 5.3 待验证

- SAOR 的 dynamic-K capacity-only 分支仍为 `not-promoted/parked-conditional`。fixed-envelope
  2-Job formal 已在 2×4090/Qwen2.5-7B 上完成 40/40 cell、0 incident、exactly-once：SAOR
  12,393 tok/s、fg JCT/P99 57.0/50.3s，在 credit 臂内前台最好；static 9,508 tok/s、fg
  JCT/P99 36.2/29.2s、SLO violation 0%，仍是更强隔离 Pareto 点。原始 validation 的 DRR/VTC
  rep2 假阴性已按 250 ms trace-resolution 规则修复；服务器完整 artifact 的 resolution-aware v2
  validation passed、credit mechanism effective 12/12，原 failed 文件只作审计历史。仍不能声称
  SAOR 胜出。当前 formal 的 `slo_weight=0`，服务状态仍
  observe-only；它验证的是 fairness-aware release，不是完整 SLO-aware controller。第一性原理
  审计确认：soft fairness release score 无法复制 static 的即时隔离；但 strict-priority 两轮 GPU
  短测达到 11,791 tok/s、fg P99 14.27s、SLO 0%，说明已知 foreground 存活信号下 release-only
  可达。`saor-v0.5` 已冻结为通用有界词典序 release：显式 per-Job priority/剩余 SLO 预算，
  completion-corrected actual-work debt cap 优先阻止饥饿，无 guard 时回退 SAOR；debt-critical
  ready head 不 fit 时只为该队首建立 reclaim barrier，其余只在 fitting heads 间选择；首轮只做
  两 Job 的 $0.125W_e/0.25W_e$ 两个 cap（$W_e$ 是 endpoint work limit，不是 request K）。v0.5.1 已完成 selector/coordinator/scheduler/Ray/runner、
  timeout 清理、lossless event ledger、readiness 与两轮汇总器；事件机制门不再依赖 250 ms
  snapshot。旧 single-head 双轮 GPU development gate 没有 cap 晋级：$0.25W_e$ 第 2 轮 debt-recovery=0，两个
  cap 的 fg P99 约 49–56s、SLO violation 85%–95%。request/event 交叉验证定位 per-Job 单-head
  pull 没有把完整 Daft/Ray ready backlog 暴露给 coordinator；该失败版本状态为
  `development-run/not-promoted/not-formal-registered`。2026-08-13 已完成独立
  `saor_bounded_ready` 修订：旧
  bounded-priority 保持单-head 回归语义，新路径只预注册已经到达的具体 request，窗口由冻结
  effective K 与 endpoint 数×W 自动派生；submission trace schema 6 分开 ready、registered、
  granted、submit/service；coordinator release-event schema 2 对 ready registration 与 grant
  统一记录 request ID 和 epoch。runner 先用 submission trace 证明 concrete-ready lifecycle 完整，
  再在 actor 同一时钟域内配对 foreground register→grant，并 fail closed 检查区间内 foreign
  fallback=0。两轮 GPU development gate 已完成：$0.125W_e$ 两轮以约 12.36K tok/s、foreground
  P99 17.58–18.15s、foreground SLO 0% 和 bulk SLO 65.8%–66.6% 通过全部门，注册 formal
  candidate；$0.25W_e$ 因 bulk 30s miss 74.4%–75.2% 两轮越界拒绝。状态为
  `development-gated/formal-registration-candidate-0125k-only`，不是 formal 胜出。post-hoc
  归因审核确认 bounded-ready 同时改变 observation/execution path 与 selector，因此 formal 前先让
  **项目内部** FIFO、DRR/WFQ、external VTC-style、strict-priority 和 proposed 使用相同 ready-window；
  Daft/Ray Data/产品原生 baseline 不接入该机制。只有 proposed 越过同 observation 的项目简单
  消融 Pareto 前沿才进入 1+3 formal；否则贡献收敛为 bounded
  ready-state exposure + 最小 guarded release，或淘汰复杂 selector；
- runtime-state-aware 请求成形、提交或路由能否超过同上限 frozen-static；
- fixed-K active-set change、burst、mixed-cost 下 ordered release 的响应时间、SLO goodput 与 tail；
- 多 job 的 5s 两作业与 1-short+3-long 四作业均已完成；仍待新 workload held-out、
  加权/SLO、公平 guard、Long→Short 与故障迁移；
- 代价模型跨时间段、新 workload 和硬件的稳定性；
- 图像 Daft built-in、Ray Data native 与 project frozen-static 的 operator-E2E/provenance
  证据已完成。现有数据把瓶颈进一步定位为 CPU prepare 与 driver/Ray submission 的组合：
  HSE static core 已显式拆出 pending-prepare、ready-block、pending-model，并以 descriptor/
  lease 做 physical-byte/work 预留；result 当前由 driver 即时审计，独立 sink queue 尚未接。
  串行流水线满足
  $X\le\min_s\mu_s$；现有 1666 image/s 与约 19K GPU-resident ceiling 的约 8.8% 比值和约
  9.6% GPU busy 同量级，说明调度/buffer 不能消灭 CPU prepare 木桶。derived-image cache、
  packed uint8/GPU normalize 与 DALI GPU/mixed preprocess 作为正交
  work-reduction 消融。仍待 static HSE GPU 对照门、packed/pinned/DALI、动态 SAOR runner、
  跨 workload 外推与小规模 sink 质量闭环，sink 不是性能排名 blocker。
- prompt 变化感知、exact/semantic 结果复用、数据库级/模型内部增量推理已进入
  `parked-conditional` 清单；当前不实现，主路径完成后仅在真实 reuse opportunity≥10% 且扣除
  lookup/build/refresh 后 oracle 潜力≥5% 时重新激活。
- 图像 short→3×long 多作业已完成 immutable manifest、64-row correctness gate 和一次
  full-size overlap rehearsal；DuckDB bounded-output 四作业已完成 128-row native gate。
  两者均未启动 formal、不能用于系统排名或策略收益；图像 proposed 角色已与具体算法名
  解耦，后续状态感知/动态调度调整只需版本化并重跑 project static/proposed。

### 5.4 不能声称

- 项目路径普遍优于 direct、DuckDB AI、Daft、Ray Data 或 vLLM 官方路径；
- sequential、length-align 或 prefix-aware 是全局最优 organizer；
- 65,536 是 vLLM 通用容量或最佳并发；
- 动态策略已经胜出；
- 图像路径提升 45.7%；
- 代价模型已经稳健解决。

## 6. 开题前统一文本 database-E2E

2026-08-07 首轮三臂因 project feeding 仅为 direct 的 89.9%/91.38%，保留为 failed-feeding 历史诊断。2026-08-08 K128 replacement 的 24/24 单元、18 formal 通过 correctness、sink、identity 与稳定性门禁；但随后 ShareGPT bounded C32–C256 扫描证明 C32 只有已测峰值的 52.07%，故 ShareGPT 三臂性能排名降级，正式原生矩阵改用达到峰值 98.22% 的最小点 C128。

开题静态地基先完成 SQuAD short-answer 均匀控制组与 ShareGPT controlled-skew 异质组。两组均比较：

- `direct_static_sharded`；
- `duckdb_ai_static_sharded`；
- `project_frozen_static`。

统一合同：PostgreSQL source、immutable equal-row manifest、双 Qwen2.5-7B vLLM endpoint、prefix cache ON、统一 PostgreSQL sink、外部 database-E2E、质量与资源指标、1 warmup + 3 formal。

SQuAD replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 136.63、136.68、137.77，service tokens/s 为 40,920.72、40,955.99、41,277.95；三臂 EM/F1 接近，project/direct service ratio 为 1.0087，均匀短输出下近似中性。

ShareGPT replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 11.36、2.26、17.55，service tokens/s 为 9,425.25、9,421.31、14,568.91。后续 bounded C32/C64/C128/C256 扫描为 9,454.88/14,057.93/17,834.14/18,158.19 tok/s，C128 是达到已测峰值 97% 的最小点；C256 仅增 1.82%，却使 waiting mean=116.8、KV max=0.9996、TTFT mean=6.18s。旧 project/C32-direct=1.5457 因对照欠供给而不作方法排名。DuckDB fixed-cap 产品语义失败 4,921/6,144 行的结论仍有效。

同一 ShareGPT Chat manifest 的原生单 job 1+3 已完成：bounded C128、Daft Native、Daft Ray、Ray Data 的 service tok/s 为 17,800/17,286/16,747/3,551，四臂 CV<0.6%。Daft 两臂 waiting mean 为 783/742、KV max≈1，呈现过量提前提交；Ray Data running mean=17.3、MFU=0.112，呈现供给不足；bounded C128 位于最小饱和区。该结果只证明官方 graph/冻结点的外部压力形态，不证明项目方法胜出或某个框架内部算法有缺陷。

5s guaranteed-overlap 对照已完成：Daft Native/Ray、Ray Data 的 short JCT 相对各自 single 增加82.42%/104.84%/32.76%，只作外部观察。项目在线 replay 下 quota-only≈0，static/shared 加入 long 后 short JCT增加3.79%/8.95%；shared 提高总吞吐但 short/Jain回退。统一 eager Project 12 formal 又显示 full→half quota-only 已使short JCT+59.00%，matched half→static+long再+58.77%，matched full→shared+long+28.90%；eager shared 相对static使short JCT−48.94%、总吞吐+31.85%、long JCT−25.75%、Jain 0.894→0.972。两种到达regime方向相反，冻结为“多Job管理必须感知arrival/active/drain状态、支持idle borrowing并保留SLO/fairness guard”的证据，不称动态普遍胜出；开题前不再扫offset/weight/更多Job追正。

Project all-at-t0 single-short 诊断已补齐统一 T0–T4 计时：T0 profiler E2E14.957s，T3 earliest model submit→latest completion11.354s，service14,361tok/s、MFU42.93%；Daft Native同一short T3为11.059s、14,727tok/s、MFU44.04%，差异仅约2.5%–2.7%。Daft缺准备前T0，因此完整E2E仍不排名；该结果排除了“Project模型请求路径慢6.4×”。随后Project eager多Job只补full single、half single、static+long、shared+long，不重跑原生三臂；arrival span均为66.76µs、12/12 formal通过。逐阶段显示matched static竞争使short service mean/P99 +50.34%/+78.62%，shared为+14.63%/+28.70%；submit→service仍约2ms。在线replay与eager结论分轨保留。

## 7. 开题叙事图

1. `opening_motivation_work_state`：固定行隐藏 work、静态上限不是状态、提交压力存在最小近饱和点与边际收益递减区，分别导出 WorkDescriptor、感知和有界控制。
2. `opening_ai_data_execution_boundary`：两项研究内容并列，算子代价估计作为共同使能部件。
3. `opening_work_to_schedule_overview`：组织输出 work/locality/deadline，调度结合 fresh state 消费。
4. `opening_work_organization_regime_v2`：work-aware 组织的必要性与 regime 局限。
5. `opening_image_stage_aware_evidence`：图像 prepare/model、transfer 形态和 active-window 动机，只承担 staged work 与状态感知必要性。
6. `opening_image_baseline_evidence_map`：Direct、Daft Built-in、Ray Data、vLLM Pooling、Project 的能力门禁、12K 结构诊断与 120K matched-resource 正式排名边界。
7. `opening_cost_model_decision_quality_v2`：代价模型 selection regret 与最坏风险。

权威输出位于 `figures/data/report_main/` 与 `figures/architecture/`，生成脚本为 `figures/scripts/generate_opening_story_figures_20260808.py`，claim 与视觉审计见 `figures/audit/opening_story_figures_contract_20260808.md`。九张正文数据图与两张单 Job 备份图已经完成可读性与证据边界审计；无同上限正式结果的 static–dynamic 示意图继续保持 `do-not-draw-no-result`。制作 PPT 或报告时统一从 `figures/opening_figure_set/` 进入：PPT 按页码汇总 17 张主讲图，其中 P02–P04 是背景/相关工作概念图；报告正文仍使用 P05–P19 的 14 张研究/证据图。图集另列 8 张 Draw.io 编辑源和 2 张备份图，权威数据与可复现源仍留在原目录。

## 8. 当前执行顺序

1. 第一性原理 framing、Claim Matrix、staged WorkDescriptor/状态合同与共同 cost enabler 已完成。图像 production descriptor builder 与 fresh snapshot 已以 observe-only 方式接入 project runner，legacy model-pixel credit 和调度决策保持不变；原生图像 single→four-job 40/40 passed，Project observe-only 24/24 passed、99K formal rows exactly-once、snapshot 100% fresh/构建均值 0.141 ms。static/proposed group JCT 只差 0.98%，因此只验收观测接入；stage controller 决策接线和 CE5 在线驱动仍待验证，不能将原生观察或 observe-only 写成动态方法胜出。
2. K128 replacement database-E2E 已通过并归档；旧 failed-feeding 结果只作历史诊断，不再进入当前数字口径。
3. 权威内容入口改为 `opening/opening_defense_outline_20260808.md`；当前只更新实验报告、紧凑数据和待画图合同，不生成新图，也不生成、覆盖或同步新的 PPT/云文档。
4. 文本原生单 job、5s 两 job 与四 job 矩阵均已完成。图像 Daft built-in/Ray Data/project
   四作业和 DuckDB bounded-output 四作业只完成冻结准备，不跑 formal；图像使用 0.5s
   offset 并要求实际 overlap。后续先完成 capability/rehearsal，再跑 native 一次；项目
   感知/动态实现调整后只重跑同 manifest 的 project static/proposed。
5. 用户已明确不需要 Wiki 同步；当前也暂停普通飞书云文档覆盖，只完成本地材料与 Git 发布。

2026-08-11 修正后的文本 phase-change 门禁显示：A-only K160 相对 K128 的每 endpoint
service rate 提升 7.77%，低压升档动机成立；但 B=2.5/3.5/4.5 均未在两个 ON 周期、
两个 endpoint 上稳定产生 waiting 或 KV>=0.85，最高档仅第二轮 endpoint-0 瞬时达到
KV=0.874。按预注册规则停止，未运行 adaptive action/formal；当前不能判断动态策略有效或
无效。后续只能以独立合同验证显式 drain/recovery 后的可重复 phase change。

## 9. 结果解释与写作规则

每个正式实验按以下顺序记录：目的、设置、合规自检、设计、全组件数据、解释、对课题含义、下一步。解释明确区分事实、推断、待确认和不能声称。

GPU 利用率优先使用 time-series mean/p50/p95/max；KV usage 按 0–1 分数读取。feeding-saturation 以同协议 bounded direct 为参照；未过门的臂不抽策略性能结论。raw rows/s、correct rows/s 和 service tokens/s 不得互相替代，语义失败必须保留在总行数分母。

正式报告、论文、PPT 和图表不使用内部实验缩写。PG18.4 AutoDL 结果必须标为 rehearsal，不能冒充目标 PostgreSQL 18.3 平台结论。

## 10. 同步入口

- 开题报告：`opening/report/opening_report.md`
- 开题 Claim Matrix：`opening/claim_matrix.md`
- 当前答辩内容合同：`opening/opening_defense_outline_20260808.md`；历史PPT v6设计
  `opening/slides/opening_defense_v6_design.md`已被取代且禁止作为生成输入
- 答辩问答：`opening/qa_bank.md`
- 当前方向速览：`overview/current_direction_and_plan.md`
- 实验状态：`experiments/plans/experiment_status_and_gaps.md`
- 文献与知识：`research/knowledge_hub.md`
- 变更日志：`PROJECT_LOG.md`

影响方向、实验结论或关键入口的修改必须同步 `PROJECT_LOG.md`、`PROJECT_INDEX.md`、根 README 和受影响目录 README。修改知识文件后按 `research/knowledge_sync_guide.md` 同步平级 Obsidian Wiki。
