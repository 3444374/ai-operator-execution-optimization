# 项目大纲

更新时间：2026-09-21

系统名称：**SemLoom**。DB-AIEL（Database-Aware AI Execution Layer）表示其所在架构层，不作为
代码接口或实验身份前缀；完整术语见 [`CONTEXT.md`](CONTEXT.md)。

本文件是项目方向、研究内容、证据等级和近期执行顺序的权威总纲。实验细节以对应结果目录的 README/CSV/JSON 为准；文献入口见 `docs/research/knowledge_hub.md`；开题材料必须服从 `docs/thesis/claim_matrix.md`。
逐轮验证过程不在本文记录：某次测试的条件、数据与解释见 `results/` 下对应记录，变更时间线见
`PROJECT_LOG.md`，当前能力状态见 `code/INFRA_STATUS.md`（按能力组织）。

## 0. 当前优先级

下一阶段研究重点是说明“哪些工作现在做、哪些稍后做，以及何时值得这样做”。
[设计主张与证据表](docs/plans/data_organization_batching.md#design-hypotheses)将已有实现分成待检验的设计选择：
当前 M1 先识别有效吞吐平台附近的供给与资源代价，以调优请求数为工作量控制的参照；局部/全局信息归 M2，
多查询策略按自身资格推进。有限窗口不是预先认定的最佳方法。
资源安全、有限模型中的数学结论和真实系统收益分别论证；现有工程工作包不直接等于论文贡献。

当前位置（2026-09-21）：工程工作包 A–F 已完成各自受控验证（图像 embed 三路径数值一致）；
M1 完整容量复查 16,400 次请求账目一致但 PG/direct 均持续供给不足，尚未选出平台候选，
原偶发故障根因待确定；M2 五种 PG 源信息方式完成固定 C4 诊断、尚无稳定收益。
能力与结论的逐项限制见[能力总览](code/INFRA_STATUS.md)与[证据台账](results/EXPERIMENT_EVIDENCE_REGISTRY.md)。

### 0.1 PostgreSQL AI 语义算子实施入口

数据库内 AI 语义算子的权威实施入口为
[`docs/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](docs/plans/postgresql_ai_semantic_operator_architecture_20260827.md)：
参考 Sema/Cortex 的数据库语义所有权、LOTUS 的 reference/optimized algorithms、IMLane 的 DB-runtime
batch pump，并把 Kalypso 的 dependency/KV admission 仅保留为后续架构参考。PostgreSQL 进程内 semantic
module 拥有 SQL、child plan、snapshot、semantic plan/result parsing 和 query lifecycle；其载体先用
extension 验证，是否升级最小 core patch 由反例审查决定。execution-provider interface 只接收数据库
编译完成的 sealed tasks。
自有 `semloom_pg` 语义算子与 SemLoom 数据执行/调度都继续完成，主实现以不依赖公司私有仓库、
可公开复现为目标。现在完整对照公司 demo 的 SQL 注册、PG 接入方式、算子语义、请求构造、取数与
结果映射、生命周期和外部执行，采用能服务本项目的经验；多算子只是其中一项。未来可把自有算子语义、处理/优化方法与
SemLoom 执行能力移植到公司系统。目标 planner/executor 承接算子方法，provider 适配复用同一执行
核心；一次执行连通不能证明算子优化已移植。参考文件、采用条件和已有部分的保留/调整方式见
[工程参照与成果移植计划](docs/plans/postgresql_ai_semantic_operator_architecture_20260827.md#frontend-adapter-strategy)。
Filter 首先服务于自然语言条件筛行，当前三值输出可以保留；二值/三值的结果承接与 NULL/error
差异需分别定义和验证。可移植不等于完全相同的 SQL 表面；公司移植不阻塞自有主线，内网复用与
外部发布/部署分别确认权限。
当前状态是 `exact-semfilter-reference-calibration-mechanism-validated`：受限 recording `SemMap`/`SemFilter` 与三参
exact `SemFilter CustomScan` paths 已在 `REL_18_3` 通过 PGXS 与生命周期 TAP，受限 Map/Filter direct `INSERT ... SELECT`、PostgreSQL-private
`PgSemanticRuntime`、thin `SemloomExecPump`、独立 operator machines 和
provider-neutral `AiOpenSpec → AiPreparedTask → AiCompletion` `open/drive/close` 接口已实现；同步单在途
UDS provider 与协议 v2/v3 分域 identity/payload/completion digest 已验证。planner-owned schema v1/v2、
transport-neutral error seam、gateway 公共目录迁移、exact instruction/parser/model policy 和 deterministic
golden 已实现；同步 fixed-model adapter、query-fixed execution profile 与真实模型小规模 capability 也已通过。
Filter direct INSERT 的既有接管问题已修复并归档；支持范围以 INFRA_STATUS 为准。
reference `CustomPath` 另有不进入 semantic digest 的 planner estimate metadata，可说明输入行数、
通用 output selectivity、NULL-adjusted calls、estimated prompt/output work 和实际 usage。planner-only
calibration mechanism 能加载并验证匹配的静态 artifact，或在缺失/失配时保留 uncalibrated reference；
当前只有 deterministic artifact 资格，真实 matched artifact、第二 physical path、载体反例审查、
Filter的多在途/乱序 completion 尚未实现。生成Map已完成[受限v6接入](results/postgresql/async_window_20260908/README.md)：
接纳确认与完成接收分离，PG保留有界行数据并关联结果，同一查询可有两个真实模型请求在途；
复杂表达式仍按窗口1执行，多活动会话和Filter/组合异步继续独立推进。
不能把既有 profiler/manifest 实验重标为数据库内算子结果。

### 0.2 核心研究链路

```text
SQL semantic intent
  -> PostgreSQL logical operator + reference/quality policy
  -> reference / optimized physical paths + semantic AI-work cost
  -> sealed tasks through the execution-provider seam
  -> SemLoom work organization + admission/routing/multi-Job execution
  -> PostgreSQL completion validation + relational result
```

数据库 semantic optimizer 决定**产生什么 AI work**：operator identity、reference behavior、近似授权、
模型角色、调用结构、选择率和下游关系基数。SemLoom 决定**这些已封闭 work 如何执行**：物理分组、
提交时机、在途上限、endpoint route 和多 Job 份额。前者使用 calls/tokens/model role/selectivity/quality
比较 semantic paths；后者使用 stage/service work、queue/capacity/locality 比较 execution policies。
两类 cost 通过 task work hint 与 completion telemetry 衔接，但不合并成一个模糊标量。

当前 recording、deterministic-golden 与 fixed-model paths 证明 carrier、生命周期、seam 和最小真实语义合同；
项目已具备 reference calibration 的生成、验证和 planner 消费机制，但 Filter 双路径成本比较仍需要真实
model/workload/service 的 held-out artifact，以及同一逻辑语义下可由 PostgreSQL 区分和选择的第二
physical path。当前 deterministic fixture 不提供真实可比较成本。
数据执行研究随后在固定 semantic task set 上比较，避免把“少做 work”和
“相同 work 执行更快”混为一个结论。

### 0.3 SAOR 系统对照准备记录（历史）

迁移前待执行的 SAOR 系统对照在运行前确定为五臂 PostgreSQL-source→validated-completion operator-E2E（Daft Native、Daft Native/Ray、Ray Data
native graph、project frozen-static、SAOR；共同 vLLM FCFS）。原生臂保留 framework-owned
执行，不注入项目控制；五臂均 `writeback=none`，不把 PostgreSQL sink 混入调度排名。FIFO/DRR/VTC-style/strict-priority 只保留历史项目内消融身份。官方 VTC
另建 S-LoRA 同栈 FCFS/VTC 服务机制组，当前兼容性未验证、formal 未授权，不与五臂系统表混排。
另有独立的四臂跨层 capability，计划比较 Daft Ray + native FCFS/DRR-on-vLLM reproduction/
VTC-on-vLLM reproduction 与 SAOR + native FCFS；当前 frozen installed-source、Job identity 和
custom-FCFS parity 均 blocked，不是可运行实验，也不改变“不修改 vLLM”的主方法边界。
共同外部到达使用 typed Job release；request arrival replay 不再被误写成 native baseline 的必需能力。
本轮 MFU denominator 已写入配置和证据指纹，但统一 FLOP numerator 不可用，故 MFU 不发布数值。
五臂 eager SAOR 的旧 profiler 冲突已在本地修复：只有旧 single-head bounded-priority 继续强制
request replay；bounded-ready 在完整 concrete pre-registration 门下直接消费 eager request envelopes。
截至 2026-08-19，当时尚无成功、可比较的完整五臂 rehearsal；formal 从未运行且继续禁止。
服务器曾有两次 fail-closed rehearsal：`ea4cbb3b` 对应保留 root
`saor_native_system_matched_matrix_20260819_r2/`，在 warmup 第 1 个 Project
selector-sanity cell 因 `unavailable:missing_gpu_peak_tflops` 被 MFU guard 拒绝；
`58154151` 对应保留 root `saor_native_system_matched_matrix_20260819_r3/`，在同一阶段因
`job 0 has no unique successful summary` 被 summary guard 拒绝。两次均通过
`run_saor_native_system_matched.py ... --rehearsal` 入口、无 formal authorization，服务器未发现
独立 tar archive；原 shell history 未保留逐字命令，因此这里只登记由 matrix index 证明的执行模式、
commit/root/cell/原因和预先选定的 runbook 的等价入口，不伪造历史 argv。

2026-08-21 本地 readiness 合同进一步 fail-closed：外层 runner 固定为独立 `DRIVER_PYTHON`，
`VLLM_PYTHON` 只用于子进程 source/package 重审；live endpoint 绑定 PID、进程 start time、未解析
argv0、`sys.prefix` 与实际 vLLM package path/version，并明确设置 `scheduling_policy=fcfs`。
readiness 拆为 static config、service identity、system preflight、correctness smoke 四阶段，仅四者
全部通过才置 `rehearsal_ready=true`。三份实际 config SHA、Daft/Ray upstream tag commit 与薄
adapter SHA 进入证据身份；formal 还必须绑定实际 rehearsal validation/root/archive SHA。`862d0008`
已在服务器完成一次 gateway 前的五臂 correctness smoke 与 rehearsal，证明可运行性但无法给原生臂
提供同口径 request tail/fairness；formal 未运行。

2026-08-21 的当前修订为五臂统一增加严格透传 observation-only gateway，并预先确定 T0--T4：T0 在
PostgreSQL 读取和 child/Ray 初始化前，T1 为首批 source data，T2/T3 为首请求到达/末请求完成，T4
为完整正确结果在内存中可见。Job/group JCT 与 correct throughput 使用完整系统边界，同时分列
source/execution/service span。共同 gateway 不排队、不重试、不重写、不接管原生 scheduler；只用
endpoint actual token usage 在共同积压窗口计算 P99/SLO、weighted Jain、service lag 与最长无服务。
within-run victim impact/recovery 可跨五臂比较，full-solo slowdown 仍需另跑 matched-solo control。
`93271012` 已在服务器通过四阶段 readiness、五臂 correctness smoke 与独立封存的五臂 rehearsal；
五臂 P99/SLO/Jain/lag/no-service 现均可用。单次观察中 SAOR 相对同 executor frozen-static 的 correct
throughput +31.01%、group/bulk JCT −23.70%，但 request P99 +18.70%/+24.11%、Jain −1.50%、
lag P95 +42.47%，继续呈效率—尾延迟—公平权衡。0s/5s 主矩阵缺少统一 pre/post service 样本，
victim inflation/recovery 只标 partial；full-solo 仍需独立控制。该 root 是 rehearsal，不作显著性排名，
formal 从未运行且继续禁止。

## 1. 题目与研究对象

题目为：

> 数据库 AI 负载的执行优化与调度研究

统一研究对象是数据库内 AI 语义算子触发的外部物理执行层：

```text
PostgreSQL SQL AI operator
  -> relational child plan / snapshot / filter / projection
  -> database-managed AI Data Execution Layer
       -> research content 1: work-unit construction and organization
       -> research content 2: state-aware admission, routing and multi-job
       -> shared cost estimator
            -> stage/service/remaining work
            -> SLO slack and uncertainty
  -> Model Service / GPU Executor
  -> Database / Vector Sink
```

对外口径：PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。

PostgreSQL extension 先提供 SQL/planner/query-lifecycle seam；若目标 LOTUS/Cortex rewrite、plan identity
或 lifecycle 出现可复现阻断，允许维护 `REL_18_3` 最小 core semantic patch。默认语义由中立
plan/task/result 合同定义，LOTUS v1.2.4 不再是核心依赖。Daft、Ray、vLLM、pgvector 和 CLIP
是物理执行与验证平台，不是贡献名称。项目不修改 vLLM continuous batching、Ray 调度器、模型结构
或 GPU kernel，也不回到传统 GPU 查询算子。

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
2026-08-13 双轮 matched-ready rehearsal 已完成：DRR/VTC-style 约 12.90K tok/s 且 30s
foreground SLO 零违约；SAOR 约 12.28K tok/s、foreground P99 17.85s。SAOR 用约 4.8%
吞吐和约 5.2% bulk JCT 代价进一步降低 tail，属于观测到的非支配折中点，但固定顺序 n=2、
selector 级 non-inferiority margin 未预注册，故 `formal_authorized=false`，不写 selector 胜出。
2026-08-14 fail-closed 复核进一步确认：旧汇总没有显式区分 completion fairness applicability，且
avoidable-idle 事件名检查错误。最终规则要求五个 bounded-ready 臂 evidence=`ok`；frozen-static
缺 registered-ready ledger，故该指标为 N/A、不能作同口径 service-lag 比较，但不误杀共同性能
矩阵。修复后的 formal 合同同时强制 runtime Job ID 非空/唯一，并要求内部消融各臂的 effective
K/W、weights 与 output cap 在运行前选定、期间不变。最终六臂 rehearsal 已完成，但独立审核前仍不得运行 formal。
修复提交 `15201946` 的同机四臂 development regression 已完成：所有请求 exactly-once、Job ID
合同通过，$0.125W_e$ 触发一次 recovery；$0.25W_e$ 再次因 recovery=0 被 runner 正确 fail closed。
该回归只验证证据链，不计入性能重复，也不改变 formal 锁定状态。
FIFO、DRR、VTC 是已有算法思想，但本实验的可执行版本均由 Project shared-credit coordinator
实现，不是 Daft/Ray Data/upstream vLLM 的原生实现；它们应称项目内标准算法 controls。
bounded-ready 副本只是在 Project harness 中配平候选集的 matched controls，也不表示这些算法
包含本项目机制。报告中必须显式区分实现来源、scheduler owner 和 observation contract。
论文的完整系统价值另用同一 2-Job/PG/vLLM/资源合同下的 Daft Native、Daft Ray、Ray Data
native、project frozen-static 与 proposed 作 system-level matched comparison：原生臂保留自身
调度且不注入 Project K/W，Project 两臂使用相同且运行期间不变的 K/W。该比较只能说明完整系统经验表现。
ready-observation bridge 已用双轮 GPU rehearsal 完成：static→single-head shared FIFO 使 tok/s
+25.96% 但 foreground P99 +99.17%；同 FIFO 下 single-head→bounded-ready 再使 tok/s +7.30%、
foreground P99 −33.62%，但前台 SLO violation 仍约 39.7%。这分离了共享效率、隔离损失和
observation 增量，也证明 observation 不能替代 selector/service differentiation。
历史 JSONL native multi-job 结果把 PG export 放在计时外，且 Daft/Ray Data 当前只可靠提供
shard/Job barrier，不满足新 system-level PG source/sink 与 request-tail schema，禁止直接拼表。

当前主场景是**单租户多 Job/workload class**，不是多租户资源管理；按 `job_id` 记账与当前范围
一致。多 Job 不由单一 VTC/Jain 指标判定。每个 Job 同时报 `multi/full-solo`（总体干扰）、
`multi/reserved-solo`（经验性保留份额非劣）和 `policy-multi/static-multi`（同竞争调度增量）；
共同积压窗口另报 weighted actual service、empirical GPS lag、最长连续无服务和 avoidable idle，
用户层报 worst-Job JCT/P99/SLO。公平与隔离分开：另以固定 victim + aggressor step/burst 报 P99
放大、goodput loss、SLO violation delta 与恢复时间。`group JCT` 等同本批 workload makespan，
仍须与 per-Job/request tail 同报。评价采用明确保护约束下的多目标/Pareto，不压成 composite。
baseline-relative empirical Pareto improvement 不等于 DRF Pareto efficiency；Jain 只表示均匀度，
不能单独证明或否定 share guarantee。

多租户是兼容的后续扩展，不阻塞当前 formal：现有 Job-level ready observation、work/debt、固定
envelope、completion release 和 idle borrowing/reclaim 作为内层保持不变；外层新增稳定
principal identity、tenant entitlement/debt 与 per-tenant buffer cap。只有进入该 scope 后才运行
同一 principal 的 1/2/4-Job 抗拆分门；flat `job_id` 竞争不能直接改名为 tenant fairness。
两层组合仍须重验 tenant floor、Job priority 不越权、双层 debt/reclaim 和非抢占请求下的恢复
时间，不能自动继承当前 Job-level 公平性质。

### 2.3 共同使能组件：算子代价估计

首版采用解析 work 特征、profile 校准和 residual correction，预测文本/图像的 stage work、operator service、JCT、remaining work 与 SLO slack。它同时服务于 active-work 初始化、`WorkDescriptor`/组织预算、路由、提交与多 job，不单列为第三项研究内容。

评价 MAE/MAPE 之外的候选配置 ranking、pairwise accuracy、selection regret、最坏 context 与预测区间；平均误差好不能替代决策质量。

### 2.4 多模态泛化

文本 `AI_COMPLETE` 是主要方法场景；图像 `AI_EMBED/AI_CLASSIFY` 是正文泛化验证。公共策略只消费 staged estimated work、credit、fresh state 和 completion event：文本 adapter 输出 source/tokenize/prompt-output/result work，图像 adapter 输出 encoded/prepare/tensor-model/result work；Organizer、Scheduler、Tracing 和配置逻辑保持一致。不适用某模态的能力必须显式声明。

图像路径新增 HSE（Heterogeneous Staged Execution）作为执行底座候选：Daft/Ray 继续拥有
数据引擎、资源放置和任务执行，typed data plane 管理 encoded/prepared/device/result block，
SAOR 只控制项目侧 Job-head admission 与 byte/work-bounded 中间态。HSE 连接研究内容一和二，
不单列为第三项贡献；static HSE 未超过预先选定的 project static 配置前，不评价动态 HSE。

## 3. 系统与实验边界

```text
PostgreSQL SQL `ai_semantic.map(...)`
  -> extension CustomPath/CustomScan or conditional native semantic node
  -> explicit reference/LOTUS-like/Cortex-like path + ordinary child plan
  -> PreparedSemanticTask + bounded open/drive/close session
  -> recording | direct HTTP | SemLoom execution provider
  -> SemLoom: Shared Cost Estimator + WorkDescriptor + Organizer
  -> SemLoom: Ray actor admission / shared credit / routing
  -> text: vLLM generation
     image: typed CLIP GPU actor
  -> unified PostgreSQL / pgvector sink
```

- 写回采用 PostgreSQL + pgvector、COPY + deferred index，属于统一 correctness/E2E guardrail，不是独立研究内容。
- 数据库内算子主矩阵共享 PostgreSQL child-plan source 与中立 semantic plan/task/result 合同；正式 baseline 必须由
  被测 backend 拥有执行与调度，SemLoom 只做数据库交接、sink、质量审计和指标适配。
- 未修改 LOTUS DataConnector/`pd.read_sql` 路径保留为外部 LOTUS 完整系统 baseline，不进入数据库内算子主矩阵。
- 自写 actor pool、credit、inflight/backpressure 或 Daft UDF 只能按清晰 provenance 标为 SemLoom 方法或 diagnostic reference。
- `BoundedReadyWindow` 属于 SemLoom 方法：不得注入 Daft、Ray Data、vLLM 或数据库产品的原生 baseline；只在 SemLoom 内部 selector 归因消融中保持一致。
- 模型/数据下载不等于数据库 workload 已导入；必须继续执行 importer，并核对 schema、行数及每条记录恰好处理一次。
- 性能参数绑定“机器 + 模型/服务配置 + 协议 + workload 分布/规模”签名，签名变化重新校准；
  同一签名只校准一次并复用预先选定的配置。K 不是逐实验手调，也不在 SAOR formal 中在线变化。

## 4. 研究问题与因果设计

四个研究问题：

1. 固定资源下达到近饱和吞吐所需的最小 active work 是多少，过载怎样影响 tail 与能耗？
2. 相同 work 下怎样组织记录，balance 与 locality 何时冲突？
3. 多 Job 活跃集、arrival 或 work mix 改变时，固定总 K 内的 idle borrowing、reclaim 和
   state-aware ordered release 能否相对 global FIFO/static/DRR 改善 worst-Job JCT/tail/SLO？
4. 多 job 共享 endpoint pool 时，怎样在 work conservation 与 weighted service lag/fairness
   之间形成可验证的 Pareto 改善？

两项策略先独立搜索静态配置，选定后分别消融，再把独立最优拼接，与小规模联合 grid 对比。联合显著优于拼接说明需要联合调优；两者接近说明可分层优化。任何结果都不改变研究对象，但会改变方法适用边界。

正式实验统一要求：immutable manifest、相同 source、相同完整结果语义、相同服务 flags、固定随机种子、warmup 与交错 formal repeats；结果保存请求、submission、资源时序和版本。调度主实验以完整结果 gather 的 correct throughput/JCT 为 headline；仅 database-E2E 护栏要求相同 sink 与 sink readback，另报告 service throughput、质量、failure 类型，以及资源使用是否满足预设要求。

## 5. 当前证据等级

### 5.1 已证明

- 固定行数不是稳定 work 代理：固定 16 行批次的 work 最小/最大中位数为 474/6,793 token，相差 14.3 倍。
- 同一静态上限不是运行状态：W65K 下 high offered load 的运行内峰值 active work/MFU 约为 100%/35%，arrival-limited 约为 29%/7%；前者不是时间平均 active work。
- 当前双 4090/Qwen/vLLM 签名下，65,536 active work/endpoint 达最大已测吞吐均值的 97.80%；下一档只增 0.92%，继续增压会恶化 P99。
- 复杂动态控制不天然优于强静态点：AIMD/PID/EWMA、adaptive flush、service quantum 与多 actor 多数未满足预先规定的约 5% 改善条件。
- 数据组织策略排名受 serving regime 影响：双 endpoint 大 KV 池下策略范围约 12%；四 endpoint 小 KV 池饱和时分化约 27% 且排名反转，重排序可使 prefix hit 降至 0.06–0.07。
- 图像 matched-resource 静态执行结构有可重复收益：主报告保留约 13%–15% operator-JCT 改善；旧 45.7% 资源不匹配，不再使用。
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
  可达。`saor-v0.5` 已在运行前确定为通用有界词典序 release：显式 per-Job priority/剩余 SLO 预算，
  completion-corrected actual-work debt cap 优先阻止饥饿，无 guard 时回退 SAOR；debt-critical
  ready head 不 fit 时只为该队首建立 reclaim barrier，其余只在 fitting heads 间选择；首轮只做
  两 Job 的 $0.125W_e/0.25W_e$ 两个 cap（$W_e$ 是 endpoint work limit，不是 request K）。v0.5.1 已完成 selector/coordinator/scheduler/Ray/runner、
  timeout 清理、lossless event ledger、readiness 与两轮汇总器；事件机制门不再依赖 250 ms
  snapshot。旧 single-head 双轮 GPU development gate 没有满足容量候选的采用条件：$0.25W_e$ 第 2 轮 debt-recovery=0，两个
  cap 的 fg P99 约 49–56s、SLO violation 85%–95%。request/event 交叉验证定位 per-Job 单-head
  pull 没有把完整 Daft/Ray ready backlog 暴露给 coordinator；该失败版本状态为
  `development-run/not-promoted/not-formal-registered`。2026-08-13 已完成独立
  `saor_bounded_ready` 修订：旧
  bounded-priority 保持单-head 回归语义，新路径只预注册已经到达的具体 request，窗口由预先确定
  effective K 与 endpoint 数×W 自动派生；submission trace schema 6 分开 ready、registered、
  granted、submit/service；coordinator release-event schema 2 对 ready registration 与 grant
  统一记录 request ID 和 epoch。runner 先用 submission trace 证明 concrete-ready lifecycle 完整，
  再在 actor 同一时钟域内配对 foreground register→grant，并 fail closed 检查区间内 foreign
  fallback=0。两轮 GPU development gate 已完成：$0.125W_e$ 两轮以约 12.36K tok/s、foreground
  P99 17.58–18.15s、foreground SLO 0% 和 bulk SLO 65.8%–66.6% 通过全部门，注册 formal
  candidate；$0.25W_e$ 因 bulk 30s miss 74.4%–75.2% 两轮越界拒绝。状态为
  `development-gated/formal-registration-candidate-0125k-only`，不是 formal 胜出。post-hoc
  归因审核确认 bounded-ready 同时改变 observation/execution path 与 selector；随后已完成
  **项目内部** FIFO、DRR/WFQ、external VTC-style、strict-priority 和 proposed 的同 ready-window
  双轮归因，以及 single-head shared FIFO→bounded-ready FIFO 的 observation bridge。proposed
  相对 VTC-style 用约 4.8% 吞吐、5.2% bulk JCT 和更长 no-service interval 换取约 31.8%
  foreground P99 改善，是观测非支配折中而不是 selector 胜出；固定顺序 n=2 且未预注册
  selector non-inferiority margin，故历史结果保持 `formal_authorized=false`。现已另建位置平衡的
  Project mechanism 1+3 合同，预先确定 VTC-style 为公平参照、5% headline、吞吐/bulk JCT/SLO/
  longest-no-service 非劣与 30s empirical debt-repayment 门；首次最终 rehearsal 在 SAOR cell
  发现单 recovery 在途无法赶上 debt 产生，10/10 recovery completion 仍留下 2 个未退出 episode，
  已正确 fail closed。修正版使用 residual-aware projected-debt work budget，按活动集同时计入
  全部 own active work 与 foreign residual，并由 schema 5 raw event 离线复算。旧 `d6259f5f`
  root 因缺少逐请求固定输出上界门降为 diagnostic；最终 `63d17300` 全新六臂 root 才是有效
  rehearsal：96/96 recovery completion、15/15 repayment completed、P95 3.234s、0 unresolved，
  1,108/1,108 projection 离线一致，estimate/overshoot-bound violation 均为 0；6,144 条 request
  独立证明当前 chat template overhead 恒为 29、`fixed_output_cap=256`，且公平 service work 使用
  endpoint total token。单次 SAOR 相对 VTC-style 吞吐 +0.43%、foreground P99 +0.11%、P95
  service lag −13.15%、longest no-service +0.014%，在 feeding gate 前只构成 Pareto 候选，
  未证明胜出。独立 raw/SHA/指标复核已通过；授权 validator 已逐字段绑定 validation SHA、
  commit/root/archive/valid-rehearsal，不完整 fairness trace 分支也已 fail-closed 修复，六臂全组件
  指标已重汇总。当前一次性完整签名 direct ceiling 为 13,684.90 tok/s，SAOR
  feeding=92.898%<95%；两侧 group/manifest/运行合同/validation/archive SHA 已绑定，足以按预先确定 gate
  停止当前 formal，但缺结构化 PG/Ray clean record，不能声称稳定损失 7.10%。wrapper/formal contract 已在运行前确定为
  `locked_failed_feeding/formal_authorized=false`，当前 1+3 formal 停止。不能下调门槛、调 K/W 或
  重跑六臂追正。当前仅先做同签名 D0 direct K-only、D1 direct K+W、有界就绪 FIFO K+W 诊断
  的三臂 1+3 配对诊断，分别隔离 W envelope 与 Project plumbing；旧负判决不可被诊断结果撤销。
  代码/合同/结构化 PG-Ray-endpoint clean gate 已就绪，服务器关机故尚无新性能数据。诊断后才补
  同一 2-Job workload 的 Daft Native/Daft Ray/Ray Data native/project static/proposed
  matched comparison；
  原生臂保留自身调度且不接 Project bounded-ready，历史数据签名不完全一致即重跑；
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
  跨 workload 外推与小规模 sink 写回、读回和质量核对，sink 不是性能排名 blocker。
- prompt 变化感知、exact/semantic 结果复用、数据库级/模型内部增量推理已进入
  `parked-conditional` 清单；当前不实现，主路径完成后仅在真实 reuse opportunity≥10% 且扣除
  lookup/build/refresh 后 oracle 潜力≥5% 时重新激活。
- 图像 short→3×long 多作业已完成原生 Daft built-in/Ray Data 40/40 runs、30 formal group，
  Project staged descriptor + observe-only snapshot 也已完成 24/24 group；这些结果只证明各原生
  执行图内的多 Job 干扰和 Project 观测接入，不证明动态策略胜出。DuckDB bounded-output 四作业
  仅完成 128-row native gate，single controls/formal 尚未运行且不阻塞当前主线。图像 proposed
  角色已与具体算法名解耦，后续状态感知/动态调度调整只需版本化并重跑 project static/proposed。

### 5.4 不能声称

- 项目路径普遍优于 direct、DuckDB AI、Daft、Ray Data 或 vLLM 官方路径；
- sequential、length-align 或 prefix-aware 是全局最优 organizer；
- 65,536 是 vLLM 通用容量或最佳并发；
- 动态策略已经胜出；
- 图像路径提升 45.7%；
- 代价模型已经稳健解决。

## 6. 开题前统一文本 database-E2E

开题静态地基比较 `direct_static_sharded`、`duckdb_ai_static_sharded`、`project_frozen_static` 三臂，
统一条件为 PostgreSQL source、immutable equal-row manifest、双 Qwen2.5-7B vLLM endpoint、
prefix cache ON、统一 PostgreSQL sink 与外部 database-E2E。全部数字、重复值与失败记录见
[replacement 记录](results/system_e2e/opening_database_e2e_text_refeed_20260808/README.md)、
[容量扫描](results/system_e2e/opening_bounded_saturation_calibration_20260808/README.md) 与
[原生单 job 观察](results/system_e2e/opening_text_native_single_job_formal_20260808/README.md)；
首轮 failed-feeding 与欠供给对照不作方法排名。

这些运行提炼出的认识：

- 同协议 bounded C128 是该签名下的最小近饱和参照（实测达 C256 的 98.22%）；高 GPU 利用率不等于
  喂饱，C256 已出现过量排队。DuckDB fixed-cap 产品语义失败 4,921/6,144 行的结论仍有效。
- 官方 graph 在预先选定配置下呈现不同外部压力形态：Daft 两臂过量提前提交（waiting mean 数百、
  KV 接近满），Ray Data 供给不足（running mean 约 17），bounded C128 位于最小饱和区；
  这不证明项目方法胜出或某框架内部算法有缺陷。
- 多 Job 干扰的两种到达形态方向相反：在线 replay 下 shared 提高总吞吐但 short/Jain 回退，
  eager 下 shared 相对 static 使 short JCT −48.94%、总吞吐 +31.85%；只作为“多 Job 管理必须
  感知到达/活跃/排空状态、支持空闲借用并保留 SLO/公平保护”的证据，不称动态普遍胜出。
- 统一 T0–T4 计时排除了“Project 模型请求路径慢 6.4×”（与 Daft Native 的 T3 差异仅约 2.5%）。

## 7. 开题叙事图

1. `opening_motivation_work_state`：固定行隐藏 work、静态上限不是状态、提交压力存在最小近饱和点与边际收益递减区，分别导出 WorkDescriptor、感知和有界控制。
2. `opening_ai_data_execution_boundary`：两项研究内容并列，算子代价估计作为共同使能部件。
3. `opening_work_to_schedule_overview`：组织输出 work/locality/deadline，调度结合 fresh state 消费。
4. `opening_work_organization_regime_v2`：work-aware 组织的必要性与 regime 局限。
5. `opening_image_stage_aware_evidence`：图像 prepare/model、transfer 形态和 active-window 动机，只承担 staged work 与状态感知必要性。
6. `opening_image_baseline_evidence_map`：Direct、Daft Built-in、Ray Data、vLLM Pooling、Project 的功能验证、12K 结构诊断与 120K matched-resource 正式排名范围。
7. `opening_cost_model_decision_quality_v2`：代价模型 selection regret 与最坏风险。

权威输出位于 `docs/figures/data/report_main/` 与 `docs/figures/architecture/`，生成脚本为 `docs/figures/scripts/generate_opening_story_figures_20260808.py`，claim 与视觉审计见 `docs/figures/audit/opening_story_figures_contract_20260808.md`。无同上限正式结果的 static–dynamic 示意图继续保持 `do-not-draw-no-result`。制作 PPT 或报告时统一从 `docs/figures/opening_figure_set/` 进入：图集当前有 21 张主讲候选图、10 张 Draw.io 编辑源和 2 张备份图，权威数据与可复现源仍留在原目录。当前 PPT 成品为已独立完成 26/26 页渲染和视觉检查的 v9；2026-08-22 至 08-25 的报告图文更新没有自动回灌该 PPT，跨材料差异审查仍待执行。

## 8. 当前执行顺序

先回答“数据库逐步提供的输入怎样组织和提交，才能及时完成查询并减少不必要的数据留存”。
复用已实现的受限单 Map 多在途、共享执行核心和组织器；真实质量、性能与可比性分别核对。
具体参数、额度和停止条件只在[数据执行计划](docs/plans/data_organization_batching.md#design-hypotheses)维护。

| 工作对象 | 近期工作 | 与其他工作的依赖 |
|---|---|---|
| 真实数据单 Map | SQuAD 输入、完整消息、PG 查询、结果消费和答案评价；已有 ShareGPT 小样本检查 | 对应实际 profile 与窗口，先核对结果关联和资源回收 |
| 静态容量与数据组织 | 先测服务和 PG 供给，再比较输入顺序、工作预算和有限窗口长度分组 | 相同任务与资源，调优数据和评测数据分开；数据留存与完成时间共同判断 |
| 多 Job 调度 | 按需适配已有共享 credit 和公平策略，检查后到查询、暂停恢复和空闲容量利用 | 不以固定等分的弱对照证明新策略；存储与计算额度分别检查 |
| 算子与框架实现 | 只优先修补已选实验中的路径、比较和测量缺口 | 组合、多阶段方法和更广 SQL 按实际需求继续验证，不要求先全面扩展 |
| Filter 语义优化 | 确定质量任务与标签，取得 reference、matched cost，再实现 proxy/oracle 第二路径与 fallback | 仍是 Filter 计划比较的重要完成项，不再阻塞独立核心或生成型 Map |
| carrier 审查 | 随真实路径核对注册身份、函数属性、PG 能力复用、placement、绑定/重扫及生命周期，包含多算子组合 | 只在目标路径出现已复现阻断时增加最小 core patch |
| 公司工程参照与成果移植 | 按主计划完整对照 SQL/PG 接入到外部执行；按需在 fork 分别移植自有算子方法和执行能力 | 保留一套可复用方法与 SemLoom 核心；目标数据库的计划/结果/lifecycle 单独验证，内网复用、外部发布与部署分别获批 |

真实 PG + SemLoom 接入须有相应真实算子、同步对照、版本化 Interface 和 PG18.3 关联/取消/资源等
验证。只有本路径接入通过且 task/model/generation/service/capacity、质量与计时条件匹配，才运行
数据库端到端或 IMLane-like batch placement 对照；不要求先完成不相关的 Filter 第二路径。
独立核心测试也不能替代这些数据库检查。

新增能力按主线、分支验收和待实现项分别记录；独立分支已验证[Map 消息](results/postgresql/semmap_messages_20260903/README.md)
及[C/Python 纯值、Python v5 与旧路径兼容](results/postgresql/semmap_values_20260903/README.md)。
PG plan/C v5/golden 和受限真实模型链路已验证并纳入当前状态；生成型 Map 的资源补证仍待实施。LOTUS compatibility/native baseline
后置；Join、aggregate、Kalypso-like lineage/KV 按真实需求另立项。旧 GPU 矩阵、SAOR、图像动态/HSE、
五臂 formal 与条件性补测继续等待各自计划和授权，不能由“可以并行研发”自动恢复。

历史文本 phase-change 判定、bounded-ready attribution 与五臂 rehearsal 保留在 §5 和结果目录，
只解释证据来源与停止原因，不构成当前隐含待办。

## 9. 结果解释与写作规则

每个正式实验按以下顺序记录：目的、设置、合规自检、设计、全组件数据、解释、对课题含义、下一步。解释明确区分事实、推断、待确认和不能声称。

GPU 利用率优先使用 time-series mean/p50/p95/max；KV usage 按 0–1 分数读取。feeding-saturation 以同协议 bounded direct 为参照；未过门的臂不抽策略性能结论。raw rows/s、correct rows/s 和 service tokens/s 不得互相替代，语义失败必须保留在总行数分母。

正式报告、论文、PPT 和图表不使用内部实验缩写。既有 PG18.4 AutoDL 结果必须按实际链路标为
rehearsal/compatibility evidence，不能冒充已经验证 `REL_18_3` planner-visible semantic operator。

## 10. 同步入口

- 开题报告：`docs/thesis/report/opening_report.md`
- 开题 Claim Matrix：`docs/thesis/claim_matrix.md`
- 当前答辩内容合同：`docs/thesis/opening_defense_outline_20260808.md`；历史PPT v6设计
  `docs/thesis/slides/opening_defense_v6_design.md`已被取代且禁止作为生成输入
- 答辩问答：`docs/thesis/qa_bank.md`
- 答辩 QA 预演手册：`docs/thesis/report/opening_defense_qa/opening_defense_qa.tex`（同目录本地 PDF）
- 当前方向速览：`docs/overview.md`
- 实验状态：`docs/plans/experiment_status_and_gaps.md`
- 文献与知识：`docs/research/knowledge_hub.md`
- 十五篇精读方法速览：`docs/research/精读文献笔记/paper_deep_reading_digest/paper_deep_reading_digest.tex`（同目录本地 PDF）
- 变更日志：`PROJECT_LOG.md`

影响方向、实验结论或关键入口的修改必须同步 `PROJECT_LOG.md`、`PROJECT_INDEX.md`、根 README 和受影响目录 README。
