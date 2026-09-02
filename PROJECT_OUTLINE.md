# 项目大纲

更新时间：2026-09-02

系统名称：**SemLoom**。DB-AIEL（Database-Aware AI Execution Layer）表示其所在架构层，不作为
代码接口或实验身份前缀；完整术语见 [`CONTEXT.md`](CONTEXT.md)。

本文件是项目方向、研究内容、证据等级和近期执行顺序的权威总纲。实验细节以对应结果目录的 README/CSV/JSON 为准；文献入口见 `research/knowledge_hub.md`；开题材料必须服从 `opening/claim_matrix.md`。

读者说明：本文 §0.3 和 §5 保留实验审计原词。“冻结”表示配置或判定标准在实验开始前选定、运行
期间不改变；“门禁/晋级”表示候选纳入比较或采用前必须满足的预设正确性、资源和性能条件；`P0`
等只表示历史实验臂或诊断名称。这些词不构成研究贡献，也不覆盖本文顶部的当前执行顺序。

## 0. 当前优先级与历史记录范围

当前目标锁定 `REL_18_3`；受限 PostgreSQL extension / planner-visible recording `SemMap`/`SemFilter`
与三参 exact `SemFilter` golden/fixed-model reference paths 已验证
`SELECT`、direct `INSERT ... SELECT`、ordinary child plan、snapshot 与 query lifecycle，并通过初始
PostgreSQL-private pump 和 provider-neutral `AiOpenSpec → AiPreparedTask → AiCompletion` 接口调用
in-process 与同步单在途 Unix-domain socket（UDS）provider。scan/pump、neutral port、
recording/UDS adapter 与 versioned wire 的职责拆分已完成。
C/Python 协议 v2 分域 identity/payload/completion digest、长度帧、Unicode、lazy open、PostgreSQL-owned `PROPAGATE_NULL`、
per-drive scratch、编码前输入上限、UTF8 校验、escaped/raw NUL、严格整数、断连、可取消
connect/response wait 和资源清理已在 PostgreSQL 18.3 通过。公共 compatibility suite 已覆盖
RLS/权限、prepared/generic-plan invalidation、savepoint、双 backend、cancel 和 no-task lazy open；
两个 operators 共用 `PgSemanticRuntime`，Map/Filter machines 分别拥有 emit 与
TRUE/FALSE/UNKNOWN keep/drop。planner 已将 recording schema v1 和 exact schema v2 真正消费的
operator/value/policy、instruction、prompt/parser、model/generation、semantic/physical identity 与
`Physical Role=reference` 写入版本化、可 copyObject 的最小 plan spec；input column 作为 executor
binding 独立保存，runtime 统一严格解码并映射为 `AiOpenSpec`。
neutral error interface 不再暴露 socket/JSON/frame operation，adapter 只返回中立类别和本地生成的定长
脱敏详情。Python gateway 的 framing、wire v2、recording adapter 与 server 已迁入
`code/src/execution_provider/`；extension 子树中的旧 import/CLI 只负责自定位并转交，不保存协议或 server 逻辑。
exact-reference 纵切面已实际消费 instruction、prompt program、result parser、model/generation
constraints 和 policy；wire v3 与 deterministic golden adapter 已端到端验证该最小 plan/task/result
contract，但 golden fixture 不是模型也不证明自然语言判断质量。之后的 4A.1 已收紧 v3 error frame、
共享 C transport/JSON 归属和 canonical-message 构造前的
input-limit preflight，并补齐 Unicode、空串与 savepoint/recovery 证据。
4B 又在 gateway 内抽出共享 v3 session runner 与 completion adapter，以 query-fixed execution profile
区分 golden 和固定 OpenAI-compatible endpoint；PostgreSQL 继续拥有 digest/model validation、严格
tristate parser 与 keep/drop。小规模 Qwen2.5-1.5B-Instruct/vLLM capability 已跑通，但不证明质量或性能。
提交 `47407751` 已分开 reference 的 semantic-input rows、NULL rate、通用 output-selectivity estimate、
estimated calls/work 与 provider 返回的实际 usage；`71a8ef7d` 又明确将其标为 uncalibrated。
`dcde2be5` 已增加离线 reference calibration artifact builder、held-out validator、跨 Python/PostgreSQL
identity 和 planner-only loader；匹配 artifact 时 EXPLAIN 保存 calibration/workload/service identity、
预测 service milliseconds 与误差，失配时继续使用 uncalibrated exact reference。该提交只用
deterministic fixture 验证合同。[2026-09-01 首轮真实采集](experiments/results/postgresql/semfilter_reference_calibration_20260901/README.md)
完成 64 条预热后，因首个 training 查询第 23 个模型输出格式错误而停止；held-out 和拟合均未运行。
[后续小切片](experiments/results/postgresql/semfilter_qualification_20260901/README.md)已修复 builder 的
可辨识性检查，并在 PG18.3 验证普通多列统计；choice 候选虽有 30/30 合法格式，预期语义仍只符合
12/27，即 9 个独立样例中 4 个符合预期、各重复三次。
[单一 prompt 后续对照](experiments/results/postgresql/semfilter_prompt_qualification_20260901/README.md)
未发现实际 messages/template 不一致；新 prompt 在 1.5B 的旧/新样例各 5/9，matched 7B 上为
7/9、6/9，均未通过。生产配置不变，整轮采集继续暂停。下一工程切片独立接入
[显式选择的 choice 生成配置](experiments/plans/postgresql_choice_profile_engineering.md)，
让数据库保存并传递三值输出要求；新 SQL option、schema 3 与 wire v4 均尚未实现。
这只验证新能力能否正确接入，不表示模型质量通过，也不更换默认 reference 或恢复真实校准。
PG 随后优先完成[真实生成型 SemMap](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#real-semmap-work-package)。
SemLoom 核心可以先用公开任务、可控时钟和执行替身验证增量 session、数据组织、有界提交与多 Job，
不等待 Filter 质量或第二路径。Filter 仍须另行取得合格 reference、真实 matched artifact 与第二 path，
其失败不会被本次排期调整改判。carrier 审查随各真实路径增量进行；新增 PG 接入、重排与端到端比较
分别验证对应语义、生命周期和资源，不借用纯核心测试。只有已复现阻断才增加最小 core patch。
并行研发不自动恢复旧 GPU 矩阵、SAOR 或五臂 formal；外部/emulated 结果保持原身份，新的实验需要
具体计划与授权。LOTUS compatibility/native baseline 后置且不阻塞主实现。

下方 2026-08-19 至 2026-08-21 的 SAOR/readiness/rehearsal 细节是已完成准备工作的历史记录，
用于解释证据来源和 formal 为何仍未授权；它们不能覆盖上述当前顺序。

### 0.1 PostgreSQL AI 语义算子实施入口

数据库内 AI 语义算子的权威实施入口为
[`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)：
参考 Sema/Cortex 的数据库语义所有权、LOTUS 的 reference/optimized algorithms、IMLane 的 DB-runtime
batch pump，并把 Kalypso 的 dependency/KV admission 仅保留为后续架构参考。PostgreSQL 进程内 semantic
module 拥有 SQL、child plan、snapshot、semantic plan/result parsing 和 query lifecycle；其载体先用
extension 验证，是否升级最小 core patch 由反例审查决定。execution-provider interface 只接收数据库
编译完成的 sealed tasks。
自有 `semloom_pg` 语义算子与 SemLoom 数据执行/调度都继续完成，主实现以不依赖公司私有仓库、
可公开复现为目标。公司 demo 作为算子工程参考，公司 fork 承担工业前端接入与获批环境验证，复用同一
SemLoom execution provider；不是替代自有前端或复制第二套调度器。接口映射提前核对，内网代码复用与
外部发布/AutoDL 部署分别确认权限。详细分工与待核对项见[前端适配设计](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#frontend-adapter-strategy)。
当前状态是 `exact-semfilter-reference-calibration-mechanism-validated`：受限 recording `SemMap`/`SemFilter` 与三参
exact `SemFilter CustomScan` paths 已在 `REL_18_3` 通过 PGXS 与生命周期 TAP，direct `INSERT ... SELECT`、PostgreSQL-private
`PgSemanticRuntime`、thin `SemloomExecPump`、独立 operator machines 和
provider-neutral `AiOpenSpec → AiPreparedTask → AiCompletion` `open/drive/close` 接口已实现；同步单在途
UDS provider 与协议 v2/v3 分域 identity/payload/completion digest 已验证。planner-owned schema v1/v2、
transport-neutral error seam、gateway 公共目录迁移、exact instruction/parser/model policy 和 deterministic
golden 已实现；同步 fixed-model adapter、query-fixed execution profile 与真实模型小规模 capability 也已通过。
reference `CustomPath` 另有不进入 semantic digest 的 planner estimate metadata，可说明输入行数、
通用 output selectivity、NULL-adjusted calls、estimated prompt/output work 和实际 usage。planner-only
calibration mechanism 能加载并验证匹配的静态 artifact，或在缺失/失配时保留 uncalibrated reference；
当前只有 deterministic artifact 资格，真实 matched artifact、第二 physical path、载体反例审查、
accepted-prefix 和多在途/乱序 completion 尚未实现；
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

迁移前待执行的 SAOR 系统对照冻结为五臂 PostgreSQL-source→validated-completion operator-E2E（Daft Native、Daft Native/Ray、Ray Data
native graph、project frozen-static、SAOR；共同 vLLM FCFS）。原生臂保留 framework-owned
执行，不注入项目控制；五臂均 `writeback=none`，不把 PostgreSQL sink 混入调度排名。FIFO/DRR/VTC-style/strict-priority 只保留历史项目内消融身份。官方 VTC
另建 S-LoRA 同栈 FCFS/VTC 服务机制组，当前兼容性未验证、formal 未授权，不与五臂系统表混排。
另有独立的四臂跨层 capability，计划比较 Daft Ray + native FCFS/DRR-on-vLLM reproduction/
VTC-on-vLLM reproduction 与 SAOR + native FCFS；当前 frozen installed-source、Job identity 和
custom-FCFS parity 均 blocked，不是可运行实验，也不改变“不修改 vLLM”的主方法边界。
共同外部到达使用 typed Job release；request arrival replay 不再被误写成 native baseline 的必需能力。
本轮 MFU denominator 被配置和证据指纹冻结，但统一 FLOP numerator 不可用，故 MFU 不发布数值。
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
commit/root/cell/原因和冻结 runbook 的等价入口，不伪造历史 argv。

2026-08-21 本地 readiness 合同进一步 fail-closed：外层 runner 固定为独立 `DRIVER_PYTHON`，
`VLLM_PYTHON` 只用于子进程 source/package 重审；live endpoint 绑定 PID、进程 start time、未解析
argv0、`sys.prefix` 与实际 vLLM package path/version，并显式冻结 `scheduling_policy=fcfs`。
readiness 拆为 static config、service identity、system preflight、correctness smoke 四阶段，仅四者
全部通过才置 `rehearsal_ready=true`。三份实际 config SHA、Daft/Ray upstream tag commit 与薄
adapter SHA 进入证据身份；formal 还必须绑定实际 rehearsal validation/root/archive SHA。`862d0008`
已在服务器完成一次 gateway 前的五臂 correctness smoke 与 rehearsal，证明可运行性但无法给原生臂
提供同口径 request tail/fairness；formal 未运行。

2026-08-21 的当前修订为五臂统一增加严格透传 observation-only gateway，并冻结 T0--T4：T0 在
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
  归因审核确认 bounded-ready 同时改变 observation/execution path 与 selector；随后已完成
  **项目内部** FIFO、DRR/WFQ、external VTC-style、strict-priority 和 proposed 的同 ready-window
  双轮归因，以及 single-head shared FIFO→bounded-ready FIFO 的 observation bridge。proposed
  相对 VTC-style 用约 4.8% 吞吐、5.2% bulk JCT 和更长 no-service interval 换取约 31.8%
  foreground P99 改善，是观测非支配折中而不是 selector 胜出；固定顺序 n=2 且未预注册
  selector non-inferiority margin，故历史结果保持 `formal_authorized=false`。现已另建位置平衡的
  Project mechanism 1+3 合同，冻结 VTC-style 为公平参照、5% headline、吞吐/bulk JCT/SLO/
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
  feeding=92.898%<95%；两侧 group/manifest/运行合同/validation/archive SHA 已绑定，足以按冻结 gate
  停止当前 formal，但缺结构化 PG/Ray clean record，不能声称稳定损失 7.10%。wrapper/formal contract 已冻结为
  `locked_failed_feeding/formal_authorized=false`，当前 1+3 formal 停止。不能下调门槛、调 K/W 或
  重跑六臂追正。当前仅先做同签名 D0 direct K-only、D1 direct K+W、P0 bounded-ready FIFO K+W
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

2026-08-07 首轮三臂因 project feeding 仅为 direct 的 89.9%/91.38%，保留为 failed-feeding 历史诊断。2026-08-08 K128 replacement 的 24/24 单元、18 formal 满足预先规定的正确性、写回、身份和稳定性要求；但随后 ShareGPT bounded C32–C256 扫描证明 C32 只有已测峰值的 52.07%，故 ShareGPT 三臂性能排名降级，正式原生矩阵改用达到峰值 98.22% 的最小点 C128。

开题静态地基先完成 SQuAD short-answer 均匀控制组与 ShareGPT controlled-skew 异质组。两组均比较：

- `direct_static_sharded`；
- `duckdb_ai_static_sharded`；
- `project_frozen_static`。

统一合同：PostgreSQL source、immutable equal-row manifest、双 Qwen2.5-7B vLLM endpoint、prefix cache ON、统一 PostgreSQL sink、外部 database-E2E、质量与资源指标、1 warmup + 3 formal。

SQuAD replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 136.63、136.68、137.77，service tokens/s 为 40,920.72、40,955.99、41,277.95；三臂 EM/F1 接近。Project 路径的外层计时还包含更多指标采集、记录写入和结束处理，因此这些记录值只用于核对完成性与质量，不能根据不到 1% 的差异判断性能高低。

ShareGPT replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 11.36、2.26、17.55，service tokens/s 为 9,425.25、9,421.31、14,568.91。后续 bounded C32/C64/C128/C256 扫描为 9,454.88/14,057.93/17,834.14/18,158.19 tok/s；C128 实测达到 C256 的 98.22%，是第一个满足预先规定 97% 选择条件的并发点。C256 仅增 1.82%，却使 waiting mean=116.8、KV max=0.9996、TTFT mean=6.18s。旧 project/C32-direct=1.5457 因对照欠供给而不作方法排名。DuckDB fixed-cap 产品语义失败 4,921/6,144 行的结论仍有效。

同一 ShareGPT Chat manifest 的原生单 job 1+3 已完成：bounded C128、Daft Native、Daft Ray、Ray Data 的 service tok/s 为 17,800/17,286/16,747/3,551，四臂 CV<0.6%。Daft 两臂 waiting mean 为 783/742、KV max≈1，呈现过量提前提交；Ray Data running mean=17.3、MFU=0.112，呈现供给不足；bounded C128 位于最小饱和区。该结果只证明官方 graph 在预先选定配置下的外部压力形态，不证明项目方法胜出或某个框架内部算法有缺陷。

5s guaranteed-overlap 对照已完成：Daft Native/Ray、Ray Data 的 short JCT 相对各自 single 增加82.42%/104.84%/32.76%，只作外部观察。项目在线 replay 下 quota-only≈0，static/shared 加入 long 后 short JCT增加3.79%/8.95%；shared 提高总吞吐但 short/Jain回退。统一 eager Project 12 formal 又显示 full→half quota-only 已使short JCT+59.00%，matched half→static+long再+58.77%，matched full→shared+long+28.90%；eager shared 相对static使short JCT−48.94%、总吞吐+31.85%、long JCT−25.75%、Jain 0.894→0.972。两种到达regime方向相反，只作为“多Job管理必须感知arrival/active/drain状态、支持idle borrowing并保留SLO/fairness guard”的证据，不称动态普遍胜出；开题前不再扫offset/weight/更多Job追正。

Project all-at-t0 single-short 诊断已补齐统一 T0–T4 计时：T0 profiler E2E14.957s，T3 earliest model submit→latest completion11.354s，service14,361tok/s、MFU42.93%；Daft Native同一short T3为11.059s、14,727tok/s、MFU44.04%，差异仅约2.5%–2.7%。Daft缺准备前T0，因此完整E2E仍不排名；该结果排除了“Project模型请求路径慢6.4×”。随后Project eager多Job只补full single、half single、static+long、shared+long，不重跑原生三臂；arrival span均为66.76µs、12/12 formal通过。逐阶段显示matched static竞争使short service mean/P99 +50.34%/+78.62%，shared为+14.63%/+28.70%；submit→service仍约2ms。在线replay与eager结论分轨保留。

## 7. 开题叙事图

1. `opening_motivation_work_state`：固定行隐藏 work、静态上限不是状态、提交压力存在最小近饱和点与边际收益递减区，分别导出 WorkDescriptor、感知和有界控制。
2. `opening_ai_data_execution_boundary`：两项研究内容并列，算子代价估计作为共同使能部件。
3. `opening_work_to_schedule_overview`：组织输出 work/locality/deadline，调度结合 fresh state 消费。
4. `opening_work_organization_regime_v2`：work-aware 组织的必要性与 regime 局限。
5. `opening_image_stage_aware_evidence`：图像 prepare/model、transfer 形态和 active-window 动机，只承担 staged work 与状态感知必要性。
6. `opening_image_baseline_evidence_map`：Direct、Daft Built-in、Ray Data、vLLM Pooling、Project 的功能验证、12K 结构诊断与 120K matched-resource 正式排名范围。
7. `opening_cost_model_decision_quality_v2`：代价模型 selection regret 与最坏风险。

权威输出位于 `figures/data/report_main/` 与 `figures/architecture/`，生成脚本为 `figures/scripts/generate_opening_story_figures_20260808.py`，claim 与视觉审计见 `figures/audit/opening_story_figures_contract_20260808.md`。无同上限正式结果的 static–dynamic 示意图继续保持 `do-not-draw-no-result`。制作 PPT 或报告时统一从 `figures/opening_figure_set/` 进入：图集当前有 21 张主讲候选图、10 张 Draw.io 编辑源和 2 张备份图，权威数据与可复现源仍留在原目录。当前 PPT 成品为已独立完成 26/26 页渲染和视觉检查的 v9；2026-08-22 至 08-25 的报告图文更新没有自动回灌该 PPT，跨材料差异审查仍待执行。

## 8. 当前执行顺序

已有 recording/真实 Filter 同步 reference、公共 runtime/provider 与生命周期验证继续复用；下一步按
工作对象分别推进，不把某一分类模型的失败变成整个执行系统研发的前置阻塞。

| 工作对象 | 近期工作 | 与其他工作的依赖 |
|---|---|---|
| 自有 PG 算子 | [四 C 可选 choice](experiments/plans/postgresql_choice_profile_engineering.md)，随后[四 D 真实生成型 SemMap](experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#real-semmap-work-package) | 新算子拥有独立语义、版本与同步验证，不等待 Filter 分类质量通过 |
| SemLoom 核心 | 现有行为表征、公开任务驱动增量 session、work organization、有界提交、多 Job 与路由 | 可以先用 fixture/外部 workload；不是已接入数据库的证据 |
| Filter 语义优化 | 确定质量任务与标签，取得 reference、matched cost，再实现 proxy/oracle 第二路径与 fallback | 仍是 Filter 计划比较的重要完成项，不再阻塞独立核心或生成型 Map |
| carrier 审查 | 随生成型 Map、PG batch/reorder、Filter 第二路径分别核对 identity、placement 和生命周期 | 只在目标路径出现已复现阻断时增加最小 core patch |
| 公司接入 | 只读接口映射、必要且单独授权的 deterministic spike，之后在 fork 正式适配 | 共用一套 SemLoom 核心；内网复用、外部发布与部署分别获批，不作为自有主实现的私有依赖 |

真实 PG + SemLoom 接入须有相应真实算子、同步对照、版本化 Interface 和 PG18.3 关联/取消/资源等
验证。只有本路径接入通过且 task/model/generation/service/capacity、质量与计时条件匹配，才运行
数据库端到端或 IMLane-like batch placement 对照；不要求先完成不相关的 Filter 第二路径。
独立核心测试也不能替代这些数据库检查。

目前新增项均是待实施设计，本次没有启动源码或模型任务。LOTUS compatibility/native baseline
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

- 开题报告：`opening/report/opening_report.md`
- 开题 Claim Matrix：`opening/claim_matrix.md`
- 当前答辩内容合同：`opening/opening_defense_outline_20260808.md`；历史PPT v6设计
  `opening/slides/opening_defense_v6_design.md`已被取代且禁止作为生成输入
- 答辩问答：`opening/qa_bank.md`
- 答辩 QA 预演手册：`opening/report/opening_defense_qa/opening_defense_qa.tex`（同目录本地 PDF）
- 当前方向速览：`overview/current_direction_and_plan.md`
- 实验状态：`experiments/plans/experiment_status_and_gaps.md`
- 文献与知识：`research/knowledge_hub.md`
- 十五篇精读方法速览：`research/精读文献笔记/paper_deep_reading_digest/paper_deep_reading_digest.tex`（同目录本地 PDF）
- 变更日志：`PROJECT_LOG.md`

影响方向、实验结论或关键入口的修改必须同步 `PROJECT_LOG.md`、`PROJECT_INDEX.md`、根 README 和受影响目录 README。
