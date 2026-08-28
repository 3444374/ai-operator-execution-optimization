# 实验状态与缺口分析

更新日期：2026-08-28

> **当前执行摘要**：`REL_18_3` extension planner-visible `SemMap` 的受限 `SELECT`、direct
> `INSERT ... SELECT`、PostgreSQL-private pump、provider-neutral `open/drive/close` seam、协议 v2
> C/Python canonical digest 与同步单在途 UDS
> recording provider 已通过 PostgreSQL 18.3 功能测试；lazy open、PostgreSQL-owned `PROPAGATE_NULL`、
> per-drive scratch、per-tuple completion copy、编码前输入上限、UTF8 校验、可取消 nonblocking connect
> 和 query-context cleanup 已验证；scan/pump、neutral port 与 recording/UDS adapters 的职责拆分已完成。
> 下一步实现 exact `SemFilter` 和最小第二 physical path。载体审查确认 extension 足够时继续使用，
> 只有 LOTUS/Cortex plan alternatives
> 或 node lifecycle 出现已复现阻断才增加最小 core patch。数据库语义资格完成后才扩 accepted-prefix、
> 多在途、增量 SemLoom session 与 IMLane-like batch placement。Kalypso-like lineage 只作后续参考，
> 不纳入当前排期。LOTUS v1.2.4
> 不是前置依赖。此前文本、
> 图像静态、observe-only 与代价估计证据继续有效，但当前不扩 GPU 矩阵、不调 SAOR。下文按日期保留
> 状态演进；日期较早的“下一步”只有被本摘要或当前架构计划再次确认时才有执行效力。

## 2026-08-20 SAOR 对照重构状态（覆盖旧八臂执行说明）

2026-08-21 进一步按系统比较第一性原理补齐共同观测：五臂的 T0 都是父 runner 实际释放 Job、且在
PostgreSQL source query 与 child/Ray 初始化之前；T1 是首批 source data 进入执行器，T2/T3 来自统一
observation-only gateway 的首请求到达/末请求完成，T4 是完整正确结果在内存中可见。headline 使用
Job JCT=T4-T0、group JCT=max(T4)-min(T0) 与 actual completed tokens/group JCT，并分列 source、
execution、service span。gateway 对所有臂使用同一实现，只按 Job/endpoint path 标识请求；不排队、
不重试、不限并发、不重写 body，也不接管 Daft/Ray 的顺序、batch 或 backpressure。共同积压窗口内
用 endpoint usage 的实际 token work 计算 weighted share/Jain、completion-accounted service lag 与
longest no-service；另报 foreground request SLO、Job JCT SLO 和 within-run victim impact/recovery。
full-solo slowdown 仍需独立 matched-solo control，当前 within-run 指标不得冒充该反事实。

本地合同已重构为五臂 database-E2E：Daft Native、Daft Native/Ray、Ray Data native graph、
project frozen-static、SAOR，服务层均为同签名 vLLM FCFS。原生三臂拒绝 bounded-ready/K/W/
credit/inflight/project selector；static 与 SAOR 只共享资源上限，static 不使用动态 ready/debt。
旧 FIFO/DRR/VTC-style/strict-priority rehearsal 继续作为历史项目内消融证据，但不生成本轮系统
cell、第二张 selector 表或主排名。

Job release 已从 `arrival replay` 中独立成 typed `[job0@0s, job1@5s]` epoch；eager 是 Job 内部
请求可见性，request arrival replay 是执行器内部可选能力，bounded-ready 是 SAOR 对 release 后
concrete work 的观察。MFU 的 peak=165 TFLOPS/GPU 与
`bf16_dense_fp32_accumulate` 进入 resolved config、fingerprint、cell 和 summary 复核；因原生与
Project 暂无统一可信 FLOP numerator，本轮 MFU 明确为 unavailable，不能用环境漂移补值。

审查发现 `e98a0f1b` 仍把 `saor_bounded_ready` 与旧 single-head
`saor_bounded_priority` 一起强制要求 `--arrival-replay`，而五臂 Project 合同正确地在 eager
模式下拒绝该 flag，因此 SAOR rehearsal cell 会在 profiler 参数校验阶段停止。现已在本地收窄
校验：旧 bounded-priority 继续要求 replay；bounded-ready 仅在 request granularity、
`bounded_concrete_pre_registration`、正 payload-byte limit 和 request trace 同时成立时允许 eager。
非 replay 执行把同一 observed Job start epoch 写入 scheduler request 与 trace seed，再通过 concrete
request envelope 进入受 K/work/bytes 限制的 register→grant→submit 路径。
真实 matched argv 回归已通过。`862d0008` 的 gateway 前 rehearsal 只保留为可运行性历史证据；
`93271012` 已在服务器完成新 gateway 合同的四阶段 readiness、五臂 correctness smoke 和独立 archive
validation 的五臂 rehearsal。单次观察中 SAOR 相对同 executor frozen-static 的 correct throughput
+31.01%、group/bulk JCT −23.70%、foreground JCT −1.72%、单位 token 能耗 −25.30%；同时 bulk/fg
request P99 +18.70%/+24.11%、weighted Jain −1.50%、lag P95 +42.47%、longest no-service +16.75%。
因此只冻结为效率—tail—公平权衡，不判 selector winner。五臂共同 request tail/fairness 已可用；
0s/5s 主矩阵的 pre/post isolation 样本仍不足，full-solo 需独立 control。formal 从未运行且继续禁止。

官方 VTC 独立 capability 合同固定 upstream commit、S-LoRA runtime owner、同栈 FCFS/VTC、
逻辑 workload SHA 与 Job release。官方文档的 CUDA/PyTorch/Ampere 假设尚未在当前 RTX 4090/
模型栈验证，因此 `blocked_unverified_runtime`；server validation 未运行，formal 未授权。
该 capability 组没有连接服务器或运行 rehearsal/formal，也没有产生性能结论。

新增的 SAOR vs DRR/VTC-on-vLLM 跨层 capability 与上述 official artifact、五臂矩阵均分离。纯
FCFS/DRR/VTC oracle、strict Job identity decoder、vLLM `--scheduler-cls` skeleton、module SHA 和
配置/evidence schema 已完成；本机无 vLLM/Daft/Ray，installed-source、Daft identity-only transport
和 custom-FCFS 八项 parity 尚未验证。DRR/VTC class path 当前主动 blocked，不伪装成可执行实现；
server validation `not_run`、formal `false`。

Date: 2026-07-20（最后更新：2026-08-15；开题证据冻结，SAOR fixed-envelope formal 已
完成但未晋级；bounded-ready v0.5.2 的 matched-observation selector 双轮 rehearsal 已完成，
2026-08-14 fail-closed 复核已将 completion fairness applicability、Job identity、K/W/weights、
projected-debt repayment 和 admission work-cost 上界统一闭环。最终 `63d17300` 六臂 rehearsal
6/6、0 incident，6,144-request fixed-output-cap audit 与 15/15 repayment episode 均通过；
`formal_authorized=false`，单次结果不判策略排名。独立审核已确认 validation/archive SHA、raw
复算和机制 counters 一致；同签名 direct ceiling 最终给出 92.898%<95% 的有效一次性负判决，
formal 合同已永久冻结为 `locked_failed_feeding`，不再补工具或尝试解锁。当前只允许先运行独立
D0/D1/P0 feeding-gap diagnostic，再决定是否有一个简单 selector-neutral 工程缺口；原生 Daft/Ray
Data 多 Job comparison 排在该诊断之后，原生 baseline 不接 bounded-ready，dynamic-K 仍退出主线）

2026-08-15 已完成但未上服务器的最小诊断设施：D0=direct K-only、D1=direct K+W65536、
P0=bounded-ready FIFO K+W65536，固定 K128、同 manifest/service/work-cost，按 `1 warm-up + 3`
配对交错执行。D1 仅用 endpoint-local typed-work reservation/completion release，不含 Job fairness、
ready window 或 SAOR selector；因此它是 Project diagnostic control，不是原生 baseline。runner 结构化
保存 PG/Ray/endpoint clean gate，direct 侧保存 request/work occupancy 与 admission wait，P0 复用
credit trace 并汇总 Ray submit/actor-ready；共同保存 vLLM、MFU、TTFT/ITL、JCT/SLO 与能耗。当前
服务器关机，所以状态是 `code-and-contract-ready / gpu-unrun`，没有新性能数字。四种 0.95 配对判决
及退出规则只见 `state_aware_work_unit_evaluation_20260808.md` §5.2，任何结果均不得修改旧
`locked_failed_feeding`。

2026-08-14 历史本地基础设施状态（已被 2026-08-20 五臂合同覆盖）：native-system matched comparison 的八臂合同、薄编排器与
两层 offline fail-closed summarizer 已完成本地测试，但用户已取消本轮服务器 rehearsal，故
GPU evidence 仍未完成。输出分成五臂 complete-system empirical 表与四臂 Project-internal
sanity 表；共享同一个 SAOR 物理 run。FIFO 臂必须写全名 **Project bounded-ready + global
FIFO matched-control**。共同到达保持 Job `[0,5]` release、Job 内 eager；PostgreSQL source
在 Job lifecycle 内计时到 validated gather；旧 root 中原生 request P99/SLO 仍只能写
`unavailable`+原因，新 gateway 合同运行后五臂才统一可用。后续固定顺序为 runtime preflight → static readiness → small
correctness/local fake rehearsal → review → separately authorized GPU execution；当前工作不含
server/GPU run，不把 formal/GPU 证据标成完成。

本文档是对 2026-07-18/19 本地 vLLM + Qwen2.5-1.5B AI_COMPLETE baseline 系列的全面审计，记录已完成实验、已证明的 claim、未完成的缺口、指标盲区、下一步实验路线图，以及 2026-07-23 完整问题审计（P0/P1/P2 分级 + 认知债务清单）。

## 2026-08-11 状态感知 phase-change 最新结论

修正 A-only backlog 判据、HTTP tail-drain 和多 Job 全局 arrival clock 后，A=20 的
K128/K160 门禁通过（每 endpoint service rate +7.77%）；B=2.5/3.5/4.5 pressure
均未形成双 endpoint、双周期 upper risk，按最高预注册档失败停止。action/formal 未运行，
state-aware 价值保持待验证；下一轮必须是带显式 drain/recovery 的独立合同，不与本轮合并。

同日完成的 SAOR capacity-only 四臂 development gate 不改变上述结论：4/4 arm 均完成
5,266 请求、0 incident；SAOR 相对 K128 吞吐 +4.36%，但相对 K160 仅 +0.52%、相对简单
threshold −1.46%，Jain 也最低。K160 相对 K128 仍有 +3.82% 吞吐和 −3.68% duration，
但 Job B P99 +23.93%、Jain −3.22%、KV P95 0.826→0.997。故 K160 冻结为强效率 baseline
兼 tail/fairness 风险点；当前 aggregate two-arm capacity adapter 标记 `not-promoted`，不在该
workload 继续扫权重/K，也不把它追加到公平专场。`saor-v0.4` 已把 fixed-envelope
SAOR-Release 定为唯一算法候选，并将 dynamic K 标记为 `parked-conditional`：下一项算法验证
不是 phase-change 调 K，而是固定总 K 下的 active-set entitlement、idle borrowing/reclaim 和
ordered release。完整报告见
`experiments/results/saor_capacity_development_20260811/README.md`。

fixed-K 决定性矩阵已于 2026-08-12 完成：direct/static/FIFO/DRR/external-VTC/SAOR 六臂
active-set 与四 matched-solo 共 40/40 cell、0 incident、exactly-once。定位性均值显示 SAOR
12,393 tok/s、fg P99 50.3s、fg slowdown 3.45，在四个 credit 臂中 fg 最好；static 只有
9,508 tok/s，却以 fg P99 29.2s、fg slowdown 2.19 和 0% SLO violation 成为更强隔离 Pareto
点。原始 validation 因 DRR/VTC rep2 的 5.8ms/4.8ms simultaneous drain 产生假阴性；现已冻结
250 ms trace-resolution 规则，并在服务器完整 artifact 上用默认 summarizer 重汇总：resolution-aware
v2 validation passed、四个 credit 臂 effective mechanism 12/12；原 failed 文件保留审计。仍不能写
SAOR 胜出，也不能把原始假阴性写成 baseline 机制失败。

当前状态为 `formal-valid / not-promoted`。formal 的 SAOR 配置
`slo_weight=0`，且不可抢占已进入 vLLM 的请求；无保护余量时，bulk 在前台到达前占满包络，
foreground 必须等待 completion 释放 credit。故下一步不扫 fairness/SLO 权重，也不跑 4-Job：
simultaneous-drain 审计与完整重汇总已完成；foreground strict-priority 两轮 GPU 短测以 11,791
tok/s 达到 fg P99 14.27s、SLO 0%，证明 release-only 可达，但 hard priority 尚无 anti-starvation。
下一步只做 2–3 个有界 lexicographic priority-window/service-lag guard 点；reservation 和 q95 work
credit 作为鲁棒性消融。达到 static fg 非劣、吞吐≥static+5% 且 bulk lag/SLO 不越界才注册 formal。

2026-08-12 本地工程增量：`saor_bounded_priority` 已接通 actual-work debt cap、每 Job 单张
recovery lease、ready-head reclaim barrier、显式 priority/SLO window、旧 SAOR fallback、
timeout waiter cleanup 和 Ray lossless release-event ledger；四臂模板只含 static、release-only、
$0.125W_e/0.25W_e$（历史 scenario ID 写 K，但实现 fraction 乘 endpoint work limit）。readiness
与两轮汇总器均 fail closed，机制门不再使用 250 ms snapshot 猜测
短转换；缺账本/空账本/序号缺口/重复一律失败。本地受影响套件通过，代码已推送。

2026-08-13 双轮 GPU development gate 已按冻结合同执行并停止。Round 1 四臂 clean；Round 2
$0.25W_e$ 的 debt-recovery grant 为 0，runner 和跨轮汇总器均 fail closed。$0.125W_e$ 两轮 fg P99
56.47/56.29s、SLO violation 94.5%/92.6%；$0.25W_e$ 为 49.03/50.10s、85.2%/87.5%，均未达到
30.7s/1% 门。GPU mean 95.8%–97.6%、tokens/s 12.2–12.4K，排除欠供给解释。逐请求/event
交叉验证进一步定位 observation gap：所有已注册 foreground head 都获 `slo_priority`，但当前
每 Job 同步 pull 一次只注册一个 head；相邻 acquire 间 coordinator 看不到 Daft/Ray 的完整 ready
backlog，因而仍向 bulk fallback。v0.5.1 状态改为
`development-run/not-promoted/not-formal-registered`；停止 cap 密扫和 4-Job，先修 bounded
ready-set/unfinished-priority observation contract，reservation 继续后置。

2026-08-13 本地 observation 修订已形成独立 `saor_bounded_ready` policy，未静默修改上述失败
对照。每 Job 只预注册已经从 source iterator 到达的具体 request；ready request 上限取该 Job
有效 K，ready work 上限取 endpoint 数×共享 W，仍处于同一冻结总包络。submission trace schema 6
新增 ready/registered/granted 时间与分段延迟，summary 新增 ready count/work 峰值；独立 AutoDL
模板、static audit 和双轮 gate profile 已通过本地测试。release-event schema 2 对 actor 内的
register/grant 同时记录 request ID 与 epoch；runner 用 submission trace 验证 concrete-ready
lifecycle，再在 coordinator 同一时钟域配对 foreground register→grant，并要求区间内 foreign
bulk fallback=0。

同日完成服务器双轮复验。首次 commit `aaa484f4` 在模型请求完成后的证据审计阶段因错误假设
submission trace 含 `submit_epoch_s` 而 fail closed；真实 schema 要求 submission trace 的
ready/registered/granted 与 request trace 的 submit 按 `submission_id` 连接。commit
`6728c569` 修复并新增生产 schema 回归后，从两个全新 root 重跑 8/8 cell、0 incident，跨轮
汇总 `status=passed`、`conclusion=formal_registration_candidate`。$0.125W_e$ 两轮均通过全部门：
12,355/12,367 tok/s、foreground P99 18.15/17.58s、foreground SLO violation 0、bulk SLO
violation 0.658/0.666；$0.25W_e$ 虽保护 foreground，但 bulk miss 0.752/0.744 两轮越过 0.723，
拒绝。当前状态改为 `development-gated/formal-registration-candidate-0125k-only`；正式重复尚未
运行，$0.25W_e$、4-Job、reservation 和 dynamic K 均不扩展。完整结论见
`experiments/results/state_aware_work_unit/saor_bounded_ready_gate_20260813/README.md`。

同日 post-hoc 归因审核增加阻塞门：bounded-ready 同时改变 multiple concrete-ready
pre-registration/execution path 与 priority/debt selector，旧 single-head FIFO/DRR/VTC/SAOR 不能
作为 selector 的干净因果对照；FIFO/DRR/VTC 虽是已有算法，但本实验运行的是 Project
shared-credit coordinator 中的本地实现，只能称项目内标准算法 controls。
另做 1--2 轮**项目内部** matched-observation rehearsal，使
project bounded-ready + FIFO、DRR/WFQ、external VTC-style、strict-priority 与 proposed 共享相同
ready-window、active K/W、ready bytes、arrival/cache/服务合同。接入 bounded-ready 的 FIFO/DRR/
VTC 只是这些项目内标准算法的 matched controls，不替代 no-bounded-ready 版本；Daft、Ray Data、
vLLM 或产品原生 baseline 继续使用各自调度且不接 bounded-ready。
现已把这项证明义务落为独立 Project mechanism 合同：六臂使用位置平衡的 1 warm-up + 3 formal
计划，VTC-style 是主公平参照，FIFO/DRR 同表报告，strict-priority 只作 SLO 边界 control；
headline 是 foreground P99 或 completion service lag 至少改善 5%，保护项是 throughput≥0.95×、
bulk JCT≤1.05×、bulk SLO delta≤0.05、foreground miss≤0.01、longest no-service≤1.05×且≤30s。
proposed 还必须至少一个完整 debt-repayment episode、P95≤30s、unresolved=0；ready demand
终止造成的 right-censored episode 单列，不能计入 repayment P95 或替代完整 episode。首个最终
六臂 rehearsal 的前五臂通过，SAOR 虽有 512 priority、10 recovery grant/completion，却留下
2 个未跨回 cap 的 episode，因而正确 fail closed；这证明原“单 recovery 在途”不能保证
repayment。代码已改为 residual-aware projected-debt work budget：所有 own active work 与
non-preemptible foreign residual 都进入投影，显式 `finish_job` 才能 censor，schema 5 由离线
汇总独立复算并检查单 request quantum overshoot。旧 `d6259f5f` root 因缺少逐请求固定输出上界
门而降为 diagnostic；最终 `63d17300` 全新六臂 root 6/6、0 incident，固定 cap=256 的
6,144-request audit 通过，15/15 repayment completed、P95 3.234s、0 unresolved，1,108/1,108
projection 一致且 estimate/overshoot 门通过。单次相对 VTC-style 的 service lag P95 −13.15%、
longest no-service +0.014%。授权字段逐项绑定、不完整 fairness trace 的 fail-closed 路径和六臂
全组件重汇总已经完成；当前完整签名 direct ceiling 为 13,684.90 tok/s，封存 SAOR 为
12,713.03 tok/s，feeding ratio=92.898%<95%。group/manifest/运行合同/validation/archive SHA 已
绑定，足以执行一次性预注册负判决；PG/Ray clean gate 未结构化落盘，不能声称稳定 7.10% 损失。合同已冻结为
`locked_failed_feeding/formal_authorized=false`；本候选保留为 valid feeding-negative，不启动 1+3
formal，也不改 95% 门、K/W 或冻结 $0.125W_e$。不能用历史
n=2 或单次 rehearsal 直接宣布胜出；授权/报告修正本身不要求重跑六臂 rehearsal。
formal 把 equal-share fairness 与 foreground/bulk differentiated service 分轨，使用
registered-ready backlog、completion-accounted empirical lag、三个 JCT 反事实、request/token
SLO goodput、最长 no-service 和 ready buffer/CPU/memory 指标。

当前执行顺序进一步冻结为“两层证据都要”。六臂 Project 内部 selector attribution 已从两个
独立 rehearsal root 完成：12/12 cell、12,288/12,288 requests、0 incident；旧分析合同曾输出
`validation=passed`，但没有显式 fairness applicability，等待按 bounded=`ok`、static=N/A 的最终
语义重签。固定顺序、每臂 n=2，
故 `selector_victory_decided=false`、历史 artifact 的 `formal_authorized=false` 不变。双轮均值下
DRR/VTC-style 为 12.90K tok/s、foreground P99 27.23/26.16s、30s SLO violation 0；guarded debt
为 12.28K tok/s、foreground P99 17.85s、SLO violation 0。相对 VTC-style，guarded debt 用
约 4.8% 吞吐、5.2% bulk JCT 和 22.7% longest-no-service 代价换取 31.8% foreground P99 与
11.7% completion-lag P95 改善，是观测到的非支配折中点，不是 selector 胜出。完整报告见
`experiments/results/state_aware_work_unit/saor_matched_ready_selector_rehearsal_20260813/`。

本次复核发现两条假通过路径：coordinator 实际记录的 barrier 动作为 `hold_start`，旧统计却
检查不存在的 `hold`，导致 avoidable-idle 零事件门几乎恒真；matched-ready 汇总也没有把
completion fairness 的可用性纳入 cell pass。新合同要求非 concrete head 的 `hold_start` 必须
计为 avoidable idle，且每个进入公平比较的 cell 都必须具备完整 registered-ready completion
ledger。服务器旧完整 artifact 显示五个 bounded-ready 臂每 Job 均有 512/512 lifecycle，
frozen-static 为 0/512；因此旧性能、SLO 与 bounded-ready 臂内的经验 lag 数值保留，但
frozen-static 只能作为 performance/isolation 参照，不能参加同口径 service-lag 排名。

同一修订还要求 runtime `job_id` 非空、单 Job 内一致且并发 Job 间唯一；selector/bridge 静态
readiness 必须冻结每臂 effective K/W 和 `(1,1)` weights。一次在修复前启动的 bounded-priority
服务器 rehearsal 已主动中断并保留为 diagnostic root，未产生 completed run，禁止续跑或进入
结果。修复提交 `15201946` 随后从全新 root 完成四臂回归：四臂均 exactly-once、Job ID 合同
通过，$0.125W_e$ 以 1 次 debt recovery 通过；$0.25W_e$ 再次因 recovery=0 被 runner fail closed，整个
manifest 正确标为 failed。本轮只验证门禁，不并入性能重复。native-system matched 仍因真实
manifest/calibration/env 未冻结而保持锁定；当前
旧 `writeback=none` root 只覆盖 PostgreSQL source→validated gather 的 operator-E2E，原生 request
P99/SLO 为 `unavailable`；新 gateway root 将补齐经验 request tail/service fairness，但仍不提供理论 bound。

下一阶段必须在相同 2-Job manifest、Job 级 `bulk@0s → foreground@5s` 且 Job 内 eager 的共同
原生到达形态、PG source、
模型/vLLM FCFS 服务签名和物理资源包络上，分列 Daft Native、Daft Ray、Ray Data native、
project frozen-static 与 proposed，完成系统级 matched comparison。原生臂保留自身调度，不
注入 Project K/W；Project 两臂冻结相同 K/W。历史原生数据只有完整签名和指标 schema 均匹配
才可复用，否则重跑。已确认历史 JSONL 原生路径在计时前读 manifest，且原生 graph 不忠实暴露
逐请求 timed replay；因此新矩阵必须把 PostgreSQL scan/materialization 放进共同
source→validated-gather 边界，并通过严格透传 gateway 采集真实 request P99/SLO，不能复制
Job/shard completion time。冻结规格见
`../../code_doc/superpowers/specs/2026-08-13-saor-native-system-matched-comparison-design.md`。
为避免把 arrival-regime 变化误归因给 SAOR，同一新合同还包含 1--2 次短的 Project 内部
bounded-ready FIFO/DRR/VTC-style/SAOR sanity block；这些臂仍是 Project controls，不是原生
baseline，且不据此授权 selector formal。

FIFO、DRR、VTC-style 是**Project coordinator 内的标准算法 controls**，不是上游原生实现；
接入 bounded-ready 的副本只用于让 selector 看到相同候选集。single-head/no-bounded-ready
实例用于完整调度包对照；每个结果名必须显式写实现来源和 observation contract。

两轮完整 artifact 已进入仓库，第一轮中间回报不再是证据缺口。当前更准确的停止门是：不立即
启动 selector 1+3 formal，也不事后为这批数据补 non-inferiority margin；先完成原生系统 matched
comparison。`single-head + shared FIFO` bridge 已完成；若完整 Project 系统胜过原生 Daft/Ray，差值也
不能全归因于 guarded-debt selector；只有业务合同明确要求比 30s 更紧的 foreground tail，且
预注册接受约 5% efficiency/bulk-JCT 代价后，才有理由将 SAOR 折中点注册为独立 selector formal。

## 图像状态增量（2026-08-10）

- Daft built-in/Ray Data 四 Job 1+3 矩阵在服务器重启恢复后 40/40 passed，包含 30 个
  formal group、48 条 formal Job，并通过 exactly-once 与真实 overlap 门禁。结果只用于
  各原生执行图内部的 Job slowdown/状态观察，不作跨框架绝对性能排名。
- Project staged descriptor + observe-only 矩阵 24/24 group passed、99K formal rows
  exactly-once。3,114 个正式 snapshot 全部 fresh，构建均值 0.141 ms；static/proposed
  group JCT 差 0.98%。因此当前只证明 production 观测面已接入，不证明 state-aware 控制
  有收益，也不允许把 shared-credit 份额变化归因于 snapshot。
- 权威结果分别归档在 `experiments/results/opening_image_native_fourjob_formal_20260810/`
  与 `experiments/results/opening_image_project_fourjob_observe_only_formal_20260810/`。

## 状态增量（2026-08-04，历史快照；当前执行以其后的开题冻结段与 §0 为准）

本节保留当日完成项和事故上下文，里面的“新下一步”不再构成执行指令；2026-08-09
开题 replacement、原生单 Job 与 guaranteed-overlap 两 Job 已完成后的停止规则优先。

**新完成（不要重跑）**：

- ✅ image 12K 三臂一致性（daft_builtin_embed + ray_data_staged + project_ray，1 warmup + 3 formal，schema-v12，single-writer matrix runner，0 incident；Daft ~64s@12K=187 img/s，fast arm setup-dominated，作结构诊断不进 matched-workload 排名）。
- ✅ image 60K×2 matched-resource schema-v12 重跑（project vs ray_data × cpu8/16，1+3 formal，0 incident）：project 在 matched CPU 两档都快（cpu8 −10.0%、cpu16 −18.5%），**确认 step-8 ~13–15% 结构性收益**；schema-v12 per-image 指标（J/1k、gpu-s/img、img/cpuS、first_output_fraction）产出。结果见 `experiments/results/image_ai_embed_operator_formal_20260803/README.md` §10。
- ✅ 算子代价估计 loader/LOO 审计：旧 283-row hierarchy 把 warmup 混入 formal，已移入
  `operator_cost_estimation_20260726/archive/allphases_pre_20260804/`，不再用于 claim。
  当时的 23-feature formal-only context-LOO 为 204 行/17 contexts；CE5 MAE 7.91s、
  candidate pairwise 0.800、macro/pooled/max regret 4.58%/0.62%/26.23%，row pairwise
  0.684，仍不晋级。该口径已被后续 429-formal/20-context 的 6-rep closure 取代；
  当前权威结论见 §1.5。tie 已冻结为 candidate-ID 字典序，不依赖 CSV 顺序。

**新下一步（两条并行线，共享 workload/观测/raw，执行控制隔离）**：

- **A 线（系统 baseline）**：①vLLM pooling CLIP 只保留为 direct-service ceiling
  候选；当前 vLLM 0.25.1 两次 1-image offline gate 均在 600s 超时且没有 embedding
  结果，状态为 **blocked**，禁止继续在线、5K 或 60K。它不是数据库/框架原生
  baseline，也不能据此声称 vLLM 普遍不支持 CLIP → ②Daft/Ray 官方 ResNet18
  vendor-code parity（commit `3f5bdd17`，GPU 8→2；upstream 使用公开 S3 parquet，
  AutoDL 小样本访问很慢；现有 venv 为 Daft 0.6.2 但 Ray 2.56.1，尚未满足官方
  Ray 2.49.2 合同，故状态仍为 **blocked-before-gate**）→ ③Doris/ClickHouse（**阻塞于 AutoDL 无
  Docker**，需独立 Docker/VM）→ ④system E2E + pgvector sink。
- **B 线（代价估计）**：双 4090 4-cell pilot v2 已 8/8；首次 320-run formal 产生两套
  表面完整结果，但两 runner 几乎全程并发，且 640/640 子运行因空 Ray 地址启动 local
  Ray，故全部判为无效并禁止进入 CE0–CE5。host-scope lease 与空参数门禁修复后，
  cache-on + shared-Ray 最小 gate 已在 `2b7da6c` 上完成 2/2、0 incident、0 local-Ray
  启动；该结果只证明运行合同可执行，不提供性能排名。长实验尚未启动，后续由远端 agent
  在单一新目录重跑 5 workloads × 2 rows × 2 output caps × 4 active-work、每 cell 1+3。
  事故证据与门禁证据分别见对应 results/feasibility 报告。
  2026-08-05 起主合同统一为真实部署的 prefix cache-on；cache-off 仅作独立机制消融，
  `service_prefix_caching` 纳入 context 身份，执行后 hit rate 禁止用作预测特征。
- 上方 §0 "下一步运行 Daft 官方 ResNet18 parity 与 60 秒以上稳态 formal" 中，**60 秒稳态 formal 已由 60K×2 schema-v12 重跑闭合**；ResNet18 parity 仍待（A②）。

## 开题冻结优先级（2026-08-07；2026-08-08 correctness 护栏与 bounded 饱和校准已通过，材料尚未最终冻结）

开题题目与研究内容按 `opening/claim_matrix.md` 冻结。以下两个开题范围的首轮结果因
项目臂 feeding 未过门而只作历史诊断；2026-08-08 已按
`opening_database_e2e_p0_20260807.md` 完成校准、冻结 K128 并整体 replacement：

1. SQuAD short-answer/cap=64 的 direct static-sharded、DuckDB AI static-sharded、
   project frozen-static 三臂统一 database-E2E，1 warmup + 3 formal。
2. 一个冻结 short/medium/long histogram 的 ShareGPT controlled-skew 三臂实验，复用
   同一 source/sink、模型、endpoint、质量、计时和资源合同。

当前 correctness 护栏目录为 `experiments/results/opening_database_e2e_text_refeed_20260808/`：
24/24 单元、18 formal 的 GPU、exactly-once、sink、identity 与稳定性门通过；SQuAD 三静态
路径可核对完成性与答案质量，但 Project 计时额外包含指标采集、记录写入和结束处理，不按不到
1% 的时间/吞吐差异排名。DuckDB AI ShareGPT 的 4,921/6,144 cap 语义失败结论有效。后续同 manifest
bounded C32/C64/C128/C256 扫描证明 C32 只有已测峰值 52.07%，因此旧 ShareGPT
project/C32-direct=1.5457 不作性能排名。正式原生矩阵使用达到峰值 98.22% 的最小点 C128；
校准见 `experiments/results/opening_bounded_saturation_calibration_20260808/`。

仍停止增加不在冻结矩阵内的开题 baseline。现有 scale-ramp 因 request 与 query-barrier timing
granularity 不同，只用于 serving capacity/overload 证据，不替代上述统一 database-E2E。
差异不足 5% 不触发换 workload、模型、数据库或扩大参数扫描。首轮四组核心图、报告与
飞书与旧 PPT 只作为历史版本；当前只整理实验报告与待画图清单。用户已要求暂停新的 PPT 成品、
云文档覆盖和 Wiki。2026-08-08 原生单 job 1+3 已完成（结果见
`opening_text_native_single_job_formal_20260808/`）；2026-08-09 又完成统一 5s offset 的
原生 short/long 两 job 观察和项目 static-partition vs shared-work-credit 同上限 A/B，
结果见 `opening_multijob_interference_20260809/`。三条原生路径都有真实 overlap。项目在线
replay 下 quota-only 近似为零，shared 提高总吞吐并缩短 long JCT，但恶化 short/Jain；
统一 eager 到达后，full→half quota-only 使 short JCT +59.00%，matched
half→static+long 再 +58.77%，而 shared 相对 static 使 short JCT −48.94%、总吞吐
+31.85%、long JCT −25.75%、Jain 0.894→0.972。两种 arrival regime 方向不同，故冻结
结论是“需要显式感知 arrival/active/drain、支持 idle borrowing 并受 SLO/fairness 约束”，
不是“动态全面胜出”。开题前停止新增 offset、weight、4+ job 或框架臂；phase-change、
weighted、图像新动态策略、cost held-out 与完整 image-first A+B 矩阵均留开题后。

上述多 Job 结果含两种不同输入可见性合同：项目 A/B 按 manifest 中
`arrival_time_s` 逐请求 replay；Daft Native/Ray 与 Ray Data 只统一 Job 的 0/5 s
启动，Job 启动后由原生 graph 看到完整 manifest。因此项目 single-short 约 71 s 与
Daft Native 约 11 s 不得解释为框架性能排名。同 manifest 的 Project all-at-t0 1+3
诊断与 eager 多 Job full/half matched control 均已完成：Project 与 Daft Native 对齐到
最早模型提交→最晚响应完成的 T3 分别为 11.354/11.059s，service throughput/MFU 仅差
约2.5%–2.7%；这排除了“项目模型请求路径慢6.4×”，也停止了K256/K512扫描。若论文阶段
需要绝对框架容量排名，才另起所有 rows 在`t=0`可见、统一完整T0、每formal≥60s的矩阵；
该轨不与在线JCT混表，也不阻塞本轮开题。

## 0. 工程优先级（2026-08-01 方向 pivot，开题冻结后恢复）

**方向决定（2026-08-01；本节为该决定的历史记录——锁定 `research/daft_db_gpu_bridge_direction_scope_20260731.md` §8 此前「贡献未锁 / 待确认」状态、并解除 `completed/image_clip_workload_lock_20260731.md` §0 当时的「build 暂停」）**：**A（模型服务状态感知的请求成形/提交）+ B（算子代价估计）一起做，image AI_EMBED (CLIP) 为首个 workload**，换 workload 暂缓。文本 vLLM 轨道（研究内容一 RC1 数据组织 + 研究内容二 RC2 提交控制）已完成 regime-dependent 闭合（见 §1.1 / §1.2），其遗留实验改为 **parked-conditional**（仅在论文收录文本结果时恢复），**不是被废弃**。

**✅ §6 go/no-go 与实现边界复测均已通过（GO）**（2026-08-01）：历史 slow-pt
路径 CPU 准备/GPU embed=**13.8–18.3**；随后在 `f3d17af` 上用 5000 图、四变体、
6 batch sizes、5+30 repeats 交错复测。torchvision tensor-decode 相对
production-np 的配对串行吞吐为 **1.14–1.22×**，但 CPU prepare 仍为 actor 的
**13.8–31.2×**；720/720 rows 完整，embedding cosine=1/max_abs=0。因而 slow
processor 混淆没有推翻异构阶段失衡，但 95% 仍只能写成理论串行非-forward占比。
项目自写 fused Daft Native/Ray 与 bounded project-Ray operator-E2E runner 已实现；fractional-GPU
actor shape 已独立校准，5000 图×3 formal 已完成。项目阶段拆分相对最佳 Daft
Native 单卡吞吐 +29.6%，相对最佳 Daft Ray 双卡 +13.8%，12/12 exactly-once；
但它是同物理机器各自最佳点，不是相同 Ray CPU reservation 的资源效率证明，也不
代表 Daft 官方 native baseline。旧 staged reference arms 已通过 256 行资源账本、
exactly-once、双 GPU 可运行门禁，但只能证明 adapter 能力。当前已新增 Daft 内置
`embed_image`，并从 Ray Data arm 移除项目 `max_active_batches`。Daft built-in 的 256 图
gate 与逐行 parity 已闭合：离线 L2 normalize 后 cosine P1=0.999788、非自身
overlap@10 mean=0.9949，正式比较采用统一 normalized contract 且把归一化计入各臂
E2E。后续正式结果已取代本段的“下一步”措辞：Daft built-in 在12K规模完成1+3/约
185 img/s，但60K×2因其物化结构触发object-store容量门禁；Ray Data native与project在
60K×2、cpu8/16 matched-resource完成1+3，project JCT同向改善10.0%/18.5%。这些只构成
静态阶段拆分的preliminary signal，不证明动态或图像proposed已胜出。
实现边界复测见 `motivation/results/gpu/image_clip_preprocess_variants_20260801/`，
operator-E2E 原始数据和七步报告见
`motivation/results/gpu/image_clip_native_baseline_20260801/`。

**image build 当前状态**：① ✅ 中性 work-unit + lazy image source + typed CLIP tensor actor；
② ✅ Daft built-in、Ray Data native 与 project frozen-static 的 provenance、语义、exactly-once
和 matched-resource 正式证据已完成，权威结果为
`experiments/results/image_ai_embed_operator_formal_20260803/`；Daft 60K 容量失败单列，
不与12K结果横向排名；③ vLLM pooling gate仍blocked，不继续在线/5K/60K；④ HSE static core
已完成 descriptor/lease、真实 ready 与 byte/work 预留接线，但尚无 GPU 对照结果；下一研究动作
是开题后先在冻结最佳静态点上过 direct-dependency vs HSE static 非劣门，再做 **A**
（state-aware请求成形/提交）+ **B**（代价估计），而不是继续增加图像开题baseline、sink或参数扫描。

**开题冻结后的调度主实验边界（2026-08-11 修正）**：上述四臂同机 image formal
先建立官方框架与项目 **best frozen-static** 强基线；项目最终 proposed 不能停在静态点。
随后固定相同资源、相同总 K/active-work 上限和相同 source/sink，比较 no-op/global FIFO、
frozen static partition、简单 shared/fair queue 与 state-aware ordered release：先做活跃 Job
集合变化，再做长短 work mix、1/2/4-job、
staggered overlap、weighted fairness 与异构 mix/offset。动态策略只有改善 observed throughput、
SLO goodput、P99/JCT 或 fairness 至少一项且 correctness/failure 不退化时才晋级；稳态不优是
允许的边界，不用弱静态点制造收益。图像复用同一策略代码，将 token work/credit 换成
frame/preprocess work/credit；Daft built-in、Ray Data native、typed Ray actor ours 必须使用同机、
同模型、同归一化语义和同 PostgreSQL/pgvector E2E 合同。文本已有 equal-workload
1/2/4-job 先验证据，并已完成一个 5s short/long guaranteed-overlap 两 job 因果点；
1-short+3-long 四作业补充也已完成。weighted/SLO、异构 burst 与图像 phase-change 仍未
完成，不能写成已覆盖。图像四作业已完成 2K short + 3×3K long、0.5s offset 的
immutable manifest/config/runner/指标合同；Daft built-in/Ray Data 原生 40/40 runs、30 formal
group 与 Project staged descriptor + observe-only 24/24 group 均已归档，只作系统内干扰和
观测接入证据，不作动态胜出或跨框架排名。DuckDB
bounded-output SQuAD 四作业的 128-row native gate 也已通过，512 outputs 全部 non-empty、
0 error、exactly-once，short/long overlap 2.948s；single controls 和 formal 均未运行。
后续项目感知/动态
实现调整保持图像 manifest、六个 scenario 和 native 配置不变，以 `policy_revision`
版本化并只重跑 project static/proposed；只有资源/模型/语义/计时合同变化才重跑 native。

第一性原理复审后，详细的分阶段 work descriptor、同上限 static/dynamic 消融顺序、
图像 baseline/质量/状态指标与多 job 场景统一见
`state_aware_work_unit_evaluation_20260808.md`。核心调整是：公共 work-unit 不再只把
`token` 改名为 `frame`，而要显式区分 prepare/model/result 等阶段需求；dynamic 只在
离线校准的安全动作集合内根据 fresh state 调整，信号缺失时回退 workload-specific
frozen-static。

**文本轨道遗留的 pivot 后分类**：

| 遗留项 | pivot 后状态 | 理由 |
|---|---|---|
| 代价模型 ranking（Spearman / pairwise / Top-K，§1.5） | 🟢 **保留（服务 B）** | B 的方法论基础；文本侧 283 profile 可直接验证 ranking/regret 方法 |
| §13 K256/W98K 等价性门禁 + disjoint 2048 formal | ⏸ parked-conditional | 文本 baseline-matrix 严谨度；论文收录文本结果时恢复 |
| §10.3 baseline 矩阵第 1–6 步（OceanBase / Daft / Ray Data formal） | ⏸ parked-conditional（image §7 用同结构，可复用） | 同上；image §7 会重建同臂结构 |
| 4-ep/1.5B prefix routing 隔离（endpoint vs model，§1.1） | ⏸ parked | 文本归因诚实性，非 A+B |
| prefix_aware_token_budget 正文实验（§1.1） | ⏸ parked | 文本 RC1 残留 |
| 动态控制信号选择（§10.4 三方式） | ⏸ parked | 文本 RC2；A 在 image 侧重做（观测 CLIP endpoint 队列） |

> **阅读指引**：§1（实验全景）+ §2（证据链）主体是已完成实验的**事实记录，不受 pivot 影响**，保留原样——但 §1.2 / §1.6 结尾的"下一步 / 唯一安全顺序"句是**规范性指令**（已就地标注 parked）。以下章节是 **pivot 前（2026-07-29）的文本轨道**前瞻 / 路线图，**整节已被 §0 取代**（标题处加 ⏸），文本轨道恢复时仍可参考：§4 全节（当前强制顺序 + 候选机制 + P0/P1/P2 路线图）、§6 完整问题审计、§9 剩余关键缺口、§10.3 推荐顺序、§10.4 动态控制信号、§11 RC2 备选方案推进顺序、§13 baseline 门禁。

## 1. 实验全景：已完成 vs 未完成

### 1.1 研究内容一：数据组织策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| 固定行 batch sweep（synthetic prompt） | ✅ 07-18 | 链路跑通 | 不是真实 workload baseline |
| ShareGPT/BurstGPT Ray 静态 batch sweep | ✅ 07-18 | Ray task > Ray actor；batch=16 时 ~260 rows/s | 离线扫表（doc_id 序），不反映在线到达 |
| Token-tail 修订版（batch 1~128, 512 行）| ✅ 07-19 | **固定行 batch 是计算量的弱代理**：正式统一口径为固定 16 行 token min/max=474/6,793（14.3×）；batch=128 时 token P95≈26,677 | — |
| Token-budget vs Fixed Row（timeout=300）| ✅ 07-19 | **Token-budget 能约束 token tail**：6144/8192 吞吐接近 fixed 32/64，token P95 大幅降低 | 4096 吞吐更低（tradeoff）；未证明在所有场景下优于 fixed |
| **Token-budget 1024–32768 容量曲线** | ⏳ 配置完成 | — | 预算甜点、过大预算的 completion barrier/HOL 代价、动态预算动作集 |
| Length-align + Prefix-aware ablation | ✅ 07-19 | length+fixed 是负结果（token P95=33407）；prefix+token6144 吞吐最高（339 rows/s）但 prefix ratio 仅 6.4% | length-align 需配 token-budget；prefix 信号太弱 |
| **Prefix 受控 workload + cache-ON 消融** | ✅ 07-26 / 07-31（2-ep/7B 跨三数据集）；⚠️ 07-31（4-ep/1.5B）；07-31 KV-budget 扫描（2-ep/1.5B，`rc1_prefix_routing/kv_budget_sweep_20260731`） | cache-OFF 0/30/70/100% screen；cache-ON 2-ep/7B batching 中性（within 1.2%）+ routing 跨分散/agent/concentrated 三数据集吞吐 \|Δ\|<2% 不过门禁（agent-trace pala P50 −7.8%/SLO −3.8pp 但吞吐 −1.9%，过饱和区间）；cache-ON 4-ep/1.5B routing **+5.9% 跨门禁**（KV-budget 扫描已排除 per-endpoint KV 为驱动：2-ep/1.5B 全 KV 范围中性、含 13–15% SLO 抖动点；matched-KV ~7GB → 2-ep −0.1% vs 4-ep +5.9%，指向 endpoint 数；agent-trace 为不同 workload 信号） | 1.5B/multiturn 下 endpoint 数（consolidation）是开关、非 per-endpoint KV（KV 扫描证伪 cache-pressure-开关 假设）；agent-trace(2-ep/7B) 是否 cache-pressure 驱动待跨 workload 验证；per-arm 命中率待 runner 增采 |
| **RC1 数据组织系统重测（5 策略 × {2-ep/0.9, 4-ep/0.43}, cache-ON, 1.5B, multiturn）** | ✅ 07-31（`rc1_data_organization/`，**取代 07-25/26 gropy；07-18/19 保留作历史动机参照**） | **regime-dependent**：2-ep（KV max 7–10% 无压力）5 策略 E2E 50–56k 紧凑，fixed≈seq>bestfit>rowcap>lenalign；4-ep（KV max **98–100% 饱和**）分化 39–50k 且**排名反转** seq>fixed>>rowcap≈bestfit>lenalign。**机制闭合（prefix_group_ratio）**：重排序类 organizer（length_align/best_fit/row_cap）打散 prefix 组（ratio 0.03）→ 4-ep 命中从 0.60–0.76 崩到 **0.06–0.07** → prefill 重算激增 → TTFT 翻倍/tail 崩（best_fit/row_cap SLO 60%）；保序类 fixed/sequential 保留局部性（ratio 0.13–0.29，命中 0.47–0.48）。consolidation 是惩罚（4-ep 比 2-ep −10～−26% + 能耗 +40%）。P0 指标（prefix_hit/TTFT）首次采集 | ✅ 喂饱门禁已补（batched bounded，gate 放宽 ≥2 endpoint）：2-ep 真上限 79,488（策略 63–71%、缺口=active-work 准入节流非饿死）；4-ep bounded 24,733 病态（策略超过）→ 准入控制是吞吐杠杆、随 regime 反向；prefix_aware_token_budget 正文实验 ⏸ parked（见 §0；能否回收 4-ep 重排序类命中率）；仅 1 workload + 1 model；MFU 未产出 |

**RC1 当前状态**：✅ 动机成立，策略机制已验证 + **regime-dependent 闭合**（07-31 系统重测：组织策略效应取决于 KV 压力 regime；cache-ON 下保 prefix 局部性是隐性目标，重排序类在 KV 饱和下崩）。⚠️ 但不是"全面胜利"——token-budget 控制 token tail 的代价是更多 HTTP 调用，这个 tradeoff 本身是论文的讨论点。✅ feeding-saturation 门禁已补（batched bounded 2-ep 79,488/4-ep 24,733 病态）：策略确实喂饱但**不榨干 raw 上限**——active-work 准入（W65536）是吞吐 binding 杠杆、效应随 regime 反向（2-ep 压住可放开、4-ep 防 thrash 应保留），这本身是研究内容二的实证信号。

### 1.2 研究内容二：调度与提交控制策略

| 实验 | 状态 | 证明了什么 | 没证明什么 |
|---|---|---|---|
| Arrival-aware K_max sweep（token6144 固定）| ✅ 07-19 | K_max=1→8 吞吐 140→329 rows/s；超 8 无收益 | 单 shape 扫参，已被后续实验替代 |
| Batch Policy × K_max 矩阵 | ✅ 07-19 | K_max 和 batch shape 耦合：fixed128 只有 4 个请求，K_max>4 无调度空间 | 仍是单 job 离线场景 |
| Shared-vLLM K_max 干扰（2-job）| ✅ 07-19 | **K_max 在共享 vLLM 下必要**：bulk unbounded 时 foreground E2E 恶化 2.3×（4.9→11.4s），bulk 自身吞吐几乎不变 | 早期预研；已被 07-26 typed AIMD + 机制 control 复验取代 |
| Shared-vLLM K_max Sweep + Adaptive | ✅ 07-19 | K_max=8 是最佳静态 guardrail；adaptive 触发了 downshift（102 次/run）| **❌ adaptive 不如 static K=8**（foreground E2E 10.2s vs 7.3s）；早期 adaptive 实现已被 07-26 版本取代 |
| AIMD/EWMA-AIMD/PID 单作业 GPU 矩阵 | ✅ 07-26 | 三者相对 static K=8 快约 30–32% E2E，但都把窗口升到 K≈16 | AIMD 与 static K=16 不可分辨；未证明反馈控制增量，也未复验 shared-vLLM 前台保护 |
| Shared-vLLM typed AIMD + adaptive flush | ✅ 07-26 | **static K8 保护前台**（E2E -27.9%、P99 -40.0% vs K16）；AIMD 0 decrease、窗口均值 15.953，与 K16 不可分辨。**根因诊断**：vLLM waiting=0 但前台已慢 38.9%——AIMD 盯着 vLLM waiting 做决策，但请求在 Ray actor 侧排队、waiting 始终为 0 | 只有 128/512 双作业；flush 分支不是完整 2×2 随机化；多 foreground size/arrival offset/>2 job 均未测试 |
| **改进 adaptive flush** | ✅ 07-26 | 自然 EOS 重复、跨 arrival-rate 与 2048 held-out 均完成 | adaptive 未优于 fixed-50；当前默认 fixed 50ms |
| **Request-level continuous replenishment** | ✅ 双卡重复与固定 active-work 对照已完成 | global K32≈per-endpoint K16，确认 K 语义；work-matched request K48≈batch K16；固定 W65K 的 request diagnostic 相对 whole batch 吞吐 +1.75%，credit-held 约降 16% | 未达到 5% 性能条件；保留逐请求完成语义作为多 Job credit/fairness 基础，不作为独立稳态吞吐贡献 |
| **Per-endpoint active-work capacity** | ✅ 07-29 扩展曲线完成 | 双 4090 八档各三次 formal；32/32 成功，65K 达最大吞吐 97.80%，下一档 +0.92% | 按预注册规则选择 65,536；98K→131K 吞吐持平而 P99/SLO 更差 |
| **Short/long static credit existence screen** | ⚠️ 07-30 screening 完成，正式判决阻塞 | 48/48 run 成功；long 的 W65K 信号稳定，K256 在短/长两侧均造成明显 SLO 退化 | 实际为 urllib、无 output token IDs、非 K×work factorial；short 未绑定等价臂分裂 48.5%，均值/中位数选点相反。审计=`inconclusive`，必须先重跑 async 等价臂 gate |
| **SLO-aware EWMA flush** | ✅ 07-29 | 双 4090 high/arrival-limited 各 3 次 formal；相对 fixed-50 吞吐 -0.52%/+0.10%，P99 -0.94%/-0.49%，30s SLO 全部零违约 | 25–50ms 动作相对 5.6–17.4s P99 缺少一阶杠杆；`near_*` 实测为 arrival-limited，不晋升动态策略 |
| **多 job/多 foreground size 扩展** | ✅ 07-29 equal-workload + ✅ 08-09 guaranteed-overlap/eager matched control | equal-workload 36/36、0 incident；4-job 条件性 +9.57% throughput。在线5s中 quota-only≈0、shared提高总吞吐但伤short/Jain；eager中quota-only short JCT +59.00%，shared相对static short JCT −48.94%、总吞吐 +31.85%、Jain 0.894→0.972 | 已证明arrival-regime dependence、idle borrowing与效率—隔离—公平权衡；held-out 4-job、weighted/SLO、异构mix与图像phase-change仍待验证 |

**RC2 当前状态**：✅ static K8 guardrail 与 fixed 50ms coalescing 均有真实
证据。跨 arrival-rate、2048 held-out 和 shared-vLLM 双作业均未显示
queue-adaptive 稳定增量；双 GPU SLO-EWMA 正式矩阵也未过 5% 门槛，因此
当前单 job 默认采用 static K8/已标定 active-work + fixed 50ms。
单作业与 shared-vLLM 复验均表明 AIMD 未优于同上限 static K=16，且根因不
是控制器参数问题——shared-vLLM 实验中 vLLM waiting 始终为 0（请求在 Ray
侧排队），AIMD 看的拥塞信号（vLLM waiting > 0 / KV usage 高）不反映 Ray 侧积压——形成"软拥塞"，即请求在 Ray actor 侧排队但 vLLM waiting 仍显示空闲。
07-30 short/long prompt 静态 credit screening 不能关闭动态路线：远端最初
使用 E2E tokens/s 算术平均得到 short/long 均为 W65K；正式 model-request
中位数却得到 short W98K、long W65K。由于 short W65K/W98K cap 均未绑定却
出现 18%/34% CV，且实验没有使用冻结的 async transport，该迁移信号与
“共同 65K”信号都不具判决资格。（文本轨道恢复后）下一步先运行 version-controlled async
等价臂 gate，只有稳定性通过才重建交错静态面。
不继续在当前稳态 workload 上调 PID 参数；动态控制在负载阶段变化/多租户/
多 GPU 场景下仍是开放问题。

### 1.3 耦合验证

| 实验 | 状态 | 证明了什么 |
|---|---|---|
| **独立最优拼接 vs 联合 grid search** | ✅ 07-26 | 18 单元筛选 + 4 候选重复；联合相对独立 -0.26% ± 2.07%，不可分辨 |

**状态**：本地单 GPU 已完成。当前证据支持分层独立优化，不支持增加联合在线
控制器；多模型、多 GPU 与跨 arrival-rate 的外推仍未验证。

### 1.4 多模态泛化验证

| 实验 | 状态 |
|---|---|
| CLIP embedding (COCO/ImageNet subset) AI_EMBED | ✅ 5K 画像、Daft built-in/Ray Data/Project 静态对照、120K 同资源重复、原生四 Job 40/40 与 Project observe-only 24/24 已完成；static HSE 已接入 runner 并有单测。尚未运行 HSE static GPU 对照、在线 stage/CE5 动作和小规模 pgvector 检索质量验证 |

### 1.5 算子代价估计 & 写回

算子代价估计已完成 formal-only 方法学审计：旧 283-row grouped-holdout 混入 warmup，
只保留为历史；当前权威样本为429条formal、20个decision contexts、每context 4个
active-work candidate。CE5的pooled/macro/max selection regret为
1.67%/2.90%/14.72%，candidate pairwise 0.808，刚过预注册max<15%合同；但只有
0.28个百分点裕量，冻结为`marginal pass`。其row MAE 3.98s还高于Ridge的3.23s，
说明评价重点是配置选择regret，而非逐行误差。定位为共同使能组件，不作为独立研究内容；
尚未在线驱动organization/routing/credit，也未验证跨模态或跨硬件泛化。写回继续使用
PostgreSQL + pgvector工程baseline。

**两个预期用途**：
1. **数据库优化编排**（主要）：为查询优化器提供 AI 算子代价估计，辅助
   选择执行计划和资源分配；
2. **提交策略辅助**（探索性）：作为 vLLM Prometheus 信号的补充，提供
   pending batch 的粗粒度工作量预估（轻/中/重分类），但不替代 Orca 式
   持续供给和 vLLM 反馈驱动的提交机制。

**当前缺口**：
- 双 4090 429-formal/20-context 的候选排序与 regret 已完成，但 max regret 14.72% 距 15% 条件
  仅 0.28 个百分点；仍需独立时间段或新 workload 校准，不能把贴线结果写成稳健泛化；
- 提交策略集成未经验证：代价估计能否将配置可靠地分为"轻/中/重"三档？
  分档后同档内 E2E 方差是否显著小于全局？决定了能否用于提交侧 workload
  分类；
- 无独立 workload/时间留出验证：所有数据来自 07-18 至 07-26，外推退化
  程度未知；
- 点估计无预测区间：编排决策仅靠一个数字，无法评估风险。
- 计划级用途尚无实证：保留 `TPC-H-derived AI operator plan validation` 为
  `planned-conditional`。只有修复后的双 4090 320-run 完全有效，且可部署估计器通过既有
  ranking/regret 门槛，才做 filter/join/materialize 位置与冻结运行配置的最小 held-out；
  不修改当前 320-run，不称官方 TPC-H/TPCx-AI 结果。完整合同见
  `completed/operator_cost_profile_dual4090_formal_20260804.md` §8。

详见 `experiments/results/operator_cost_estimation_20260726/README.md`。

### 1.6 同条件强 baseline 与 project runtime

| 实验 | 状态 | 已有证据 | 当前缺口 |
|---|---|---|---|
| Official/直接客户端 512 行 C128/C256 | ✅ 07-29 | 4/4 cell、512/512 exactly-once、0 incident；vLLM Bench C256 15,351 total tok/s，bounded C256 14,532；C128→C256 仍提升 24.3%/33.0% | C256 是 `max_num_seqs` 配置硬上限，不是已证明的经验平台 |
| Project profiler 同 512 manifest 校准 | 🟡 首次 64 行 gate fail closed，已修复待 re-gate | HTTP 前发现 trace target 276 与 capped manifest work 256 的语义分叉；source hash 一致，排除数据漂移。项目 work/guard 已统一为 `min(trace target, completion cap)`，旧现场保留，512 未启动 | 完整测试与全新 64 行 re-gate 通过后，扫描 K32/64/128/256 与 work16K–98K |
| 2,048 行 disjoint formal | 🟡 数据已就绪，待 manifest/gate | 远端库已含多个 2048 行重建 workload（`sharegpt_multiturn` 在 `doc_id=300000..302047`，另含 `sharegpt_concentrated`/`sharegpt_burstgpt`），disjoint 2048 行不再缺；raw/text/session/Qwen token 已核对 | 选定一个与主实验 disjoint 的 2048 行 workload（如 `doc_id=300000..302047` 之外的重建集）导出只读 manifest，先 64 行 gate，再 1 warm-up + 3 repeats |

这组结果已经推翻“历史 project 约 8.0–8.2K tok/s 是双 4090 物理极限”的
解释。**文本 baseline 轨道恢复时**的安全顺序（当前 parked-conditional，见 §0）：先完成 project 512 校准，再准备 disjoint formal
数据；formal 通过后才恢复多 job 或新策略搜索。不得跨协议、arrival replay
或 workload 直接比较历史数字。

---

## 2. 证据链完整性评估

```
✅ 已证明（可写进论文正文）：
   ├── "固定行 batch 是模型请求代价的弱代理"（token-tail revision）
   ├── "Token-budget batching 能约束 per-request token tail"（token-budget vs fixed）
   └── "共享 vLLM 下无界 inflight 伤害并发小作业延迟"（shared-vLLM interference）

⚠️ 部分证明（有信号但需补实验）：
   ├── "Token-budget 在约束 token tail 同时保持吞吐竞争力"（tradeoff 存在）
   ├── "K_max 作为 admission control guardrail 调节吞吐-延迟 tradeoff"（coupling 已显示）
   └── "Length-align 配合 token-budget 有效"（仅 ablation，无正式对照）

❌ 未证明（关键缺口）：
   ├── "Queue-adaptive flush 优于最佳静态 timeout"（未证明）
   ├── "上游 request-level continuous replenishment 能放大 vLLM continuous batching 收益"（双卡链路已跑通，但 work-matched K48 与 batch K16 吞吐不可分辨）
   ├── "SLO-aware 动态 flush 优于最佳静态窗口"（已实现并重复，但未证明）
   ├── "Prefix-aware 在 cache-off 受控 prefix 比例下有效"（未证明）
   └── "策略代码对多模态 workload 可复用"（未启动）
```

新增的部分证据：

- queue-adaptive 相对 fixed-25 有稳定收益，但 fixed-50 与其不可分辨；
- AIMD/EWMA/PID 相对 static K=8 的收益来自把窗口升至约 16；AIMD 与
  static K=16 不可分辨，尚无动态反馈增量证据；
- 当前单 GPU 下独立拼接与联合候选不可分辨，分层优化足够。

---

## 3. 指标盲区

**历史范围**：本节审计的是 2026-07-18/19 的早期 CSV。后续正式 runner 已补
`tokens/s`、request/resource time series、MFU 与多 Job phase trace；这里的“缺失”
不能解释为当前所有实验仍缺，只用于说明早期结果为什么不能承担更强 claim。

### 3.1 已采集但未充分利用

当前 CSV 中已有但未在分析中充分利用的列：
- `batch_service_s_p99`：仅在 latency probe 中使用，未系统化到每个实验
- `vllm_request_prefill_time_mean_s` / `vllm_request_decode_time_mean_s`：prefill vs decode 占比可用于判断 batch 压力的类型
- `bounded_wait_s`：已在 K_max sweep 中使用，但未与 token P95、service P95 做交叉分析

### 3.2 关键缺失指标

| 缺失指标 | 为什么重要 | 对应实验 |
|---|---|---|
| **`tokens/s`** | 比 `rows/s` 更公平的效率指标——归一化了不同行的计算量差异。token-budget=4096 的 rows/s（301）低于 fixed 32（325），但 tokens/s 可能持平 | 所有实验 |
| **per-request e2e latency 分布** | batch-level P95 掩盖了 batch 内部单个请求的真实延迟。对 length-align/prefix-aware 论证至关重要 | RC1 分组策略实验 |
| **inflight/queue 时间序列** | 当前只有 final gauge。没有时间序列无法诊断 adaptive 为什么不如 static：初始 overshoot 的伤害有多大？downshift 后恢复需要多久？ | RC2 adaptive 实验 |
| **`service_p99`**（系统性采集） | 系统论文审稿人关心 tail。当前仅在 latency probe 中有 batch_service_s_p99 | 所有实验 |
| **`K_max` 时间序列**（adaptive 模式）| 当前只有 `adaptive_upshifts/downshifts` 计数和 `adaptive_limit_mean`，没有每次变化的时间戳和新值 | RC2 adaptive 实验 |

### 3.3 AI_EMBED vs AI_COMPLETE 指标选择差异

早期 AI_EMBED 预研曾把每行近似视为等量，以分阶段 wall time 做比较；后续图像画像已经
证明 encoded size、decode/resize 与 model work 并不恒等，因此正式图像实验也不能把
“一张图片”直接当作可迁移 work 单位。

AI_COMPLETE 的直接证据更明显：固定 16 行 batch 的 token min/max 为 474/6,793
（14.3×），"一行"不再是有意义的比较单位。应该用：
- **计算量归一化指标**：`tokens/s` 替代/补充 `rows/s`
- **分布指标**：token P50/P95/P99、service P50/P95/P99
- **服务端压力指标**：queue time、running/waiting requests
- **控制器行为指标**：K_max 时间序列、upshift/downshift 时间戳

详细分析见 `learning/metric_selection_methodology.md`。

---

## 4. 下一步实验路线图 — ⏸ pivot 前文本轨道（含下方 候选机制 + P0/P1/P2 路线图），整节已被 §0 取代

### 当前强制顺序（2026-07-29）— ⏸ pivot 前文本轨道顺序，已被 §0 取代（保留供文本轨道恢复时参考）

1. 在同一 512 行 immutable Chat manifest 上完成 project static-K 与
   token-work 校准；
2. 用 direct C256 的 15,351 total tok/s 作为当前配置 hard-ceiling 参考，
   比较 project 的 time-to-ceiling、JCT、P99、MFU 与 minimum active work；
3. 独立 2,048 行 held-out 数据已重建：使用 sharegpt_multiturn（doc_id
   300000–302047）/ sharegpt_concentrated 作为 2,048 行源，加载对应只读
   manifest；先 64 行 gate，再 1 warm-up + 3 repeats；
4. 单 job strong baseline 结论冻结后，才进入 1/2/4-job shared-credit/fairness；
5. 旧的 25–50ms adaptive flush 调参不再优先。

### 候选机制优先级（跨论文，2026-07-24）

设计各阶段实验时，"先试哪个机制"见下表。深度（控制律/旋钮/反馈信号）见对应精读笔记与 `research/knowledge_hub.md` §5；fatal flaw 见 `reference/strategy_design_literature_basis.md` §3.1，不在此重复。

| 阶段 | 候选机制 | 来源指针 | 先试? | 隔离实验 |
|---|---|---|---|---|
| RC2（P0-1） | CONCUR 死区非对称 AIMD（无 EWMA，KV 信号，α=2 增/β=0.5 减） | `concur_2025.md`；§5.5 | ⭐⭐ 首选（= 下文 P0 改进方向的文献具象） | CONCUR-AIMD vs 两档 bang-bang vs 静态 K=8，记 K_max 时序 |
| RC2 | Clipper AIMD（加性增 + 10% 乘减） | `clipper_nsdi2017.md`（论文 §4.3.1）；§5.2 | ⭐ 同系族 ablation 对照 | 同上 |
| RC2 | Delayed batching（flush 时机子问题） | `clipper_nsdi2017.md`（论文 §4.3.2）；§5.2 | ⭐ | 扫 flush wait timeout |
| RC2 | DistServe M/D/1 / SABER USL | `distserve_osdi2024.md` / `saber_2025.md`；§5.5 | P2 | USL 拟合 + out-of-sample 残差审计 |
| RC1（P1-2） | Length-align+token-budget / Bin-packing | `bucketserve_2025.md` / `multibin_batching_2024.md`；§5.5 | ⭐ 正式对照未做 | token-only vs +length vs +bin-packing |
| RC1（P1-1） | Prefix-aware（受控 prefix ratio） | `vllm_sosp2023.md`（APC）；§5.1 | ⭐ 受控实验未做 | prefix ratio=0/30/70/100% |

CONCUR-AIMD 首选理由：无 EWMA 契合 `code/AGENTS.md` "保持简单"（Ray `ConcurrencyCapBackpressurePolicy` 因 ~400 行被废弃）、原生用 KV cache 信号（我们有 vLLM Prometheus）、非对称 AIMD 直接对应 P0-1 改进方向。**RC2 P0 前置**先做变长 output 重验（见 §6.1 P0-1 混淆变量 H），排除 `--completion-max-tokens 64` 固定 output 消除自回归不可预测性这个混淆变量，再投入控制器改进。

### P0：修 RC2 核心 claim（最高优先，1-2 周）

#### Arrival replay 单 GPU smoke 门禁（2026-07-25）

正式 flush 对比前先固定 `token_budget=6144`、静态 `K_max=8`，分别运行
`immediate`、`fixed_timeout`、`queue_adaptive`。每个策略执行 1 次 warm-up
和 1 次 smoke，链路必须是 PostgreSQL → Daft → Arrow → Ray task/actor →
真实 vLLM；不得用 fake backend 形成新结论。

只有以下产物均非空时才进入正式重复：

- 主运行 CSV（含 server/pgvector 版本、tokens/s、service p99）；
- per-request/submission 明细；
- flush trace 与 admission/control trace；
- GPU、vLLM queue/running/KV 时间序列；
- 保存完整命令、版本、workload、endpoint 和随机种子的 manifest。

`--source-order arrival_time` 只负责排序；必须同时使用
`--arrival-replay` 才能称为在线 flush 实验。本地 Daft/Ray contract 只证明
执行语义，不是性能证据。

**目标**：让 queue-adaptive flush 在同一 shared-vLLM setup 下超越静态 K_max=8。

**前置（2026-07-24 补充）**：变长 output 重验。当前实验 `--completion-max-tokens 64` 固定 output，消除了自回归"输出长度不可预测"特性（adaptive 的物理前提，见 `service_scheduling_backpressure.md` §0.5）。在改控制器前，先用变长 output（让模型按 EOS 自然早停）重跑 adaptive vs static K_max=8，排除这个混淆变量；保留固定 output 组作对照（隔离 prefill 异质性）。CSV 记录每请求实际 `completion_tokens`。详见 P0-1 的"混淆变量排查"段与假设 H。

**改进方向**：
1. 渐进 ramp-up：从 min=4 开始，每 N 次成功提交无 queue buildup 则 +2
2. 比例控制：不是两档切换，而是 `K_max = max(min, min(max, target × factor))`
3. 每次提交前检查 vLLM metrics，而非批量提交后

**放弃条件**：如果 3 轮改进后 adaptive 仍不能达到静态 K=8 的 90% 性能（foreground E2E ≤ 8s），RC2 降级为"K_max admission control 必要性论证 + queue-adaptive 作为 Discussion 探索方向"。

**同时追加指标**：inflight/queue 时间序列、K_max 时间序列、`tokens/s`。

**2026-07-26 执行结果**：变长输出、完整 control/request/resource trace 和
单作业 GPU 矩阵已完成。AIMD、EWMA-AIMD、PID 都迅速升到 K≈16；追加
static K=16 机制 control 后，AIMD 的 E2E +0.66%、tokens/s -0.69%，差异
不可分辨。shared-vLLM foreground/background 的 static K8/static K16/AIMD
三次重复已完成：AIMD 0 次 decrease、窗口均值 15.953，相对 K16 的前台 E2E
+1.22%、P99 +1.98%、后台真实 tokens/s -1.45%。追加 adaptive flush 后四项
变化仍小于 0.3%；当前收敛为 static K8 + fixed 50ms。

### P0（并列）：两项策略联合消融（1 周）

**目标**：回答"分层独立优化是否足够"。

**设计**：
- best token-budget（当前 6144）+ best K_max（当前 8）独立拼接
- vs token-budget × K_max 联合 grid search
- 保持同一 workload（ShareGPT/BurstGPT, 512 rows, arrival_time 序）

**同时追加指标**：`tokens/s`、`service_p99`。

### P1：Prefix cache 机制确认 + 显式联合消融（2-ep/7B ✅ 完成 07-31；4-ep/1.5B ⚠️ 跨门禁待隔离）

- cache-OFF 受控 prefix 0/30/70/100% + 2048 请求扩展（07-26）；
- cache-ON batching 消融（07-30，`experiments/results/prefix_cache_data_org_20260730/`）：上游 batching 顺序中性（within 1.2%，CV ≤0.5%）；
- cache-ON prefix-affinity routing 消融，2-ep/7B（07-31）：跨三个 workload 吞吐全部中性——分散 ShareGPT（`prefix_cache_routing_req_20260730/`，纯 routing −0.1%、length-align +1.8%，<5% 门禁）、agent-trace（`prefix_routing_agent_20260730/`，\|Δ\|<2%）、concentrated（`prefix_routing_concentrated_20260730/`，\|Δ\|<1.2%）。
- ⚠️ **agent-trace 尾延迟信号**（2-ep/7B，`prefix_routing_agent_20260730/`）：pala P50 −7.8%（64.2 vs 69.6s）、SLO −3.8pp（78% vs 82%）、goodput +17%，但吞吐 −1.9%（过饱和区间 SLO 违约 78–83%、未过门禁）；concentrated cache 压力低、同信号弱。
- cache-ON prefix-affinity routing 消融，4-ep/1.5B（07-31，`experiments/results/prefix_cache_routing_4ep_1.5b_20260731/`）：prefix_affinity 相对 least_queued **+5.9%**（46,943 vs 44,317 tok/s，raw 不重叠、CV≤0.9%）、SLO −6.3pp、P95 −3.15s，**跨过 5% 门禁**。
- **结论（分层）**：低淘汰压力 regime（2-ep/7B，APC 覆盖 working set）prefix 方向收口，vLLM APC 覆盖上游 prefix 组织/路由优化；高淘汰压力 regime（4-ep/1.5B APC 不够覆盖，或 2-ep/7B agent-trace 高 cache 压力 workload）prefix_affinity / pala 重新显现收益（+5.9% 吞吐或 P50 −7.8%）。**cache 淘汰压力是信号是否显现的开关**，现由两个独立高压数据点支持（4-ep/1.5B 改 endpoint/model；agent-trace 只改 workload）。⚠️ 4-ep/1.5B 同时改了 model/endpoint 数/KV 大小（混淆）且 SLO 违约 25–31% 处于过饱和 regime——跨门禁但需隔离消融后正式晋级。
- 残留：per-arm prefix cache 命中率未记录（resources.csv 只采样 KV 用量，待 runner 增采）；4-ep/1.5B 的 +5.9% 需 4-ep/7B 或 2-ep/1.5B 隔离 endpoint 数 vs model size；agent pala 的 P50 改善需人为缩 KV 制造可控淘汰率单调验证；低 prefix 重复率 workload 泛化未测。高淘汰压力 regime 的 KV 淘汰/重算瓶颈信号（4-ep 25–31% SLO 违约 + affinity 收益、2-ep/7B agent-trace P50 改善）为 Mooncake/共享 KV cache 方向提供动机数据点。

### P2：多模态泛化（⚠️ 触发条件已被 §0 pivot 取代——image 多模态现为首要 workload，不再等文本 P0/P1）

**目标**：验证策略代码的模态无关性。

**设计**：
- CLIP embedding + ImageNet/HF subset
- 同一套 `organizers.py` + `model_backends.py` 代码
- 验证 frame-budget ↔ token-budget 类比、queue-adaptive flush ↔ 完全复用

---

## 5. 审稿人视角：如果现在投稿会被拒在哪里

基于 idea-evaluator + ars-reviewer 模拟审稿的共识：

| 审稿人 concern | 严重度 | 修复路径 |
|---|---|---|
| Adaptive < static 是负面结果 | **MAJOR** | 改进控制器或重构 claim |
| 两项策略缺乏联合分析 | **MAJOR** | P0 联合消融实验 |
| 实验规模仅 512 行、单 GPU | Concern | P1 规模扩展至 2048 行 |
| Token-budget 方法 novelty 薄（贪心算法）| Concern | 诚实 framing：贡献是"表征优化空间"而非"发明新算法" |
| 无写回、单 endpoint | Minor（已声明）| Discussion 中讨论边界 |

---

## 6. 完整问题审计（2026-07-23）— ⏸ 文本轨道问题清单（parked-conditional，见 §0；仅当论文收录文本结果时为生效优先级）

以下审计覆盖所有已知问题（不含"ML as Native Operator"叙事定位问题，该问题已在 2026-07-23 对话中单独讨论，结论为搁置至后续阶段）。问题按 P0/P1/P2 分级。

### 6.1 P0 阻塞级：不解决无法写论文

#### P0-1：RC2 核心策略——动态控制未证明优于最佳静态（已从"负结果"推进为"信号选择诊断"）

**07-19 初始发现**：Shared-vLLM interference 实验：adaptive tuned 的 foreground E2E=10.2s，静态 K_max=8 的 foreground E2E=7.3s。Adaptive 触发了 102 次 downshift，控制器在运作，但效果比简单静态 guardrail 差 ~40%。

**07-26 复验（typed AIMD + 机制 control）**：

- **单作业**：AIMD/EWMA-AIMD/PID 全部迅速升到 K≈16；加入 static K=16 机制对照后，AIMD 的 E2E +0.66%、tokens/s -0.69%，差异不可分辨。三者相对 static K=8 的 ~30% E2E 改善来自"更高并发"而非"动态反馈"。
- **Shared-vLLM 前台/后台（128/512）**：static K=8 将前台 E2E 降低 27.9%、P99 降低 40.0%（相对 K=16），确认 guardrail 价值。但 AIMD 三轮 **0 次 decrease**、774 次决策仅 12 次 increase，窗口从 8 快速升至 16 后不变（均值 15.953）。相对 K=16 前台 E2E +1.22%、P99 +1.98%、后台 tokens/s -1.45%。Adaptive flush 追加后四项变化 <0.3%。
- **根因诊断**：AIMD 的拥塞信号（vLLM waiting > 0 / KV usage 高）在 shared-vLLM 场景下完全看不到（vLLM waiting=0 但请求已在 Ray 侧积压）——前台延迟已恶化 38.9%，但 vLLM `waiting` 始终为 0（请求在 Ray 侧排队，尚未进入 vLLM waiting 队列）。控制器观测不到"软拥塞"，自然不会降载。这不是控制器参数问题，是**观测信号的表达能力不足**。

**影响**：研究内容二可以 claim "K_max admission control 在共享 vLLM 下是必要的 guardrail"（✅ 强证据），但不能 claim "自适应提交控制是有效的"。当前默认使用 static K=8 + fixed 50ms。

**演进判断**：变长 output（自然 EOS 上限 512）已纳入 07-26 实验，排除了固定 output 混淆变量。不再继续在稳态 workload 上调 AIMD/EWMA/PID 参数。若继续动态控制方向，必须改用反映 Ray 侧积压的信号——候选路径包括逐请求完成时间（request-level replenishment 的副产品）或端到端 SLO slack 作为反馈信号。

**放弃条件**（已触发，但结论从"负结果"变为"边界条件"）：动态控制器在稳态 workload 的三种独立实现均未优于最佳静态上限。论文可诚实 framing 为"在稳态 workload 下简单静态 guardrail 足够；负载阶段变化/多租户/多 GPU 下的动态控制仍是开放问题"，而非回避或强行包装为正面结果。

#### P0-2：两项策略联合消融完全没有数据

**事实**：`PROJECT_OUTLINE.md`“研究问题与因果设计”规定的核心验证是“分别独立搜索配置后拼接，
再与联合 grid search 对比”。本段记录的是该日期下的历史状态：batch_policy × K_max matrix 实验
（07-19）已显示两者存在耦合，但当时独立拼接与联合搜索尚未运行；后续完成状态以上方当前汇总为准。

**需要回答**：token-budget 最优值（当前 6144）+ K_max 最优值（当前 8）独立拼接，是否与 joint space 中搜索的 (token_budget*, K_max*) 一致？
- 一致 → 分层独立优化即可，论文可分开写两项策略
- 不一致 → 必须联合优化，论文只有一个贡献（joint scheduling）

**设计**：token_budget ∈ {4096, 6144, 8192} × K_max ∈ {4, 8, 16}，共 9 点 grid。同一 workload（ShareGPT/BurstGPT, 512 rows, arrival_time 序）。

**同时追加指标**：`tokens/s`、`service_p99`。

#### P0-3：关键指标 `tokens/s` 缺失，`rows/s` 在 AI_COMPLETE 场景下是有偏指标

**2026-07-25 状态更新：本轮新增实验已修复。** profiler 现在直接输出
`tokens_per_s`；加速到达 flush 实验对已经完成的 15 条正式运行使用 vLLM
Prometheus 的实际 `prompt_tokens_delta + generation_tokens_delta` 事后无损补算，
结果见 `experiments/results/accelerated_arrival_flush_20260725/`。旧实验仍需在
跨 workload 比较前补算，不能把本项标记为全历史数据已修复。

**事实**：历史实验以 `rows/s` 作为主吞吐指标，但同一 workload 中固定 16 行 batch 的 token min/max 为 474/6,793（14.3×）。Token-budget=4096 的 rows/s（301）低于 fixed 32（325）；历史记录没有足够证据据此断言其 `tokens/s` 一定持平或更高，因此只保留“rows 不是稳定 work 代理”的结论。

**影响**：无法公平比较不同策略的效率。token-budget 策略的核心 tradeoff（更多小请求 vs 更少大请求）在 `rows/s` 指标下被扭曲。

**同样缺失**：
- `service_p99`：系统性 tail latency 采集（当前仅 P95）
- inflight/queue 时间序列：只有终值 gauge，无法诊断 adaptive 行为
- per-request e2e latency 分布：对 length-align/prefix-aware 分组策略论证至关重要

**量化方法**：`tokens/s = SUM(prompt_tokens + completion_tokens) / operator_wall_s`。对于使用同一 tokenizer 的 workload，`prompt_tokens` 列已存在；可累计每行的 `prompt_tokens + completion_max_tokens` 作为计算量 proxy。

#### 2026-07-25 加速到达 flush 策略筛选结果

**事实（真实单 GPU E2E）**：在 ShareGPT/BurstGPT 前 512 条、
arrival scale `0.0005`、token budget 6144、静态 `K_max=8` 下，每策略 1 次
预热 + 5 次正式重复。fixed timeout 相对 immediate 将 submission 减少
8.984%，但 tokens/s 仅提高 0.185%，置信区间重叠。当前 queue-adaptive
平均 batch rows 为 1.0，tokens/s 低 0.966%，没有形成有效 coalescing。

**设计判定**：当前 queue-adaptive 版本不进入联合搜索。下一轮必须先在 64 条
真实门禁中同时满足 exactly-once、平均 batch rows > 1 和 service P99
guardrail，再运行 512 条矩阵。完整证据、故障恢复记录和 claim boundary 见
`experiments/results/accelerated_arrival_flush_20260725/README.md`。

#### 2026-07-25 双窗口 adaptive flush 改进结果

**事实（真实单 GPU E2E）**：修正低负载 fallback、`K_max` 压力阈值和
event-time catch-up 后，64 行与 1024 行门禁均通过。512 行、每策略 1 次预热 +
5 次正式重复中，adaptive 相对新版 fixed timeout 的 observed tokens/s 提升
3.671%，submissions 减少 23.500%，平均 batch rows 提升 30.732%，batch
service P99 均值降低 8.010%。每轮 512 个文档 exactly-once。

**设计判定**：queue-adaptive flush 已从“不能形成 batch”推进为正向候选策略，
可以进入随机化复验和 batching × submission 联合搜索候选池。仍不能标记为最终
验证完成：当前策略按组运行而非逐 repeat 随机化，生成上限固定为 16 tokens，
尚缺 per-request E2E P99、变长输出和 2048 行 held-out。完整结果见
`experiments/results/adaptive_flush_window_20260725/README.md`。

### 6.2 P1 严重级：需补实验，但不会动摇论文根基

#### P1-1：Prefix-aware 在自然 workload 上信号太弱（6.4%），未做受控实验

**事实**（07-19 ablation）：prefix ratio 从 4.1%（random）提升到 6.4%（prefix-aware），不足以支撑 prefix-aware 有效性论证。

**需要**：构造 prefix ratio = 0/30/70/100% 的受控 workload，仅在 prefix+token6144 条件下评估。需采集 vLLM APC/cache metrics（如果 vLLM 暴露）。

**诚实考量**：如果自然 workload（ShareGPT/BurstGPT）只有 4-6% prefix share，prefix-aware 在实际场景中的收益也许天然有限——这本身是一个有价值的发现，需诚实面对。

#### P1-2：Length-align + fixed rows 是负结果，正确组合（length-align + token-budget）未做正式对照

**事实**（07-19 ablation）：length + fixed 32 导致 token P95=33407（因为长文本被集中到同一 fixed-row batch）。`length + token 6144` 的 token P95=6126，效果好，但它是 ablation 的一部分而非正式对照实验。

**需要**：正式对比 token-budget-only vs token-budget+length-align vs token-budget+bin-packing，在同一 workload 和 metric 下。

#### P1-3：所有实验 512 行规模，无 scale-out 验证

**事实**：所有 07-18/19 实验均为 `total_rows=512`。2048 行扩展在计划中但未执行。

**风险**：512 行下 K_max=8 饱和；2048 行下最优 K_max 可能是 16 或 32。当前的"最优"参数组合可能只是小规模 artifact。

**需要**：至少一个实验（最优 token-budget + 最优 K_max）scale 到 2048 行。

#### P1-4：Token-budget 的 tradeoff 未系统表征

**事实**：Token-budget=4096 约束 token P95 至 4092，但 model calls 从 4（fixed128）增至 19。Tradeoff 存在但未被定量分析。

**需要**：系统表征"token tail 每降低 X%，HTTP 调用增加 Y%"的关系曲线。这本身是论文的有效讨论点——"token-budget 不是免费午餐，但在 token tail 敏感的 scenario 下是合理的 tradeoff"。

### 6.3 P2 方法论/设计问题

#### P2-1：Daft 引擎级参数实验空间完全未探索

**事实**：优化空间定义为"策略级决策 + 引擎级参数"，但当前实验仅覆盖策略级。Daft 的 `into_batches`、`repartition`、`@daft.cls batch_size`、`max_concurrency` 等引擎级参数无系统实验数据。

**选择**：要么砍掉"引擎级参数系统表征" claim（诚实说明"本文聚焦策略级决策，引擎级参数使用推荐值"），要么花 1 周跑参数 sweep。

#### P2-2：单 job 离线扫表 vs arrival-aware 之间的叙事断层

**事实**：早期实验（token-tail revision、token-budget vs fixed）用 `--source-order doc_id`（离线扫表模式），后期 K_max 实验才切换到 `--source-order arrival_time`。论文不能从离线扫表实验直接跳到"arrival-aware scheduling 需要 K_max"的结论。

**缓解**：在论文中明确区分两种实验模式的角色——离线扫表回答"数据组织"，arrival-aware 回答"提交控制"。或对关键实验用两种 source_order 各跑一遍。

#### P2-3：Baseline 矩阵大量未实际运行

**事实**：`baseline_reference.md` 定义 G1-G6、W1-W7、D1-D4、X1-X3 共 20 个 baseline，实际跑过的 <5 个。不影响核心贡献，但审稿人可能问"为什么不和 X baseline 比"。

**缓解**：投稿前清理 baseline 文档——实际跑过的标 ✅，计划但未跑的标"不在本文 scope 内"，避免给审稿人留下"承诺了但没做"的印象。

#### P2-4：无多 endpoint / 多 GPU 实验

**事实**（2026-07-23 审计快照）：当时 AI_COMPLETE 实验均为单 RTX 5070 + 单 vLLM 实例。**已修复（2026-07-31）**：AI_COMPLETE 已在 AutoDL 双 4090 上完成多 endpoint 实验——2-endpoint/Qwen2.5-7B 与 4-endpoint/Qwen2.5-1.5B prefix-affinity routing（见 §1.2 "Prefix 受控 workload + cache-ON 消融" 行）。"单 RTX 5070 / 无多 endpoint" 缺口已闭合；4-ep/1.5B 的 prefix 收益跨 5% 门禁但仍混淆待隔离，分池路由场景已具备前置条件。

#### P2-5：跨查询 batching 是隐含效果而非显式策略

**事实**：vLLM 内部做 continuous batching（请求自动合并），但 Ray 层没有显式的"跨查询请求融合"机制。当前 Shared-vLLM K_max Interference 实验是两 job 共享同一 endpoint（跨查询共享服务），不是跨查询主动合并请求。

**多模态场景下的重要性提升（2026-07-23 更新）**：在纯文本场景下 vLLM 的 continuous batching 掩盖了"没有跨查询请求池"这个问题——所有 AI_COMPLETE 请求都走同一个 vLLM endpoint，vLLM 内部自动合并。但在多模态场景下：
- AI_COMPLETE → vLLM（Qwen2.5-1.5B）
- AI_EMBED → CLIP backend（vLLM pooling / Triton / Infinity 等成熟服务均可能有
  服务端 batching；是否开启必须作为 manifest 因子）
- AI_CLASSIFY → Qwen2.5-VL endpoint

因此不能再以“CLIP 天然没有 continuous batching”作为跨查询请求池的立题依据。
只有在**上游 owns batching**（例如 tensor-input Ray GPU actor 不再二次攒批）
或成熟服务的 batching 在异质多 job 下被实验证明不足时，显式跨查询池才是可验证方法；
否则它只是与服务端重复实现。

**论文影响**：如果 claim "跨查询 continuous batching"作为方法贡献，需要在 Ray 层实现显式的全局请求池 + 算子类型感知路由（同类合并、异类分池）。纯文本场景下这个贡献被 vLLM 内部机制掩盖，多模态场景才是它真正体现价值的地方。

**与 RC2 的关系**：如果 adaptive 控制器在 P0 阶段降级，跨查询合并 + 算子类型感知路由可以作为 RC2 的方法补充贡献，不依赖 adaptive 控制器的性能。

### 6.4 认知债务：文档承诺 vs 实际交付

| 文档中的承诺 | 实际状态 |
|---|---|
| baseline_reference.md：G1-G6 + W1-W7 + D1-D4 + X1-X3（20 个 baseline）| 实际跑过 <5 个 |
| knowledge_hub.md §10.5.1：优化空间三层框架，"引擎级参数系统表征" | 引擎级参数实验为 0 |
| knowledge_hub.md §7.2：实验五阶段（前置→一→二→三→四）| 阶段三（耦合验证）未做、阶段四（写回）降级 |
| PROJECT_OUTLINE.md：actor pool 分池路由、异构 actor pool | 无多 endpoint 实验，分池路由无场景 |

**行动**：投稿前必须清理——要么补齐关键 baseline，要么诚实标注"不在本文 scope"。

---

## 7. 更新检查清单

当本文件中的缺口被新的实验结果填补时，同步更新：
- `experiments/results/local_vllm_qwen15b_baseline/README.md`
- `PROJECT_OUTLINE.md` §当前最重要证据、§近期优先级
- `PROJECT_LOG.md`
- `figures/README.md`（如有新增图）
- `learning/local_vllm_ray_baseline_walkthrough.md`（如实验结果影响讲解）
- 本文件 §6 完整问题审计（标记已修复的问题）

## 8. 2026-07-25 Request lifecycle 基础设施门禁

**已补齐的观测缺口**：

- 真实 64-prompt `PostgreSQL -> Daft -> Arrow -> Ray task -> vLLM` 路径已输出
  client-observed request E2E P50/P95/P99、SLO violation/goodput、request 与
  submission 显式外键；
- seeded runner 已验证固定 seed 顺序、运行前空闲门禁、失败即停、incident
  审计、凭据脱敏和原子 manifest；
- request、submission、flush、resource 和 run CSV 均带 PostgreSQL/pgvector
  版本，最终 exactly-once 与分位数重算通过。

**边界**：该门禁只有 fixed/adaptive 各一次，且规模为 64，不替代多轮正式对比；
1 秒 SLO 是 instrumentation 阈值而非业务 SLO；submission endpoint 内的 prompt
共享 completion timestamp，仍不是 vLLM 内部逐 sequence 完成时间。

原始数据和七步解释见
`experiments/results/request_lifecycle_gate_20260725/README.md`。

### 2026-08-04 下一轮观测 schema

文本 profiler 已增加 TTFT/ITL P50/P95/P99、prefix-cache hit、SLO input/output/total
token goodput、显式单价成本、padding waste、P99 SLO scale 和调度控制开销；
shared-vLLM 增加实际 work、SLO token goodput 与活跃重叠期间累计服务差；代价模型
增加 Q-error/Spearman/pick-rate/selected-runtime/regret；AI_EMBED 增加基于显式相关
真值的 Recall@K/MRR/nDCG 离线门禁。formal repeat 后处理统一输出 sample std、CV、
Student-t 95% CI 和配对回退次数。代码单测通过不等于真实 vLLM histogram 已可用；
下一次远端 gate 必须验证各 status 为 `ok`，并使用新结果目录，禁止追加旧 schema。

## 9. 2026-07-26 提交控制与联合实验闭环

### 已补齐

- vLLM 每个 choice 的真实 output-token count 与 finish reason；
- ChatML 自然 EOS 门禁，以及不截断 prompt 的 context-safe 数据源过滤；
- 512 请求 fixed-25 vs queue-adaptive 随机化 n=5；
- token budget `{4096,6144,8192}` × K_max `{4,8,16}` ×
  fixed/adaptive 的 18 单元 SLO-constrained 筛选；
- 独立拼接、联合候选、fixed-25 baseline、fixed-50 机制对照各 n=3；
- **单作业 AIMD/EWMA-AIMD/PID 矩阵**：三者基于不同控制律，均在
  workload backlog 下迅速升到 K≈16；加入同上限 static K=16 对照后不可分辨；
- **Shared-vLLM typed AIMD + adaptive flush（128 前台 / 512 后台）**：
  static K=8 保护前台（E2E -27.9%、P99 -40.0%），AIMD 0 decrease、
  窗口均值 15.953，与 K=16 不可分辨；adaptive flush 约 89.4% 选 50ms，
  行为接近 fixed-50。

### 当前结论

- K16 吞吐最高，但所有单元均超过 1% SLO violation 门槛；
- 独立拼接相对 fixed-25 tokens/s `+4.76% ± 2.29%`；
- 联合候选相对独立拼接 `-0.26% ± 2.07%`，没有可分辨增量；
- 相同 8192/K8 下 adaptive 相对 fixed-50 `-0.75% ± 0.97%`；
- 当前默认应保持 sequential token-budget + static K8，并在本 workload 使用
  简单 fixed-50；adaptive 只保留为跨 arrival-rate 候选；
- **Shared-vLLM**：K_max guardrail 价值已在 07-19 初步证明、07-26 typed AIMD
  复验证实；但 AIMD 无法观测 Ray 侧软拥塞（vLLM waiting=0），动态控制
  相对最佳静态无增量；当前共享服务默认继续使用 static K=8 + fixed 50ms。

### 剩余关键缺口 — ⏸ 文本轨道待办（parked-conditional，见 §0；item 6 多模态已升为 §0 首要 workload、item 7 动态控制信号 = §0 parked）

1. 用相同 per-GPU K 完成单/双 endpoint 容量曲线，替代历史 global K 同值
   的不公平对照；
2. 07-29 八档 request-level active-work 扩展曲线已完成，按预注册规则选择
   65,536；
3. 已在该饱和点固定每 endpoint 256 actor slots，完成
   1×256/2×128/4×64 三次重复。16-slot 草案按当前 332 work/request 与
   1337 work/organization-batch 估算会严重欠载，已在启动前否决；三个
   arm 的每 endpoint Ray CPU reservation 同时固定为 0.5。2×128/4×64
   相对 1×256 仅 +2.00%/+0.75%，未过 5% 门槛，保留 1×256；
4. 已固定 1×256 pool、planning budget 和 active work，完成 whole batch、
   complete-row service quantum 512/1024/2048/4096 与 request diagnostic。
   512/request 将 credit-held 降约 16%，但吞吐相对 batch 最高仅 +1.75%，
   固定 quantum 不晋升；8192 因会退化为 batch control 未运行；
5. SLO-aware EWMA flush 已完成正式对照且未晋升；不在同一 25–50ms 动作
   空间继续调 alpha/deadband；
6. 多模态复用，以及 shared-vLLM 4-job held-out、
   workload mix、更多 arrival offset 和 weighted/SLO overlap fairness；5s short/long
   guaranteed-overlap 已闭合最小两作业权衡，不再扩扫追正（prefix cache-on 2-ep/7B 已完成：batching + routing 均中性、
   prefix 方向在该 regime 收口；4-ep/1.5B routing +5.9% 跨门禁、高淘汰压力 regime
   有条件重新打开，待 4-ep/7B 或 2-ep/1.5B 隔离消融）；
7. 动态控制的信号选择问题——逐请求完成时间或端到端 SLO slack 可能替代
   当前 vLLM waiting 信号（不反映 Ray 侧积压），但尚未验证。

原始数据与七步解释见：

- `experiments/results/adaptive_flush_randomized_20260726/README.md`
- `experiments/results/joint_batching_submission_512_20260726/README.md`
- `experiments/results/shared_vllm_adaptive_admission_20260726/README.md`
- `experiments/results/adaptive_admission_controller_20260726/README.md`
- `experiments/results/dual_gpu_shared_vllm_formal_20260729_1135/README.md`

## 10. 2026-07-26 文献驱动执行链缺口重审

### 10.1 关键边界校正

- Orca/vLLM 的 iteration-level/continuous batching 位于模型服务内部：完成请求
  会被移出执行集合，waiting 请求可在后续迭代中进入；
- 当前 Daft/Ray 上游没有修改 vLLM，但 submission 仍可能形成整批完成屏障；
- 因此“vLLM 已有 continuous batching”和“上游已能逐请求持续补位”不是同一件事；
- 当前两档 `QueueAdaptiveFlush` 已完成代码和实验，但只是 baseline，不能标成
  Clipper/Clockwork/CONCUR 等文献机制的完整落地。

### 10.2 新增代码缺口

| 缺口 | 当前状态 | 最小闭环 |
|---|---|---|
| Request-level completion/credit release | 已实现并完成双 4090 重复 | 固定 active work 后继续验证 exactly-once 与逐请求释放 |
| Continuous replenishment | 上游已实现；K-count 双卡对照未隔离独立收益 | 固定 active work 的 whole-submission vs request-credit 对照 |
| Token-work admission | 已实现；八档扩展曲线完成并选定 65,536 | 后续策略固定该 work，不再靠增加 offered load 获得表面收益 |
| Complete-row service quantum | gate 与 24-run 正式重复完成 | credit-held 降约 16% 但吞吐增益不足 5%；不晋升固定 quantum，request 保留作精确控制基础 |
| Bounded Ray actor pool | 固定 slots、worker routing 与失败清理已实现并完成正式重复 | 多 actor 未过 5% 晋升门槛；当前保留 1×256，多 job 分池另行验证 |
| SLO-aware adaptive flush | 24-run 正式重复完成 | oldest slack、arrival/service EWMA、容量下界与 deadband 均已接入；相对 fixed-50 未过 5% 门槛，不晋升 |
| Completion-span/HOL 观测 | 有 request/submission join key | 记录同 submission 首末完成跨度和 credit idle |
| Endpoint-local controller | topology/接口具备 | 两个真实 endpoint 后验证独立状态与回退 |

**2026-07-27 shared-vLLM 信号选择诊断补充**：shared-vLLM 实验中 AIMD 0 次
decrease 的根因不是控制器参数问题，而是 vLLM Prometheus `waiting` 始终为 0
（请求在 Ray 侧排队，尚未进入 vLLM waiting 队列），当前观测信号无法识别
"软拥塞"。Completion-span/HOL 观测和 request-level replenishment 的副产品——
逐请求完成时间——可作为反映 Ray 侧积压的信号，使动态控制真正有价值。这
将 request-level replenishment 的优先级从"工程改进"提升为"可能解锁动态控制
价值的必要前置”（当前 parked-conditional，见 §0；仅动态控制方向恢复时生效）。

### 10.3 推荐顺序与成功标准 — ⏸ pivot 前文本轨道（2026-07-29），已被 §0 取代

1. 16K–131K active-work、Actor Pool、service quantum、SLO-aware EWMA 和
   Shared-vLLM equal-workload 矩阵均已完成；在继续增加策略前，先补同规模
   同条件强 baseline；
2. 第一层统一 Chat Completions，比较 direct-vLLM ceiling、OceanBase
   `AI_COMPLETE`、同 PostgreSQL bounded AsyncIO、Daft+Ray static 和当前
   token-work/request-refill；
3. 第二层比较 Daft `prompt()` Native/Ray、Ray Data HTTP Processor 与 ours，
   以排除收益只是官方框架能力；
4. 各 arm 独立 calibration，在冻结参数后运行 32/64/128/256 瞬态与 2,048
   held-out；报告 time-to-ceiling、ramp regret 和 minimum saturating work；
5. baseline 锁定后再进行 4-job held-out 与 staggered/weighted 机制隔离；
6. Prefix cache、length-align、多模态和 UCB 继续保持后续优先级；UCB 必须等
   epoch reward 能按产生请求的 arm 正确归因后再接入。

**2026-07-29 实施状态**：`service_scheduling_backpressure.md` §13 的
1/2/4-job 三臂 gate 与 formal 均已完成。36/36 group run、0 incident；
shared credit 精确峰值未越过 256 request/65,536 work，最终归零。
2-job 无 5% 增量；4-job shared 相对 independent 聚合吞吐 +9.57%、
max P99 -22.52%，但三次吞吐变化为 +8.43%/-0.28%/+22.60%。因此只晋升为
高竞争条件性候选，需 held-out 复验；staggered/weighted 仍未验证。

**2026-07-29 baseline 门禁状态**：首轮 64 行双 GPU core gate 已让
vLLM Bench、bounded HTTP、Daft Native/Ray 和 Ray Data HTTP 全部通过
64/64 exactly-once、0 incident、双 endpoint 与空队列门禁。但这只建立功能
证据。等价性审计发现的 vLLM Bench 双重 chat template 与 Ray Data `1..n`
autoscaling 已修复，提交 `f2e82bd` 的全新 re-gate 再次 5/5 通过。Daft/Ray
Data 的 shard-barrier 观测仍不能与 request-level P95 横比；而各 adapter 的
client token 口径也不一致。当前在每个 cell 前后增加 endpoint-local vLLM
prompt/generation cumulative counter 差分门禁；该真实双 GPU gate 通过前，
推荐顺序中的第 4 步 calibration 继续阻塞。

服务端 counter 门禁与 256 行 scale gate 随后通过。direct-vLLM/bounded 在
C32 均约 4.93K total tokens/s，在 C64 均约 8.34K；vLLM Bench C128 的真实
peak concurrency=128，达到 12.76K，较 C64 再增 53%。bounded C128 被
httpx 0.28.1 默认 100 总连接/20 keepalive 截断，因此该点作废，客户端 pool
已用回归测试显式绑定配置并发。全新 bounded-only C128 re-gate 观测到
running=124/125，达到 12.47K total tokens/s、JCT 8.048s，与 vLLM Bench
C128 只差约 2.3%，没有剩余明显协议分叉。

该结果推翻“历史约 8K 是双 4090/vLLM 物理 ceiling”的解释：8K 仅是当时
project profiler + arrival replay + 旧请求语义下的平台。现有 256 行清单每端
只有 128 行，不能有效测 C256；下一 direct ceiling 需至少 512 行。更高优先级
是先让 project profiler 支持同一冻结 manifest、Chat Completions、no replay，
再比较吞吐、JCT、ramp regret 与最小饱和 work；不继续调当前上游策略参数。

晋级要求是相对最佳静态基线改善 observed tokens/s 或 SLO goodput，且 request
P99、failure、exactly-once 不退化。否则记录负结果，不增加控制复杂度。

完整机制卡、文献映射、fatal-flaw audit 和候选池见
`reference/literature_driven_pipeline_optimization_guide.md`。

### 10.4 RC2 核心瓶颈：AIMD 选错了观测信号（2026-07-27 集中梳理）— ⏸ parked-conditional（见 §0），文本轨道恢复且需做动态控制信号选择时再启用

**问题**：07-26 shared-vLLM 实验中 AIMD 0 次 decrease，根因是 AIMD 盯着
vLLM Prometheus `vllm:num_requests_waiting` 做决策——但请求在 Ray actor
侧排队，尚未进入 vLLM waiting queue，该信号始终为 0。vLLM 本身暴露了
完整的 Prometheus 指标（`num_requests_running`、`gpu_cache_usage_perc`、
`generation_tokens_total` 等），不需要修改即可获取。问题不在"拿不到信号"，而
在 **AIMD 选了不反映 Ray 侧排队状态的信号**。

真正需要但 vLLM 不暴露的细粒度信息（无论是否修改 vLLM 都拿不到）：

- per-iteration token batch composition（每次 forward pass 的具体 prefill/decode token 组成）
- per-request in-flight progress（请求 X 当前已生成多少 decode token）

**三种使用已有 vLLM Prometheus 信号做提交决策的方式**（均不需要修改 vLLM）：

| 方式 | 核心思路 | 落地成本 | 精度 | 已文档化 |
|---|---|---|---|---|
| **1. 模拟器模拟 vLLM 内部调度** | SFS 确定性 token-batch 模拟器重建 vLLM 内部调度过程。利用"我们提交了什么 + vLLM Prometheus running count + 离线校准的 β 参数"，模拟每个 token batch 的组成，预测 TTFT | 中（~200 行 Python + 离线 β 校准） | 高（TTFT MAPE <5%） | 模式 10 / §11 方案 A |
| **2. 解析模型估计系统能力** | LPS + USL：Prometheus `generation_tokens_total` 差分 → μ（服务率），arrival config → λ（到达率）。LPS 给等待时间、USL 给吞吐退化曲线和峰值并发 | 低（所有信号已有，无需新基础设施） | 中（平均行为，无 per-request 分布） | 模式 11+16 / §11 方案 B+E |
| **3. 客户端推断积压状态** | 用自己的 request lifecycle trace（submit + completion time）+ vLLM Prometheus gauge 做 EWMA：推断当前服务速率、oldest request slack、inflight token backlog——这些 Ray 侧信号才真正反映"软拥塞" | 低（trace + Prometheus 已有，~50 行 EWMA 状态） | 中（间接推断，滞后 1-2 个请求完成周期） | §10.2 缺口表 + §10.3 推荐顺序 |

**三种方式之间的关系**：
- 方式 2 和 3 共享同一套外部信号（Prometheus + lifecycle trace），可以**同时启用**——方式 2 给 K_max 解析上界，方式 3 给 flush timeout 动态调节
- 方式 1 可以**叠加**在 2+3 之上：当 2+3 的解析推断显示"当前接近饱和"时，用 1 做精确的 per-request TTFT 预测来决定哪些请求立即提交、哪些等待
- 推荐**渐进式推进**：先方式 3（零新依赖）→ 再方式 2（验证 K=8 解析依据）→ 最后方式 1（需要模拟器基础设施）

## 11. 2026-07-27 提交策略（RC2）文献驱动备选方案 — ⏸ parked-conditional（见 §0），文本轨道恢复时参考；方案 A–G 机制卡保留

以下从新精读的 SFS (arXiv 2026) 及其他 5 篇代价估计论文中提取的
提交策略备选技术方案。每个方案标注来源、落地难度、和与当前 K_max +
queue-adaptive flush 的关系。设计模式全文见
`research/knowledge_hub.md` §5.7。

### 方案 A：SFS What-If 预演（模式 10）

**来源**：SFS (Patel et al., arXiv 2026, §4.1)

**核心思路**：在每次 flush 决策时，用确定性 token-batch 模拟器预测
"如果现在提交这个 pending batch，每个请求的 TTFT 是多少"，只放行
TTFT 在 SLO 内的请求。

**与当前方案的关系**：
- 当前：`QueueAdaptiveFlush` 看 queue depth + vLLM waiting 做 25ms/50ms
  二元决策——粗粒度，不感知 per-request SLO
- 升级后：SFS 模拟器输出 per-request TTFT → 按 SLO 做精确准入 →
  flush 不再是"全部提交/全部等待"而是"选择性提交"

**实现步骤**：
1. 实现 token-batch simulator（Python, ~200 行），逻辑：给定 vLLM
   workload snapshot → 逐 iteration 模拟 token batch 组成和处理时间 →
   输出新请求的 TTFT 估计
2. 为 Qwen2.5-1.5B 校准 4 个 β 参数（离线 profile：记录若干 token batch
   的 composition→time 映射，线性回归）
3. 接入 vLLM Prometheus 获取 running request count 和 prefill/decode
   composition

**预期效果**：TTFT MAPE <5%（SFS 论文结果），亚毫秒决策开销

**风险评估**：
- vLLM Prometheus 可能不够细粒度（prefill/decode token composition
  无法直接从 `vllm:running_requests` 获取）
- SFS 假设每 decode 序列每 batch 恰好 1 token——在 chunked prefill
  下成立，但 speculative decoding 下不成立
- **放弃条件**：如果 Prometheus 信号粒度不足以支撑 token-batch 模拟，
  回退到方案 B（LPS 解析模型）

### 方案 B：LPS Queueing Model 指导 K_max 选择（模式 11）

**来源**：SFS §4.2（Average-case estimator, eq. 10-11）

**核心思路**：用 Limited Processor Sharing 公式估计给定 (λ, μ, K) 下的
平均等待时间：`W_avg = (λ/μ)^K / (μ - λ)`。不替代 K_max 动态调节，
但提供 K_max 初始值和解空间约束。

**与当前方案的关系**：
- 当前：K=8 来自实验暴力搜索（"对比 K=8/16/32 选最好的"）
- 升级后：从 profile 数据估计 μ（请求服务率 ≈ tokens/s /
  avg_tokens_per_request），从 arrival replay 参数获取 λ → LPS 公式
  输出推荐的 K 范围 → 作为 AIMD 的搜索边界

**实现步骤**：
1. 从已有 profile CSV 估计 μ（`observed_tokens_per_second /
   avg_prompt_tokens` per workload type）
2. 对每个 λ（arrival rate）计算使 `W_avg < SLO_slack` 的最小 K
3. 将 LPS-K 作为 K_max 的初始值或搜索下界

**预期效果**：减少 K_max 搜索空间，提供解析可解释性

**风险评估**：LPS 假设 Poisson 到达 + 指数服务时间（现实中请求
服务时间是 token-length 相关的，非无记忆）。SFS 论文显示 LPS
与实测高度一致（Qwen3-0.6B），但需在本地环境验证

### 方案 C：Token-Batch 处理时间线性回归（模式 12）

**来源**：SFS §4.1（eq. 9, 4-parameter regression）

**核心思路**：不模拟低层 GPU kernel——用 4 参数线性回归直接从 token
batch composition 估计 batch 处理时间。参数有物理含义：β1（dense
计算 ∝ tokens）、β2（attention ∝ context·decode_tokens）、β3（prefill
attention ∝ prefill_chunk·context + prefill²）

**与方案 A 的关系**：方案 C 是方案 A（SFS 预演）的子组件——SFS 模拟器
需要 `T_j(τ)` 函数来估计每个 token batch 的处理时间。方案 C 提供了
这个函数的校准方法。

**落地难度**：中——需获取 per-iteration token batch composition。
如果 vLLM 不暴露此信息，可用 Prometheus 的 `vllm:prompt_tokens_total`
和 `vllm:generation_tokens_total` 的差分做粗粒度近似

### 方案 D：轻/中/重 Workload 分档提交（模式 13）

**来源**：SPOS + Heinrich R3 + 项目已有计划（operator_cost README）

**核心思路**：不追求精确预测 E2E 秒数做准入——将 pending batch 分为
"轻（E2E < t_low）/ 中（t_low < E2E < t_high）/ 重（E2E > t_high）"
三档。提交策略按档位差异化：
- 轻 batch：激进提交（低风险，无需等待更多合并）
- 中 batch：正常 waiting window
- 重 batch：延长等待（需更多请求摊销 compute overhead）

**与当前方案的关系**：在 `QueueAdaptiveFlush` 的 25ms/50ms 两层之上
增加第三维度——batch weight classification

**落地难度**：低——当前 Ridge 可能已有足够排序能力做分档（MAE 11.68s
vs E2E 范围 ~5-300s）。需做的只是定义档位阈值并验证

### 方案 E：USL 并发-吞吐估计（模式 16）

**来源**：SABER (arXiv 2025, §IV.B) — USL 拟合 LLM 推理 per-request
速度退化曲线，R²=0.99

**核心思路**：USL `σ(N) = λN / (1 + σ(N-1) + κN(N-1))` 从 concurrency
sweep 数据（K=1,2,4,8,16,32,64）拟合出完整并发-吞吐退化曲线。峰值并发
`N* = √((1-σ)/κ)` 给出"再多发也没用"的解析上界。

**与当前方案的关系**：
- 当前：K=8 来自实验暴力搜索（"对比 K=8/16/32 选最好的"）
- 升级后：USL 给出解析 K_max 上界，与经验值互相校验——一致则经验值
  有理论支撑，不一致则说明 vLLM KV cache 抢占机制不服从 USL 平滑退化
  假设（同样有论文价值）
- 与方案 B（LPS）互补：LPS 建模等待时间随并发变化，USL 建模吞吐随
  并发退化——两者共同提供 K_max 的完整解析依据

**预期效果**：为 K_max 选择提供理论支撑，减少对暴力扫参的依赖

**风险评估**：USL 假设平滑二次退化（σ(N-1) 争用项 + κN(N-1) 一致性项），
vLLM 的 KV cache 抢占是不连续的阶跃退化——USL 可能只在"内存未耗尽"
区间拟合良好。SABER 代码未开源（仅方法论可迁移），~1000 采样点需在
本地重新采集

### 方案 F：双信号 Deadband 控制架构（模式 17）

**来源**：CONCUR (arXiv 2026, §4.3) — proactive + reactive 双信号 +
deadband 宽度 0.3

**核心思路**：不用单一信号驱动自适应——用两个独立信号（如 proactive
预警信号 + reactive 确认信号），仅在两者同时越界且变化幅度超出 deadband
时才触发动作。核心价值在于防止控制器振荡。

**与当前方案的关系**：
- 当前：queue-adaptive flush 看 queue depth 一个信号 → 25ms/50ms 二元；
  AIMD 控制器用单一信号 → 102 次 downshift/run 振荡（07-19 实验）
- 升级后：双信号（如 queue_depth + oldest_request_age 或 token_backlog
  + arrival/service_ratio）+ deadband（如 30%）——两个信号同时"说该
  发了"才改 timeout，变化量不够 deadband 不动作

**预期效果**：消除或大幅减少控制器振荡，使自适应 flush 在稳态 workload
下行为接近最优静态（fixed-50），在负载变化时及时切换

**风险评估**：需选定第二信号并调 deadband 参数。CONCUR 的双信号
（U_t KV cache 使用率 + H_t 命中率）是针对 agentic KV cache 抖动的，
数据库 AI 算子的"第二信号"需要独立选择。如果所选信号对不独立
（高度相关），退化为单信号 + 死区——仍有改善但不如双信号

### 方案 G：Credit-Based Admission（模式 18）

**来源**：SCORPIO (arXiv 2025, §3.4) — TRP credit accumulation +
VBS admission control

**核心思路**：不设全局 K_max——每个请求按 SLO 紧松度获得不同 credit
累积速率 TRP(r) = min S_TP / S_TP(r)，credit ≥ 1.0 时准入。紧 SLO
请求更快被放行（不被大 batch 拖累），松 SLO 请求在 credit 慢速累积中
自然合并（摊销 overhead）。

**与当前方案的关系**：
- 当前：K_max 是全局固定值，所有请求不分紧迫度按 FIFO 顺序提交
- 升级后：per-request deadline tracking + credit accumulation →
  "该不该发"由请求的 SLO 紧迫度决定而非全局 K_max

**预期效果**：在 SLO 异构场景下（混合 workload、多模态请求混跑、
在线+离线混合）提升 SLO goodput

**风险评估**：当前批量离线场景 SLO 同质 → credit 退化为均匀累积 = FIFO，
不体现区分度。只有在 SLO 异构性存在时才发挥价值——可能需要等到多模态
或多 job 场景。需增加 per-request deadline tracking 基础设施

**放弃条件**：如果未来 workload 始终保持 SLO 同质（纯离线批处理），
Credit-based admission 退化为 FIFO，与当前方案等价——不值得额外复杂度

### 提交策略备选方案优先级

```
落地难度 →        低              中              高
收益 ↓
高               方案D 分档提交   方案A SFS预演
                方案F Deadband
中               方案B LPS模型    方案C Batch回归
                方案E USL估计
低               方案G Credit-Based（需 SLO 异构场景）
```

**建议推进顺序**：
1. **先做方案 D**（最低风险，已有 Ridge 模型）：验证 Ridge 的分档能力
2. **再做方案 B + E**（解析指导，不需改 pipeline）：LPS + USL 联合审计
   当前 K=8 选择
3. **再做方案 F**（控制架构升级，改动 ~50 行）：双信号 deadband 架构
   解决振荡问题
4. **最后做方案 A**（需要 SFS 模拟器 + Prometheus 接入）：在前几项确认
   有效后再投入
5. **方案 G 待 SLO 异构场景出现后启动**

### 与现有 RC2 缺口的整合

| 现有缺口 (§9) | 新增文献方案 | 整合方式 |
|--------------|------------|---------|
| request-level continuous replenishment | 方案 D（分档提交）+ 方案 G（Credit-Based） | 分档/credit 决定"哪些请求可以立即补位" |
| SLO-aware EWMA flush | 方案 A（SFS 预演）+ 方案 F（Deadband） | Deadband 消振 + SFS 提供 per-request TTFT |
| 软拥塞（Ray 侧积压 vLLM 不可见） | 方案 A + C + 方案 F | 双信号架构的第二信号可选逐请求完成时间 |
| K_max 选择 | 方案 B（LPS）+ 方案 E（USL） | LPS 等待时间 + USL 吞吐退化，联合推导 |

### 不纳入 RC2 的方案

- **GNN/图表示的代价模型**（CONCERTO/GRACEFUL/COSTREAM）：属于 RC4
  代价估计范畴，不直接用于在线提交决策
- **SFS 的 accuracy-cost-latency 路由框架**：SFS 论文做 multi-model
  routing（"选哪个模型实例"），项目是 single-model admission control
  （"什么时候提交给同一个 vLLM"），决策框架不同

### 跨 RC4→RC2 的辅助技术

以下技术主要属于代价估计（RC4）范畴，但对提交策略（RC2）有直接辅助
价值，在对应 README 中有详细方案：

- **Output-Length 预测器**（模式 15 | `operator_cost_estimation_20260726/README.md` 第一批 #3）：
  用 LightGBM 从 prompt 特征预测实际输出 token 数，替代 `completion_max_tokens`
  作为 Ridge 特征。对 RC2 的价值：更准确的实际计算量估计 → 更好的
  per-request 工作量预估 → SLO-aware flush 和 token-work admission 的
  输入质量提升。
- **Probe Execution**（模式 14 | `operator_cost_estimation_20260726/README.md` 第三批 #11）：
  验证 partial execution → full E2E 的相关性，加速 profile 数据收集。
  对 RC2 的价值：更快的 (λ, μ) 参数校准，减少 LPS K_max 选择和新
  workload 接入的 profile 成本。

---

## 12. 关于"已排除"技术的状态说明（2026-07-27 审计）

以下技术在 07-26 实验中未表现出优于当前 baselines 的结果，但代码和
实验记录均已保留，**不视为永久排除**。AIMD/EWMA-AIMD/PID 与 SLO-EWMA flush 已于 07-29 在双 4090 重跑（见 §1.2），单 GPU 限制不再适用于这两项；其余技术的当前结论仍受限于 Qwen2.5-1.5B、512 行稳态 workload 等测试条件——在不同硬件/模型/负载
/多租户场景下可能重新体现出价值：

| 技术 | 当前结论 | 保留位置 | 重新激活条件 |
|------|---------|---------|------------|
| AIMD/EWMA-AIMD/PID 自适应准入 | 相对 static K=16 无增量；shared-vLLM 下 vLLM waiting=0，AIMD 看的信号不反映 Ray 侧积压 | `code/src/scheduling/submission_control/adaptive.py`、`pid.py` | 改用反映 Ray 侧积压的信号后（逐请求 completion time 观测→可能解锁动态控制价值，见 §10.3 诊断） |
| Two-level queue-adaptive flush | 相对 fixed-50ms 无稳定增量（89.4% 时间选 50ms，行为接近 fixed-50） | `code/src/scheduling/submission_control/flush.py` | 多 workload shape、变长输出、多租户到达模式下重新评估 |
| GNN/Transformer 代价模型 | 283 行数据远未达到需要 GNN 的规模（Heinrich R1 + Pathak & Mankodi 一致结论） | 未实现（仅保留设计文档） | profile 数据增长到千级/万级行后 |

**重要**：上述技术不是"被否定"，而是"在已测试条件下未优于更简单的
baseline"。代码实现均保持可用状态，后续重新激活时改动量预计较小（主要
是接入新观测信号或切换 workload 配置）。特别是 AIMD 自适应准入——
§10.2 的诊断指出选错信号是根因而非控制器参数问题，一旦
request-level completion replenishment 提供了逐请求完成时间信号，
动态控制的价值可能需要重新评估。

---

## 13. 2026-07-29 baseline 优势验证的当前门禁 — ⏸ pivot 前文本轨道门禁（K256/W98K），已被 §0 取代；文本轨道恢复时为入口

512 行 direct C256 已达到 vLLM Bench 15,351、bounded HTTP 14,532 total
tokens/s；project 单次 K256 为 11,736。project 的 9-cell calibration 因理论
等价的 K256/W98K 相差 2.83× 而失效。只读诊断把主差异定位到首次
full-concurrency 的 HTTP/vLLM request wall，而不是 active-work credit 或 actor
创建。

因此当前唯一安全远端动作是运行 K256/W98K 的 1 warm-up + 3 repeats
等价性门禁。它通过后才依次执行：

1. direct/official/project 单 job 独立 calibration 与 held-out；
2. 32/64/128/256-row transient saturation；
3. matched one/two-GPU scaling；
4. bounded HTTP、independent project、shared static credit、fair credit 的
   1/2/4-job 矩阵；
5. 官方 OceanBase capability 通过则测产品 arm，否则仅测明确标注的
   OceanBase-style 轻量模拟；pgai 保持 embedding 对照。

所有主 arm 使用同一双 endpoint。不得通过挑选 Daft Native 单次高值、弱连接
池、不同 request body 或不同输出 work 寻找优势。晋级门槛以
baseline 身份与晋级门槛以 `baseline_reference.md` 为准，文本运行合同以
`completed/text_native_baseline_rerun_20260802.md` 为准。
