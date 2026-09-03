# Research Experiment Results

## 统一入口

先读 [`EXPERIMENT_EVIDENCE_REGISTRY.md`](EXPERIMENT_EVIDENCE_REGISTRY.md)。该台账只登记主要机制结果与证据强度，
区分“设计预留、功能测试、真实链路验证、GPU 筛选、重复或留出验证”，避免把代码完成度误写成性能证据。
它不定义工程架构或下一步实施顺序。

## PostgreSQL 生成型 Map PG plan/权限（2026-09-03）

[PG plan 与权限验证](postgresql/semmap_pg_plan_20260903/README.md)：独立分支 `2205ccbb`，
新 SQL 0.2.0/schema 4、固定参数、复制/严格解码、原生 EXECUTE ACL/hook 与缓存计划检查；
PG18.3 `-O2 -Werror`、regression 1/1、TAP 1260/1260、本地/服务器各 136/136 与 C11 通过。
本分支未合入 main，实际生成执行仍明确拒绝；没有新 Map 模型或资源验收。失败与中间结果单独保留。
后续 [SQL wrapper 修复](postgresql/semmap_pg_plan_20260903/README.md#sql-wrapper-source-check) `676615fa`
关闭内联后的参数来源旁路，重新通过 PG18.3 TAP 1283/1283、regression 1/1 和两端各 136/136；
只接管显式 Map，普通 SQL 内联保留，仍没有 C v5 执行或模型资格。

## PostgreSQL 生成型 Map 纯值与 Python v5（2026-09-03）

[纯值与 gateway 验证](postgresql/semmap_values_20260903/README.md)：独立分支源码 `425d2b1c`，
C/Python 身份/完成值和严格 Python v5 复用旧 session/HTTP；本地/服务器各 135/135，重新运行
PG18.3 `-Werror`、regression 1/1、TAP 1022/1022。尚未接 PG Map plan/C v5/SQL；未运行真实模型或资源验收。
独立复核发现的数字输入错误、记录器脱敏和测试标签问题已处理，失败与历史结果保留。
后续[深层 JSON 输入隔离](postgresql/semmap_values_20260903/README.md#json-depth-repair)在 `a1bbdd30`
通过本地 136/136 和 C11；仅修 gateway 读帧，未重跑服务器 PG 或真实模型，原验收身份不变。
上述消息、纯值/v5 与修复已随 `b0400944` 合入本地 main；[合并复查](postgresql/semmap_values_20260903/README.md#main-integration)
重新通过本地 136/136 与 C11，不新增 PG 或真实模型资格。

## PostgreSQL 生成型 Map 消息编译（2026-09-03）

[规范消息与旧路径兼容](postgresql/semmap_messages_20260903/README.md)：独立分支源码 `6903cf46`，
C/Python 共享两消息编码，Map 原样文本、Filter 字节不变；本地/服务器各 107/107，重新执行 PG18.3
`-Werror`、regression 1/1、TAP 1022/1022。只完成消息子切片，不表示完整值/摘要、生成型 SQL/v5、
golden/真实模型或资源验证通过；后续已随上方修改合入本地 main，原证据范围不变。

## PostgreSQL 函数对象身份（2026-09-02）

[身份加固与准备计划验证](postgresql/function_identity_20260902/README.md)：测试源码 `934f4f61`
修复同签名非成员误接管，生产仅修改函数查找。最终 PG18.3 `-Werror`、regression 1/1、TAP 1022/1022
通过，新增身份 103 项包含两个物理连接的仅成员 ADD/DROP 手动刷新。实现与证据已合入 main；仅成员变更的
跨会话自动失效仍 pending，须按记录刷新所有相关连接。不涉及新模型、Map 生成或多会话执行。

## PostgreSQL choice profile（2026-09-02）

[真实 choice 服务验证](postgresql/choice_service_20260902/README.md)：`0a1c12d3` 的 PG18.3 → gateway
→ Qwen2.5-1.5B/vLLM 完成 14 次 old/choice 请求及两个 NULL 对照；累计 15/100，含首轮工具失败的 1 次。
本地/服务器各 94/94。四 C 工程验证完成，质量与校准未通过；启动及计数工具失败、原始输出全部保留，
无新 TAP/构建；源码与证据已进入当前集成版本。

[资源与预算工具验证](postgresql/choice_resources_20260902/README.md)：`4464fe9b` 的受控 PG18.3
fixture 检查通过，含 v3/v4 各 5,164 次、取消/阻塞 DNS 各 10 次与恢复；本地 91/91。
复用旧已验证二进制，未重跑 TAP，真实模型请求为 0；多会话仍是待实现能力。失败和完整时间序列保留。

[Filter INSERT 修复](postgresql/semfilter_insert_20260902/README.md)：`39007150` 的 PG18.3 regression 1/1、
TAP 919/919、本地/服务器各 83/83 通过。生产只修改 Filter planner，新增 171 项验证真正的写入、
事务和限制；该次未做资源/真实模型验证，后续结果见上方记录。此前 SELECT 接线与 INSERT 失败证据保持原提交身份。

[PG C/wire v4 接线](postgresql/choice_pg_wire_20260902/README.md)：`80bb7fc5` 的 choice SELECT 共用
现有 runtime 与严格 parser。本地/服务器各 83/83、PG18.3 regression 1/1、TAP 748/748；实际新旧
HTTP 请求仅差 choice 字段。该次另用旧/新二进制复现 Filter INSERT 未 lowering，未做资源与真实模型验证；
后续修复及验收见上方记录。

[gateway v4 与请求映射](postgresql/choice_gateway_v4_20260902/README.md)：`7d72d9ad` 实现严格 v4、
共享 session 和显式 fixed-model choice 映射。本地/服务器各 83/83，PG18.3 regression 1/1、TAP
537/537 通过；该历史切片当时未接 C codec/open spec，PG 拒绝执行。未调用真实模型。

[PG plan 接入](postgresql/choice_pg_plan_20260902/README.md)：第四个 option 与 schema 3 已实现，
保存完整 profile，支持计划复制、prepared plan 和 EXPLAIN；该切片实际执行明确拒绝，未接 wire v4。
最终 PG18.3 regression 1/1、TAP 537/537、本地/服务器各 68/68 测试通过；没有真实模型调用。

[首个实现切片](postgresql/choice_profile_contract_20260902/README.md)：新增严格 profile 值与 C/Python
规范字节编码，本地/服务器合同 68/68 通过，另有 C11 和 PG18.3 `-Werror` 编译记录。
该历史切片尚未接入 SQL/plan/wire、未安装扩展或调用模型；后续 PG 接入以上方记录为准。

## PostgreSQL exact SemFilter reference calibration（2026-09-01）

[单一分类 prompt 对照](postgresql/semfilter_prompt_qualification_20260901/README.md)：实际消息与
chat template 核对一致。1.5B 唯一新 prompt 的旧/新样例各 5/9，matched 7B 为 7/9、6/9；每例
三次重复，全部格式合法但没有配置满足全部正确要求。生产配置不变，校准继续暂停。保留中止的
默认参数失配尝试；两个完整尝试和部分尝试合计 321 次 completion 请求。

[校准前独立小切片](postgresql/semfilter_qualification_20260901/README.md)：builder 拒绝共线观测，
PG18.3 多列统计将普通谓词估计从 8 修正为 64；choice 格式 30/30，但两种配置的语义预期均只符合
12/27。reference 资格未通过，整轮采集继续暂停；PG18.3 回归 1/1、TAP 437/437、Python 59/59 通过。

[首轮真实采集](postgresql/semfilter_reference_calibration_20260901/README.md)：64 条预热完成，但首个
training 查询第 23 个模型响应违反严格输出格式，PG18.3 以 `22000` 终止语句。held-out 未运行，
没有拟合或 artifact；脱敏逐请求数据、错误、构建/测试日志和 SHA 清单已归档。机制资格不变，
真实成本校准仍未通过。

## 状态感知 phase-change（2026-08-11）

| Directory | Content | Boundary |
|---|---|---|
| `phase_change_state_aware_corrected_early_stop_20260811/` | 修正门禁、HTTP tail-drain 与多 Job 全局 arrival clock 后的 A-only + 三档 pressure | A-only 证明 K160 相对 K128 service rate +7.77%；B=2.5/3.5/4.5 均未形成双 endpoint、双周期降档压力，按门禁停止，未跑 action/formal。 |
| `saor_capacity_development_20260811/` | SAOR 控制 microbenchmark、paired trace replay 与 2×4090 四臂 capacity-only development gate | 4/4 arm、0 incident；SAOR 相对 K128 +4.36%，相对 K160 +0.52%、相对简单 threshold −1.46%，未晋级。一次顺序运行且 provenance 有已修复缺口，不作 formal 排名。 |
| `saor_active_set_release_formal_20260812_69affc7e/` | fixed-envelope 2-Job 六 active-set 臂 + 四 matched-solo，1+3 formal | 40/40、0 incident、exactly-once；resolution-aware v2 完整 validation passed、credit mechanism effective 12/12。SAOR 在 credit 臂内 fg 最好，但 static 显著更强，未晋级。 |
| `saor_priority_reachability_smoke_20260812_91ffcaa/` | static/SAOR/foreground strict-priority 三臂，两轮 rehearsal-only release 上界短测 | strict-priority 11,791 tok/s、fg JCT/P99 20.04/14.27s、fg SLO 0%；相对 SAOR fg P99 −73.02%，但 formal repeats=0，仅证明 release-only 可达性，不是 proposed/winner。 |
| `state_aware_work_unit/saor_bounded_priority_gate_20260813/` | static/SAOR/$0.125W_e$/$0.25W_e$ 两轮 bounded-priority development gate | 第 2 轮 $0.25W_e$ debt-recovery=0 被 fail-closed；两 cap fg P99 49–56s、SLO violation 85%–95%，均未晋级。交叉验证定位 ready backlog 未完整暴露给 coordinator；formal/4-Job/reservation 未运行。 |
| `state_aware_work_unit/saor_bounded_ready_gate_20260813/` | 同 selector、改为项目自有的有界 concrete-ready 预注册后的两轮四臂 development gate | 8/8 cell、0 incident；$0.125W_e$ 两轮全过，均值 12,361 tok/s、fg P99 17.87s、fg SLO 0%、bulk 30s miss 0.662；$0.25W_e$ 被拒绝。候选需先过 project-only matched-observation 归因门，formal 尚未运行；原生 baseline 不使用该机制。 |
| `state_aware_work_unit/saor_matched_ready_selector_rehearsal_20260813/` | frozen-static 与同 bounded-ready observation 下 FIFO/DRR/VTC-style/strict-priority/SAOR 的双轮 Project 内部 selector 归因 rehearsal | 12/12 cell、0 incident；DRR/VTC-style 约 12.90K tok/s 且 fg 30s SLO 零违约，SAOR 12.28K tok/s、fg P99 17.85s。SAOR 是观测到的效率—tail 非支配折中点，不是 selector winner；固定顺序 n=2 且 selector 级 non-inferiority margin 未预注册，`formal_authorized=false`。原生 baseline 数量为 0。 |
| `state_aware_work_unit/saor_ready_observation_bridge_rehearsal_20260813/` | frozen-static→single-head shared FIFO→bounded-ready FIFO 三臂双轮 Project bridge | 6/6 cell、0 incident；共享容量 tok/s +25.96% 但 fg P99 +99.17%，bounded-ready 在相同 FIFO 下再使 tok/s +7.30%、fg P99 −33.62%，仍有约 40% fg SLO violation。分离了效率/隔离与 observation 效应；不是 native baseline/formal。 |
| `state_aware_work_unit/saor_project_mechanism_rehearsal_20260814/` | frozen-static + 同 bounded-ready observation 的 FIFO/DRR/VTC-style/strict-priority/SAOR 最终六臂 Project mechanism rehearsal | `63d17300` root 6/6、0 incident，validation passed；6,144-request fixed-output-cap audit 通过，SAOR 96/96 recovery completion、15/15 repayment completed、P95 3.234s、0 unresolved，1,108/1,108 projection 一致。单次相对 VTC-style lag P95 −13.15%、no-service +0.014%；只进入独立审核，不判排名、不授权 formal。 |
| `state_aware_work_unit/saor_native_system_matched_manifest_20260819/` | native-system matched 的 Job0/Job1 SHA、512-row、合并顺序与独立 formal 授权模板 | 完整 prompt manifest 留在 Git 外；readiness 逐 Job 校验并要求 combined=Job0+Job1。merge 与 rehearsal 均不自动授权 formal。 |
| `state_aware_work_unit/saor_native_system_matched_calibration_20260819/` | Project selection 与 Daft Native/Daft Ray/Ray Data 原生执行 selection identity | Daft C1/B1 是 vendor control，Ray Data C8/B16 是一次 development screen 冻结点；只声明身份匹配，不夸大为统计最优。 |
| `state_aware_work_unit/saor_native_system_matched_gateway_rehearsal_20260821/` | 五臂统一 T0--T4 与 observation-only gateway 的单次 GPU rehearsal | 5/5、exactly-once、archive validation 通过；SAOR 相对同 executor static 吞吐 +31.01%、group JCT −23.70%，但 P99/lag/no-service 变差、Jain −1.50%。只作 rehearsal 观察；0s/5s pre/post isolation 样本不足，formal 未运行。 |

## 开题统一文本 database-E2E（2026-08-08 correctness 护栏）

| Directory | Content | Boundary |
|---|---|---|
| `opening_database_e2e_text_refeed_20260808/` | SQuAD + ShareGPT 三静态臂 replacement；24/24 单元、18 formal、统一 PG source/sink、MFU/能耗/服务状态 | correctness/稳定性通过；SQuAD 可核对完成性与答案质量，但 Project 计时额外包含指标采集、记录写入和结束处理，不按不到 1% 的差异排名。ShareGPT C32 后续证实欠供给，旧 1.546 比值不排名；DuckDB 有 4,921/6,144 cap 语义失败。 |
| `opening_database_e2e_text_20260807/` | 首轮相同三臂合同 | project feeding 89.93%/91.38% 未过门，只保留为 failed-feeding 历史诊断，性能数字已被 replacement 取代。 |

## 开题文本原生框架入口（2026-08-08）

| Directory | Content | Boundary |
|---|---|---|
| `opening_text_native_gate_20260808/` | bounded、Daft Native/Ray、Ray Data 的 256-row capability gate，以及 Ray Data C4/C8/C16 最小筛选 | 6/6 gate 正确性与 provenance 通过；冻结 C8/B16 measured peak 供正式矩阵使用。n=1 gate 不作框架性能排名。 |
| `opening_text_native_single_job_formal_20260808/` | 同一 2,048-row ShareGPT manifest 的 bounded C128、Daft Native/Ray、Ray Data 1+3 formal，含 MFU、服务压力与能耗 | 16/16 cells、12 formal 通过；Daft 两臂高 waiting/KV，Ray Data 当前路径低 running/MFU。只报告官方 graph/冻结点外部现象，不称项目胜出。 |

## 开题文本多 Job 干扰（2026-08-09）

| Directory | Content | Boundary |
|---|---|---|
| `opening_multijob_interference_20260809/` | online exact-short、Project eager full/half/static/shared、三条原生5s overlap观察，以及逐请求阶段/状态分解 | online下shared提高aggregate但伤short/Jain；eager下quota-only +59.00%、matched static竞争+58.77%、shared竞争+28.90%，shared相对static short JCT−48.94%。结论是arrival-regime dependence和idle borrowing动机，不是动态普遍胜出。 |
| `opening_fourjob_interference_20260809/` | 1 short+3 matched long 的 Project full/quarter/static/shared 因果分解，以及 Daft Native/Ray、Ray Data 原生 single→four-job 三重复 | 按三次 formal 均值，Project shared 相对 static 总吞吐 +8.68%，四个 Job JCT 全部改善，属于效率/JCT 子向量上的 baseline-relative empirical Pareto；但 raw-work Jain 0.960→0.923、long 收益不均，且 long1/2 未达到 quarter-solo 非劣。报告 full/reserved/static 三反事实；历史 compact 数据不足以补 event-level service lag。只作轨内证据，不作完整 Pareto、理论公平性质或跨框架绝对排名。 |
| `opening_image_native_fourjob_formal_20260810/` | 同一 2K short + 3×3K long、0.5s offset 的 Daft built-in/Ray Data single→four-job 1+3 正式观察 | 40/40 runs、30 formal group 通过；只比较各系统内 Job slowdown，不作跨框架绝对排名，不称 Project/state-aware 胜出。 |
| `opening_image_project_fourjob_observe_only_formal_20260810/` | 同一图像 manifest 的 Project single/static/proposed-role staged descriptor + observe-only snapshot 1+3 正式门禁 | 24/24 group runs、99K formal rows exactly-once；3,114 个 snapshot 100% fresh、构建均值 0.141 ms；static/proposed group JCT 差 0.98%，只证明观测接入，不称动态胜出。 |
| `opening_project_short_all_at_t0_diagnostic_20260809/` | 同一 short manifest 的 Project all-at-t0 1+3，统一 T0–T4 timer 与 Daft raw 对齐 | Project T3 model-request window 11.354s vs Daft 11.059s，service tokens/s 与 MFU 差约−2.5%；Daft T0 未采集，完整系统 E2E 不排名。 |

## 图像 AI_EMBED operator（2026-08-03/04）

| Directory | Content | Boundary |
|---|---|---|
| `image_ai_embed_operator_formal_20260803/` | 60K×2 held-out Ray Data/project 2×2 CPU formal、Daft 12K capacity consistency，以及 schema-v12 派生观测 | Ray/project 同规模 matched-resource 结论有效；Daft 因物化容量上限单列。跨规模只描述独立平台上的 images/s/单位资源，absolute JCT/first output 不混排。 |

## 双 GPU 调度与容量（2026-07-28/29）

| Directory | Content | Boundary |
|---|---|---|
| `multicard_scale_ramp_enhanced_20260807/` | bounded HTTP + harness-pre-split DuckDB AI 的九档规模曲线，三重复并补齐 during-cell vLLM gauges、身份、能耗和失败 cell 紧凑证据 | bounded 27/27、DuckDB 22/27；两路径身份与计时粒度分开，不作产品原生多 endpoint 排名；逐请求输出和日志留服务器。 |
| `multicard_lbrr_scale_ramp_enhanced_20260807/` | 单 DuckDB→nginx round-robin→双 vLLM 的九档规模曲线，27/27 三重复紧凑证据 | `gateway_system_diagnostic`，query-barrier 口径；不冒充 DuckDB 原生调度，也不与 request-level E2E 延迟混排。 |
| `static_credit_prompt_length_screen_20260730/` | Short/long prompt static request/work credit screening, with independent median/CV/equivalent-arm audit. | 48/48 succeeded, but urllib/no-token-ID and 48.5% divergence among no-pressure short arms make the dynamic GO/NO-GO inconclusive. Retained as mechanism-audit evidence; rerun the async equivalence gate. |
| `dual_gpu_shared_vllm_formal_20260729_1135/` | 1/2/4-job independent-full, static-partition and endpoint-shared DRR comparison. | 36/36 succeeded with exact global request/work bounds. Two jobs show no gain; four jobs improve aggregate throughput by 9.57% and max P99 by 22.52%, but repeat-level results are heterogeneous, so this is a high-contention candidate rather than a universal default. |
| `dual_gpu_slo_ewma_flush_formal_20260729/` | Fixed-50, queue-25/50 and SLO-EWMA-25/50 under high and arrival-limited replay. | 24/24 succeeded; SLO-EWMA changes throughput by -0.52%/+0.10% versus fixed and all arms have zero 30s-SLO violations. It does not meet the promotion gate. |
| `dual_gpu_service_quantum_20260729/` | Fixed-work batch/512/1024/2048/4096/request completion-granularity comparison. | Fine granularity reduces credit-held by about 16% but changes throughput by at most +1.75%; no fixed quantum meets the promotion gate. |
| `dual_gpu_actor_pool_shape_20260729/` | Fixed-work, fixed-slot and fixed-Ray-CPU 1×256/2×128/4×64 actor-pool comparison. | Multi-actor shapes gain at most 2.00%, below the preregistered 5% promotion threshold; retain 1×256 for the current single-job homogeneous endpoints. |
| `dual_gpu_active_work_saturation_20260729/` | Dual-4090 eight-point request-level active-work saturation curve with three formal repeats per cap. | 65,536 is the preregistered smallest saturation point; above it throughput plateaus while P99/SLO worsen. |
| `dual_gpu_active_work_curve_20260728/` | Earlier five-point active-work curve used to discover that the original upper bound was still rising. | Superseded for capacity selection by the 2026-07-29 extension; retains diagnostic and reproducibility value. |
| `dual_gpu_request_replay_20260728/` | Whole-submission barrier versus request-level replenishment under K-count controls. | K48 matches batch K16 at nominal matched work; K64 mixes in about 33% more offered work. |

## Output-aware Packing (2026-07-26)

### Current mechanism decision

| Directory | Content | Boundary |
|---|---|---|
| `row_cap_aware_packing_512_20260726/` | Prefix-cache-audited 512-row row-cap/token-budget/packing screening plus three-repeat confirmation. | Row-cap-first has a small 512-row signal; cache-enabled exploratory runs are retained only as invalid-ordering audit evidence. |
| `row_cap_aware_packing_1024_20260726/` | Held-out 1024-row sequential/classic-BFD/row-cap-first comparison with request SLO, energy, and MFU. | Negative default-adoption result: about 1% throughput gain caused SLO violation to rise from 50.39% to 88.67%; sequential remains default. |

### Earlier gates and superseded runs

| Directory | Content | Boundary |
|---|---|---|
| `row_cap_aware_packing_gate_20260726/` | 64-row real PostgreSQL→Daft→Ray→vLLM gate for sequential, classic BFD, and BFD-inspired row-cap-first placement. | Infrastructure validation only; 6/6 runs and all request/resource/MFU invariants pass, but one formal repeat cannot support performance ranking. |
| `output_aware_bfd_gate_20260726/` | Superseded pre-fix 64-row output-aware BFD gate. | Lifecycle/resource evidence remains auditable, but sequential and BFD row caps were not matched; excluded from algorithm comparisons. |
| `output_aware_bfd_gate_v2_20260726/` | 64-row real-component gate for output-cost modes, sequential/BFD packing, request/resource traces, power, energy, and MFU. | Infrastructure validation only; all token-budget policies share token and row caps. |
| `output_aware_bfd_512_v2_20260726/` | Six-cell 512-row sequential/BFD × output-cost comparison, with 18 formal runs and plot-ready summaries. | BFD trace is a positive candidate at 512 rows, but n=3 and trace metadata is not a paired output oracle. |
| `output_aware_bfd_1024_20260726/` | Held-out 1024-row confirmation against same-cost sequential and strongest practical baseline. | Negative scale confirmation: current BFD does not generalize; row-cap-aware joint tuning is required. |
| `output_aware_bfd_512_20260726/` | Superseded failed run that exposed inconsistent sequential/BFD row caps and a timeout incident. | Audit evidence only; excluded from performance conclusions. |

## Local Baselines

| Directory | Content | Boundary |
|---|---|---|
| `opening_bounded_saturation_calibration_20260808/` | ShareGPT bounded HTTP C32/C64/C128/C256 容量校准，含 MFU、服务状态与服务器原始归档 SHA。 | C128 实测达到 C256 的 98.22%，是第一个满足预先规定 97% 选择条件的并发点；只用于确定 bounded 对照并纠正旧 C32 欠供给口径，不作框架性能排名。 |
| `shared_vllm_adaptive_admission_20260726/` | Real shared-endpoint foreground/background K8/K16/AIMD repeats plus adaptive-flush follow-up, with exact request-token accounting. | Static K8 protects foreground tails; AIMD saturates near K16 with zero decreases and provides no feedback gain. Adaptive flush behaves mostly like fixed-50 and has no stable increment. |
| `adaptive_admission_controller_20260726/` | Real 64-request gate, randomized 512-request static/AIMD/EWMA/PID matrix, and AIMD-vs-static-K16 mechanism control. | Dynamic controllers beat K=8 by converging near K=16, but AIMD is indistinguishable from static K=16; shared-service protection remains unverified. |
| `vllm_cuda_graph_512_20260726/` | Matched eager/CUDA-Graph 64-request gates plus one warm-up and three formal 512-request repeats per arm, with full prompt/output/request/resource/MFU tracing. | CUDA Graph is the current local steady-state baseline: E2E -71.76% and observed tokens/s +254.05% versus eager; this is deployment tuning, not an upstream scheduling contribution. |
| `adaptive_flush_cross_rate_20260726/` | Real 512-request fixed-25/fixed-50/adaptive screens at about 51.4 and 12.85 req/s replay intensity. | Fixed-50 remains best or equivalent across the tested range; adaptive does not justify default complexity. |
| `text_heldout_2048_20260726/` | Natural-EOS 2048-request held-out fixed-50/adaptive comparison with exact request and MFU audits. | Fixed-50 keeps a 1.75% throughput and 2.61% P99 advantage in the single screen; sustained backlog still amplifies tail latency. |
| `prefix_aware_batching_20260726/` | Controlled 0/30/70/100% shared-prefix workloads, code-semantic audits, and real vLLM screens. | With prefix cache disabled, prefix-only grouping has no stable benefit; sequential token-budget remains default. |
| `operator_cost_estimation_20260726/` | Formal-only 23-feature decision-context LOO over 204 real formal rows；历史 all-phase 结果已归档。 | CE5 candidate pairwise 0.800、macro/pooled/max regret 4.58%/0.62%/26.23%；row pairwise 0.684 未过门槛，不晋级。 |
| `operator_cost_profile_pilot_20260804/` | 双 4090 四候选 cost-profile v1/v2 运行合同门禁与完整 raw trace。 | v2 8/8、0 incident、512 unique requests/cell、23 维四向量同 context；n=1 只验证采样合同和约 4 小时 formal 预算，不作配置排名。 |
| `operator_cost_profile_dual4090_formal_20260804/` | 首次双 4090 320-run formal 的并发 runner 与空 Ray 地址事故审计。 | 两套输出均排除：几乎全程共享 GPU/vLLM 竞争，且 640/640 子运行启动 local Ray；本目录不含性能结论。 |
| `adaptive_flush_randomized_20260726/` | Natural-EOS gate, randomized 512-request fixed-25/fixed-50/queue-adaptive repeats, and exact output-token/finish tracing. | Fixed-50 and adaptive both beat fixed-25 by about 32% tokens/s and are indistinguishable; fixed-50 is the simplest current candidate. |
| `joint_batching_submission_512_20260726/` | Real 18-cell token-budget × K_max × flush screen plus randomized repeated validation of independent splice, joint candidate, and fixed-50 mechanism control. | Under the 1% SLO gate, independent splice and joint search are indistinguishable; fixed-50 is the simplest current workload-specific candidate. |
| `local_vllm_qwen15b_baseline/` | Local `AI_COMPLETE` baseline for `PostgreSQL -> Daft -> Ray -> vLLM Qwen2.5-1.5B`, including synthetic smoke, ShareGPT/BurstGPT fixed row-batch sweep CSVs, and a latency metric probe. | Local PG rehearsal, fixed row-batch baseline only; not a token-aware scheduling result and not a PostgreSQL 18.3 internal-platform result. |
| `accelerated_arrival_flush_20260725/` | Real single-GPU accelerated-arrival comparison of immediate, fixed-timeout, and queue-adaptive flush, with run, submission, flush, and resource traces. | Controlled accelerated replay on one RTX 5070. Fixed timeout reduced submissions but did not yield a statistically separable throughput gain; the current queue-adaptive rule formed no multi-row batches. |
| `adaptive_flush_window_20260725/` | Corrected dual-window adaptive flush gates, 1024-row probe, and 512-row repeated comparison with plot-ready traces. | Positive single-GPU candidate evidence under accelerated replay; fixed policy-group order, fixed 16-token output cap, and missing per-request E2E tails still require follow-up. |
| `request_lifecycle_gate_20260725/` | Real 64-prompt PostgreSQL→Daft→Arrow→Ray→vLLM gate for request lifecycle, seeded runner, SLO fields, and explicit request→submission identity. | Infrastructure validation only: one run per strategy, fixed then adaptive; not policy performance evidence. |

本目录保存正式研究实验结果和小范围优化测试记录。

## 当前状态

正式优化实验已经开始；本目录保存方法实验。早期 GPU-backed 画像和动机实验仍位于：

```text
motivation/results/gpu/
motivation/results/pg18_4_fake/
motivation/results/fake_cpu/
```

新增结果必须对应两项研究内容、共同代价估计或多模态泛化中的明确问题，并同步
`EXPERIMENT_EVIDENCE_REGISTRY.md`；不要从本目录列表推断当前执行优先级。

## 结果命名建议

```text
YYYYMMDD_<research_area>_<short_name>.md
YYYYMMDD_<research_area>_<short_name>.csv
```

示例：

```text
20260720_sink_pgvector_writeback.md
20260720_scheduling_bounded_inflight.md
20260720_batching_partition_ablation.md
```

## 记录要求

- 明确对应研究内容。
- 明确 baseline 和优化方案。
- 明确运行命令、参数、CSV 和日志。
- 明确结论边界，不把局部调优写成完整论文贡献。
- 如需图表，放入 `figures/` 并在结果报告中引用。
