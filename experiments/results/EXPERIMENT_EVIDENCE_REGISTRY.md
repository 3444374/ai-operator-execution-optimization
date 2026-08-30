# 实验与机制证据台账

更新日期：2026-08-30

文档角色：本文只回答机制是否实现、通过何种验证以及是否已有真实 GPU 性能证据；它不定义架构、
接口或下一步顺序。具体数字和逐次运行证据仍以各结果目录的 `README.md`、`manifest.json` 和 CSV 为准。
当前证据支持 `REL_18_3` extension recording `SemMap/SemFilter`、同步单在途 UDS、shared runtime、neutral
provider port、planner-owned 最小 recording plan spec 和公共 compatibility suite 的功能与资源生命周期；
不支持包含 instruction/parser/model policy 的完整 `SemanticPlanSpec`、真实模型
语义、第二 physical path、bounded async 或性能优化已经完成。当前顺序看工程计划和
`experiments/plans/experiment_status_and_gaps.md`，既有文本、图像和 SAOR 条目继续保持外部物理执行身份。

## 1. 证据等级

| 等级 | 含义 | 可以声称 | 不可以声称 |
|---|---|---|---|
| 设计预留 | 只有接口、设计或后续计划 | 架构允许扩展 | 机制已经可用或有效 |
| 功能测试 | 有单元测试或纯控制器测试 | 局部语义符合预期 | 端到端可运行或性能更好 |
| 契约门禁 | 真实组件小规模链路通过，或执行路径集成测试通过 | 数据、身份、约束和指标链路正确 | 小样本性能排序可泛化 |
| 真实 GPU 筛选 | 有真实 PostgreSQL/Daft/Ray/vLLM/GPU 对照 | 在给定设置中观察到差异 | 跨规模、跨模型或跨硬件泛化 |
| 重复或留出验证 | 有随机顺序重复、跨到达率或留出规模验证 | 当前实验范围内的机制判断 | 未测试环境中的普遍结论 |

代码测试和性能证据必须分开记录。通过单元测试仅说明实现符合约定，不等于吞吐、尾延迟、能耗或 MFU 得到改善。

## 2. 机制完成度与当前结论

| 机制 | 代码与测试入口 | 真实结果 | 当前证据与结论 |
|---|---|---|---|
| PostgreSQL planner-visible `SemMap` / exact `SemFilter` | `code/postgres/semloom_pg/`；12 项静态 contract、8 项 Python protocol、PGXS SQL regression 与 TAP lifecycle/fault tests | 双 RTX 4090 服务器隔离源码构建 `REL_18_3`（upstream `62d6c7d3…`）；提交 `e89060a7` 以 `-Werror` 无警告 build、regression 1/1、TAP 193/193、Python/static 20/20 通过 | 功能测试：两个 deterministic reference carriers 已验证 ordinary child、Filter 三值/NULL/cardinality、`LIMIT` 前 placement、preload、prepared/generic plan invalidation、RLS/权限、snapshot/savepoint、cancel/recovery、多 backend 与 direct `INSERT ... SELECT`。plain EXPLAIN、LIMIT 0、zero-row child 与 NULL-only 执行不连接 provider。in-process/UDS adapters 对 Map/Filter rows 和归一化 EXPLAIN 一致；UDS fault tests 保持既有 Unicode、断连、digest、取消、编码、整数、NUL、SQLSTATE 和脱敏消息。尚无模型/GPU 性能证据，rescan/EPQ/parallel、join/aggregate 和更宽 query shape 未支持。 |
| PostgreSQL-private plan/runtime + neutral provider seam | `code/postgres/semloom_pg/src/{sem_plan_spec,recording_contract,pg_semantic_runtime,sem_pump,sem_operator_machine,sem_map_machine,sem_filter_machine,ai_provider_port,provider,recording_provider,uds_provider,wire_v2}.{h,c}`、`gateway/` 与上述 contract/TAP | 精确 18.3 产物 ID `tap-run-e89060a7` 中 `semloom_pg.so` SHA-256 为 `a2fc37c372ff0bd892e1e75e3a404d7688d85291e6ad13151e786ad7cdeb4ec0`。仓库外 `resource-run-e89060a7/resource-smoke.txt`：Map 2,000×100,000-byte 输入产生 200,018,000 字节，RSS 起始/峰值/结束 21,340/22,172/22,172 KiB，FD 43/43/41；Filter 20,000 行含 15,000 个非 NULL task、输出 5,000 行，RSS 22,172/22,204/22,204 KiB，FD 43/43/41。历史 `d3a22dcf` shared runtime、`0b9948ee` hardening 与 `d08eda38` seam/resource 结果继续绑定各自提交 | planner 把当前 recording identity/algorithm/role 编码为版本化 named plan fields，executor 严格校验并在唯一位置映射为 `AiOpenSpec`；input column 不进入 semantic digest。`AiProviderError` 不含 transport operation，runtime 只按 neutral code 映射 SQLSTATE，adapter detail 为本地定长脱敏数据。当前 smoke 未观察到累计 payload 近似线性增长或 FD 泄漏，但不是性能结论。完整 instruction/parser/model plan、accepted-prefix、多在途/乱序 completion、direct HTTP 与 SemLoom adapter 尚未实现。 |
| extension/core semantic carrier | 条件性 `code/postgres/pg18_core_patch/`；当前不存在 | 无 | 尚未完成 plan identity、prepared-plan、hook coexistence 与 LOTUS/Cortex alternatives 反例审查；不能声称 core 必须或无需修改。 |
| IMLane batch-placement profile / Kalypso reference direction | gateway/SemLoom 后续实现；当前不存在 | 无 | database-batch placement 在数据库资格后验证；lineage/prefix lease 与 KV-aware execution 仅作参考，未进入当前排期。 |
| `SemFilter` 关系语义 | `sem_filter_path.c`、`sem_filter_machine.c`、公共 runtime 与 PGXS/TAP | `d3a22dcf` exact-18.3 functional qualification；无模型/GPU 结果 | exact reference path 已验证 TRUE/FALSE/UNKNOWN、SQL NULL、ordinary predicate、tuple identity、`LIMIT` 前 keep/drop、invalid completion、savepoint/cancel/UDS recovery 和 adapter parity。第二 proxy/oracle path、近似质量 policy、真实模型与异步乱序 completion 尚未实现。 |
| LOTUS v1.2.4 compatibility/native baseline | companion extension profile、`code/tests/compatibility/lotus_v124/` fixtures 与独立 native runner；当前不存在 | 无 | 只保留为后置兼容和完整系统 baseline，messages/output/error parity 尚未实现且不阻塞核心。 |
| 固定行 batching | `code/src/scheduling/organization/batching.py`、profiler 与 baseline tests | `local_vllm_qwen15b_baseline/` | 真实 GPU baseline；用于和计算量感知组织方式比较。 |
| Sequential token-budget | `code/src/scheduling/organization/{batching,token_budget}.py`、对应 tests | baseline、joint、BFD 与 row-cap 系列；`rc1_data_organization/`（07-31 系统重测） | 已重复验证；当前数据组织默认，必须同时满足 token budget 和 row cap。**07-31 干净平台**：2-ep/4-ep 均属第一梯队（保 prefix 局部性，ratio 0.13），4-ep KV 饱和 regime 下**最稳**（50k，命中 0.47–0.48，SLO 17%）。 |
| Length-align | batching 实现与测试 | local baseline 早期消融；`rc1_data_organization/`（07-31 系统重测，2-ep+4-ep） | 早期仅初筛。**07-31 干净平台**：2-ep（无 KV 压力）length_align 已是最慢（50.3k vs fixed 56.3k，命中最低 0.60——按长度排序破坏 prefix 局部性）；4-ep（KV 饱和）进一步崩到 39.4k、命中 0.06。**cache-ON 下 length-align 的 HOL 收益被丢掉的 cache 复用抵消**，regime-dependent 不稳定收益。 |
| Prefix-aware | batching 实现与语义测试 | `prefix_aware_batching_20260726/`、`prefix_cache_data_org_20260730/`、`prefix_cache_routing_req_20260730/`、`prefix_routing_agent_20260730/`、`prefix_routing_concentrated_20260730/`、`prefix_cache_routing_4ep_1.5b_20260731/`、`rc1_prefix_routing/kv_budget_sweep_20260731/` | cache-off 0/30/70/100% 无稳定收益；cache-on 2-ep/7B batching/routing 跨数据集吞吐均中性（\|Δ\|<2%）。4-ep/1.5B routing +5.9% 跨门禁，但 matched-KV 隔离显示 2-ep/1.5B 在 gpu_mem_util 0.3–0.9 均中性（−0.1%～+1.0%），因此当前证据更支持 **endpoint consolidation** 是信号显现的驱动因素，而不是单纯 per-endpoint KV 大小或“cache 压力开关”；4-ep 饱和深度仍是残余混淆。agent-trace 的 P50/SLO 信号属于不同 workload，不能合并成统一 cache-pressure 结论。文本 prefix 遗留实验已 parked。 |
| BFD、output-aware、row-cap-first | batching 与 cost-mode tests | output-aware BFD、row-cap-aware packing 全系列；`rc1_data_organization/`（07-31 系统重测） | 512 行有局部信号，1024 行未泛化且 SLO 明显恶化；不采用为默认，只保留可复用设计点。**07-31 干净平台**：best_fit 在 2-ep 命中最高（0.76）；row_cap 命中 0.68（第 4/5）。两者在 4-ep KV 饱和时**一起崩最惨**（命中 0.07、SLO 60%、TTFT ~1.1s）——重排序装箱打散 prefix 组（ratio 0.03）。regime-dependent：仅适合无压力 regime。 |
| Arrival replay 与 request lifecycle | lifecycle、runner 和 trace tests | `request_lifecycle_gate_20260725/` 及 flush 系列 | exactly-once、request→submission、arrival/flush/complete 时间链已闭环。 |
| Per-endpoint active-work admission | active-work credit、least-work/least-queued routing 与 profiler tests | `dual_gpu_active_work_saturation_20260729/` | 双 4090 八档、每档三次 formal。65K 达到最大吞吐 97.80%，下一档仅 +0.92%；按预注册规则选为最小饱和点。98K→131K 吞吐持平且 P99/SLO 更差。 |
| Static credit workload-existence audit | `summarize_static_credit_workload_surface.py` 与等价臂门禁模板 | `static_credit_prompt_length_screen_20260730/` | Short/long 48/48 成功，但 urllib/no-token-ID、非 factorial、short 无准入压力等价臂分裂 48.5%，且均值/中位数选择冲突；证据等级为真实 GPU screening，动态 GO/NO-GO=`inconclusive`。 |
| Request-level continuous replenishment | request 粒度 replay、逐请求 credit release、Ray adapter 与 trace tests | `dual_gpu_request_replay_20260728/` | 双 4090 三次重复已跑通。等名义工作量的 request K48 与 batch K16 吞吐不可分辨；K64 吞吐最高但 offered work 高约 33%，且 P99 更差。机制可用，但独立性能增量尚未证明。 |
| Complete-row service quantum | `slice_service_quanta`、offline/replay expansion、completion/credit trace tests | `dual_gpu_service_quantum_20260729/` | 固定 65K work 的三次重复完成；512/request 将 credit-held 降约 16%，但四档 quantum 吞吐相对 batch 仅 -0.03%～+0.54%，request +1.75%。固定 quantum 不晋升，request 保留作精确控制基础。 |
| Static K_max | admission 与 profiler tests | local baseline 干扰实验、joint search | shared-vLLM 下 `K_max=8` 有必要性证据；当前静态安全基线。 |
| Immediate/fixed/adaptive flush | scheduler、flush policy 与 trace tests | accelerated/window/randomized/cross-rate/2048/joint/shared-vLLM | Adaptive 稳定优于 fixed-25，但未优于 fixed-50；shared-vLLM 下约 89.4% 决策选择 50ms，也没有稳定增量。当前默认 fixed 50ms。 |
| SLO-aware EWMA flush | `SloAwareEwmaFlush`、feedback/provider、arrival-gap completion 与 trace tests | `dual_gpu_slo_ewma_flush_formal_20260729/` | 双 4090 高压/arrival-limited 各三次 formal；相对 fixed-50 吞吐 -0.52%/+0.10%，P99 -0.94%/-0.49%，30s SLO 全部零违约。未过 5% 晋升门槛，fixed-50 保持默认。 |
| AIMD、EWMA、PID admission | `code/src/scheduling/submission_control/{adaptive,pid}.py` 及测试 | `adaptive_admission_controller_20260726/`、`shared_vllm_adaptive_admission_20260726/`、`hol_age_diagnostic_512_20260728/` | 单作业与 shared-vLLM 双作业重复均完成。AIMD 在共享服务中 0 次 decrease、窗口均值 15.953；相对 static K16 前台与吞吐均略差，当前没有动态反馈增量证据。hol_age 诊断进一步确认：换 HOL-age 信号 + request-level replenish 仍未击败 static K16（SLO-goodput 反而 −56%/−70%/−81%），锁定刻画型 framing。 |
| UCB 多臂老虎机 | `code/src/scheduling/submission_control/ucb.py`、对应 scheduling tests | 无端到端结果 | 有有限 K_max action set、探索/利用和 SLO reward 的纯控制器测试；尚未接入 profiler。必须先封闭 epoch 内请求完成与 reward 归因，避免把跨 epoch completion 记到错误 arm。 |
| SAOR capacity-only development | `scheduling/submission_control/saor.py`、`scheduling/runtime/saor_capacity.py`、shared-vLLM adapter 与 SAOR tests | `saor_capacity_development_20260811/` | 纯策略、非因果 paired replay 与真实 capacity actuation 已接。四臂各一次：SAOR 相对 K128 +4.36%，但相对 K160 +0.52%、相对 threshold −1.46%，Jain 最低；未过晋级门。完整 ordered release/fairness/stage queues 未执行，不能称完整 SAOR 或定理已验证。 |
| SAOR fixed-envelope active-set release | named shared-credit runtime、direct/project 统一 runner、matched solo、lifecycle/mechanism fail-closed 汇总 | `saor_active_set_release_formal_20260812_69affc7e/` | 40/40、0 incident、exactly-once；resolution-aware v2 在服务器完整 artifact 上重汇总为 passed，credit mechanism effective 12/12；原 failed 文件保留审计。SAOR 12,393 tok/s、fg P99 50.3s，为 credit 臂 fg 最好；static fg P99 29.2s/SLO 0%，故 SAOR 未晋级。 |
| Foreground strict-priority release-only upper bound | shared-credit `strict_priority`、Job lifecycle close、priority evidence、三臂 readiness/summary | `saor_priority_reachability_smoke_20260812_91ffcaa/` | 两轮 rehearsal-only：11,791 tok/s、fg JCT/P99 20.04/14.27s、SLO 0%，相对 SAOR fg P99 −73.02%。证明 non-preemptive release ordering 可达，不是 formal/proposed；hard priority 仍缺 anti-starvation 和 bulk lag/SLO guard。 |
| SAOR bounded-priority development | `saor_bounded_priority` selector、actual-work debt/reclaim barrier、lossless event ledger、双轮 fail-closed 汇总 | `state_aware_work_unit/saor_bounded_priority_gate_20260813/` | 双轮四臂 GPU gate 无 cap 晋级：0.25K 第 2 轮 debt-recovery=0；两个 cap fg P99 49–56s、SLO violation 85%–95%。event ledger 证明可见前台 head 都获 priority，但 per-Job 同步 pull 只暴露单 head，完整 ready backlog 不可见；先修 observation contract，不启动 formal/4-Job/reservation。 |
| SAOR bounded-ready development | `saor_bounded_ready` 有界 concrete-request 预注册、跨 submission/request trace lifecycle join、actor-side register/grant event ledger | `state_aware_work_unit/saor_bounded_ready_gate_20260813/` | 两个独立 rehearsal root、8/8 cell、0 incident；0.125K 两轮全过 correctness/mechanism/foreground/bulk/efficiency 门，均值 12,361 tok/s、fg P99 17.87s、fg SLO 0%、bulk SLO 0.662，注册 formal candidate。0.25K 两轮 bulk SLO 0.752/0.744 越界，拒绝。尚无 formal repeats、4-Job、reservation 或理论公平结论。 |
| SAOR matched-ready selector attribution | completion-accounted registered-ready lag、bounded-ready FIFO/DRR/VTC-style/strict-priority/guarded-debt 同 observation 双轮汇总 | `state_aware_work_unit/saor_matched_ready_selector_rehearsal_20260813/` | 12/12 cell、0 incident、12,288 requests exactly-once。DRR/VTC-style 约 12.90K tok/s、fg P99 27.23/26.16s、fg SLO 0；SAOR 12.28K tok/s、fg P99 17.85s、lag P95 57.7K work。相对 VTC-style，SAOR 用约 4.8% 吞吐、5.2% bulk JCT 和 22.7% longest-no-service 代价换取 31.8% fg P99 与 11.7% lag 改善。它是 development 中的非支配折中点，不是已授权 formal 的 selector winner；native baseline count=0。 |
| SAOR ready-observation bridge | frozen-static/single-head shared FIFO/bounded-ready FIFO 三臂配置、readiness 与双轮 raw-effect 汇总 | `state_aware_work_unit/saor_ready_observation_bridge_rehearsal_20260813/` | 6/6 cell、0 incident、6,144 requests exactly-once。共享容量双轮均值 tok/s +25.96%、fg P99 +99.17%；bounded-ready 在同 FIFO 下 tok/s +7.30%、fg P99 −33.62%，但 fg SLO violation 仍约 39.7%。证明 static isolation、shared efficiency 与 ready exposure 是不同效应；不授权 formal，native baseline count=0。 |
| SAOR 五臂共同观测系统对照 | 五臂 runner、T0--T4、严格透传 observation-only gateway、archive/identity/trace validator | `state_aware_work_unit/saor_native_system_matched_gateway_rehearsal_20260821/` | `93271012` 的 5/5 rehearsal cell、exactly-once 与独立 archive validation 通过；五臂 request P99/SLO/Jain/lag/no-service 已可比较。单次 SAOR 相对同 executor static 提高吞吐、降低 group JCT，但 P99、Jain、lag 和最长无服务变差，只支持效率—尾延迟—公平权衡。formal 未运行，0s/5s isolation 只有 partial。 |
| Actor pool 分池与 endpoint routing | `code/src/scheduling/runtime/ray_adapter.py`、profiler/trace 与契约测试 | `dual_gpu_actor_pool_shape_20260729/` | 固定 65K work、256 slots 和 0.5 CPU/endpoint 的三次重复已完成；2×128/4×64 相对 1×256 仅 +2.00%/+0.75%，未达 5% 晋升门槛。当前同构单 job 保留 1×256；多 job 分池仍待验证。 |
| Shared-vLLM endpoint credit 与 DRR | `code/src/experiments/shared_vllm/`、named shared-credit runtime 与测试 | `dual_gpu_shared_vllm_formal_20260729_1135/` | 双 4090 36/36 group run、0 incident；全局 256 request/65,536 work 安全与归零通过。2-job 无增量；4-job 聚合吞吐 +9.57%、max P99 -22.52%，但逐 repeat 不稳定，暂作高竞争条件性候选。 |
| Batching × submission 联合搜索 | scenario runner 与汇总工具 | `joint_batching_submission_512_20260726/` | 18 单元筛选和候选重复完成；当前单 GPU 下联合候选未显著优于独立拼接。 |
| vLLM CUDA Graph | 服务配置与相同 profiler 路径 | `vllm_cuda_graph_512_20260726/` | 重复真实对照显著优于 eager；作为本地部署 baseline，不作为上游调度研究贡献。 |
| 算子代价估计 | `code/src/planning/costs/`、`code/scripts/analysis/estimate_operator_cost.py`、context-LOO driver 及测试 | `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/` 等 | cache-on 有效集含 429 formal、20 context × 4 candidate；Hybrid pooled/macro/max regret 1.67%/2.90%/14.72%，candidate pairwise 0.808。max regret 仅以 0.28 pp 通过 15% 门，属于 marginal pass，仍需跨时间/workload/硬件校准。 |
| 多模态 cost adapter / image path | 中性 `work_units` + lazy Daft image source + typed CLIP tensor actor、staged descriptor、真实 static HSE broker 与 observe-only snapshot 已实现；两个 CLIP profiler；`code/scripts/data/import_coco_images.py` | `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`、`image_clip_preprocess_variants_20260801/`、`experiments/results/image_ai_embed_operator_formal_20260803/`、`opening_image_native_fourjob_formal_20260810/`、`opening_image_project_fourjob_observe_only_formal_20260810/` | 静态 operator-E2E、matched-resource、原生多 Job 干扰和 Project observe-only 证据已完成；尚未完成 HSE static GPU 非劣、stage/CE5 在线动作、小规模 pgvector 检索质量闭环和跨 workload/硬件验证。vLLM pooling 0.25.1 能力门仍 blocked，不生成性能排名。 |
| 图像原生四 Job 干扰 | Daft built-in/Ray Data vendor-owned single→four-job runner、immutable 1+3 manifest 与 fail-closed 汇总 | `opening_image_native_fourjob_formal_20260810/` | 40/40 runs、30 formal group 通过；两条路径均出现非均匀 Job slowdown。只作各路径内部 normalized 干扰和状态证据，不作跨框架绝对排名。 |
| 图像 staged descriptor / observe-only snapshot | production staged descriptor、freshness/signature 检查与 Project control trace | `opening_image_project_fourjob_observe_only_formal_20260810/` | 24/24 group、99K formal rows exactly-once；3,114 个 snapshot 全 fresh、构建均值 0.141 ms。static/proposed group JCT 差 0.98%，只证明观测接入，不证明 state-aware 动作收益。 |
| 多 endpoint/多 GPU 调度 | endpoint/pool 配置与 routing contract | request replay、active-work saturation、Actor Pool、Shared-vLLM formal 与 5s 两 Job 干扰实验 | 真实双 4090 容量、admission、worker identity、equal-weight 1/2/4-job 和 5s staggered 两 Job 证据已建立；尚不能声称 weighted/held-out、多 Job 路由增量或故障迁移有效。 |
| 两 Job guaranteed-overlap 干扰 | online exact-short、Project eager full/half/static/shared、原生两 Job runner与统一逐请求时间汇总器 | `opening_multijob_interference_20260809/` | 5s offset下项目因果链与三条原生观察已闭合。online下shared提aggregate但伤short/Jain；eager下quota-only+59.00%、matched static/shared竞争+58.77%/+28.90%，shared相对static short JCT−48.94%、aggregate+31.85%。证明arrival-regime dependence与idle borrowing，不证明动态普遍胜出。 |
| 文本四 Job quota/competition/shared 因果分解 | Project full/quarter single + static/shared multi；Daft Native/Ray、Ray Data 各自 single→four-job | `opening_fourjob_interference_20260809/` | 按三次 formal 均值，shared 相对 static 使 group throughput +8.68%、四个 Job JCT 全部改善，是效率/JCT 子向量的 baseline-relative empirical Pareto；但 raw-work Jain 0.960→0.923，long1/2 的 shared/quarter-solo=1.29/1.14，未达到经验性保留份额非劣。compact CSV 可算三反事实，不能补 event-level service lag/starvation；不称完整 Pareto 或 DRF/Themis/VTC 理论性质。 |
| Project short all-at-t0 计时诊断 | 同 short manifest、冻结 K128/W65K/8×32 actor 的 Project 1+3 与 T0–T4 raw 汇总 | `opening_project_short_all_at_t0_diagnostic_20260809/` | T3 model-request window 11.354s vs Daft Native11.059s，service tokens/s与MFU只差约−2.5%；排除“Project模型请求路径慢6.4×”。Daft缺外层T0，完整系统E2E不排名；short不要求60s。 |
| 文本原生框架入口 | official/native adapter、单 job matrix 与 native multi-job runner tests | `opening_text_native_gate_20260808/` | bounded、Daft Native/Ray、Ray Data 的 6/6 capability/calibration gate 通过；只证明入口、provenance、exactly-once 和最小 Ray Data 筛选可用，n=1 不作性能排名。 |
| 文本原生单 job 正式观察 | bounded C128、Daft Native/Ray、Ray Data official graph + vLLM/GPU time series | `opening_text_native_single_job_formal_20260808/` | 16/16 cells、12 formal，吞吐/JCT CV<0.7%。Daft waiting mean=783/742、KV max≈1；Ray Data running=17.3、MFU=0.112；证明压力形态不同，不证明项目方法胜出。 |
| ShareGPT bounded 饱和校准 | bounded HTTP C32/C64/C128/C256 + vLLM/GPU time series | `opening_bounded_saturation_calibration_20260808/` | 8/8 cells 通过；formal C128 达 C256 已测峰值 98.22%，冻结为正式对照。C32 仅 52.07%，高 GPU util 不等于喂饱；C256 明显过量排队。 |
| Ray task/actor 与 vLLM capacity 调优 | 执行接口、参数字段和实验设计 | CUDA Graph、双 GPU request replay、active-work、Actor Pool 与 service quantum | 已固定 vLLM 8192 batched-token/256 seq capacity，并标定上游 65K work 饱和点；增加 actor 或固定 quantum 均未过 5% 门槛。 |

## 3. 主要结果目录（非穷举）

本节和同目录 `README.md` 都是高频结果的非穷举导航；未列出的结果直接查看目录及其 README。
机制是否实现、运行和可声明以本文件 §2 和对应原始结果为准，避免两个目录清单因新增结果产生
无声分叉。

| 结果目录 | 角色 | 当前状态或结论 |
|---|---|---|
| `saor_capacity_development_20260811/` | 两档 capacity-only SAOR 与 frozen lower/upper、legacy threshold 的开发门 | 4/4 arm、5,266 requests/arm、0 incident；真实动作安全，但一次顺序运行、server dirty-worktree provenance 和旧 manifest 漏字段使其只作 development。当前分支 `not-promoted`。 |
| `saor_active_set_release_formal_20260812_69affc7e/` | fixed-envelope 2-Job active-set killer matrix | 40/40、0 incident、exactly-once；原始 failed validation 保留，resolution-aware v2 完整 validation passed、effective 12/12。SAOR 未越过 static，状态为 `formal-valid/not-promoted`。 |
| `saor_priority_reachability_smoke_20260812_91ffcaa/` | strict-priority release-only upper bound | 两轮短测均过 lifecycle/mechanism，foreground P99 14.27s、SLO 0%；development smoke only，不作正式排名。 |
| `state_aware_work_unit/saor_bounded_priority_gate_20260813/` | bounded-priority 双轮开发门 | Round 1 clean；Round 2 的 0.25K 机制门 fail-closed，汇总结论 `diagnostic_only`。所有请求正确完成且 GPU 饱和，负结果不是欠供给；ready-set 可见性断点阻塞 formal。 |
| `state_aware_work_unit/saor_bounded_ready_gate_20260813/` | bounded-ready 双轮开发门 | 两轮均 clean，汇总结论 `formal_registration_candidate`；只冻结 0.125K，0.25K 因 bulk SLO 两轮越界拒绝。development rehearsal 不是正式排名；formal/4-Job/reservation 仍未运行。 |
| `opening_text_native_gate_20260808/` | 开题文本原生框架 capability gate 与 Ray Data C4/C8/C16 最小筛选 | 6/6 gate 通过；冻结单次 measured peak C8/B16 供正式矩阵。只有一次 256-row gate，不支持框架正式性能排名。 |
| `opening_text_native_single_job_formal_20260808/` | ShareGPT 原生单 job 1+3 同环境正式观察 | bounded/Daft Native/Daft Ray/Ray Data=17,800/17,286/16,747/3,551 tok/s；状态指纹稳定。官方 graph 外部现象可报告，内部归因与项目优势不可声称。 |
| `opening_multijob_interference_20260809/` | short/long guaranteed-overlap 的 online/eager、single/multi、project/native 统一证据与 request-time 分解 | online quota-only≈0但eager quota-only+59.00%；long同时影响service与上游pending。Project/原生只作各轨normalized影响，完整raw留服务器，Git保存紧凑派生数据。 |
| `opening_image_native_fourjob_formal_20260810/` | Daft built-in/Ray Data 图像 single→four-job 正式观察 | 40/40 runs、30 formal group；exactly-once/overlap 通过。只报告各系统内部 Job slowdown，不作跨框架绝对排名。 |
| `opening_image_project_fourjob_observe_only_formal_20260810/` | Project staged descriptor + observe-only snapshot 正式门禁 | 24/24 group、99K formal rows exactly-once；snapshot 100% fresh、构建均值 0.141 ms，总体 JCT 近似中性。 |
| `opening_project_short_all_at_t0_diagnostic_20260809/` | Project eager single 的三次 formal、统一timer合同、Daft raw T3/T4对齐 | T0=14.957s但Daft T0缺失；可比短诊断T3为11.354s vs11.059s，服务吞吐/MFU差约2.5%，不扩成60s容量排名。 |
| `opening_bounded_saturation_calibration_20260808/` | ShareGPT bounded HTTP 容量扫描与 C128 选择 | C32/C64/C128/C256 formal tok/s=9,455/14,058/17,834/18,158；C128 实测达到 C256 的 98.22%，是第一个满足预先规定 97% 选择条件的并发点，C256 已出现过量排队。单次 formal 只用于校准，不支持统计排名。 |
| `opening_database_e2e_text_refeed_20260808/` | 开题统一 database-E2E 文本三臂 replacement：SQuAD 均匀 + ShareGPT 异质 | 24/24 单元与 18 formal 的完成性、质量、source/sink、identity 和稳定性检查通过。SQuAD 因外层计时实现不同不作不到 1% 的性能排名；ShareGPT C32 对照欠供给，DuckDB 4,921/6,144 行 cap 语义失败，均不支持方法性能排名。旧 `_20260807/` 仅保留为 failed-feeding 诊断。 |
| `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/` | cache-on 双 4090 代价估计正式结果 | 429 formal、20 context × 4 candidate；Hybrid pooled/macro/max regret 1.67%/2.90%/14.72%，pairwise 0.808；最坏 regret 为边界通过。 |
| `hol_age_diagnostic_512_20260728/` | HOL-age 诊断实验实际运行（6 臂 × 3 formal，24/24 ok） | **负向**：aimd_hol/replenish/aimd_hol_replenish SLO-goodput（6.78/4.62/2.91）远低于 static_k16（15.27），P99 恶化 4–13×。「诊断优先」假设被否定——补 HOL-age 信号 + request-level replenish 后动态稳态仍不优于最佳静态。 |
| `hol_age_diagnostic_512_20260727/` | HOL-age 诊断预注册设计 + 配置（设置 A） | 07-27 本机无 GPU 未运行；实际执行在 `_20260728/`。含预注册判据与 6 臂设计。 |
| `oceanbase_b1_gate_20260731/` | OceanBase B1 baseline 门禁 #1 验证 + 部署阻塞 | 门禁通过（CE 4.5.0 含 `AI_COMPLETE`/`DBMS_AI_SERVICE`，静态确证）；当前 AutoDL 容器 observer init step 4/18 clog errcode -9100 自杀（seccomp 等，容器内不可修）。降为待部署，复跑需特权容器/VM。 |
| `prefix_cache_routing_req_20260730/` | cache-on prefix-affinity routing 消融（request 粒度，3 臂） | 12/12 ok；纯 routing -0.1%、length-align +1.9%（均 <5% 门禁）。2-ep/7B 下 prefix 方向收口。 |
| `prefix_cache_routing_4ep_1.5b_20260731/` | cache-on prefix-affinity routing 消融（4×Qwen2.5-1.5B，request 粒度，2 臂；含 4-endpoint 调整与 stale-Ray 事故记录） | 8/8 ok、0 incident；prefix_affinity 相对 least_queued **+5.9%**（46,943 vs 44,317 tok/s，raw 不重叠、CV≤0.9%）、SLO −6.3pp、P95 −3.15s，**跨过 5% 门禁**。⚠️ 混淆（1.5B×4-ep×更小 KV）、过饱和 regime（SLO 违约 25–31%），需隔离消融后正式晋级。 |
| `rc1_prefix_routing/kv_budget_sweep_20260731/` | KV-budget × prefix_affinity 隔离扫描（2-ep/1.5B，1 endpoint/GPU，扫 gpu_mem_util 0.3–0.9；新存储约定 raw/+README） | 2-ep 全 KV 范围 prefix_affinity **中性**（Δ ∈ [−0.1%, +1.0%]，含 13–15% SLO 抖动点）；matched-KV（~7GB）：2-ep/0.45 −0.1% vs 4-ep/0.43 +5.9% → **endpoint 数（consolidation）是驱动，非 per-endpoint KV 大小**。跨引擎共享池价值定位在多 endpoint consolidation，不在小 KV。 |
| `rc1_data_organization/` | RC1 数据组织系统重测（5 策略 × {2-ep/0.9, 4-ep/0.43}, 1.5B, multiturn, cache-ON, P0 指标 prefix_hit/TTFT；新存储约定 raw/+README；**取代 07-25/26 gropy；07-18/19 保留作历史动机参照**） | 20+20 formal、CV 1–6%、GPU 79–90%。**regime-dependent 闭合**：2-ep（KV max 7–10% 无压力）5 策略 E2E 50–56k 紧凑、fixed≈seq>bestfit>rowcap>lenalign；4-ep（KV max **98–100% 饱和**）分化 39–50k、**排名反转为 seq>fixed>>rowcap≈bestfit>lenalign**。机制 smoking gun `prefix_group_ratio`：重排序类 organizer（length_align/best_fit/row_cap）打散 prefix 组（0.03）→ 4-ep 命中从 0.60–0.76 **崩到 0.06–0.07** → prefill 重算激增 → TTFT 翻倍（0.2–0.3s→0.6–1.1s）、best_fit/row_cap SLO 60%；保序类 fixed/sequential ratio 0.13–0.29、命中 0.47–0.48。consolidation 是惩罚（4-ep −10～−26% + 能耗 +40%）。**与 #28 routing / KV-sweep 闭环**：上游策略价值在 4-ep 饱和 regime 才显现。✅ feeding 门禁已补（batched bounded，gate 放宽 ≥2 endpoint）：2-ep 真上限 **79,488** tok/s（策略 63–71%，缺口=active-work 准入节流 W65536 把 inflight 压到 4–22 vs K256，**非饿死**；`model_wall≈operator_wall` 无 pipeline 瓶颈）；4-ep bounded **24,733 病态**（unthrottled thrash 小 KV，策略 160–202% 超过）→ **准入控制是吞吐杠杆、效应随 regime 反向**（2-ep 压住上限可放开 W、4-ep 防 thrash 应保留）。 |
| `prefix_cache_data_org_20260730/` | cache-on prefix-aware batching 消融（batch 粒度，3 臂）+ routing 报告交叉引用 | 12/12 ok；上游 batching 顺序 within 1.2% 中性。vLLM APC 覆盖上游 prefix 组织。 |
| `prefix_routing_agent_20260730/` | cache-on prefix-affinity routing 跨数据集消融（agent-trace，2-ep/7B，3 臂）+ 跨数据集合并分析 | 12/12 ok、0 incident；吞吐三臂 \|Δ\|<2% 中性，但 pala P50 −7.8%/SLO −3.8pp/goodput +17%（高 cache 压力，过饱和区间，吞吐 −1.9% 未过门禁）。 |
| `prefix_routing_concentrated_20260730/` | cache-on prefix-affinity routing 跨数据集消融（concentrated ShareGPT，2-ep/7B，3 臂） | 12/12 ok、0 incident；吞吐 \|Δ\|<1.2% 中性，pala 信号弱（cache 压力低于 agent）。自包含简表 + 指向 agent 报告的合并分析。 |
| `static_credit_prompt_length_screen_20260730/` | Short/long prompt 下 request K 与 active-work 的存在性筛选 | 48/48 成功；long W65K 有稳定正信号，但 short 未绑定等价臂高方差、urllib/no-token-ID 与非 factorial 设计使正式判决阻塞。保留为机制审计，先重跑 async 等价臂 gate。 |
| `dual_gpu_shared_vllm_formal_20260729_1135/` | 1/2/4-job independent/static/shared-DRR 核心矩阵 | 36/36、0 incident；容量安全与公平门槛通过。2-job 无收益，4-job 聚合过门槛但重复异质，需 held-out 复验。 |
| `dual_gpu_slo_ewma_flush_formal_20260729/` | high/arrival-limited 下 fixed、queue-adaptive 与 SLO-EWMA 对照 | 24/24 成功；exactly-once 与 completion-lag 审计通过。25–50ms 控制窗口相对 5.6–17.4s P99 缺少一阶杠杆，SLO-EWMA 不晋升。 |
| `dual_gpu_service_quantum_20260729/` | 固定 work/pool 的 batch、四档 complete-row quantum 与 request 对照 | 24/24 成功；HOL/credit barrier 确实缩短，但没有转化为超过 5% 的稳态吞吐或 SLO 收益。 |
| `dual_gpu_actor_pool_shape_20260729/` | 固定 work/slots/CPU 的 1×256/2×128/4×64 Ray actor 拓扑对照 | 12/12 成功；多 actor 未达到预注册晋升门槛，当前保留 1×256，不能外推到多 job 隔离与故障迁移。 |
| `dual_gpu_active_work_saturation_20260729/` | 八档 request-level per-endpoint active-work 扩展饱和曲线 | 32/32 成功；65K 是预注册最小饱和点，98K/131K 不再增加吞吐且尾延迟更差。 |
| `dual_gpu_active_work_curve_20260728/` | request-level per-endpoint predicted-token work 的早期五档容量曲线 | 真实双 4090、五档各三次 formal；当时 49K 只是 knee candidate、65K 是最高已测边界。该选择已被 `dual_gpu_active_work_saturation_20260729/` 的八档扩展取代，当前同签名采用预注册最小饱和点 65,536。 |
| `dual_gpu_request_replay_20260728/` | 双 endpoint whole-submission barrier 与 request-level continuous replenishment 对照 | 真实双 4090、每臂三次 formal。global K32≈per-endpoint K16；work-matched request K48≈batch K16。K64 是最高已测吞吐点，但没有隔离 offered-work 增量，不能称为最优或机制胜出。 |
| `shared_vllm_adaptive_admission_20260726/` | Shared-vLLM 前台/后台 static K8、static K16、AIMD 及 adaptive-flush 对照 | 真实单 GPU 三次重复；K8 保护前台，K16 提升后台吞吐。AIMD 饱和至 K16 且无 decrease；adaptive flush 大部分选择 50ms，均无稳定增量。 |
| `adaptive_admission_controller_20260726/` | Static K=8、AIMD、EWMA-AIMD、PID 矩阵及 AIMD vs static K=16 机制对照 | 真实单 GPU 重复；动态策略相对 K=8 的收益来自升至 K≈16，未优于同上限静态策略。 |
| `accelerated_arrival_flush_20260725/` | Immediate/fixed/adaptive flush 首轮真实对照 | 真实 GPU 筛选；旧 adaptive 未形成多行 batch。 |
| `adaptive_flush_cross_rate_20260726/` | 跨到达率筛选 | Fixed-50 最好或等价，adaptive 不具默认资格。 |
| `adaptive_flush_randomized_20260726/` | 变长输出、随机顺序重复 | Fixed-50 与 adaptive 均优于 fixed-25，但二者不可分辨。 |
| `adaptive_flush_window_20260725/` | 双窗口 adaptive 改进 | 512 行有正向候选信号，后续更严格实验收缩了结论。 |
| `joint_batching_submission_512_20260726/` | 数据组织与提交控制联合搜索 | 独立拼接和联合候选不可分辨；fixed-50 最简单。 |
| `local_vllm_qwen15b_baseline/` | 本地固定 batch、token-budget、K_max 等早期基线 | 真实链路基础证据，部分早期消融没有现代 trace 完整度。 |
| `operator_cost_estimation_20260726/` | 已有 profile 的离线二次分析 | 粗粒度成本估计可用，严格 SLO 精度不足。 |
| `output_aware_bfd_1024_20260726/` | BFD 留出规模验证 | 负结果；当前 BFD 不随规模泛化。 |
| `output_aware_bfd_512_20260726/` | 修复前失败审计 | 21/24 计划运行完成后超时；约束不一致，排除性能结论。 |
| `output_aware_bfd_512_v2_20260726/` | 修复后 512 行六单元对照 | BFD trace 有局部候选信号，不能视为配对输出 oracle。 |
| `output_aware_bfd_gate_20260726/` | 修复前 64 行门禁 | 链路数据可审计；row cap 不一致，排除算法性能比较。 |
| `output_aware_bfd_gate_v2_20260726/` | 修复后 64 行门禁 | 约束和 trace 通过；仅基础设施证据。 |
| `prefix_aware_batching_20260726/` | Prefix 比例受控筛选 | Cache-off 时无稳定收益。 |
| `request_lifecycle_gate_20260725/` | 请求生命周期门禁 | 身份、时间和 SLO trace 闭环；不是策略性能证据。 |
| `row_cap_aware_packing_1024_20260726/` | Row-cap-first 留出规模验证 | 吞吐约增 1% 但 SLO violation 大幅增加；默认否决。 |
| `row_cap_aware_packing_512_20260726/` | Row-cap-first 筛选与重复 | 有小幅局部信号；不足以覆盖 1024 行负结果。 |
| `row_cap_aware_packing_gate_20260726/` | Row-cap-first 64 行门禁 | 约束、请求、资源和 MFU 字段通过。 |
| `text_heldout_2048_20260726/` | Flush 策略留出规模验证 | Fixed-50 吞吐与 P99 均略优于 adaptive；持续积压放大尾延迟。 |
| `vllm_cuda_graph_512_20260726/` | Eager 与 CUDA Graph 部署对照 | CUDA Graph 为当前本地 steady-state baseline。 |

台账中已登记的正式目录均有各自的 `README.md`。失败、被替代或门禁目录仍保留，是为了记录事故和排除理由，不应从台账中删除。尚未形成独立报告的原始运行目录不计入该句。

## 4. 统一复现入口

大多数新实验由 seeded scenario runner 执行：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\experiments\run_ai_operator_scenarios.py `
  --config <scenario_config.json> `
  --profiler code\scripts\profiling\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir <run-output-directory> `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`scenario_config.json` 是参数来源；`manifest.json` 的 `completed_runs[].command` 保存每一轮展开后的精确命令，`incidents` 保存失败与恢复记录。早期 baseline 使用独立脚本，按其目录 README 复现。

代价估计使用：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\analysis\estimate_operator_cost.py `
  --input-csv <runs.csv> `
  --output <model.json> `
  --target e2e_s `
  --test-fraction 0.25 `
  --seed 20260726 `
  --alpha 1.0
```

## 5. 一次实验应保留的证据

- 问题、baseline、变量和明确的晋级/否决条件。
- 环境与服务配置，特别是模型、vLLM 版本、CUDA Graph/eager、prefix cache 和 GPU。
- `scenario_config.json`、随机顺序、warm-up 与 formal repeat。
- `runs.csv` 以及 request、submission、flush、control、resource trace。
- PostgreSQL 与 pgvector 版本、vLLM success/token delta、exactly-once 请求审计。
- E2E、request P50/P95/P99、SLO violation、observed tokens/s、GPU 利用率、显存、功耗、能耗和 MFU。
- `summary_long.csv` 或 `comparison_summary.csv` 等绘图友好汇总。
- 事实、推断、待确认和不能声称的内容分开写。

## 6. 尚无证据覆盖的能力（2026-08-30）

以下条目只说明证据缺口，不构成实施顺序：

1. 真实 `SemanticPlanSpec`、同步 exact 真实模型 reference、reference/optimized path identity、AI-work
   cost、quality policy/evidence 与 fallback 尚无实现资格证据。
2. extension/core carrier audit、accepted-prefix、多在途、增量 SemLoom adapters 与 IMLane-like batch
   placement 尚无相应验证；Kalypso-like lineage/prefix lease 仍只是参考方向。
3. 图像 HSE static GPU 非劣、stage/CE5 在线动作和小规模 pgvector 检索质量尚未完成。
4. 五臂共同观测 rehearsal 已完成，但 formal、matched-solo/full-solo isolation 与跨层 scheduler capability
   仍未完成或未获授权。
5. 代价估计仍需独立时间段或新 workload 校准、预测区间和在线决策增量；现有 429-run 结果只支持
   marginal feasibility。
6. 文本 Shared-vLLM held-out、weighted/SLO、UCB reward 归因、多 endpoint 故障迁移和剩余 prefix
   隔离统一为 `parked-conditional`，不阻塞当前主线。
