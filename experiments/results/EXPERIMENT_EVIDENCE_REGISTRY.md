# 实验与机制证据台账

更新日期：2026-07-28

本文是正式方法实验的统一入口，回答三个问题：机制是否已经实现、是否只通过了功能测试、是否已有真实 GPU 性能证据。具体数字和逐次运行证据仍以各结果目录的 `README.md`、`manifest.json` 和 CSV 为准。

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
| 固定行 batching | `code/src/scheduling/batching.py`、profiler 与 baseline tests | `local_vllm_qwen15b_baseline/` | 真实 GPU baseline；用于和计算量感知组织方式比较。 |
| Sequential token-budget | `code/src/scheduling/batching.py`、batching tests | baseline、joint、BFD 与 row-cap 系列 | 已重复验证；当前数据组织默认，必须同时满足 token budget 和 row cap。 |
| Length-align | batching 实现与测试 | local baseline 早期消融 | 有真实初筛但缺独立、受控的正式重复；不能声称稳定收益。 |
| Prefix-aware | batching 实现与语义测试 | `prefix_aware_batching_20260726/` | cache-off 的 0/30/70/100% 受控筛选无稳定收益；默认关闭，启用 prefix cache 后才值得重测。 |
| BFD、output-aware、row-cap-first | batching 与 cost-mode tests | output-aware BFD、row-cap-aware packing 全系列 | 512 行有局部信号，1024 行未泛化且 SLO 明显恶化；不采用为默认，只保留可复用设计点。 |
| Arrival replay 与 request lifecycle | lifecycle、runner 和 trace tests | `request_lifecycle_gate_20260725/` 及 flush 系列 | exactly-once、request→submission、arrival/flush/complete 时间链已闭环。 |
| Per-endpoint active-work admission | active-work credit、least-work/least-queued routing 与 profiler tests | `dual_gpu_active_work_saturation_20260729/` | 双 4090 八档、每档三次 formal。65K 达到最大吞吐 97.80%，下一档仅 +0.92%；按预注册规则选为最小饱和点。98K→131K 吞吐持平且 P99/SLO 更差。 |
| Request-level continuous replenishment | request 粒度 replay、逐请求 credit release、Ray adapter 与 trace tests | `dual_gpu_request_replay_20260728/` | 双 4090 三次重复已跑通。等名义工作量的 request K48 与 batch K16 吞吐不可分辨；K64 吞吐最高但 offered work 高约 33%，且 P99 更差。机制可用，但独立性能增量尚未证明。 |
| Complete-row service quantum | `slice_service_quanta`、offline/replay expansion、completion/credit trace tests | `dual_gpu_service_quantum_20260729/` | 固定 65K work 的三次重复完成；512/request 将 credit-held 降约 16%，但四档 quantum 吞吐相对 batch 仅 -0.03%～+0.54%，request +1.75%。固定 quantum 不晋升，request 保留作精确控制基础。 |
| Static K_max | admission 与 profiler tests | local baseline 干扰实验、joint search | shared-vLLM 下 `K_max=8` 有必要性证据；当前静态安全基线。 |
| Immediate/fixed/adaptive flush | scheduler、flush policy 与 trace tests | accelerated/window/randomized/cross-rate/2048/joint/shared-vLLM | Adaptive 稳定优于 fixed-25，但未优于 fixed-50；shared-vLLM 下约 89.4% 决策选择 50ms，也没有稳定增量。当前默认 fixed 50ms。 |
| AIMD、EWMA、PID admission | `code/src/scheduling/adaptive_admission.py`、`pid_admission.py` 及测试 | `adaptive_admission_controller_20260726/`、`shared_vllm_adaptive_admission_20260726/` | 单作业与 shared-vLLM 双作业重复均完成。AIMD 在共享服务中 0 次 decrease、窗口均值 15.953；相对 static K16 前台与吞吐均略差，当前没有动态反馈增量证据。 |
| UCB 多臂老虎机 | `code/src/scheduling/ucb_admission.py`、`code/tests/test_adaptive_admission.py` | 无端到端结果 | 有有限 K_max action set、探索/利用和 SLO reward 的纯控制器测试；尚未接入 profiler。必须先封闭 epoch 内请求完成与 reward 归因，避免把跨 epoch completion 记到错误 arm。 |
| Actor pool 分池与 endpoint routing | `code/src/scheduling/runtime/ray_adapter.py`、profiler/trace 与契约测试 | `dual_gpu_actor_pool_shape_20260729/` | 固定 65K work、256 slots 和 0.5 CPU/endpoint 的三次重复已完成；2×128/4×64 相对 1×256 仅 +2.00%/+0.75%，未达 5% 晋升门槛。当前同构单 job 保留 1×256；多 job 分池仍待验证。 |
| Batching × submission 联合搜索 | scenario runner 与汇总工具 | `joint_batching_submission_512_20260726/` | 18 单元筛选和候选重复完成；当前单 GPU 下联合候选未显著优于独立拼接。 |
| vLLM CUDA Graph | 服务配置与相同 profiler 路径 | `vllm_cuda_graph_512_20260726/` | 重复真实对照显著优于 eager；作为本地部署 baseline，不作为上游调度研究贡献。 |
| 算子代价估计 | `code/src/cost_estimation.py`、`code/scripts/estimate_operator_cost.py` 及测试 | `operator_cost_estimation_20260726/` | 283 行、70 配置组、五个 grouped held-out split；可作粗粒度提示，不能作严格 SLO 预测。 |
| 多模态 cost adapter | 中性 `cost_units`/策略接口 | 无图像 workload 结果 | 基础抽象已留出，真实 image source、frame/patch cost、CLIP/Qwen-VL 链路尚未完成。 |
| 多 endpoint/多 GPU 调度 | endpoint/pool 配置与 routing contract | request replay、active-work saturation 与 Actor Pool gate | 真实双 4090 容量、admission 与 worker identity 证据已建立；尚不能声称 1/2/4 job 公平性、路由增量或故障迁移有效。 |
| Ray task/actor 与 vLLM capacity 调优 | 执行接口、参数字段和实验设计 | CUDA Graph、双 GPU request replay 与 active-work saturation | 已固定 vLLM 8192 batched-token/256 seq capacity，并标定上游 65K work 饱和点；Actor Pool/service quantum 继续分层验证。 |

## 3. 全部正式结果目录

| 结果目录 | 角色 | 当前状态或结论 |
|---|---|---|
| `dual_gpu_service_quantum_20260729/` | 固定 work/pool 的 batch、四档 complete-row quantum 与 request 对照 | 24/24 成功；HOL/credit barrier 确实缩短，但没有转化为超过 5% 的稳态吞吐或 SLO 收益。 |
| `dual_gpu_actor_pool_shape_20260729/` | 固定 work/slots/CPU 的 1×256/2×128/4×64 Ray actor 拓扑对照 | 12/12 成功；多 actor 未达到预注册晋升门槛，当前保留 1×256，不能外推到多 job 隔离与故障迁移。 |
| `dual_gpu_active_work_saturation_20260729/` | 八档 request-level per-endpoint active-work 扩展饱和曲线 | 32/32 成功；65K 是预注册最小饱和点，98K/131K 不再增加吞吐且尾延迟更差。 |
| `dual_gpu_active_work_curve_20260728/` | request-level per-endpoint predicted-token work 容量曲线 | 真实双 4090、五档各三次 formal。吞吐 CV 均低于 1%；49K 为当前 knee candidate，65K 为最高已测吞吐边界，尚未找到吞吐下降点。 |
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
  code\scripts\run_ai_operator_scenarios.py `
  --config <scenario_config.json> `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir <run-output-directory> `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`scenario_config.json` 是参数来源；`manifest.json` 的 `completed_runs[].command` 保存每一轮展开后的精确命令，`incidents` 保存失败与恢复记录。早期 baseline 使用独立脚本，按其目录 README 复现。

代价估计使用：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\estimate_operator_cost.py `
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

## 6. 仍欠缺的正式实验

1. Shared-vLLM 不同 foreground size、arrival offset 和 job 数量下的 static K8 边界与公平性；当前只完成一个 128/512 双作业规模。
2. UCB profiler 集成前的封闭 epoch/reward 归因设计与测试，随后才可做 GPU 对照。
3. 在 burst/gap、foreground/background 与不同 SLO 下验证完整
   SLO-aware EWMA flush，避免稳态饱和负载没有控制决策机会。
4. 真实多 endpoint/多 GPU 的多 job 公平性、路由与故障迁移。
5. Ray task/actor 有效并发和 vLLM scheduling capacity 的分层调优。
6. Prefix cache 开启后的 prefix-aware 独立消融。
7. 图像 workload 的多模态泛化验证。
8. 代价估计的独立时间段、新 workload、跨模型校准和预测区间。
