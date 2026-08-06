# Recent Figure Assets

## Output-aware Packing Evidence (2026-07-26)

| File | Purpose | When to read/use |
|---|---|---|
| `code/INFRA_STATUS.md` | Current Daft+Ray AI-operator infra flow, implementation completeness, evidence boundaries, and prioritized remaining work | Use for a single implementation-status handoff before reading detailed plans |
| `experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md` | Unified map from implemented/tested mechanisms to code, tests, principal result directories, evidence level, current decision, and remaining validation | Read first when asking what has actually been implemented, tested, proven, rejected, or left unverified |
| `experiments/plans/baseline_reference.md` | AI_COMPLETE / AI_EMBED / AI_CLASSIFY 的统一 baseline/benchmark 总入口 | 先确认比较层级、原生性、证据等级、指标合同与当前门禁，再进入模态执行合同 |
| `experiments/plans/text_native_baseline_rerun_20260802.md` | 文本 ceiling/control/vendor-native baseline 原生性审计与复测合同 | 远端重测前读取；定义 Chat/Completions 分轨、64-row validity gate、512-row calibration 与 2,048-row held-out 合同、指标和结论边界 |
| `experiments/plans/archive/database_ai_operator_baseline_matrix_20260729.md` | 2026-07-29 文本 baseline 预注册与逐日执行历史 | 仅追溯旧实验；不再作为当前 gate、calibration 或 formal 的运行依据 |
| `experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md` | DuckDB `ai` bounded-output 产品对比协议：任务划分、5 类共同指标、operator-only vs database-E2E 两计时边界、请求等价门禁 | 启动任何 DuckDB bounded-output 对照（句子计数 micro gate / SQuAD 短答案 / 三臂正式）前必读 |
| `code/scripts/baselines/run_official_baseline_gate.py` | Reproducible two-endpoint official baseline core gate runner | Use after freezing the PostgreSQL manifest; starts both shards per cell, preserves logs/raw output, waits for empty vLLM queues and stops on the first failed gate |
| `experiments/results/oceanbase_b1_gate_20260731/README.md` | OceanBase B1 capability gate + 2026-08-02 independent deployment recheck | Confirm CE 4.5.0 contains AI_COMPLETE/DBMS_AI_SERVICE and why the ordinary AutoDL container still cannot init observer before retrying on a deployable host |
| `experiments/results/oceanbase_b1_gate_20260731/install_runbook.md` | Reusable OceanBase CE install/start runbook (apt path, config gotchas, verified observer start command, bootstrap + dynamic AI_COMPLETE steps) | Follow when redeploying OceanBase on a privileged container or systemd VM |
| `feasibility/results/image_staged_resource_gate_20260802/` | Daft/Ray Data staged 256-row 双卡显式资源与输出等价门禁，含 `raw/` 原始 CSV/manifest | 确认 SQL reader 不再被常驻 actor 饿死、schema v4 CPU 账本正确；不能把单次冷启动吞吐用于系统排名 |
| `experiments/results/prefix_cache_routing_req_20260730/README.md` | Cache-on prefix-affinity routing ablation, request granularity, three arms (least_queued / prefix_affinity / +length-align) | Review why pure routing is -0.1% neutral and the prefix direction is closed under vLLM APC |
| `experiments/results/prefix_cache_routing_4ep_1.5b_20260731/README.md` | 4-endpoint prefix-affinity routing ablation (4×Qwen2.5-1.5B, 2 arms) + the code/config/vLLM/deploy adjustments that enabled 4 endpoints | Review why prefix_affinity is +5.9% (crosses 5% gate) at 4-ep/1.5B but neutral at 2-ep/7B — high-eviction regime; confounded (model×endpoints×KV) and saturated, needs isolation |
| `experiments/results/prefix_cache_data_org_20260730/README.md` | Cache-on prefix-aware batching ablation (batch granularity) plus routing-section cross-reference | Review why upstream batching order is neutral when vLLM APC is on |
| `experiments/results/rc1_data_organization/README.md` | RC1 data-org systematic re-run (5 strategies × {2-ep/0.9, 4-ep/0.43}, 1.5B, multiturn, cache-ON, P0 metrics); supersedes 07-25/26 gropy (07-18/19 retained as historical motivation) | Review the regime-dependent finding (strategy ranking reverses 2-ep vs 4-ep) and the prefix_group_ratio mechanism (reorder organizers collapse cache hit to 0.06 under 4-ep KV saturation) |
| `experiments/results/dual_gpu_shared_vllm_formal_20260729_1135/README.md` | Shared-vLLM 1/2/4-job independent/static/shared-DRR seven-step formal report | Audit the capacity/fairness pass, the 2-job no-gain boundary, the 4-job conditional gain and repeat instability |
| `experiments/results/dual_gpu_shared_vllm_formal_20260729_1135/formal_summary.csv` | Plot-ready throughput, MFU, JCT, P99, SLO and fairness statistics | Compare all nine formal cells without downloading remote request traces |
| `experiments/results/dual_gpu_shared_vllm_formal_20260729_1135/credit_summary.csv` | Shared-credit exact request/work peaks and waiting utilization audit | Verify global capacity, final-zero and work-conserving behavior |
| `experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/README.md` | High/arrival-limited fixed, queue-adaptive and SLO-EWMA flush formal comparison | Audit why a functioning load-sensitive controller did not pass the 5% gate and why fixed-50 remains the single-job baseline |
| `experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/formal_summary.csv` | Plot-ready throughput, SLO, tails, utilization, selected waits, fallback and completion-lag summary | Compare all six formal arms without downloading large remote traces |
| `experiments/results/dual_gpu_service_quantum_20260729/README.md` | Fixed-saturation-work batch/complete-row-quantum/request comparison with causal credit analysis | Distinguish a real HOL/credit reduction from the absence of a steady-state throughput win |
| `experiments/results/dual_gpu_service_quantum_20260729/formal_summary.csv` | Plot-ready granularity, submissions, throughput, tails, SLO, credit-held, Ray delay and energy | Audit the no-promotion decision and RPC/control trade-off |
| `experiments/results/dual_gpu_actor_pool_shape_20260729/README.md` | Fixed-work, fixed-slot and fixed-CPU dual-4090 Ray actor-pool shape comparison | Retain 1×256 for the current single-job homogeneous endpoint case; do not extrapolate the negative scaling result to multi-job pool isolation |
| `experiments/results/dual_gpu_actor_pool_shape_20260729/formal_summary.csv` | Plot-ready actor topology throughput, variability, tails, SLO, MFU and Ray overhead | Audit the preregistered 5% promotion decision |
| `experiments/results/dual_gpu_active_work_saturation_20260729/README.md` | Dual-4090 eight-point request-level active-work saturation curve, preregistered selection, and seven-step analysis | Use 65,536 as the matched-work control for subsequent Ray strategy experiments |
| `experiments/results/dual_gpu_active_work_saturation_20260729/formal_summary.csv` | Plot-ready means, variability, adjacent gains, tails, SLO, MFU and maximum work seen | Audit the 97%/next-gain<3% saturation decision |
| `experiments/results/static_credit_prompt_length_screen_20260730/` | Short/long prompt request-K and active-work screening, resolved manifests/runs, median summary and fail-closed decision audit | Preserve the 48/48 real-GPU evidence while treating the dynamic GO/NO-GO as inconclusive because async/token-ID/equivalent-arm gates failed |
| `experiments/results/dual_gpu_active_work_curve_20260728/README.md` | Dual-4090 request-level per-endpoint active-work capacity curve with three formal repeats per cap | Use 49,152 as the current knee candidate and 65,536 only as the best tested throughput boundary, not a proven optimum |
| `experiments/results/dual_gpu_active_work_curve_20260728/formal_summary.csv` | Plot-ready active-work means, variability, tails, SLO goodput, utilization, and energy | Select matched-work control points for subsequent mechanism comparisons |
| `experiments/results/dual_gpu_request_replay_20260728/README.md` | Dual-4090 batch-barrier vs request-level replenishment repeats with admission-work audit | Use the work-matched K48 comparison; treat K64 only as the best tested request K, not an isolated mechanism win or capacity optimum |
| `experiments/results/dual_gpu_request_replay_20260728/formal_summary.csv` | Plot-ready formal means and predicted work per admission unit | Compare throughput, MFU, tails, bounded wait, and nominal per-endpoint offered work |
| `experiments/results/shared_vllm_adaptive_admission_20260726/README.md` | Real shared-vLLM foreground/background K8/K16/AIMD repeats and adaptive-flush follow-up | Review the admission throughput-tail tradeoff, why AIMD never decreases, and why adaptive flush mostly collapses to fixed-50 |
| `experiments/results/shared_vllm_adaptive_admission_20260726/admission_flush_comparison.csv` | Plot-ready admission × flush comparison with exact per-request token accounting | Plot foreground E2E/P99, background and system tokens/s, energy, MFU, and admission limits |
| `experiments/results/adaptive_admission_controller_20260726/README.md` | Real static K8/AIMD/EWMA/PID matrix plus AIMD-vs-static-K16 mechanism control | Review why the apparent adaptive gain is attributable to a higher concurrency ceiling rather than feedback |
| `experiments/results/adaptive_admission_controller_20260726/comparison_summary.csv` | Plot-ready controller-family and mechanism-control statistics | Plot E2E, tokens/s, request P99, goodput, energy, MFU, admission limit, and baseline-relative changes |
| `experiments/results/vllm_cuda_graph_512_20260726/README.md` | Real eager-vs-CUDA-Graph service comparison with matched 64 gates, one warm-up, three audited 512 formal repeats per arm, and startup-cost evidence | Use CUDA Graph as the current local steady-state baseline; retain the eager preflight incident and non-fatal Windows Ray shutdown stderr boundary |
| `experiments/results/vllm_cuda_graph_512_20260726/comparison_summary.csv` | Plot-ready eager/graph formal means, sample standard deviations, minima, and maxima | Plot E2E, request tails/SLO, throughput, energy, GPU pressure, and MFU |
| `experiments/results/vllm_cuda_graph_512_20260726/graph_startup_evidence.json` | Machine-readable Docker timestamps and raw vLLM startup/compile/graph-capture log lines | Audit one-time CUDA Graph startup cost separately from steady-state request metrics |
| `experiments/results/adaptive_flush_randomized_20260726/README.md` | Natural-EOS randomized flush comparison with exact output-token and finish-reason observation | Review why adaptive beats fixed-25 but is not proven better than fixed-50 |
| `experiments/results/adaptive_flush_randomized_20260726/chatml_flush_formal_512/summary_long.csv` | Plot-ready n=5 variable-output flush metrics | Plot E2E, request tails, SLO, submissions, energy, GPU, and MFU |
| `experiments/results/adaptive_flush_randomized_20260726/chatml_three_way_512/summary_long.csv` | Plot-ready natural-EOS fixed-25/fixed-50/adaptive randomized repeats | Plot the evidence supporting fixed-50 as the simplest current single-GPU candidate |
| `experiments/results/joint_batching_submission_512_20260726/README.md` | SLO-constrained 18-cell batching × submission search and repeated candidate validation | Review independent splice vs joint search and the fixed-50 mechanism decision |
| `experiments/results/joint_batching_submission_512_20260726/screen/summary_long.csv` | Plot-ready 18-cell screen | Plot token budget, K_max, flush, SLO, throughput, energy, and MFU |
| `experiments/results/joint_batching_submission_512_20260726/candidate_repeat/summary_long.csv` | Plot-ready repeated independent/joint/mechanism comparison | Plot n=3 candidate means, variability, and policy boundaries |
| `experiments/results/adaptive_flush_cross_rate_20260726/README.md` | Fixed-25/fixed-50/adaptive cross-arrival-rate real GPU screen | Review why adaptive is not the current default |
| `experiments/results/text_heldout_2048_20260726/README.md` | Natural-EOS 2048-request held-out validation | Review scale behavior, exact request audit, and tail-latency growth |
| `experiments/results/prefix_aware_batching_20260726/README.md` | Controlled prefix-ratio workload and prefix-aware code/experiment audit | Review cache-off mechanism boundary and organizer fixes |
| `experiments/results/operator_cost_estimation_20260726/README.md` | Formal-only 23-feature、13-context LOO 的算子 E2E 代价估计；all-phase 历史证据归档 | 检查 warmup 隔离、预测、候选 ranking、macro/pooled/max regret 与晋级门槛 |
| `experiments/results/multicard_saturated_2048_20260806/README.md` | 多卡饱和三臂 1w+3f（bounded 天花板 + duckdb harness_pre_split_diagnostic + project_static）；group 口径 project +2.55%/Welch p=0.0284（显著高于 duckdb harness，≠ 优于产品）；ADDENDUM 含 lb_rr@64（72480 不可审计，lbrr64 未提交）+ bounded collapse（ps8_collapse 已提交，group 口径 c64=36560/c128=24836） | 审计饱和对比、harness 身份订正、project 显著高 vs duckdb harness 的边界 |
| `experiments/results/multicard_rich_metric_2048_20260806/README.md` | 多卡 rich-metric 饱和 screening（committed aggregator 可复现）；身份 harness_pre_split_diagnostic + 措辞订正 | 审计 cache-hot screening + EM/F1（2048 子集分母）+ 调度开销 35-37% |
| `experiments/results/multicard_scale_ramp_20260806/README.md` | 多卡 scale-ramp（4096/8192/10570，c=32 固定）；8192/10570 duckdb cap-64 归因收紧（不声称超长/非 bug/不可用） | 审计规模延伸 + GPU 稳态 ~95% + duckdb 大尺度零错误门禁失败 |
| `experiments/results/multicard_concurrency_sweep/phase2_2048_tb/README.md` | 多卡并发扫描（2048，c=1..64，text-baselines venv 修 duckdb 1.5.4）；group 口径 bounded c32=87393/duckdb c2=79088（c 无关 set-oriented）/project K32=77381；C_total=64 顺序 bounded>project>duckdb；project prefix-hit K1 0.91→K32 0.96 | 审计完整并发曲线 + duckdb set-oriented + C_total=64 排序；1 rep/cell diagnostic |
| `experiments/results/multicard_lbrr_scale_ramp/README.md` | lb_rr 规模爬坡（64..10570，C_total=64，nginx round-robin，warmup_per_cell=false）；ttft 口径峰值 72934@2048，4096+ 下降；256 门禁 0 error/未观察 max_tokens-truncation（finish_reason 空≠审计非 length）+ nginx 8000/8001 对称 | 审计 lb_rr 规模曲线（diagnostic_observation_pending_evidence_fix，**不引用跨四臂 cache-thrash**）；身份 system_comparison_role=gateway_system_diagnostic（协议 §2.6 gateway 轨）；uncontrolled-cache |
| `deploy/autodl/multicard_concurrency_sweep.example.json` / `multicard_lbrr_scale_ramp.example.json` / `multicard_scale_ramp.example.json` | 多卡 ramp driver 的预注册 config 模板（concurrency-sweep / lb_rr scale-ramp / scale-ramp）；含 warmup_per_cell/vllm_config_strict/C_total 语义 | 复现多卡 ramp run 的配置入口（deploy §9.1 calibration 模板） |
| `experiments/plans/experiment_report_honesty_checklist.md` | 实验报告/数据诚实性检查清单（6 类反复错误 + §7 gate 4 项 + 8 步勾选） | 写多卡/吞吐/身份/统计报告前强制对照；是 `AGENTS.md §7.5/§6` + `bounded_output_duckdb_comparison_protocol_20260805.md` + `provenance.py::ComparisonRole` 的可勾选投影 |
| `experiments/results/operator_cost_profile_pilot_20260804/` | 双 4090 四候选 v1/v2 cost-profile pilot，含完整压缩 raw、summary 和七步报告 | 审计新机器采样合同、23 维候选可区分性、trace 完整性和 formal 时间预算 |
| `experiments/results/operator_cost_profile_dual4090_formal_20260804/` | 双 4090 首次 320-run formal 的并发 runner、空 Ray 地址与整体排除证据 | 复核为什么两套表面完整数据均不能进入 CE0–CE6，以及修复后的重跑门禁 |
| `feasibility/results/cost_profile_cacheon_gate_20260805/` | 已提交 main 的双 4090 cache-on + shared-Ray 真实两运行门禁 | 复核 cache 声明/命中计数、exactly-once、双 endpoint 与 0 local-Ray 启动；不作性能排名 |
| `feasibility/results/duckdb_ai_semantic_gate_20260805/` | DuckDB community `ai` 双 endpoint capability 与 ShareGPT fixed-cap 语义门禁，含最小 raw 证据 | 复核 4 行可运行和 64 行截断错误，决定另建 bounded-output 产品轨；不作性能排名 |
| `feasibility/results/squad_v11_dev_import_20260805/` | SQuAD v1.1 dev（10570 行）importer provenance：canonical SHA256 门禁、content_hash、多答案 JSONB、prompt 模板 hash | bounded-output 主对比轨的数据源合同；后续 gate 的 content_hash 校验基准 |
| `feasibility/results/request_equivalence_gate_20260805/` | 三方请求等价门禁（canonical = DuckDB `ai_completion_request_json` = 项目 `build_completion_request_body`）+ 单请求 vLLM prompt-token delta 交叉核验 | PASSED（37=37 prompt tokens、无隐藏 system prompt、默认 temp 0.1）；证明三臂送进模型的请求一致 |
| `feasibility/results/squad_capability_256_v2_20260805/` | SQuAD 256 行 DuckDB-ai capability gate v2（answers[0] 分桶、round 配额）；report.json 命令字段已脱敏 | v2 是过渡期有效 evidence；被 v3（更严谨抽样）取代，保留作历史 |
| `feasibility/results/squad_capability_256_v3_20260805/` | SQuAD 256 行 DuckDB-ai capability gate v3（原始 whitespace 词数的 max-答案分桶 + largest-remainder、workload 完整性 fail-closed、vLLM counter 可归因、full-set exactly-once、脱敏、七步 README） | EM 80.86% / F1 89.86%、attribution=attributable、workload_integrity=verified；**已被 v4 取代**（v3 分桶未用 SQuAD normalize），保留作历史单臂 evidence |
| `feasibility/results/squad_capability_256_v4_20260805/` | SQuAD 256 行 DuckDB-ai capability gate v4（**canonical**：SQuAD-normalize 分桶 + `sample_manifest.jsonl` + /version 修复 + codex 第七轮全部修复后重跑；workload 完整性、可归因、full-set exactly-once、脱敏、七步 README） | EM 81.64% / F1 89.82%（209/256，独立复算一致）、sample_hash `d0e0e987`（≠v3）、vLLM 0.25.1、attribution=attributable、integrity=verified；当前 canonical 256 单臂样本 |
| `feasibility/results/squad_capability_full_10570_20260805/` | SQuAD 全量 10570 行 DuckDB-ai gate（`--mode full` + `--strict-attribution` + service-config-hash + fail-closed；扩展并发上限 32）；原始证据 report.json/per_row/manifest **保持不变**，README §8 含审计订正 | **fail-closed FAILURE（维持）**：10569/10570 成功、1 NULL；exactly-once/三 hash 一致/归因==10570 全通过；EM 80.32%/F1 89.36%。1 NULL = full-set query 中**单次、机制未定**的生成尾部事件（并发/批状态候选、未隔离）；该行质量不论 NULL 或孤立重放文本都 0 分。状态：`capability_gate_status=failure`/`comparison_admission=eligible_with_documented_failure`/`formal_run_gate_passed=false` |
| `feasibility/results/squad_truncation_diag_572700c8_20260805/` | full gate 失败行 572700c8 的定点截断诊断（direct vLLM + DuckDB `ai_try_complete` × cap{64,128,256} × 3 重复；cache/retry off；附 `ai_completion_request_json`，两路径语义等价非字节相同） | **截断不可复现**：cap=64 孤立重放 3×3 全部 `stop`/46 token/文本一致 → 推翻「确定性 rambling」，记为偶发、机制未定。该 46-token 文本 EM=0/F1=0（错答）。诊断专用，不回灌 cap=64 |
| `feasibility/results/squad_database_e2e_duckdb_ai_20260805/` | SQuAD database-E2E runner 单臂实测（DuckDB-ai，全 10570，cap=64，strict-attribution，统一 sink `document_completions`）；`report.json` + `per_row_evidence.csv`（EM/F1 独立复算一致）。**机器原始文件保持不变；README §8 含 codex 复核订正** | **database-E2E 边界已测**：wall 93.9s = scan 0.14 + construct 0.23 + adapter 93.2（op_jct 87.8）+ sink 0.26；adapter 占 99.27%、operator query barrier 占 93.54%（不归因给模型）。`correct_rows/s` 90.42、sunk 10570。**订正**：failure_rate 原报告 0.000189（error+NULL 双计）→ 正确 0.0000946（去重失败行）；状态字段解耦（single_run_valid / formal_run_gate_passed 恒 false（单次） / comparison_admission=pending_formal_repeat）。单臂测量，非排名 |
| `feasibility/results/squad_database_e2e_direct_client_20260805/` | SQuAD database-E2E runner 单臂实测（direct_client，全 10570，cap=64，strict-attribution，统一 sink）；`report.json` + `per_row_evidence.csv`（EM/F1 独立复算一致）+ `sunk_status.csv`（补取，10570 行）。**机器原始文件保持不变；README §8 含 codex 复核订正** | **direct 臂 database-E2E 边界已测**：wall 91.9s = scan 0.13 + construct 0.23 + adapter 91.2（op_jct 90.9）+ sink 0.26；0 error/0 NULL → status=success，但 finish_reason `{stop:10569, length:1}`（1 行截断返回 partial text，非 error）。`correct_rows/s` 92.29、EM 80.22%。**与 DuckDB-ai 核心差异**：同一 source row（`572700c8…`）在两次独立 full 中触顶 cap=64，DuckDB-ai 转 NULL→failure，direct 返回 partial text→success（截断的产品语义差异，非吞吐差异）。订正：truncation_count/rate 与 per-row latency 列归档 report/CSV 无，需正式 rerun 落盘；状态字段同 DuckDB-ai 臂解耦。单臂测量，非排名 |
| `experiments/results/row_cap_aware_packing_512_20260726/README.md` | Prefix-cache-corrected 512-row screening and repeated confirmation | Review why sequential remains default and how cache-enabled data was excluded |
| `experiments/results/row_cap_aware_packing_512_20260726/nocache_repeats/summary_long.csv` | Plot-ready 512-row repeated metrics | Plot throughput, request tails/SLO, packing, energy, vLLM pressure, and MFU |
| `experiments/results/row_cap_aware_packing_1024_20260726/README.md` | Held-out 1024-row mechanism decision | Review the SLO-goodput regression that blocks row-cap-first default adoption |
| `experiments/results/row_cap_aware_packing_1024_20260726/summary_long.csv` | Plot-ready 1024-row repeated metrics | Plot scale confirmation and mechanism trade-offs |
| `experiments/results/row_cap_aware_packing_gate_20260726/README.md` | Row-cap-aware packing 64-row real gate | Audit sequential/classic-BFD/row-cap-first correctness, request/resource traces, and MFU prerequisites before 512 screening |
| `experiments/results/output_aware_bfd_gate_v2_20260726/README.md` | Output-aware BFD 64-row real gate | Audit unified token/row constraints and GPU/power/energy/MFU availability |
| `experiments/results/output_aware_bfd_gate_20260726/README.md` | Superseded pre-fix 64-row gate | Preserve infrastructure evidence while excluding uncontrolled algorithm comparisons |
| `experiments/results/output_aware_bfd_512_20260726/README.md` | Superseded 512-row failure audit | Understand the row-cap mismatch, timeout, and why old data is excluded |
| `experiments/results/output_aware_bfd_512_v2_20260726/README.md` | Corrected six-cell 512-row report | Review three-repeat candidate evidence and claim boundaries |
| `experiments/results/output_aware_bfd_512_v2_20260726/runs.csv` | Corrected 512-row run metrics | Plot request E2E, packing, GPU, power, energy, vLLM pressure, and MFU |
| `experiments/results/output_aware_bfd_512_v2_20260726/summary_long.csv` | Corrected 512-row plot-ready summary | Compare six scenarios with mean, sample standard deviation, and range |
| `experiments/results/output_aware_bfd_1024_20260726/README.md` | 1024-row held-out confirmation | Review the negative scale result and row-cap fragmentation boundary |
| `experiments/results/output_aware_bfd_1024_20260726/runs.csv` | 1024-row run metrics | Audit nine formal runs and 9,216 request lifecycle rows |
| `experiments/results/output_aware_bfd_1024_20260726/summary_long.csv` | 1024-row plot-ready summary | Plot throughput, E2E, energy, GPU, vLLM pressure, and MFU |

| File | Purpose | When to read/use |
|---|---|---|
| `figures/data/backup/b07_local_vllm_ray_throughput.png` / `.svg` | Local `AI_COMPLETE` Daft + Ray + vLLM throughput support figure | Learning and backup explanation of throughput across six fixed row-batch settings; not an optimized scheduling result |
| `figures/data/backup/b08_local_vllm_ray_e2e_time.png` / `.svg` | Local `AI_COMPLETE` Daft + Ray + vLLM end-to-end time support figure | Learning and backup explanation of end-to-end time across six fixed row-batch settings |
| `figures/data/backup/b09_local_vllm_ray_task_stage_timing.png` / `.svg` | Local `AI_COMPLETE` Ray task stage timing support figure | Learning and backup explanation of source, Daft organize, Ray submit, fan-in, and operator wall timings |
| `figures/data/backup/b10_local_vllm_request_count_inflight.png` / `.svg` | Local `AI_COMPLETE` request-count and in-flight utilization support figure | Learning and backup explanation of how large fixed row batches reduce request count and can underfill the in-flight window |
| `figures/data/backup/b11_local_vllm_token_tail_performance.png` / `.svg` | Local `AI_COMPLETE` token-tail performance support figure | Learning and backup explanation of why fixed row batches do not control token-tail cost or service-tail latency |
| `figures/data/backup/b12_local_vllm_latency_probe_breakdown.png` / `.svg` | Local `AI_COMPLETE` vLLM latency metric probe support figure | Learning and backup explanation of client batch latency versus vLLM server-side latency metrics |
| `figures/data/backup/b13_local_vllm_token_tail_penalty.png` / `.svg` | Local `AI_COMPLETE` token-tail penalty support figure | Explain the link between token P95 and model-service latency tail |
| `figures/data/backup/b14_local_vllm_service_tail_gap.png` / `.svg` | Local `AI_COMPLETE` service-tail gap support figure | Explain P50-to-P95 latency widening for large fixed row batches |
| `figures/data/backup/b15_local_vllm_token_budget_throughput.png` / `.svg` | Local `AI_COMPLETE` token-budget throughput support figure | Compare fixed-row and token-budget throughput under the same local vLLM setup |
| `figures/data/backup/b16_local_vllm_token_budget_tail_queue.png` / `.svg` | Local `AI_COMPLETE` token-budget token-tail and queue support figure | Show token-budget controls token P95 and queue pressure; motivates K_max follow-up |
| `figures/data/backup/b17_local_vllm_arrival_kmax_sweep.png` / `.svg` | Local `AI_COMPLETE` preliminary arrival-aware K_max support figure | Single-shape scheduling sweep with `token_budget=6144`; use as preliminary evidence only |
| `figures/data/backup/b18_local_vllm_batch_kmax_e2e.png` / `.svg` | Local `AI_COMPLETE` batch policy x K_max end-to-end support figure | Show how fixed-row and token-budget shapes interact with K_max in end-to-end time |
| `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.png` / `.svg` | Local `AI_COMPLETE` batch policy x K_max service-pressure support figure | Show vLLM queue time and batch service P95 rising when inflight submissions are too large |
| `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.png` / `.svg` | Local `AI_COMPLETE` batch policy request-granularity support figure | Explain why K_max cannot help once upstream batch shape creates too few Ray submissions |
| `figures/data/backup/b21_local_vllm_kmax_interference_small_job.png` / `.svg` | Local `AI_COMPLETE` shared-vLLM K_max interference support figure | Show unbounded background inflight harms foreground small-job latency on a shared vLLM endpoint |
| `figures/data/backup/b22_local_vllm_length_prefix_tail.png` / `.svg` | Local `AI_COMPLETE` length/prefix data-organization tail figure | Compare token tail and service tail for fixed, token-budget, length-align, and prefix-aware policies |
| `figures/data/backup/b23_local_vllm_length_prefix_signal.png` / `.svg` | Local `AI_COMPLETE` length/prefix organization-signal figure | Show prompt-token spread and prefix-group ratio; does not prove cache benefit |
| `figures/data/backup/b24_local_vllm_interference_sweep_small_job.png` / `.svg` | Local `AI_COMPLETE` shared-vLLM foreground interference sweep figure | Show foreground E2E/service/queue impact under background K_max 8/16/unbounded/adaptive |
| `figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.png` / `.svg` | Local `AI_COMPLETE` shared-vLLM bulk tradeoff sweep figure | Show bulk throughput plateau and service/queue pressure under larger background inflight |
| `opening/slides/opening_defense_20260720_v5.pptx` | 开题答辩 PPT v5 | Current incremental deck based on v4; adds three data-organization mechanism slides after original slide 14 without rerunning `build_ppt.py` |
| `opening/slides/opening_defense_v6_design.md` | 开题答辩 PPT v6 设计说明 | Defines the v5-based design-first deck, motivation-test scope, conditional official-baseline result gate, top-venue architecture figures, synchronization scope, and QA gates before editing the PPTX |
| `figures/architecture/data_organization_token_budget_mechanism.png` / `.svg` | 数据组织策略机制图：token-budget batching | Formal mechanism figure for converting fixed-row batches into token-budget submissions; not an experimental-result claim |
| `figures/architecture/data_organization_length_align_mechanism.png` / `.svg` | 数据组织策略机制图：length-aligned grouping | Formal mechanism figure for sorting/grouping rows by token length to reduce within-batch compute variance |
| `figures/architecture/data_organization_prefix_aware_mechanism.png` / `.svg` | 数据组织策略机制图：prefix-aware grouping | Formal mechanism figure for grouping shared system prompts; prefix-cache benefit still requires vLLM metric validation |
| `figures/scripts/generate_data_organization_strategy_mechanism.py` | 数据组织策略机制图生成脚本 | Regenerate the three formal mechanism figures and scan for forbidden visible tokens |
| `figures/audit/data_organization_strategy_mechanism_audit.md` | 数据组织策略机制图审计记录 | Read before citing the mechanism figures in report/PPT/thesis material |
| `figures/architecture/submission_control_queue_adaptive_mechanism.png` / `.svg` | 提交控制策略机制图：队列自适应提交 | Formal mechanism figure for converting fixed flush into queue-state feedback; validate with queue wait, P95/P99, tokens/s, and E2E time |
| `figures/architecture/submission_control_kmax_admission_mechanism.png` / `.svg` | 提交控制策略机制图：在途上限准入控制 | Formal mechanism figure for bounding in-flight requests at the service entrance; validate with K_max sweep and foreground/background interference |
| `figures/architecture/submission_control_pool_routing_mechanism.png` / `.svg` | 提交控制策略机制图：分池路由 | Formal mechanism figure for binding submission parameters to request shape and actor-pool choice |
| `figures/scripts/generate_submission_control_mechanisms.py` | 提交控制策略机制图生成脚本 | Regenerate the three formal mechanism figures and scan for forbidden visible tokens |
| `figures/audit/submission_control_strategy_mechanism_audit.md` | 提交控制策略机制图审计记录 | Read before citing the submission-control mechanism figures in report/PPT/thesis material |
| `figures/scripts/generate_local_vllm_ray_baseline_charts.py` | Local vLLM Ray baseline chart generator | Regenerate `b07`-`b25` from the ShareGPT/BurstGPT baseline CSVs |
| `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` | Local vLLM Ray baseline chart audit | Check data source, figure role, chart choice, visual QA, and conclusion boundary |
| `learning/local_vllm_ray_baseline_walkthrough.md` | Local vLLM + Ray baseline learning walkthrough | Read when explaining what the fixed row-batch baseline does and does not prove |
| `learning/archive/early_experiments_walkthrough.md` | 早期实验学习讲解（已归档） | pre-convergence 时期实验（组件可行性、fake/CPU、PG18.4 接入等）的历史参考 |
| `learning/metric_selection_methodology.md` | AI_EMBED vs AI_COMPLETE 观察变量选择方法论 | 理解为什么从"阶段时延拆分"转向"多维分布表征" |
| `learning/text_native_baseline_guide.md` | 文本 AI 算子 baseline 初学者讲解 | 理解 ceiling/control/native/project 的区别、请求链路、分轨与正式结果读法 |
| `learning/observability_metrics_guide.md` | 新观测指标与 fail-closed 输入合同讲解 | 下一轮运行前确认 TTFT/ITL、goodput、成本、公平、CI/CV、代价模型与检索质量口径 |
| `learning/vllm_clip_pooling_gate_guide.md` | 图像 vLLM pooling direct-service ceiling 的角色与能力门禁读法 | 区分“接口已注册”“本机可运行”和“已获得性能上限”，避免把 blocker 当性能结果 |
| `figures/architecture/runtime_strategy_rule_table.png` / `.svg` | 信号触发候选策略规则表 | 与闭环图配套使用，说明观测信号、候选动作和保护约束；不作为已验证结论 |
| `figures/architecture/runtime_strategy_control_loop.png` / `.svg` | 运行时信号驱动的上游执行闭环图 | 当前首选策略机制图；用一个 AI_COMPLETE SQL 例子说明数据组织（token-budget/length-align/prefix-aware）、提交控制（queue-adaptive flush/K_max/routing）、vLLM 部署平台（观测不修改）的分工；不重切数据库侧已物化批次 |
| `figures/scripts/generate_runtime_strategy_control_loop.py` | 运行时策略闭环图生成脚本 | 重新生成策略机制图 PNG/SVG，并执行边框、箭头和禁用术语自检 |
| `figures/audit/runtime_strategy_control_loop_audit.md` | 运行时策略闭环图审计记录 | 检查新策略机制图的角色、旧图关系、遮挡、箭头和禁用术语 |
| `figures/audit/top_venue_strategy_figure_design_notes.md` | 顶会系统论文方法图设计备忘 | 重绘策略设计图前阅读，采用 control-loop + running example + compact rule table |
| `figures/audit/strategy_figure_micro_design_points.md` | 策略图小机制设计点与论文下载清单 | 重绘策略图前拆分 batch/partition、反压、路由、写回约束和规则表等小图 |
| `figures/audit/local_reference_figure_reading_notes.md` | 本地 PDF 图形阅读笔记 | 记录已下载论文中的机制图经验，并合并到运行时控制闭环图方案 |
# PROJECT_INDEX.md

本文件是项目索引，供 Codex 快速定位材料。先读 `AGENTS.md`，再按任务类型读本文件中的对应材料。

## 1. 快速阅读顺序

### 只想了解当前课题

1. `AGENTS.md`：长期规则、用户目标、当前确定方向。
2. `README.md`：项目概览和目录结构。
3. `PROJECT_OUTLINE.md`：当前题目、研究内容、关键证据、近期优先级。
4. `overview/current_direction_and_plan.md`：阶段性技术路线和计划。
5. `motivation/results/gpu/README.md`：真实 GPU-backed E2E 结果入口。

### 要继续做实验

1. `AGENTS.md`：实验规则。
2. `motivation/plans/workloads.md`：三类 AI 算子场景、动机测试和后续实验优先级。
3. `motivation/plans/integration.md`：PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线。
4. `feasibility/benchmarks/README.md`：组件 benchmark 脚本和运行命令。
5. `motivation/results/README.md`：动机测试正式结果阅读顺序和结论边界。
6. `motivation/results/gpu/README.md`：真实 GPU-backed E2E 结果入口。

### 要在 AutoDL 远端继续实验

1. `AGENTS.md`：项目边界、实验规则与 Git 约束。
2. `PROJECT_OUTLINE.md`：权威近期优先级。
3. `experiments/plans/experiment_status_and_gaps.md`：当前唯一实验矩阵、
   baseline、成功标准和剩余缺口。
4. `deploy/AGENTS.md`：部署目录边界。
5. `deploy/autodl/README.md` 顶部“新对话 / 新 agent 的唯一操作入口”：
   全新实例准备、开机恢复、服务门禁、gate、正式启动与恢复命令。
6. `deploy/autodl/*.example.json`：实际实验配置；不得从聊天记录手工重建参数。

换机器或新建环境时，在进入平台 runbook 前先读
`deploy/runtime/README.md`：选择 machine profile、检查 Python 能力和模型/数据资产，
并把 `preflight.json` 保存到仓库外 artifact 目录。检查不自动安装；安装/下载是显式
子命令。

新对话完成以上读取后直接按 runbook 做只读状态检查；不要重新探索 Python、
CUDA、模型、数据库和日志路径。只有固定路径或门禁失败时，才进入
`deploy/autodl/README.md` 对应详细故障章节。

### 要写调研/和导师沟通

1. `AGENTS.md`：沟通边界和不能声称什么。
2. `motivation/plans/ai_sql_surface.md`：数据库 AI 算子现状和 AI 算子 SQL 触发面分析。
3. `research/literature_and_evidence_review.md`：文献与官方资料依据。
4. `notes/communication_notes.md`：已有沟通问题和话术。

## 2. 核心文件地图

| 文件 | 内容 | 什么时候读 |
|---|---|---|
| `AGENTS.md` | 项目长期规则、用户真实目标、当前选题边界 | 每次开始任务先读 |
| `PROJECT_INDEX.md` | 文件索引和阅读顺序 | 不知道材料在哪里时读 |
| `PROJECT_OUTLINE.md` | 项目总纲：当前题目、研究内容、关键证据、近期优先级 | 快速了解最新进展 |
| `README.md` | 工作区总览、当前方向、目录结构 | 了解项目背景 |
| `overview/AGENTS.md` | 总览目录规则 | 修改 `current_direction_and_plan.md` 时读 |
| `overview/current_direction_and_plan.md` | 当前方向的快速参考卡片（TL;DR） | 2 分钟了解课题全貌 |
| `deploy/autodl/README.md` | AutoDL 单一 runbook：环境准备、开机恢复、gate、正式实验和中断恢复；顶部有"两条推理引擎 track（文本 vLLM / 多模态 CLIP）"概念总览 | 新对话接手远端实验时按顶部唯一入口直接操作 |
| `deploy/autodl/text_serving.md` | 文本模态（vLLM 生成式 LLM）推理服务引擎部署：vLLM 是什么（continuous batching/APC/KV cache）、vLLM vs CLIP 差异、Qwen 模型下载 + start_endpoints.sh、sharegpt_multiturn 数据集、runner/合同/喂饱门禁、7 条坑 | 跑文本 AI_COMPLETE 实验时读；与 image_serving.md 对称 |
| `deploy/autodl/image_serving.md` | 图像 CLIP 部署：五臂 fused/staged operator-E2E gate、显式 Ray 资源账本、Ray GPU actor、vLLM pooling、COCO/PG BYTEA 边界 | 准备/跑图像 AI_EMBED/AI_CLASSIFY 时读；先过 schema v4 资源/正确性门禁，再区分 operator E2E 与含 sink 的 system E2E |
| `deploy/runtime/README.md` | AutoDL/单 5070/其他 Linux GPU 的多机器合同、自动 profile preflight、显式依赖/资产补齐和按运行签名校准流程 | 任意机器开始/恢复实验、发现缺包/缺数据或新增 workload 时先读 |
| `deploy/runtime/assets.json` | 通用 Python 能力组和公开/受许可模型数据资产清单 | 检查环境、补可选依赖或下载新资产；不替代数据库 importer |
| `deploy/runtime/profiles/*.json` | 双 4090、单 5070 和通用 Linux NVIDIA GPU 的自动匹配与最低机器能力合同 | 检查 CPU/GPU/显存/磁盘/命令，不承诺性能最优 |
| `deploy/runtime/runtime.env.example` | 与平台无关的仓库外环境变量模板 | 新机器集中设置五个根目录、模型、endpoint、数据库和 MFU 口径 |
| `code/scripts/environment/manage_environment.py` | 自动硬件/profile 识别、只读 check + 显式 install-python/download CLI | 保存匿名 machine ID/能力报告；默认不改变环境，受许可资产 fail closed |
| `code/scripts/environment/scan_git_secrets.py` | 高精度 Git 隐私扫描器（私钥 / api token / `sshpass -p` / 外部 `user:pw@<真实host或IP>`）；默认扫暂存区，`--all` 扫全仓 | commit 前跑；配 `.githooks/pre-commit` 自动拦截；本地默认 `postgres:postgres@localhost` 与模板 host 放行 |
| `code/scripts/environment/secret_scan_baseline.txt` | scan_git_secrets 的 reviewed 误报 allowlist（每行一个正则，附原因）| 当前仅放行模型生成证据里的占位符 `ondigitalocean.com` URL；保持精简，新增需写明理由 |
| `.githooks/pre-commit` | commit 前自动跑 scan_git_secrets | 一次性启用：`git config core.hooksPath .githooks`；`--no-verify` 仅限已验证误报 |
| `code/requirements/*.txt` | 通用下载与 learned estimator 等可选能力依赖 | 只装进明确指定的 driver/analysis Python，不装进所有环境 |
| `code/scripts/analysis/select_strategy_calibration.py` | 从 feeding/direct/token-budget/actor-shape 证据生成冻结校准合同和环境覆盖 | 同协议 actor 曲线完成后、启动数据组织/提交策略/多 job formal 前执行 |
| `code/scripts/analysis/summarize_static_k_workload_surface.py` | 判定不同 workload 的静态 K 最优点迁移和错配代价是否足以支持动态控制 | static-K workload surface 后 fail-closed 决定是否继续 adaptive formal |
| `code/scripts/analysis/summarize_static_credit_workload_surface.py` | 跨 workload 审计 request/work credit 的中位数、CV、等价无压力臂、token-ID 覆盖与交叉 regret | 禁止用不稳定均值表直接给出动态 GO/NO-GO |
| `code/scripts/analysis/compare_cost_estimators_contextloo.py` | 完整 decision-context LOO 的逐 fold、宏/pooled、repeat 聚合 ranking 与晋级合同 JSON | 复核代价估计 unseen-context 泛化；禁止只凭平均 regret 晋级 |
| `code/scripts/profiling/profile_image_clip_preprocess_variants.py` | 交错比较当前 production-np、历史 legacy-pt 与 torchvision CLIP preprocessing，并经过同一 tensor actor 做 embedding parity gate | 复核 image motivation 是否能外推到当前代码边界；不是 E2E 方法结果 |
| `code/scripts/profiling/gate_vllm_clip_pooling.py` | 单图离线 vLLM CLIP pooling 输入/输出/版本 capability worker | 只判断本机模型/API 能否返回合法 embedding，不作吞吐排名 |
| `code/scripts/profiling/run_vllm_clip_pooling_gate.py` | 跨 macOS/Linux 的 vLLM gate 进程组超时、日志和退出码监管器 | engine 初始化可能阻塞时生成可归因 pass/error/timeout 证据 |
| `feasibility/results/vllm_clip_pooling_gate_20260804/` | vLLM 0.25.1 CLIP pooling 两次 600 秒离线能力门禁、环境快照、完整日志和七步负结果报告 | 复核为什么当前环境停在 capability blocker，不能继续在线/5K/60K 或报告吞吐 |
| `code/scripts/experiments/run_image_clip_e2e.py` | 同 PostgreSQL BYTEA、模型/GPU 和输出审计下运行 Daft built-in、Ray Data native graph、诊断 reference 与 project arms | 跑 operator-E2E gate/formal；schema v12 记录 baseline provenance、计时内输出合同、首输出结构信号和单位工作资源，并拒绝把项目自写 Daft UDF 当正式 native baseline |
| `code/src/modalities/image/metrics.py` | 从图像 run 已观测总量派生 first-output/E2E、60s duration gate 和单位图片资源指标 | 跨规模只能比较独立达平台的速率/单位成本；不推断隐藏调度或逐图 latency |
| `code/scripts/analysis/augment_image_observability.py` | 为历史 schema-v11 图像 CSV 生成不覆盖 raw 的派生指标副本 | 复用已有 12K/60K 数据，不为纯代数字段重跑；缺原始字段时 fail-closed |
| `experiments/results/image_ai_embed_operator_formal_20260803/` | Ray Data/project 60K×2 matched-resource formal、Daft 12K capacity consistency、派生单位资源与指标定义 | 阅读当前图像 operator 结果；区分同规模正式排名、Daft 容量上限和跨规模描述性指标 |
| `code/configs/image_vendor_baselines.json` | Daft 官方 803,580-row ResNet18 Daft/Ray Data 入口的仓库、commit、文件 SHA256、官方 workload 与允许适配范围 | 跑 vendor-code parity 前核对；禁止改写官方调度图 |
| `code/scripts/experiments/run_image_clip_matrix.py` | 固定 seed 交错编排图像 warmup/formal，持有结果目录租约并校验 unique rows、exactly-once、稳态时长 | 防止手工顺序漂移、并发写结果和短作业误入 formal；输出 raw CSV、逐 run manifest/log 与外层 schedule |
| `code/scripts/data/import_coco_images.py` | 从目录或 ZIP 流式导入 COCO 图像 BYTEA，保留 source doc_id，单事务替换指定 workload | 正式规模避免完整解压副本；失败回滚，不覆盖无关 workload |
| `code/scripts/profiling/profile_clip_transfer_ceiling.py` | CLIP batch16/64/256 的 R0 GPU-resident、R1 pinned FP16、R2 pageable FP32 逐 repeat CUDA-event/wall 诊断 | 分离 compute、H2D 与 ownership/dtype 边界；属于 synthetic ceiling，不作系统排名 |
| `deploy/autodl/image_project_static_formal.example.json` | 60K unique × 2 logical passes 下 project 8/16 preprocess actors × active16/32 的交错 1+3 矩阵 | 先冻结项目静态点；分开审计 unique/pass/processed rows，任何 formal 查询阶段不足 60 秒则 fail closed |
| `deploy/autodl/image_ray_data_native_crosscheck.example.json` | Ray Data 原生图在 cpu8 下对 batch16/64/256/512 做 60K unique×2 passes、交错 1+2 长稳态边界复核 | 只调官方 batch 参数；512 不比 64 改善 3% 即停止，不继续扫描 1024 |
| `deploy/autodl/image_documents_workload_key.sql` | 把 legacy `PRIMARY KEY(doc_id)` 原子迁移为 `(workload_name, doc_id)`，重复执行安全、未知 schema 拒绝 | 允许 COCO train/val 保留重叠 source ID；导入、source、correctness/writeback 统一 workload-scoped identity |
| `code/src/modalities/image/resource_budget.py` | 图像 Ray graph 的 source/preprocess/model CPU slot 精确账本与 affinity 超卖门禁 | 新增或调整 Daft/Ray Data/project actor/source 形状时复用；禁止只给常驻 actor CPU 而饿死 SQL reader |
| `code/src/baselines/image/provenance.py` | 图像 arm 的 upstream、实现来源、scheduler owner、自定义调度与 formal eligibility 合同 | 新增 baseline arm 或解释结果前读取；未登记来源的 arm fail closed |
| `code/scripts/profiling/profile_clip_preproc_stages.py` | 对历史 slow CLIP processor 的可见 method 子阶段计时 | 只用于解释 resize 占比；未归因时间不能写成具体转换主因 |
| `motivation/results/gpu/image_clip_preprocess_variants_20260801/` | 四种 CLIP processor/decode 边界的 720 条 raw repeats、manifest、日志与七步报告 | 判断 slow-path 动机能否外推到当前/fast 实现；不能当作 Daft/Ray E2E 方法结果 |
| `motivation/results/gpu/image_clip_native_baseline_20260801/` | 项目自写 fused Daft fractional-GPU UDF 校准、5000 图×3 operator-E2E、派生 summary 与七步报告 | 仅作历史机制诊断；不是官方/native baseline，不进入正式排名 |
| `motivation/results/gpu/image_host_path_screening_20260802/` | 线程/资源合同修复后的 preprocess、source、active-window 单因素 screening、schema v8 分段诊断及 `raw/` 原始 CSV/manifest | 查图像 feeding 木桶迁移、16 actor/active32 候选和 PCIe 初步 NO-GO；不可当 formal baseline 排名 |
| `motivation/results/gpu/image_clip_transfer_ceiling_20260802/` | R0/R1/R2 batch16/64/256×30 的 raw、summary 与七步 H2D/compute ceiling 报告 | pinned H2D约24–25GB/s、pageable/ownership 更重；synthetic diagnostic，不作系统排名或 PCIe 最终判决 |
| `motivation/results/gpu/image_embedding_parity_20260803/` | Daft built-in 与 project_ray 的 256 图逐行 embedding parity 摘要、七步报告，以及 `raw/normalized_contract_gate/` 中 schema v11 CSV/manifest、两份 `.npz` 与 per-row 数据 | 独立重算归一化后 cosine/近邻重合并冻结正式 baseline 的统一输出合同；capture 运行不可作性能排名 |
| `motivation/results/gpu/ray_data_calibration_20260803/` | Ray Data 原生 `map_batches` 的 5000 图 screening、60K unique×2 passes 长稳态 batch 复核、raw CSV/manifest 和 `long_crosscheck_summary.csv` | 冻结原生 baseline 的 batch64/cpu8/gpu2/source4；只用于独立校准，不与 Daft/project 跨系统排名 |
| `motivation/plans/image_host_data_path_bottleneck.md` | R0→R4 表示阶梯、低扰动/侵入式双轨计时与 CPU/Ray/PCIe/GPU GO/NO-GO 门槛 | 重测 image motivation、判断 GPU feeding 缺口来自哪一段时读 |
| `research/AGENTS.md` | 背景调研规则 | 写文献、资料依据时读 |
| `research/README.md` | 调研目录入口 | 了解 research/ 下有什么 |
| `research/literature_and_evidence_review.md` | 文献与官方资料依据 | 写调研、论文动机时读 |
| `research/existing_ai_operator_execution_chains.md` | 现有数据库 AI 算子四类执行形态、PolarDB/OceanBase/pgai/Daft/Ray 路线与严格 baseline 层级 | 比较外部系统路线、判断哪些数字可同表排名时读 |
| `research/knowledge_hub.md` | **知识库总汇**——按问题快速定位参考材料、已知结论和待研究缺口 | 开始设计、做决策前先读 |
| `research/vllm_continuous_batching_reference.md` | vLLM Continuous Batching 机制详解（调度器、APC、metrics） | 设计上游动态 batching 策略时读 |
| `experiments/plans/literature_driven_pipeline_optimization_guide.md` | 文献驱动的执行链优化指南 | 继续寻找优化点、审计三层 batch/Orca 式补位、设计完整 adaptive flush 或设置候选晋级/放弃条件时读 |
| `research/ray_actor_dynamic_batching_reference.md` | Ray Serve 动态 batching + Ray Core actor 模式 | 设计 Ray actor 自适应提交架构时读 |
| `research/inference_pipeline_interaction_literature.md` | 推理管线交互文献汇总 | 写相关工作、确认研究空白时读 |
| `motivation/AGENTS.md` | 动机实验规则 | 搭建 AI 算子场景或端到端动机测试前读 |
| `motivation/README.md` | 动机测试目录详细说明 | 了解 motivation/ 下有什么、怎么组织 |
| `motivation/plans/workloads.md` | 三类 AI 算子场景、动机测试和 idea-evaluator 评估 | 比较候选场景、决定下一步测试时读 |
| `motivation/plans/integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线 | 规划集成和测试时读 |
| `motivation/plans/image_host_data_path_bottleneck.md` | 逐层加入 H2D、Ray tensor、JPEG preprocess 和 DB/Daft，并预注册容量平台与瓶颈 GO/NO-GO | 设计图像数据路径动机复测时读 |
| `motivation/plans/ai_sql_surface.md` | 数据库 AI 算子现状、推荐业务场景、动机测试标准 | 搭建业务场景前读 |
| `motivation/benchmarks/fake_embed_pipeline.py` | fake `AI_EMBED(text)` 端到端动机测试脚本 | 验证 embedding / RAG 链路中的 fan-in 成本 |
| `motivation/benchmarks/workload_matrix.py` | 三类候选 AI 算子场景动机测试脚本 | 比较不同 AI 算子的瓶颈形态 |
| `motivation/benchmarks/granularity.py` | AI 算子粒度归因动机测试脚本 | 拆分 task/object/fan-in/invocation 的收益 |
| `motivation/benchmarks/backpressure.py` | AI 算子模型服务反压模拟脚本 | 验证 queue wait、token backlog、in-flight 和 backpressure |
| `motivation/results/README.md` | 动机测试结果阅读顺序和结论边界 | 讲解动机测试、整理实验结论时读 |
| `motivation/results/gpu/README.md` | 真实 GPU-backed E2E 结果入口 | 当前最优先引用的正式证据 |
| `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md` | GPU-backed embedding 链路拆分 | 引用 stage breakdown 和 fine/coalesced 对比 |
| `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md` | pgai-integrated GPU-backed rerun | 引用最新 rerun 结果 |
| `motivation/results/fake_cpu/analysis.md` | fake/CPU 历史预研分析 | 了解早期为什么关注 task/object/invocation/fan-in/backpressure |
| `motivation/results/pg18_4_fake/` | PG18.4 本地同构预演 | 只作为预演和历史信号，不代表真实 GPU-backed 结论 |
| `feasibility/AGENTS.md` | 可行性验证规则 | 做组件 benchmark 或环境验证前读 |
| `feasibility/README.md` | 可行性验证目录入口 | 了解 feasibility/ 下有什么、怎么组织 |
| `feasibility/benchmarks/README.md` | benchmark 说明和运行命令 | 运行组件 benchmark |
| `feasibility/results/README.md` | 可行性结果索引 | 查看组件验证、连接验证、smoke 结果 |
| `experiments/AGENTS.md` | 正式研究实验规则 | 设计优化实验、消融实验前读 |
| `experiments/README.md` | 正式研究实验入口 | 了解三项研究内容的实验规划 |
| `experiments/results/local_vllm_qwen15b_baseline/README.md` | 本地 vLLM Qwen2.5-1.5B `AI_COMPLETE` 静态行 batch baseline | 做 token-aware batching 和调度消融前，作为固定本地对照 |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv` | Local `AI_COMPLETE` ShareGPT/BurstGPT fixed row-batch rerun through Daft + Ray + vLLM | Baseline CSV for later data-organization and scheduling comparisons |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv` | Local `AI_COMPLETE` Daft + Ray + vLLM latency metric probe | Verifies batch token/latency and vLLM server-side metric collection |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv` | Local `AI_COMPLETE` token-budget versus fixed-row matrix | First policy comparison for data-organization experiments; 512 rows, local vLLM, no writeback |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv` | Local `AI_COMPLETE` preliminary arrival-aware K_max sweep | Single request-shape scheduling sweep; superseded by the batch policy x K_max matrix for static baseline selection |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv` | Local `AI_COMPLETE` batch policy x K_max matrix | Coupled fixed-row/token-budget and admission-control experiment; main static scheduling baseline before queue-adaptive flush |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv` | Local `AI_COMPLETE` foreground small-job K_max interference result | Small-job solo/bounded-bulk/unbounded-bulk CSV for shared-vLLM admission-control motivation |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv` | Local `AI_COMPLETE` background bulk-job K_max interference result | Bulk-job bounded/unbounded CSV paired with the foreground interference result |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_length_prefix_ablation_20260719.csv` | Local `AI_COMPLETE` length-align and prefix-aware ablation | First data-organization ablation with token-tail, service-tail, prompt-token spread, and prefix-ratio metrics |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_small_20260719.csv` | Local `AI_COMPLETE` foreground small-job shared-vLLM sweep | Formal K_max/adaptive interference sweep for foreground latency and vLLM queue pressure |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_bulk_20260719.csv` | Local `AI_COMPLETE` background bulk-job shared-vLLM sweep | Paired bulk-job CSV for background throughput and service-pressure tradeoff |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_small_20260719.csv` | Local `AI_COMPLETE` tuned adaptive foreground interference supplement | Shows adaptive downshift can trigger, but foreground latency remains worse than static K_max=8 |
| `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_bulk_20260719.csv` | Local `AI_COMPLETE` tuned adaptive bulk interference supplement | Paired bulk CSV recording adaptive downshifts, upshifts, and effective limit mean |
| `experiments/results/accelerated_arrival_flush_20260725/README.md` | 真实单 GPU 加速到达 flush 策略实验报告 | immediate / fixed-timeout / queue-adaptive 的设置、统计、负结果边界与下一步 |
| `experiments/results/accelerated_arrival_flush_20260725/manifest.json` | 加速到达实验复现清单 | 代码提交、镜像、版本、硬件、参数、门禁和中途服务恢复记录 |
| `experiments/results/accelerated_arrival_flush_20260725/formal_runs.csv` | 512 条 workload 的原始运行级结果 | 每策略 1 次预热 + 5 次正式重复，真实 PostgreSQL/pgvector/vLLM 版本 |
| `experiments/results/accelerated_arrival_flush_20260725/formal_run_metrics.csv` | 绘图友好的正式逐运行指标 | 15 条正式运行，含 observed tokens/s |
| `experiments/results/accelerated_arrival_flush_20260725/formal_metric_summary.csv` | 正式运行统计汇总 | 均值、样本标准差、95% t 置信区间、最小值和最大值 |
| `experiments/results/accelerated_arrival_flush_20260725/formal_*_trace.csv` | flush、submission 与 resource 原始轨迹族 | 保留逐重复身份，可用于后续时序图与 batch formation 图 |
| `experiments/results/accelerated_arrival_flush_20260725/scale_probe_runs.csv` | 1024 条同到达密度规模探针 | 每策略单次真实运行，用于判断扩大行数能否改变 batch formation |
| `experiments/results/adaptive_flush_window_20260725/README.md` | 双窗口 adaptive flush 改进实验报告 | 64/1024 门禁、512 行重复统计、正向候选证据与 claim boundary |
| `experiments/results/adaptive_flush_window_20260725/manifest.json` | 双窗口实验复现清单 | 代码提交、真实组件版本、硬件、参数、门禁和限制 |
| `experiments/results/adaptive_flush_window_20260725/formal_512_runs.csv` | 双窗口正式逐运行结果 | 每策略 1 次预热 + 5 次正式重复，保留全面 profiler 指标 |
| `experiments/results/adaptive_flush_window_20260725/formal_512_metric_summary.csv` | 双窗口正式统计汇总 | E2E、rows/s、tokens/s、submissions、batch rows 与 service P99 的均值、标准差和 95% CI |
| `experiments/results/adaptive_flush_window_20260725/*_trace.csv` | 双窗口 flush、submission 与 resource 轨迹 | schema 2 窗口字段、exactly-once 文档覆盖与绘图时序 |
| `experiments/results/request_lifecycle_gate_20260725/README.md` | 真实 64-prompt request lifecycle 基础设施门禁报告 | 逐请求 E2E/SLO、显式 submission 外键、seeded runner 审计与结论边界 |
| `experiments/results/request_lifecycle_gate_20260725/manifest.json` | seeded 门禁调度与完成清单 | 脱敏配置、随机顺序、逐运行命令、完成状态与 incident |
| `experiments/results/request_lifecycle_gate_20260725/runs.csv` | fixed/adaptive 真实运行级门禁数据 | vLLM token/success delta、request 分位数、SLO、batch 与资源指标 |
| `experiments/results/request_lifecycle_gate_20260725/*.requests.csv` | 每个场景 64 行逐请求 lifecycle | arrival/flush/submit/completion、E2E、SLO 和 request→submission 外键 |
| `experiments/results/request_lifecycle_gate_20260725/*.submissions.csv` | schema 2 submission trace | 显式 submission ID、doc coverage、token 与 backend service timing |
| `code_doc/superpowers/specs/2026-07-25-adaptive-flush-window-design.md` | Adaptive flush 双窗口改进设计 | fixed-timeout fallback、事件时间 catch-up、trace 与真实 GPU 门禁 |
| `code_doc/superpowers/specs/2026-07-25-ai-operator-execution-infra-design.md` | AI 算子外部执行 infra 总体设计 | 数据进入与组织、运行时控制、Ray 执行、可观测性、策略搜索及后续多模态/代价估计边界 |
| `code_doc/superpowers/specs/2026-07-26-output-aware-bfd-design.md` | 输出成本与离线 BFD 设计 | 共享成本语义、确定性 BFD、global/local scope、trace 元数据证据边界与真实组件门禁 |
| `code_doc/superpowers/specs/2026-07-26-row-cap-aware-packing-and-observation-design.md` | Row-cap-aware packing 与非阻塞观测设计 | 将 BFD 降为候选对照，修复 adaptive 观测阻塞风险，并以真实 64→512→1024 门禁选择策略 |
| `code_doc/superpowers/specs/2026-07-26-multi-endpoint-routing-readiness-design.md` | 多 endpoint 路由就绪设计 | 修复 equal-load 偏置、增加 estimated-work 候选，并明确逻辑双 endpoint 与未来真实多 GPU 的证据边界 |
| `code_doc/superpowers/specs/2026-07-26-ray-vllm-execution-tuning-design.md` | Ray 与 vLLM 执行层调优设计 | CUDA Graph、task/actor 并发、服务容量、endpoint-local actor pool 和 prefix-cache 分阶段真实门禁 |
| `code_doc/superpowers/specs/2026-07-28-dual-gpu-experiment-correctness-design.md` | 双 GPU 实验正确性与共享调度设计 | 修复 token-budget/row-cap 混淆、组织/提交指标语义和多 job 共享 Ray cluster 门禁 |
| `code_doc/superpowers/specs/2026-07-29-saturated-ray-actor-pool-replenishment-design.md` | 饱和 active-work、service quantum 与 Ray actor pool 补位设计 | 先标定 GPU 饱和点，再以固定 work/总 slots 消除 whole-submission HOL，验证有界 actor pool 与 endpoint-local completion replenishment |
| `code_doc/superpowers/plans/2026-07-25-adaptive-flush-window-implementation.md` | Adaptive flush 双窗口实施计划 | TDD 窗口选择、event-time replay、profiler trace 与真实单 GPU 分级门禁 |
| `code_doc/superpowers/plans/2026-07-25-request-lifecycle-scenario-runner-implementation.md` | AI 算子执行 infra 第一阶段实施计划 | request lifecycle、单 prompt E2E/SLO、seeded scenario runner 与真实 64 行门禁 |
| `code_doc/superpowers/plans/2026-07-26-output-aware-bfd-implementation.md` | 输出成本与确定性 BFD 实施计划 | 共享成本、通用 cost_units BFD、Arrow/Daft 接入、离线 lifecycle、真实 64→512 单 GPU 门禁 |
| `code_doc/superpowers/plans/2026-07-26-row-cap-aware-packing-and-observation-implementation.md` | Row-cap-aware packing 与非阻塞观测实施计划 | TDD 接线、BFD 机制级消融、真实 64 行门禁、512 筛选与 1024 held-out 确认 |
| `code_doc/superpowers/plans/2026-07-26-single-gpu-text-closure-implementation.md` | Single-GPU text evidence closure plan | Execute cross-rate, 2048 held-out, controlled prefix, and cost-estimation tasks |
| `code_doc/superpowers/plans/2026-07-26-ray-execution-foundation-implementation.md` | Ray 执行基础实施计划 | TDD 解耦 endpoint/actor worker，接入 CPU/并发/零 GPU/零重试资源契约并完成全量验证 |
| `code_doc/superpowers/plans/2026-07-26-vllm-ray-tuning-experiments.md` | vLLM 与 Ray 调优实验计划 | 分阶段完成 CUDA Graph、task/actor、vLLM capacity 真实单 GPU 门禁与重复实验 |
| `code_doc/superpowers/plans/2026-07-28-dual-gpu-experiment-correctness-implementation.md` | 双 GPU 实验正确性实施计划 | TDD 修复服务元数据、组织/提交指标、共享 Ray cluster 契约与 AutoDL 分阶段模板 |
| `code_doc/superpowers/plans/2026-07-29-saturated-ray-execution-foundation-implementation.md` | 饱和 Ray 执行基础实施计划 | TDD 实现 runner 独占、Ray 失败清理、扩展 active-work 曲线、固定 service quantum 与有界可观测 actor pool，再分两次远端 gate 验证 |
| `code_doc/superpowers/plans/2026-07-29-slo-aware-ewma-flush-implementation.md` | SLO-aware EWMA flush 实施计划 | TDD 实现 oldest-slack、arrival/service EWMA、deadband 与 stale fallback，并在高压/临界双 GPU 负载下做固定资源消融 |
| `code_doc/superpowers/plans/2026-07-29-shared-vllm-fairness-implementation.md` | Shared-vLLM 1/2/4-job 实施计划 | 全局 credit 观测、group runner、双 GPU gate 与 formal 门槛 |
| `code/src/planning/costs/regression.py` | Engine-independent grouped split, ridge cost model, and regression metrics | Build offline operator-cost estimates without post-execution feature leakage |
| `code/scripts/analysis/estimate_operator_cost.py` | Reproducible profile-CSV cost-estimation CLI | Generate model schema, coefficients, splits, and held-out metrics |
| `code/scripts/analysis/summarize_formal_repeats.py` | Formal repeat CI/CV and paired regression-count postprocessor | Run after every new-schema formal matrix before writing claims or figures |
| `code/scripts/analysis/evaluate_embedding_retrieval.py` | Exact cosine AI_EMBED retrieval-quality evaluator using explicit relevance | Run on diagnostic saved embeddings; reports Recall@K/MRR/nDCG and excludes self |
| `code/scripts/experiments/run_kmax_interference_experiment.py` | Shared-vLLM K_max interference runner | Starts background bulk and foreground small jobs against the same vLLM endpoint |
| `code/scripts/experiments/run_shared_vllm_experiment.py` | Shared-vLLM 正式 group runner | 同步启动 1/2/4 job，隔离 per-job trace 并生成组级指标/manifest |
| `code/scripts/baselines/run_official_baseline.py` | 同条件 Chat Completions baseline 薄入口 | 执行 immutable endpoint shard、归一化 vLLM Bench、验证 exactly-once/双 endpoint gate |
| `code/scripts/baselines/squad_capability_gate.py` | SQuAD v1.1 dev capability gate（DuckDB-ai arm）；全量/分层双模式、确定性分层抽样（largest-remainder + 多答案 max SQuAD-normalized 桶）、sample manifest + 逐行证据 CSV（sample hash 与 EM/F1 可复算）、canonical content hash 对齐 importer provenance、workload 完整性 fail-closed、vLLM counter 归因门禁、full-set exactly-once、命令/异常脱敏、失败结构化归档 | 验证 SQuAD bounded-output 管线（输出解析/EM-F1/错误统计），不发布排名 |
| `code/scripts/baselines/squad_database_e2e_runner.py` | SQuAD bounded-output database-E2E **单 endpoint**顶层 runner（DuckDB-ai + direct_client + project_static）。project_static shell-out profiler，使用独立 completion evidence、实际 source-scan prompt fingerprints、DB/importer 完整性读取与 sink readback 形成非循环证据链；报告强制写 endpoint_count=1 / multi-endpoint method=false | 三臂可运行；project_static 同时受统一计时墙阻塞与单 endpoint 方法退化约束，只能做正确性/管线开销 diagnostic，不能证明 endpoint-aware 方法 |
| `code/src/baselines/common/squad_identity.py` | SQuAD gate/runner 共享 helper（identity/attribution/integrity：`_pg_server_identity`/`_gpu_identity`/`_vllm_version`/`_git_commit`/`_scrape_status`/`_endpoint_idle`/`_assess_attribution`/`_structured_content_hash`/`_validate_workload_integrity`/`_load_importer_provenance`）| capability gate 与 E2E runner 共用，禁止第三份拷贝 |
| `code/src/baselines/common/redact.py` | 命令行与连接串脱敏共享模块（`redact_argument_list`/`redact_database_url`/`redact_text`）| 任何把 sys.argv / DSN / 异常文本写入 evidence 的 gate 或 runner 复用，禁止第三份拷贝 |
| `code/src/baselines/text/orchestration/postgres_manifest.py` | 正式 PostgreSQL workload 的不可变 baseline manifest 导出核心 | 按 workload/doc_id/limit/offset 读取完整行，固定 output 代价语义、source hash 与 endpoint 分片前输入 |
| `code/src/baselines/text/controls/batched_completions.py` | 无 Ray 的持久异步 fixed-row multi-prompt Completions control | 同协议标定 HTTP packing 上限，验证每个完整 prompt exactly-once 后再测试项目 token-budget；不作为 native baseline |
| `code/src/baselines/README.md` | 文本 comparison harness 分层与代码归属 | 修改 baseline adapter 或调度归属前读取，防止 control/native 混写 |
| `code/ARCHITECTURE_REFACTOR_PLAN.md` | 源码域、文本/图像正交边界、依赖方向和分阶段迁移状态 | 调整目录、拆大文件或移动 scripts/tests 前读 |
| `code/scripts/{data,services,baselines,profiling,experiments,analysis,environment}/` | 按职责分组的稳定 CLI 入口 | 运行脚本前先按任务类型定位；可复用逻辑必须留在 `src/` |
| `code/tests/{data,planning,scheduling,serving,modalities,observability,baselines,experiments,infrastructure,architecture}/` | 镜像生产职责的测试目录 | 使用 `unittest discover -s code/tests -t code` 做递归发现 |
| `code/src/baselines/common/provenance.py` | 文本 arm 原生性与来源 fail-closed 合同（`ComparisonRole` 含 `project_scheduled_method`——项目方法 under test，区别于 baseline/control）| 每个 summary/gate 写入 role、scheduler owner、custom scheduling、formal eligibility 与 upstream source；project 方法强制 `custom_scheduling_code=True`/`formal_baseline_eligible=False` |
| `code/src/baselines/text/frameworks/` | Daft prompt / Ray Data vendor-native runtime adapters | 框架拥有 batching/backpressure；只做 workload payload/response 适配，不注入项目调度 |
| `code/src/baselines/text/ceilings/` | vLLM Bench 官方服务容量上限 | 只衡量 service ceiling，不冒充数据库/框架 baseline |
| `code/src/baselines/text/controls/` | bounded Chat/Completions 项目自写 direct controls | 隔离 feeding/HTTP packing；`formal_baseline_eligible=false` |
| `code/src/baselines/text/products/` | DuckDB-ai / OceanBase 等数据库产品原生 adapter + direct_client 直连服务 control + project_static shell-out wrapper（按"每次模型调用的执行风格"共置；direct_client 是 `direct_client_control`，project_static 是 `project_scheduled_method`，均 `custom_scheduling_code=True`、非产品 baseline） | 产品 adapter 需真实 SQL AI Function + capability gate 通过；direct_client/project_static 是 project-scheduled，`formal_baseline_eligible=false` |
| `code/src/baselines/text/products/direct_client.py` | direct_client 臂：httpx async + `asyncio.Semaphore(32)` 固定并发、per-request `/v1/chat/completions`，复用 `build_completion_request_body`；暴露 `finish_reason`/`completion_tokens`/per-request latency（DuckDB-ai 不暴露） | 揭示同 cap 下"截断的产品语义"差异（partial text vs NULL）；`direct_client_control` + `custom_scheduling_code=True`（Semaphore 是项目调度代码），非产品 baseline |
| `code/src/baselines/text/products/project_static.py` | project_static 无连接 wrapper：子进程调用 profiler；completion/source-scan/summary 三类证据 fail-closed；强制 effective K、65K 类 token-work credit、raw chat/temperature=0/httpx async/fixed-output-cap/prefix-cache 声明 | 论文方法 under test；`project_scheduled_method`，非 vendor baseline/control；`scheduler_owner=project_ray_frozen_static` |
| `code/tests/baselines/text/test_baseline_provenance.py` | 文本 native baseline 资格单测 + direct_client/project_static 诚实性回归（custom_scheduling/非 baseline/角色正确）| 阻止项目自写 scheduler 被标记为 vendor-native；project_static 不得标为 direct_client_control 或任何 native baseline |
| `code/tests/baselines/common/test_partition_policy.py` | manifest 分片策略 + policy-aware gate 单测（equal_rows 128:128/奇数差≤1/顺序不变/同 seed 同结果/duplicate fail-closed；CLI 两 policy 路由+元数据；equal_rows 不因 work-skew 失败、work-balanced 在 skew>2% 失败）| 多卡静态分片 baseline（DuckDB 2×1 / bounded_static_2x1）的分片+门禁改动后运行 |
| `code/src/infrastructure/runtime_env.py` | driver、multi-job subprocess 与 Ray worker 的共享 PYTHONPATH/数值线程环境 | 防止 1/2/4-job 因每进程 OpenBLAS 线程膨胀而在请求前耗尽 OS 线程 |
| `code/src/infrastructure/config_env.py` | 文本、图像、shared-vLLM 和 baseline 配置共用的严格 `${ENV_VAR}` 展开 | unset 立即失败；完整 scalar 保留 JSON 数值/布尔类型 |
| `code/src/infrastructure/environment.py` | machine profile、Python 能力与模型/数据资产检查/补齐核心 | 默认只读，安装下载由薄 CLI 显式触发 |
| `code/src/experiments/shared_vllm/` | Shared-vLLM 编排包：config/runner/runtime/evidence/metrics | 配置校验、三臂 credit 语义、并发执行、exactly-once、资源证据与公平性汇总 |
| `figures/AGENTS.md` | 图表长期规则 | 做图、改图、审查图前必读 |
| `figures/README.md` | 图资产入口 | 查找正式图、备份图和绘图脚本 |
| `learning/AGENTS.md` | 学习讲解规则 | 写学习材料前读 |
| `learning/README.md` | 学习材料入口 | 了解实验 walkthrough 和术语讲解 |
| `learning/code_architecture_guide.md` | 公共执行阶段、文本/图像模态和 baseline 隔离的代码导读 | 理解重构后的 `src` 目录与迁移验证方法 |
| `learning/experiment_walkthrough.md` | 按推进顺序讲解已完成实验 | 学习实验链路、参数和结果读法 |
| `learning/metric_selection_methodology.md` | AI_EMBED vs AI_COMPLETE 观察变量选择方法论 | 理解为什么从"阶段时延拆分"转向"多维分布表征" |
| `opening/AGENTS.md` | 开题工作规则 | 写开题报告、PPT、飞书材料前读 |
| `opening/README.md` | 开题工作区入口 | 了解开题材料分布和同步规则 |
| `opening/navigation.md` | 开题材料导航 | 不知道开题材料在哪时读 |
| `opening/report/opening_report.md` | 开题报告正文 | 写报告、和导师沟通、定方向 |
| `opening/literature/reading_list.md` | 开题文献精读清单 | 查看文献精读优先级和引用边界 |
| `opening/literature/top15_reading_notes/` | 开题精读 Top 15 拷贝 | 15/15 严格 CCF-A 正式 research paper 的自包含快照，权威版在 `research/reading_notes/` |
| `research/reading_notes/` | 精读笔记权威库（49 篇 + 模板） | 所有精读笔记单一来源；新增 VTC、Llumnix、LOTUS、Palimpzest、Abacus、SemBench、FairServe、DLPM、Autellix、Chiron |
| `research/reference/` | 当前可解析参考 PDF（21 份） | Top 15 PDF 15/15 齐全；题录和版本索引见 `research/reference/REFERENCE_INDEX.md` |
| `research/ai_operator_literature_inventory.md` | 文献分级清单 | 查看 Top 15、核心补充、题录勘误、baseline 与代价估计关系 |
| `research/top15_ranked_papers.md` | 开题 Top 15 | 15/15 CCF-A 正式论文；按 AI 算子、LLM 调度、Ray、代价估计组织 |
| `research/gpu_scheduler_data_placement_supplement_20260715.md` | GPU 调度与数据放置补充调研 | 查看策略控制器设计的前沿系统依据、可借鉴思想和后续精读清单 |
| `research/evaluation_metrics_survey_20260731.md` | AI 算子/推理服务文献 + 数据库厂商评估指标调研与 gap 分析 | 设计新实验指标、对照文献标准指标时读；P0 缺口 TTFT/ITL/prefix-cache-hit-rate |
| `research/daft_db_gpu_bridge_direction_scope_20260731.md` | 方向 reframe scope：DB↔GPU 经 Daft 桥接 + 三痛点 + offline-batch 候选；已撤回传输瓶颈/结构性空白预设 | 方向/题目讨论、Daft 痛点、workload 选型时读；以 staged baseline 后的证据为准 |
| `research/reference/README.md` | 本地 PDF 状态 | 查看 21 份实体 PDF、Top 15 完整性和维护规则 |
| `research/reference/REFERENCE_INDEX.md` | 权威题录索引 | 查看 DOI、正式轨道、核心补充级别和工程资料入口 |
| `data/README.md` | 本地 workload 数据说明；raw payloads 被 git ignore | 查看 ShareGPT/BurstGPT 下载位置、用途和边界 |
| `code/AGENTS.md` | 正式工程代码规则 | 后续迁移可复用代码前读 |
| `code/tests/architecture/test_architecture_boundaries.py` | AST 导入边界与旧兼容入口 fail-closed 门禁 | 新增模块、改变跨层依赖或迁移路径后运行 |
| `code/src/data/sources/postgres_text.py` | PostgreSQL data source 后端：psycopg/Arrow baseline、Daft SQL entry、`doc_id`/`arrival_time` source order | 切换或修改数据入口与读取顺序时读 |
| `code/src/data/materializers/text.py` | ArrowOrganizer / DaftOrganizer 数据组织后端 | 接入或比较 Arrow 与 Daft 文本数据组织路径时读 |
| `code/src/modalities/text/costs.py` | 与引擎无关的 prompt/output 成本模式解析 | 修改 prompt-only、固定输出上限或 trace 输出成本语义前读 |
| `code/src/infrastructure/runner_lease.py` | 场景输出目录的原子单写者租约、owner 身份校验和显式 stale recovery | 修改 runner 幂等、恢复或 manifest/CSV 单写者边界前读 |
| `code/src/observability/profiling/` | profiler 应用子包：CLI/config、正式 schema/trace、replay 和 Ray 接线；旧根级 `profile_*.py` 已删除 | 修改画像应用参数、运行接线或结果契约前读 |
| `code/src/observability/profiling/traces.py` | profiler control/flush/submission/request/resource 的版本化 CSV 序列化 | 修改 trace schema、生命周期标识或 endpoint/GPU 归因前读 |
| `code/src/observability/profiling/cli.py` / `config.py` / `schema.py` | profiler 参数面、CLI/env 解析与正式汇总 schema 的独立边界 | 修改运行参数、环境切换优先级或 runs.csv 字段前读 |
| `code/src/observability/profiling/replay.py` | Arrow planning-batch/request/service-quantum envelope、arrival replay 与 lifecycle seed 组装 | 修改 token-budget 关批、complete-row quantum、request 粒度补位或 replay 时间语义前读 |
| `code/src/observability/profiling/manifest_guard.py` | 同条件 project runtime 的 fail-closed manifest 行语义、payload 契约与固定 endpoint 证据 | 修改 direct/project 公平比较、source offset 或 manifest 映射时读 |
| `code/src/observability/profiling/ray.py` | Ray task/actor submitter、typed scheduler、credit 释放与 fan-in 接线 | 修改 actor pool、request-level replenishment 或 Ray 资源语义前读 |
| `code/src/planning/packing/scalar.py` | 与模态无关的确定性 BFD 标量容量装箱与指标 | 修改离线 batch membership、超预算行处理或 packing 指标前读 |
| `code/src/serving/backends/` | common 合同、fake/HTTP embedding、vLLM-compatible async completion 与 Ollama backend | 修改模型服务接入、vLLM/Ollama endpoint 或 AI_COMPLETE backend 前读 |
| `code/src/modalities/image/` | 图像 typed batch/result/semantics、lazy Daft source、CLIP preprocess、bounded Ray CPU→GPU pipeline 与输出审计；baseline 已隔离到 `baselines/image/` | 实现 image path-B、切换 backend 或审计 embedding/执行语义前读 |
| `code/src/modalities/image/resource_sampling.py` | host per-core CPU、visible/active-device GPU 的低频采样与明确汇总语义 | 图像 E2E 资源采样；不能把低频 GPU util 当 MFU |
| `code/src/data/sinks/postgres.py` | `none/json_text/pgvector` embedding 写回与 completion JSON-text 写回 | 修改写回路径或后续接 Lance sink 前读 |
| `code/src/observability/metrics/` | timing/CSV/statistics/resources/vLLM/retrieval/SQuAD EM-F1 指标子模块及兼容包入口 | 修改 profiling 指标、资源效率、检索/问答质量、CSV 输出或计时边界前读 |
| `code/src/data/workloads/text.py` | 内置 synthetic / controlled workload seed | 仅用于 smoke/dev；最终 baseline 优先用 ShareGPT/BurstGPT importer |
| `code/src/experiments/scenarios/core.py` | 可复现的 warm-up / formal 场景交错顺序生成器 | 修改实验随机化与运行顺序前读 |
| `code/src/scheduling/` | Daft→Arrow→Ray 正式链路中的 typed scheduling core；按 `core/`、`organization/`、`submission_control/`、`endpoint_routing/`、`runtime/` 分包，旧根级兼容模块已删除 | 实现或审查运行时策略前读 |
| `code/src/scheduling/core/errors.py` | 可重试 endpoint capacity 背压与终止性调度错误的 typed 边界 | 修改健康/容量语义或 scheduler retry 控制流前读 |
| `code/src/scheduling/organization/` | 上游 static/service-quantum token-budget 决策 | 修改数据组织预算控制或动态安全动作集前读 |
| `code/src/scheduling/organization/service_quantum.py` | 将 planning batch 按预测 work 切成不拆单行的有界 service-completion 单元 | 修改 HTTP/Ray completion 粒度、whole-submission HOL 或 quantum 超预算语义前读 |
| `code/src/scheduling/submission_control/` | static/adaptive admission、active-work 与多 job shared fair credit | 修改提交反压、公平性或 endpoint capacity 语义前读 |
| `code/src/scheduling/endpoint_routing/` | round-robin、least-queued、least-work、manifest-pinned、prefix-affinity 路由 | 修改多 endpoint 选择策略前读 |
| `code/src/scheduling/runtime/` | 有界 Ray actor worker pool、submit/complete adapter、worker contract、metrics observation cache 与 named credit actor | 修改 Ray worker slots、worker routing、completion cleanup 或服务观测接线前读 |
| `code/scripts/data/import_ai_complete_workload.py` | ShareGPT prompt + BurstGPT trace 归一化导入脚本；支持显式 prompt-token eligibility、按过滤后 offset 选择不重叠 suffix、逐字段核验既有 prefix 和 append-only 防覆盖 | 构造最终可比 `AI_COMPLETE` baseline workload 或补 held-out 行前运行 |
| `code/scripts/data/import_squad_workload.py` | SQuAD v1.1 validation/dev 专用 importer（bounded-output AI_COMPLETE 主对比轨）；锁定 prompt 模板、cap=64、fail-closed 校验 canonical SHA256 + 行数，reference_answers JSONB 多答案 + source_example_id；写 provenance（版本/split/SHA256/官方 repo/revision/下载方式/content hash/importer commit） | 跑 SQuAD bounded-output 三臂对照前导入；篡改文件会被 SHA256 门禁拒。配合 `code/tests/data/test_import_squad_workload.py` |
| `code/scripts/data/import_bounded_output_workload.py` | 通用 bounded-output 包装 importer（`--template` 支持任意 wrap，把源 workload 包成短输出 workload）；幂等、新 doc_id base 避 PK 冲突 | 构造句子计数等 microbenchmark workload |
| `code/scripts/experiments/run_ai_operator_scenarios.py` | 带空闲门禁、失败审计和原子 manifest 的 seeded 场景运行器 | 执行随机化策略对比或真实基础设施门禁前运行 |
| `code/tests/infrastructure/test_runner_lease.py` | runner 活跃 owner、stale recovery、fingerprint 与租约释放测试 | 修改场景 runner 恢复或单写者边界后运行 |
| `code/scripts/analysis/summarize_output_aware_bfd.py` | output-aware BFD 重复实验的长表统计汇总 | 汇总吞吐、E2E、packing、GPU、能耗与 MFU 正式结果时运行 |
| `code/tests/data/test_sources.py` | data source 查询构造和 source factory 单元测试 | 修改数据入口行为后运行 |
| `code/tests/planning/test_organizers.py` | 数据组织后端最小单元测试 | 修改 organizer 接口或 batch 行为后运行 |
| `code/tests/modalities/text/test_request_costs.py` | 输出成本模式、来源标签与严格输入校验 | 修改成本估计语义后运行 |
| `code/tests/planning/test_packing.py` | 确定性 BFD、超预算单行与 packing 汇总测试 | 修改装箱算法后运行 |
| `code/tests/planning/test_cost_context_loo.py` | 候选 repeat 聚合、宏统计与 pooled selection 口径测试 | 修改 context-LOO 证据或晋级聚合时运行 |
| `code/tests/experiments/test_output_aware_summary.py` | 正式重复实验长表汇总与 warm-up/失败过滤测试 | 修改 output-aware 汇总脚本后运行 |
| `code/tests/serving/test_model_backends.py` | 模型后端最小单元测试 | 修改 fake 或 compatible HTTP embedding backend 后运行 |
| `code/tests/serving/test_vllm_clip_pooling_gate.py` | vLLM 0.25/旧 pooling 输出解析和输入 fail-closed 测试 | 修改 CLIP capability worker 时运行 |
| `code/tests/serving/test_vllm_clip_pooling_harness.py` | gate 正常退出与整进程组 timeout=124 测试 | 修改跨机器超时监管时运行 |
| `code/tests/modalities/image/test_image_contracts.py` | 图像 embedding shape/finite、CLIP v5 pooler output、work units 和 lazy source query 测试 | 修改 `code/src/modalities/image/` 后运行 |
| `code/tests/modalities/image/test_image_clip_preprocess_variants.py` | 图像 processor 对照脚本的 spatial-work 与 embedding parity 计算测试 | 修改图像受控复测脚本后运行 |
| `code/tests/modalities/image/test_image_execution.py` | 图像 streaming exactly-once、向量归一化和执行时间边界测试 | 修改图像 E2E baseline/pipeline 后运行 |
| `code/tests/modalities/image/test_image_resource_sampling.py` | visible/active GPU 与 host busy-core 汇总测试 | 修改图像资源指标后运行 |
| `code/tests/baselines/image/test_image_baseline_contract.py` | vendor-native eligibility、scheduler ownership 与诊断 reference fail-closed 测试 | 新增/改名图像 baseline arm 后运行 |
| `code/tests/baselines/image/test_image_vendor_baseline_manifest.py` | 官方图像 baseline commit/file pin 与“禁止项目调度”适配策略测试 | 更新 upstream pin 或 vendor parity 合同时运行 |
| `code/tests/data/test_sinks.py` | 写回后端最小单元测试 | 修改 sink/writeback 行为后运行 |
| `code/tests/data/test_workloads.py` | 内置 workload seed 单元测试 | 修改 smoke/dev workload 后运行 |
| `code/tests/scheduling/test_token_budget_controller.py` | static/service-quantum budget 与 arrival EWMA 契约测试 | 修改动态预算选择规则后运行 |
| `code/tests/scheduling/test_shared_credit.py` | 多 job endpoint credit、借用、公平轮转与 ID 隔离测试 | 修改共享 admission 纯策略后运行 |
| `code/tests/scheduling/test_shared_credit_ray.py` | named Ray actor 复用与配置一致性边界测试 | 修改共享 credit 的 Ray ownership 接线后运行 |
| `code/tests/experiments/test_shared_vllm_experiment.py` | Shared-vLLM 配置、容量语义、组级指标与公平性测试 | 修改多 job runner 后运行 |
| `code/tests/data/test_import_ai_complete_workload.py` | ShareGPT/BurstGPT importer 单元测试 | 修改 importer 或 trace 过滤逻辑后运行 |
| `code/tests/data/test_import_squad_workload.py` | SQuAD v1.1 dev importer 单元测试（多答案/特殊字符/重复 ID/缺答案/content hash/行数门禁/canonical SHA256 门禁/模板）| 修改 SQuAD importer 后运行 |
| `code/tests/baselines/test_duckdb_ai_sentence_count_gate.py` | 句子计数 gate 纯函数单测（句子切分/整数 fullmatch/行数门禁/dist）| 修改 sentence-count gate 后运行 |
| `code/tests/baselines/test_squad_capability_gate.py` | SQuAD capability gate 纯函数单测（largest-remainder 配额/多答案桶/确定性分层/order 不变/structured hash 对齐 importer/workload 完整性/归因/脱敏 wiring）| 修改 stratified_sample / integrity / attribution / redact 后运行 |
| `code/tests/baselines/test_squad_database_e2e_runner.py` | database-E2E runner 单测（sink adapter shape+sidecar/runner 指标除法/零墙安全/mocked E2E barrier 的计时块与 3 状态字段/fail-closed 与 eligibility 分离）| 修改 `squad_database_e2e_runner.py` 或 `_results_to_sink_payload`/`_runner_metrics` 后运行 |
| `code/tests/baselines/text/test_direct_client.py` | direct_client 臂单测（DirectClientConfig 校验/`_validate_requests` 多 endpoint+cap 拒绝/run_direct_client cap-mismatch 前置拒绝）| 修改 `direct_client.py` config 校验或 `_validate_requests` 后运行 |
| `code/tests/baselines/text/test_project_static.py` | project_static wrapper 单测：完整冻结 argv、effective K、active-work、请求语义、completion/source-scan 证据解析与重复/畸形 fail-closed | 修改 `project_static.py` argv 或证据合并后运行 |
| `code/tests/observability/test_completion_evidence_trace.py` | profiler completion evidence + source-scan fingerprints 单测；完成行缺输出、批内数量错、重复 doc_id 均 fail-closed | 修改 profiler trace/evidence writer 后运行 |
| `code/tests/baselines/common/test_redact.py` | 共享脱敏模块单测（DB-URL/arg list/URL flag/redact_text）| 修改 `src/baselines/common/redact.py` 后运行 |
| `code/tests/environment/test_scan_git_secrets.py` | Git 隐私扫描器纯函数单测（私钥/token/sshpass/外部 host 拦截；localhost/模板/example host 放行）| 修改 `scan_git_secrets.py` 拦截规则后运行 |
| `code/tests/scheduling/test_scheduling_models.py` | scheduling request/endpoint/topology schema 单元测试 | 修改 typed scheduling metadata 前运行 |
| `code/tests/scheduling/test_scheduling_policies.py` | static admission 与 round-robin routing 单元测试 | 修改 admission/routing baseline 前运行 |
| `code/tests/scheduling/test_scheduler.py` | bounded-inflight 与 exactly-once deterministic scheduler 测试 | 修改 scheduler orchestration 前运行 |
| `code/tests/scheduling/test_adaptive_admission.py` | AIMD、EWMA-AIMD、PID、UCB 控制律、边界与 reward 单元测试 | 修改动态 admission controller 前运行 |
| `code/tests/scheduling/test_dynamic_admission.py` | 缓存采样、stale hold、typed trace 与动态降窗调度不变量测试 | 修改 observation provider 或 dynamic gate 前运行 |
| `code/tests/scheduling/test_flush_policies.py` | immediate、fixed-timeout、queue-adaptive flush 与 hard max-wait 单元测试 | 修改独立 flush policy 前运行 |
| `code/tests/scheduling/test_runtime_batching.py` | pending batch、token membership、单调 arrival replay 与 flush deadline 单元测试 | 修改回放事件循环或 batch builder 前运行 |
| `code/tests/scheduling/test_ray_adapter.py` | 通用 Ray submission adapter 的 request identity 与 endpoint 映射测试 | 修改 Ray adapter 前运行 |
| `code/tests/observability/test_postgres_profile_scheduling.py` | profiler 静态 Ray task/actor 接线、路由与旧指标兼容测试 | 修改 profiler 提交路径前运行 |
| `code/tests/observability/test_metrics.py` | 资源/MFU、检索/SQuAD 质量汇总与 CSV header/schema-safe append 测试 | 修改指标输出、质量评估或追加契约前运行 |
| `code/tests/experiments/test_kmax_interference_script.py` | shared-vLLM K_max runner 默认输出 schema 版本测试 | 修改干扰实验默认输出路径前运行 |
| `code/tests/scheduling/test_scheduling_daft_ray_contract.py` | 真实 DaftOrganizer→Arrow RecordBatch→arrival replay→单节点 Ray task/actor exactly-once contract | 修改 Daft/Ray/replay adapter boundary 前运行 |
| `code/tests/scheduling/test_request_lifecycle.py` | request/submission exactly-once join、时钟域与 SLO 语义测试 | 修改逐请求 lifecycle schema 或计时边界前运行 |
| `code/tests/experiments/test_experiment_scenarios.py` | seeded schedule、runner 脱敏、失败即停与 manifest 测试 | 修改场景运行器或实验顺序前运行 |
| `code/scripts/profiling/postgres_ai_operator_profile.py` | PostgreSQL→Daft→Ray→模型服务画像；CSV 记录 endpoint/actor worker、Ray 资源与提交计数契约 | 运行或审计正式 AI 算子链路前读 |
| `code/scripts/profiling/daft_text_organizer_smoke.py` | Daft/Arrow organizer smoke 入口 | 验证文本阶段 Daft 最小接入 |
| `code/scripts/README.md` | 脚本详细说明 | 运行 PostgreSQL 画像、pgai SQL profile、本地 embedding server、Daft text organizer smoke |
| `code_doc/superpowers/plans/` | Superpowers implementation plans for code work | 按 superpowers 工作流执行多步代码任务前读 |
| `code_doc/superpowers/plans/2026-07-25-adaptive-admission-controller-design.md` | RC2 adaptive admission controller 与 shared-vLLM 实验设计 | 实现 AIMD 控制器、时序指标或重跑干扰实验前读 |
| `code_doc/superpowers/plans/2026-07-25-runtime-scheduling-strategy-suite-design.md` | 动态 batching、flush、控制器族、actor pool、endpoint routing、联合搜索和指标总设计 | 实现完整运行层策略框架与单 GPU 对照实验前读 |
| `code_doc/superpowers/plans/2026-07-25-scheduling-foundation-implementation.md` | typed schema、topology、静态 admission/routing 与 deterministic scheduler 的 TDD 实施计划 | 开始运行层策略框架第一阶段代码前读 |
| `code_doc/superpowers/plans/2026-07-25-ray-static-wiring-implementation.md` | typed scheduler 接入生产 static Ray task/actor 路径的 TDD 计划 | 重构 profiler Ray 提交循环前读 |
| `code_doc/superpowers/plans/2026-07-25-adaptive-controller-family-implementation.md` | AIMD、EWMA-AIMD、PID、UCB 控制器族及 Ray 接入的 TDD 计划 | 实现或审查动态 admission controller 前读 |
| `code_doc/superpowers/plans/2026-07-25-arrival-replay-flush-runtime-implementation.md` | arrival replay、pending batch 与独立 flush runtime 的 TDD 计划 | 运行 queue-adaptive flush 单 GPU 实验前读 |
| `code_doc/superpowers/plans/2026-07-25-accelerated-arrival-replay-implementation.md` | arrival time scale 的 TDD 接线、真实门禁与正式单 GPU flush 矩阵 | 执行加速回放正式实验前读 |
| `code_doc/superpowers/specs/2026-07-25-accelerated-arrival-replay-design.md` | BurstGPT accelerated replay 的缩放语义、验证要求与单 GPU 正式实验矩阵 | 实现时间缩放参数或解释加速回放结果前读 |
| `code_doc/superpowers/specs/2026-07-26-output-aware-bfd-design.md` | output-aware BFD、成本来源、全局/局部范围与可比性设计 | 实现或解释动态 batch 装箱策略前读 |
| `code_doc/superpowers/specs/2026-07-26-row-cap-aware-packing-and-observation-design.md` | row cap、token budget、packing 与 adaptive 非阻塞观测的候选选择设计 | 继续数据组织与提交控制联合优化前读 |
| `code_doc/superpowers/plans/2026-07-26-output-aware-bfd-implementation.md` | BFD、离线逐请求 E2E、资源效率指标与 64/512/1024 实验实施计划 | 继续当前数据组织策略主线前读 |
| `code_doc/superpowers/plans/2026-07-26-row-cap-aware-packing-and-observation-implementation.md` | 非阻塞 adaptive 观测、row-cap-first packing 与真实 GPU 策略筛选计划 | 执行当前数据组织优化和机制选择前读 |
| `code_doc/superpowers/specs/2026-07-29-daft-ray-baseline-advantage-validation-design.md` | Daft+Ray baseline 优势验证的批准规格 | 查询预注册门槛、staged matrix、负结果规则与远端停止条件 |
| `code_doc/superpowers/plans/2026-07-29-daft-ray-baseline-advantage-validation-implementation.md` | actor readiness、HTTP timing、等价性门禁和交接计划 | 继续实现或交给新 agent 运行前读 |
| `deploy/pgai/` | pgai Docker Compose 部署 | 启动 pgai 测试环境 |
| `deploy/postgres18.4/` | PostgreSQL 18.4 Docker Compose 部署 | 启动 PG18.4 同构预演环境 |
| `deploy/autodl/` | AutoDL 云服务器 runbook、环境模板、模型下载/endpoint 启动脚本与双 GPU 场景模板 | 2× GPU 云上复现：配置化 vLLM 多 endpoint + PG18.4 + Ray/Daft |
| `deploy/runtime/` | 多机器自动 profile、软件能力组、模型/数据资产与参数校准合同 | 在 AutoDL、单 5070 或其他 Linux/NVIDIA 主机开始/恢复实验前做 preflight |
| `deploy/autodl/dual_gpu_capacity_scaling.example.json` | 单/双 endpoint 相同 per-GPU K 的容量扩展模板 | 先确定双 GPU 公平 scaling 与每卡静态甜点 |
| `deploy/autodl/dual_gpu_token_budget_curve.example.json` | disjoint manifest、async multi-prompt、固定 per-endpoint active work 的 2K–65K token-budget 曲线 | feeding formal 通过后，在等量 offered work 下证明预算不是越大越好并冻结 held-out 静态点 |
| `deploy/autodl/dual_gpu_data_organization.example.json` | disjoint manifest、async multi-prompt、固定 active work/最佳预算的数据组织隔离模板 | 比较 fixed16、sequential、row-cap-aware 与 length-align，避免预算、transport、offered load 和 flush 混淆 |
| `deploy/autodl/dual_gpu_request_replay.example.json` | batch barrier 与 request-level replenishment 模板 | 容量与组织阶段完成后运行，并按实际 batch rows 对齐 request K |
| `deploy/autodl/dual_gpu_active_work_curve.example.json` | 第一优先级的 request-level per-endpoint active-token credit 容量曲线 | 先标定模型/负载相关的 offered-work 饱和区，避免按 batch K 暗中改变 request 并发 |
| `deploy/autodl/dual_gpu_cost_profile_pilot.example.json` | 双 4090 算子代价新数据的 4-cell cache-on active-work pilot v3（1 warmup+1 formal） | 验证单 runner、共享 Ray、cache 声明/命中观测和总耗时；不作性能排名 |
| `deploy/autodl/dual_gpu_cost_profile_formal.example.json` | 5 workloads × 2 rows × 2 output caps × 4 active-work 的 cache-on 双 4090 formal v2 | 320-run formal-only 数据集；不得与旧单 5070 或 cache-off 样本静默合并 |
| `experiments/plans/operator_cost_profile_pilot_20260804.md` | B 线新数据采集的固定项、唯一变量、门禁、三层指标和跨硬件隔离合同 | 启动任何新增 cost-profile GPU run 前读取 |
| `experiments/plans/operator_cost_profile_dual4090_formal_20260804.md` | 双 4090 20-context formal 合同；§8 为局部估计过门槛后才启动的 TPC-H-derived AI 查询计划条件验证 | 启动/恢复 320-run，或评审代价模型是否进入计划级 held-out 前读取 |
| `deploy/autodl/dual_gpu_actor_pool_shape.example.json` | 固定每 endpoint 256 个可见 slot/0.5 CPU 的 1/2/4/8/16 Ray actor 拓扑对照 | 在同协议饱和点选择达到峰值 97% 的最小 actor 数，不改变 offered-load 上限 |
| `deploy/autodl/dual_gpu_service_quantum.example.json` | 固定 planning budget、active work 和 actor slots 的 batch/512/1024/2048/4096/request 完成粒度对照 | 量化批内 HOL、credit-held 空转与 completion-driven replenishment |
| `deploy/autodl/dual_gpu_slo_ewma_flush.example.json` | 高压/临界到达率下 fixed、queue-adaptive 与 SLO-aware EWMA flush 对照 | 在固定 request-level、65K active work 和 1×256 actor pool 下验证动态关批是否改善吞吐、SLO-goodput或尾延迟 |
| `deploy/autodl/dual_gpu_shared_vllm_gate.example.json` | Shared-vLLM 双 GPU 最小门禁模板 | 2 job × independent/partition/shared-DRR，各 64 行 |
| `deploy/autodl/dual_gpu_shared_vllm_formal.example.json` | Shared-vLLM 1/2/4-job 正式模板 | j4 gate 后使用有界 async actor pool，三种 credit 策略，1 warmup + 3 repeats |
| `deploy/autodl/dual_gpu_shared_vllm_j4_gate.example.json` | Shared-vLLM 4-job 独立能力门禁 | 在 j4 formal 前验证有界 actor pool、VMA/线程和 exactly-once |
| `deploy/autodl/dual_gpu_shared_vllm_j4_formal.example.json` | Shared-vLLM 4-job 独立正式模板 | j4 gate 通过后用于故障隔离或单独复验 |
| `deploy/autodl/dual_gpu_submission_policy.example.json` | 保留 multi-prompt batch 的 active-work、least-work routing、动态预算与 adaptive flush 消融 | 完成静态预算和 active-work 标定后运行；单项有效才进入组合候选 |
| `deploy/autodl/dual_gpu_static_k_workload_surface.example.json` | low/near-capacity/burst × K64/128/256 静态性能面 | 先判断不同 workload 的最佳静态点和错配代价是否足以支持动态策略 |
| `deploy/autodl/dual_gpu_static_credit_prompt_length_gate.example.json` | short/long prompt 的 K256、K256+W65K、K256+W98K async 等价臂门禁 | 在重建静态 credit surface 前验证 transport、token IDs、跨 workload 交错与未绑定臂 5% 等价性 |
| `deploy/autodl/dual_gpu_endpoint_adaptive_gate.example.json` | 双 endpoint typed adaptive 256 行可运行性门禁 | 验证 endpoint-local state/metrics/action trace，禁止当作性能结果 |
| `deploy/autodl/dual_gpu_official_baseline_gate.example.json` | 64 行双 GPU 文本 comparison validity gate（历史兼容文件名） | calibration 前验证 provenance、Chat 请求等价、exactly-once、endpoint 分片与空队列 |
| `deploy/autodl/dual_gpu_duckdb_ai_capability_gate.example.json` | DuckDB `ai` community extension 的两 shard harness capability gate（历史文件名） | 每 shard 仍是一个 BASE_URL；只作 `harness_pre_split_diagnostic`，不得称扩展原生双 endpoint 或进入产品原生主排名 |
| `deploy/autodl/single_endpoint_squad_database_e2e.example.json` | SQuAD bounded-output **单 endpoint 产品语义轨**冻结服务配置 + 三臂命令模板；显式锁 active endpoint=1/GPU0、method-degenerate 标记、model/cap=64/DuckDB 并发/sink/归因，并归档 `--service-config-hash` 依据 | 只用于单 endpoint 产品语义与管线开销；不能称双 GPU 方法排名。项目方法走独立双 endpoint direct/bounded control vs endpoint-aware 策略消融；第三方 gateway 仅为可选系统轨 |
| `deploy/autodl/dual_gpu_official_baseline_calibration.example.json` | 文本 ceiling/control/native arm 独立标定网格 | 删除未接线 Daft partition_count；只扫描各 arm 真实暴露参数，不得直接当 formal |
| `deploy/autodl/dual_gpu_text_native_baseline_formal.example.json` | 2,048 行文本原生 baseline held-out 正式合同（contract only） | 统一 formal runner 落地且 calibration 冻结后执行至少 60 秒、1 warmup + 3 interleaved repeats；若不足 60 秒，baseline/project 共同扩容 |
| `deploy/autodl/dual_gpu_completions_baseline_gate.example.json` | 无 Ray fixed-row multi-prompt Completions 双 GPU transport ceiling | 在相同 Completions 协议下扫描 1/4/16/32 行 HTTP packing，保持每 endpoint 最多 256 active prompts |
| `deploy/autodl/dual_gpu_project_chat_feeding.example.json` | project Chat `urllib`/持久 async transport 与 1×256/2×128/4×64 actor 校准 | 先通过同协议 bounded 95% feeding 门禁，再运行 Chat 策略或官方 runtime 排名 |
| `deploy/autodl/dual_gpu_project_completions_feeding.example.json` | project 原始 multi-prompt Completions fixed-row feeding 校准 | 隔离 Ray/HTTP transport 后才进入 token-budget、length-align 与 adaptive flush 消融 |
| `deploy/autodl/dual_gpu_same_condition_project_equivalence_gate.example.json` | project K256 与 nonbinding W98K 的首次高并发等价性门禁 | 1 same-pressure warmup + 3 repeats；未收敛到 5% 内禁止 broad calibration |
| `deploy/autodl/dual_gpu_same_condition_project_calibration.example.json` | 同 512 行 immutable Chat manifest 的 project static-K 与 active-work 校准模板 | direct C256 后测 project 达到 ceiling 所需的最小上游压力 |
| `deploy/autodl/dual_gpu_same_condition_project_formal.example.json` | disjoint 2,048 行 manifest 的 project static request-credit 与 token-work 正式模板 | 数据补齐、64 行 gate 与参数冻结后运行 1 warmup + 3 repeats |
| `notes/AGENTS.md` | 沟通材料规则 | 整理导师/企业侧反馈时读 |
| `notes/communication_notes.md` | 和同事/导师需要确认的问题和沟通话术 | 准备沟通 |

## 3. 实验规划在哪里

全局项目路线和近期实验任务：

`PROJECT_OUTLINE.md`

主要内容：

- 当前开题题目、研究内容（两项策略 + 多模态泛化验证 + 算子代价估计共同使能组件）；
- 实验主线和当前最重要证据；
- 近期优先级；
- 双向同步规则。

动机测试的计划（场景设计、集成路线）：

`motivation/plans/workloads.md`：三类 AI 算子场景、动机测试和后续实验优先级。

`motivation/plans/integration.md`：PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线和分阶段实验。

可行性验证的参考（组件、环境、脚本可用性）：

`feasibility/benchmarks/README.md`。

正式研究实验（方法有效性验证）：

`experiments/plans/`

| 文件 | 作用 |
|---|---|
| `archive/research_design_catalog.md` | 课题研究方案候选目录（已归档）：28 个候选方案的六维评估，作为设计历史参考 |
| `baseline_reference.md` | 实验 Baseline 参考矩阵：从 CCF-A 文献中提取的各方向最优 baseline 策略（GPU 调度/数据组织/提交控制）|
| `strategy_design_literature_basis.md` | 策略设计思路的文献依据与边界：区分可借鉴思想、baseline/边界和本文自己的策略定义 |
| `strategy_design_implementation_reference.md` | 策略设计与系统实现参考：把 Ray、vLLM、Daft、GPU 数据放置和 DB AI 算子机制沉淀为两项策略 + 端到端验证、实验变量和实现优先级（2026-07-17 已统一口径）|
| `data_organization_batching.md` | 研究内容一实验计划：token-budget、length-aligned、prefix-aware grouping + Daft 引擎级参数 |
| `service_scheduling_backpressure.md` | 研究内容二实验计划：queue-adaptive flush、actor pool 分池路由、K_max 动态控制 + Daft 引擎级参数 |
| `sink_writeback_coordination.md` | 写回工程参考（不作为独立实验阶段）：COPY + deferred index baseline |
| `cross_layer_killer_experiment.md` | 耦合验证实验计划：独立最优拼接 vs 联合 grid search（含策略级 + 引擎级参数的完整交互面）|
| `experiment_status_and_gaps.md` | **实验状态与缺口分析（2026-07-20）**：已完成/未完成实验表、证据链完整性、指标盲区、P0/P1/P2 路线图、审稿人视角风险。当前实验设计的第一参考。|
| `image_clip_workload_lock_20260731.md` | 🔴 **首个 workload（当务之急）**：AI_EMBED 执行门禁 + AI_CLASSIFY 正式候选；ImageNet/ResNet18 单标签与 COCO/CLIP multi-label 两条质量轨道 | 设计图像实验、选择 top-1/top-5 或 mAP/F1、审计五臂 fused/staged baseline 和 host-data-path 门禁时读 |
| `msmarco_embedding_workload_20260731.md` | ⏸ 文本轻对照（降级）：MS MARCO 文本，token ID 紧凑搬运轻，瓶颈不显现——仅作"文本下不显现"的边界对照 |

所有实验计划遵循从 vLLM/Orca/TurboVecDB/GaussML/FlexPushdownDB 五篇 CCF-A 论文提取的共同方法论：曲线 > 单点、先暴露瓶颈再优化、同硬件公平 baseline、消融拆开、诚实报告边界、统计严谨。

## 4. 实验代码在哪里

活跃可行性 benchmark：

`feasibility/benchmarks/`

| 文件 | 作用 |
|---|---|
| `README.md` | benchmark 说明和运行命令 |
| `requirements.txt` | Python 依赖：Ray、NumPy、PyArrow |
| `code/requirements-dev.txt` | 固定 Ruff 版本的开发检查依赖 |
| `pyproject.toml` | Python 3.12 代码风格与 correctness lint 基线 |
| `common.py` | CSV 输出、表格打印、依赖检查等公共函数 |
| `ray_many_objects_benchmark.py` | 固定总数据量下 Ray many-object fan-in |
| `ray_arrow_fanout_fanin_benchmark.py` | Arrow RecordBatch 版 Ray `N upstream -> P downstream` fan-out/fan-in |
| `analyze_results.py` | 汇总 CSV 并生成可行性报告 |

早期排除性实验（Ray small task、object transfer、Arrow serialization、shuffle simulation）保留在 `feasibility/benchmarks/` 中作为历史组件参考。这些实验证明了对应方向不是当前瓶颈，不代表真实 GPU-backed 数据库 AI 算子链路瓶颈。

动机测试脚本：

`motivation/benchmarks/`

| 文件 | 作用 |
|---|---|
| `fake_embed_pipeline.py` | fake `AI_EMBED(text)` 端到端链路 |
| `workload_matrix.py` | 三类候选 AI 算子场景对比 |
| `granularity.py` | task/object/fan-in 收益来源拆分 |
| `backpressure.py` | 模型服务反压离散事件模拟 |

动机测试正式结果位于：

`motivation/results/`

- `fake_cpu/`：CPU/fake 历史预研（仅作背景参考）
- `cpu/`：CPU baseline 对照
- `gpu/`：GPU-backed E2E 主动机结果（当前最优先引用）
- `pg18_4_fake/`：PG18.4 本地同构预演（不代表真实平台结论）

推荐命令：

```bash
python motivation/benchmarks/fake_embed_pipeline.py \
  --upstream 8 32 \
  --downstream 8 32 \
  --total-rows 65536 \
  --embedding-dim 128 \
  --repeats 3 \
  --output motivation/results/fake_cpu/fake_embed_pipeline.csv
```

```bash
python feasibility/benchmarks/analyze_results.py \
  --results-dir feasibility/results
```

运行环境：

- 使用 `.venv`；
- 当前没有必要使用 conda；
- Ray benchmark 在 macOS 沙箱中可能需要提权运行。

## 5. 实验结果在哪里

可行性验证结果（组件、环境、连接）：

`feasibility/results/`

动机测试正式结果（唯一来源）：

`motivation/results/`

讲解动机实验时引用 `motivation/results/gpu/`（GPU-backed，最优先）或 `motivation/results/fake_cpu/`（历史预研）。`feasibility/results/` 仅保留组件 benchmark 结果和环境验证。

正式论证优先引用：

1. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`：GPU-backed embedding 链路拆分。
2. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`：双 endpoint Ray task/actor 动机。
3. `motivation/results/gpu/pgai_integrated_key_rerun_20260714.md`：pgai-integrated GPU-backed rerun。
4. `motivation/results/gpu/pgvector_writeback_20260714.md`：pgvector(384) 写回对比。

## 6. 文献和资料依据在哪里

业务场景和动机测试文件：

`motivation/plans/ai_sql_surface.md`

主要内容：

- 现有数据库 AI 算子例子；
- AI 算子、数据库 AI 算子、模型 kernel、传统查询算子的区别；
- 推荐初步场景：批量 Embedding / RAG 数据准备；
- 最小原型设计；
- 瓶颈矩阵和动机测试判定标准。

集成与测试方法文件：

`motivation/plans/integration.md`

主要内容：

- PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线；
- 无设备/低端设备/PostgreSQL 18.3 平台的分阶段实验；
- AI_EMBED 算子集成形态；
- 瓶颈与优化点映射。

文献审查文件：

`research/literature_and_evidence_review.md`

主要内容：

- Ray 论文和官方文档；
- Daft Ray runner、partitioning、shuffle、join strategy；
- Spark partition / shuffle / AQE 类比；
- Arrow / Lance 论文背景；
- Snowflake / pgai / PostgresML / pgvector 外部系统依据；
- 本地实验和外部证据如何对应；
- 当前不能声称什么。

外部系统执行链路对比：

`research/existing_ai_operator_execution_chains.md`

使用原则：

- 写调研、汇报、论文动机时优先引用该文件；
- 不要只引用本地 microbenchmark；
- 结论必须区分"文献/官方文档""本地实验""合理推断""待确认"。

## 7. 当前方向边界

开题报告正式题目：

> 数据库 AI 负载的执行优化与调度研究方向。

两项策略设计 + 多模态泛化验证 + 算子代价估计共同使能组件（2026-07-29 更新，写回为实验设置）：

1. **AI workload 感知的动态数据组织与批处理构造策略**（研究内容一）：对比 token-budget 与固定 batch_size 在吞吐和 P99 上的差异，利用异构 actor pool + Daft 引擎级参数实现。
2. **调度与提交控制策略**（研究内容二）：利用 Ray actor 研究去中心化的调度与提交控制，候选策略包括 queue-adaptive flush、K_max 动态控制、actor pool 分池路由等。
3. **多模态泛化验证**（正文实验）：在图像 workload 上使用同一套策略代码验证模态无关性。
4. **算子代价估计**（贯穿两项策略的重要组件，不作为独立研究内容）。

主场景：`AI_COMPLETE`（文本 LLM）+ `AI_EMBED/AI_CLASSIFY`（图像，多模态泛化验证）。vLLM 为部署平台 + baseline，Daft 为数据引擎，不修改其内部调度器。PG18.4 本地预演，后续进入 PG18.3 内部平台复测。

当前不要优先做：

- 改造整个 Ray；
- 泛泛 Daft + Ray + Lance 集成；
- 单纯 Arrow serialization 优化；
- 单纯数据库 GPU 查询算子优化；
- 没有真实 workload 的 toy benchmark。
- 把 PG18.4 本地预演写成 PostgreSQL 18.3 内部平台结论。

## 8. 下一步优先工作

**已完成**：
- ✅ vLLM + Qwen2.5-1.5B baseline 建立（07-18）
- ✅ Daft 文本阶段直接接入，链路跑通
- ✅ Token-tail revision + Token-budget vs Fixed Row 对照
- ✅ Shared-vLLM K_max 干扰实验
- ✅ Queue-adaptive flush 首次实现与测试（⚠️ adaptive 当前不如静态 K_max=8，foreground E2E 10.2s vs 7.3s，见 experiment_status_and_gaps.md P0-1）

**当前缺口**（详见 `experiments/plans/experiment_status_and_gaps.md`）：
1. **P1**：Shared-vLLM 核心 1/2/4-job 已完成；补 4-job held-out、
   staggered idle borrowing、weighted overlap fairness 与异构 mix/offset
2. **P1**：Prefix cache-on 与 length-align 独立消融
3. **P2**：图像 workload 多模态泛化验证
4. 算子代价估计增加独立时间段、新 workload 与预测区间

**Scope 缩减触发条件**：Month 1 无 vLLM baseline → 多模态降 Discussion（✅ 已建立，未触发）；研究内容一+二的消融实验未完成不启动多模态 pipeline；VLM 生成始终 optional；Adaptive 3 轮不能超 static K_max=8 → 研究内容二降级。

详细实验计划见 `experiments/plans/`，以 `PROJECT_OUTLINE.md` §近期优先级和 `experiments/plans/experiment_status_and_gaps.md` 为准。

优先沟通问题：

- 达梦实际是否会使用 Ray/Daft/Lance 做数据库内置 AI 算子；
- 数据从数据库到外部执行链路的格式是什么；
- 真实 AI 算子是否批处理，是否涉及 join/groupby/repartition/embedding preprocessing；
- 为什么需要 Ray，而不是数据库内部线程池或普通服务。
