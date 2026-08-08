# Research Experiment Results

## 统一入口

先读 [`EXPERIMENT_EVIDENCE_REGISTRY.md`](EXPERIMENT_EVIDENCE_REGISTRY.md)。该台账登记主要机制结果，并区分“设计预留、功能测试、真实链路门禁、GPU 筛选、重复或留出验证”，避免把代码完成度误写成性能证据。

## 开题统一文本 database-E2E（2026-08-07）

| Directory | Content | Boundary |
|---|---|---|
| `opening_database_e2e_text_20260807/` | SQuAD uniform + ShareGPT controlled-skew；direct / DuckDB AI / project frozen-static；24/24 单元、18 formal、统一 PG source/sink | project feeding 89.93%/91.38% 均未过门，只支持负结果与诊断；DuckDB ShareGPT 有 4,936/6,144 cap 语义失败。开题前 baseline 到此停止。 |

## 图像 AI_EMBED operator（2026-08-03/04）

| Directory | Content | Boundary |
|---|---|---|
| `image_ai_embed_operator_formal_20260803/` | 60K×2 held-out Ray Data/project 2×2 CPU formal、Daft 12K capacity consistency，以及 schema-v12 派生观测 | Ray/project 同规模 matched-resource 结论有效；Daft 因物化容量上限单列。跨规模只描述独立平台上的 images/s/单位资源，absolute JCT/first output 不混排。 |

## 双 GPU 调度与容量（2026-07-28/29）

| Directory | Content | Boundary |
|---|---|---|
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
| `dual_gpu_active_work_curve_20260728/` | Dual-4090 request-level per-endpoint active-work curve over 16,384–65,536 predicted tokens. | Throughput still rises at 65K but marginal gain has fallen to 5.5%; 49K is the current knee candidate and 65K is only the best tested boundary. |
| `dual_gpu_request_replay_20260728/` | Dual-4090 batch-barrier/request-level replenishment comparison with three formal repeats per arm and admission-work audit. | K48 is the work-matched request control and matches batch K16 throughput; K64 is the best tested request K but carries about 33% more offered work and has worse P99, so it is not an isolated replenishment win or a capacity optimum. |
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

当后续开始验证三项研究内容中的方法或调优策略时，再在本目录登记结果。

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
