# 修复前 Output-aware BFD 64 行门禁

## 实验设置与问题

该门禁在真实 PostgreSQL 18.4、pgvector 0.8.2、Daft native、Ray task、vLLM/Qwen2.5-1.5B 和 RTX 5070 上运行，没有使用 fake backend。它比较 sequential/BFD 与 prompt-only、fixed-output-cap、trace-metadata 三种成本模式，共六个场景，每个场景一次 warm-up 和一次 formal。

问题是验证 output-aware packing 的执行链路、request/submission/resource trace 和 MFU 字段是否可用。它不是性能排名实验。

## 运行与原始数据

复现参数见 `scenario_config.json`，使用统一 runner：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\experiments\run_ai_operator_scenarios.py `
  --config experiments\results\output_aware_bfd_gate_20260726\scenario_config.json `
  --profiler code\scripts\profiling\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\output_aware_bfd_gate_20260726 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`manifest.json` 保存 12 条展开后的精确命令；`runs.csv` 保存 12 条成功运行；逐轮 `.requests.csv`、`.submissions.csv`、`.flush.csv`、`.control.csv` 和 `.resources.csv` 保存原始轨迹。

## 参数与指标含义

本节定义 `scenario_config.json` 中关键参数和 `summary_long.csv` 中关键指标的含义，供读原始数据时参考。

| 参数 | 取值 | 含义 |
|---|---|---|
| `--total-rows` | 64 | 每个场景处理的总行数（ShareGPT/BurstGPT 负载，按 `doc_id` 顺序） |
| `--ray-batch-rows` | 16 | 单次 operator invocation 提交的行数上限（`ray_batch_rows` 硬 cap） |
| `--token-budget` | 6144 | 单个 batch 的成本预算（prompt + output 估计 token 数上限） |
| `--batching-policy` | `token_budget` / `best_fit_token_budget` | sequential 按 FIFO 装箱；BFD 按 best-fit-decreasing 装箱 |
| `--output-cost-mode` | `prompt_only` / `fixed_output_cap` / `trace_target_output` | 成本估计中 output 部分：忽略 / 取 `--completion-max-tokens=16` / 取 trace 推得的目标 output |
| `--max-inflight` | 8 | 同时在飞的最大请求数 |
| `--model-workers` | 2 | 并发向 vLLM 发送请求的 worker 数 |
| `--completion-max-tokens` | 16 | 每条请求的最大输出 token 数 |
| `--writeback-mode` | none | 关闭写回（本门禁只验证上游链路） |
| `--gpu-peak-tflops` | 61.7 | RTX 5070 BF16 峰值 TFLOPS，用于 MFU 分母 |

| 指标 | 含义 |
|---|---|
| `tokens_per_s` | 观测吞吐，按 vLLM Prometheus 的 prompt+generation token 速率折算 |
| `request_e2e_s_p99` | 单请求端到端时延 P99（提交到结果返回） |
| `batch_service_s_p99` | 单 batch 在模型服务侧的服务时间 P99 |
| `operator_invocations` | operator 被调用的次数（即拆出的 batch 数） |
| `mfu_estimate` | MFU 估计 = 已算 token 速率 / (峰值 TFLOPS × 精度系数)，无量纲比率 |
| `gpu_utilization_pct_*` | nvidia-smi 采样的 GPU SM 利用率百分比（均值/P95/max） |
| `vllm_running` / `vllm_waiting` | vLLM 引擎中 running / waiting 序列数采样 |
| `packing_budget_utilization_*` | 成本预算装箱利用率（每个 batch 实际成本 / 预算） |

## 严谨性自检与结果

- 12/12 scheduled runs 完成，0 incident，`runs.csv` 状态均为 `ok`。
- 六个场景和 warm-up/formal 身份完整，真实组件版本、GPU、能耗和 MFU 字段可审计。
- 观测链路可用性快照（6 个 formal 场景，源：`summary_long.csv`）：`tokens_per_s` 范围 3166–3474；`mfu_estimate` 范围 0.176–0.201；`operator_invocations`（每场景 batch 数）依次为 seq_prompt=2、seq_fixed=2、seq_trace=6、bfd_prompt=5、bfd_fixed=5、bfd_trace=6；`request_e2e_s_p99` 范围 3.15–3.47 s。这些数字只证明 trace/MFU/吞吐/时延字段在 6 个场景下均被成功采集，不代表任何算法或成本模式性能排名。
- 但 sequential token-budget 当时没有采用与 BFD 相同的 `ray_batch_rows` hard cap，因此两种算法的最大 batch 行数没有被控制（上表中 `operator_invocations` 在 seq 与 bfd 两组间不可比即为此症状）。

## 结论边界

本目录只证明修复前的端到端观测链路能够工作。由于核心约束不一致，任何 sequential 与 BFD 的吞吐、延迟、能耗或 MFU 差异都不能作为算法性能结论。

修复后的门禁位于 `../output_aware_bfd_gate_v2_20260726/`，正式规模结果位于 `../output_aware_bfd_512_v2_20260726/` 和 `../output_aware_bfd_1024_20260726/`。保留本目录是为了审计旧数据被排除的原因。

### 证据分级

- **本地实验事实**：12/12 runs 完成、状态 `ok`；6 个 formal 场景的 `tokens_per_s`、`mfu_estimate`、`operator_invocations`、`request_e2e_s_p99`、GPU/能耗/vLLM 字段均被成功写入 `summary_long.csv`（数值见"严谨性自检与结果"快照）——即 trace 链路与 MFU/资源观测字段在修复前的链路上确实可用。
- **本地实验事实**：sequential 与 BFD 在本目录运行时未共享相同的 `ray_batch_rows` 硬 cap，`operator_invocations` 在两组间不可比（已在"严谨性自检与结果"中记录此症状）。
- **不能声称**：任何 sequential vs BFD、或三种 cost mode 之间的吞吐 / 延迟 / 能耗 / MFU 差异作为算法性能结论——核心约束不一致使这些对比无效。
- **待确认**：修复后链路（`output_aware_bfd_gate_v2_20260726/`）在相同 `ray_batch_rows` 硬 cap 下是否能复现本目录的 trace 字段完整性。
