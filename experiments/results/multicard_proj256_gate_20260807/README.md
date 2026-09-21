# project_static 2-endpoint 挂死修复 — 256 go/no-go 验证门（2026-08-07）

> **性质（experiments/AGENTS.md §结果边界）**：这是一个**修复验证门（fix-validation gate）**，不是正式性能排名。它只回答一个问题：commit `e49ac53`（bound actor-ready `ray.wait` 90s + bound profiler `subprocess.run` 900s）是否让原本 2-endpoint 无限挂死的 `project_static` 臂**能完成**。**不能**从此门禁的 256 行 wall 时间推断 project_static 相对 baseline 的吞吐优劣（256 行未饱和、operator 工作量极小、db_fetch 主导，见 §4）。

## 1. 实验目的

`full_grid_sweep_plan.md` §1 的前置：在 `project_static` 2-endpoint 挂死修复落地后，用 scale=256/K=32/reps=3 端到端证明该臂不再挂死、能产出有效 formal ok 行，从而**解锁 project_static 进入 4 臂网格**（§0 非目标在门通过前 BLOCKED 整臂）。

## 2. 实验设置

- 平台：2×4090，两 vLLM endpoint（8000@GPU0 / 8001@GPU1），共享 Ray head（127.0.0.1:6380，session_2026-08-06_18-22-49）。
- 服务配置（**strict preflight declared==effective**）：`--max-num-seqs 256`、`--max-num-batched-tokens 8192`、`--enable-prefix-caching`(ON)、`--max-model-len 8192`、`--gpu-memory-utilization 0.90`、qwen2.5-7b、TP=1。**vLLM cmdline 完整快照见** `../multicard_scale_ramp_formal_20260806/run_provenance.json`（同一对 vLLM 进程，本门禁未重启服务）。
- 调度合同：project_static 冻结静态值——K=max_inflight=32（8 actor × 4 ray_concurrency=32 slot，effective_k=32）、token_budget=6144、active_work_per_endpoint=65536、`httpx_async`、raw chat、fixed_output_cap、prefix-cache ON。
- reps=3（1 warmup + 3 formal，`warmup_per_cell=true`：每 measured cell 前用 bounded_http@c32 暖 manifest 两 endpoint）。
- workload：squad_v11_dev_short_answer，max_tokens=cap=64，rows=256（project_static 从 PG 读全 256 行，按 organizer 路由到 endpoint）。
- 驱动：`code/scripts/baselines/multicard_scale_ramp.py` + `run_project_static`（commit `e49ac53`）。配置 `ramp_proj256_gate.json`（2-endpoint）/ `ramp_proj256_parity_1ep.json`（1-endpoint 隔离，§5）。

## 3. 合规性自检（§7.5C + §1.4 a–f）

| 项 | 门禁要求 | 2-endpoint gate | 1-endpoint parity |
|---|---|---|---|
| (a) 3/3 passed exit0 | 必要 | ✅ 3/3 passed, exit0, K=32 | ✅ 3/3 passed, exit0, K=32 |
| (b) formal status==ok | 必要 | ✅ 3 rep 全 ok | ✅ 3 rep 全 ok |
| (c) GPU util>0 (per-sample) | 必要 | ✅ max=100%（mean 12–24%） | ✅ max=100%（mean 32–59%） |
| (d) exactly-once 256, 0 failed | 必要 | ✅ rows=256 unique=256 全 completed | ✅ rows=256 unique=256 全 completed |
| (e) 两 endpoint 均用 | 必要 | ✅ 125/131, 128/128, 125/131（skew≤4.6%） | n/a（单 endpoint 设计） |
| (f) cell wall < 60s | 必要 | ✅ e2e_s 3.0–3.4s | ✅ e2e_s 1.9–3.6s |

- **挂死根因闭合**：`full_grid_sweep_plan.md` §1.2 的两层根因（F1 无界 `ray.get` + F2 无界 `subprocess.run`）已由 `e49ac53` 有界化；本门禁 3 reps 各 < 4s 完成，未触发任一 timeout（90s barrier / 900s subprocess），证明触发层（commit 140eefd `_ensure_ray_head` 的 stale-pointer 修复）+ 症状层（本 commit 的有界化）共同闭合。
- **诊断附加（§1.4）**：未触发 readiness barrier → 无 stuck-actor diagnostics 输出（无 `ray.cluster_resources()` 快照，因 barrier 直接通过）；task #119（capture profiler stderr to locate deadlock）随本门禁通过关闭。

## 4. 实验数据（per-rep timing；**非吞吐排名**）

| rep | e2e_s | operator_wall_s | db_fetch_s | writeback_s | GPU max% |
|---|---|---|---|---|---|
| **2-endpoint gate** | | | | | |
| 1 | 3.411 | 0.668 | 2.423 | 0.009 | 100 |
| 2 | 2.998 | 0.642 | 2.095 | 0.008 | 100 |
| 3 | 3.213 | 0.624 | 2.349 | 0.009 | 100 |
| **1-endpoint parity** | | | | | |
| 1 | 1.902 | 1.149 | 0.488 | 0.013 | 100 |
| 2 | 2.993 | 1.122 | 1.599 | 0.010 | 100 |
| 3 | 3.619 | 1.119 | 2.226 | 0.009 | 100 |

**口径与不能声称**：
- `e2e_s` 是 profiler 整边界（含 post-loop vLLM metrics scrape + trace-CSV IO + finish_job，**不含** actor-ready/Ray-init）；`operator_wall_s` 是更紧的 adapter-equivalent 跨度；`db_fetch_s` 是 PG 读 256 squad 行。256 行下 db_fetch/operator 占比高，**e2e_s 不与 in-process 臂的 wall 直接可比**（见 project_static.py docstring 的边界不对称声明）。
- 256 行未饱和（model_serving 工作 <1s），**不能**从此处 wall 推断 project_static vs bounded/duckdb 的稳态吞吐排序——那需要 §6 的饱和规模 ramp。
- finish_reason：DuckDB-ai 扩展不适用本臂（project_static 走 profiler）；evidence 全 `completed`、0 failed，**未观察到 max_tokens truncation error**（不声称"已审计 0 length"，profile summary 无 finish_reason 列则只报证据所见）。

## 5. 1-endpoint parity（§1.4 隔离）

同一 ramp driver、单 `endpoint_url`（8000）、K32、256、reps=3：3/3 passed。隔离"共享 head wiring 是否真修好"，避免 2-endpoint 症状被掩盖。1-endpoint 的 operator_wall（~1.12s）高于 2-endpoint（~0.65s），符合"全 256 请求压 1 GPU（C_total=32/GPU）vs 分担 2 GPU（16/GPU）"的预期，且两路径均远 < 60s。

## 6. 对课题含义与下一步

- **project_static 整臂 UNBLOCKED**（§0 非目标解除）：可进入 4 臂网格。
- **下一步**：补 project_static K32×9×3（全 9 scales, reps=3, 2-endpoint）→ 与已完成的 bounded/duckdb/lb_rr 拼成干净 4 臂峰值并发规模 ramp（`full_grid_sweep_plan.md` §4.3 排序 #3，~0.3h）。2048 饱和规模 cell 同时充当本门禁的回归项（§1.4）。

## provenance

- 驱动 commit `e49ac53`（含挂死修复）；vLLM 服务 cmdline + strict-preflight 快照见 `../multicard_scale_ramp_formal_20260806/run_provenance.json`（同进程）。
- 原始：`ramp_run.json`（每 cell status/exit/effective_k）+ 每 rep `project_static_{summary,resource,completion_evidence,source_scan,request_trace}.csv`。
- 身份：`comparison_role=project_scheduled_method`（非 baseline，§3.5 #2；主字段=系统角色）；`scheduler_owner=project_token_budget_organizer + ray_actor_pool + per_endpoint_active_work_credit + vllm`。
