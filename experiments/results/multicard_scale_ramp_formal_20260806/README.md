# 三条系统路径 scale/calibration sweep（formal, reps=3, 2026-08-07）

> **命名边界（experiments/AGENTS.md §结果边界）**：本轮是 **bounded_http（ceiling）/ duckdb_ai（harness 预切 2-proc）/ lb_rr（nginx gateway 1-proc）三条系统路径的 scale/calibration sweep**，**非**完整三臂正式排名——**不含 project_static**（2-endpoint hang 未修，见 `experiments/plans/full_grid_sweep_plan.md` §1）。只答：同冻结服务配置下三条路径的容量曲线/稳定性/规模拐点差异。**不能**答"项目方法是否优于 baseline"（须先修 hang + 同合同重跑 project_static）。

本目录（`multicard_scale_ramp_formal_20260806`）= bounded_http + duckdb_ai；lb_rr 单独在 `multicard_lbrr_scale_ramp_formal_20260806/`（单进程经 nginx gateway，manifest/scheduler 不同，分轨）。

## 1. 实验目的
同一冻结 vLLM 服务下，bounded ceiling / duckdb harness-pre-split / duckdb+nginx gateway 三条执行路径的吞吐随**规模**（scale）如何变化；饱和点与塌陷点落在哪里；三路径拐点是否一致（→ 瓶颈在服务端还是上游）。

## 2. 实验设置
- 平台：2×4090，vLLM qwen2.5-7b 两 endpoint（8000@GPU0 / 8001@GPU1），nginx 8500 round-robin（lb_rr 用）。
- 服务配置（**strict preflight 验证 declared==effective**，见 `run_provenance.json`）：`--max-num-seqs 256`、`--max-num-batched-tokens 8192`、`--enable-prefix-caching`（ON）、`--max-model-len 8192`、`--gpu-memory-utilization 0.90`、dtype auto、tensor-parallel=1。
- 调度合同：**concurrency 冻结在饱和点 C_total=64**（bounded/duckdb c=32×2 shards；lb_rr c=64 ≈32/backend）；scale 唯一移动变量（deploy §9.1 calibration）。
- reps=3（1 warmup + 3 formal，`warmup_per_cell=true`：每 measured cell 前暖该 manifest 两 endpoint，cache-hot）。
- workload：squad_v11_dev_short_answer，max_tokens=cap=64。规模 {64,128,256,512,1024,2048,4096,8192,10570}。
- driver commit：5878d51（冻结）。配置：`actual_run_config.json`（待生成 sidecar；当前为 `deploy/autodl/multicard_*_scale_ramp.example.json` 派生）。

## 3. 合规性自检（§7.5C）
- **喂饱 vLLM**：bounded（ceiling）在饱和点 C_total=64 峰值 ~89.5k tok/s @ scale 512。
- **strict preflight 通过**：两 endpoint cmdline 实际带三 flag（`run_provenance.json::strict_preflight_output`）。
- **策略到极限**：C_total=64 为 phase2_2048_tb 校准的饱和点。
- **稳定**：大规模（≥4096）sample CV 0.4–1.3%；小规模（64/128/512）CV 7–13%（短 run 正常）。
- **lb_rr backend balance**：每 cell backend request-skew 与 token-work-skew 均 ≤10%（`(max-min)/max` @ `multicard_scale_ramp.py:366`）。
- **0 error / 未观察到 max_tokens truncation error**（passed cells；finish_reason 字段空 ≠ 审计非 length，DuckDB-ai v0.4.14）。

## 4. 实验设计
scale ramp @ frozen concurrency C_total=64；3 路径 × 9 scale × 3 reps。duckdb_ai 与 bounded_http 共用 2-shard `squad_dev` manifest（manifest 预分）；lb_rr 用 endpoint_count=1 `lbrr_dev` manifest（全行→nginx 分），分轨。详见 `experiments/plans/full_grid_sweep_plan.md` §3 校准合同。

## 5. 实验数据（tokens/s，vLLM counter 口径 = 唯一三路径同口径可比量；sample CV n-1）

| scale | bounded (ceiling) | duckdb (harness 预切) | lb_rr (nginx gateway) |
|---|---|---|---|
| 64 | 45513 (cv13%) | 15563 (cv13%) | 20081 (cv2.6%) |
| 128 | 43503 (cv7.8%) | 22888 (cv12%) | 24879 (cv5.1%) |
| 256 | 84530 (cv4.2%) | 45072 (cv2.2%) | 49562 (cv3.2%) |
| 512 | **89506** ← bounded 峰 | 60613 (cv12%) | 62910 (cv4.6%) |
| 1024 | 85950 (cv2.7%) | 68756 (cv4.2%) | 70090 (cv2.6%) |
| 2048 | 88118 (cv1.3%) | **76832** ← duckdb 峰 | **74088** ← lb_rr 峰 |
| 4096 | 43266 (cv0.8%) | 41695 (cv0.4%) | 39401 (cv0.7%) |
| 8192 | 43281 (cv0.6%) | 42030 (cv0%, **2/3 partial**) | 39331 (cv0.4%) |
| 10570 | 42283 (cv0.5%) | **FAILED 0/3** (shard exit 2) | 38540 (cv0.9%) |

**计时边界分列（不可混比）**：bounded = `model_serving_wall_s`（request 粒度，有 per-row E2E/TTFT）；duckdb / lb_rr = `query_jct_s`（query_barrier 粒度，整条 SQL JCT，**无** per-row E2E）。跨这两类的 request-E2E/TTFT 横比被 aggregator 禁止。每 cell 全部单次值见 `ramp_aggregate.json::reps` + 各 `summary.json`。

**失败 cell（完整落盘 `run_error.json`）**：duckdb_ai 8192 rep3 (shard 0 exit 2)；duckdb_ai 10570 rep1/2/3 (shard 1 exit 2) = DuckDB-ai 扩展大规模崩溃（`full_grid_sweep_plan.md` §5.2 已知限，非本轮新 bug）。

## 6. 结果解释（事实/推断/不能声称）
- **事实**：bounded 全程 ceiling；三路径均在 2048→4096 tok/s 腰斩后平台到 10570；duckdb ≈ lb_rr（duckdb 略高 ~3%）；duckdb 8192 partial / 10570 全 fail。
- **推断**：三路径上游机制不同却同在 4096 拐点 → 瓶颈在 vLLM 服务端（KV/调度饱和）而非上游路径；交互项预期弱（full grid plan §2.4 据此推荐十字切片而非完整矩形）。
- **不能声称**：项目方法优于/劣于 baseline（无 project_static）；lb_rr 与 bounded/duckdb 同柱排名（lb_rr 是 gateway 系统轨，分轨）；per-row E2E/TTFT 四路径横比（timing_granularity 不兼容）。

## 7. 对课题含义
上游数据组织/调度策略的价值窗口在服务端未饱和区（≤2048）；服务端饱和后上游差异被掩盖。gateway（lb_rr）相对 harness 预切（duckdb）的 nginx 开销小但稳定存在。duckdb harness 大规模崩溃是路径容量边界（非调度可救）。

## 8. 下一步
- 修 project_static 2-endpoint hang（`full_grid_sweep_plan.md` §1）→ 256 门 → 补 project_static K32×9×3 (~0.3h) → 4 臂峰值并发规模 ramp。
- 十字切片（并发 @2048 × 4 臂 + 规模 @C64 × 4 臂，~59 cells）回答饱和点/过载塌陷。
- duckdb_ai 8192/10570 崩溃根因（DuckDB-ai 扩展，单独诊断）。

## provenance
- `run_provenance.json`：两 vLLM 完整 cmdline + strict-preflight 输出 + nginx conf SHA（ed09a376…）+ model/dtype/TP/gpu-mem。
- `ramp_aggregate.{json,md}`：全指标聚合（per-cell identity / query_jct vs model_serving_wall / sample CV / reps 单次值）。
- 每 cell `identity.json`（comparison_role 主字段=系统角色：bounded=direct_client_control / duckdb=harness_pre_split_diagnostic / lb_rr=gateway_system_diagnostic）。
- driver commit 5878d51；raw 排 requests.csv（含 output_text）已排除。
