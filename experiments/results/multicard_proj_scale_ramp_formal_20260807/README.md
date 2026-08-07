# project_static 峰值并发规模 ramp（formal, reps=3, 2026-08-07）+ 4 路径对比

> **性质（experiments/AGENTS.md §结果边界）**：本目录补全 **4 条系统路径 scale/calibration sweep 的第 4 臂（project_static）**。与前序 bounded/duckdb（`../multicard_scale_ramp_formal_20260806/`）+ lb_rr（`../multicard_lbrr_scale_ramp_formal_20260806/`）拼成同一冻结服务合同下的 **4 路径峰值并发（C_total=64）规模 ramp**。这是 `full_grid_sweep_plan.md` §4.4 Tier C（完整规模轴、峰值并发、无并发扫），回答"4 路径容量曲线/拐点/稳态 ordering"。**project_static 是 project_scheduled_method（非 baseline）**：对比是**同 offered-load 下的 ordering**，**非**"项目方法优于 baseline"的系统性主张（§3.5 #2，须分轨）。

## 1. 实验目的

补全 project_static 臂（2-endpoint 挂死修复 `e49ac53` + 256 门 `6d3f59b` 通过后解锁），与已完成的 bounded/duckdb/lb_rr 在**同冻结服务 + 同峰值并发 C_total=64**下做规模爬坡，回答：4 路径吞吐/拐点是否一致（→ 瓶颈在服务端还是上游）；project_static 在同 offered-load 下相对 baseline 轨的 ordering 是否稳定；project_static 大规模（8192/10570）是否稳健（对照 duckdb 在 10570 崩溃）。

## 2. 实验设置

- 平台/服务：2×4090，vLLM qwen2.5-7b 两 endpoint（8000/8001），**strict preflight declared==effective**（三 flag：`--max-num-seqs 256`/`--max-num-batched-tokens 8192`/`--enable-prefix-caching` ON、max-model-len 8192、gpu-mem 0.90、TP=1）。vLLM cmdline 快照见 `../multicard_scale_ramp_formal_20260806/run_provenance.json`（同进程，本 ramp 未重启服务）。
- 调度合同：project_static 冻结静态值——K=32（8 actor × 4 ray_concurrency=32 slot，effective_k=32，C_total=2K=64）、token_budget=6144、active_work_per_endpoint=65536、`httpx_async`、raw chat、fixed_output_cap、prefix-cache ON。与 bounded/duckdb（c=32/shard，C_total=64）、lb_rr（c=64 total ≈32/backend，C_total=64）**同 offered-load per-backend ≈32**。
- reps=3（1w+3f，`warmup_per_cell=true`：每 cell 前用 bounded_http@c32 暖两 endpoint，cache-hot）。
- workload：squad_v11_dev_short_answer，max_tokens=cap=64，9 scales {64..10570}。
- 驱动：commit `e49ac53`（含挂死修复）+ aggregator `multicard_ramp_aggregate.py`。配置 `ramp_proj_scale_formal.json`（服务器本地，绝对路径，同 `ramp_scale_formal.json` 风格）。

## 3. 合规性自检（§7.5C）

- **喂饱 vLLM**：project 峰值 91407 tok/s @ scale 256；饱和规模（≥2048）GPU0 util mean 60–89%、tokens/s 与 bounded 同量级（见 §5）。
- **strict preflight 通过**（同进程，沿用 formal sweep）。
- **策略到极限**：K=32 为 phase2 冻结峰值（slot 上限）。
- **稳定**：大规模（≥4096）sample CV 0.3–0.8%；中小规模 CV 0.9–2.2%；scale 64 CV 21%（短 run，screening）。
- **27/27 passed, 0 failed**（含 8192、10570 全 3/3，对照 duckdb 10570 全 fail）。
- **0 error / 未观察到 max_tokens truncation error**（evidence 全 completed；finish_reason 字段空 ≠ 审计非 length）。
- **60s 稳态门禁（§3.5 #7）**：仅 scale 10570（operator_wall 59.0s）达 ≥60s；8192（44.8s）近。4096（20.3s）未达——但 4096 拐点由 **4 臂一致 + 饱和规模（8192/10570）平台**交叉印证为真，非单点。

## 4. 实验设计

scale ramp @ 冻结峰值并发 C_total=64；project_static × 9 scales × reps=3 = 27 cells。scale 唯一移动变量（deploy §9.1 calibration）。manifest 用 2-shard `squad_dev_<N>.jsonl`（manifest 预分；project_static 从 PG 读全集自路由）。身份 `comparison_role=project_scheduled_method`（非 baseline，分轨）。

## 5. 实验数据

### 5.1 四路径 tokens/s（vLLM counter 口径，唯一四臂同口径可比量；sample CV n-1）

| scale | bounded (ceiling) | duckdb (harness 预切) | lb_rr (nginx gateway) | **project (method)** | project operator_wall_s |
|---|---|---|---|---|---|
| 64 | 45513 (cv13%) | 15563 (cv13%) | 20081 (cv2.6%) | **49935 (cv21%)** | 0.29 |
| 128 | 43503 (cv7.8%) | 22888 (cv12%) | 24879 (cv5.1%) | **47952 (cv1.2%)** | 0.48 |
| 256 | 84530 (cv4.2%) | 45072 (cv2.2%) | 49562 (cv3.2%) | **91407 (cv2.1%)** | 0.63 |
| 512 | 89506 ← bounded 峰 | 60613 (cv12%) | 62910 (cv4.6%) | **86897 (cv0.9%)** | 1.30 |
| 1024 | 85950 (cv2.7%) | 68756 (cv4.2%) | 70090 (cv2.6%) | **83958 (cv1.9%)** | 2.78 |
| 2048 | 88118 (cv1.3%) | 76832 ← duckdb 峰 | 74088 ← lb_rr 峰 | **76464 (cv2.2%)** | 5.42 |
| 4096 | 43266 (cv0.8%) | 41695 (cv0.4%) | 39401 (cv0.7%) | **42403 (cv0.8%)** | 20.31 |
| 8192 | 43281 (cv0.6%) | 42030 (cv0%, **2/3 partial**) | 39331 (cv0.4%) | **42288 (cv0.3%)** | 44.83 |
| 10570 | 42283 (cv0.5%) | **FAILED 0/3** | 38540 (cv0.9%) | **41146 (cv0.3%)** | 59.01 |

（bounded/duckdb/lb_rr 数据来自 `../multicard_scale_ramp_formal_20260806/README.md` + `../multicard_lbrr_scale_ramp_formal_20260806/README.md`，同合同 reps=3。）

### 5.2 project_static 详情（request 粒度，有 per-row TTFT/E2E）

| scale | tok/s | rows/s | TTFT P50 | E2E P50 | prefix-hit | GPU0 util mean |
|---|---|---|---|---|---|---|
| 64 | 49935 | 37.9 | 71.0ms | 1.78s | 0.95 | 32.8% |
| 128 | 47952 | 56.8 | 63.7ms | 2.00s | 0.94 | 19.2% |
| 256 | 91407 | 103.9 | 52.6ms | 2.35s | 0.96 | 24.3% |
| 512 | 86897 | 136.3 | 54.0ms | 3.03s | 0.95 | 29.4% |
| 1024 | 83958 | 242.7 | 53.3ms | 2.45s | 0.95 | 52.2% |
| 2048 | 76464 | 238.0 | 53.0ms | 5.25s | 0.94 | 60.9% |
| 4096 | 42403 | 170.2 | 141.1ms | 11.77s | 0.66 | 88.2% |
| 8192 | 42288 | 161.3 | 153.9ms | 23.59s | 0.65 | 86.8% |
| 10570 | 41146 | 160.4 | 155.1ms | 31.44s | 0.64 | 88.6% |

**计时边界分列（不可混比）**：project/bounded = `request` 粒度（per-row TTFT/E2E 可比于 bounded，**不**可比于 duckdb/lb_rr 的 `query_barrier`）。project 的 `operator_wall_s` 是 adapter-equivalent 紧跨度（含 organizer/submit/fan-in/vLLM，不含 actor-ready/Ray-init）；`e2e_s` 含 post-loop metrics scrape + trace IO。每 cell 全单次值见 `ramp_aggregate.json::reps`。

## 6. 结果解释（事实 / 推断 / 不能声称）

- **事实**：4 路径均在 2048→4096 tokens/s 腰斩后平台到 10570（bounded 88k→42k、duckdb 77k→42k、lb_rr 74k→39k、project 76k→42k）；project 在 8192/10570 全 3/3 passed（42288/41146），duckdb 在 10570 全 fail；project ≈ bounded（小-中规模差 −1%…+8%，2048 project 低 13%）；duckdb ≈ lb_rr（duckdb 略高 ~3%）。
- **推断**：① 4 路径上游机制不同却同在 4096 拐点 → **瓶颈在 vLLM 服务端**（KV/调度饱和），非上游路径；大规模 prefix-hit 从 0.95 塌到 0.64 + TTFT 53ms→155ms 佐证 KV 饱和。② project 大规模稳健（无 duckdb 扩展崩溃路径），适合作"上游策略效果"的可测宿主。③ project ≈ bounded 在预期内：K=32 + active-work credit 在同 offered-load 下与 bounded c=32 semaphore 提供相近 GPU 喂给；差异在 organizer/credit 语义而非裸吞吐。
- **不能声称**：项目方法优于/劣于 baseline（project 是 method 非 baseline，机制不同须分轨，只给 ordering）；lb_rr 与 bounded/duckdb/project 同柱排名（gateway 系统轨，分轨）；per-row TTFT/E2E 四臂横比（timing_granularity 不兼容，仅 project↔bounded 可比）；4096 拐点的"精确阈值"（4096 operator_wall 20s < 60s 稳态门，拐点靠 4 臂一致 + 饱和规模交叉印证，不报单点精确值）。

## 7. 对课题含义

上游数据组织/调度策略的价值窗口在服务端未饱和区（≤2048）；服务端饱和（≥4096）后上游差异被掩盖——这与 3-path 结论一致，project 第 4 臂强化而非改变该结论。project_static 经此 ramp 证实可作为后续"策略效果"实验的可测宿主（稳健到 10570）。duckdb 大规模崩溃是路径容量边界（非调度可救），project 无此限。

## 8. 下一步

- **核心 unfinished 已完成**：4 路径峰值并发规模 ramp 闭环。
- 并发扫掠（切片 A，`full_grid_sweep_plan.md` §2.4）为**可选扩展**（回答饱和/过载点），非"未完成"补全；当前优先推进用户指定的 320-run 算子代价实验（`operator_cost_profile_dual4090_formal_20260804.md`）。
- duckdb_ai 8192/10570 崩溃根因（§3.5 + `full_grid_sweep_plan.md` §5.2）仍待单独诊断（DuckDB-ai 扩展限）。

## provenance

- 驱动 commit `e49ac53`（挂死修复）+ aggregator；vLLM 服务 cmdline + strict-preflight 见 `../multicard_scale_ramp_formal_20260806/run_provenance.json`（同进程）。
- 本目录：`ramp_run.json`（27 cell status/exit/effective_k）+ `ramp_aggregate.{json,md}`（全指标 mean/CV/reps 单次值）+ 每 cell `project_static_{summary,resource}.csv`。
- **raw per-request evidence**（completion_evidence/request_trace/source_scan，含 output_text + 指纹，8192/10570 行数大）保留服务器端 `experiments/results/multicard_proj_scale_ramp_formal_20260807/`，未进 git（体积，同 formal sweep raw 排除口径）；可在服务器复核。
- 身份：`comparison_role=project_scheduled_method`（主字段=系统角色）；`scheduler_owner=project_token_budget_organizer + ray_actor_pool + per_endpoint_active_work_credit + vllm`。

## 9. 增强 §7.5D 观测（2026-08-07，ramp-enhanced 重跑，task #33）

bounded/duckdb/lb_rr 用增强 instrumentation（`VllmGaugeSampler` during-cell 轮询）重跑，补原 ramp 缺的 §7.5D 观测；project 不需重跑（其 profiler 已采样 gauges，aggregator 现已 surface）。aggregate 见 `../multicard_scale_ramp_enhanced_20260807/ramp_aggregate.{json,md}`（bounded+duckdb）、`../multicard_lbrr_scale_ramp_enhanced_20260807/`（lb_rr）、本目录 re-aggregate（project）。

**4 臂 §7.5D 关键指标**（reps=3 mean；MFU = `[0,1]` **分数非 %**，per `_compute_efficiency` = mean(per-GPU estimated_flops)/(`GPU_PEAK_TFLOPS_BF16=165`×1e12×service_wall)，`multicard_ramp_aggregate.py:GPU_PEAK_TFLOPS_BF16`；KV = 分数 §7.5F）：

| scale | arm | MFU(frac) | run_max | KV_max | ITL p99 | J/1k-tok |
|---|---|---|---|---|---|---|
| 2048 | bounded | 0.230 | 52 | 0.036 | 24.9ms | 8.4 |
| 2048 | duckdb | 0.195 | 48 | 0.033 | 24.9ms | 8.6 |
| 2048 | lb_rr | 0.188 | 50 | 0.035 | 24.9ms | 8.7 |
| 2048 | project | 0.244 | — (prof 口径) | — | — | 6.5 |
| 10570 | bounded | **0.678** | 58 | 0.061 | 142ms | 20.3 |
| 10570 | duckdb | FAILED（崩溃）| — | — | — | — |
| 10570 | lb_rr | 0.640 | 62 | 0.067 | 124ms | 21.1 |
| 10570 | project | 0.618 | — (prof 口径) | — | — | 19.5 |

**口径警告（不可混比）**：bounded/duckdb/lb_rr 的 `run_max` = `vllm_running_total_max`（**Σ 两 endpoint**，VllmGaugeSampler during-cell 轮询）；project 的对应量是 `vllm_running_prof_max`（profiler per-run，** caliber 不同**，分列不并排比）。waiting_max ~0（四臂均无显著排队）。ITL 仅 gate 臂有（project summary 无 ITL histogram）。

**新观测（原 ramp 缺、本次补齐）**：
- **MFU 4 臂横比（首次）**：2048（未饱和）MFU 0.19–0.24（memory-bound，util% 高但 FLOP 密度低）；10570（饱和）MFU 0.62–0.68（compute-bound）。**util% ≠ MFU** 教科书特征再现。project MFU 0.244 @ 2048（与 bounded 0.230 接近）。
- **during-cell running_max 52–62（Σ≈64 inflight 的 81–97%）**——**§7.5C(1) 喂饱门证据**（原 ramp 只有 before/after idle=0）。bounded 52/64、lb_rr 62/64：vLLM 持续被喂近满，**feeding-saturation 成立**。
- **KV_max 0.033–0.067**（低，working set 只占 KV 3–7%）；4096+ 的 prefix-hit 塌（0.96→0.62）是 cache **多样性驱逐**非 KV 容量饱和。
- **能耗 J/1k-tok 8→21**（2048→10570，随 prefix-hit 降而升，更多真实 prefill FLOP/token）。

**不能声称**：project vs bounded 的 running 横比（caliber 不同）；MFU 绝对值跨论文比（vLLM estimated_flops 启发式，保守）。
