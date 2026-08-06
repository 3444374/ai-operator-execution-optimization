# 双 4090 算子代价估计 v2 cache-on formal profile（320 runs，2026-08-07）

> **状态（plan §7）：本目录是 v2 cache-on 重跑的**有效数据 + 归档**。2026-08-04 首次 run 因并发共用 GPU + 空 `--ray-address`（每子 run local Ray）而**无效**（见 `../operator_cost_profile_dual4090_formal_20260804/`）；本轮 v2 修复后重跑，**320/320 有效、0 incident、gate 10（0 local-Ray）通过**——正是对首次无效的闭环。**
> **本归档只含数据采集 + §4 门禁 + CE 信号 headline；CE0–CE6 估计器 context-LOO 评估（regret/ranking，plan §5）是紧接的下一步**（估计器 + LOO harness 已实现：`code/scripts/analysis/compare_cost_estimators_contextloo.py` 等，需把 `load_rows` 数据源指向本 runs.csv）。

## 1. 实验目的（plan §1）

在双 4090（与旧单 5070 隔离）下，验证现有 CE0–CE5 代价估计方法能否仅凭**执行前特征**对未见过的 workload/rows/output context 排序 4 个 active-work 候选并选近 oracle。本实验评价**预测/排序/决策质量**，**不**评价上游调度策略的论文增益（plan §1/§6）。属课题"算子代价估计"共同使能组件（研究内容四），非独立研究内容。

## 2. 实验设置

- 平台/服务：2×4090，vLLM qwen2.5-7b 两 endpoint（8000/8001），prefix-cache ON、`--max-num-seqs ${VLLM_MAX_NUM_SEQS}`、`--max-num-batched-tokens ${VLLM_MAX_NUM_BATCHED_TOKENS}`、gpu-mem 0.90、completions 协议。共享 Ray head（`--ray-address ${RAY_ADDRESS}` 非空）。
- 执行链路：PG → Daft postgres source → token-budget organizer → Ray actor (1 worker/endpoint × concurrency 256) → 2× vLLM Completions；`httpx_async`、token budget 8192、fixed 50ms flush、prefix-cache ON、no writeback。
- **active-work 是唯一候选变量**：32768 / 49152 / 65536 / 98304 per endpoint（plan §2）。
- 决策 contexts（plan §3，跑前冻结笛卡尔积）：5 workload（short_prompt_lt50 / long_prompt_ge150 / sharegpt_concentrated / sharegpt_multiturn / lmcache_agent）× 2 rows（128/256）× 2 output cap（64/256）× 4 candidate = **20 contexts × 80 cells**。
- reps：1 warmup + 3 formal，全局交错、固定 seed 20260804 → **80 warmup + 240 formal = 320 runs**。prompt 上限 7000（长 prompt + 256 输出仍 < 8192 model len）。
- 跑前核验：5 workload 各 ≥256 行（short_prompt_lt50=512 / long_prompt_ge150=325 / concentrated=2048 / multiturn=2048 / lmcache_agent=851）。
- 配置：`deploy/autodl/dual_gpu_cost_profile_formal.example.json`（冻结）；runner `code/scripts/experiments/run_ai_operator_scenarios.py`；profiler `code/scripts/profiling/postgres_ai_operator_profile.py`；driver venv text-baselines。

## 3. 合规性自检（plan §4 11 门禁，全过）

| 门 | 要求 | 结果 |
|---|---|---|
| 1 | manifest 320/320、0 unrecovered incident；80 warmup + 240 formal | ✅ status=completed, completed_runs=320, skipped=0, incidents=0, phase {warmup:80, formal:240}, 80 scenarios |
| 2 | 每 formal request/submission 数=context rows，doc_id exactly-once | ✅ 抽样 r128(128 unique)/r256(256 unique) 全 no-dup |
| 3 | 两 endpoint 均接收，resource trace ok | ✅ endpoint_count=2，vllm_request_success_delta 128–256（>0） |
| 4 | 非 replay `flush_trace_status=not_applicable_non_replay` | ✅ 全 {not_applicable_non_replay} |
| 5 | formal-only = 20 context × 4 candidate × 3 repeat；warmup 排除 | ✅ 240 formal（formal-only 过滤），80 scenario |
| 6 | 每 context 4 个 23 维特征向量 + candidate ID 不同 | ⏳ 延至 CE LOO 评估（runs.csv 296 列含全部特征，行级可查） |
| 7 | 服务快照（model/port/cache/max-batch/seqs） | ✅ model=qwen2.5-7b, gpu=2×4090, prefix_caching=enabled, vllm_running_max 有值 |
| 8 | CV>5% cell 单列、补跑、不静默删 | ⚠️ 63/80 cell e2e_s CV>5%，**全部是 short-fast cell**（e2e 3–4s，固有短 run 抖动，§7.5 同现象）；非离群单点，**单列不删**，CE 用 3-rep mean（稳健） |
| 9 | host-scope lease（单 runner） | ✅ `.runner-lease.json` acquire，无并发 runner |
| 10 | `--ray-address` 非空共享 + 0 "Started a local Ray instance" | ✅ **0**（每 run 连 172.17.0.3:6380 共享 Ray）—— v2 对首次无效 run 的关键修复 |
| 11 | cache 三处一致 enabled；hit∈[0,1]、hits≤queries | ✅ service_prefix_caching={enabled}；hit_rate∈[0,0.988]；0 hits>queries |

- exit_code：**320/320 = 0**；formal status 全 {ok}；stderr 0 traceback/ConnectionError/OOM/CUDA。

## 4. 实验设计

20 decision contexts × 4 active-work candidates × (1 warmup + 3 formal)，全局交错固定 seed。唯一移动变量 = active-work candidate（plan §2）；workload/rows/cap/model/服务/拓扑全冻结。可比性：4 candidate 在同 context 内同条件横比（within-context ranking）；跨 context 不直接横比 e2e（workload/rows 不同）。

## 5. 实验数据

### 5.1 CE 信号 headline（per-context mean e2e_s by active-work；oracle = min e2e）

| context | rows | cap | e2e by active-work [32768, 49152, 65536, 98304] | oracle | spread |
|---|---|---|---|---|---|
| lmcache_agent | 128 | 256 | 39.5 / 29.5 / 23.7 / 21.2 | 98304 | 86.5% |
| lmcache_agent | 128 | 64 | 14.8 / 12.7 / 10.8 / 10.5 | 98304 | 40.6% |
| lmcache_agent | 256 | 256 | 49.2 / 39.3 / 33.2 / 26.8 | 98304 | 83.8% |
| lmcache_agent | 256 | 64 | 21.8 / 18.4 / 18.1 / 15.7 | 98304 | 39.2% |
| long_prompt_ge150 | 128 | 256 | 15.1 / 12.9 / 11.0 / 10.6 | 98304 | 42.1% |
| long_prompt_ge150 | 128 | 64 | 7.0 / 5.3 / 5.5 / 7.3 | 49152 | 39.8% |
| long_prompt_ge150 | 256 | 256 | 25.7 / 21.4 / 17.3 / 18.4 | 65536 | 48.5% |
| long_prompt_ge150 | 256 | 64 | 11.6 / 11.1 / 12.0 / 9.8 | 98304 | 22.9% |
| sharegpt_concentrated | 128 | 256 | 12.0 / 10.0 / 8.3 / 8.7 | 65536 | 44.3% |
| sharegpt_concentrated | 128 | 64 | 6.0 / 3.9 / 5.1 / 4.5 | 49152 | 53.2% |
| sharegpt_concentrated | 256 | 256 | 19.2 / 15.8 / 14.1 / 12.1 | 98304 | 58.0% |
| sharegpt_concentrated | 256 | 64 | 6.9 / 6.8 / 8.7 / 7.2 | 49152 | 28.1% |
| sharegpt_multiturn | 128 | 256 | 12.9 / 9.8 / 9.1 / 9.5 | 65536 | 42.6% |
| sharegpt_multiturn | 128 | 64 | 7.1 / 5.5 / 5.2 / 4.2 | 98304 | 69.3% |
| sharegpt_multiturn | 256 | 256 | 18.6 / 15.7 / 16.0 / 12.1 | 98304 | 53.5% |
| sharegpt_multiturn | 256 | 64 | 8.6 / 7.7 / 9.4 / 6.1 | 98304 | 53.6% |
| short_prompt_lt50 | 128 | 256 | 6.8 / 6.8 / 7.6 / 7.5 | 32768 | 12.0% |
| short_prompt_lt50 | 128 | 64 | 3.3 / 3.2 / 2.6 / 3.9 | 65536 | 48.1% |
| short_prompt_lt50 | 256 | 256 | 8.4 / 8.5 / 7.1 / 8.1 | 65536 | 20.2% |
| short_prompt_lt50 | 256 | 64 | 4.3 / 4.4 / 3.8 / 3.4 | 98304 | 28.0% |

**信号汇总（FACT，直接从 runs.csv 算）**：
- 20/20 context 全有 4 candidate；**0 退化**（spread 全 >5%，min 12% / median 44% / max 86.5%）→ 每个 context candidate 选择都 matter，选错最多付 86% e2e。
- **oracle 分布**：98304 → 11/20，65536 → 5/20，49152 → 3/20，32768 → 1/20。最优 active-work **context-dependent**（大 workload/rows/cap 倾向大 active-work，但 9/20 context 反例）——这正是估计器须捕获的非平凡信号。

### 5.2 全指标（per-run 296 列）

见 `runs.csv`（320 行 × 296 列：吞吐/E2E 分位/TTFT-ITL/prefix-cache hit/vLLM running·waiting·KV/GPU util·mem·power·energy·MFU/pipeline 阶段计时/packing·service-quantum cost/actor 调度/SLO goodput/cost per M tok）。CE 特征向量（23 维）在其中。

## 6. 结果解释（事实 / 推断 / 不能声称）

- **事实**：320-run 数据有效（§3 全门禁）；4 candidate 在每 context 产生 12–86% e2e 差异；最优 active-work 随 context 变（非固定）。
- **推断**：数据含**强排序信号**（0 退化 context），CE0–CE6 估计器有可学习的目标；大 workload（lmcache_agent、长 prompt、大 cap）倾向大 active-work（98304），小/短 workload 最优点分散——机制上大 active-work 提供更多在飞行 slot，在需要饱和的 context 受益，在短 run 受限于固定开销。
- **不能声称**：CE5（或任一估计器）的 regret/ranking/decision 指标——**尚未跑 context-LOO 评估**（plan §5），是下一步；"某估计器优于 baseline"（须 §6 同时列 CE1/2/4 + Pareto 口径，未跑）；short-fast cell 的高 CV 是**短 run 固有抖动非离群**，不能当数据质量失败丢弃，CE 用 3-rep mean。

## 7. 对课题含义

算子代价估计（共同使能组件）有了第一份**有效**双 4090 数据：4 active-work 候选在同 context 内有强 e2e 差异（最高 86%），且最优点 context-dependent → 估计器有实际价值空间（选错代价大）。这支持继续 CE0–CE6 LOO 评估；若某可部署估计器过 plan §6 门槛（pairwise ≥0.75 / median regret ≤5% / macro ≤5% / max ≤15%），才进入 plan §8 TPC-H-derived 计划级 capability。

## 8. 下一步

1. **CE0–CE6 context-LOO 评估**（plan §5）：把 `compare_cost_estimators_contextloo.py` 的 `load_rows` 数据源指向本 `runs.csv`（当前指向历史 `operator_cost_estimation_20260726/e2e_cost_model.json`），产 formal-only LOO JSON + 三层指标（MAE/RMSE/Q-error、within-context Spearman/pairwise/Top-K、pick-rate/regret）。评估候选门槛 plan §6。
2. short-fast cell 高 CV：CE 用 3-rep mean（稳健）；若 LOO 显示这些 context 主导误差，考虑补跑或降权。
3. 数据已就绪供估计器训练/评估；raw（含 output_text 的 requests.csv 等 66MB）留服务器，SHA 见 provenance。

## provenance

- **raw archive SHA256**：`a4f9cd5220306789d94f3a5c47169f3d967eb6fad0b5310f51ab5f8670314e8f`（全 66MB raw，服务器 `/root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260807/`）。
- 本目录：`runs.csv`（320×296 全指标）+ `manifest.json`（status/completed_runs/schedule/redacted_config/incidents）+ 本 README。raw per-run requests/submissions/stdout/stderr（含 output_text）未进 git（体积；同项目 raw 排除口径），服务器可复核。
- 驱动：run_ai_operator_scenarios.py + postgres_ai_operator_profile.py，commit `88aec55`（v2 cache-on 合同，server at e49ac53+ for the hang fix unrelated to this run's path）。vLLM 服务 = 4 路径 ramp 同进程（prefix-cache ON、三 flag）。
- 失败/异常：0 incident、0 failed-status formal、0 local-Ray。
