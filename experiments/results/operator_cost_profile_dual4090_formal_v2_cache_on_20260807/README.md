# 双 4090 算子代价估计 v2 cache-on formal profile（320 runs，2026-08-07）

> **状态（plan §7）：本目录是 v2 cache-on 重跑的**有效数据 + 归档 + CE0–CE6 context-LOO 评估**。2026-08-04 首次 run 因并发共用 GPU + 空 `--ray-address`（每子 run local Ray）而**无效**（见 `../operator_cost_profile_dual4090_formal_20260804/`）；本轮 v2 修复后重跑，**320/320 有效、0 incident、gate 10（0 local-Ray）通过**——正是对首次无效的闭环。**
> **CE LOO 主结果（§5.3，6-rep 重跑后）**：高 CV 补跑（63 scenario × 6 reps）+ 17 低 CV scenario（3 reps）合并重评后，**CE5_hybrid（解析+残差校正）过完整 plan §6 promotion contract**（pooled regret 1.67%、median 0%、macro 2.90%、**max 14.72%**（<15%，**marginal**）、candidate pairwise 0.808）——首个过 contract 的估计器。6-rep tighten mean 把 CE5 的 max regret 从 3-rep 的 **39.77% → 14.72%**（证实"高 CV 噪声驱动 max regret"假设）；CE3_ridge（max 22.71%）/ CE4_lightgbm（max 26.89%）仍 fail max 门槛；CE0/CE2 退化、CE1 17.8%。CE5 row MAE 3.98 略高于 CE3 3.23（accuracy≠selection，Heinrich）。
> ⚠️ **3-rep 版（v2 原始 320-run）先前结论已修订**：3-rep 下 CE3/CE5 macro 6.42% + max 39.77% 全 FAIL；6-rep 补跑后 CE5 转 PASS（见 §5.3 + §9 erratum 续）。**边际警告**：CE5 max 14.72% 贴 15% 线，是 marginal pass，非稳健通过——换 split / 更多 context 可能翻转。CE4 LightGBM（max 26.89%）未优于 CE3/CE5（小数据集 20 context，非线性学习器未增益）。

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

## 3. 合规性自检（plan §4 11 门禁：**11/11 全过**（gate 8 已 6-rep 补跑闭环，CE5 过 contract，见 §5.3/§6））

| 门 | 要求 | 结果 |
|---|---|---|
| 1 | manifest 320/320、0 unrecovered incident；80 warmup + 240 formal | ✅ status=completed, completed_runs=320, skipped=0, incidents=0, phase {warmup:80, formal:240}, 80 scenarios |
| 2 | 每 formal request/submission 数=context rows，doc_id exactly-once | ✅ **结构化核验（audit F23）**：240/240 formal run 的 requests.csv doc_id 全唯一（46080 docs，0 dup/miss；先前为抽样，现全量） |
| 3 | 两 endpoint 均接收，resource trace ok | ✅ **结构化核验（audit F23）**：240/240 formal run 均向两 endpoint 各提交 >0（`actor_worker_submission_counts` 形如 `64;64`，双 endpoint 均非 0）；endpoint_count=2 |
| 4 | 非 replay `flush_trace_status=not_applicable_non_replay` | ✅ 全 {not_applicable_non_replay} |
| 5 | formal-only = 20 context × 4 candidate × 3 repeat；warmup 排除 | ✅ 240 formal（formal-only 过滤），80 scenario |
| 6 | 每 context 4 个 23 维特征向量 + candidate ID 不同 | ✅ **结构化核验（audit F22）**：20/20 context 的 4 candidate 特征向量两两 distinct + candidate ID distinct（`estimate_operator_cost.feature_vector` 23 维，先前 "延至 LOO" 已闭合） |
| 7 | 服务快照（model/port/cache/max-batch/seqs） | ✅ model=qwen2.5-7b, gpu=2×4090, prefix_caching=enabled；**+ 08-07 fresh `run_provenance.json`（同 PID 478170/478172）实测 max-num-seqs=256 / max-num-batched-tokens=8192 / prefix-cache ON，declared==effective（audit F7+F8）** |
| 8 | CV>5% cell 单列、补跑、不静默删 | ✅ **补跑完成（audit F10/F18，2026-08-07）**：63 高 CV scenario 已 6-rep 重跑（`merged_runs_6rep_20260807.csv`），合并 17 低 CV（3 reps）= 429 formal 行重评 CE LOO。**6-rep tighten mean 把 CE5 max regret 从 39.77%→14.72%**（证实高 CV 噪声驱动假设），CE5 过 §6 contract。CV>5% 对 sub-10s cell 仍固有（rerun 降 SEM 非降 CV），但 mean 精度足够让 CE5 过门。详见 §5.3/§6 |
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

### 5.3 CE0–CE6 context-LOO 评估（plan §5；6-rep 重跑后）

**6-rep 合并数据**（63 高 CV scenario × 6 reps + 17 低 CV × 3 reps = 429 formal 行 / 20 context / 4 candidate），`ce_context_loo_rerun_20260807.json` + `merged_runs_6rep_20260807.csv`。完整 plan §6 contract 矩阵：

| 估计器 | pooled regret% | §6 regret：median / macro / max（门槛 5/5/15%） | candidate pairwise（≥0.75） | row MAE s | 3-rep→6-rep max regret | contract |
|---|---|---|---|---|---|---|
| CE0_mean | 47.38 | 39.7 / 37.2 / 80.1（全 FAIL） | 0.50 ✗ | 6.22 | — | **FAIL** |
| CE1_analytical | 21.47 | 13.5 / 17.8 / 54.0（全 FAIL） | 0.53 ✗ | 3.33 | — | **FAIL** |
| CE2_lookup | 47.38 | 39.7 / 37.2 / 80.1（全 FAIL） | 0.50 ✗ | 5.29 | — | **FAIL**（退化） |
| CE3_ridge | 1.96 | 0.0 ✓ / 3.88 ✓ / **22.71 ✗** | 0.796 ✓ | 3.23 | 39.77→22.71 | **FAIL**（max 不过） |
| CE4_lightgbm | 2.23 | 0.0 ✓ / 3.33 ✓ / **26.89 ✗** | 0.767 ✓ | 4.95 | （3-rep 时 skipped） | **FAIL**（max 不过） |
| **CE5_hybrid** | **1.67** | 0.0 ✓ / **2.90** ✓ / **14.72 ✓** | **0.808 ✓** | 3.98 | **39.77→14.72** | **PASS ✅** |

- **CE5_hybrid 过完整 §6 contract**（4/4 子门：median 0%、macro 2.90%、max 14.72%、candidate pairwise 0.808 全过）——**首个过 contract 的估计器**。按 plan §6 可接管 active-work 候选选择。
- **6-rep tighten mean 的效果（证实 §3 gate 8 假设）**：CE5 max regret 39.77%→14.72%（−63%），CE3 39.77%→22.71%；macro 也降（CE5 6.42%→2.90%）。高 CV 噪声确实是 max regret 的主因。
- **CE5 > CE3（在 max 上）**：CE5 max 14.72 vs CE3 22.71——**残差校正在紧数据上现增益**（3-rep 时 CE3≈CE5；6-rep 时 CE5 的残差校正把最差 fold 拉回 15% 内）。但 CE5 row MAE 3.98 > CE3 3.23——**accuracy≠selection**（Heinrich：预测 MAE 略高但选择 regret 更低）。
- **CE4 LightGBM 未增益**：max 26.89（最差于 CE3/CE5），candPair 0.767；小数据集（20 context）非线性学习器未超过 Ridge/hybrid。
- **⚠️ 边际警告**：CE5 max 14.72% 贴 15% 线，**marginal pass**——换 split / 更多 context / 不同 reps 可能翻转。这是"刚过门"，非"稳健通过"。

### 5.2 全指标（per-run 296 列）

见 `runs.csv`（320 行 × 296 列：吞吐/E2E 分位/TTFT-ITL/prefix-cache hit/vLLM running·waiting·KV/GPU util·mem·power·energy·MFU/pipeline 阶段计时/packing·service-quantum cost/actor 调度/SLO goodput/cost per M tok）。CE 特征向量（23 维）在其中。

### 5.4 MFU（Model FLOPs Utilization）— 复审 `mfu-audit` 验证（2026-08-07）

> ⚠️ **单位**：`mfu_estimate` 是 **[0,1] 分数，不是百分比**（代码 `code/src/observability/metrics/resources.py:213-218`，status check `ok if estimate <= 1.0`）。先前版本曾误读成 "0.4%"——实际是 **40%**。

- **公式（对 240 formal 行逐一复算，最大误差 1.04e-7）**：`mfu_estimate = vllm_estimated_flops_per_gpu_delta / (gpu_peak_tflops × 1e12 × operator_wall_s)`。
- **常数**：`gpu_peak_tflops=165`（RTX 4090 bf16 dense fp32-accumulate，正确）；`mfu_time_basis=operator_wall_s`（≈ model_request_wall_s，比值 1.003，**未被 db_fetch/writeback 稀释**）；`model_flops_per_token=0.0`（此方法下未用，vLLM 直接给 FLOPs）。⚠️ 切勿用 `model_service_s`（788s，跨 actor/请求累积和，非 wall）做分母——会得假 ~0.55%。
- **真值分布**：均值 **40.78%** / 中位 39.63%，范围 **11.78%–78.78%**。按切片：output cap（o64 **48.9%** > o256 32.7%，decode 越多越 memory-bound，r=−0.508）；rows（128: 35.9% < 256: 45.7%）；active-work（W32k 33.8% < W98k 47.3% 单调）；workload（long_prompt 50.8% > multiturn 44.4% > concentrated 39.7% > agent 35.0% ≈ short 34.0%）。极值 cell：MAX long×256×o64 = 67.75%（单次 78.78%）；MIN agent×128×o256 = 17.33%（单次 11.78%）。
- **util ≠ MFU（关键洞察）**：MFU 与 `gpu_util_pct` **几乎不相关（r=−0.061）**——最低 MFU cell（11.8–17.1%）反而 gpu_util 73–97%（GPU "忙"但卡在 HBM，长 decode memory-bound）；最高 MFU cell（71–79%）gpu_util 65–90%（compute-bound prefill）。这是教科书式 LLM 推理特征：**util%（SM 占用）≠ MFU（有用 FLOP 率），低 MFU + 高 util = memory-bound decode，不是 GPU 没干活**。
- **合理性**：7B / 128–256 行 / cap 64–256 / vLLM 连续批处理，均值 ~40% MFU 健康；naive 2N 上界约是实测 1.86×（decode 远低于 2N/token 峰值，方向对）。vLLM FLOP 估计可能对 decode token 打折（推断 N≈4.1B vs 实际 7.6B）→ 保守方向。
- **缺口**：4 路径 ramp（bounded/duckdb/lb_rr/project 规模 ramp）**完全没记 MFU**——bounded/duckdb/lb_rr raw 从没读 vLLM FLOP counter，project raw 有 counter 但被 `gpu_peak_tflops=0.0` 挡住。**4 臂 MFU 横比需 ramp 重跑加 FLOP counter + 配 peak=165**（见 §8）。


## 6. 结果解释（事实 / 推断 / 不能声称）

- **事实**：320-run 数据有效（§3：**11/11 门禁全过**，gate 8 已 6-rep 补跑闭环）；4 candidate 在每 context 产生 12–86% e2e 差异；最优 active-work 随 context 变；**6-rep 合并 CE LOO（§5.3）：CE5_hybrid 过完整 §6 contract**（pooled 1.67%、median 0%、macro 2.90%、max 14.72%、candPair 0.808）；CE3（max 22.71）/CE4（max 26.89）fail max；CE0/CE2 退化、CE1 17.8%。
- **推断**：6-rep tighten mean 把 CE5 max regret 从 39.77%→14.72%（−63%）——**证实"高 CV 噪声驱动 max regret"假设**；CE5 的残差校正在紧数据上现增益（max 14.72 vs CE3 22.71，3-rep 时 CE3≈CE5）。CE5 row MAE 3.98 > CE3 3.23 但 regret 更低 → accuracy≠selection（Heinrich）。大 workload 倾向大 active-work（98304）。
- **不能声称**：CE5"稳健过 contract"——max 14.72% **贴 15% 线，marginal pass**，换 split/更多 context 可能翻转；"CE5 优于 baseline"——本实验无系统 baseline（CE0-CE5 是代价估计方法 baseline，非数据库 AI 算子系统 baseline），只能说 CE5 在 4 active-work 候选选择上过 §6；CE4"不增益"是 20 context 小数据观察，非 LightGBM 普遍结论。

## 7. 对课题含义

算子代价估计（共同使能组件）有了第一份**有效**双 4090 数据 + LOO 评估（6-rep 补跑后）：4 active-work 候选在同 context 内有强 e2e 差异（最高 86%），最优点 context-dependent；**CE5_hybrid（解析+残差校正）过完整 §6 promotion contract**（max 14.72%，marginal）——**首个可按 §6 接管 active-work 候选选择的估计器**。6-rep 补跑证实高 CV 噪声是先前 max regret 过高的根因（39.77%→14.72%）。**条件性下一步**：CE5 过 §6 满足 plan §8 TPC-H-derived 计划级 capability 的前置（须 CE5 在计划级再验证 + max 14.72% 的 marginal 程度需更多 context 确认稳健性）。

## 8. 下一步

1. **gate 8 补跑（最高优先，直接降 max regret）**：补跑 63 个 CV>5% cell（尤其 13 个 mean>15s 的 o64 cell）至 CV≤5% 或预注 reps floor；这是 max regret 39.77% 的根因。
2. **CE4 LightGBM 补跑**：装 lightgbm 到 text-baselines venv，重跑 context-LOO（数据已就绪）。
3. **harness 路径硬编码修复**：`compare_cost_estimators_contextloo.py` 的 `load_rows`/`_source_evidence` 假设 REF_JSON + source_csv 在 REPO_ROOT 下（本评估用 wrapper 临时 patch + 把 runs.csv 拷进 repo 树）；改为 `--data-csv` 参数化。
4. **contract 口径澄清**：harness 的 promotion_contract 用行级 pairwise，但 plan §6 指 candidate-aggregated（CE3/CE5 0.758 实际过）——harness 应改用 candidate pairwise 作 gate，避免误判。
5. plan §8 TPC-H-derived 计划级 capability 仍 `planned-conditional`：须先有估计器过 §6 完整 contract。

## 9. Erratum（2026-08-07 复审修正）

经 6-dimension 对抗式复审（6 reviewer × 每发现 1 skeptic 验证；23 finding / 18 confirmed / 5 refuted），本 README 先前版本有 **2 处 HIGH 口径错误，已在上文修正**：

1. **CE regret 口径**（§5.3/§6/banner）：先前把 **pooled** decision_regret 3.70% 当作 "过 plan §6 的 5% 门槛"。实际 §6 freeze median/macro/max；CE3/CE5 macro 6.42% + max 39.77% **FAIL**。修正：§5.3 表现完整 median/macro/max 矩阵。
2. **CE pairwise 口径**（§5.3/§6）：先前把 **行级** pairwise（0.692）当 contract 阻塞点。实际 plan §6 指 **candidate-aggregated** pairwise；CE3/CE5 = 0.758 **PASS**。真正 blocker 是 macro/max regret（非 pairwise）。修正：§5.3 改报 candidate pairwise + §6/§7 重述 blocker。
3. **Gate 8 归因**（§3）：先前 "全部 short-fast cell（3–4s）" **不实**——实测 median 8.70s、13 cell >15s、26 cell >10s，高 CV 集中在 o64 跨 workload。修正：§3 gate 8 行重述 + 标"未满足（补跑未做）"。

**结论方向不变**（无估计器过完整 contract），但 blocker 从 "pairwise" 改为 "macro/max regret（高 CV fold 驱动）"。其余 confirmed medium finding（60s 门用 operator_wall 而非 model_serving_wall、§provenance comparison_role 机器闭合缺失、prediction 层指标原未列表等）记录在案、低风险。复审 workflow 脚本留 `journal.jsonl` 可追溯。

### 9.1 续（2026-08-07，6-rep 补跑后）

**结论方向已变**：3-rep 下全部 FAIL；**6-rep 补跑（gate 8 闭环）后 CE5_hybrid 转 PASS**（max 39.77%→14.72%，证实高 CV 噪声驱动假设）。这不是改判 3-rep 结论——3-rep 数据下确实全 FAIL（口径正确）；而是**补跑提供了更紧的 oracle 标签**，使 CE5 的最差 fold regret 降到 15% 内。**新事实**：CE5 过 §6（marginal，max 14.72 贴 15）、CE3/CE4 仍 fail max、CE4 LightGBM 首次跑（未增益）。**仍不能声称**：CE5 稳健过门（marginal）、CE5 优于系统 baseline（本实验无系统 baseline）。raw SHA + merged_runs_6rep + ce_context_loo_rerun 见本目录 + 服务器 `experiment-artifacts/dual_gpu_cost_profile_merged_6rep_20260807/`。

## provenance

- **raw archive SHA256**：`a4f9cd5220306789d94f3a5c47169f3d967eb6fad0b5310f51ab5f8670314e8f`（全 66MB raw，服务器 `/root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260807/`）。
- 本目录：`runs.csv`（320×296 全指标）+ `manifest.json`（status/completed_runs/schedule/redacted_config/incidents）+ 本 README。raw per-run requests/submissions/stdout/stderr（含 output_text）未进 git（体积；同项目 raw 排除口径），服务器可复核。
- 驱动：run_ai_operator_scenarios.py + postgres_ai_operator_profile.py，commit `88aec55`（v2 cache-on 合同，server at e49ac53+ for the hang fix unrelated to this run's path）。vLLM 服务 = 4 路径 ramp 同进程（prefix-cache ON、三 flag）。
- 失败/异常：0 incident、0 failed-status formal、0 local-Ray。
