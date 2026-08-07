# 双 4090 算子代价估计 v2 cache-on formal profile（320 runs，2026-08-07）

> **状态（plan §7）：本目录是 v2 cache-on 重跑的**有效数据 + 归档 + CE0–CE6 context-LOO 评估**。2026-08-04 首次 run 因并发共用 GPU + 空 `--ray-address`（每子 run local Ray）而**无效**（见 `../operator_cost_profile_dual4090_formal_20260804/`）；本轮 v2 修复后重跑，**320/320 有效、0 incident、gate 10（0 local-Ray）通过**——正是对首次无效的闭环。**
> **CE LOO 主结果（§5.3）**：CE3_ridge 与 CE5_hybrid pooled 决策 regret 3.70%、candidate pairwise 0.758、median fold regret 0%——但 **plan §6 contract 的 macro-mean regret 6.42%（>5%）+ max regret 39.77%（>15%）FAIL**，故**无估计器过完整 promotion contract**。
> ⚠️ **本节经 6-dimension 对抗式复审修正**（见文末 §9 erratum）：先前版本两处误——(a) 把 **pooled** 3.70% 当 "过 5% 门槛"，实际 contract 用 median/macro/max，其中 macro 6.42% + max 39.77% 不过；(b) 把 **行级** pairwise（0.692）当 contract 阻塞点，实际 plan §6 指 **candidate-aggregated** pairwise，CE3/CE5 = 0.758 **实际 PASS**——真正 blocker 是 macro/max **regret**（非 pairwise）。CE0/CE2 退化（regret 49.7%）；CE1 12.1%；CE4 LightGBM skipped。

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

## 3. 合规性自检（plan §4 11 门禁：**9 全过 + gate 6 由构造保证/LOO 间接验 + gate 8 未补跑，见 §6 限制**）

| 门 | 要求 | 结果 |
|---|---|---|
| 1 | manifest 320/320、0 unrecovered incident；80 warmup + 240 formal | ✅ status=completed, completed_runs=320, skipped=0, incidents=0, phase {warmup:80, formal:240}, 80 scenarios |
| 2 | 每 formal request/submission 数=context rows，doc_id exactly-once | ✅ 抽样 r128(128 unique)/r256(256 unique) 全 no-dup |
| 3 | 两 endpoint 均接收，resource trace ok | ✅ endpoint_count=2，vllm_request_success_delta 128–256（>0） |
| 4 | 非 replay `flush_trace_status=not_applicable_non_replay` | ✅ 全 {not_applicable_non_replay} |
| 5 | formal-only = 20 context × 4 candidate × 3 repeat；warmup 排除 | ✅ 240 formal（formal-only 过滤），80 scenario |
| 6 | 每 context 4 个 23 维特征向量 + candidate ID 不同 | ⏳ 延至 CE LOO 评估（runs.csv 296 列含全部特征，行级可查） |
| 7 | 服务快照（model/port/cache/max-batch/seqs） | ✅ model=qwen2.5-7b, gpu=2×4090, prefix_caching=enabled, vllm_running_max 有值 |
| 8 | CV>5% cell 单列、补跑、不静默删 | ⚠️ **未满足（补跑未做）**：63/80 cell e2e_s CV>5%（max 39.3% @ multi_r128_o64_w98304）；每 cell 恰 3 rep，**无补跑**。先前 "全部 short-fast 3–4s" **不准确**（复审 §9 修正）——实测 median 8.70s、**13 cell >15s、26 cell >10s**、仅 6 cell 在 3–4s；高 CV 集中在 **o64（小 output cap）跨多 workload**（multi/long/concentrated），非短 run 专属。单列不删，CE 用 3-rep mean，**但 SEM 在 39% CV cell ≈ 22%，使部分 context oracle 选法落入噪声**（与 §5.3 max regret 39.77% 同源，见 §6） |
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

### 5.3 CE0–CE6 context-LOO 评估（plan §5；leave-one-decision-context-out，240 formal 行 / 20 context / 每 context 4 candidate）

`ce_context_loo_20260807.json`（folds=20）。下表为**完整 plan §6 contract 矩阵**（复审 §9 修正：先前版本只报 pooled regret + 行级 pairwise，掩盖了真正的 blocker）：

| 估计器 | pooled regret% | §6 regret：median / macro / max（门槛 5/5/15%） | candidate pairwise（§6，≥0.75） | row MAE s（mean） | promotion contract |
|---|---|---|---|---|---|
| CE0_mean | 49.71 | 41.6 / 41.5 / 86.5（median/macro/max 全 FAIL） | 0.50 ✗ | — | **FAIL** |
| CE1_analytical | 12.11 | 9.8 / 12.1 / 41.2（全 FAIL） | 0.60 ✗ | 3.51 | **FAIL** |
| CE2_lookup | 49.71 | 41.6 / 41.5 / 86.5（全 FAIL） | 0.50 ✗ | — | **FAIL**（全 tie 退化） |
| **CE3_ridge** | 3.70（pooled） | **0.0 ✓ / 6.42 ✗ / 39.77 ✗** | **0.758 ✓** | 3.41 | **FAIL**（median 过；macro+max regret 不过） |
| CE4_lightgbm | — | — | — | — | skipped（lightgbm 依赖未装） |
| **CE5_hybrid**（解析+残差） | 3.70（pooled） | **0.0 ✓ / 6.42 ✗ / 39.77 ✗** | **0.758 ✓** | 4.24 | **FAIL**（macro+max regret 不过） |

- **pooled regret 3.70% 不是 plan §6 contract 指标**（它是 selected/oracle runtime 跨 20 context 求和比）。§6 freeze 的是 **median / macro-mean / max** fold regret；CE3/CE5 median 0% 过，但 **macro 6.42% > 5%、max 39.77% > 15%（2.6×）不过**。
- **pairwise 门槛 plan §6 指 candidate-aggregated**：CE3/CE5 candidate pairwise 0.758 **实际过 0.75**（harness 另报的行级 pairwise 0.692 不是 §6 gate；先前版本误把行级当 contract 是错的）。
- **故 CE3/CE5 真正的 blocker 是 macro/max regret，不是 pairwise**。CE6 oracle 仅作上界。CE3≈CE5（pooled/macro/max/MAE 几近一致 → 本数据上残差校正未在 Ridge 之上再增益；CE5 MAE 4.24 略高于 CE3 3.41）。
- **max regret 39.77% 与 §3 gate 8 同源**：高 CV（o64 cell）使个别 fold 的 oracle 选法落入噪声，estimator 选错 → 单 fold regret 飙高 → max 被拉大。补跑高 CV cell（gate 8）是最可能降低 max regret 的手段。

### 5.2 全指标（per-run 296 列）

见 `runs.csv`（320 行 × 296 列：吞吐/E2E 分位/TTFT-ITL/prefix-cache hit/vLLM running·waiting·KV/GPU util·mem·power·energy·MFU/pipeline 阶段计时/packing·service-quantum cost/actor 调度/SLO goodput/cost per M tok）。CE 特征向量（23 维）在其中。

## 6. 结果解释（事实 / 推断 / 不能声称）

- **事实**：320-run 数据有效（§3：9/11 门禁全过 + gate 6 由构造保证/LOO 间接验 + gate 8 补跑未做）；4 candidate 在每 context 产生 12–86% e2e 差异；最优 active-work 随 context 变；CE LOO（§5.3）CE3/CE5 pooled regret 3.70%、candidate pairwise 0.758（过 §6）、median fold regret 0%，**但 macro-mean 6.42% + max 39.77% 不过 §6**；CE0/CE2 退化（pooled 49.7%），CE1 12.1%。
- **推断**：数据含**强排序信号**（0 退化 context）；Ridge/解析+残差能把"选中候选"做到 candidate pairwise 0.758 + median regret 0%（多数 fold 选对），**但 macro/max regret 被 ~9 个高 CV（o64）fold 拉大**——这些 fold 的 oracle 选法落入 3-rep 噪声，estimator 选错即单 fold regret 飙高。CE3≈CE5（残差校正未在 Ridge 之上再增益）。大 workload（lmcache_agent、长 prompt、大 cap）倾向大 active-work（98304）。
- **不能声称**：任一估计器"过完整 promotion contract 可接管计划选择"——**全部 FAIL**（CE3/CE5 因 macro/max regret，非 pairwise；CE0/CE1/CE2 全维度不过）；CE4 LightGBM 未跑（skipped），CE3/CE5 vs CE4 对比待补；CE5"优于 baseline"不成立（与 CE3 持平）。**高 CV cell（63/80）非 "全 short-fast"**（实测 13 cell >15s），不能当数据质量失败丢弃，但也不能谎称已补跑——gate 8 未满足是**已记录的限制**，CE 用 3-rep mean 是 pragmatic 替代非门禁豁免。

## 7. 对课题含义

算子代价估计（共同使能组件）有了第一份**有效**双 4090 数据 + 首次 LOO 评估：4 active-work 候选在同 context 内有强 e2e 差异（最高 86%），最优点 context-dependent；可部署估计器（Ridge、解析+残差）在多数 fold 选对候选（candidate pairwise 0.758 过 §6、median fold regret 0%），**但 macro-mean 6.42% + max 39.77% regret 未达 §6**，主因是高 CV（o64）fold 的 oracle 落入噪声。按 §6 不接管计划选择。下一步若**补跑高 CV cell（gate 8）降低 max regret** + 补 CE4（LightGBM），可能过 contract → 进入 plan §8 TPC-H-derived 计划级 capability。

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

## provenance

- **raw archive SHA256**：`a4f9cd5220306789d94f3a5c47169f3d967eb6fad0b5310f51ab5f8670314e8f`（全 66MB raw，服务器 `/root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260807/`）。
- 本目录：`runs.csv`（320×296 全指标）+ `manifest.json`（status/completed_runs/schedule/redacted_config/incidents）+ 本 README。raw per-run requests/submissions/stdout/stderr（含 output_text）未进 git（体积；同项目 raw 排除口径），服务器可复核。
- 驱动：run_ai_operator_scenarios.py + postgres_ai_operator_profile.py，commit `88aec55`（v2 cache-on 合同，server at e49ac53+ for the hang fix unrelated to this run's path）。vLLM 服务 = 4 路径 ramp 同进程（prefix-cache ON、三 flag）。
- 失败/异常：0 incident、0 failed-status formal、0 local-Ray。
