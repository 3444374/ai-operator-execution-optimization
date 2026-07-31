# RC1 数据组织策略系统重测（2-ep + 4-ep，Qwen2.5-1.5B，prefix-cache ON）

> 存储约定：`experiments/results/rc1_data_organization/<exp>_<date>/{README.md, raw/}`。本报告覆盖 2-ep/0.9 与 4-ep/0.43 两拓扑（同一 workload/合同/策略集），原始数据分别在 `dataorg_2ep_1.5b_cacheON_20260731/raw/`（102 文件）、`dataorg_4ep_1.5b_cacheON_20260731/raw/`（102 文件）、`bounded_2ep_batched_20260731/raw/`（2-ep 真上限）、`bounded_4ep_1.5b_cacheON_20260731/raw/`（4-ep，病态）、`bounded_2ep_1.5b_cacheON_20260731/raw/`（早期 batch-1，弱）。流程遵循根 `AGENTS.md` §7.5。
>
> **取代**：07-25/26 token-tail/token-budget/length-align gropy 实验（旧数据集、rows/s、单 5070、未喂饱）。最原始 07-18/19 动机实验保留作历史参照。

## 1. 实验目的

在当前干净平台（2×4090 + 最新 sharegpt_multiturn + tokens/s + httpx_async + token-IDs + P0 指标）系统重测 RC1 数据组织策略，回答：

- **Q1**：5 种组织策略（fixed_rows、sequential token_budget、length_align、best_fit/BFD、row_cap_aware）在 token 吞吐/延迟/命中率上是否有显著差异？
- **Q2**：差异是否 **regime-dependent**——即取决于 KV 压力 regime（2-ep/0.9 无压力 vs 4-ep/0.43 饱和）？
- **Q3**：cache-ON 下，组织方式对 prefix cache 命中率的影响机制？

关系到研究内容一（数据组织策略）+ 与 #28 prefix_affinity routing 归因（4-ep KV 压力下局部性才关键）的闭环。

## 2. 实验设置

- **平台**：AutoDL 2×4090，vLLM 0.25.1，PostgreSQL 18.4 + pgvector。
- **模型**：Qwen2.5-1.5B-Instruct（与 #19 KV-sweep / #28 routing 干净证据基线一致）。
- **拓扑**：**2-ep/0.9**（1 endpoint/GPU，干净低淘汰基线）+ **4-ep/0.43**（2 endpoint/GPU，consolidation/淘汰压力）。prefix-caching ON。
- **workload**：`sharegpt_multiturn`（2,048 行，prompt_tokens 3–1486，target_output 1–256）。manifest `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。
- **调度合同**：Completions / httpx_async / return-token-IDs / K256 inflight / W65536 active-work / token_budget=32768（策略变量除外）/ request 粒度 / fixed-50ms flush / least_queued routing / seed 20260729。
- **重复**：1 warmup + 3 formal（formal 交错）× 5 策略 × 2 拓扑 = 20 runs/拓扑。
- **P0 指标**：`prefix_cache_hit_rate`、`time_to_first_token_mean_s`、TBT/ITL 分布（本次新增采集，见 #27）。
- **配置**：`/root/autodl-tmp/gates/rc1_dataorg_{2ep,4ep}_1.5b_cacheON.json`、`rc1_bounded_2ep_1.5b.json`。
- **原始数据**：`dataorg_{2ep,4ep}_1.5b_cacheON_20260731/raw/`（runs.csv[262–266 cols] + manifest.json + per-run requests/submissions/resources CSV + stdout/stderr log）。

## 3. 合规性自检

| 项 | 2-ep/0.9 | 4-ep/0.43 | 判定 |
|---|---|---|---|
| 喂饱 vLLM（GPU util mean ≥~80%） | 79–85%（borderline） | **86–90%** | ✅（4-ep 明确喂饱；2-ep 79–80% 算 borderline 但 feed-confirmed） |
| `vllm_num_requests_running` | running_after=0（drained，during-run 在 resources.csv time-series） | 同 | 用 GPU util + tok/s 确认 |
| feeding-saturation（batched bounded 真上限） | ceiling **79,488**；策略 feed% **63–71%**（W65536 准入节流，非饿死） | ceiling 24,733 **病态**（unthrottled thrash 小 KV）；策略 160–202% 超过 | ⚠️ 严格 ≥95% 不过，但 `model_wall≈operator_wall` 无 pipeline 瓶颈（见 §3 机制解读） |
| 策略到极限 / 稳定 | matched-config，CV **1–6%** | CV **1–3%** | ✅ 稳定 |

**feeding-saturation 门禁（batched bounded 真上限，2026-07-31 补测；gate 已放宽到 ≥2 endpoint）**：

batched bounded（b16-c64 / b32-c32，2,048/2,048 完成、0 失败、两 cell 均 `status: passed`）真上限：
- **2-ep ceiling = 79,488 tok/s**（b32-c32，unthrottled，wall 20.8s）。
- **4-ep = 24,733 tok/s**（b16-c64，wall 66.9s）——**病态值**（见下），非有效上限。

| 拓扑 | bounded ceiling | 策略 E2E | feed% | 判定 |
|---|---|---|---|---|
| 2-ep | 79,488 | 50–56k | **63–71%** | ❌ 严格 ≥95% 门禁不过；但缺口=准入控制节流（非 pipeline 饿死） |
| 4-ep | 24,733（病态） | 39–50k | 160–202% | 策略*超过* unthrottled bounded → 该 bounded 非有效上限 |

**机制解读（这才是 feeding 门禁的真正含义）**：
1. **2-ep**：策略 `model_request_wall`(27.5s) ≈ `operator_wall`(27.5s) → **非模型开销可忽略、无 pipeline 瓶颈**。未达 79k 真上限的 21–37% 缺口 = **active-work 准入门（W65536）把 inflight 压到 4–22（远低于 K256 上限）**——是**故意节流**（换 SLO/公平），不是饿死 vLLM。GPU 79–85% 印证"在干活、但没榨干"。
2. **4-ep**：unthrottled batched bounded **自己搞慢自己**——在小 KV 池（0.43、KV max 98–100%）上一次性打 256 并发 batched 请求 → 淘汰风暴 + 重 prefill → 24k（比策略还低）。策略的 active-work 准入**反而帮忙**（节流到 inflight 8–22 → 少 thrash → 39–50k）。→ **unthrottled bounded 在 4-ep 不是有效上限；准入节流是 4-ep 解法的一部分。**

> **总判定**：策略**确实在喂 vLLM**（GPU 80–90%、`model_wall≈operator_wall`、非饥饿），但**不榨干 raw 上限**——因为 active-work 准入门是 binding 杠杆，且 regime-dependent：**2-ep 它压住上限（可放开 W 提速）、4-ep 它防 thrash（应保留）**。feeding 门禁结论从"过/不过"细化成"**准入控制是吞吐杠杆，效应随 regime 反向**"——直接接研究内容二（调度/提交控制）。

## 4. 实验设计

固定 workload/调度合同/策略集，**两维**：(a) 拓扑（2-ep/0.9 vs 4-ep/0.43），(b) 组织策略 5 种。每 cell = 1 warmup + 3 formal（formal 中位数报告）。`Δ%` = 4-ep 相对 2-ep。

## 5. 实验数据（全组件，formal 中位数）

### 5.1 吞吐 + 端到端延迟

| 拓扑 | 策略 | E2E tok/s | 模型侧 tok/s | operator tok/s | rows/s | req p95(s) | SLO 违约 | goodput/s |
|---|---|---|---|---|---|---|---|---|
| 2-ep | fixed_rows_16 | **56,291** | 61,251 | 61,219 | 69.4 | 27.5 | 0% | 69.4 |
| 2-ep | sequential_tb | 55,579 | 63,451 | 63,419 | 68.6 | 28.1 | 0% | 68.6 |
| 2-ep | length_align_tb | **50,304** | 56,679 | 56,648 | 62.1 | 30.7 | 6% | 58.3 |
| 2-ep | best_fit_tb | 54,708 | 60,381 | 60,350 | 67.5 | 29.8 | 0% | 67.5 |
| 2-ep | row_cap_aware_tb | 51,769 | 58,878 | 58,847 | 63.9 | 31.6 | **23%** | 49.2 |
| 4-ep | fixed_rows_16 | 47,396 | 53,214 | 53,182 | 58.5 | 34.3 | 21% | 46.2 |
| 4-ep | sequential_tb | **50,012** | 55,358 | 55,326 | 61.7 | 32.3 | 17% | 51.2 |
| 4-ep | length_align_tb | **39,446** | 43,078 | 43,047 | 48.6 | 40.2 | 21% | 38.3 |
| 4-ep | best_fit_tb | 40,304 | 44,471 | 44,440 | 49.7 | 40.6 | **60%** | 19.9 |
| 4-ep | row_cap_aware_tb | 40,354 | 44,331 | 44,300 | 49.8 | 40.6 | **60%** | 19.9 |

- **2-ep 策略挤在 50–56k（紧凑）**；**4-ep 策略分化为 39–50k（两簇）**：fixed/sequential ≈ 47–50k，重排序类（length_align/best_fit/row_cap）≈ 39–40k。
- E2E/模型侧 ≈ 88–92%（pipeline 开销 ~8–12%）。

### 5.2 vLLM 模型服务（含 P0 指标）

| 拓扑 | 策略 | KV mean | **KV max** | prefix_cache_hit_rate | TTFT mean(s) | vLLM e2e(s) | prompt Δ | gen Δ |
|---|---|---|---|---|---|---|---|---|
| 2-ep | fixed_rows_16 | 4% | 7% | 0.71 | 0.20 | 1.80 | 1,170,963 | 486,043 |
| 2-ep | sequential_tb | 4% | 7% | 0.75 | 0.29 | 1.88 | 1,170,963 | 485,338 |
| 2-ep | length_align_tb | 7% | 10% | **0.60** | 0.28 | 2.14 | 1,170,963 | 485,518 |
| 2-ep | best_fit_tb | 7% | 10% | **0.76** | 0.26 | 2.02 | 1,170,963 | 484,841 |
| 2-ep | row_cap_aware_tb | 7% | 10% | 0.68 | 0.31 | 2.07 | 1,170,963 | 484,759 |
| 4-ep | fixed_rows_16 | 62% | **98%** | 0.48 | 0.61 | 4.21 | 1,170,963 | 485,496 |
| 4-ep | sequential_tb | 67% | **100%** | 0.47 | 0.72 | 4.33 | 1,170,963 | 483,989 |
| 4-ep | length_align_tb | **79%** | **100%** | **0.06** | 0.96 | 5.42 | 1,170,963 | 484,216 |
| 4-ep | best_fit_tb | 79% | **100%** | **0.07** | 1.09 | 4.94 | 1,170,963 | 485,678 |
| 4-ep | row_cap_aware_tb | 78% | **100%** | **0.07** | 1.05 | 4.95 | 1,170,963 | 485,188 |

> `vllm_kv_cache_usage_perc` 是分数（0–1），表内已 ×100 显示为百分比（vLLM HELP: "1 means 100%"）。
> **KV max 4-ep 普遍 100%** = KV 池打满（淘汰压力真实）；2-ep max 7–10% = 无压力。prompt Δ 跨策略一致（同 workload）→ 吞吐差异来自命中率/重算，非数据量。

### 5.3 GPU + 能耗 + MFU（拓扑级中位数）

| 指标 | 2-ep | 4-ep |
|---|---|---|
| GPU util mean / max | 83% / 100% | 89% / 100% |
| GPU mem used mean (MiB) | 46,196 | 30,112 |
| GPU mem util % | 94% | 61% |
| GPU power mean (W) | 664 | 704 |
| GPU energy (J) | 20,420 | 28,517 |
| **J / 1k tokens** | **12.3** | **17.2（+40%）** |
| MFU | 未产出（status 字段，无数值） | 同 |

- 4-ep 能效更差（17.2 vs 12.3 J/1k tok）——consolidation 既慢又费能。

### 5.4 pipeline 阶段计时（秒，拓扑级中位数）

| 阶段 | 2-ep | 4-ep |
|---|---|---|
| db_fetch / source_fetch | 1.43 | 2.43 |
| organizer (plan+collect) | 0.13 | 0.10 |
| submit | 0.13 | 0.14 |
| fanin | 0.02 | 0.02 |
| actor_ready | 1.30 | 1.30 |
| **bounded_wait**（active-work 准入，与模型服务并发） | **25.68** | **34.13** |
| model_request_wall | 27.51 | 37.16 |
| operator_wall / E2E wall | 27.52 | 37.17 |

- 非模型开销小（db_fetch ~1.4–2.4s + organizer ~0.1 + submit ~0.1 + fanin ~0.02）；`bounded_wait` 是 wall 主体（与模型服务并发、非相加）。
- 4-ep wall 比 2-ep 高 ~35%（37 vs 27.5s）→ 吞吐低。

### 5.5 Ray / actor / packing（机制关键）

| 拓扑 | 策略 | inflight seen / limit | packing util | batch_tokens p95 | **prefix_group_ratio** |
|---|---|---|---|---|---|
| 2-ep | fixed_rows_16 | 12 / 256 | 0.00（行基） | 17,655 | **0.29** |
| 2-ep | sequential_tb | 4 / 256 | 0.97 | 35,334 | 0.13 |
| 2-ep | length_align_tb | 4 / 256 | 0.97 | 37,315 | **0.03** |
| 2-ep | best_fit_tb | 4 / 256 | 0.99 | 37,156 | **0.03** |
| 2-ep | row_cap_aware_tb | 4 / 256 | 0.99 | 37,808 | **0.03** |
| 4-ep | fixed_rows_16 | 22 / 256 | 0.00 | 17,676 | **0.29** |
| 4-ep | sequential_tb | 8 / 256 | 0.97 | 35,244 | 0.13 |
| 4-ep | length_align_tb | 8 / 256 | 0.97 | 37,056 | **0.03** |
| 4-ep | best_fit_tb | 8 / 256 | 0.99 | 37,194 | **0.03** |
| 4-ep | row_cap_aware_tb | 8 / 256 | 0.99 | 37,732 | **0.03** |

- **`prefix_group_ratio` 是机制 smoking gun**：fixed_rows 保序 → 0.29（prefix 组内聚）；sequential 0.13；**重排序类（length_align/best_fit/row_cap）= 0.03（prefix 组被打散）**。
- inflight seen（4–22）远低于 K256 上限 → **active-work 门（W65536）是 binding 约束，非 K256**。

## 6. 结果解释

### 事实
- **2-ep（无 KV 压力，max 7–10%）**：5 策略 E2E 50–56k（紧凑），fixed_rows ≈ sequential_tb > best_fit_tb > row_cap_aware_tb > length_align_tb。prefix 命中 0.60–0.76（best_fit 最高，length_align 最低）。
- **4-ep（KV 饱和，max 98–100%）**：5 策略 E2E 39–50k（分化为两簇），**排名反转**：sequential_tb(50k) > fixed_rows(47k) >> row_cap≈best_fit(40k) > length_align(39k)。
- **prefix 命中在 4-ep 崩塌**：重排序类 0.60–0.76 → **0.06–0.07**；保序类 fixed/sequential 仍 0.47–0.48。
- **4-ep 比 2-ep 慢 10–26%**（consolidation 是惩罚，非收益），且能耗 +40%。
- TTFT：2-ep 0.2–0.3s → 4-ep 0.6–1.1s（重排序类最高 ~1.1s）。

### 推断（机制闭合）
- **机制（cache-ON 下）**：组织方式决定 `prefix_group_ratio` → 决定 prefix cache 命中。重排序类 organizer（length_align 按长度排、best_fit 按 output work 装箱、row_cap 按行数上限装）**破坏 doc_id 顺序 → 打散同 prefix 请求 → prefix_group_ratio 0.03**。
  - 2-ep 无压力时影响小（KV 放得下，淘汰少）→ 命中仍 0.60–0.76，吞吐差异 ~12%。
  - **4-ep KV 饱和时影响放大**：同 prefix 请求被 least_queued 散到 4 端 + 淘汰前无法复用 → 命中崩到 0.06–0.07 → prefill 重算激增 → TTFT 翻倍、吞吐/tail 崩。保序类（fixed/sequential）保留局部性 → 命中 0.47 → 受影响小。
- 这与 #28 prefix_affinity routing 归因闭环：**4-ep KV 压力下，局部性/路由才决定性影响吞吐**；2-ep 无压力 regime 下组织方式差异小。

### 不能声称 / 待查
- **不能声称"X 策略绝对优于 Y"**——依赖 regime：2-ep fixed/sequential 占优，4-ep sequential 占优，但 4-ep 全员慢于 2-ep。
- **feeding-saturation 门禁严格不过（2-ep 63–71% < 95%）**——但缺口是 active-work 准入节流（inflight 4–22 vs K256 上限），非 pipeline 饿死（`model_wall≈operator_wall`）。**不能声称"策略榨干 raw vLLM 上限"**——W65536 准入门是 binding 杠杆。4-ep unthrottled bounded 病态（24,733），非有效上限。
- sequential_tb 在 2-ep GPU 79%（borderline）；MFU 未产出（待查 mfu_status）。
- 仅 1 workload（sharegpt_multiturn）+ 1 model（1.5B）；`sharegpt_concentrated` 泛化对照未跑。

## 7. 对课题含义

1. **数据组织策略效应是 regime-dependent**——这是研究内容一的核心结论：不能脱离模型服务状态（KV 压力）谈组织策略优劣。2-ep（无压力）策略差异 ~12%；4-ep（饱和）差异 ~27% 且排名反转。
2. **cache-ON 下，"保 prefix 局部性"是组织策略的隐性目标**——重排序类（length_align/BFD/row_cap）以丢失 cache 复用为代价换取长度/output 均匀，在 KV 压力下净亏。**prefix-aware 组织**（`prefix_aware_token_budget`，已在 #28 侧验证）应能回收这部分命中。
3. **consolidation 非收益**：4-ep/0.43 比 2-ep/0.9 慢且费能——多 endpoint 小池 + 高 churn + 局部性丢失。这反衬 2-ep/0.9 是更高效的默认拓扑。
4. 与 #19（KV-sweep）/ #28（routing）共同支撑课题 spine：**上游调度/组织策略的价值在模型服务饱和 regime（4-ep）才显现**；2-ep 无压力 regime 是干净对照基线。

## 8. 下一步

1. **~~补 feeding-saturation 门禁~~ ✅ 已补（2026-07-31）**：batched bounded 真上限 = 2-ep 79,488 / 4-ep 24,733（病态）；结论从"过/不过"细化成"**准入控制是吞吐杠杆、效应随 regime 反向**"（2-ep 压住上限可放开、4-ep 防 thrash 应保留）。**新待办**：2-ep 放开 W65536（→∞或 K256）测策略能否逼近 79k 上限，验证准入节流是否为唯一缺口。
2. **prefix-aware 组织策略正文实验**：`prefix_aware_token_budget` × {2-ep, 4-ep}，验证能否把 4-ep 重排序类的命中率从 0.06 拉回，回收吞吐。
3. **泛化对照**：`sharegpt_concentrated` workload + 7B model（若推进多模态前需文本结论稳）。
4. **MFU 采集修复**（mfu_status 无数值）。
5. **#24 文档同步**：registry（experiment_status §1.1）标 07-25/26 为 superseded、本重测为 canonical；PROJECT_OUTLINE §当前最重要证据 补 regime-dependent 结论；PROJECT_LOG 记录。
