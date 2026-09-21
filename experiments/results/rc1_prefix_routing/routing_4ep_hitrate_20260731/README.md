# 4-ep prefix-affinity routing 重跑（带 prefix_cache_hit_rate）——归因闭合

## 1. 实验目的

闭合 4-ep/1.5B prefix_affinity 相对 least_queued 收益（原始 run +5.9%）的**机制归因**：到底是 prefix cache 命中率提升（→ KV/prefix 复用故事）还是 endpoint 数驱动的并行度（→ 非 KV）？为此给 runner 补了 `vllm_prefix_cache_hit_rate` 采集（P0 指标，见 `research/evaluation_metrics_survey_20260731.md` §P0#3），重跑同一 4-ep/0.43 配置、同 workload，直接读命中率。

## 2. 实验设置

- **平台**：AutoDL 2×4090，vLLM 0.25.1，PG 18.4 + pgvector。
- **拓扑**：4 endpoint Qwen2.5-1.5B-Instruct，2 endpoint/GPU，`gpu_mem_util=0.43`，端口 8000-8003，prefix-caching ON。
- **workload**：`sharegpt_multiturn`（2,048 行）。请求 manifest `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。
- **调度合同**：Completions、httpx_async、return-token-ids、K256、W65536、token_budget=8192、request 粒度、fixed-50ms、seed 20260729、1 warmup + 3 formal × 2 场景。
- **新增指标（P0）**：`vllm_prefix_cache_hit_rate`（= `prefix_cache_hits_delta / prefix_cache_queries_delta`，vLLM 计数器 delta）、`vllm_time_to_first_token_mean_s`。
- **远端代码**：git at `a26c1e2` + scp 的 3 个 P0 文件（`metrics.py`/`schema.py`/`postgres_ai_operator_profile.py`，来自 `308d2b8`；完整 git fetch 无 turbo 太慢，写up注明此 desync）。
- **原始数据**：`raw/`（runs.csv + manifest.json + 40 per-run trace）。

## 合规性自检

- **喂饱 vLLM：是，且过饱和**。`vllm_num_requests_running` mean **~261 / max ~365**（4 端合计），`waiting` mean 12–15 / max 38–54（有队列堆积）→ vLLM 是瓶颈、KV 抖动中。
- **⚠️ KV cache 重度饱和**：during-run `vllm_kv_cache_usage_perc` mean **0.8–0.9 / max 1.0**（80–100%）——**4-ep/0.43 是重淘汰 regime**。这**纠正了 KV-budget sweep 的过度泛化**（2-ep 测得 6–45% 无压力 → 不能推广到 4-ep；4-ep 高并发 + 小 per-endpoint 池 → 饱和）。
- **正式 feeding-saturation 门禁（vs bounded）未算**（无 bounded 臂）。

## 3. 实验设计

同原始 4-ep run（`prefix_cache_routing_4ep_1.5b_20260731`）的配置，唯一差别：runner 现在采集 `prefix_cache_hit_rate` + TTFT。两臂 A/B：`prefix_affinity`（rendezvous hash 钉同 prefix 到一端）vs `least_queued`（散到 4 端）。8/8 run、0 incident。

## 4. 实验数据（formal 中位数）

| 场景 | routing | tp_med | **prefix_cache_hit_rate** | TTFT mean | SLO 违约 | p95 |
|---|---|---|---|---|---|---|
| route_affinity_tb | prefix_affinity | 46,860 | **0.2740** | **0.365s** | 28.0% | 37.0s |
| route_least_queued_tb | least_queued | 45,735 | **0.2235** | 0.554s | 29.1% | 37.4s |

- **Δ 吞吐 = +2.5%**（本次）；原始 run 为 +5.9%。两 run：affinity 吞吐稳（~46,860–46,943），least_queued baseline 在变（44,317–45,735）→ **Δ ∈ [+2.5%, +5.9%]，跨 5% 门禁 borderline、不稳健**。
- **Δ 命中率 = +5.05pp**（0.274 vs 0.224）；**Δ TTFT = −0.189s**（affinity 更低，少 prefill）。

## 5. 结果解释

### 事实
- **prefix_affinity 提高 prefix_cache_hit_rate +5pp**（0.274 vs 0.224），同时 **TTFT 降 0.19s**、SLO 违约降 1.1pp、p95 降 0.4s。方向一致：**更少 prefill 重算**。
- 4-ep/0.43 KV cache **80–100% 饱和**（重淘汰）。命中率 ~22–27% → ~73–78% 查询 miss（部分是跨引擎散落：prefix 缓存在 endpoint A，请求被 least_queued 路由到 B → miss）。

### 推断（归因闭合）
- **机制是 prefix 复用，不是纯并行度**。affinity 把同 prefix 钉到一个 endpoint → 该 endpoint APC 命中更多 → 少 prefill 重算 → 吞吐↑、TTFT↓。在 4-ep KV 饱和 regime 下，这个机制产生 +2.5–5.9% 吞吐。
- **纠正 KV-budget sweep 的过度泛化**：2-ep 全 KV 范围无压力（6–45%）→ routing 中性，是对的；但**不能由此推断 4-ep 也无压力**——4-ep/0.43 实测 80–100% 饱和。routing 收益**只在 KV 压力 regime（4-ep）出现**，无压力 regime（2-ep）不出现。

### 不能声称
- **不能声称 "+5.9% 稳健跨 5% 门禁"**——重复测量显示 Δ ∈ [+2.5%, +5.9%]，单 run（原始 +5.9%）不足以 claim 跨门禁；它是 **borderline**。
- **不能声称 4-ep 饱和完全来自 per-endpoint 池小**——高并发（~261 running）+ prefix cache 累积都贡献；本实验未隔离两者。
- 正式 feeding-saturation 门禁（vs bounded）仍未算。

## 6. 对课题含义

1. **prefix routing 机制确认是 prefix 复用（在 KV 压力 regime）**——cross-engine prefix 散落真实（~73–78% miss，部分跨引擎），affinity 回收 +5pp 命中率 → +2.5–5.9% 吞吐。这**复活了"跨引擎共享 KV"方向的动机**（4-ep KV 压力 + 命中率机制都是真的）。
2. **但吞吐 payoff modest + variable**（+2.5–5.9%，borderline）→ prefix routing 是 weak/borderline 效应。共享 KV 池要进一步推高命中率（跨引擎复用、无 routing 约束），但 affinity 已回收一部分，**共享池的边际 upside 在本 workload 估计有限**。
3. **regime 依赖**：routing/KV 策略只在 KV 压力 regime（多 endpoint 整合 + 小 per-endpoint 池）有效；无压力 regime（2-ep/大池）无效。这给"何时值得做跨引擎 KV 管理"划了边界。

## 7. 下一步

1. **修正 KV-budget sweep README** 的"机制非 KV / Mooncake 动机削弱"过度泛化（2-ep 无压力对，但不能推广到 4-ep；4-ep 实测饱和）。
2. **#22 补 bounded HTTP baseline** → 正式算 feeding-saturation 门禁。
3. **3+ 重复**确认 Δ 的稳健性（+2.5% vs +5.9% 的方差）——borderline 结论需更多重复。
4. 共享 KV 池方向：动机复活但 payoff 有限；若推进，需在 4-ep 饱和 regime 评估 LMCache 能否把命中率推过 affinity 的 27%。
