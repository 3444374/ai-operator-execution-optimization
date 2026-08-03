# RC1 数据组织策略系统重测 plan（2026-07-31）

> 关联：执行流程遵循根 `AGENTS.md` §7.5（pre-flight / 干净合同 / 合规自检 / 全数据 README / 存储）。本 doc 只定 RC1 特定的合同、策略清单、拓扑、取代范围。

## 1. 为什么重测

早期 RC1 数据组织实验（07-18/19/26：token-tail、token-budget vs fixed、length-align、prefix-aware、output-aware BFD、row-cap、joint batching、text_heldout、vllm_cuda_graph）有三个问题：
1. **数据集旧/有问题**（sharegpt_burstgpt / 早期 multiturn，已调整）。
2. **配置摸索期**（单 RTX 5070、rows/s 无 tokens/s、无 token-IDs、urllib）。
3. **多数没喂饱 vLLM**（project ~8 tok/s vs bounded ~14.5 = ~55%，未达 feeding-saturation 门禁）→ 策略结论无意义。

→ 在当前干净平台（2×4090 + 最新 multiturn + tokens/s + httpx async + token-IDs）上系统重测一遍，所有策略同一合同 → 可比。**最原始的 07-18/19 动机实验（token-tail、token-budget）保留作历史参照**（见 #26 清理）。

## 2. 统一干净配置合同

- **平台**：AutoDL 2×4090，vLLM 0.25.1，PG 18.4 + pgvector。
- **拓扑（双测）**：**2-ep/0.9**（1 endpoint/GPU，干净低淘汰基线）+ **4-ep/0.43**（2 endpoint/GPU，consolidation/淘汰压力）。
- **workload**：`sharegpt_multiturn`（2,048 行，最新修正版）；可选 `sharegpt_concentrated` 做泛化对照。
- **指标**：`tokens_per_s`(E2E) + `model_request_tokens_per_s` + `operator_tokens_per_s` + `service_p99` + SLO + TTFT 分位（P50/P95/P99）+ TBT/ITL 分布 + `prefix_cache_hit_rate`（**prefix 策略归因的关键指标——不采命中率则 routing/分组收益无法证明来自 cache 复用**）+ per-arm APC 命中率（待 runner 增采，见 §6）。
- **transport**：httpx_async + return-token-IDs；prefix-cache ON。
- **调度合同**：K256 inflight / W65536 active-work / token_budget=8192（策略变量除外）/ request 粒度 / fixed-50ms flush / seed 固定。
- **重复**：1 warmup + 3 formal（formal 交错）。
- **baselines**（喂饱门禁锚点）：bounded HTTP（同协议无 Ray）+ vLLM Bench（容量上限）——**按各自标准方法测**，2-ep + 4-ep 各一份。

## 3. 策略清单（每个 × {2-ep, 4-ep}）

| # | 策略 | `--batching-policy` + 机制 | 说明 |
|---|---|---|---|
| 1 | fixed-row | `fixed_rows` | row-based baseline（非 token 感知） |
| 2 | sequential token-budget | `token_budget`（sequential） | 当前默认 |
| 3 | length-align | `token_budget` + length_align | 组内按长度排序降 HOL |
| 4 | output-aware BFD | `best_fit_token_budget` | bin-packing by output work |
| 5 | row-cap packing | `row_cap_aware_packing` | 行数上限 packing |

**prefix-aware 不重测**（07-30/31 `prefix_cache_data_org_*` + routing 已在干净平台测过，复用）。具体 flag 以 `postgres_ai_operator_profile.py --help` + 现有模板为准（建 config 时核验）。

## 4. 合规门禁（每个 run 跑完先自检，根 AGENTS §7.5.C）

1. 喂饱：`gpu_utilization_pct_mean` ≥ ~80% + vLLM running 高 + waiting 低。
2. feeding-saturation：E2E ≥ 95% of 同协议 bounded（用 §2 baselines）。
3. 策略到极限：参数在效应区间；A/B 同 config 仅策略不同。
4. 稳定：formal CV 合理。

任一不过 → 不抽策略结论，诊断/重跑。

## 5. 存储 + 写up

- **存储**：`experiments/results/rc1_data_organization/<strategy>_<topology>_<date>/{README.md, raw/}`（根 AGENTS §7.5.E）。
- **README**：根 AGENTS §7.5.D 顺序（目的/设置/合规自检/设计/全组件数据表/解释/含义/下一步）。
- **最终连贯报告**：一个 RC1 总报告（所有策略 × 两拓扑对比）+ registry + PROJECT_LOG + experiment_status §1.1 更新（标旧 RC1 为 superseded）。

## 6. 已知指标缺口（重测前最好先修）

- ~~`vllm_kv_cache_usage_perc` 当前实现可疑~~ → **已澄清（见 `kv_budget_sweep`）**：是分数（0–1）非百分比，按分数读正常可靠（0.06 = 6%，非"坏"）。重测已按分数读 + 用 TTFT/命中率/bounded 信号交叉印证 4-ep 饱和，无需改采其他指标。
- per-arm APC 命中率未采（resources.csv 只采样 KV 用量）→ runner 增采 `prefix_cache_hit_rate`。

## 7. 执行顺序

1. （修指标：§6，优先级高，影响所有 cache 结论。）
2. **#22 baselines**：bounded HTTP + vLLM Bench × {2-ep, 4-ep}。
3. **#23 策略**：5 策略 × {2-ep, 4-ep}（每 run 合规自检）。
4. **#24 写up + 同步**：连贯报告 + registry/PROJECT_LOG/experiment_status。
5. （#26 清理：重测后删/归档被取代的 07-25/26 gropy 实验，保 07-18/19 原始动机。）
