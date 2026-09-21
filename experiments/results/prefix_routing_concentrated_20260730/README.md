# Prefix-affinity routing 消融——concentrated ShareGPT

日期：2026-07-30（运行于 2026-07-30，2026-07-31 补数据 + 报告）
分支：`main`
平台：AutoDL 2×4090，2× Qwen2.5-7B-Instruct（1 endpoint/卡），vLLM 0.25.1，PG 18.4 + pgvector 0.8.5

**跨数据集合并分析**（agent-trace + concentrated 对比）：见
[`../prefix_routing_agent_20260730/README.md`](../prefix_routing_agent_20260730/README.md) §4–§7。
**分散 ShareGPT 前置报告**：[`../prefix_cache_routing_req_20260730/README.md`](../prefix_cache_routing_req_20260730/README.md)。

## 1. 研究问题与设计

**问题**：分散 ShareGPT（766 session）下 prefix_affinity 相对 least_queued 完全中性（−0.1%，见前置报告）。本实验换成 **prefix 高度集中**的 workload（`sharegpt_concentrated`，2,048 行、283 session、组内行数更多），检验：在 prefix 集中度更高、单组更大的 workload 上，2-ep/7B 下 prefix-affinity routing（含二级排序 pala）是否仍中性。

**三臂**（固定 batching=token_budget，只变 routing；第三臂叠加二级排序）：

| 场景 | routing | batching-policy |
|---|---|---|
| `route_least_queued_tb` | least_queued | token_budget |
| `route_affinity_tb` | prefix_affinity（rendezvous hash → 同 prefix 钉同一 endpoint） | token_budget |
| `route_affinity_pala_tb` | prefix_affinity + prefix_aware_length_align 二级排序 | prefix_aware_length_align_token_budget |

1 warmup + 3 formal repeats，formal 由 runner 交错洗牌。

## 2. 实验设置（链路、参数、数据来源）

- **链路**：PostgreSQL（`documents` 表，`sharegpt_concentrated` workload）→ 请求 manifest（`/root/autodl-tmp/gates/` 下 jsonl）→ Ray actor pool（1×256，request 粒度）→ 2 个 vLLM endpoint（`http://127.0.0.1:8000/8001/v1/completions`，GPU 0;1）→ Completions 响应。本实验**无写回**（writeback=none，只测推理链路吞吐/延迟）。
- **数据来源**：`sharegpt_concentrated`，2,048 行，由 ShareGPT 多轮对话按 prefix 集中重组而来，283 session、组内行数更多 → prefix 重复率高。
- **参数含义**：K256 = 全局 max-inflight 上限；W65536 = 每 endpoint 的 prompt+预估 output work credit（active-work 准入上限）；token_budget=8192 = 每批 prompt+output token 预算；request 粒度 = 逐请求提交与路由（`prefix_affinity` 的 per-request rendezvous hash 只在 request 级有意义，`manifest_guard.py:82-93` 强制）；SLO 30s。
- **指标含义**：`model_request_tokens_per_s` = model-request 层吞吐（主指标）；`request_e2e_s_p50/p95` = 单请求端到端延迟分位；`request_slo_violation_ratio` = 超过 30s SLO 的请求占比；`request_slo_goodput_per_s` = 满足 SLO 的 goodput。
- **prefix cache ON**：两 endpoint 均带 `--enable-prefix-caching`（前置报告已用 live probe 校验、日志显示 Prefix cache hit rate ~71%）。
- **配置**：由分散报告的 `prefix_cache_routing_req_20260730.json` 派生，仅替换 `source_workload_name=sharegpt_concentrated`，其余硬编码（token-budget=8192、W65536、request 粒度、SLO 30s）保证三臂是干净 A/B。manifest `status=completed`，12/12 ok，**0 incident**。

## 3. 实验数据（model-request tok/s 中位数，3 formal reps）

| 场景 | mr tok/s 中位数 | raw | CV | vs least_queued | P50 s | P95 s | SLO 违约 | goodput/s |
|---|---|---|---|---|---|---|---|---|
| route_least_queued_tb | 19,639 | [19666, 19639, 19600] | 0.1% | — | 45.7 | 84.3 | 68% | 7.4 |
| route_affinity_tb | 19,635 | [19873, 19635, 19500] | 0.8% | −0.0% | 45.9 | 84.5 | 69% | 7.2 |
| route_affinity_pala_tb | 19,874 | [19845, 19958, 19874] | 0.2% | +1.2% | 45.1 | 82.6 | 68% | 7.6 |

数字来自 `runs.csv`（formal 行，按 scenario 取中位数）。GPU 利用率 p50 = 100%、KV cache p95 ≈ 0.4（两数据集一致）。

## 4. 结果解释

### 事实
- **吞吐三臂全部中性**：affinity −0.0%、pala +1.2%，|Δ| < 1.2%，远低于 5% 晋升门禁；CV ≤0.8%。
- **pala 的尾延迟/SLO 信号很弱**：P50 45.1 vs 45.7s（−1.3%）、P95 82.6 vs 84.3s、SLO 违约 68% vs 68%（持平）、goodput 7.6 vs 7.4（+3%）。方向与 agent-trace 上 pala 的信号一致（P50/SLO 改善），但幅度小得多。
- SLO 违约 68% 表明该 workload 同样处于**过饱和/cache 抖动区间**（2048 行、283 组、GPU 100%），但比 agent-trace（78–83%）轻。

### 推断
- **prefix 集中度提高并未让 routing 重新有效**：concentrated 的 prefix 重复率比分散更高，按「碎片化假设」本应更有利于 affinity 反碎片化，但吞吐仍中性。说明在 2-ep/7B、APC 覆盖 working set 时，即便 prefix 高度集中，naive least_queued 散到两 endpoint 后每个 endpoint 仍能看到足够重复 prefix 使 APC 命中——routing 无杠杆。
- **pala 信号弱是因为 cache 压力不够**：concentrated 的 283 组、KV p95 ~0.4 表明 per-endpoint APC 仍有余量，length-align 二级排序降低 HOL blocking 的杠杆小；对比 agent-trace（working set ≈ per-endpoint KV、高淘汰）pala P50 −7.8%，**信号随 cache 淘汰压力增大而增强**。

### 不能声称
- 不能声称 routing/pala 在 concentrated 上有收益：吞吐 \|Δ\| < 1.2%、pala P50 仅 −1.3%，均未过门禁。
- 不能声称「concentrated 与 agent 的差异来自 prefix 集中度」单一变量：两者同时 differs in 行数（2048 vs 851）、session 数（283 vs 133）、prompt 长度分布与 cache 压力；「cache 淘汰压力是开关」是跨数据集方向性推断，未经单独控制 cache 压力的隔离实验确认。
- per-arm APC 命中率仍未单独记录（`resources.csv` 只采样 KV 用量，与所有 prefix 实验同一缺口）。
- 结论类别：吞吐中性 = **本地实验事实**；「cache 淘汰压力是开关」= **合理推断（跨数据集方向性，待隔离验证）**；pala 在高 cache 压力下的 P50 收益 = **待确认（需人为缩 KV 单调验证）**。

## 5. 对课题含义

1. concentrated 与分散、agent-trace 一起，把 2-ep/7B routing 的中性结论扩展到**三种 prefix 浓度**的 workload（分散 766 / concentrated 283 / agent 133 session）。研究内容二的 routing 在 APC 充分覆盖 working set 时被 vLLM 内部覆盖，**收口判定稳健**。
2. concentrated 上 pala 信号弱、agent-trace 上 pala 信号强，组合起来支持「cache 淘汰压力是 prefix 方向价值是否显现的开关」——与 4-ep/1.5B 的 +5.9% 吞吐收益同机制（见 4ep 报告 §6、agent 报告 §5）。concentrated 是该框架的「低淘汰/中性」端数据点。
3. 不改变上游调度主线当前默认配置（sequential token-budget + static K8 + fixed 50ms）。

## 6. 下一步

1. **隔离 cache 压力变量**：在 2-ep/7B 上人为缩 `gpu_memory_utilization` 制造可控淘汰率，复测 pala P50 改善是否随淘汰率单调增强——若是，则「cache 压力开关」可定量刻画，concentrated（低淘汰）与 agent-trace（高淘汰）的差异可归因。
2. **补 per-arm APC 命中率**（`prefix_cache_hit_rate`），使 pala 的 P50 改善能干净归因到命中率差异。
3. 与 4-ep/1.5B、agent-trace 联动，把「cache 淘汰压力开关」作为 prefix 方向的总判定框架（见 `experiments/plans/experiment_status_and_gaps.md` P1 段）。
