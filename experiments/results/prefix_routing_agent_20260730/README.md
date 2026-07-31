# Prefix-affinity routing 跨数据集消融（agent-trace + concentrated）

日期：2026-07-30（运行于 2026-07-30，2026-07-31 补数据 + 报告）
分支：`main`
平台：AutoDL 2×4090，2× Qwen2.5-7B-Instruct（1 endpoint/卡），vLLM 0.25.1，PG 18.4 + pgvector 0.8.5
前置报告（分散 ShareGPT）：`../prefix_cache_routing_req_20260730/README.md`
伴生数据目录：`./`（agent-trace）+ `../prefix_routing_concentrated_20260730/`（concentrated）

## 0. 本报告的位置

本目录（`prefix_routing_agent_20260730/`）是 **agent-trace** 数据集的运行数据 + 跨数据集合并分析。`prefix_routing_concentrated_20260730/README.md` 是 concentrated 数据集的自包含简表 + 指回此处的指针。三份报告（分散 / agent / concentrated）共同构成 2-endpoint/7B 下 prefix-affinity routing 的跨数据集稳健性证据。

## 1. 研究问题与设计

**问题**：分散 ShareGPT（766 session、prefix 重复率中等）下 `prefix_affinity` 相对 `least_queued` 完全中性（−0.1%，见前置报告 §4）。该报告 §「不能声称」明确「低 prefix 重复率 workload 未测」。本组实验回答：**在不同 prefix 浓度 / cache 压力的真实 workload 上，2-ep/7B 下 prefix-affinity routing（含二级排序 pala）是否仍中性，还是出现可测信号？**

**两个数据集**（其余配置完全一致，2 endpoint/7B、request 粒度、K256、W65536、token_budget 8192、SLO 30s、cache ON）：

| 数据集 | workload | 行数 | session | prefix 共享率 | cache 压力 |
|---|---|---|---|---|---|
| agent-trace | `lmcache_agent` | 851 | 133 | 高（多轮 agent 对话，后续轮含全部历史） | 高（working set ≈ per-endpoint KV） |
| concentrated | `sharegpt_concentrated` | 2,048 | 283 | 高（人为集中 prefix 分组） | 中（2048 行、283 组，KV 余量更大） |

**三臂**（固定 batching=token_budget，只变 routing；第三臂叠加二级排序）：

| 场景 | routing | batching-policy |
|---|---|---|
| `route_least_queued_tb` | least_queued | token_budget |
| `route_affinity_tb` | prefix_affinity（rendezvous hash → 同 prefix 钉同一 endpoint） | token_budget |
| `route_affinity_pala_tb` | prefix_affinity + prefix_aware_length_align 二级排序 | prefix_aware_length_align_token_budget |

1 warmup + 3 formal repeats，formal 由 runner 交错洗牌。

## 2. 实验设置

- Completions 协议，httpx_async，return-token-ids。
- 2 endpoint（GPU 0;1，各 1× Qwen2.5-7B），prefix cache ON（`--enable-prefix-caching`，前置报告已用 live probe 校验）。
- K256（max-inflight），W65536（per-endpoint active-work credit），1×256 actor pool，request 粒度（`manifest_guard.py:82-93`：`prefix_affinity` 仅在 request 粒度被允许）。
- 配置由分散报告的 `prefix_cache_routing_req_20260730.json` 派生，仅替换 `source_workload_name`，保证三臂在每个数据集内是干净 A/B。

## 3. 失败与重跑

- 两数据集 manifest 均 `status=completed`，agent 12/12、concentrated 12/12，**0 incident**。
- 无 raylet 崩溃、无 OOM、无重跑（与 4-ep/1.5B 实验的 stale-Ray 事故无关；见 `../prefix_cache_routing_4ep_1.5b_20260731/README.md` §4）。

## 4. 实验数据（model-request tok/s 中位数，3 formal reps）

### 4.1 agent-trace（`lmcache_agent`，高 cache 压力）

| 场景 | mr tok/s 中位数 | raw | CV | vs least_queued | P50 s | P95 s | SLO 违约 | goodput/s |
|---|---|---|---|---|---|---|---|---|
| route_least_queued_tb | 20,167 | [20171, 19991, 20167] | 0.4% | — | 69.6 | 124.2 | 82% | 1.2 |
| route_affinity_tb | 19,909 | [19921, 19830, 19909] | 0.2% | −1.3% | 71.0 | 125.6 | 83% | 1.1 |
| route_affinity_pala_tb | 19,776 | [19776, 20137, 19755] | 0.9% | **−1.9%** | **64.2** | 123.6 | **78%** | **1.4** |

### 4.2 concentrated（`sharegpt_concentrated`，中 cache 压力）

| 场景 | mr tok/s 中位数 | raw | CV | vs least_queued | P50 s | P95 s | SLO 违约 | goodput/s |
|---|---|---|---|---|---|---|---|---|
| route_least_queued_tb | 19,639 | [19666, 19639, 19600] | 0.1% | — | 45.7 | 84.3 | 68% | 7.4 |
| route_affinity_tb | 19,635 | [19873, 19635, 19500] | 0.8% | −0.0% | 45.9 | 84.5 | 69% | 7.2 |
| route_affinity_pala_tb | 19,874 | [19845, 19958, 19874] | 0.2% | +1.2% | 45.1 | 82.6 | 68% | 7.6 |

## 5. 结果解释

### 事实
- **吞吐维度：两数据集三臂均中性**。agent 上 affinity −1.3% / pala −1.9%；concentrated 上 −0.0% / +1.2%。全部 |Δ| < 2%、均低于 5% 晋升门禁，CV ≤0.9%、repeat 基本不重叠方向不一致。
- **agent-trace 上 pala 出现尾延迟 / SLO 方向性信号**（前置分散报告未观察到）：P50 **64.2 vs 69.6s = −7.8%**、SLO 违约 **78% vs 82% = −3.8pp**、SLO goodput **1.4 vs 1.2 = +17%**。P95/P99 方向相同但幅度更小（123.6 vs 124.2s）。
- **concentrated 上 pala 的同方向信号很弱**：P50 45.1 vs 45.7s（−1.3%）、SLO 持平 68%。该数据集 cache 压力更低（KV 余量大），与「信号随 cache 压力增大而增强」一致。
- 两数据集 GPU 利用率均 100%、KV p95 均 ~0.4 → 均处于 GPU 饱和、cache 接近满载的 regime。

### 推断
- **吞吐中性可推广到不同 prefix 浓度的 2-ep/7B workload**：无论 prefix 集中（concentrated）还是高 cache 压力（agent），2 endpoint/7B 下 vLLM APC 仍覆盖上游 routing 能做的 prefix 优化 → 吞吐中性结论稳健。这强化了前置报告 §4 的判定。
- **agent-trace 的 pala 尾延迟信号是 cache 压力驱动的**：agent 的 working set 让 per-endpoint APC 处于高淘汰区间（SLO 违约 78–83% = 过饱和/thrashing），此时 length-align 二级排序降低 batch 内 HOL blocking、缩短等待，P50/goodput 改善；而 concentrated 的 cache 余量大，同样的二级排序无杠杆。这与 4-ep/1.5B（同样高淘汰压力）下 routing 显现 +5.9% 吞吐收益的机制同源——**cache 压力是 prefix 方向信号是否显现的关键开关**。

### 不能声称
- **不能声称「pala 改善吞吐」**：agent 上 pala 吞吐 −1.9%（负），concentrated +1.2%（低于门禁）。pala 的信号只在 P50/SLO/goodput 维度，不在吞吐；且未过 5% 门禁，不作为策略晋级依据。
- **不能声称「pala 普遍改善尾延迟」**：只在 agent-trace（单一高 cache 压力 workload、2-ep/7B、过饱和 regime）观察到；concentrated 上信号弱。pala 的 P50 改善幅度（−7.8%）处在 thrashing 区间（SLO 违约 78–83%），绝对 tail latency 仍很差，相对比较成立但绝对值不可外推。
- **per-arm APC 命中率仍未单独记录**（与前置/4-ep 报告同一缺口）：cache 行为臂间不等价只能由吞吐+tail 的方向性差异间接推断，无法干净归因到命中率。
- **agent-trace 的 −1.9% 吞吐 + P50 改善不矛盾**：length-align 改变 batch 组成可降低某些请求的等待，同时略微降低整体 decode 密度 → 吞吐微降但尾延迟改善，是合理的 tradeoff，不是测量噪声。

## 6. 对课题含义

1. **2-endpoint/7B routing 中性结论在跨数据集上稳健**（分散 / agent / concentrated 三数据集吞吐全部 |Δ| < 2%）。研究内容二的 routing 在 APC 充分时被 vLLM 内部覆盖，维持「收口」判定。
2. **agent-trace 的 pala P50/SLO 信号 + 4-ep/1.5B 的 +5.9% 共同指向同一机制**：cache 淘汰压力是 prefix 方向价值是否显现的开关。低淘汰（2-ep/7B + 充分 KV）→ 上游 routing 无空间；高淘汰（1.5B/4-ep 或 agent 长上下文）→ 上游 routing / 二级排序重新有尾延迟甚至吞吐空间。
3. **为「跨引擎 KV 管理 / 共享 KV 池」（Mooncake 思路）提供第二个动机数据点**：agent-trace 在不改 endpoint 数/模型的前提下、仅靠 workload 的高 cache 压力就重现了「上游 prefix 优化重新有效」的现象，独立于 4-ep/1.5B 的 model×endpoint 混淆。
4. 不改变上游调度主线（数据组织 + 提交控制）的当前默认配置（sequential token-budget + static K8 + fixed 50ms）。

## 7. 下一步

1. **隔离 cache 压力变量**：在 2-ep/7B 上人为缩 `gpu_memory_utilization` 制造高淘汰区间，复测 pala P50 改善是否随淘汰率单调增强——若是，则「cache 压力开关」可定量刻画。
2. **补 per-arm APC 命中率**（`prefix_cache_hit_rate`），使 agent 的 P50 改善能干净归因到命中率差异而非 batch 形状。
3. 与 4-ep/1.5B 报告联动：把「cache 压力开关」作为 prefix 方向的总判定框架写入 `experiment_status_and_gaps.md`（2-ep/7B 中性是低淘汰端、4-ep/1.5B +5.9% 与 agent pala P50 是高淘汰端）。
