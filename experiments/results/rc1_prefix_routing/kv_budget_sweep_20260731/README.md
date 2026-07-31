# KV-budget × prefix_affinity routing 扫描实验（2-ep/1.5B）

## 1. 实验目的

回答一个**隔离归因**问题：4-endpoint/Qwen2.5-1.5B 实验里 prefix_affinity 相对 least_queued 的 **+5.9%**（`../prefix_cache_routing_4ep_1.5b_20260731/`），驱动因素到底是**每 endpoint KV 预算（淘汰压力）**，还是 **endpoint 数（consolidation 拓扑）**？

方法：固定 2 endpoint（1/GPU），只扫 `gpu_mem_util ∈ {0.3, 0.45, 0.6, 0.9}`（每 endpoint KV 从小到大、淘汰压力从高到低），每点测 `least_queued` vs `prefix_affinity`。如果驱动因素是 KV 预算，2-ep 在小 KV（高淘汰）下应复现 +5.9%；如果 2-ep 全程中性，则驱动因素是 endpoint 数。

这关系到跨引擎共享 KV（Mooncake/LMCache 方向）的动机定位：共享池的价值到底在"小 KV/高淘汰"还是"多 endpoint 碎片化"。

## 2. 实验设置

- **平台**：AutoDL 2×4090，vLLM 0.25.1，PostgreSQL 18.4 + pgvector。
- **拓扑**：2 endpoint Qwen2.5-1.5B-Instruct，**每 GPU 1 endpoint**，`gpu_mem_util` 扫描 ∈ {0.3, 0.45, 0.6, 0.9}（util 控制 per-endpoint KV 预算）。端口 8000/8001，prefix-caching ON。
- **每 endpoint GPU 显存占用（实测）**：0.3→~8.4GB、0.45→~12GB、0.6→~15.6GB、0.9→~22.8GB（权重 ~3GB + 上下文，余者为 KV）。
- **workload**：`sharegpt_multiturn`（2,048 行，doc_id 300000–302047，prompt_tokens 3–1486，target_output 1–256）。请求 manifest `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。
- **调度合同**：Completions 协议、httpx_async、return-token-ids、`--endpoint-gpu-ids 0,1`、K256 (max-inflight)、W65536 (active-work)、token_budget=8192、**request 粒度**、fixed-50ms flush、prefix-cache ON、seed 20260729。
- **重复**：1 warmup + 3 formal repeats × 2 场景（`route_least_queued_tb`、`route_affinity_tb`）。util 0.9 的第 3 个 formal rep 偶发失败（subprocess_nonzero），故 0.9 为 n=2，其余 n=3。
- **指标**：`model_request_tokens_per_s`（中位数）、`request_slo_violation_ratio`、`request_e2e_s_p95`、CV。
- **配置**：`/root/autodl-tmp/gates/prefix_routing_2ep_util{0.3,0.45,0.6}.json`（util 0.9 复用 `prefix_cache_routing_2ep_1.5b.json`）。
- **原始数据**：`raw/`（4 个子目录，每点 runs.csv + manifest.json + per-run request/submission/resource trace）。

## 合规性自检

- **喂饱 vLLM：是**。`vllm_num_requests_running` 持续 mean ~139 / max ~194，`vllm_num_requests_waiting` ≈ 0（max 0–3）→ GPU 持续满载、无饥饿。（`gpu_utilization_pct=0%` 是 `gpu_metrics_status=snapshot` 的瞬时采样假象，不可信。）
- **E2E 效率**：E2E tokens/s ≈ 模型侧的 **88–91%**（pipeline 开销 ~9–12%：DB fetch + Daft organize + fan-in；`writeback=none`）。
- **正式 feeding-saturation 门禁（E2E ≥95% of 同协议 bounded）= 未算出**：本扫描**没跑 bounded HTTP 臂**，缺同 config 的 bounded 上限。**合规缺口**——#22 RC1 baselines 补 bounded HTTP 后该门禁才可正式算。
- **指标异常（待排查）**：`vllm_kv_cache_usage_perc` 全 util 点 0.1–0.5%（mean 0.1–0.3%），与 util 0.3–0.6 的 13–15% SLO 违约（理应 KV 抖动）**不一致**——疑似 snapshot 采样漏峰或指标在该 setup 下不可靠。影响"KV 压力"叙事的可信度（吞吐/SLO 模式方向上仍支持"大 KV = 少重算 = 快"，但 KV-usage 指标未佐证）。
- **策略是否到极限**：routing 两臂同 config（仅 routing 不同）、同 ~139 running → matched-KV 对比有效；Δ 的相对结论不依赖绝对饱和度。

## 3. 实验设计

固定 endpoint 数 = 2、workload、调度合同，**只变 `gpu_mem_util`**（per-endpoint KV 预算）。每点 A/B：`prefix_affinity`（rendezvous hash by prefix_key 钉到一端）vs `least_queued`（散到两端的 baseline）。Δ = (affinity − least_queued) / least_queued。

参照点：4-ep/1.5B/0.43（`prefix_cache_routing_4ep_1.5b_20260731/`，每端 ~7GB KV，2 endpoint/GPU）= **+5.9%**。

## 4. 实验数据（model-request tok/s 中位数）

| util | per-endpoint 显存 | prefix_affinity | least_queued | Δ | SLO 违约（aff/lq） | P95 |
|---|---|---|---|---|---|---|
| 0.3 | ~8.4GB | 54,221 [54015,54518,54221] cv0.5% | 54,250 [53631,54678,54250] cv1.0% | **−0.1%** | 14.9% / 13.4% | 32.8s / 32.2s |
| 0.45 | ~12GB | 54,264 [54386,54264,53539] cv0.8% | 54,335 [54477,54335,54213] cv0.2% | **−0.1%** | 12.5% / 14.4% | 31.9s / 32.6s |
| 0.6 | ~15.6GB | 54,653 [54653,54195,54770] cv0.6% | 54,119 [54119,54324,54063] cv0.3% | **+1.0%** | 14.2% / 14.8% | 32.6s / 32.8s |
| 0.9 | ~22.8GB | 64,804 [64709,64900] cv0.2% (n=2) | 64,565 [64683,64447] cv0.3% (n=2) | **+0.4%** | 0.0% / 0.0% | 27.5s / 27.6s |

**端到端视角（E2E `tokens_per_s`，全链路 DB→organize→Ray→vLLM→fan-in，`writeback=none`）：**

| util | E2E affinity | E2E least_queued | E2E Δ | E2E/模型 (aff/lq) | E2E p95 (aff/lq) | E2E p99 (aff/lq) |
|---|---|---|---|---|---|---|
| 0.3 | 48,277 | 49,274 | **−2.0%** | 89.0% / 90.8% | 32.8s / 32.2s | 33.5s / 32.8s |
| 0.45 | 49,676 | 48,612 | **+2.2%** | 91.5% / 89.5% | 31.9s / 32.6s | 32.6s / 33.3s |
| 0.6 | 48,592 | 48,369 | **+0.5%** | 88.9% / 89.4% | 32.6s / 32.8s | 33.3s / 33.5s |
| 0.9 | 57,044 | 56,992 | **+0.1%** | 88.0% / 88.3% | 27.5s / 27.6s | 28.2s / 28.3s |

E2E 吞吐 48–57k tok/s（util 0.9 最优 ~57k、0% SLO；0.3–0.6 ~48–49k、13–15% SLO）；**E2E ≈ 模型侧的 88–91%**（pipeline 开销 ~9–12%）。

## 5. 结果解释

### 事实
- **2 endpoint 下，prefix_affinity 在整个 KV 预算范围内中性**：Δ ∈ [−0.1%, +1.0%]，4 个 util 点全部 <5% 门禁。
- 包括**正在淘汰抖动的 util 0.3–0.6**（SLO 违约 12–15%，P95 ~32s）——即便单 engine KV 小到引发明显抖动，2 endpoint 下 affinity 仍无收益。
- 只有 util 0.9（~22.8GB 显存，working set 完全放下，SLO 违约 0%）回到正常吞吐（~64.8k vs 抖动点的 ~54.2k）。
- **E2E 视角同结论但更噪**：E2E Δ ∈ [−2.0%, +2.2%]（pipeline 开销叠加波动），仍全部 <5% 门禁 → E2E 下 prefix_affinity 也中性。原结论（endpoint 数是驱动）在 E2E 视角不变。

### 推断（matched-KV 对比）
- **2-ep/0.45（~12GB 显存，~7–8GB KV）= −0.1%** 对 **4-ep/0.43（~7GB KV/端）= +5.9%**：per-endpoint KV 量级相当，**只差 endpoint 数（2 vs 4）**，结果从中性跳到 +5.9%。
- → **驱动因素是 endpoint 数（consolidation 拓扑），不是 per-endpoint KV 大小**。同 prefix 散到 **4 个** engine cache 才会碎片化到让 affinity 路由回收重算有意义；散到 2 个不够碎，每端 APC 仍能看到足够重复 prefix。

### 不能声称
- **4-ep regime（SLO 违约 25–31%）的抖动深度比 2-ep 最高（14%）更深**，本扫描没能把"endpoint 数"和"抖动深度"完全分离——2-ep 即使把 util 压到 0.3（vLLM 下限附近）也只到 14% SLO 违约，到不了 4-ep 的 28%。matched-KV 对比强烈指向 endpoint 数，但严格说仍混淆了"endpoint 数 × 更深抖动"。
- **util 0.9 只有 n=2**（第 3 rep 偶发失败）；Δ=+0.4% 在 2 rep 上仍稳（CV≤0.3%、两臂不重叠），但重复数少于其他点。
- per-arm APC 命中率未单独记录（与 0730/0731 prefix 实验同一缺口），吞吐方向性差异间接说明 cache 行为；本扫描在 2-ep 下臂间无方向性差异，与"中性"结论一致。

## 6. 对课题含义

1. **prefix_affinity（及跨引擎共享 KV）是"多 endpoint 现象"**：价值在 prefix 碎到**多个** engine cache 时（4-ep consolidation），不在单个 engine 的 KV 小。这**比"小 KV/高淘汰"假设更贴合 DB-AI 真实场景**——一台 DB 服务器上整合多个模型 endpoint 共享 GPU。
2. **sharpens 跨引擎 KV 方向的动机**：共享池的价值场景是"多 endpoint 整合"（现实部署形态），不是"单 endpoint 小 KV"。若推进 Mooncake/LMCache，评估 regime 应是 4-ep/多 endpoint consolidation，不是 2-ep/小 KV。
3. **2-ep 是干净基线**：per-endpoint KV 预算在 2-ep 下不触发 routing 收益 → RC1 数据组织策略重测（#21–24）用 2-ep 作干净基线是合理的（策略效应不被 routing/consolidation 混淆）；4-ep 作为"策略 × consolidation 交互"的 follow-up。

## 7. 下一步

1. **隔离 endpoint 数 vs 抖动深度**：在 4-ep 下扫 util（0.2/0.3/0.43）看 +5.9% 是否随 endpoint 数稳定 vs 随抖动深度变化；或 2-ep 下用更大 working set（如 4096 行）把抖动推到 28% 看是否仍中性。
2. **若推进跨引擎 KV 池**：评估 regime 锁定 4-ep/multi-endpoint consolidation（共享池跨 ≥3–4 engine 才有意义）；2-ep 场景预期无收益（本扫描已示）。
3. **per-arm APC 命中率**：runner resources 增采 `prefix_cache_hit_rate`，使未来 cache 相关实验能直接归因。
4. 方向决策（#20）：cross-engine KV 池是否符合课题上游调度主线——本结果提供"多 endpoint consolidation 是真场景"的实证支撑，待与导师确认是否纳入。
