# KV-budget × prefix_affinity routing 扫描实验（2-ep/1.5B）

> 存储约定：`experiments/results/<方向>/<exp>_<date>/{README.md, raw/}`。本 README 是全数据范本（所有组件的指标 + 说明）；流程固化在 `AGENTS.md`。

## 1. 实验目的

回答一个**隔离归因**问题：4-endpoint/Qwen2.5-1.5B 实验里 prefix_affinity 相对 least_queued 的 **+5.9%**（`../prefix_cache_routing_4ep_1.5b_20260731/`），驱动因素到底是**每 endpoint KV 预算（淘汰压力）**，还是 **endpoint 数（consolidation 拓扑）**？

方法：固定 2 endpoint（1/GPU），只扫 `gpu_mem_util ∈ {0.3, 0.45, 0.6, 0.9}`（每 endpoint KV 从小到大），每点测 `least_queued` vs `prefix_affinity`。如果驱动是 KV 预算，2-ep 在小 KV 下应复现 +5.9%；如果 2-ep 全程中性，则驱动是 endpoint 数。

关系到跨引擎共享 KV（Mooncake/LMCache）方向的动机定位。

## 2. 实验设置

- **平台**：AutoDL 2×4090，vLLM 0.25.1，PostgreSQL 18.4 + pgvector。
- **拓扑**：2 endpoint Qwen2.5-1.5B-Instruct，**每 GPU 1 endpoint**，`gpu_mem_util` ∈ {0.3, 0.45, 0.6, 0.9}（控制 per-endpoint KV 池大小）。端口 8000/8001，prefix-caching ON。
- **workload**：`sharegpt_multiturn`（2,048 行，doc_id 300000–302047，prompt_tokens 3–1486，target_output 1–256）。manifest `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。
- **调度合同**：Completions / httpx_async / return-token-ids / `--endpoint-gpu-ids 0,1` / K256 inflight / W65536 active-work / token_budget=8192 / **request 粒度** / fixed-50ms flush / prefix-cache ON / seed 20260729。
- **重复**：1 warmup + 3 formal × 2 场景（util 0.9 第 3 rep 偶发失败 → n=2，其余 n=3）。
- **原始数据**：`raw/`（4 子目录：`prefix_routing_2ep_1.5b_util{0.3,0.45,0.6}_20260731` + `prefix_cache_routing_2ep_1.5b_20260731`[=0.9 ablation]；每点 runs.csv[262 cols] + manifest + per-run requests/submissions/resources CSV）。

## 合规性自检

- **喂饱 vLLM / GPU：是**。`gpu_utilization_pct_mean` = **88–91%**（max 100%，`below_10pct_ratio` ~10%——仅启动/排空瞬间空闲）；`vllm_num_requests_running` mean **~139 / max ~194**（持续高并发），`waiting` ≈ 0。→ GPU 持续满载、**无饥饿**。（注：runs.csv 里另有一个 `gpu_utilization_pct` 列 = 单次 snapshot，值为 0，是采样假象，**不用它**；用 `*_mean/p50/p95/max` 系列。）
- **E2E 效率**：`tokens_per_s`(E2E) ≈ `model_request_tokens_per_s` 的 **88–91%**（pipeline 开销 ~9–12%）。
- **正式 feeding-saturation 门禁（E2E ≥95% of 同协议 bounded）= 未算出**：本扫描**没跑 bounded HTTP 臂**，缺同 config 的 bounded 上限 → **合规缺口**。#22 RC1 baselines 补 bounded HTTP 后才可正式算。
- **`vllm_kv_cache_usage_perc` 是分数（0–1）非百分比**（vLLM HELP: "1 means 100 percent usage"）：实测 mean 0.06–0.29 / max 0.09–0.45 = **6–45% 用量**。working set 跨 util 稳定 ~1.2–1.4GB，**所有 util 点都放得下（peak ≤45%）→ 全程无持续 KV 淘汰**。（注：早先一度把 0.06 读成 0.06% 当"指标坏"，是 scale 读错，已纠正——指标正常。）
- **策略是否到极限 / 平台**：两臂同 config（仅 routing 不同）、同 ~139 running → matched-KV 对比有效。**但本扫描（2-ep）平台 = util 0.9（65k/0% SLO）；routing 实验里 4-ep/0.43 的 util 是按"2/GPU 共享"倒推、非扫出来的平台** → 见 §5 平台方法论 caveat。

## 3. 实验设计

固定 endpoint 数 = 2、workload、调度合同，**只变 `gpu_mem_util`**。每点 A/B：`prefix_affinity`（rendezvous hash by prefix_key 钉到一端）vs `least_queued`（散到两端的 baseline）。Δ = (affinity − least_queued) / least_queued。参照点：4-ep/1.5B/0.43 = **+5.9%**。

## 4. 实验数据（全组件，formal 中位数）

> 指标单位见各表头；`aff`/`lq` = affinity / least_queued 两臂。原始 262 列在 `raw/*/runs.csv`，time-series 在 `raw/*/0*_*.resources.csv`，per-request 在 `raw/*/0*_*.requests.csv`。

### 4.1 吞吐 + 端到端延迟

| util | arm | E2E tok/s | 模型侧 tok/s | operator tok/s | rows/s | E2E wall (s) | req p50 | req p95 | req p99 | SLO 违约 | goodput/s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.3 | aff | 48,277 | 54,221 | 54,189 | 59.7 | 34.3 | 19.0 | 32.8 | 33.5 | 14.9% | 50.8 |
| 0.3 | lq | 49,274 | 54,250 | 54,224 | 60.9 | 33.6 | 18.4 | 32.2 | 32.8 | 13.4% | 52.8 |
| 0.45 | aff | 49,676 | 54,264 | 54,235 | 61.5 | 33.3 | 18.1 | 31.9 | 32.6 | 12.5% | 53.8 |
| 0.45 | lq | 48,612 | 54,335 | 54,304 | 60.2 | 34.1 | 18.8 | 32.6 | 33.3 | 14.4% | 51.5 |
| 0.6 | aff | 48,592 | 54,653 | 54,623 | 60.1 | 34.1 | 18.7 | 32.6 | 33.3 | 14.2% | 51.6 |
| 0.6 | lq | 48,369 | 54,119 | 54,088 | 59.8 | 34.2 | 19.0 | 32.8 | 33.5 | 14.5% | 51.0 |
| 0.9 | aff | **57,044** | **64,804** | 64,759 | **70.6** | **29.1** | 16.0 | **27.5** | 28.2 | **0%** | **70.6** |
| 0.9 | lq | **56,992** | **64,565** | 64,522 | **70.5** | **29.1** | 16.1 | **27.6** | 28.3 | **0%** | **70.5** |

- **routing Δ（模型侧）**：0.3 −0.1% / 0.45 −0.1% / 0.6 +1.0% / 0.9 +0.4% → **全中性**。
- **routing Δ（E2E）**：0.3 −2.0% / 0.45 +2.2% / 0.6 +0.5% / 0.9 +0.1% → 更噪但仍全 <5% → **E2E 也中性**。

### 4.2 vLLM 模型服务

| util | arm | 模型侧 tok/s | running mean/max | waiting mean/max | KV usage mean/max⚠️ | vLLM e2e lat (s) | queue | inference | prefill | decode | gen tokens Δ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.3 | aff | 54,221 | 136/191 | 0/0 | 0.29/0.39⚠️ | 2.32 | 0.00 | 2.30 | 0.05 | 2.25 | 485,352 |
| 0.45 | aff | 54,264 | 141/192 | 0/0 | 0.15/0.19⚠️ | 2.33 | 0.00 | 2.32 | 0.05 | 2.26 | 484,150 |
| 0.6 | aff | 54,653 | 139/192 | 0/0 | 0.09/0.12⚠️ | 2.33 | 0.00 | 2.31 | 0.05 | 2.26 | 485,822 |
| 0.9 | aff | 64,804 | 136/191 | 0/0 | 0.06/0.08⚠️ | 1.95 | 0.00 | 1.94 | 0.03 | 1.91 | 484,882 |

（lq 臂与 aff 几乎相同，略。）**decode 主导**（~2.25s 占 vLLM e2e 2.3s 的 97%），prefill 0.05s，queue 0。util 0.9 的 decode 更快（1.91 vs 2.25s）→ 吞吐更高。KV usage 列是**分数（0.06–0.45 = 6–45%）**，working set ~1.4GB 跨 util 稳定、全程放得下 → **无 KV 淘汰**（util 越高池越大、用量 % 越低，但绝对 working set 不变）。

### 4.3 GPU + 能耗 + MFU

| util | arm | GPU util mean/max | <10% 比例 | 显存 used mean (MiB) | 显存 util% | 功耗 mean (W) | 能耗 (J) | J/1k tok | MFU |
|---|---|---|---|---|---|---|---|---|---|
| 0.3 | aff | 88.6/100 | 0.11 | 17,796 | 36.2% | 710 | 24,213 | 14.6 | 0.36 |
| 0.45 | aff | 91.1/100 | 0.09 | 25,188 | 51.3% | 732 | 24,390 | 14.7 | 0.36 |
| 0.6 | aff | 89.1/100 | 0.10 | 32,368 | 65.9% | 715 | 24,132 | 14.6 | 0.36 |
| 0.9 | aff | 88.0/100 | 0.11 | 46,798 | 95.3% | 690 | 19,846 | **12.0** | 0.26 |

- GPU util mean 88–91%（喂饱）；显存 util 36%→95% 随 util（vLLM 预留池）。
- util 0.9 能效最好（12.0 J/1k tok vs ~14.6）；**MFU 反而在 0.9 掉到 0.26**（vs 0.36 @ 0.3–0.6）——疑似 MFU 估计口径受显存占用影响，待查。

### 4.4 pipeline 阶段计时（秒）

| util | arm | db_fetch | source_fetch | organizer | submit | fanin | bounded_wait | actor_ready | model_req_wall | operator_wall | E2E wall |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.3 | aff | 2.30 | 2.30 | 0.05 | 3.78 | 0.38 | 23.7 | 3.91 | 30.5 | 30.5 | 34.3 |
| 0.9 | aff | 2.04 | 2.04 | 0.04 | 3.94 | 0.34 | 18.6 | 3.84 | 25.6 | 25.6 | 29.1 |

（0.45/0.6 与 0.3 接近；lq 与 aff 接近。）`bounded_wait`（~19–24s）= active-work 准入控制下的等待（**与模型服务并发、非相加**），是 wall 的主体；实际非模型开销小（db_fetch ~2s + submit ~3.8s + fanin ~0.4s + organizer ~0.05s）。

### 4.5 Ray/actor/调度 + 请求尾部

| util | arm | max_inflight seen/limit | actor slots/端 | packing util mean/p95 | prefix_group_ratio | batch_tokens p95 | batch_service p95 (s) | finish stop ratio |
|---|---|---|---|---|---|---|---|---|
| 0.3–0.9 | 全 | ~194 / 256 | 256 | 0.94 / 0.99 | 0.40 | ~1,500 | 2.2–2.7 | 0.20–0.21 |

所有 util 点稳定：inflight 顶到 ~194（未到 256 上限）、packing 接近满、prefix_group 0.40、**79% 请求撞 256-token output 上限**（stop ratio 0.21）。

## 5. 结果解释

### 事实
- **2 endpoint 下，prefix_affinity 全 KV 范围中性**（模型侧 Δ ∈ [−0.1%, +1.0%]，E2E Δ ∈ [−2.0%, +2.2%]，全 <5% 门禁）。
- util 0.3–0.6 吞吐 ~48–49k tok/s、SLO 违约 13–15%、req p95 ~32s；util 0.9 吞吐 ~57k、**0% SLO**、p95 ~27.5s。**util 越高（KV 池越大）越快**。
- decode 主导 vLLM 延迟（97%）；GPU 持续 88–91% 利用。

### 推断（matched-KV 对比：endpoint 数效应作为 DATA 成立，但机制非 KV）
- **2-ep/0.45（~7GB KV）= −0.1%** vs **4-ep/0.43（~7GB KV/端）= +5.9%**：per-endpoint KV 量级相当、只差 endpoint 数 → **endpoint 数（consolidation）效应作为 DATA 成立**。
- ⚠️ **但机制不是 KV**：本扫描证明 2-ep 全程无 KV 淘汰（usage 6–45%）；4-ep 每 endpoint KV 用量更低（~5%）、也无 KV 压力。→ +5.9% **不是"KV 碎片化"**，更可能是 endpoint 数驱动的**并行度 / 每 endpoint batch 容量 / 内存预算**差异（2-ep 的 util 效应——低 util → 有效 batch 小 → decode 慢 → SLO 违约——本身就像"内存给 batching 的预算"问题，不是 KV）。**需重新归因。**
- ⚠️ **平台方法论 caveat**：routing 实验没走"先扫平台再对比"。本扫描找到 **2-ep 平台 = util 0.9**（routing 在此中性）；**4-ep/0.43 不是扫出来的平台**（是 2/GPU 共享倒推的），所以 4-ep +5.9% 可能在非平台点测得，需 4-ep 自己的 util 扫描确认平台后再判 routing。

### 不能声称 / 待查
- **不能声称 4-ep +5.9% 是"KV 碎片化"驱动**——本扫描证明无 KV 压力，机制待重新归因（并行度 / batch 容量 / 内存预算）。
- **不能声称 routing 在 4-ep 平台上的真实收益**——4-ep 平台未扫（feasible util 上限 ~0.45，需确认 0.43 是否其平台）。
- 2-ep util 效应（0.3–0.6 慢）机制：最可能是**内存给 batching 的预算不足**（非 KV 淘销），待控制实验确认。
- util 0.9 n=2（第 3 rep 偶发失败）；MFU 在 0.9 反常下降待查。
- **正式 feeding-saturation 门禁未算**（缺 bounded 臂）。

## 6. 对课题含义

1. **endpoint 数（consolidation）效应作为 DATA 成立**（matched-KV：2-ep 中性、4-ep +5.9%），但**机制不是 KV**（全程无 KV 淘汰）——更可能是并行度 / batch 容量 / 内存预算，待重新归因。
2. **⚠️ "跨引擎共享 KV（Mooncake/LMCache）"方向的动机被削弱**：本 workload（multiturn、~1.4GB working set）下**根本没有 KV 淘汰** → 没有跨引擎复用空间。共享 KV 池有价值的前提（KV 淘汰压力）**在本 setup 不成立** → 该方向需在能产生真实 KV 压力的场景重评，或重新判断是否还值得做。
3. 2-ep（util 0.9 平台）是 RC1 数据组织策略重测的**干净基线**（策略效应不被 routing/consolidation/memory-constraint 混淆）。

## 7. 下一步

1. **重新归因 4-ep +5.9%（非 KV）**：4-ep 扫 util 找平台 + 测 routing，确认是不是 endpoint 数驱动的并行度 / batch 容量——优先于跨引擎 KV 池。
2. **重评跨引擎 KV 池方向**：本 workload 无 KV 淘汰 → 动机削弱；除非换能产生真实 KV 压力的场景（更大 working set / 更小池），否则不建议投入。
3. **#22 补 bounded HTTP baseline** → 正式算 feeding-saturation 门禁。
4. **平台方法论补进流程**：先扫参数找平台、再在平台上对比（加进 AGENTS §7.5.C）。
5. 小改进：`vllm_kv_cache_usage_perc` CSV 列名（装分数 0–1）可改 `_ratio` 或加文档，防再误读。
