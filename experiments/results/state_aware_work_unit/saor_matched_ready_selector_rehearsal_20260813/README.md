---
experiment_id: saor-matched-ready-selector-rehearsal-20260813
date: 2026-08-13
status: completed-development-rehearsal-diagnostic-after-fairness-gate-audit
evidence_level: two-independent-fixed-order-gpu-rehearsals
execution_commit: 2d3a49308702c3fcf3b2a75cf4319fc2ac2d9a9d
analysis_commit: 1af46401e873cec88119253a8bdd663da515a490
formal_repeats: 0
conclusion: observed-nondominated-tradeoff-not-selector-victory
---

# SAOR matched-ready selector 双轮归因 rehearsal

> 2026-08-14 证据门复核：本报告中的 GPU 性能、SLO、correctness 以及五个 bounded-ready 臂的
> lifecycle 数据不变；但旧汇总在 frozen-static completion fairness 为 `unavailable` 时仍允许
> cell 通过，因此旧的全矩阵 `validation.status=passed` 已撤销，当前身份为 diagnostic。
> frozen-static 只保留 performance/isolation 参照，不进入同口径 service-lag 排名；修复后的
> summarizer 会对此 fail closed。下文保留旧输出字段以便审计，不代表 formal gate 仍通过。

## 1. 实验目的

本实验回答一个比“SAOR 是否比 static 快”更窄、更严格的问题：当 Project FIFO、DRR、
external VTC-style、strict-priority 和 SAOR 都看见完全相同的 bounded concrete-ready 请求集合时，
SAOR 的 guarded priority/debt selector 是否仍有独立增量。

这属于 **Project 内部 selector attribution**，不是 Daft/Ray Data 原生系统比较。Daft Native、
Daft Ray 和 Ray Data native 均未参与，`native_baseline_count=0`。因此本实验不能回答完整 SAOR
系统是否优于无 SAOR、无 bounded-ready 的原生框架；该问题必须用下一阶段独立的 system-level
matched comparison 回答。

## 2. 实验设置

| 项目 | 冻结合同 |
|---|---|
| 硬件 | 2×RTX 4090 24,564 MiB；driver 595.58.03；32 个 Ray CPU slot |
| 软件 | PostgreSQL、Daft 0.7.21、Ray 2.56.1、vLLM 0.25.1 |
| 模型服务 | Qwen2.5-7B-Instruct；两个 endpoint；显式 FCFS；continuous batching、chunked prefill、prefix cache ON；`max_num_seqs=256`、`max_num_batched_tokens=8192` |
| 数据链路 | PostgreSQL → Daft native source → Ray actor → vLLM；writeback none |
| workload | 同一租户内 bulk 512 requests @0s + foreground 512 requests @5s；两个 immutable work-balanced manifest |
| 请求语义 | Chat Completions；output cap 256；temperature 0；token budget 6144；arrival scale 0.0001 |
| 项目包络 | 每 endpoint K128/W65536；8 actors/endpoint；actor concurrency 32；credit quantum 2048 |
| bounded-ready | 每 Job 的 request/work 上限由冻结 K/W 派生；另设 64 MiB/Job logical-payload safety cap |
| 重复 | 两个独立输出 root；每臂每轮一次 rehearsal；固定顺序；formal repeats=0 |

六臂身份必须显式写 observation contract：

| 臂 | 身份 | observation contract |
|---|---|---|
| project frozen-static | 同栈静态参照 | single-head、static partition |
| Project harness + bounded-ready FIFO | matched-observation control | bounded concrete pre-registration |
| Project harness + bounded-ready DRR | matched-observation control | bounded concrete pre-registration |
| Project harness + bounded-ready VTC-style | matched-observation control | bounded concrete pre-registration |
| Project harness + bounded-ready strict-priority | foreground SLO 上界 control | bounded concrete pre-registration |
| SAOR guarded-debt $H_B=0.125W_e$ | proposed candidate | bounded concrete pre-registration |

FIFO、DRR、VTC 是已有算法思想，但这里运行的是 Project shared-credit coordinator 中的本地实现，
不是 Daft/Ray Data/upstream vLLM 或 VTC artifact 的原生实现。它们是 standard-algorithm
controls；这些 bounded-ready 副本也不冒充 vendor-native baseline。

## 3. 严谨性与合规自检

| 门禁 | 结果 |
|---|---|
| runtime preflight | 在分析提交上重新执行，`status=ok`；2×4090、Python/Daft/Ray/text/analysis 依赖、路径与磁盘均通过；`nvcc` 仅为 optional missing |
| 代码验证 | 服务器在分析提交上运行 shared-vLLM + formal-tools 共 91 项测试，全部通过 |
| 两轮身份 | 相同 config fingerprint、执行 commit 和 service signature；两个 manifest 均 `completed`、0 incident |
| correctness | 12/12 cell；12,288/12,288 requests 完成；0 request failure、0 actor failure |
| observation | 五个 bounded-ready 臂两轮均 lifecycle complete、两个 Job 都有 ready interval、actor event join 通过 |
| proposed 机制 | 两轮均有 512 次 SLO-priority grant、9/12 次 debt-recovery grant；avoidable idle=0、foreign-over-critical=0、recovery in-flight max=1 |
| 资源指标 | 每个 cell 的 model/resource/MFU status 均为 `ok`；使用 during-run time-series 聚合，不使用单点 GPU snapshot |
| 稳定性 | 各臂 tokens/s 双轮 sample CV 为 0.08%–0.58% |
| 汇总结果 | 旧分析提交曾输出 `validation.status=passed`；2026-08-14 新门禁复核后降为 diagnostic，因为 frozen-static completion fairness evidence 不可用。`selector_victory_decided=false`、`formal_authorized=false` 不变 |

### 3.1 runner 有效性修复

最初两个诊断 root 在 static 完成后把 FIFO foreground 的 barrier→first-submit 延迟误判为
“replay start missed”。逐 Job barrier 事件证明两个 Job 都按时跨过 replay barrier，first submit
变晚是 FIFO selector 的真实排队结果。执行提交 `2d3a4930` 将有效性条件改为约束
`replay_observed_start_epoch_s`，并保留“submit 不得早于 barrier”的因果门。两个成功 root 均在
修复后从全新目录重跑；旧失败 root 不进入本报告。

### 3.2 仍然存在的证据限制

- 两轮都是固定臂顺序，而不是 balanced/interleaved formal；不能排除顺序或 warm-cache 漂移；
- 每臂只有两个 development 样本，不能计算可靠置信区间或完成 non-inferiority 推断；
- selector 级 protected margins 没有在看到这批 matched-ready 结果前精确冻结，不能事后挑阈值
  授权 formal；
- writeback=none，无 PostgreSQL sink、质量或价格结论；能耗字段未形成可排名证据；
- static 没有 complete registered-ready ledger，其 completion-accounted service lag 为
  `unavailable`；CSV 中的 0 是不可用占位，不表示 static 的 lag 为零。2026-08-14 起这不再只是
  结果限制，而是 cell-pass 的硬门：缺少该证据会使全矩阵 validation fail closed。

## 4. 指标定义

当前 workload 是 foreground/bulk **differentiated service**，不是 equal-share fairness 专场：

- foreground：request P99、30s request/token SLO goodput；
- bulk：JCT/P99、30s miss guard、最长无服务；
- efficiency：correct tokens/s、group JCT、MFU；
- service fairness：仅在 registered-ready backlog 完整时，从 request completion event 按 actual
  work 记账；每个 completion epoch 把完成 work 按当时 active Job 权重分配为 empirical ideal，
  $L_i(t)=S_i^{ideal}(t)-S_i^{actual}(t)$，报告正 lag P95/max；
- `completion_longest_no_service_s`：Job 有 registered-ready backlog 时，相邻 completion event
  之间的最长时间；
- Jain 使用每 Job `actual_work/JCT/weight`，只作描述，不是 VTC、GPS、DRF 或 SLO 公平证明。

completion-accounted 指标是**上游、完成粒度、经验性**指标。它不能观察 vLLM 内部逐 token
continuous batching 服务，也不是理论 service-lag 上界。

MFU 为 0–1 分数，代码口径是：

```text
vllm_estimated_flops_per_gpu_delta
------------------------------------------------
group duration × 165 TFLOPS × 10^12
```

它与 `gpu_utilization_pct_mean` 分列；后者接近 100% 不代表不同策略做了相同的有效模型工作。

## 5. 实验数据

### 5.1 全部单次核心值

| round | arm | tok/s | group JCT(s) | bulk JCT(s) | fg JCT(s) | bulk P99(s) | fg P99(s) | bulk miss | fg miss | MFU | lag P95(work) | longest no-service(s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | frozen-static | 9,452.2 | 92.09 | 90.47 | 36.32 | 83.59 | 29.10 | 0.678 | 0.000 | 0.356 | unavailable | unavailable |
| 1 | bounded-ready FIFO | 12,938.7 | 67.28 | 65.79 | 46.33 | 58.69 | 39.58 | 0.541 | 0.393 | 0.486 | 146,591 | 12.02 |
| 1 | bounded-ready DRR | 12,919.3 | 67.37 | 65.81 | 34.58 | 58.54 | 27.45 | 0.621 | 0.000 | 0.487 | 67,173 | 7.07 |
| 1 | bounded-ready VTC-style | 12,891.0 | 67.51 | 65.69 | 33.28 | 58.64 | 26.31 | 0.623 | 0.000 | 0.486 | 64,740 | 7.07 |
| 1 | bounded-ready strict-priority | 12,068.4 | 72.15 | 70.70 | 20.12 | 63.37 | 13.55 | 0.795 | 0.000 | 0.456 | 56,755 | 17.30 |
| 1 | SAOR guarded-debt | 12,272.9 | 70.89 | 68.99 | 24.33 | 61.40 | 17.74 | 0.656 | 0.000 | 0.463 | 57,714 | 8.67 |
| 2 | frozen-static | 9,529.7 | 91.33 | 89.65 | 35.71 | 82.83 | 29.03 | 0.668 | 0.000 | 0.359 | unavailable | unavailable |
| 2 | bounded-ready FIFO | 12,899.1 | 67.51 | 65.74 | 46.08 | 58.69 | 38.81 | 0.537 | 0.391 | 0.485 | 146,189 | 11.55 |
| 2 | bounded-ready DRR | 12,879.3 | 67.60 | 65.79 | 33.86 | 58.68 | 27.01 | 0.621 | 0.000 | 0.485 | 65,542 | 7.06 |
| 2 | bounded-ready VTC-style | 12,910.7 | 67.43 | 65.67 | 33.21 | 58.49 | 26.01 | 0.623 | 0.000 | 0.486 | 65,894 | 7.07 |
| 2 | bounded-ready strict-priority | 12,004.5 | 72.55 | 70.86 | 20.15 | 63.49 | 13.51 | 0.799 | 0.000 | 0.453 | 57,044 | 17.28 |
| 2 | SAOR guarded-debt | 12,287.0 | 70.81 | 69.13 | 24.64 | 61.63 | 17.95 | 0.656 | 0.000 | 0.464 | 57,674 | 8.68 |

### 5.2 双轮均值

| arm | tok/s | group JCT(s) | bulk JCT(s) | fg P99(s) | bulk miss | fg miss | MFU | lag P95(work) | no-service(s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| frozen-static | 9,491.0 | 91.71 | 90.06 | 29.06 | 0.673 | 0.000 | 0.358 | unavailable | unavailable |
| bounded-ready FIFO | **12,918.9** | **67.39** | 65.76 | 39.20 | **0.539** | 0.392 | 0.486 | 146,390 | 11.78 |
| bounded-ready DRR | 12,899.3 | 67.48 | 65.80 | 27.23 | 0.621 | 0.000 | 0.486 | 66,357 | **7.07** |
| bounded-ready VTC-style | 12,900.8 | 67.47 | **65.68** | 26.16 | 0.623 | 0.000 | **0.486** | 65,317 | **7.07** |
| bounded-ready strict-priority | 12,036.4 | 72.35 | 70.78 | **13.53** | 0.797 | 0.000 | 0.454 | **56,900** | 17.29 |
| SAOR guarded-debt | 12,279.9 | 70.85 | 69.06 | 17.85 | 0.656 | 0.000 | 0.463 | 57,694 | 8.67 |

### 5.3 模型服务与主机资源均值

| arm | vLLM E2E mean | queue mean | TTFT P95 | GPU util mean | running mean/max | waiting mean/max | KV mean/max | CPU busy P95 | host memory P95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| frozen-static | 6.67s | 0.00s | 0.81s | 98.17% | 73.2/174 | 0.00/1 | 0.269/0.423 | 17.5 cores | 13.5% |
| bounded-ready FIFO | 8.52s | 0.02s | 2.03s | 96.93% | 125.7/256 | 0.65/17 | 0.428/0.551 | 16.2 cores | 10.3% |
| bounded-ready DRR | 8.52s | 0.03s | 2.28s | 96.67% | 124.3/256 | 1.69/46 | 0.436/0.550 | 18.1 cores | 10.4% |
| bounded-ready VTC-style | 8.53s | 0.03s | 2.28s | 96.56% | 124.6/256 | 0.98/34 | 0.434/0.556 | 17.9 cores | 10.6% |
| bounded-ready strict-priority | 7.31s | 0.18s | 4.03s | 96.71% | 96.0/256 | 3.36/53 | 0.364/0.587 | 15.1 cores | 10.9% |
| SAOR guarded-debt | 7.60s | 0.09s | 2.90s | 96.94% | 104.6/255 | 1.91/31 | 0.388/0.574 | 16.7 cores | 11.1% |

所有臂 GPU utilization 都在约 96.6%–98.2%，但 MFU 从 0.358 到 0.486、tok/s 从 9.49K
到 12.92K。该数据再次证明只看 GPU utilization 不能判断是否完成了更多有效模型工作，也不能
作为动态调度的单一反馈信号。

### 5.4 ready-buffer 与机制证据

- bounded-ready 的 ready-work P95 约 129.9K–130.5K，logical payload bytes P95 约
  0.45–0.46 MiB；实际最大 logical bytes 490,163，仅占 64 MiB/Job safety cap 的约 0.73%，
  bytes cap 在本实验中不绑定；
- SAOR 两轮最大 ready set 都是 256 requests、约 131K predicted work；
- 两轮分别有 512/512 SLO-priority、9/12 debt-recovery、503/500 fallback grant；
- 两轮均为 constraint conflicts=2、recovery in-flight max=1、foreign fallback=0、avoidable
  idle=0、foreign grant over debt-critical=0；
- Jain 双轮均值：static 0.915、FIFO 0.778、DRR 0.851、VTC-style 0.858、strict 0.980、SAOR
  0.941。strict 的 Jain 最高但 bulk miss 最差，直接说明 Jain 不能单独判定 differentiated-service
  策略好坏。

## 6. 结果解释

### 6.1 事实

1. **SAOR 相对 frozen-static 在本次观测维度上形成经验性 Pareto 改善**：tok/s +29.39%、
   group JCT −22.74%、bulk JCT −23.32%、foreground P99 −38.59%，bulk miss 也没有恶化。
   这说明完整 Project shared/bounded-ready/guarded-release 路径具有继续做 system comparison 的价值。
2. **FIFO 只追求工作守恒不够**：吞吐最高，但 foreground P99 39.20s、SLO violation 39.16%。
3. **DRR/VTC-style 是决定性的简单 control**：约 12.90K tok/s，foreground 30s SLO violation
   都为 0，bulk JCT 约 65.7–65.8s，最长无服务约 7.07s。
4. **strict-priority 给出了前台上界和 bulk 代价**：foreground P99 13.53s，但 bulk miss
   79.69%、最长无服务 17.29s。
5. **SAOR 位于 VTC/DRR 与 strict-priority 之间**。相对 VTC-style，SAOR foreground P99
   −31.78%、lag P95 −11.67%，但 tok/s −4.81%、group JCT +5.01%、bulk JCT +5.15%、
   longest no-service +22.68%。相对 strict，SAOR tok/s +2.02%、bulk JCT −2.42%、bulk miss
   下降约 14.1 个百分点，但 foreground P99 +31.91%。

### 6.2 推断

SAOR 是本次双轮观测中的**非支配折中点**，但不是 selector victory。它确实用一部分效率和
bulk completion 速度换得更低的前台 tail 与 completion-accounted lag；然而 DRR/VTC-style 已经
满足预注册 30s foreground SLO，SAOR 把 P99 从约 26–27s 再压到 17.85s 并没有增加这个 30s
request SLO 下的 foreground goodput。

因此当前证据更支持：

> bounded concrete-ready exposure + shared admission 是主要系统增量；guarded debt 是否值得作为
> 独立算法贡献，取决于业务是否需要比 30s 更紧的 foreground tail，且愿意接受约 5% 的效率/
> bulk-JCT 代价。

### 6.3 不能声称

- 不能称 SAOR selector 已击败 DRR/VTC-style，也不能称 formal 已获授权；
- 不能把 SAOR vs static 的全部差值归因于 selector，因为 static/shared capacity、single-head/
  bounded-ready 和 selector 同时变化；
- 不能把 completion lag 写成 vLLM 内部逐 token 公平或理论上界；
- 不能称 Daft Native/Ray Data 已被比较或击败；
- 不能根据两轮固定顺序短测外推 4-Job、跨硬件、图像或多租户结论。

## 7. 对课题的含义与下一步

当前 matched-observation gate 已完成，但只达到
`observed-nondominated-tradeoff / formal-not-authorized`：不立即启动 selector 1+3 formal，也不
继续扫描 debt cap、dynamic K、reservation 或 4-Job 来追正。

本报告形成时冻结了两项后续工作；其中 observation bridge 现已完成：

1. **系统级 native matched comparison**：同一 2-Job immutable workload、0/5s arrival、PG
   source/sink、Qwen/vLLM FCFS 服务和物理资源下，分列 Daft Native、Daft Ray、Ray Data native、
   project frozen-static 与当前 SAOR。原生臂保留自己的 batching/backpressure/scheduler，禁止
   注入 K/W、credit 或 bounded-ready。该层只判断完整系统经验表现；
2. **observation bridge（已完成）**：双轮结果表明
   `frozen-static → single-head + shared FIFO` 使 tok/s +25.96% 但 foreground P99 +99.17%；
   `single-head + shared FIFO → bounded-ready + FIFO` 再使 tok/s +7.30%、foreground P99
   −33.62%，但 foreground SLO violation 仍约 39.7%。完整报告见
   `../saor_ready_observation_bridge_rehearsal_20260813/`。

历史原生 two-job 数据不能直接与本实验拼表：当前 SAOR 使用 PG→Daft source、manifest-selected
request、arrival replay 和新的 P99/SLO/lag schema；旧 native runner 在计时前读取 JSONL
manifest，并且 Daft `prompt()`/Ray Data official graph 当前只可靠暴露 shard/Job barrier，不能把
复制的 shard completion time冒充逐请求 P99。后续必须重跑 matched system comparison；原生
request P99 若官方路径不可观测，应明确记为 `unavailable`，用共同可观测的 group/Job JCT、
correct service throughput、MFU、服务压力和资源指标作跨系统主比较。

## 8. 原始材料与完整归档

仓库内 [`raw/`](raw/) 保存：两轮 manifest/group runs、新版 preflight、completion-accounted
ablation summary 与 validation。完整逐请求、submission、credit、release event 和 resource traces
保留在服务器仓库外归档：

```text
/root/autodl-tmp/experiment-artifacts/
  saor_matched_ready_selector_rehearsal_2d3a493_r3_r4_full.tar.gz
SHA256 d98f22689f4745fd7eb3b4557a17224234bdeb69b89a00d416b4abdd9576a14c
```

紧凑汇总由分析提交 `1af46401` 从执行提交 `2d3a4930` 的原始事件离线重算；它没有修改任何
GPU 结果或补造 static 的 service lag。2026-08-14 门禁复核保留该旧 validation 作为审计历史，
不再将其 `passed` 字段解释为当前合同下的通过证据。
