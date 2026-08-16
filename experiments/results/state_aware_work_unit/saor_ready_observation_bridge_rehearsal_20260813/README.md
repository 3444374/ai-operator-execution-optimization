---
experiment_id: saor-ready-observation-bridge-rehearsal-20260813
date: 2026-08-13
status: completed-development-rehearsal-formal-not-authorized
evidence_level: two-independent-fixed-order-gpu-rehearsals
execution_commit: 3cfc5eb24b985e9e86cc8e79f7e5167ce283ed34
analysis_commit: 77b12d45f8df2ff9120960e406f07ee27661c65c
formal_repeats: 0
conclusion: shared-capacity-and-ready-observation-effects-separated
---

# SAOR ready-observation bridge 双轮 rehearsal

## 1. 实验目的

此前 frozen-static 与 bounded-ready FIFO 的差值同时包含 static partition→shared capacity 和
single-head→bounded-ready 两项变化，不能把全部收益归因于 bounded-ready。本实验固定相同
workload、K/W、FIFO、vLLM FCFS 与物理资源，用三臂逐段拆分：

```text
project frozen-static + single-head
    → project shared FIFO + single-head       （shared-capacity effect）
    → project shared FIFO + bounded-ready     （ready-observation/path effect）
```

三臂都是 Project 内部 reference/control，`native_baseline_count=0`；Daft Native、Daft Ray 和
Ray Data native 未参与。

实现所有权如下，避免把“已有算法”误写成“调用原生实现”：

| 名称 | 算法来源 | 本实验的实际实现 owner | 下游模型服务 |
|---|---|---|---|
| frozen-static | 项目冻结静态参照 | Project scheduler/static partition | upstream vLLM FCFS + continuous batching |
| shared FIFO | 经典 FIFO | Project `shared_credit.py` coordinator | upstream vLLM FCFS + continuous batching |
| bounded-ready FIFO | 经典 FIFO + 项目 observation | Project `shared_credit.py` + `BoundedReadyWindow` | upstream vLLM FCFS + continuous batching |

因此本报告中的 “FIFO” 不表示 Daft、Ray Data 或 vLLM 内置了这条多 Job FIFO 路径。

## 2. 实验设置

| 项目 | 冻结合同 |
|---|---|
| 硬件 | 2×RTX 4090 24,564 MiB；driver 595.58.03；32 Ray CPU slot |
| 软件 | PostgreSQL、Daft 0.7.21、Ray 2.56.1、vLLM 0.25.1 |
| 模型服务 | Qwen2.5-7B-Instruct；2 endpoint；FCFS、continuous batching、chunked prefill、prefix cache ON；max seqs 256、batched tokens 8192 |
| 链路 | PostgreSQL → Daft native source → Ray actor → vLLM；writeback none |
| workload | bulk 512 requests @0s + foreground 512 requests @5s；相同 immutable manifests |
| 请求 | Chat Completions；output cap 256；temperature 0；token budget 6144；arrival scale 0.0001 |
| Project envelope | 每 endpoint K128/W65536；8 actors/endpoint；32 slots/actor；quantum 2048 |
| bounded-ready | request/work 上限由 K/W 派生；logical payload cap 64 MiB/Job |
| 重复 | 两个独立 root；每臂每轮一次 rehearsal；固定顺序；formal=0 |

## 3. 严谨性自检

| 门禁 | 结果 |
|---|---|
| runtime preflight | `status=ok`；2×4090 与 core/text/analysis 能力通过；nvcc 仅 optional missing |
| readiness | `profile=ready_observation_bridge`、3 scenarios、0 error |
| 服务器测试 | shared-vLLM/formal-tools/config 共 101 项通过 |
| 两轮身份 | execution commit、config fingerprint、service signature 相同；两个 root 均 completed/rehearsal/0 incident |
| correctness | 6/6 cell；6,144/6,144 requests；0 request/actor failure；输入 prompt tokens 每臂每轮均 636,378 |
| observation | bounded-ready 两轮 actor event join、两个 Job ready lifecycle、ready intervals 均完整 |
| 模型/资源 | 6/6 cell metrics/resource/MFU status 均 ok；使用 during-run 聚合值 |
| 汇总 | `status=passed`；同时固定两个 effect `decided=false`、`formal_authorized=false` |

两轮都是固定顺序且每臂 n=2，适合判断方向是否复现，不适合置信区间、显著性或 formal 排名。

## 4. 指标与公平口径

当前是同一租户的 bulk/foreground differentiated-service 场景：

- efficiency：correct tokens/s、group JCT、MFU；
- foreground：request P99 与 30s SLO violation/goodput；
- bulk：JCT/P99/SLO miss；
- Jain 只作描述，不能单独代表 differentiated-service 好坏；
- registered-ready completion lag 只对 bounded-ready FIFO 可用。static 和 single-head 没有完整
  registered-ready ledger，必须记 `unavailable`，不能把 CSV 的 0 占位当零 lag。

MFU 与 GPU utilization 分列。GPU util 三臂都在约 96.8%–98.3%，但 MFU 与 tokens/s 明显不同，
继续证明 GPU utilization 不能作为唯一调度信号。

## 5. 实验数据

### 5.1 双轮均值

| arm | tok/s | group JCT(s) | bulk JCT(s) | fg JCT(s) | bulk P99(s) | fg P99(s) | bulk miss | fg miss | MFU | Jain |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| frozen-static / single-head | 9,547.6 | 91.15 | 89.45 | **36.25** | 82.63 | **29.48** | 0.674 | **0.000** | 0.360 | **0.912** |
| shared FIFO / single-head | 12,026.4 | 72.39 | **62.32** | 65.60 | **55.91** | 58.71 | **0.463** | 0.959 | 0.455 | 0.695 |
| shared FIFO / bounded-ready | **12,903.4** | **67.44** | 65.81 | 45.60 | 58.69 | 38.97 | 0.535 | 0.397 | **0.484** | 0.781 |

### 5.2 两段 effect（逐轮相对百分比）

| effect | round | tok/s | group JCT | bulk JCT | fg P99 | fg SLO violation | MFU |
|---|---:|---:|---:|---:|---:|---:|---:|
| static→single-head shared FIFO | 1 | +25.24% | −20.12% | −30.49% | +99.12% | +96.88 pp | +25.72% |
| static→single-head shared FIFO | 2 | +26.69% | −21.03% | −30.15% | +99.21% | +94.92 pp | +27.18% |
| single-head→bounded-ready FIFO | 1 | +7.87% | −7.31% | +5.83% | −33.65% | −56.84 pp | +6.97% |
| single-head→bounded-ready FIFO | 2 | +6.72% | −6.36% | +5.36% | −33.58% | −55.47 pp | +5.89% |

两轮 effect 简单均值：

- **共享容量**：tok/s +25.96%、group JCT −20.58%、bulk JCT −30.32%，但 fg P99 +99.17%、
  fg violation +95.90 pp；
- **bounded-ready exposure/path**：在相同 shared FIFO 下 tok/s +7.30%、group JCT −6.83%、
  fg P99 −33.62%、fg violation −56.15 pp，但 bulk JCT +5.59%。

### 5.3 服务与资源均值

| arm | GPU util | MFU | running mean/max | waiting mean/max | KV mean/max | TTFT P95 | CPU busy P95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| frozen-static | 98.30% | 0.360 | 73.8/173 | 0.00/0.5 | 0.270/0.421 | 0.79s | 20.0 cores |
| single-head shared FIFO | 97.67% | 0.455 | 99.8/252.5 | 0.91/21 | 0.409/0.553 | 2.18s | 25.4 cores |
| bounded-ready FIFO | 96.75% | 0.484 | 124.5/254.5 | 0.93/26.5 | 0.430/0.553 | 2.12s | 24.1 cores |

bounded-ready FIFO 的 ready requests/work/logical bytes 最大值均值为 256/131,064/491,032 bytes；
64 MiB/Job bytes cap 不绑定。completion-accounted lag P95 为 146,672 work，最长无服务 11.65s。

## 6. 结果解释

### 6.1 事实

1. static partition 保护 foreground，但保留的空闲份额不能被 bulk 充分借用，总吞吐/MFU最低。
2. single-head shared FIFO 释放共享容量后效率显著提高，但 foreground 被持续 bulk head 压住，
   两轮 P99 都约 58.7s、SLO violation 94.9%–96.9%。工作守恒不等于服务可接受。
3. FIFO 不变、只改 bounded-ready observation/execution path 后，吞吐继续提高且 foreground P99
   两轮都下降约 33.6%；这不是随机单轮方向。
4. bounded-ready FIFO 仍有约 39s foreground P99 和约 40% SLO violation，因此 observation 本身
   不足以满足 30s SLO。matched-ready 结果中 DRR/VTC-style/SAOR 的 fg SLO=0，说明 selector/
   service differentiation 仍是必要组成。

### 6.2 对 SAOR 与动态调度的修正

本实验给出了比“动态 K”更直接的动态调度场景：固定总 K/W 下，static reservation 保护前台却
浪费可借容量；无保护 shared FIFO 提高效率却破坏前台；系统需要根据 active Job/ready work/SLO
在二者之间动态借用、回收和选择 release order。

因此：

> 当前动态性的主要证据是 Job 级共享份额与 release order，而不是在线调 K。K/W 继续冻结在
> 机器签名对应的最小饱和点；只有未来出现不同 workload/static oracle 分离且反馈策略超过最强
> 静态上限，才重新启用 dynamic-K。

### 6.3 不能声称

- 不能称 bounded-ready 单独解决了 foreground SLO；
- 不能称 SAOR selector 胜过 DRR/VTC-style，或 formal 已授权；
- 不能称 Daft/Ray native 已被比较或击败；
- 不能把 completion lag 外推为 vLLM 内部 token-level fairness/theorem；
- 不能从固定顺序 n=2 外推跨硬件、4-Job、多租户或图像。

## 7. 对课题的含义与下一步

observation bridge 已完成，不再是下一步缺口。当前证据链变为：

1. shared capacity 解释效率提升与隔离损失；
2. bounded-ready 解释额外效率和部分 foreground 恢复；
3. matched-ready selector 解释 DRR/VTC/SAOR 如何进一步实现服务区分；
4. 尚缺系统级原生 matched comparison，才能判断完整 Project/SAOR 相对无 SAOR、无
   bounded-ready 的 Daft Native/Daft Ray/Ray Data native 的经验价值。

下一步不跑 selector formal、不扫 K/cap/reservation/4-Job。先统一 native 与 Project 的 PostgreSQL
source/sink、immutable workload、arrival、模型/vLLM、correctness 和共同可观测指标。native 若无
官方 per-request timestamps，request P99/SLO 必须记 unavailable，用 group/Job JCT、correct
service throughput、MFU、服务压力和资源作跨系统比较。

## 8. 原始材料与归档

仓库内 [`raw/`](raw/) 保存两轮 manifest/group evidence、bridge metrics/effects、preflight、
readiness 和 validation。完整 request/submission/resource/credit/event traces 保存在服务器仓库外：

```text
/root/autodl-tmp/experiment-artifacts/
  saor_ready_observation_bridge_rehearsal_3cfc5eb2_r1_r2_full.tar.gz
SHA256 452698ca794eb131ce53562e48c633aff9d63432884a2fe2919c78afb36f0436
```

GPU 执行绑定 `3cfc5eb2`；字段可用性修正与双轮离线汇总绑定 `77b12d45`，未改运行数据。
