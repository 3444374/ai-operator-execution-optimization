---
experiment_id: saor-bounded-ready-gate-20260813
date: 2026-08-13
status: completed-development-gate
evidence_level: two-independent-rehearsal-roots
git_commit: 6728c5691eb201941a778dd7eb0547531718deba
formal_repeats: 0
conclusion: formal_registration_candidate_0125k_only
review_status: matched_observation_attribution_gate_required_before_formal
---

# SAOR bounded-ready 双轮开发门禁

## 1. 实验目的

本实验验证前一轮定位的 observation gap 是否真的被修复：在不修改 vLLM、固定每 endpoint
K128/W65536 的前提下，将数据库/Daft 已经到达的具体 request 以有界 ready set 暴露给共享
coordinator，能否同时满足：

1. concrete-ready 生命周期与 coordinator 事件可以逐 request 连接；
2. foreground ready→grant 区间内不向 bulk 发 `saor_fallback`；
3. foreground P99/SLO、bulk SLO 和总吞吐同时通过冻结门槛；
4. 不扩大 K/W，继续由未修改 vLLM FCFS + continuous batching 执行模型侧调度。

这是两个独立输出目录的 `development rehearsal`，每臂每轮一次，`formal_repeats=0`。它只决定
是否注册 formal 候选，不是正式性能排名或理论公平证明。

## 2. 实验设置

| 项目 | 冻结合同 |
|---|---|
| 硬件 | 2×RTX 4090 24,564 MiB；driver 595.58.03；32 个 Ray CPU slot |
| 软件 | PostgreSQL 18.4、pgvector 0.8.5、Ray 2.56.1、Daft 0.7.21、vLLM 0.25.1 |
| 模型服务 | Qwen2.5-7B-Instruct；两个独立 endpoint；显式 FCFS；continuous batching、chunked prefill、prefix cache ON；`max_num_batched_tokens=8192`、`max_num_seqs=256`、GPU memory utilization 0.9 |
| 数据链路 | PostgreSQL → Daft native → Ray actor → vLLM；writeback none |
| workload | long/bulk 512 + short/foreground 512；foreground offset 5s；相同 immutable manifests |
| 请求 | Chat Completions、output cap 256、token budget 6144、fixed-50ms flush、arrival scale 0.0001 |
| 上游安全包络 | 每 endpoint K128/W65536；8 actors/endpoint；actor concurrency 32；shared quantum 2048 |
| 四臂 | static partition、原 `saor_release`、bounded-ready $0.125W_e$、bounded-ready $0.25W_e$ |
| priority/guard | foreground priority=1、30s SLO/window；bulk priority=0；actual-work debt cap 为 $0.125W_e$ 或 $0.25W_e$ |

$W_e$ 是单 endpoint 的 work-credit 上限 65,536，因此两点实际为 8,192/16,384 work units。
历史配置和 scenario label 中的 `0.125K/0.25K` 只是旧显示名；代码执行的是
`fraction × endpoint work_limit`，不是 `fraction × request K`。正式材料统一使用
$H_B/W_e\in\{0.125,0.25\}$，避免把 request K 与 work W 混为同一资源。

运行前依次执行 runtime preflight、静态 readiness，然后从两个全新 root 启动相同四臂：

```bash
PYTHONPATH=code "$DRIVER_PYTHON" \
  code/scripts/analysis/audit_saor_formal_readiness.py \
  --profile bounded_ready_development \
  --config deploy/autodl/saor_bounded_ready.example.json \
  --output "$ARTIFACT_ROOT/saor_bounded_ready_readiness.json"

PYTHONPATH=code "$DRIVER_PYTHON" \
  code/scripts/experiments/run_shared_vllm_experiment.py \
  --rehearsal \
  --config deploy/autodl/saor_bounded_ready.example.json \
  --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
  --python-executable "$DRIVER_PYTHON" \
  --output-dir "$ARTIFACT_ROOT/saor_bounded_ready_rehearsal_<unique-id>" \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --ray-address "$RAY_ADDRESS"

PYTHONPATH=code "$DRIVER_PYTHON" \
  code/scripts/analysis/summarize_saor_bounded_priority_gate.py \
  --profile bounded_ready \
  --matrix-root "$ROUND1_ROOT" \
  --matrix-root "$ROUND2_ROOT" \
  --output-dir "$SUMMARY_ROOT"
```

## 3. 严谨性与合规自检

| 门禁 | 结果 |
|---|---|
| machine/runtime preflight | `status=ok`；2×4090、依赖、模型/workload 资产、磁盘和 PostgreSQL 可用 |
| static readiness | `profile=bounded_ready_development`，`status=passed`，4 个冻结 scenario、0 error |
| 服务身份 | 两个实际 vLLM cmdline 均显式 FCFS；同一 Qwen2.5-7B 配置；Ray 单节点健康 |
| 独立性 | 两个全新 root；相同 commit/config fingerprint/service signature；每轮启动前无旧 runner |
| correctness | 8/8 cell、8,192/8,192 request 完成；0 failed、0 actor failure、0 incident；两个 manifest 均 `completed` |
| 指标 | 每个 cell 的 model/resource metrics 均 `ok`；使用 during-run mean/P95/max，不用单次 GPU snapshot |
| 负载有效性 | GPU mean 94.32%–98.26%，running mean 73.0–113.3；static 即使 waiting=0 仍为 98% GPU mean，再次说明 waiting=0 不是欠供给判据 |
| 稳定性 | 两轮 tokens/s sample CV：static 0.159%、SAOR 0.300%、$0.125W_e$ 0.066%、$0.25W_e$ 0.428% |
| 正式性 | **未运行 formal**：两个 root 都是 rehearsal，固定顺序且每臂每轮一次 |

### 首次失败与修复链

commit `aaa484f4` 的首次运行在第三臂完成两个 Job 的模型请求后，由 runner 以
`KeyError: submit_epoch_s` fail closed。真实 CSV 证明 submission trace 只拥有
ready/registered/granted，scheduler submit 时间属于 request trace。修复没有复制或伪造时间列，
而是在 commit `6728c569` 中按 `submission_id` 一对一连接两份 trace，并增加“submission 无
`submit_epoch_s`”的生产 schema 回归测试。失败 root 保留，未续跑、未进入下表。

## 4. 门禁定义

冻结 hard gates 为：foreground request P99≤30.7s、foreground SLO violation≤0.01、bulk SLO
violation≤0.723、总 tokens/s≥9,984。bounded-ready 臂还必须满足：

- release-event sequence 无空账本、gap 或 duplicate；
- `slo_priority` 和 `debt_recovery` grant 均至少一次；
- recovery in-flight≤1；avoidable idle=0；foreign debt-critical grant=0；
- submission concrete-ready lifecycle 完整；actor-side request join 成功；
- foreground 至少两个 concrete-ready request、ready work>0，且其 register→grant 区间内
  foreign `saor_fallback`=0。

slowdown 与 Jain 只作诊断，不事后加入 hard gate。Jain 使用每 Job
`actual_work / JCT / weight` 的 achieved service rate，再计算
$(\sum_i x_i)^2/(n\sum_i x_i^2)$；它不是 VTC service-lag 定理。

这里的效率和保护阈值是**动态候选晋级门**，不是要求 static baseline 自己“通过”的有效性门。
static 仍是隔离/Pareto 锚点，表中的 `fail: efficiency` 只表示它没有达到 proposed 的吞吐晋级
下限。bulk 的 30s violation 在 static 下已约为 0.67；在缺少外部业务合同证明 30s 是 bulk
绝对 SLO 前，它只称为相对 static 的 request-deadline miss guard，不称 bulk 已满足绝对 SLO。

## 5. 实验数据

### 5.1 全部单次核心结果

| round | arm | tok/s | bulk JCT(s) | fg JCT(s) | bulk P99(s) | fg P99(s) | bulk SLO viol. | fg SLO viol. | Jain | all gates |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | static | 9,523.61 | 89.63 | 36.09 | 82.74 | 29.35 | 0.670 | 0.000 | 0.914 | fail: efficiency |
| 1 | SAOR release | 12,338.83 | 68.99 | 62.96 | 62.38 | 56.24 | 0.461 | 0.916 | 0.722 | fail: foreground |
| 1 | bounded-ready $0.125W_e$ | **12,355.00** | 69.11 | **24.75** | 61.59 | **18.15** | **0.658** | **0.000** | 0.940 | **pass** |
| 1 | bounded-ready $0.25W_e$ | 12,141.26 | 70.05 | 24.64 | 62.69 | 17.96 | **0.752** | 0.000 | 0.944 | fail: bulk |
| 2 | static | 9,502.26 | 90.09 | 36.28 | 83.30 | 28.93 | 0.668 | 0.000 | 0.913 | fail: efficiency |
| 2 | SAOR release | 12,391.22 | 68.78 | 60.97 | 62.15 | 54.31 | 0.469 | 0.867 | 0.728 | fail: foreground |
| 2 | bounded-ready $0.125W_e$ | **12,366.59** | 68.80 | **24.35** | 61.24 | **17.58** | **0.666** | **0.000** | 0.942 | **pass** |
| 2 | bounded-ready $0.25W_e$ | 12,214.99 | 69.53 | 24.67 | 62.66 | 17.97 | **0.744** | 0.000 | 0.941 | fail: bulk |

两轮均值下，$0.125W_e$ 为 12,360.79 tok/s、foreground P99 17.87s、foreground SLO
violation 0、bulk SLO violation 0.662、Jain 0.941：

- 相对 static：吞吐 +29.94%，foreground P99 −38.68%，且 bulk SLO violation 没有恶化；
- 相对原 SAOR release：吞吐 −0.03%（近似持平），foreground P99 −67.67%，但 bulk SLO
  violation 从 0.465 增至 0.662，仍在预注册 0.723 上限内；
- $0.25W_e$ 虽保护 foreground，但 bulk SLO violation 两轮为 0.752/0.744，均越界，拒绝。

### 5.2 模型服务与资源时序

| round/arm | GPU mean | running mean/max | waiting mean/max | KV mean/max | MFU | vLLM TTFT P95/P99(s) | ITL P95/P99(s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1/static | 98.15% | 73.77/172 | 0.00/0 | 0.271/0.426 | 0.359 | 0.763/0.953 | 0.057/0.239 |
| 1/release | 97.11% | 111.46/209 | 1.32/20 | 0.415/0.550 | 0.467 | 2.231/2.498 | 0.052/0.319 |
| 1/$0.125W_e$ | 97.58% | 105.89/255 | 2.06/33 | 0.393/0.576 | 0.466 | 2.968/4.594 | 0.056/0.271 |
| 1/$0.25W_e$ | 94.74% | 104.46/256 | 1.57/21 | 0.415/0.585 | 0.458 | 2.420/4.390 | 0.052/0.304 |
| 2/static | 98.26% | 73.04/172 | 0.00/0 | 0.271/0.427 | 0.358 | 0.745/0.948 | 0.052/0.239 |
| 2/release | 97.84% | 113.29/240 | 1.15/36 | 0.418/0.550 | 0.468 | 2.234/2.498 | 0.071/0.285 |
| 2/$0.125W_e$ | 96.79% | 106.18/255 | 1.67/37 | 0.393/0.576 | 0.467 | 2.448/4.431 | 0.059/0.277 |
| 2/$0.25W_e$ | 94.32% | 104.72/256 | 1.59/21 | 0.417/0.585 | 0.461 | 2.480/4.478 | 0.057/0.291 |

KV 是 0–1 分数；MFU 也是分数。vLLM histogram TTFT/ITL 是模型服务总体分布，不能替代上表
按 Job 的外部 request E2E P99。$0.125W_e$ 的 vLLM TTFT P99 比 static 高，但 foreground 外部 P99
显著降低，说明这是不同层次、不同混合总体，不能互相替代。

### 5.3 bounded-ready 机制证据

| round | cap | event count | fg ready intervals | max ready requests/work | priority grants | debt recovery | fallback grants | conflicts | recovery max | foreign fallback | avoidable idle |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | $0.125W_e$ | 2,048 | 512 | 256 / 84,837 | 512 | 13 | 499 | 2 | 1 | 0 | 0 |
| 1 | $0.25W_e$ | 2,048 | 512 | 256 / 84,837 | 512 | 10 | 502 | 2 | 1 | 0 | 0 |
| 2 | $0.125W_e$ | 2,052 | 512 | 256 / 85,057 | 512 | 10 | 502 | 2 | 1 | 0 | 0 |
| 2 | $0.25W_e$ | 2,048 | 512 | 256 / 84,832 | 512 | 10 | 502 | 2 | 1 | 0 | 0 |

四个 bounded-ready cell 均为 `ok:actor_event_join`、lifecycle complete、event sequence complete。
这里的 fallback grant 是 foreground 区间之外的合法工作守恒回退；`foreign fallback=0` 表示
foreground concrete request 已经 registered 到 grant 的区间内没有向 bulk 误发 fallback。

### 5.4 本实验不适用或未进入门禁的指标

- writeback=none，因此无 pgvector 写回吞吐、检索质量或数据库 sink 结论；
- completion temperature=0、相同 immutable prompts/output cap，未做生成质量排名；
- token price 未配置，成本指标为 unavailable；开发门不据此作成本结论；
- full archive 保存 per-request/submission/credit/resource/release-event trace；Git 只保存 compact
  group/manifest/gate 证据，不用 compact 表补造 event-level theorem。

## 6. 结果解释

| 类型 | 判断 |
|---|---|
| 事实 | bounded ready set 已在真实 PostgreSQL/Daft/Ray/vLLM 链路中生效，不是只通过单元测试：每轮 foreground 512 个 request 均形成可连接生命周期，峰值 256 request/约 85K work。 |
| 事实 | $0.125W_e$ 两轮同时通过 correctness、机制、foreground、bulk 与效率门；汇总器返回 `status=passed`、`formal_registration_candidate`。 |
| 事实 | $0.25W_e$ 两轮都只因 bulk SLO 越界失败；不能在 formal 中继续携带这一臂作为候选。 |
| 推断 | 旧 SAOR 的主要问题包含 observation contract：暴露 concrete-ready set 后，$0.125W_e$ 组合在保持 release 吞吐的同时把 foreground P99 从约 55.3s 降到 17.9s。由于 observation path 与 selector 同时变化，当前不能把全部改善归因给 debt selector。 |
| 待确认 | $0.125W_e$ 的 actual-work debt recovery 是否提供了超出同 ready-window FIFO/DRR/VTC/strict-priority 的 bulk 保护；必须用 matched-observation 消融后判定。 |
| 不能声称 | 不能称 SAOR 已正式胜出、跨 workload/硬件泛化或已有公平定理；不能称所有动态 K 都必要；不能把 Jain≈0.94 写成 max-min/VTC 公平保证；不能称 reservation 有效。 |

## 7. 对课题的含义与下一步

这轮结果把 SAOR 状态从 `development-unrun` 推到
`development-gated/formal-registration-candidate`，并且只冻结 **bounded-ready $0.125W_e$**。它
给出了当前最清楚的动态调度需要场景：bulk 已经占用共享包络、5s 后短 foreground 到达；static
保护前台但损失约 30% 吞吐，原 SAOR 保吞吐却严重伤前台，bounded-ready $0.125W_e$ 在两轮短测中
同时保持吞吐、前台 SLO、bulk guard 和较高的 achieved-service Jain。

2026-08-13 post-hoc 方法审核将下一步进一步收紧：**候选参数冻结不等于立即启动 formal**。
当前 `saor_bounded_ready` 同时改变 ready-set observation/execution path 与 priority/debt selector，
因此先把 bounded-ready observation 从 selector 解耦，让 global FIFO、DRR/WFQ、external
VTC-style、strict-priority 和 proposed 使用相同 ready-window，做 1--2 轮最小归因 gate：

1. 若简单 matched-ready 策略已落在同一 throughput/foreground/bulk Pareto 前沿，不能把收益
   写成 SAOR selector；贡献应收敛为 bounded ready-state exposure + guarded release，或淘汰
   不必要的复杂选择器；
2. 只有 proposed 相对 matched-ready killer baselines 在至少一个预注册主指标上有增量、其余
   protected metrics 非劣，才启动 1 warm-up + 3 formal；
3. formal 使用 balanced/interleaved 顺序并明确 warm-cache steady state 或 cell reset 合同，保存
   per-class SLO goodput、ready/registered→grant→submit→completion 尾延迟、共同积压 empirical
   service lag、三个 JCT 反事实、最长 no-service、ready bytes/host memory/CPU 和能耗；
4. $0.25W_e$ 只保留为 rejected ablation；不继续扫 cap、dynamic K、reservation、4-Job 或图像
   泛化，直到归因 gate 与 2-Job formal 闭合。

公平结论另分两种模式：equal-share 模式用相同权重与 service lag 比较 DRR/VTC；当前
foreground/bulk 模式属于 differentiated service，目标是 foreground SLO isolation + bulk
bounded starvation/non-inferiority，不要求两类延迟相等。bulk 30s miss rate 在应用语义未确认前
只是保护 guard；主要 bulk 保护应包括 reserved-share JCT、最大正 lag 与最长无服务时间。

## 原始材料与完整归档

仓库内 compact evidence 位于 [`raw/`](raw/)：preflight、readiness、首次失败 manifest、两轮
manifest/group runs、gate/mechanism summary 与 validation。服务器仓库外完整归档包含所有
per-request/submission/credit/state/resource/release-event 文件：

```text
/root/autodl-tmp/experiment-artifacts/
  saor_bounded_ready_development_6728c56_20260813_full.tar.gz
SHA256 9beb9c94ad6e8e47a084a08fe4a08d0c088d8f24b7a2fd695ae613cad62799fb
```
