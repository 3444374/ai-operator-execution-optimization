---
experiment_id: saor-bounded-priority-gate-20260813
date: 2026-08-13
status: completed-development-gate-not-promoted
evidence_level: two-round-rehearsal-diagnostic
git_commit: 2de6f931ef50a19ffaf33661f66c5e91295e5ac5
formal_repeats: 0
conclusion: diagnostic_only
---

# SAOR v0.5.1 bounded-priority 双轮开发门禁

## 1. 实验目的

在不修改 vLLM、固定每 endpoint K128/W65536 的前提下，验证有界词典序
`saor_bounded_priority` 是否能同时达到：

1. foreground P99 不劣于 frozen static；
2. 保留 shared release 的吞吐；
3. bulk SLO 不越过预注册边界；
4. lossless event ledger 中真实出现 SLO priority 与有界 debt recovery。

本实验是两轮 `development rehearsal`，不是 1 warm-up + 3 formal。任一 cap 未在两轮中通过
全部门禁就不得注册 formal，也不得在线增加 cap。

## 2. 实验设置

| 项目 | 冻结合同 |
|---|---|
| 硬件/服务 | 2×RTX 4090；Qwen2.5-7B；vLLM 0.25.1；两个独立 endpoint |
| vLLM 调度 | 两个实际进程均显式 `--scheduling-policy fcfs`；continuous batching、chunked prefill、prefix cache ON |
| 数据链路 | PostgreSQL 18.4 / pgvector 0.8.5 → Daft → Ray actor → vLLM；writeback none |
| workload | long/bulk 512 + short/foreground 512；foreground offset 5s；固定 immutable manifests |
| 请求合同 | chat completions；output cap 256；token budget 6144；arrival scale 0.0001 |
| 上游包络 | 每 endpoint K128/W65536；8 actors/endpoint；actor concurrency 32；shared quantum 2048 |
| 四臂 | static partition、原 SAOR release、bounded 0.125K、bounded 0.25K |
| priority 合同 | foreground priority=1、30s SLO/window；bulk priority=0、actual-work fairness debt cap |
| 重复 | 两个全新 root，各 arm 最多一个 rehearsal warmup；formal repeats=0 |

runner 使用 `run_shared_vllm_experiment.py --rehearsal`，不是单 profiler scenario runner。完整命令
见唯一计划的 Task 8；本地只保留脱敏 compact evidence，完整 2.2 MiB 归档留服务器仓库外：

```text
/root/autodl-tmp/experiment-artifacts/
  saor_bounded_priority_gate_20260813_2de6f93_full.tar.gz
SHA256 be6ce0a3c81351276f8e603cbbe6100b9e8b72fbfe70e75c142ca3b0658a2bb4
```

## 3. 合规性自检

| 门禁 | 结果 |
|---|---|
| machine/runtime preflight | `status=ok`；2×4090、32 Ray CPU、依赖、磁盘和 PostgreSQL 可用 |
| static readiness | `profile=bounded_priority_development`，`status=passed`、0 error |
| 服务身份 | 两 endpoint 健康、实际 cmdline 显式 FCFS；启动前无旧 runner，running/waiting=0 |
| Round 1 | manifest `completed`，4/4 arm、0 incident，lease 释放 |
| Round 2 | 4 个 arm 都产生完整 request/trace，但 0.25K 的 debt-recovery=0，runner fail closed；manifest `failed`、1 unrecovered incident |
| correctness | 下表全部 arm 1024/1024 请求完成、0 failed、0 actor failure；metrics/resources 均 `ok` |
| feeding | GPU mean 95.84%–98.41%；vLLM running mean 73.57–114.42；不是欠供给假阴性 |
| 正式性 | **未通过**：Round 2 非 clean rehearsal，formal 未启动 |

跨轮汇总器按设计仍写出诊断表，然后以非零退出拒绝比较准入：
`validation.status=failed`、`conclusion=diagnostic_only`、两个 cap 的
`cap_passed_both_rounds=false`。

## 4. 实验设计与门禁

预注册门槛为 foreground P99≤30.7s、foreground SLO violation≤0.01、bulk SLO violation≤0.723、
tokens/s≥9,984。bounded 臂还必须满足：event sequence 无 gap/duplicate、SLO-priority grant≥1、
debt-recovery grant≥1、recovery in-flight≤1、avoidable idle=0、foreign grant over debt-critical=0。

static 与原 SAOR 是定位基线；只有两个 bounded cap 需要通过全部机制门。slowdown 只作诊断，
不作为事后新增 hard gate。

## 5. 实验数据

### 5.1 全部单次值

| round | arm | tok/s | bulk JCT(s) | fg JCT(s) | bulk P99(s) | fg P99(s) | bulk SLO viol. | fg SLO viol. | GPU mean | waiting mean | KV mean/max | Jain |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | static | 9,487.34 | 89.99 | 36.45 | 83.17 | **29.72** | 0.674 | **0.000** | 97.70% | 0.033 | 0.270/0.423 | **0.913** |
| 1 | SAOR release | 12,394.04 | 68.43 | 56.31 | 61.62 | 49.49 | 0.473 | 0.830 | 97.59% | 0.455 | 0.427/0.551 | 0.743 |
| 1 | bounded 0.125K | 12,204.43 | 68.91 | 63.23 | 62.41 | 56.47 | 0.459 | 0.945 | 95.84% | 1.168 | 0.408/0.548 | 0.720 |
| 1 | bounded 0.25K | 12,381.70 | 68.45 | 55.77 | 61.91 | 49.03 | 0.469 | 0.852 | 96.43% | 0.547 | 0.426/0.553 | 0.745 |
| 2 | static | 9,519.12 | 89.74 | 36.37 | 82.97 | **29.21** | 0.674 | **0.000** | 98.41% | 0.000 | 0.271/0.423 | **0.913** |
| 2 | SAOR release | 12,470.57 | 68.50 | 60.12 | 61.85 | 53.18 | 0.469 | 0.863 | 97.53% | 0.870 | 0.420/0.550 | 0.729 |
| 2 | bounded 0.125K | 12,368.28 | 68.99 | 63.19 | 62.45 | 56.29 | 0.457 | 0.926 | 97.63% | 1.271 | 0.418/0.551 | 0.721 |
| 2 | bounded 0.25K | 12,322.93 | 68.60 | 56.94 | 61.85 | 50.10 | 0.463 | 0.875 | 97.38% | 0.545 | 0.424/0.554 | 0.741 |

`waiting` 与 KV 使用时序聚合，不使用单次 snapshot。KV 为 0–1 分数。

### 5.2 机制事件

| round | bounded arm | event sequence | SLO-priority grants | debt-recovery grants | conflicts | recovery max | avoidable idle | foreign grants | 机制门 |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 1 | 0.125K | complete | 512 | 3 | 3 | 1 | 0 | 0 | pass |
| 1 | 0.25K | complete | 512 | 1 | 0 | 1 | 0 | 0 | pass |
| 2 | 0.125K | complete | 512 | 1 | 0 | 1 | 0 | 0 | pass |
| 2 | 0.25K | complete | 512 | **0** | 0 | 0 | 0 | 0 | **fail** |

### 5.3 控制面—执行面交叉验证

逐请求 `arrival→submit` 分解显示，foreground 的 submit→service 只有约 2–8ms（P50–P99），
真正的尾部在 coordinator grant 之前：

| round | cap | fg arrival→submit P50(s) | P95(s) | P99(s) | 30s 内已 grant 的 fg 请求 |
|---:|---:|---:|---:|---:|---:|
| 1 | 0.125K | 39.82 | 49.19 | 49.54 | 53/512 |
| 1 | 0.25K | 31.04 | 38.53 | 39.18 | 145/512 |
| 2 | 0.125K | 40.44 | 49.21 | 49.66 | 70/512 |
| 2 | 0.25K | 32.08 | 39.49 | 40.16 | 122/512 |

event ledger 同时显示：每个可见 foreground head 都被选为 `slo_priority`，但 foreground 到达后
仍有 421 个 bulk grant。它们不是越过一个已注册 foreground head：当前 scheduler 每个 Job 的
同步 pull loop 一次只向 coordinator 注册一个待选请求；foreground request 获 grant 后，下一条尚未
注册的短空隙中 ready set 只剩 bulk，selector 正确回退 SAOR。因此实现观察到的 ready set 并不等于
数学模型假设的完整 Daft/Ray ready backlog。

## 6. 结果解释

| 类型 | 判断 |
|---|---|
| 事实 | static 两轮都通过 foreground 门，但吞吐未过 9,984 floor；三个 shared/priority 臂都过吞吐门，却全部远未过 foreground P99/SLO 门。没有任何 bounded cap 两轮全过。 |
| 事实 | 0.25K 的 debt recovery 是低频边界事件（1/0 次），不具稳定机制可达性；0.125K 为 3/1 次，但 foreground 反而更差。 |
| 推断 | 失败的首要原因不是 vLLM waiting、GPU 欠供给或 selector 没认出 priority，而是“完整 ready backlog”的建模假设与“每 Job 单 head 同步轮询”的运行时可见性不一致。 |
| 推断 | 0.125K 更小 debt cap 使 bulk recovery 更早介入，不能修复 ready-set 空隙，反而增加 priority/fairness 冲突。 |
| 不能声称 | 两轮 rehearsal 不能构成正式性能排名；不能称 bounded priority、reservation 或动态 K 有效；也不能因失败就外推所有外部 release 调度无效。 |

## 7. 对课题的含义与下一步

v0.5.1 不晋级 formal，停止该模板上的 cap 密扫，也不扩 4-Job。下一步不是先加 reservation，
而是修正 Daft/Ray 与 coordinator 的 observation contract：

1. 将 Job 已到达且完成数据准备的 bounded ready set 批量/异步注册到 coordinator，而不是每 Job 只暴露一个 pull head；或维持 per-Job `ready_count/ready_work + unfinished-priority epoch`，使 priority 状态不因单个 acquire 返回而消失。
2. 数学模型显式区分 source backlog、ready backlog、coordinator waiting、granted active 与 vLLM running/waiting，并把“仅对已注册 ready head 的工作守恒”改为端到端 ready-set 可见性条件。
3. 先以同一四臂、同 cap 跑机制 gate：foreground 到达后存在 ready work 时不得出现 bulk fallback；再做两轮 rehearsal。只有新 observation contract 过门后，才重新讨论 bounded debt cap、reservation/预测上界鲁棒性消融和 formal。

该结论也回答公平性评价：Jain 只能描述归一化进度，不能替代 class-specific P99/SLO、bulk lag/
SLO、最大 service disparity、饥饿、工作守恒与 correctness。当前 bounded 臂 Jain 约 0.72–0.75，
且 foreground SLO violation 85%–95%，所以不能称公平改善。

## 2026-08-14 fail-closed 门禁回归

formal evidence 修复提交 `15201946` 在同机、同模板上重新运行一次四臂 development rehearsal。
四臂均完成 512+512 请求且 exactly-once；八个实际 Job ID 全部非空，每组两个 Job ID 唯一；
GPU mean 为 96.84%–98.29%，MFU status 全部为 `ok`，两个 endpoint 运行后均 drain 到
running=waiting=0。

| arm | tok/s | duration(s) | bulk/fg JCT(s) | bulk/fg P99(s) | bulk/fg SLO viol. | MFU | debt recovery | avoidable idle | runner 判定 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| static | 9,536.06 | 91.27 | 89.56 / 36.36 | 82.70 / 29.21 | 0.674 / 0.000 | 0.359 | 0 | 0 | pass |
| SAOR release | 12,317.18 | 70.62 | 68.83 / 53.53 | 62.43 / 46.88 | 0.484 / 0.750 | 0.464 | 0 | 0 | pass |
| bounded $0.125W_e$ | 12,358.43 | 70.40 | 68.62 / 55.88 | 62.19 / 49.22 | 0.473 / 0.805 | 0.466 | 1 | 0 | pass |
| bounded $0.25W_e$ | 12,384.93 | 70.27 | 68.68 / 54.86 | 62.28 / 47.86 | 0.488 / 0.768 | 0.467 | **0** | 0 | **fail closed** |

最后一臂没有产生预期的 debt-recovery grant，runner 因而写入 1 个 unrecovered mechanism
incident，并把整个 manifest 标为 `failed`。这与 2026-08-13 Round 2 的失败形态一致，说明新
Job-ID 与 evidence 修复没有误杀前三臂，也没有把既有 $0.25W_e$ 机制失败“修成通过”。历史
scenario ID 中的 `0125k/025k` 实际乘的是 endpoint work limit。本轮只用于代码/门禁回归，
不增加性能重复数、不改变 $0.125W_e/0.25W_e$ 的既有结论。

服务器 diagnostic root：
`/root/autodl-tmp/experiment-artifacts/saor_bounded_priority_rehearsal_15201946_regression_20260814/`。
该目录保存 manifest、四份 group record、逐 Job request/submission/runs CSV、resource/credit/
release-event trace；不与原 2026-08-13 两轮数据合并。

## 原始材料

仓库内 compact evidence 位于 `raw/`：preflight/readiness、两轮 manifest/group runs、8 份 group
record、fail-closed gate/mechanism summary、validation、artifact provenance 与
`ready_set_gap_summary.csv`。逐请求/submission/resource/event 全量原始证据留在上述服务器归档；
本报告的交叉验证数字由该归档复算。
