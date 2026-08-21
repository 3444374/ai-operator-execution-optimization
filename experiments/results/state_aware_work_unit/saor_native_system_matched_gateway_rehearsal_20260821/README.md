# SAOR 五臂共同观测 rehearsal

日期：2026-08-21

状态：`rehearsal-only / validation-passed / formal-not-run`

## 1. 实验目的

验证五条真实系统路径能否在不接管原生框架调度的前提下，以统一外部时钟比较 correct
throughput、Job/group JCT、request SLO、Job SLO、隔离和 actual-work fairness：

1. Daft Native；
2. Daft Ray；
3. Ray Data HTTP Processor；
4. Project frozen-static；
5. Project bounded-ready SAOR。

本次只有一个 warmup-only rehearsal cell/臂，用来验证可运行性、证据口径并观察量级，不是
三重复 formal 统计，也不签发 formal authorization。

## 2. 实验设置

- commit：`932710125bd2a57525072181a969d032763aa869`；
- 硬件：2× RTX 4090；模型：Qwen2.5-7B-Instruct；
- 服务：两个 vLLM 0.25.1 Chat endpoints，native FCFS，prefix cache ON；
- workload：Job0=long 512 行（bulk），Job1=short 512 行（foreground），release=`[0s,5s]`；
- source：相同 PostgreSQL snapshot/query/doc_id/prompt bytes；
- completion：`writeback=none`，T4 是 1,024 行完整正确结果在内存中可见；digest 与
  exactly-once 在计时边界外封存，不连接输出 sink；
- common gateway：四条 Job×endpoint loopback path，只做一次原样转发；无 queue、semaphore、
  retry、cache、route choice、payload rewrite；
- request SLO：30s；foreground Job JCT SLO：30s；Job0 不设 Job JCT SLO；
- fairness：只在两 Job 共同 gateway backlog 内，按 endpoint 返回的 actual prompt+output tokens
  计算等权 service share、Jain、completion-accounted lag 与最长无服务。

统一时钟：T0=Job 实际 release（PostgreSQL/source 和 child/Ray init 前），T1=首批验证数据进入
executor，T2=首请求到达 gateway，T3=末请求完成，T4=完整正确结果可见。Job JCT=T4−T0，
group JCT=max(T4)−min(T0)，correct throughput=actual completed tokens/group JCT。

## 3. 严谨性自检

- 环境 check、static config、installed/live vLLM identity、endpoint/PG/Ray/GPU clean、同协议
  bounded baseline、五臂 correctness smoke 均通过；
- 五个 rehearsal cells 全部 `passed`，每臂 1,024 requests、`retry_count=0`、body identity 通过、
  exactly-once 通过；
- gateway dispatch P99 为 0.116ms–75.413ms，最大 76.770ms；高并发 eager 臂开销更高，但最大值
  仍小于对应 group JCT 的 0.11%，证据中保留而不假定为零；
- root 29MiB、archive 6.5MiB；archive SHA-256=
  `90b47110044030c912f93ae54ecb9937554bdb8f81c6e0bcd00a449e059b28c5`；
- 独立 validator 重哈希 root/archive、matrix index、三份 config、native provenance 和全部 cell，
  `valid_rehearsal=true`；
- 首次 smoke 曾因新 `request_slo_violation_ratio` 未映射旧持久化字段而 fail-closed，修复后在新
  commit/new root 完成；失败 root 不进入本表。

## 4. 完整系统表现（单次 rehearsal 观察）

`Job0/Job1` 分别为 bulk/foreground。request miss 是各 Job 的 30s request SLO；FG Job SLO 是
30s 完整 Job JCT。

| arm | correct tok/s | group JCT (s) | Job0/Job1 JCT (s) | Job0/Job1 req P99 (s) | Job0/Job1 req miss | FG Job SLO | weighted Jain | Job0:Job1 work share | lag P95 (tokens) | longest no-service (s) |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|
| Daft Native | 13,285.95 | 65.52 | 60.78 / 60.52 | 57.12 / 57.19 | 60.35% / 100% | violated | 0.6203 | 89.12% : 10.88% | 296,201.9 | 38.17 |
| SAOR | 11,669.24 | 74.55 | 74.55 / 40.32 | 10.80 / 10.96 | 0% / 0% | violated | 0.9438 | 62.20% : 37.80% | 56,108.5 | 2.49 |
| Daft Ray | 11,617.79 | 74.89 | 70.20 / 69.89 | 55.08 / 55.75 | 59.77% / 100% | violated | 0.6468 | 86.95% : 13.05% | 299,521.6 | 36.37 |
| Project frozen-static | 8,907.45 | 97.71 | 97.71 / 41.03 | 9.10 / 8.83 | 0% / 0% | violated | 0.9582 | 60.45% : 39.55% | 39,383.9 | 2.13 |
| Ray Data HTTP | 4,759.75 | 182.91 | 182.91 / 175.11 | 5.34 / 5.37 | 0% / 0% | violated | 0.7270 | 80.64% : 19.36% | 219,847.5 | 3.87 |

阶段拆分如下。source=T1−T0，execution=T4−T1，service=T3−T2。

| arm | Job0 source / execution / service (s) | Job1 source / execution / service (s) |
|---|---:|---:|
| Daft Native | 0.40 / 60.38 / 57.21 | 0.31 / 60.21 / 57.21 |
| Daft Ray | 0.33 / 69.88 / 56.14 | 0.26 / 69.63 / 55.76 |
| Ray Data HTTP | 0.34 / 182.57 / 165.17 | 0.26 / 174.85 / 152.07 |
| Project frozen-static | 7.89 / 89.83 / 89.65 | 7.61 / 33.42 / 33.28 |
| SAOR | 7.60 / 66.95 / 66.62 | 7.57 / 32.76 / 32.11 |

## 5. SAOR 因果归因：Project static vs SAOR

两臂共享 PostgreSQL source、Daft/Ray executor、token budget 6,144、K=128/endpoint、W=65,536、
actor shape、模型和服务；只改变 static admission 与 bounded-ready SAOR。

| 指标 | frozen-static | SAOR | SAOR 相对变化 |
|---|---:|---:|---:|
| correct throughput (tok/s) | 8,907.45 | 11,669.24 | +31.01% |
| group / bulk JCT (s) | 97.71 | 74.55 | −23.70% |
| foreground JCT (s) | 41.03 | 40.32 | −1.72% |
| bulk / foreground req P99 (s) | 9.10 / 8.83 | 10.80 / 10.96 | +18.70% / +24.11% |
| weighted Jain | 0.9582 | 0.9438 | −1.50% |
| service lag P95 (tokens) | 39,383.9 | 56,108.5 | +42.47% |
| longest no-service (s) | 2.13 | 2.49 | +16.75% |
| energy (J / 1k observed tokens) | 87.55 | 65.40 | −25.30% |

SAOR 机制证据为 `actor_event_join` + `lossless_ledger`，0 avoidable-idle events、0 projected
overshoot-bound violations、98 recovery completions；118 constraint conflicts 是约束实际生效的
计数，不是请求失败。

## 6. 结果解释

### 事实

- Daft Native 的本次 correct throughput 最高、group JCT 最短，但两 Job 共同积压窗口的 Jain 最低，
  foreground 只获得 10.88% actual token work，最长无服务 38.17s，request SLO 最差。
- Ray Data 的单请求 P99 最短但完整系统 JCT 最长。其低 inflight 让 endpoint request latency 短，不能
  据此推断 source/feeding 完成快；这正是同时报告 T0–T4 与 T2–T3 的必要性。
- Project 两臂 request SLO 都通过，但 30s foreground Job JCT SLO 都失败；request-level SLO 与完整
  Job SLO 不能互相替代。
- 在本次单 run 中，SAOR 相对 frozen-static 显著提高吞吐、降低 group/bulk JCT 和单位 token 能耗，
  foreground JCT 只小幅改善；与此同时 request P99、Jain、service lag 和最长无服务均变差。这是明确的
  效率—尾延迟—公平权衡，不支持“SAOR 全面胜出”。

### 不能声称

- 这是单次 rehearsal，没有 repeat/CV，不能作为 formal 显著性结论或最终排名；
- 0s/5s 是 Job release，不保证所有框架在 5s 前已经产生 T2。本次四臂在 foreground release 前没有
  victim request completion，Daft Native 虽在 release 时已有 victim backlog，但也没有完整 pre/post
  P99 样本。因此 P99 inflation/recovery 是 `partial`，不能补造 full-solo slowdown；
- `longest no-service` 与 lag 是 gateway completion-accounted empirical 指标，不等价于 vLLM 内部每个
  token 的连续调度轨迹；
- Project 资源数字来自相同 profiler 的时序聚合；当前 compact index 对 native/project 的资源字段形状
  不完全相同，所以本报告只做 Project A/B 能耗归因，不给五臂能耗排名。

## 7. 对课题含义与下一步

共同 gateway 已闭合“五臂可比较 request tail/fairness”缺口，且没有把 Daft/Ray 改成 bounded-ready。
当前最重要现象不是单一 winner，而是：原生 eager 路径可获得更高完整系统吞吐，却可能产生很差的
Job service share；SAOR 相对同 executor 的 static 点提高效率，但本次没有改善公平性。

若要补“多 Job 分别被影响多久”的因果隔离结论，应另加一个预注册、固定 release offset 的
guaranteed-service-overlap panel，或为每臂跑 matched-solo controls；它与 0s/5s 完整系统主表分开，
不能根据各臂运行时动态改变 offset。Formal 继续锁定，需先独立审核本 rehearsal 与指标定义。

原始 compact evidence：

- `raw/rehearsal_compact_metrics.json`；
- `raw/rehearsal_validation.json`；
- 完整 29MiB root 与 6.5MiB archive 保留在服务器 Git 外 artifact root。
