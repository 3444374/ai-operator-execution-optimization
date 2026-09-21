# SAOR 五臂 native-system matched rehearsal

日期：2026-08-21
状态：`historical-pre-gateway / rehearsal-only / validation-passed / formal-not-run`

> 该 root 早于共同 observation-only gateway，只证明五臂可运行性与封存门；其中原生 request
> tail/fairness unavailable。它不能回答当前完整五臂指标问题，必须用新 T0--T4/gateway 合同重跑。

## 1. 实验目的

在启动正式重复前，验证 Daft Native、Daft Native/Ray、Ray Data native graph、Project
frozen-static 与 SAOR 能否在同一双 vLLM FCFS 服务、同一两 Job manifest 和 `job0@0s / job1@5s`
到达合同下完成五臂系统矩阵。排名边界为 PostgreSQL source→validated model completion；五臂均
`writeback=none`，不写 PostgreSQL completion sink。

本次只回答合同与可运行性问题，不回答哪一臂性能最好。

## 2. 实验设置

- 运行 commit：`862d0008b013278eb7d6dfa69aa40adae4c2d635`
- GPU/服务：2×RTX 4090，两个 Qwen2.5-7B vLLM 0.25.1 endpoint，native FCFS
- 数据：两个冻结 manifest，每 Job 512 行，共 1,024 行；Job1 在 Job0 后 5 秒 release
- 执行：先跑五臂 correctness smoke，再跑一次五臂 rehearsal warmup schedule
- 正确性：executor-owned completion trace 与冻结 `doc_id` 集合独立核对缺失、重复、exactly-once
  和内容 digest
- 完整原始 root 与 tar 留在服务器仓库外；Git 只保留 compact validation 和本页单次观测

## 3. 严谨性自检

| 门禁 | 结果 |
|---|---|
| static config / installed source / live service identity | 通过 |
| endpoint health、PostgreSQL、Ray CPU/GPU clean、bounded baseline | 通过 |
| correctness smoke | 五臂 5/5 passed |
| rehearsal | 五臂 5/5 passed |
| completion | 每臂 1,024/1,024，exactly-once passed |
| sealed archive deep validation | passed；tar 与 completed root 全文件一致 |
| formal repeat / 稳定性 | 未运行；禁止据此排名 |

本次只有一个 rehearsal 样本，且 schedule 是 warmup schedule。资源 trace 已保留，但未完成 3 个交错
formal repeats、repeat CV 和全部性能合规门，因此下表只是诊断观测。

## 4. 实验设计

原生三臂保留 framework-owned execution/scheduling，项目只提供 source、typed Job release、完成证据和
指标采集，不注入 bounded-ready、shared credit 或项目调度。Project frozen-static 与 SAOR 共享执行器、
容量上限和请求语义，只改变提交控制策略。五臂共同使用相同服务身份和 no-writeback 完成边界。

## 5. 单次 rehearsal 观测

| arm | group JCT (s) | service throughput (tok/s) | Job0 JCT (s) | Job1 JCT (s) | request P99 / SLO | completion |
|---|---:|---:|---:|---:|---|---|
| Daft Native | 67.035 | 12,988.31 | 62.134 | 61.946 | unavailable | 1,024/1,024 |
| Daft Native/Ray | 76.183 | 11,427.56 | 71.357 | 71.091 | unavailable | 1,024/1,024 |
| Ray Data native graph | 183.805 | 4,736.43 | 183.689 | 177.084 | unavailable | 1,024/1,024 |
| Project frozen-static | 100.172 | 8,691.12 | 98.458 | 41.597 | Job0 90.739s / Job1 34.093s；两者 SLO violation=1.0 | 1,024/1,024 |
| SAOR | 76.671 | 11,353.91 | 75.290 | 41.214 | Job0 66.724s / Job1 32.714s；两者 SLO violation=1.0 | 1,024/1,024 |

`service throughput` 是模型服务 token 总量除以该臂 group JCT，单位 token/s。P99 是 Project profiler
请求时钟的秒数；原生框架未提供同口径请求生命周期，故不填 0、不由 Job JCT 反推。

## 6. 指标适用性与解释

| 指标 | 当前五臂是否可比 | 原因 |
|---|---|---|
| service throughput、group/per-Job JCT、完成正确性 | 是（正式结论仍需交错重复） | 五臂有共同计时与完成边界 |
| P99 / SLO violation | 否；只可在两个 Project 臂内作同口径诊断 | 原生三臂没有共同 request-level clock |
| empirical service lag / 最长无服务时间 | 否 | SAOR 有完整 registered-ready→completion ledger，frozen-static 与原生臂没有；SAOR 本次最长无服务时间为 4.909s，仅为单臂机制证据 |
| Jain / weighted fairness | 否 | 两 Job 都完成 512 行，`Jain(completed_rows)=1` 没有信息量；直接对异质、错峰 Job 的 work/JCT 求 Jain 会混入工作量和 arrival 差异 |

若要把后三项升级为五臂共同指标，必须先新增不改变框架调度所有权的被动 request lifecycle/ready-ledger
采集，并为每一臂补同 manifest 的 Job0/Job1 matched-solo control。公平性应使用共同 overlap window 的
归一化 attained service，或 `solo JCT / co-run JCT` slowdown，再按冻结 Job weight 计算 Jain；不能使用
完成行数伪造公平。

## 7. 对课题的含义

本次证明五臂 no-writeback 合同、eager SAOR、服务身份门、system preflight、completion evidence 和
封存验证可以端到端闭合。它没有证明 SAOR 胜出，也没有提供跨五臂的 P99、service lag 或公平性结论。

## 8. 下一步

1. 先独立审核 sealed rehearsal archive 和本报告的指标适用性。
2. 如公平与 tail 是 headline，先实现 observation-only native lifecycle + matched-solo capability，再重跑
   correctness smoke/rehearsal；不要从现有数据补算伪指标。
3. 只有审核后另行签发绑定 rehearsal validation/root/archive SHA 的 formal authorization，才运行
   1 warmup + 3 interleaved formal；本次未签发、未运行 formal。
