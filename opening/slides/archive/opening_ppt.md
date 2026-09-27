# 开题汇报 PPT 源稿

> 更新日期：2026-07-29。本文件是下一版 PPT 的内容口径，不直接代表最终排版；生成 PPTX 前仍需读取学校模板，并保护用户已经手工调整的页面。

## 1. 题目页

标题：数据库 AI 负载的执行优化与调度研究

副标题：面向 Daft/Ray 上游数据组织、饱和效率与多 job 共享调度

备注：

```text
汇报讲稿：
课题研究数据库 AI 算子调用外部模型服务时的上游执行链路。vLLM 是固定下游，不修改其内部；重点是数据怎样组织、以多少在途 work 提交，以及多个 job 怎样共享服务。

答辩备注：
不做 GPU kernel、模型结构、Ray scheduler 内核或 Serverless 资源扩缩容。
```

## 2. 背景：数据库成为 AI workload 入口

主结论：AI 算子把数据库执行路径扩展为“数据组织—外部推理—结果写回”。

- 工业入口：Cortex AISQL、BigQuery AI、Oracle Vector、PostgreSQL AI 生态
- 学术系统：LOTUS、Galois、GaussML、Palimpzest
- 新成本：token work、模型调用、队列、GPU capacity、失败/重试、写回

备注：

```text
汇报讲稿：
已有系统证明场景真实，但系统收益可能来自减少模型调用、换模型、执行并发或写回优化，必须区分因果来源。
```

## 3. 问题边界

```text
Database source
  → Daft / Arrow organization
  → Ray actors: work estimation + credit + routing
  → vLLM continuous batching（不修改）
  → fan-in / PostgreSQL + pgvector
```

主结论：上游调度不能突破 GPU 物理上限；它要解决的是以更小压力更快达到 capacity ceiling，并在多 job 下控制排队和公平性。

## 4. 文献基线

| 方向 | Top 15 代表 | 核心补充 |
|---|---|---|
| 数据库 AI 算子 | LOTUS、Galois、GaussML | Palimpzest、SemBench、Cortex AISQL、InferDB、SmartLite |
| LLM serving/fairness | vLLM、Orca、Sarathi、SGLang、VTC、Llumnix、DistServe | Database Perspective on LLM Inference Systems、Splitwise、Clockwork、FairServe、DLPM、Autellix、Chiron |
| Ray | Ray OSDI 2018 | Ray Data Streaming Batch、Daft 官方实现 |
| 代价估计 | Learned Cost Models、GRACEFUL、COSTREAM、Abacus | CONCERTO、SFS |

页脚：Top 15 为 15/15 严格 CCF-A 正式 research paper；CIDR、Tutorial、Companion、arXiv 单列。

## 5. 收窄后的研究缺口

已有工作分别覆盖：

- semantic operator 的质量/成本/调用数优化；
- vLLM 内部 continuous batching 与 KV 管理；
- serving 内部/服务层多租户公平和多实例调度；
- Ray/Daft 的批数据执行。

仍缺少的组合问题：

> 不修改 vLLM 时，数据库 AI operator 的 Daft/Ray 上游如何用统一 work 估计协调数据组织、最小饱和压力、request-level replenishment 和多 job shared credit，并在同模型、同 work、同硬件的官方 baseline 上验证。

## 6. 三个研究问题、两项方法

| 研究问题 | 回答什么 | 归属 |
|---|---|---|
| 饱和效率 | 最小饱和 active work、time-to-ceiling、ramp regret | 调度与提交控制 |
| 数据组织 | 相同 token work 下怎样分组和提交 | 数据组织策略 |
| 多 job | shared credit、idle borrowing、fairness、job JCT | 调度与提交控制 |

代价估计贯穿三问，不单列第三项贡献；多模态是泛化验证。

## 7. 研究内容一：数据组织

- fixed rows：强静态 baseline
- token-budget：按预测 work 控制 submission
- length-align：减少 batch 内 work 离散
- prefix-aware：仅在 prefix cache 开启并有命中证据时验证
- semantic operator plan：LOTUS/Palimpzest/Abacus 代表“减少 work”，与“相同 work 执行更快”分开

指标：JCT、tokens/s、P99、active-work 波动、cache 命中、调用数和质量。

## 8. 研究内容二：Ray 调度提交

- request-level continuous replenishment
- endpoint-shared request/work credit
- work-conserving idle borrowing
- 多 job fair queue / weighted service
- Ray actor 保存 job 状态、累计服务量与异步完成事件

强 baseline：独立标定的固定 request/work credit。动态策略只有超过同上限静态策略才晋级。

## 9. 代价估计：共同使能组件

```text
解析模型
  + 少量 profile 校准
  + actual usage / completion residual correction
```

预测：

- prompt/output token work
- operator service time、JCT、remaining work、SLO slack

驱动：

- active-work/K 初始化
- 数据组织、endpoint 路由、提交策略

评价：误差 + ranking + 决策 regret + prediction interval；首版不做复杂 learned optimizer。

## 10. Baseline 设计

| 层 | Baseline |
|---|---|
| serving ceiling | vLLM Bench |
| 无 Daft/Ray | 现有数据库 AI_COMPLETE；bounded HTTP |
| 官方 runtime | Daft `prompt()` Native/Ray；Ray Data HTTP Processor |
| 数据库 AI 系统 | LOTUS、Palimpzest；SemBench 作为 workload/指标依据 |
| proposed | fixed/token-work、request refill、shared-credit/fair queue |

同条件契约：相同 model/endpoint/manifest/output/work；每个 arm 独立 calibration，不要求无限调优，只要求合理强并进入平台期。

## 11. 已有结果：先填满 GPU

双 4090 active-work 曲线显示：

- 65,536 达到最大已测均值的 97.8%；
- 再加到 98K/131K 吞吐基本不增，P99 上升；
- 说明后续策略不能靠增加 offered work 获得表面收益。

主结论：先锁定 capacity ceiling 和最小饱和 work，再比较策略。

## 12. 已有结果：静态强 baseline 很重要

- request-level K 值不能直接与 batch K 平移，应按 token work 对齐；
- actor pool 形状、固定 service quantum、25–50ms SLO/EWMA flush 均未达到 5% 晋级门槛；
- adaptive 与 fixed-50 在当前秒级 P99 场景不可分辨。

主结论：负结果收窄了设计空间；不再围绕 alpha/deadband 参数挖掘。

## 13. 多 job 的价值位置

单 job 饱和时，上游很难提高 GPU 峰值吞吐；多 job 时需要选择谁先获得有限 credit。

评价同时报告：

- 聚合 throughput/capacity efficiency
- 每 job JCT、P95/P99、slowdown、SLO
- Jain fairness、idle borrowing
- endpoint-shared request/work 上限与 exactly-once

VTC 提供 service-counter 基线，Llumnix提供 virtual usage/动态纠偏思想。

## 14. 实验计划

1. 同规模同条件 baseline calibration 与 held-out 对照
2. 小 job transient：minimum saturating work、time-to-ceiling、ramp regret
3. 数据组织正式重复：token-budget/length/prefix 分离消融
4. 多 job：1/2/4 job、staggered、weighted、heterogeneous workload
5. cost-model held-out calibration 与决策 regret
6. 图像 workload 复用 frame work/credit 做泛化验证

门槛：至少 5% 性能增益，或吞吐不变时饱和 work 至少降低 20%、P99 至少降低 10%、time-to-ceiling/ramp regret 至少改善 10%。

## 15. 风险与控制

| 风险 | 控制 |
|---|---|
| baseline 默认配置过弱 | 每 arm 独立 calibration，记录 resolved config |
| 减少 work 与执行加速混淆 | 同 work runtime 与 system-level quality/cost 分开 |
| 延迟计时语义不同 | 统一 manifest-arrival→completion，拆 client wait/service |
| “力大砖飞”掩盖策略 | 先固定最小饱和 active work |
| 自适应策略参数挖掘 | 预注册门槛，负结果停止 |
| cost model 只提升 MAPE | 必须改善 ranking/regret |

## 16. 总结

- 研究目标不是突破 vLLM 物理上限，而是提升上游饱和效率、数据组织效率和多 job 可控性。
- 保留两项方法：数据组织；调度与提交控制。
- 代价估计升级为共同使能组件，多模态用于泛化验证。
- 正式 baseline 覆盖 direct serving、无 Daft/Ray、官方 runtime 和数据库 AI 系统。
- 已有负结果帮助排除无效控制器，后续聚焦同条件 baseline、transient ramp 和多 job fairness。
