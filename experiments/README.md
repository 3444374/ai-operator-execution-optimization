# Research Experiments

本目录是正式研究实验入口，用于规划、运行和记录研究内容的优化实验与消融实验。它不同于 `motivation/`：动机测试回答”为什么这个课题值得做”，本目录回答”提出的方法或调优是否真的有效”。

## 当前状态

文本主线的正式研究实验已经开展：数据组织、K_max/flush 提交控制、联合搜索、prefix-aware、BFD/row-cap、CUDA Graph baseline 和算子代价估计均已有不同等级的证据。统一完成度、全部结果目录和结论边界见 [`results/EXPERIMENT_EVIDENCE_REGISTRY.md`](results/EXPERIMENT_EVIDENCE_REGISTRY.md)。

当前尚未完成的主要验证是 adaptive K_max 的 shared-vLLM 双作业复验、UCB 的 epoch reward 正确归因与端到端接入、真实多 endpoint/多 GPU 调度、Ray/vLLM 容量分层调优、prefix cache 开启后的独立消融，以及图像 workload 多模态泛化。AIMD/EWMA/PID 的单作业 GPU 矩阵已完成，但没有优于同上限 static K=16。GPU-backed `AI_EMBED` 动机证据仍放在 `motivation/results/gpu/`，不与方法实验混放。

## 目录分工

| 路径 | 作用 |
|---|---|
| `plans/` | 正式研究实验计划，按研究内容组织 baseline、变量、消融和指标 |
| `results/` | 正式研究实验结果、小改动调优记录和结论边界 |

## 研究内容对应实验

| 研究内容 | 主要实验问题 | 初始候选实验 |
|---|---|---|
| 研究内容一：数据组织策略 | batch 构造方式（按计算量 vs 按行数）、分组策略如何影响端到端性能 | token-budget vs 固定 batch_size、length-aligned vs prefix-aware vs random、Daft into_batches/repartition/batch_size 参数 sweep |
| 研究内容二：调度与提交控制策略 | Ray actor 自适应提交、routing、K_max 动态控制如何影响 queue wait 和 GPU utilization | queue-adaptive flush vs 固定 K_max、actor pool 分池 routing、Daft max_concurrency/gpus 参数 sweep |
| 多模态泛化验证 | 文本上的策略在图像 workload 上是否一致有效 | 同一套策略代码，文本 df[“prompt”] → 图像 df[“image”]，token-budget → frame-budget |
| 算子代价估计（补充） | profile-driven 成本预测是否可用 | 基于已有实验数据做 grouped held-out 二次分析；报告 MAE/MAPE/RMSE/R²，不预设 MAPE 必须低于 20% |

写回使用 PostgreSQL + pgvector（COPY + deferred index baseline），不作为独立实验阶段。

## 结果记录要求

每个结果至少包含：

1. 对应研究内容和研究问题。
2. 实验链路与运行命令。
3. 参数、指标和 CSV / 日志路径。
4. baseline、优化方案和消融设置。
5. 真实结果和主要数字。
6. 能说明什么、不能说明什么。
7. 下一步需要补的验证。

图表统一放在 `figures/`。本目录只引用图，不长期保存图副本。
