# Research Experiments

本目录是正式研究实验入口，用于规划、运行和记录研究内容的优化实验与消融实验。它不同于 `motivation/`：动机测试回答”为什么这个课题值得做”，本目录回答”提出的方法或调优是否真的有效”。

## 当前状态

文本主线的正式研究实验已经开展：数据组织、K_max/flush 提交控制、联合搜索、prefix-aware、BFD/row-cap、CUDA Graph baseline 和算子代价估计均已有不同等级的证据。统一完成度、全部结果目录和结论边界见 [`results/EXPERIMENT_EVIDENCE_REGISTRY.md`](results/EXPERIMENT_EVIDENCE_REGISTRY.md)。

Adaptive K_max 的 shared-vLLM 双作业复验已经完成：static K8 保护前台，AIMD 无 decrease 并饱和至 K≈16，未优于 static K16；追加 adaptive-flush 分支同样没有稳定增量。双 4090 request-level active-work 已扩展到 16K–131K 并按预注册规则选择每 endpoint 65,536；固定资源的有界 Actor Pool 三形状、complete-row service quantum 和 SLO-aware EWMA flush 重复均已完成，三者都未达到 5% 晋升门槛。SLO-EWMA formal 还确认 25–50ms flush 动作相对 5.6–17.4s request P99 缺少一阶控制杠杆。当前保留 `request + 1×256 + 65K active work + fixed-50` 作为单 job 基线。尚未完成的主要验证是多 job 共享 endpoint 的 request/work credit 与公平队列、UCB 的 epoch reward 正确归因与端到端接入、路由/故障迁移、prefix cache 开启后的独立消融，以及图像 workload 多模态泛化。GPU-backed `AI_EMBED` 动机证据仍放在 `motivation/results/gpu/`，不与方法实验混放。

2026-07-30 的 f203257 双协议 formal 已关闭 feeding 缺口：
Completions fixed16 达同协议 direct 的 97.7%，Chat async K256 与 bounded
Chat 同量级。后续模板已冻结 32K throughput-oriented budget、K256/endpoint、
65K active work 和 1×256 async actor；49K 另记为 SLO-goodput 候选。
旧 8K length-align 显示 P50/SLO 的明确正信号，但必须在冻结合同下重跑。
4-job `ray_task` 因数百 worker 撞上容器 VMA 上限，当前默认 formal 改为
1/2/3-job，j4 使用有界 actor pool 的独立 gate/formal。

2026-07-29 起，继续增加调度策略前先补两层同规模 baseline：第一层为无
Daft/Ray 的 OceanBase `AI_COMPLETE` 与同 PostgreSQL bounded AsyncIO；第二层
为 Daft `prompt()` Native/Ray 和 Ray Data HTTP Processor。两层与 ours 均统一
重跑 Chat Completions，并以 direct-vLLM 作为容量上限。正式预注册见
`plans/database_ai_operator_baseline_matrix_20260729.md`。

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
| 算子代价估计（共同使能组件） | work/service/JCT 预测能否改进 active-work、组织、路由与提交决策 | 简单解析模型 + profile + residual；报告误差、配置 ranking、决策 regret 与预测区间 |

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
