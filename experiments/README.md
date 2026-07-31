# Research Experiments

本目录是正式研究实验入口，用于规划、运行和记录研究内容的优化实验与消融实验。它不同于 `motivation/`：动机测试回答”为什么这个课题值得做”，本目录回答”提出的方法或调优是否真的有效”。

## benchmark / workload 选型（当前主线，2026-07-31）

学长反馈（`../notes/communication_notes.md` §5）把场景 reframe 成"**数据库↔GPU 经 Daft 桥接、算子多样、大数据量、流式 pipeline**"，并定原则：**先锁被认可的 benchmark/workload**。核心判据：数据搬运瓶颈有两段——送 vLLM（拥挤）+ **DB 读出来 / CPU 搬到 GPU**（机会）；**当前 prompt 文本每行 ~1KB、搬运太轻，瓶颈不显现**，必须换"每行 payload 重"的 workload。方向 scope 见 [`../research/daft_db_gpu_bridge_direction_scope_20260731.md`](../research/daft_db_gpu_bridge_direction_scope_20260731.md)（Daft 三痛点核实 + 可防御界面 + §10.1 benchmark 三层）。

**首个 workload**：**图像 AI_EMBED (CLIP)**——每行 CPU→GPU 搬运 ~600KB（文本 ~600×）+ JPEG decode/resize 重，让 **DB 读 + CPU→GPU 数据搬运瓶颈真正显现**。设计 + go/no-go 门禁见 [`plans/image_clip_workload_lock_20260731.md`](plans/image_clip_workload_lock_20260731.md)。

**benchmark 三层**（讲清楚，见 scope §10.1）：① 数据集 = ImageNet/COCO（公开经典）；② 质量协议 = ANN-benchmarks recall@10（CCF 认可）；③ 吞吐/搬运协议 = 无现成 benchmark（厂商全闭源），项目 §7.5 自定（自定本身是贡献）。可引名字 = BigVectorBench（VLDB'25）image 切片 + ANN-benchmarks。

**升级路径**（benchmark 名始终 BigVectorBench）：CLIP image（当前）→ +MS MARCO text（轻对照，证明文本下不显现）→ +audio → 三模态触发冷启动 regime（若解封）。MS MARCO 降级为"文本轻对照"（[`plans/msmarco_embedding_workload_20260731.md`](plans/msmarco_embedding_workload_20260731.md)）——文本搬运太轻，不满足判据。workload 选型汇总见 [`plans/README.md`](plans/README.md) §〇。

## 当前状态

文本主线的正式研究实验已经开展：数据组织、K_max/flush 提交控制、联合搜索、prefix-aware、BFD/row-cap、CUDA Graph baseline 和算子代价估计均已有不同等级的证据。统一完成度、全部结果目录和结论边界见 [`results/EXPERIMENT_EVIDENCE_REGISTRY.md`](results/EXPERIMENT_EVIDENCE_REGISTRY.md)。

Adaptive K_max 的 shared-vLLM 双作业复验已经完成：static K8 保护前台，AIMD 无 decrease 并饱和至 K≈16，未优于 static K16；追加 adaptive-flush 分支同样没有稳定增量。双 4090 request-level active-work 已扩展到 16K–131K 并按预注册规则选择每 endpoint 65,536；固定资源的有界 Actor Pool 三形状、complete-row service quantum 和 SLO-aware EWMA flush 重复均已完成，三者都未达到 5% 晋升门槛。SLO-EWMA formal 还确认 25–50ms flush 动作相对 5.6–17.4s request P99 缺少一阶控制杠杆。当前保留 `request + 1×256 + 65K active work + fixed-50` 作为单 job 基线。尚未完成的主要验证是多 job 共享 endpoint 的 request/work credit 与公平队列、UCB 的 epoch reward 正确归因与端到端接入、路由/故障迁移（prefix-affinity routing：2-ep/7B 中性（−0.1%，<5% 门禁，见 `results/prefix_cache_routing_req_20260730/`）；4-ep/1.5B +5.9% 跨门禁（46,943 vs 44,317 tok/s）但混淆 model×endpoint×KV 且过饱和（SLO 违约 25–31%），有条件重新打开，待隔离消融（见 `results/prefix_cache_routing_4ep_1.5b_20260731/`）；cache-ON prefix batching 消融见 `results/prefix_cache_data_org_20260730/`），以及图像 workload 多模态泛化。GPU-backed `AI_EMBED` 动机证据仍放在 `motivation/results/gpu/`，不与方法实验混放。

2026-07-30 的 f203257 双协议 formal 已关闭 feeding 缺口：
Completions fixed16 达同协议 direct 的 97.7%，Chat async K256 与 bounded
Chat 同量级。后续模板已冻结 32K throughput-oriented budget、K256/endpoint、
65K active work；旧 Completions 三形状证据暂保留 1×256 async actor，
但最终合同现在要求在 32K 工作点补跑 1/2/4/8/16 同协议曲线后重新选择。
一份 512 行 Chat 曲线显示约 4–8 actors 进入平台、16 actors 方差增大；它是
协议特定 feeding 诊断，不能直接替换 Completions 默认。49K 另记为
SLO-goodput 候选。
旧 8K length-align 显示 P50/SLO 的明确正信号，但必须在冻结合同下重跑。
4-job `ray_task` 因数百 worker 撞上容器 VMA 上限；有界 actor gate 已在同一
65530 VMA 容器通过，默认 formal 因而恢复为 1/2/4-job。

2026-07-30 的 short/long prompt static-credit screening 完成 48/48 run，
但未通过正式判决门禁：运行使用 urllib 且未返回 token IDs，short 的
K256/W65K/W98K 没有形成准入等待却出现 48.5% 中位数吞吐分裂，
W65K/W98K repeat CV 达 18%/34%。远端算术平均的“共同 W65K”与正式
中位数选择相反，因此只登记为真实 GPU screening / 机制审计，状态为
`inconclusive`。下一步先运行 async/token-ID 等价臂 gate，不直接启动
adaptive formal。

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
