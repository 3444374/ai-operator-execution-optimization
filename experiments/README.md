# Research Experiments

本目录是正式研究实验入口，用于规划、运行和记录研究内容的优化实验与消融实验。它不同于 `motivation/`：动机测试回答”为什么这个课题值得做”，本目录回答”提出的方法或调优是否真的有效”。

## benchmark / workload 选型（当前主线，2026-08-02）

学长反馈（`../notes/communication_notes.md` §5）把场景 reframe 成“数据库↔GPU 经
Daft 桥接、算子多样、大数据量、流式 pipeline”，并强调先锁 workload。2026-08-01
审计后，图像 CLIP 的选择理由收紧为“让 DB/CPU/Ray/H2D/GPU 木桶效应可测”，不再
预设数据搬运是瓶颈或执行层为空白。方向 scope 见
[`../research/daft_db_gpu_bridge_direction_scope_20260731.md`](../research/daft_db_gpu_bridge_direction_scope_20260731.md)。

**首个 workload**：**图像 AI_EMBED (CLIP)**——JPEG decode/processor + 约 600KB
pixel tensor 让 DB/CPU/Ray/H2D/GPU 的木桶效应成为可测变量，但不预设哪一段是主瓶颈。
设计 + go/no-go 门禁见 [`plans/image_clip_workload_lock_20260731.md`](plans/image_clip_workload_lock_20260731.md)。

**benchmark 四层**：① 产品/算子语义用 SemBench 和 OceanBase/PolarDB/Snowflake/
BigQuery 官方文档对齐；② 公开多模态执行协议参考 Ray Data 与 PolarDB 的 ImageNet、
PDF、audio、video benchmark；③ 任务质量按 ImageNet top-1/top-5、COCO mAP/F1 或
embedding Recall@K/MRR/nDCG；④ 本项目同机 database-operator track 统一 PostgreSQL
BYTEA、模型、预处理、资源、计时边界和 sink，采集阶段/硬件诊断。公开 benchmark
不是全闭源，但没有一套与本项目 PostgreSQL→CPU preprocess→GPU→pgvector 完全一致
的现成协议，因此必须同时保留公开 file/object track 和本项目 database track。

baseline 如何检索、证据如何分级、哪些指标必须记录，以
[`plans/baseline_reference.md`](plans/baseline_reference.md) 的检索流程和指标合同为准。
文本 AI_COMPLETE 的原生性审计、Chat/Completions 分轨和 64→512→4096 复测合同见
[`plans/text_native_baseline_rerun_20260802.md`](plans/text_native_baseline_rerun_20260802.md)。
MS MARCO 仅作为文本 embedding 轻对照，不能把图像、文本、音频强行统称为
BigVectorBench。

## 当前状态

文本实验已经形成不同等级的历史证据，当前统一 parked-conditional；具体数字只从
[`results/EXPERIMENT_EVIDENCE_REGISTRY.md`](results/EXPERIMENT_EVIDENCE_REGISTRY.md)
和对应结果目录读取，不再在本入口复制容易过期的参数与“下一步”。

图像 runner 已新增 Daft 内置 `embed_image` native arm；Ray Data arm 只使用官方
`read_sql/map_batches/ActorPoolStrategy` graph，由 Ray Data 自己调度。原有 Daft
Native/Ray/staged 是项目自写 UDF reference，旧 5K×3 数据只保留为机制诊断，不能称
官方 baseline；旧 256 行 staged gate 也需在移除项目式 inflight 后重做 native gate。
后续还需直接复用 Daft 官方 803,580-row ResNet18 脚本做 vendor-code parity，并完成
60 秒以上稳态、统一 pgvector sink、任务 ground truth 和失败 run 落盘。当前执行状态
以 [`plans/experiment_status_and_gaps.md`](plans/experiment_status_and_gaps.md) §0 为准。

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
