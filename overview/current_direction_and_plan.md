# 当前方向与计划

最后更新：2026-08-01

> 本文是两分钟快速参考卡片。完整定义以 `PROJECT_OUTLINE.md` 为准；当前执行顺序以
> `experiments/plans/experiment_status_and_gaps.md` §0 为准；实验数字以各结果目录的
> README、manifest 和 CSV 为准。

## 1. 当前重点

- **内部执行方向已锁定**：A（模型服务状态感知的请求成形/提交）+ B（算子代价估计）一起做，
  首个 workload 为图像 `AI_EMBED (CLIP)`。
- **外部 framing 尚待确认**：是否把“数据库↔GPU 经 Daft 桥接”作为最终题目/scope，仍待
  导师和学长确认；这不阻塞已经锁定的 image workload 与 path-B 工程验证。
- **文本轨道不是废弃**：vLLM 文本实验已完成 regime-dependent 机制闭环，遗留 formal
  统一标为 `parked-conditional`，仅在论文正文需要文本结果时恢复。
- **CLIP operator-E2E 门禁已通过**：在先校准 Daft actor shape 后，COCO val 5K×3
  formal 中静态阶段拆分相对 fused Daft Native 单卡为 1.296×、相对 fused Daft Ray
  双卡为 1.138×。这还不是对最强 staged Daft/Ray Data 的胜利；下一步是补 staged
  baseline、统一 pgvector sink、direct ceiling 和资源归一化，而非继续做小画像。

## 2. 课题定位

研究数据库 AI 算子外部分布式数据处理链路中，上游如何组织请求、估计工作量、控制提交节奏，
并根据模型服务状态协调 CPU 数据准备与 GPU 推理。项目不修改 vLLM、Ray scheduler 或模型 kernel。

两项研究内容保持不变：

1. **数据组织策略**：token/frame budget、长度/局部性感知分组和 Daft 引擎参数。
2. **调度与提交控制**：active-work、request-level credit、共享 credit、idle borrowing 和多 job 公平队列。

图像 workload 用于正文的多模态泛化验证；算子代价估计是两项策略的共同使能组件，不独立扩张为
第三项研究内容。写回使用 PostgreSQL + pgvector 工程 baseline。

## 3. 当前技术路径

**Image 目标路径（operator-E2E runner/formal 已完成，system-E2E 待补）**：

```text
PostgreSQL
  → Daft DataFrame
  → Ray CPU decode / preprocess + organizer / scheduler
  → typed tensor-input CLIP backend（Ray GPU actor 主路径；每张 4090 一个 actor）
  → PostgreSQL + pgvector
```

文本证据路径仍保留：

```text
PostgreSQL → Daft → Ray organizer / scheduler → vLLM → PostgreSQL
```

## 4. 已建立的关键证据

| 证据 | 当前可得出的结论 |
|---|---|
| 文本 feeding 同协议对照达到 direct client 的约 97.7% | 项目链路可以接近模型服务容量；feeding 缺口基本闭合 |
| 65,536 active work/endpoint 达最大吞吐的 97.8% | 固定 token-aware credit 是当前简单、稳健的文本默认点 |
| AIMD/PID/EWMA、动态 flush、多 actor 多数未过 5% 门槛 | 不能声称复杂动态策略普遍胜过强静态 baseline |
| 2-ep 与 4-ep cache-ON 数据组织排名反转 | 上游组织/准入价值依赖 endpoint consolidation 与 KV 饱和 regime |
| matched-KV：2-ep 中性、4-ep prefix routing +5.9% | 目前更支持 endpoint consolidation，而非单纯 per-endpoint KV 大小是驱动；仍有饱和深度混淆 |
| CLIP 5K 串行画像：CPU 准备/actor forward=`13.8–18.3` | 图像链路存在异构流水线候选空间；尚未证明 CPU、Ray/host copy 或 PCIe 谁是主瓶颈 |
| CLIP operator-E2E：project/fused-Daft=单卡 1.296×、双卡 1.138× | 独立校准后，静态阶段拆分优于 fused UDF；Daft-on-Ray/Ray Data staged 尚未测，故不能声称优于主流异构流水线 |

CLIP 画像进一步表明主要瓶颈位于 CPU processor 整体（fast path 约
4.4–4.8ms/image）；子阶段实验只直接测得 resize 约 1.3ms，剩余时间尚未充分归因，
不能全部写成 normalize。后续 operator-E2E 已把该候选空间转化为静态阶段拆分的
正结果，但状态感知策略仍未与冻结最佳静态 pipeline 对照。

## 5. 当前实施顺序

1. ✅ Fused Daft Native/Ray actor-shape 校准与 bounded project-Ray COCO 5K×3
   operator-E2E formal 已完成。
2. 先跑 `motivation/plans/image_host_data_path_bottleneck.md` 的 R0→R4 表示阶梯，
   再给三臂接统一 pgvector sink，并补 bounded direct CLIP、CPU-budget-normalized
   curve、Daft-on-Ray staged、Ray Data staged、vLLM pooling、naive 等完整 baseline；
   OceanBase `AI_EMBED` 等待可部署环境。
3. 在同 workload、同硬件、同计时边界下校准 frame budget、K、actor/endpoint 形状和静态 active work。
4. 实现 A：读取 CLIP endpoint queue/active-work 的状态感知请求成形与提交。
5. 实现 B：首版 `<100 LOC` 的解析代价模型 + profile 校准 + residual correction。
6. 用吞吐、JCT、P95/P99、SLO goodput、GPU busy ratio、能耗和 Recall@10 做正式消融。

晋级门槛：相对各自独立标定的强静态/系统 baseline 至少改善约 5%，重复方向一致，且质量不退化。

## 6. 仍不能声称

- 不能把静态阶段拆分胜过 Daft Native/Ray 写成“动态状态感知策略已胜出”；后者尚未
  与冻结最佳静态 project pipeline 正式对照。
- 不能把赢 fused Daft UDF 写成“优于 PolarDB/Daft 异构流水线”；staged baseline 尚未测。
- 不能把 CPU preprocess 主导写成“CPU→GPU 数据传输主导”。
- 不能把 4-ep 病态 bounded 值当作服务上限，或把 text/image 跨协议吞吐直接比较。
- 不能把 prefix/KV 机制迁移到 CLIP；CLIP 没有自回归 KV cache、TTFT 或 TPOT。
- 不能把 PG18.4 AutoDL rehearsal 写成 PG18.3 内部平台结论。
- 不能把“内部执行锁定”写成“导师已确认最终 scope”。

## 7. 文档入口

| 内容 | 权威入口 |
|---|---|
| 项目总纲 | `PROJECT_OUTLINE.md` |
| 当前执行状态与 parked 项 | `experiments/plans/experiment_status_and_gaps.md` §0 |
| 图像 workload、baseline 与门禁 | `experiments/plans/image_clip_workload_lock_20260731.md` |
| 5K CLIP 初始画像 | `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md` |
| 当前实现边界复测 | `motivation/results/gpu/image_clip_preprocess_variants_20260801/` |
| CLIP Daft Native/Ray operator-E2E | `motivation/results/gpu/image_clip_native_baseline_20260801/` |
| 正式机制证据台账 | `experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md` |
| 代码完成度与边界 | `code/INFRA_STATUS.md` |
| 文献与设计依据 | `research/knowledge_hub.md` |
