# 项目大纲

本文件是根目录下的项目总纲入口，用于快速了解当前方向、实验主线和近期调整点。详细材料仍以各目录 README、结果报告和开题报告为准。

## 当前题目与方向

开题报告当前收敛后的正式题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

项目总方向：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

当前重点不是传统数据库 GPU 查询算子，也不是模型 kernel 优化。数据库 AI 算子在本项目中主要作为 workload 入口和验证场景，研究主体是数据进入 Daft/Arrow 数据组织层、Ray task/actor 执行层、GPU-backed 模型服务和 Lance / pgvector / PostgreSQL sink 后的分布式执行与存储协同问题。

## 研究内容

当前开题报告采用三项研究内容：

1. AI workload 感知的数据组织与批处理构造方法。
2. GPU 推理服务状态感知的 Ray 并行调度与反压控制方法。
3. AI workload 执行链路中的持久化边界分析与轻量写回优化（不作为独立方法贡献，为 RC1↔RC2 跨层协同提供边界确认）。

阶段划分、执行画像和瓶颈归因不是独立研究内容，而是动机测试、方案设计和评价依据。

## 实验主线

当前实验主线优先从 `motivation/` 进入：

| 文件 | 用途 |
|---|---|
| `motivation/README.md` | 动机测试目录总入口 |
| `motivation/plans/workloads.md` | 三类 AI 算子场景、动机测试和后续实验优先级 |
| `motivation/plans/integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线 |
| `motivation/results/README.md` | 动机测试结果阅读顺序和结论边界 |
| `motivation/results/gpu/README.md` | 真实 GPU-backed E2E 结果入口 |

`feasibility/` 当前只作为组件、环境、脚本可用性的验证入口；`feasibility/guide.md` 不再承担项目实验大纲职责。

## 实验结论写作标准

所有实验结论、实验数据分析、开题可行性分析和飞书实验摘要，都参考 `learning/AGENTS.md` 的讲解标准组织。写实验结论时至少说明：

1. 为什么做这个实验，验证哪个问题。
2. 实验链路是什么，数据从哪里来，经过哪些系统或进程，写回哪里。
3. 参数和指标分别代表什么，例如 rows、batch、task/actor、ObjectRef、queue wait、bounded wait、fan-in、writeback。
4. 真实数据结果是什么，数字来自哪个 CSV 或报告。
5. 结果能说明什么，不能说明什么。
6. 结论属于本地实验事实、模拟实验事实、合理推断、待确认问题，还是不能声称的内容。
7. 下一步实验或消融如何验证当前解释。

正式报告可以比学习材料更凝练，但分析精细程度不能低于上述标准。

## 当前最重要证据

正式论证优先引用：

1. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`
   - 真实 GPU-backed embedding 链路拆分。
   - 1024 行下 fine / coalesced 端到端约 `13.4x`。
   - 16384 行下 operator 和 writeback 均为大块成本。
2. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`
   - 双 endpoint 下 Ray task / actor 开始体现并发 routing 价值。
   - 端到端收益仍受 writeback 约束。
3. `motivation/results/pg18_4_fake/system_profile.md`
   - PG18.4 本地同构 fake-model 历史画像。
   - 只能作为预演和历史信号，不代表真实 GPU-backed 结论。
4. `motivation/results/fake_cpu/analysis.md`
   - fake/CPU 历史预研。
   - 只用于解释早期为什么关注 task/object/invocation/fan-in/backpressure。

## 近期优先级

1. P0：接入 vLLM / Ray Serve（GPU baseline 升级到 S 级）；完成 B 系列写回工程实验（COPY + unlogged staging + deferred HNSW index，写回 baseline 升级到 A 级）。
2. P1：各研究内容独立 grid search——RC1 的 `batch_size × partition_count`、RC2 的 `K_max × endpoint_count × routing`、RC3 的三路写回架构对比（driver fan-in / worker-direct / queue-worker）。
3. P2：Killer Experiment（BL1-BL6），验证独立最优组合 vs 跨层联合最优的核心假设。
4. 扩展到 `AI_FILTER/AI_CLASSIFY`（simulated）和 `AI_COMPLETE`（simulated），验证方法跨 workload 泛化。
5. 后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地预演写成正式平台结论。

详细实验计划见 `experiments/plans/` 下四个独立计划文件和 `experiments/plans/README.md`。

## 同步规则

项目规划和开题材料采用双向同步：

- 开题报告必须基于当前项目进展、实验事实和后续规划撰写。
- 开题报告的题目、研究内容、技术路线、实验边界或侧重点变化时，项目整体规划、实验优先级和文档入口也要同步调整。
- 修改方向类内容时，至少检查 `README.md`、`PROJECT_INDEX.md`、本文件、`overview/current_direction_and_plan.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`opening/report/opening_report.md` 和 `opening/work_rules.md`。

## 日志入口

项目级简要操作日志见：

```text
PROJECT_LOG.md
```

开题材料的详细日志见：

```text
opening/logs/project_log.md
```

