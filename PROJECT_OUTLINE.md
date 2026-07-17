# 项目大纲

本文件是根目录下的项目总纲入口，用于快速了解当前方向、实验主线和近期调整点。详细材料仍以各目录 README、结果报告和开题报告为准。

## 当前题目与方向

开题报告当前收敛后的正式题目：

> 面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究。

**2026-07-16 方向重大更新**：主场景从 AI_EMBED 转向 AI_COMPLETE（生成式 LLM 推理），上游 batching 从静态固定 batch_size 转向探索按 token 量动态组织的方式，Ray 从 task executor 升级为架构设计空间（异构 actor pool + 去中心化自适应提交）。vLLM 定位为部署平台。Daft 确认为数据引擎，文本阶段直接接入，多模态阶段复用同一套 pipeline。详见 `PROJECT_LOG.md` 2026-07-16 条目。

**2026-07-17 更新**：与导师讨论后明确多模态实验进入正文（§5.3 策略泛化性验证），不是仅 Discussion。算子代价估计作为 §6.1 补充讨论。优化空间从纯策略层扩展为"策略级决策 + 引擎级参数"联合表征。Daft 从"后续切换"改为文本阶段直接接入。

当前重点不是传统数据库 GPU 查询算子，也不是模型 kernel 优化。数据库 AI 算子在本文中作为 workload 入口，研究重点是上游 Ray 数据执行层的调度优化——探索数据组织策略和提交控制策略，利用 Ray actor 实现去中心化自适应提交。Daft 作为数据引擎，提供 Rust 执行内核、Arrow 零拷贝、Morsel 流式背压和 `@daft.cls` GPU UDF 接口。

## 研究内容

当前开题报告采用两项策略设计 + 多模态泛化验证 + 算子代价估计（补充）：

1. **AI workload 感知的动态数据组织与批处理构造策略**（研究内容一）：对比 token-budget batching 与固定 batch_size 在端到端吞吐和 P99 延迟上的差异，以及 length-aligned/prefix-aware 分组与随机分组的效果差异。利用 Ray actor 异构化实现。引擎级参数（Daft `into_batches`、`batch_size`、`repartition`）与策略级决策共同构成优化空间。
2. **运行层调度与提交控制策略**（研究内容二）：利用 Ray actor 研究去中心化的调度与提交控制，候选策略包括 queue-adaptive flush、K_max 动态控制、actor pool 分池路由等。引擎级参数（Daft `max_concurrency`、`gpus` 分配）与策略级决策共同构成优化空间。
3. **多模态泛化验证**（正文实验，§5.3）：在图像 workload 上使用同一套策略代码和配置逻辑，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。
4. **算子代价估计**（补充讨论，§6.1，不作为独立研究内容）：基于实验阶段采集的 profile 数据，建立 AI 算子的端到端成本估计方法，辅助编排决策。

写回使用 PostgreSQL + pgvector（COPY + deferred index baseline），不作为独立研究内容，仅在实验设置中说明。

**主场景：AI_COMPLETE**（生成式 LLM 推理，文本）+ **AI_EMBED/AI_CLASSIFY**（图像，多模态泛化验证）。AI_EMBED 文本预研已完成（真实 GPU-backed 链路）。

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

1. **前置（必须最先完成）**：接入 vLLM + 小 LLM（Qwen2.5-1.5B 级，适配 RTX 5070 12GB VRAM）替代手动 HTTP endpoint，建立 continuous batching baseline；构造 AI_COMPLETE workload（三类 token 长度分布 + shared prefix 控制）。Daft 在文本阶段直接接入，替代裸 Arrow RecordBatch 构造。
2. **核心实验（文本）**：研究内容一动态 batching 消融——静态固定 batch_size vs token-budget vs length-aligned vs prefix-aware grouping，同时探索 Daft `into_batches`、`batch_size`、`repartition` 等引擎级参数；研究内容二自适应提交消融——固定 K_max vs queue-adaptive flush vs actor pool 分池路由，同时探索 Daft `max_concurrency`、`gpus` 分配。
3. **耦合验证**：独立最优（研究内容一最优 + 研究内容二最优）拼接 vs 联合 grid search（batching policy, submission policy, 引擎级参数）。如交互不显著，调整 claim 为"分层独立优化框架"。
4. **多模态泛化验证（正文 §5.3）**：图像数据集（ImageNet/HuggingFace），CLIP embedding + 分类。同一套策略代码，`df["prompt"]` 换为 `df["image"]`，验证 token-budget → frame-budget、queue-adaptive flush 复用。VLM 生成实验（Qwen2.5-VL-3B）标记为 optional。
5. **算子代价估计（§6.1 讨论，最低优先级）**：基于上述实验已采集的 profile 数据，不新增实验。仅当核心优化实验全部完成后才启动。
6. 后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地预演写成正式平台结论。

写回使用 PostgreSQL + pgvector（COPY + deferred index baseline），不作为独立实验阶段。

**Scope 缩减触发条件**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion
- 文本 RC1+RC2 消融未完成前，不启动 Daft 多模态 pipeline
- VLM 生成实验始终标记为 optional

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

