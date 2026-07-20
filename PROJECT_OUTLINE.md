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

`feasibility/` 当前只作为组件、环境、脚本可用性的验证入口。

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

1. `experiments/results/local_vllm_qwen15b_baseline/README.md`
   - **2026-07-18/19 本地 vLLM + Qwen2.5-1.5B AI_COMPLETE baseline 全系列**。
   - Token-tail revision：固定行 batch=8 时 token 跨度 13.9×，batch=128 时 token P95=26678——证明固定行数是计算量的弱代理。
   - Token-budget vs Fixed Row：token_budget=6144/8192 约束 token P95 至 ~6141/8171（vs fixed 64/128 的 16377/26677），吞吐接近。
   - Shared-vLLM K_max 干扰：bulk unbounded 时 foreground E2E 恶化 2.3×（4.9→11.4s）而 bulk 自身吞吐几乎不变——证明 K_max 在共享 vLLM 下必要。
   - Queue-adaptive flush 已实现，但当前不如静态 K_max=8，需改进控制器。
   - 边界：本地 rehearsal，不代表 PG18.3 内部平台结果。
   - 状态与缺口审计：`experiments/plans/experiment_status_and_gaps.md`。
2. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`
   - 真实 GPU-backed embedding 链路拆分（AI_EMBED 预研，已完成）。
   - 1024 行下 fine / coalesced 端到端约 `13.4x`。
   - 16384 行下 operator 和 writeback 均为大块成本。
3. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`
   - 双 endpoint 下 Ray task / actor 开始体现并发 routing 价值。
   - 端到端收益仍受 writeback 约束。

## 近期优先级

**已完成**：
- ✅ vLLM + Qwen2.5-1.5B baseline 建立（07-18）
- ✅ Daft 文本阶段直接接入，`PostgreSQL → DaftPostgresSource → DaftOrganizer → Ray → vLLM` 链路跑通
- ✅ 固定行 batch token-tail revision（动机证据：行数是计算量的弱代理）
- ✅ Token-budget vs Fixed Row 对照（策略信号：token-budget 约束 token tail）
- ✅ Shared-vLLM 2-job K_max 干扰实验（动机证据：K_max 在共享 vLLM 下必要）
- ✅ Queue-adaptive flush 首次实现与测试（但未超越静态 K_max=8）
- ✅ Length-align + Prefix-aware 初步 ablation

**当前缺口（详见 `experiments/plans/experiment_status_and_gaps.md`）**：

1. **P0（最高优先）**：改进 queue-adaptive 控制器，在 shared-vLLM 下超越静态 K_max=8。如 3 轮改进后仍不能超越，RC2 降级为"K_max admission control 必要性论证 + queue-adaptive 探索性讨论"。
2. **P0（并列）**：两项策略联合消融——独立最优拼接 vs 联合 grid search。判定分层独立优化是否足够。
3. **P1**：Prefix 受控 workload 实验（prefix ratio 0/30/70/100%）+ 至少一个实验 scale 到 2048 行。
4. **P2（触发条件：P0+P1 完成）**：多模态泛化验证（CLIP embedding + ImageNet/HF subset）。
5. 算子代价估计（§6.1 讨论，最低优先级）：基于已采集的 profile 数据，不新增实验。
6. 后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地预演写成正式平台结论。

**指标盲区**（需在后续实验中补齐）：
- `tokens/s`：比 `rows/s` 更公平的 AI_COMPLETE 效率指标
- `service_p99`：系统性采集 tail latency
- inflight/queue 时间序列：诊断 adaptive 行为
- per-request e2e latency 分布：支持分组策略论证

写回使用 PostgreSQL + pgvector（COPY + deferred index baseline），不作为独立实验阶段。

**Scope 缩减触发条件**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion（✅ 已建立，未触发）
- 文本 RC1+RC2 消融未完成前，不启动 Daft 多模态 pipeline
- VLM 生成实验始终标记为 optional
- Adaptive 控制器 3 轮改进后不能超过 static K_max=8 → RC2 降级

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

