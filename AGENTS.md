# AGENTS.md

本文件是项目级长期规则（方向、边界、工作方式）。详细内容见各目录的 `README.md`、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md`。

## 1. 项目目标

研究方向：数据库 AI 负载的执行优化与调度。不回到传统数据库内核或 GPU 查询算子。

**课题定位**：优化数据库 AI 算子外部执行链路的上游调度——数据如何组织为请求、以什么节奏发送、如何根据模型服务状态调节并发。vLLM 为部署平台（不修改其内部）。Ray 作为架构设计空间，利用其 actor 模型和异步能力实现调度方案。Daft 作为数据引擎（Rust 核心 + Arrow 零拷贝 + `@daft.cls` GPU UDF），从文本阶段直接接入，多模态阶段复用同一套 pipeline 代码。

**方向已收敛，策略候选池开放**：经过 2026-07-16 的讨论与文献收集，优化方向已明确收敛到上游调度（数据组织 + 提交控制），但具体策略不提前锁定——动态 batching（token-budget/length-align/prefix-aware）、K_max 自适应、queue-adaptive flush、actor pool 分池路由等均为候选方案，最终采用哪些由后续实验数据决定。新增候选策略应记入 `research/knowledge_hub.md` §5 供以后参考。

**两项策略设计 + 多模态泛化验证 + 算子代价估计（补充）**：

1. **研究内容一：数据组织策略**。探索按计算量（token 量/frame 量）而非固定行数的动态组织方式，以及按计算量相似度分组对推理效率的影响。利用异构 actor pool 实现。引擎级参数（Daft `into_batches`、`batch_size`、`repartition`）与策略级决策（token-budget、length-align、prefix-aware）共同构成数据组织优化空间。
2. **研究内容二：调度与提交控制策略**。利用 Ray actor 的 stateful + async 能力，研究去中心化的调度与提交控制。候选策略包括 queue-adaptive flush、K_max 动态控制、actor pool 分池路由等。引擎级参数（Daft `max_concurrency`、`gpus` 分配）与策略级决策共同构成提交控制优化空间。
3. **多模态泛化验证**（正文实验，验证策略抽象不依赖数据模态）。在图像 workload（AI_EMBED/AI_CLASSIFY，CLIP/Qwen2.5-VL）上使用同一套策略代码和配置逻辑，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。
4. **算子代价估计**（补充讨论，不作为独立研究内容）。基于实验阶段采集的 profile 数据，建立 AI 算子的端到端成本估计方法，辅助编排决策。

写回使用 PostgreSQL + pgvector，COPY + deferred index 为工程 baseline。不作为独立研究内容，仅在实验设置中说明。

**验证方式**：研究内容一和二的每种策略通过消融实验对比（静态 baseline vs 动态策略）。两项策略分别独立搜索最优配置后拼接，再与联合 grid search 对比——联合显著优于拼接则说明需要联合调优，两者接近则分层独立优化即可。无论哪种结果，都不改变课题的核心贡献（上游优化策略设计）。多模态实验使用同一套策略代码，仅替换数据列类型（`df["prompt"]` → `df["image"]`）。

**主场景**：AI_COMPLETE（生成式 LLM，文本）+ AI_EMBED/AI_CLASSIFY（图像，多模态泛化验证）。AI_EMBED 文本预研已完成。vLLM 为部署平台。Daft 为数据引擎。

详细描述见 `PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md`。

## 2. 当前边界

```text
PostgreSQL 18.3
  → Daft DataFrame（数据引擎，文本 df["prompt"] / 图像 df["image"]）
  → Ray 动态 Batching（token-budget / length-align / prefix-aware）
    + Ray actor 架构（异构 actor pool / queue-adaptive flush / 去中心化）
  → AI_COMPLETE（文本 LLM，主场景）/ AI_EMBED/AI_CLASSIFY（图像，多模态泛化验证）
  → vLLM Continuous Batching + PagedAttention（部署平台，不修改）
  → PostgreSQL + pgvector（写回）
```

不要把主线写成：改造 vLLM 或 continuous batching、改造 Ray 调度器、Daft/Ray 单纯集成、Arrow serialization 优化、传统 GPU 查询算子、模型 kernel 优化（GQA/MQA/Flash-Attention）、Python toy benchmark。

## 3. 当前证据与下一步

已有实验：GPU-backed AI_EMBED 预研链路（37.5× fine vs coalesced、pgvector writeback 0.897s vs JSON 1.567s）。详细数据见 `motivation/results/gpu/`。CPU/fake 实验仅历史参考。

下一步优先级：① 建立 vLLM + Qwen2.5-1.5B baseline → ② Daft 集成 + 数据组织策略 + 提交控制策略消融（文本）→ ③ 耦合验证（独立拼接 vs 联合 grid search）→ ④ 多模态泛化验证（图像，同一套策略代码）→ ⑤ 算子代价估计（基于已有数据）。写回使用 PostgreSQL + pgvector（COPY + deferred index），不作为独立实验阶段。详见 `PROJECT_OUTLINE.md` §近期优先级。

**Scope 缩减触发条件（2026-07-17 约定）**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion
- 文本 RC1+RC2 消融未完成前，不启动 Daft 多模态 pipeline
- VLM 生成实验（AI_COMPLETE 多模态版）始终标记为 optional

## 4. 目录规则

| 目录 | 用途 |
|---|---|
| `overview/` | 项目总纲（`current_direction_and_plan.md` 为 TL;DR 快速参考卡片，以 `PROJECT_OUTLINE.md` 为权威总纲） |
| `research/` | 背景调研、文献依据（第一入口：`knowledge_hub.md`） |
| `motivation/` | 动机场景、端到端测试（脚本→`benchmarks/`，计划→`plans/`，结果→`results/`） |
| `feasibility/` | 组件、环境、脚本可用性验证（不承担实验大纲职责） |
| `experiments/` | 正式研究实验（方法有效性验证） |
| `code/` | 可复用工程代码 |
| `figures/` | 图资产（架构图、实验图、绘图脚本、审计） |
| `opening/` | 开题报告、PPT、飞书、文献 |
| `learning/` | 学习讲解材料 |
| `notes/` | 沟通记录、待确认问题 |

进入子目录前先读该目录的 `AGENTS.md`（规则），再读 `README.md`（内容）。

## 5. 实验规则

- 正式结果放对应目录：动机 → `motivation/results/`，可行性 → `feasibility/results/`，方法 → `experiments/results/`
- GPU-backed E2E 优先于 CPU/fake；CPU/fake 仅供调试或历史对照
- 每条 CSV 记录 `server_version` 和 `pgvector_version`
- 新实验必须有明确问题、运行命令、CSV 输出、结果解释
- 区分数据生成、序列化、`ray.put`、fan-in、写回等阶段边界
- warm-up 忽略或标注；Python baseline 与 Ray baseline 共享数据读取和写回路径

## 6. 严谨性规则

遵循 `karpathy-guidelines`：不确定就问、先定义可验证目标、做最小实验、每个结论标注来源类型、方向选择先做 fatal-flaws audit。

禁止：
- 凭感觉定题；只用 microbenchmark 支撑完整结论；把 Ray 说成"很慢"无上下文
- 把 Daft/Ray/Lance 产品化适配写成既定事实；因写过 benchmark 就反向寻找论文问题
- **在正式材料（开题报告、论文、PPT、图表）中使用 `RC1/RC2/RC3`、`BL1/BL2`、`Phase 0/1/2/3`、`P0/P1/P2` 等内部代号**（内部工作文档可用缩写）

## 6.5 文献优先设计规则

设计系统/算法/实验方案时，优先从项目 CCF-A 文献清单提取设计模式和策略，不凭空设计。完整方法论见 `research/README.md` §文献优先设计方法论，Baseline 矩阵见 `experiments/plans/baseline_reference.md`。

## 7. 实验结果讲解规则

按七步结构：实验设置 → 实验设计 → 严谨性自检 → 实验数据（基于 CSV）→ 结果解释（事实/推断/待确认/不能声称）→ 对课题含义 → 下一步。禁止把 microbenchmark 包装成完整论文结论。详见 `learning/AGENTS.md`。

## 8. 沟通规则

对外表述：**数据库内置 AI 算子的外部分布式数据处理执行链路优化**。待确认事项见 `notes/communication_notes.md`。

## 9. 更新规则

**影响项目结构、方向、实验结论或关键入口的操作，必须记入 `PROJECT_LOG.md`。**

| 变更类型 | 必须更新 |
|---|---|
| 目录结构变化 | `PROJECT_INDEX.md`、`README.md`、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md`、受影响目录的 `README.md` |
| 实验结论变化 | 结果报告、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md` |
| 方向/题目变化 | `AGENTS.md` §1、`opening/report/opening_report.md`、`opening/feishu/`、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md` |
| 规则变化 | 对应目录 `AGENTS.md`；如影响全局同步根 `AGENTS.md`，记入 `PROJECT_LOG.md` |
| 新增/删除文件 | `PROJECT_INDEX.md`、所在目录 `README.md` |
| 新增/更新图表 | `figures/README.md`、`figures/audit/`；如影响主线同步 `opening/report/` |

## 10. Git 规则

**禁止在 commit message 中添加 Co-Authored-By 或任何形式的 AI 署名。** 所有 commit 的用户署名只能是项目开发者本人。

## 11. 知识库同步

项目有平级 Obsidian LLM Wiki 知识库（`../ai-operator-wiki/`）。项目是知识唯一来源，知识库是编译查询界面。

**触发条件**（满足任一即提醒）：
- 用户在对话中说"记住""记下来""同步到知识库""加到 wiki"等——**立即执行同步**
- 会话中**任何知识文件被创建或修改**（`research/`、`opening/literature/`、`experiments/plans/` 下的 `.md`，或用户指定的新知识路径）——**会话结束前提醒**

**操作指南**——执行同步时读取 `research/knowledge_sync_guide.md`。
