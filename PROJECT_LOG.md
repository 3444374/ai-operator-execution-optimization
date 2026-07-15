# 项目日志

## 2026-07-16 实验计划与开题报告同步更新

- **开题报告对齐实验计划**：6 项修改——
  1. §4.1 新增 Killer Experiment 六组对照（BL1-BL6）定义，明确核心 claim 的验证条件
  2. §4.2 新增"合理默认 vs 诊断工具"区分：逐行调用和无界 in-flight 仅作为诊断工具，不作为论文 §7 方法对照 baseline
  3. §3.2 RC3 扩展：提及 B 系列工程实验和三路写回架构对比（driver / worker-direct / queue-worker）
  4. §4.1 末尾新增 FILTER/COMPLETE 模拟 workload 诚实声明（参照 Orca 合成权重做法）
  5. §6 新增统计严谨性指标（中位数、IQR、重复次数、Ray 重启、随机种子）
  6. §2.3 补上 ColStorEval[50] 引用
- **PROJECT_INDEX.md 同步更新**：§3 新增四个实验计划文件列表、§7 更新 RC3 标题、§8 更新下一步优先级（P0/P1/P2 结构）
- **PROJECT_OUTLINE.md 同步更新**：研究内容 RC3 标题降级为"边界分析与轻量写回优化"、近期优先级改为 P0/P1/P2 三阶段
- 上述修改使开题报告与 `experiments/plans/` 下四份实验计划在 BL 矩阵、baseline 分级、统计规范和 workload 标注上口径一致

## 2026-07-16 实验计划六项评估方法论修正

- **修正四个实验计划文件**，统一遵循从 vLLM/Orca/TurboVecDB/GaussML/FlexPushdownDB 五篇 CCF-A 论文提取的六项评估标准：
  1. **前置依赖声明**（§0）：每个文件写明 P0 必须先完成 vLLM + B 系列，否则所有 baseline 是 suboptimal
  2. **假设先行**（§2）：每个实验段在参数矩阵之前先写"要推翻什么假设"，不是盲目扫参——每个假设标注对应实验段和推翻后的含义
  3. **模型 batch scaling 前置实验**（RC1 §4）：在讨论 batch_size 选择之前，先跑模型自身的 batch_size→吞吐曲线
  4. **FILTER/COMPLETE 诚实标注**（RC1/RC2 §3）：标注为 simulated workload（参照 Orca 合成权重的做法）
  5. **统计规范**（各文件 §10）：重复次数、中位数（不取平均值）、IQR、Ray 状态重置、warm-up 策略、随机种子——全部标准化
  6. **可验证边界**（各文件 §11）："When does it NOT help?" 的每个边界条件对应一个可跑实验点，不是空洞自省
- 修改文件：`data_organization_batching.md`（重写）、`service_scheduling_backpressure.md`（新增 §0/§2/§10，修正 §9/§11）、`sink_writeback_coordination.md`（新增 §0/§2/§10，修正 §9）、`cross_layer_killer_experiment.md`（新增 §0/§2）

## 2026-07-16 实验计划骨架填充 + 评估方法论标准化

- **四个实验计划文件新建**：`experiments/plans/` 下三份研究内容实验计划 + 一份跨层 Killer Experiment 计划。
  - `data_organization_batching.md`（RC1）：Grid search、workload 对比、selectivity-aware 策略、模型 batch scaling 前置实验
  - `service_scheduling_backpressure.md`（RC2）：K_max sweep、routing 策略、adaptive vs static K_max、vLLM baseline 前置实验
  - `sink_writeback_coordination.md`（RC3）：B 系列工程 baseline（UPSERT vs COPY, logged vs unlogged, online vs deferred index）、三路架构对比、sink 对照
  - `cross_layer_killer_experiment.md`（跨层核心）：BL1-BL4 + 联合方案的完整矩阵、代价模型 R²、消融瀑布、跨 workload 泛化、统计严谨性要求
- **评估方法论标准化**：所有四个计划遵循从 vLLM (SOSP 2023)、Orca (OSDI 2022)、TurboVecDB (VLDB 2025)、GaussML (ICDE 2024)、FlexPushdownDB (VLDB 2021) 五篇 CCF-A 论文提取的共同原则——曲线 > 单点、先暴露瓶颈再优化、同硬件公平 baseline、消融拆开、诚实报告边界、统计严谨。
- **实验前置依赖明确**：P0 必须先跑 vLLM 接入 + B 系列写回工程 baseline，否则所有 Grid Search 都基于 suboptimal baseline。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 Baseline 分级重构：移除 strawman C 级

- **Baseline 分级重构**：`experiments/plans/research_design_catalog.md` §10.1-§10.4。
  - 移除 "C 级（Naive）"——row-by-row 调用、无界 in-flight 等故意劣化配置降级为"诊断工具"，只用于 §4 理解瓶颈机制，不作为 baseline 对照。
  - "合理默认配置"（coalesced batch=64、driver 写回）取代旧 C 级作为 §4 动机展示的参照点——这是正常工程师会写的第一版代码，不是 strawman。
  - S/A/B 三级保留：S（文献最优）→ A（工程最优）→ B（单维调优）。§7 方法对照至少包含 A 级。
  - §10.1 新增原则 5："动机展示不用 strawman"。§10.3 检查清单新增两条防 strawman 项。
  - A2.1 baseline 描述从 "Unbounded in-flight" 改为 "Ray 默认行为（无显式 K_max）"。
- **未同步到其他文件**：`baseline_reference.md`、`AGENTS.md` 和 `experiments/plans/README.md` 的 strawman 相关措辞已经合理，无需修改。

## 2026-07-15 研究方案候选目录 + Baseline 设计考量

- **新增研究方案候选目录**：`experiments/plans/research_design_catalog.md`，覆盖三个研究内容和跨层协同优化的 28 个候选方案，每个方案在六个维度（文献支撑、工程可行性、硬件可行性、开源依赖、创新空间、实验可验证性）上评分。
- **方案来源**：基于 57 篇文献清单 + 2026 年 7 月前沿检索（Ray Serve 2025 Custom Router/Async Inference/Autoscaling、NexusSched 两层调度、Multi-Bin Batching 队列理论、MAB 反馈控制、GFS/DARIS 优先级抢占、Arrow Flight Ballista/Spark SPIP、Iceberg v3 Deletion Vectors、COSTREAM/CONCERTO/GRACEFUL Learned Cost Models）。
- **Baseline 设计考量**：目录第 10 节为每个候选方案指定了对应 baseline（文献最优 S 级 / 工程最优 A 级 / 常见实践 B 级 / Naive C 级），并给出实现优先级（P0: COPY deferred index + vLLM 接入）。
- **分阶段路线图**：Phase 0-4，覆盖 2026-07 至 2026-10，Phase 3 的 Killer Experiment（BL1-BL6）是论文核心 claim 的验证点。
- **风险分析**：6 项风险（vLLM 消除外部调度收益 / 单 GPU 限制 / 写回优化边际 / workload 扩展 / Joint Opt 增量 < 10% / PG18.3 平台），每项标注了反证条件。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 写回文献调研 + Baseline 矩阵 + 文献优先设计规则

- **文献清单 v3**：`opening/literature/ai_operator_literature_inventory.md` 从 45 篇扩充至 57 篇，新增写回/持久化方向 12 篇 CCF-A 文献（第六组精读 + E 组补充）。
- **新增实验 Baseline 参考矩阵**：`experiments/plans/baseline_reference.md`，覆盖 GPU 调度侧（6 个）、写回侧（7 个）、数据组织侧（4 个）、跨层决策侧（3 个），所有 baseline 标注来源论文/系统。
- **新增文献优先设计规则（§6.5）**：根 `AGENTS.md` 加入"系统/算法/实验方案设计时，优先从 CCF-A 文献提取设计模式"的规则。完整方法论写入 `research/README.md` §文献优先设计方法论。
- **idea-evaluator 评估**：课题方向 Accept with Revisions，无 CRITICAL 缺陷，paradigm-shift probe 4/4 yes。五项调整建议已记录在对话中。
- 同步更新：`AGENTS.md` §6.5、`research/README.md`、`experiments/plans/README.md`、`experiments/plans/baseline_reference.md`（新建）。

## 2026-07-13 制图脚本目录归位

- 将 `code/scripts/make_chain_breakdown_figures.py` 迁移到 `figures/scripts/make_chain_breakdown_figures.py`。
- 明确 `code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口；绘图、图表复现和素材筛选脚本统一放入 `figures/scripts/`。
- 同步更新 `code/AGENTS.md`、`code/README.md`、`code/scripts/README.md`、`figures/scripts/README.md` 和 `figures/learning/README.md`，避免后续实验代码目录与图资产目录混用。

## 2026-07-13 图资产规则沉淀

- 根据今天系统架构图和实验数据图的多轮修改经验，扩展 `figures/AGENTS.md` 为项目级图表长期规则文件。
- 规则中明确：论文级核心图先用 `figure-designer` 判断图型和版式；投稿/论文级质检可用 `nature-figure`；报告转 PPT 可用 `nature-paper2ppt` 或 `ppt-master`；实验数据图优先用 Python + Matplotlib / Seaborn 从 CSV 可复现生成。
- 补充系统架构图常见返工点：箭头遮挡、编号越界、模块未对齐、框内内容不规整、观测层和执行层割裂、图文术语不一致。
- 补充实验图规则：哪些数据值得画图，哪些只适合表格或文字；正式图必须标注数据来源、warm-up 处理、证据层级和不能声称的结论。
- 在 `PROJECT_INDEX.md` 顶部补充图资产规则入口，提醒后续新增、修改、迁移或审查图表前先读 `figures/AGENTS.md`。

## 2026-07-13 项目目录一致性复核

- 复核根目录、`overview/`、`research/`、`motivation/`、`learning/`、`opening/` 和 `figures/` 中与当前开题方向相关的入口文件。
- 将 `overview/project_outline.md` 从旧的“数据库内置 AI 算子外部执行链路”口径重写为“数据库驱动 AI 工作负载的分布式数据执行与存储协同优化”口径。
- 同步更新 `AGENTS.md`、`PROJECT_INDEX.md`、`research/literature_and_evidence_review.md`、`research/existing_ai_operator_execution_chains.md`、`motivation/plans/integration.md` 和 `motivation/results/README.md` 中的旧表述。
- 对 `motivation/results/pg18_4_fake/system_profile.md` 与 `motivation/results/fake_cpu/analysis.md` 增加当前口径说明，保留历史实验语境，但明确真实瓶颈归因应优先引用 GPU-backed 结果。
- 本次复核只调整会影响项目规划、阅读入口和方向判断的文件；历史日志和旧实验过程记录不做大面积改写。

本文件记录项目级简要操作，便于日后复盘方向、入口和关键材料调整。详细实验日志仍放在对应结果目录；开题材料的详细修改记录见 `opening/logs/project_log.md`。

## 2026-07-13 开题主线调整为数据库驱动 AI workload

- 根据用户确认的判断，将开题题目从“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”调整为“面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究”。
- 同步更新项目级方向口径：数据库 AI 算子主要作为 workload 入口和验证场景，研究主体调整为 Daft/Arrow 数据组织、Ray 执行调度、GPU 模型服务和 Lance / pgvector / PostgreSQL sink 之间的数据执行与存储协同。
- 同步修改 `README.md`、`PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`AGENTS.md`、`overview/current_direction_and_plan.md`、`motivation/plans/integration.md` 以及 opening 相关源稿，避免项目规划与开题报告割裂。
- 已生成新的本地飞书源稿 `opening/feishu/opening_report_wiki.md`；飞书写入时 `lark-cli` 在用户目录刷新锁文件处返回 `Access is denied`，提升权限重试被自动审批拒绝，需后续获得权限后再同步线上 wiki。

## 2026-07-12 根目录总纲与项目日志

- 新增 `PROJECT_OUTLINE.md`，作为根目录项目总纲入口，汇总当前题目、研究内容、实验主线、关键证据、近期优先级和同步规则。
- 新增 `PROJECT_LOG.md`，作为项目级简要操作日志，用于记录跨目录、影响项目方向或入口结构的调整。
- 后续如果开题报告、实验主线、项目方向或关键入口发生变化，需要同步更新 `PROJECT_OUTLINE.md` 和本日志。

## 2026-07-12 实验主线入口调整

- 将项目实验主线入口从 `feasibility/guide.md` 调整到 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md`。
- 明确 `feasibility/` 只负责组件、环境和脚本可用性验证，不承担当前实验大纲、开题主线或 GPU-backed 性能结论职责。

## 2026-07-12 开题与项目规划双向同步

- 明确开题报告和项目规划不是单向关系：开题报告基于项目进展撰写；开题题目、研究内容、技术路线或侧重点调整后，也会反向影响项目规划、实验优先级和对外口径。
- 项目入口文档需要与 `opening/report/opening_report.md` 保持一致，不能长期出现不同方向。

## 2026-07-12 开题报告与飞书内容复核

- 按当前 `PROJECT_OUTLINE.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md` 复核开题报告与飞书源稿。
- 确认 `opening/report/opening_report.md` 当前主线基本合适：正式证据优先引用真实 GPU-backed 结果，PG18.4 / fake / CPU 结果有边界说明。
- 清理 `opening/feishu/opening_report_wiki.md` 的本地源稿说明，避免发布到飞书后出现工作流元话语。
- 补充飞书后续计划：后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地同构预演写成正式平台结论。
- 修正 `motivation/results/README.md` 中 GPU-backed 结果入口的过时措辞。

## 2026-07-12 实验结论写作标准

- 根据用户反馈，将 `learning/AGENTS.md` 的实验讲解标准提升为项目级实验结论写作参照。
- 更新 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md` 和 `opening/work_rules.md`，要求实验结论、数据分析、开题可行性分析和飞书实验摘要都说明实验目的、链路流程、参数含义、数据来源、结果读法、不能证明什么、结论类型和下一步验证。
- 后续正式报告可以比学习材料更凝练，但结论边界和分析精细程度不能低于 `learning/AGENTS.md` 的要求。

## 2026-07-12 开题实验飞书页与 PPT 生成

- 新增 `opening/feishu/motivation_feasibility_wiki.md`，按真实 GPU-backed 证据、fake/CPU 历史预研、可行性验证边界和下一步实验组织动机测试与可行性测试内容。
- 使用 user 身份覆盖写入动机测试与可行性测试飞书 wiki：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`，飞书返回成功并生成 5 个 Mermaid whiteboard。
- 基于学校 PPT 模板生成开题汇报 PPTX：`opening/slides/opening_defense_20260712.pptx`，内容来自开题报告、GPU-backed 动机实验和当前项目总纲。
- 已将 PPTX 以 user 身份导入为飞书在线幻灯片：`https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg`。
# 2026-07-14 pgvector(384) writeback comparison

- Updated `code/scripts/postgres_ai_operator_profile.py` so `--setup --embedding-dim 384` creates `document_embeddings.embedding_vector` as `vector(384)`.
- Ran the same GPU-backed Ray actor chain for no writeback, JSON text writeback, and pgvector `vector(384)` writeback.
- Added result report and CSV under `motivation/results/gpu/`.
- Added report-main figure `figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png`.
- Updated opening report, learning walkthrough, figure indexes, and result indexes. Boundary: PG18.4 local rehearsal, not PostgreSQL 18.3 internal platform.

# 2026-07-14 合并 agent/postgres18-local-profile 分支并全项目校准

- 将 `origin/agent/postgres18-local-profile` 合并到 `main`，恢复 `opening/` 开题材料目录。
- 分支带来的重构：`validation/` → `feasibility/`，`motivation/` 脚本 → `benchmarks/`、设计文档 → `plans/`、结果按 `fake_cpu/cpu/gpu/pg18_4_fake` 分类。
- 新增目录：`deploy/`、`experiments/`、`figures/`、`learning/`、`opening/`、`projects/`。
- 创建 `CLAUDE.md` 作为 Claude Code 环境规则入口，导入全部 `AGENTS.md`。
- 全项目文档路径校准（12 个文件）：
  - 根 `AGENTS.md`：§3 证据更新为 GPU-backed 结果，§4 目录加新结构，§5 实验规则更新。
  - 根 `README.md`：目录树重写、标题对齐 `PROJECT_OUTLINE.md`、证据和运行命令更新。
  - `PROJECT_INDEX.md`：全文重写，所有路径更新，新目录入口，当前证据优先级。
  - `overview/current_direction_and_plan.md`、`overview/project_outline.md`：加弃用声明，指向根 `PROJECT_OUTLINE.md`。
  - `motivation/results/README.md`：从扁平文件列表重写为子目录结构。
  - `feasibility/benchmarks/README.md`：命令路径和脚本引用全部更新。
  - `opening/ppt_rules.md`：图表规则重写，引用 `figures/` 为权威来源，Python+Matplotlib 优先于 ECharts。
  - `opening/work_rules.md`：过期引用更新。
  - `experiments/AGENTS.md`：新增 `karpathy-guidelines` 和图表 skill 引用。
- 镜像同步规则：`CLAUDE.md` 和 `AGENTS.md` §9 包含相同的 6 行变更→更新清单，互相指向对方。
