# 开题答辩 v7 图文一致性审计

日期：2026-08-12

对象：`opening_defense_20260812_v7.pptx`（20 页）

依据：`opening/claim_matrix.md`、`opening/opening_defense_outline_20260808.md`、`figures/audit/opening_required_data_figures_20260810.md`

## 审计结论

- 第 5–19 页全部使用 `figures/opening_figure_set/main_png` 的 14 张主讲图，且能支撑对应页面结论；比较合同与 Claim Matrix 一致。构建脚本不再直接引用旧 `report_main` 或旧 architecture PNG。
- 第 2、3 页当前为文字页，标题和正文已收窄到“AI 算子进入数据库工作流”和“形成新的外部执行链路”。逻辑可以成立，但页面偏空，建议用下列两张背景结构图增强；它们不是缺失的实验论据。
- 第 4、10、20 页承担文献归类、设计要求和贡献收束，不需要强行加入数据图；第 19 页使用已确认的研究基础与后续工作计划图。第 4、10 页此前的图文叠放已取消。
- 当前 20 页没有“缺图就无法成立”的页面。第 2、3 页为建议补图，第 4 页为可选增强；补图不改变后续证据链。

## 逐页映射

| 页 | 页面结论 | 当前视觉 | 一致性 | 备注 |
|---:|---|---|---|---|
| 1 | 题目与研究边界 | 学校封面 | 完全一致 | 无需数据图 |
| 2 | AI 算子进入数据库工作流 | 文字页 | 逻辑一致、建议补图 | 产品名称由正文与讲稿补充；建议增加 AI-Native 演进与控制点图 |
| 3 | AI 算子形成外部执行链路 | 文字页 | 逻辑一致、建议补图 | 建议增加传统算子与外部 AI 执行链路对照图 |
| 4 | 相邻层已有强工作但缺统一闭环 | 文献按层归类文字页 | 完全一致 | 不把结构图和文献清单混画 |
| 5 | 研究对象位于数据库与模型服务之间 | `P05_研究空白_AI数据执行层` | 完全一致 | 两项研究内容与共同代价估计均出现 |
| 6 | 同一任务在不同执行图中呈现不同供给状态 | `opening_text_baseline_evidence_map` | 完全一致 | 左右两轨不跨 panel 排名；右轨不混入 Project |
| 7 | 行数与静态上限不能代表 work/state | `opening_motivation_work_state` | 完全一致 | 三个 panel 只作机制拼图，不跨合同排名 |
| 8 | 图像 work 必须分阶段 | `opening_image_stage_aware_evidence` | 完全一致 | prepare/transfer/model 与 active-window 均覆盖 |
| 9 | 原生执行图存在 Short/Long 多 Job 干扰 | `opening_native_fourjob_normalized_impact` | 完全一致 | 只做路径内部归一化，不作绝对框架排名 |
| 10 | 四项设计要求由现象导出 | 两行映射文字页 | 完全一致 | 不与第 11 页系统图重复 |
| 11 | AI 数据执行层形成前向组织、反向状态闭环 | `P11_系统架构_数据组织与状态调度闭环` | 完全一致 | cost 同时服务组织与调度 |
| 12 | WorkDescriptor 连接组织与调度 | `P12_研究内容一_WorkUnit与数据组织` | 完全一致 | 文本/图像字段和消费者关系均出现 |
| 13 | 数据组织策略依赖服务压力与 locality | `opening_work_organization_regime_v2` | 完全一致 | 吞吐、cache hit 和策略关系同页呈现 |
| 14 | 固定总上限，仅按活跃集调整份额与释放顺序 | `P14_研究内容二_状态感知提交与多作业调度` | 完全一致 | 图中包含 fallback、credit、fair queue、snapshot |
| 15 | 共享调度存在效率—隔离—公平权衡 | `opening_multijob_interference_tradeoff` | 完全一致 | full/quarter 是控制，static/shared 是同上限 A/B |
| 16 | 代价估计以排序和决策损失验收 | `opening_cost_model_decision_quality_v2` | 完全一致 | 20 context 点云、pairwise、均值与最坏 regret 均覆盖 |
| 17 | 图像 baseline 必须分能力、诊断和正式比较 | `opening_image_baseline_evidence_map` | 完全一致 | 12K 与 120K 分开解释；blocked 路径无伪造数值 |
| 18 | 图像四 Job 同样出现任务级干扰 | `opening_image_fourjob_normalized_impact` | 完全一致 | Daft Built-in、Ray Data、Project static/shared 均出现 |
| 19 | 后续实验逐层形成因果闭环 | `P19_研究基础与后续工作计划` | 完全一致 | 使用已确认的概念图收束研究基础、后续工作与评价原则 |
| 20 | 两项研究内容、共同使能与跨模态验证 | 贡献总结页 | 完全一致 | 无需新增图 |

## 可选替换图提示词

### 第 2 页：AI-Native Data Infra 演进与本课题控制点

> 生成一张 16:9、白底、顶会系统论文风格的中文矢量架构图，沿用现有图的深蓝、亮蓝、橙色、青绿色配色，线条 1.5–2 pt，圆角矩形、无渐变、无阴影、留白充分。横向分为四层：① 数据与 SQL 入口（结构化表、文本、图像）；② 数据库 AI 算子（AI_COMPLETE、AI_EMBED、AI_CLASSIFY、AI_FILTER）；③ 外部 AI 数据执行层（Work Unit 构造、状态快照、提交/路由、多 Job credit，使用蓝色大边框突出并标注“本课题研究边界”）；④ 模型服务与 GPU（vLLM / typed GPU actor）及结果回写。顶部用一条克制的演进箭头表示“SQL Analytics → Vector/ML → SQL + LLM / Multimodal”，不要放产品 Logo，只在页脚小字列 Snowflake Cortex AISQL、BigQuery ML/AI、Oracle AI Vector Search、pgai/pgvector 作为实例。所有箭头必须表示真实数据或反馈方向；反馈从模型服务的 running/waiting/KV/completion 返回执行层。避免 3D、发光、装饰性图标和密集小字。

### 第 3 页：传统算子与 AI 算子的执行假设对照

> 生成一张 16:9、白底、顶会系统论文风格的中文对照图，保持深蓝、橙色、青绿色体系。左右两条等宽流水线：左侧“传统数据库算子”依次为 rows/selectivity → CPU/I/O operator → result，下面列成本字段 rows、cardinality、CPU、I/O；右侧“外部 AI 算子”依次为 records → request organization → CPU prepare/tokenize → queue/admission → GPU model service → result gather，下面用四个小卡片列 work 字段 token/frame、encoded bytes、prefix locality、output work，以及 state 字段 running/waiting、KV、completion rate、Job remaining work。中间用醒目的但克制的结论框写“固定行数、固定 batch、固定并发不能单独描述 AI 执行”。强调右侧多阶段和反馈箭头，不修改模型内部 batching。线条清晰、字号可在 PPT 100% 缩放阅读，不使用误差线、点云或数据条。

### 第 4 页（可选）：代表工作分层图

> 生成一张四列文献分类图，不画性能数据：数据库 AI（LOTUS、Galois、GaussML、Cortex AISQL）、数据执行（Ray Data、Daft、NeuStream）、推理服务（Orca、vLLM、Sarathi-Serve、VTC）、代价与决策（Learned Cost Models、GRACEFUL、COSTREAM、Abacus）。每列只放论文/系统名和它拥有的状态，底部用一条跨列缺口标注“数据库 Job 语义—上游数据阶段—模型服务运行状态尚未形成统一闭环”。保持白底、细线、无 Logo、无论文封面拼贴。

## 使用规则

1. 新图必须替换现有图后再次逐页渲染，不得直接叠在原图或长文字上。
2. 图中任何数值必须来自已有正式结果；上述第 2–4 页均为结构/文献图，不应加入未经实验支持的百分比。
3. 新图不改变第 6–18 页已冻结的数据合同，也不改变 Claim Matrix 中“已证明 / 待验证”的边界。
