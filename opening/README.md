# 开题工作区

## 2026-08-21 文献与研究问题基线

- 开题 Top 15 已升级为 15/15 严格 CCF-A 正式 research paper；权威清单见
  `../research/top15_ranked_papers.md`，自包含快照见
  `literature/top15_reading_notes/`。
- `../research/reading_notes/` 当前有 49 篇历史文献笔记，现按泛读库管理；新的权威精读笔记进入
  `../research/精读文献笔记/`。当前共有 16 篇主笔记和 134 张论文原图裁剪件；其中新增 Kalypso
  原稿已从下载目录原件逐字节恢复，作为 arXiv 核心补充，不进入现有 Top 15、十五篇速览或已定稿正文。
  `../research/reference/` 当前工作区有 6 份可解析 PDF 实体（Galois、Abacus、Palimpzest、Sema、
  Parrot、Kalypso）；历史题录继续由索引保留。
- Tutorial、Companion、CIDR、MLSys、arXiv 统一放在核心补充层，不写成
  CCF-A full research。
- 开题保持两项研究内容：数据组织；调度与提交控制。代价估计升级为两项内容
  的共同使能组件，多模态仍是泛化验证。
- 当前对外问题层级为两项核心研究问题和一个支撑问题。第一条总动机是记录数不能代表
  分阶段 AI 工作量，对应数据组织；第二条总动机是单一静态量或局部指标不能代表当前
  可调度状态，其下区分配置层、模型服务层和 Job 层，对应提交、路由与多 Job 调度。
  代价估计仍是共同使能组件，图像仍用于跨模态验证，不增加平级研究问题。

本目录用于准备开题报告、开题汇报 PPT 和进度材料。当前 26 页对外答辩 PPT 已按“入口变化—
执行假设—分层研究现状—研究空白—动机—方法”重构；中文 Markdown 报告保持项目事实基线。
普通飞书云文档等待用户对指定文档 URL 的最终覆盖授权，
Wiki 不同步。所有发布面仍以本地权威稿和 Claim Matrix 为唯一输入。

开题材料不是独立于项目的展示层。当前报告确定的题目和研究内容会反向影响 `overview/` 中的阶段规划、`motivation/` 中的后续实验设计以及项目级 README / PROJECT_INDEX 的方向说明。修改开题题目、研究内容或实验边界时，需要同步检查这些项目入口，避免开题材料和项目主线割裂。

同步关系是双向的：开题报告要根据项目当前进展、实验事实和后续规划来写；后续如果开题报告因为导师反馈、实验新结果或题目收敛而调整方向，项目规划、实验优先级和文档入口也要跟着调整。不能只把 opening 当成最终展示目录。

## 当前汇报主线

当前对外主线：

```text
数据库正在成为 AI 任务的入口
  -> Database 与 Model Service 之间出现 AI Data Execution Layer
  -> 动机实验一：固定行数不能表示分阶段 AI 工作量
  -> 研究内容一：工作描述与数据组织
  -> 动机实验二：单一要素不能表示当前运行状态
  -> 动机实验三：总体效率不能表示各 Job 的完成进度和服务结果
  -> 研究内容二：固定准入范围内的提交、路由和多 Job 调度
  -> 用文本与图像检验工作量与共享份额抽象，代价估计共同辅助决策
```

一句话口径：

> 本课题研究数据库触发 AI 算子后、数据进入模型服务前的 AI 数据执行层：按 token 或图像帧描述工作量，并依据可用容量和运行状态控制提交、路由与多作业共享；不修改数据库内核、vLLM 调度器或模型内部实现。

## 目录结构

| 路径 | 作用 |
|---|---|
| `AGENTS.md` | 本目录长期规则 |
| `work_rules.md` | 跨报告、PPT、飞书、文献和答辩材料的变更路由清单；联动任务时读取 |
| `ppt_rules.md` | PPT 创建、增量编辑、导出和实际打开检查清单；PPT 任务时读取 |
| `report/` | 开题报告正文与 Word 版本材料 |
| `slides/` | PPT 源稿、讲稿备注、PPTX 输出 |
| `feishu/` | 飞书进度汇报稿 |
| `literature/` | 开题文献精读清单（`reading_list.md`）+ 既有 Top 15 笔记快照（`top15_reading_notes/`）；泛读、精读和 PDF 分别在 `research/reading_notes/`、`research/精读文献笔记/` 与 `research/reference/` |
| `literature/top15_reading_notes/` | 2026-07-29 Top 15 笔记自包含历史快照（含 figs）；不随新精读库自动更新 |
| `assets/` | 图、SVG、表格、流程图、模板素材说明 |
| `logs/` | 非实验类 project log |

## 当前需要维护的材料

| 材料 | 主文件 | 状态 |
|---|---|---|
| 第一性原理复审 | `first_principles_reassessment_20260808.md` | 当前方法、实验与图的调整依据 |
| 答辩内容大纲 | `opening_defense_outline_20260808.md` | **当前权威入口：20 页主讲内容大纲；每页已补齐核心问题、内容块、证据、页面结论与转场，并建立背景—动机—研究内容—实验对应表** |
| 开题报告 | `report/opening_report.md` | 以用户确认的第一、二章为基线微调：第三章按三个动机问题、研究目标和两项研究内容组织，第四章区分研究方案、初步结果与可行性；正文为学校模板七部分、16 张图片和 53 条按首次出现顺序编号的参考文献 |
| 开题报告 Word 版 | `report/数据库_AI_负载的执行优化与调度研究_开题报告.docx` | 严格沿用学校 Word 模板的 A4 版心、封面、页脚页码和签字页；正文按宋体/Times New Roman、小四、1.5 倍行距排版，一级标题四号黑体、其余标题小四黑体、图注五号黑体，含 16 张正文图片和 53 条按首次出现顺序编号的参考文献；学号、专业和指导教师留待填写 |
| 开题报告 QA | `report/opening_report_20260824_qa.md` | 审查七部分结构、三个动机问题到两项研究内容的因果映射、代价估计角色、表格范围、16 张图和 53 条参考文献；精读文献覆盖与题录连续性已复查，2026-08-20、2026-08-12 版本保留为历史审查记录 |
| 开题答辩 QA 手册 | `report/opening_defense_qa/opening_defense_qa.tex`（同目录本地 PDF） | 83 题双层回答；其中 39 题详细展开总体方案、数据组织以及固定容量下的提交、路由与多 Job 调度，另含 7 条连续追问链、已有工作差异表、术语白话表、关键数字和现场表述红线；设计说明也收纳在同名目录 |
| 开题报告专用图片 | `report/figures/` | 当前正文引用 5 张背景/方案图和 11 张数据图；权威可编辑源在 `../figures/architecture/editable/`，数据图源在 `../figures/opening_figure_set/` 与 `../figures/data/report_main/` |
| 开题叙事与 Claim Matrix | `claim_matrix.md` | 2026-08-09 已复审；实验准入、主张等级、禁止外推、材料 readiness 与总目标完成条件的当前依据 |
| 开题 PPT 设计 | `slides/opening_defense_v6_design.md` | 28 页历史设计底稿；当前 26 页 v9 以 v5 演示经验和学校模板为基础，优先服从对外叙事 |
| 开题 PPTX | `slides/opening_defense_20260812_v9.pptx` | 当前 26 页对外答辩版；删去重复文献页并合并验证与结尾内容，QA 见 `slides/opening_defense_20260812_v9_qa.md` |
| 开题飞书历史快照 | `feishu/opening_report_wiki.md` | **已过期，禁止同步**；仍含首轮failed-feeding数字。当前权威正文为`report/opening_report.md`，用户恢复云文档工作后再由权威正文重新生成同步源 |
| 动机测试飞书 wiki 源稿 | `feishu/motivation_feasibility_wiki.md` | 已同步到飞书 |
| 飞书进度汇报 | `feishu/progress_update.md` | 已同步当前进展 |
| 文献精读清单 | `literature/reading_list.md` | 候选清单已补；未来全文精读进入 `research/精读文献笔记/` |
| 开题 Top 15 历史快照 | `literature/top15_reading_notes/` | 15 篇既有笔记的自包含快照，不代表新精读库完成状态 |
| GPU 调度与数据放置补充调研 | `research/gpu_scheduler_data_placement_supplement_20260715.md` | 已补，作为策略控制器设计依据与后续精读清单 |
| 本地 PDF 子集索引 | `research/reference/README.md` | 已登记当前已下载的部分论文，非完整文献库 |
| 答辩问答 | `qa_bank.md` | 2026-08-09 已完成四部件、baseline provenance、两/四 Job、K256/K512、sink 与跨模态攻击面审计 |
| 材料同步日志 | `logs/project_log.md` | 初版已建 |

## 与项目其他目录的关系

- 实验事实优先来自 `motivation/results/`。
- 实验讲解和术语口径参考 `learning/experiment_walkthrough.md`。
- 文献与外部系统证据参考 `research/`。
- 当前方向和阶段计划参考 `overview/current_direction_and_plan.md`。
- 开题材料中的实验结论必须回到真实 CSV / 报告，不能只引用聊天结论。
- 开题报告中收敛出的题目、研究内容和评价边界需要回写项目入口文档，作为后续实验和规划的约束。
- 如果开题报告改变了研究内容或侧重点，需要同步检查 `motivation/plans/workloads.md`、`motivation/plans/integration.md` 和后续实验优先级，避免实验继续围绕旧问题展开。

## 下一步

1. 本地权威报告已按两项研究内容和目标/当前双路径架构完成重构；下一步先完成 LOTUS 1.2.4 `sem_map` 迁移和 PostgreSQL planner-visible 最小实现，再更新实现状态图。
2. SQuAD/ShareGPT replacement、原生单 job、两 job 最小因果与四 job 扩展均已完成；停止增加开题 baseline、offset、weight 或更多 job 数扫描。
3. v9 只保留学校页眉、配色和身份识别，不逐框仿制模板；页数和版面服从现场叙事。
4. Claim Matrix、问答和实验状态继续用于核对正文主张。当前 PPT 与飞书发布面尚未同步本轮报告；只有在用户恢复相应工作后，才从本地权威稿重新生成并执行差异审查。
## 飞书发布面（等待指定 URL 覆盖授权）

以下链接用于识别既有发布面；普通云文档只有在用户明确授权覆盖该 URL 后才写入：

| 飞书文档 | 链接 | 用途 |
|---|---|---|
| 开题报告（revision 289历史发布面） | https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb | 已落后于当前本地报告；恢复同步时从`report/opening_report.md`重新生成并差异审计 |
| 开题报告与开题汇报（旧版，已过时） | https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink | 旧版，保留作为历史参考 |
| 动机测试与可行性测试 | https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink | 承载动机实验、可行性实验、分阶段性能剖析和实验结论边界 |
| 开题汇报飞书幻灯片 | https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg | 旧版在线幻灯片，当前内容和形式先作废，后续需基于新版报告重做 |

同步规则见 `feishu/README.md`。
## 可参考 skill

开题工作中，文献调研优先参考 `nature-academic-search`；需要系统综述或研究问题收敛时参考 `deep-research` 和 `academic-research-suite`；PPT 制作参考 `ppt-master` 和 `nature-paper2ppt`；写作润色参考 `humanizer`；流程与学术边界参考 `vibe-research-workflow` 和 `karpathy-guidelines`；飞书同步使用 `lark-doc`，遇到 Base 或飞书幻灯片再分别使用 `lark-base`、`lark-slides`。

这些 skill 只是方法工具。后续执行时先看本项目的真实材料和当前目标，再决定是否调用对应 skill，不能为了调用 skill 而调用。

具体触发规则见 `AGENTS.md`。
## 导航入口

准备开题材料时，先看：

```text
opening/navigation.md
```

它说明需要项目内容、实验结果、文献、PPT 素材、飞书同步信息时分别去哪里找，也说明报告、PPT、飞书版之间如何保持同步。
## 2026-07-15 飞书同步历史记录

> 本节仅记录当时的飞书同步面，已被本 README 上方当前入口取代，不是当前发布状态。

开题报告已同步到飞书新文档：
https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb

同步内容包括：完整七章正文、三层上游执行策略图、运行时策略闭环图，
以及 GPU-backed 动机实验图（粒度对比、阶段时延、endpoint 对比、pgvector 写回对比）。
旧版 wiki (GCxowlVJbinzgRkoHDmc06cSn9J) 保留作为历史参考，不再更新。

中文 Markdown 报告当前为七部分正文、16 张图和 53 条参考文献，并已完成本地结构、主张和图文审查。本轮只把用户提供的桌面 PPT 作为实验角色参照，未修改 PPTX；旧版飞书 wiki 保留历史版本，不再作为当前开题报告同步面。
