# 开题工作区

## 2026-07-29 文献与研究问题基线

- 开题 Top 15 已升级为 15/15 严格 CCF-A 正式 research paper；权威清单见
  `../research/top15_ranked_papers.md`，自包含快照见
  `literature/top15_reading_notes/`。
- `../research/reading_notes/` 当前有 49 篇权威精读笔记，
  `../research/reference/` 当前有 21 份可解析 PDF，Top 15 PDF 15/15 齐全。
- Tutorial、Companion、CIDR、MLSys、arXiv 统一放在核心补充层，不写成
  CCF-A full research。
- 开题保持两项研究内容：数据组织；调度与提交控制。代价估计升级为两项内容
  的共同使能组件，多模态仍是泛化验证。
- 三个研究问题为：最小饱和压力与 transient ramp、相同 work 的数据组织、
  多 job shared credit/fairness。

本目录用于准备开题报告、开题汇报 PPT 和进度材料。当前阶段只维护本地 Markdown、
紧凑实验数据和待画图合同；按用户要求暂停新图、PPT、飞书云文档覆盖、Wiki 与 DOCX。
恢复这些产物时仍以本地权威稿和 Claim Matrix 为唯一输入。

开题材料不是独立于项目的展示层。当前报告确定的题目和研究内容会反向影响 `overview/` 中的阶段规划、`motivation/` 中的后续实验设计以及项目级 README / PROJECT_INDEX 的方向说明。修改开题题目、研究内容或实验边界时，需要同步检查这些项目入口，避免开题材料和项目主线割裂。

同步关系是双向的：开题报告要根据项目当前进展、实验事实和后续规划来写；后续如果开题报告因为导师反馈、实验新结果或题目收敛而调整方向，项目规划、实验优先级和文档入口也要跟着调整。不能只把 opening 当成最终展示目录。

## 当前汇报主线

冻结主线：

```text
数据库正在成为 AI workload 的入口
  -> Database 与 Model Service 之间出现 AI Data Execution Layer
  -> 先标定最小饱和 work，再研究 work-unit 的 balance/locality
  -> 以同上限 strong static 约束 state-aware admission/routing/multi-job
  -> 用文本与图像检验 work/credit 抽象边界，代价估计共同辅助决策
```

一句话口径：

> 本课题研究数据库触发 AI 算子后、数据进入模型服务前的 AI 数据执行层：按 token/frame work 构造 work-unit，并依据容量和运行状态控制提交、路由与多作业共享；不修改数据库内核、vLLM 调度器或模型 kernel。

## 目录结构

| 路径 | 作用 |
|---|---|
| `AGENTS.md` | 本目录长期规则 |
| `work_rules.md` | 开题工作的任务组织和目标管理规则 |
| `ppt_rules.md` | 开题 PPT 制作规则 |
| `report/` | 开题报告正文与 Word 版本材料 |
| `slides/` | PPT 源稿、讲稿备注、PPTX 输出 |
| `feishu/` | 飞书进度汇报稿 |
| `literature/` | 开题文献精读清单（`reading_list.md`）+ 开题精读 Top 15 拷贝（`top15_reading_notes/`）；单篇精读笔记与 PDF 全集已迁至 `research/reading_notes/` 与 `research/reference/` |
| `literature/top15_reading_notes/` | 开题要求精读的 Top 15 篇笔记拷贝（自包含快照，含 figs）；权威版在 `research/reading_notes/` |
| `assets/` | 图、SVG、表格、流程图、模板素材说明 |
| `logs/` | 非实验类 project log |

## 当前需要维护的材料

| 材料 | 主文件 | 状态 |
|---|---|---|
| 第一性原理复审 | `first_principles_reassessment_20260808.md` | 当前方法、实验与图的调整依据 |
| 答辩内容大纲 | `opening_defense_outline_20260808.md` | **当前权威入口：19 项 take-away、必要数据、claim 边界、实验与图表合同；暂不制作 PPT** |
| 开题报告 | `report/opening_report.md` | 2026-08-09 本地证据口径已对齐；已写入 replacement、原生单 Job、5 s 两 Job 与四部件实现边界，暂不同步云文档 |
| 开题叙事与 Claim Matrix | `claim_matrix.md` | 2026-08-09 已复审；实验准入、主张等级与禁止外推的当前依据 |
| 开题 PPT 设计 | `slides/opening_defense_v6_design.md` | 28 页历史设计底稿；当前暂停 PPT 制作 |
| 开题 PPTX | `slides/opening_defense_20260807_v6.pptx` | v5 模板继承的待替换底稿；已有 QA 不代表新内容已冻结 |
| 开题飞书历史快照 | `feishu/opening_report_wiki.md` | **已过期，禁止同步**；仍含首轮failed-feeding数字。当前权威正文为`report/opening_report.md`，用户恢复云文档工作后再由权威正文重新生成同步源 |
| 动机测试飞书 wiki 源稿 | `feishu/motivation_feasibility_wiki.md` | 已同步到飞书 |
| 飞书进度汇报 | `feishu/progress_update.md` | 已同步当前进展 |
| 文献精读清单 | `literature/reading_list.md` | 候选清单已补，待精读（笔记全集在 `research/reading_notes/`） |
| 开题精读 Top 15 拷贝 | `literature/top15_reading_notes/` | 开题要求精读的 15 篇笔记自包含快照 |
| GPU 调度与数据放置补充调研 | `research/gpu_scheduler_data_placement_supplement_20260715.md` | 已补，作为策略控制器设计依据与后续精读清单 |
| 本地 PDF 子集索引 | `research/reference/README.md` | 已登记当前已下载的部分论文，非完整文献库 |
| 答辩问答 | `qa_bank.md` | 2026-08-09 已完成四部件、baseline provenance、overlap、2→4+ Job、K256/K512、sink 与跨模态攻击面审计 |
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

1. SQuAD/ShareGPT replacement、原生单 job 与 5s guaranteed-overlap 两 job 最小证据均已完成；停止增加开题 baseline、offset、weight 或 4+ job 扫描。
2. 依据 `opening_defense_outline_20260808.md` 整理动机—挑战—设计—证据边界和待画图数据合同；当前暂不实际画图。
3. 完成 Claim Matrix、问答、实验状态与实现边界的一致性审计；用户已明确豁免 Wiki，同步云文档和制作 PPT 均暂停。
4. 后续获准绘图时只做 A/C 标签级重绘与 F/H 首次生成；B、WorkDescriptor 总览、D、E 不重画，G 无结果且不画。论文阶段再恢复同上限 phase-change、weighted/SLO、图像动态与 held-out cost 验证。
## 飞书发布面（当前同步暂停）

以下链接只用于识别既有发布面；当前均不是可直接覆盖的同步目标：

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
# 2026-07-15 Status Note

开题报告已同步到飞书新文档：
https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb

同步内容包括：完整七章正文、三层上游执行策略图、运行时策略闭环图，
以及 GPU-backed 动机实验图（粒度对比、阶段时延、endpoint 对比、pgvector 写回对比）。
旧版 wiki (GCxowlVJbinzgRkoHDmc06cSn9J) 保留作为历史参考，不再更新。

PPT 尚未按当前报告重做；旧版飞书 wiki 保留历史版本，不再作为当前开题报告同步面。
