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

本目录用于同步准备开题报告、开题汇报 PPT 和飞书进度汇报。当前阶段优先维护本地 Markdown 源稿和飞书文档，不急于生成 DOCX。后续顺序是：本地 Markdown -> 飞书文档补全 -> PPT -> PPT 同步飞书 -> 最终 DOCX 生成。

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
| 开题报告 | `report/opening_report.md` | **2026-08-07 本地冻结稿，含最终统一三臂表与四张核心图** |
| 开题叙事与 Claim Matrix | `claim_matrix.md` | **2026-08-07 已冻结，报告/PPT/实验准入的当前依据** |
| 开题 PPT 设计 | `slides/opening_defense_v6_design.md` | 28 页冻结设计与逐页 claim/source 约束 |
| 开题 PPTX | `slides/opening_defense_20260807_v6.pptx` | **由 v5 模板继承生成；28 页、备注完整、无空 placeholder、overflow test 通过** |
| 开题飞书源稿 | `feishu/opening_report_wiki.md` | 与本地报告完全一致；线上正文 revision 289 与四个图片块已回读通过 |
| 动机测试飞书 wiki 源稿 | `feishu/motivation_feasibility_wiki.md` | 已同步到飞书 |
| 飞书进度汇报 | `feishu/progress_update.md` | 已同步当前进展 |
| 文献精读清单 | `literature/reading_list.md` | 候选清单已补，待精读（笔记全集在 `research/reading_notes/`） |
| 开题精读 Top 15 拷贝 | `literature/top15_reading_notes/` | 开题要求精读的 15 篇笔记自包含快照 |
| GPU 调度与数据放置补充调研 | `research/gpu_scheduler_data_placement_supplement_20260715.md` | 已补，作为策略控制器设计依据与后续精读清单 |
| 本地 PDF 子集索引 | `research/reference/README.md` | 已登记当前已下载的部分论文，非完整文献库 |
| 答辩问答 | `qa_bank.md` | 已扩展 |
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

1. 以 `claim_matrix.md`、本地报告和 v6 PPTX 为冻结口径，不再新增开题 baseline。
2. v6 已完成程序化渲染、画布检查和 Microsoft PowerPoint 真实打开检查。
3. 飞书线上报告与四张核心图已同步并回读；平级 Wiki 目录恢复后执行知识镜像。
4. 导师确认后生成学校模板 DOCX，并恢复开题后实验 backlog。
## 飞书同步目标

后续需要写入飞书的主要目标：

| 飞书文档 | 链接 | 用途 |
|---|---|---|
| 开题报告（新版，当前同步目标） | https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb | 承载开题报告正文最新版，口径为三层上游执行策略、写回瓶颈判定和端到端评价 |
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
