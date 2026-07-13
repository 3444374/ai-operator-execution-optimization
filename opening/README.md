# 开题工作区

本目录用于同步准备开题报告、开题汇报 PPT 和飞书进度汇报。当前阶段优先维护本地 Markdown 源稿和飞书文档，不急于生成 DOCX。后续顺序是：本地 Markdown -> 飞书文档补全 -> PPT -> PPT 同步飞书 -> 最终 DOCX 生成。

开题材料不是独立于项目的展示层。当前报告确定的题目和研究内容会反向影响 `overview/` 中的阶段规划、`motivation/` 中的后续实验设计以及项目级 README / PROJECT_INDEX 的方向说明。修改开题题目、研究内容或实验边界时，需要同步检查这些项目入口，避免开题材料和项目主线割裂。

同步关系是双向的：开题报告要根据项目当前进展、实验事实和后续规划来写；后续如果开题报告因为导师反馈、实验新结果或题目收敛而调整方向，项目规划、实验优先级和文档入口也要跟着调整。不能只把 opening 当成最终展示目录。

## 当前汇报主线

建议主线：

```text
数据库正在成为 AI workload 的入口
  -> 传统数据库执行过程无法覆盖数据组织、GPU 推理服务、调度、写回等新成本
  -> 本项目构建 Daft/Arrow + Ray + GPU endpoint + Lance/数据库 sink 的可控执行路径
  -> 初步实验发现 batch、模型服务路由、writeback 都会影响端到端性能
  -> 后续研究数据组织、Ray 调度反压和持久化写回的协同优化
```

一句话口径：

> 本课题关注数据库驱动 AI workload 的分布式数据执行与存储协同优化，重点研究 Database source、Daft / Arrow batch、Ray task / actor、GPU 模型服务、fan-in、Lance / pgvector / PostgreSQL sink 等阶段的瓶颈定位与协同优化。

## 目录结构

| 路径 | 作用 |
|---|---|
| `AGENTS.md` | 本目录长期规则 |
| `work_rules.md` | 开题工作的任务组织和目标管理规则 |
| `ppt_rules.md` | 开题 PPT 制作规则 |
| `outline.md` | 开题报告、PPT、飞书汇报的统一内容骨架 |
| `report/` | 开题报告正文与 Word 版本材料 |
| `slides/` | PPT 源稿、讲稿备注、PPTX 输出 |
| `feishu/` | 飞书进度汇报稿 |
| `literature/` | 文献清单、精读笔记、CCF-A 优先候选 |
| `assets/` | 图、SVG、表格、流程图、模板素材说明 |
| `logs/` | 非实验类 project log |

## 当前需要维护的材料

| 材料 | 主文件 | 状态 |
|---|---|---|
| 开题报告 | `report/opening_report.md` | 初稿已补 |
| 开题 PPT 源稿 | `slides/opening_ppt.md` | 16 页源稿已补 |
| 开题 PPTX | `slides/opening_defense_20260712.pptx` | 已按模板生成，并已导入飞书幻灯片 |
| 开题飞书 wiki 源稿 | `feishu/opening_report_wiki.md` | 已同步到飞书 |
| 动机测试飞书 wiki 源稿 | `feishu/motivation_feasibility_wiki.md` | 已同步到飞书 |
| 飞书进度汇报 | `feishu/progress_update.md` | 已同步当前进展 |
| 文献精读清单 | `literature/reading_list.md` | 候选清单已补，待精读 |
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

1. 检查 `slides/opening_defense_20260712.pptx` 和飞书幻灯片的人工放映效果，必要时微调版式和讲稿备注。
2. 继续根据导师反馈和新实验同步调整 `report/opening_report.md`、`feishu/opening_report_wiki.md`、`feishu/motivation_feasibility_wiki.md`、`slides/opening_ppt.md` 和项目总纲。
3. 后续补充更正式的 GPU-backed 图表和实验结果时，同步更新开题报告、飞书文档和 PPT。
4. 最后生成 DOCX。DOCX 必须使用学校 Word 模板，继承模板里的章节样式、字体、行间距、图表标注和参考文献格式。
## 飞书同步目标

后续需要写入飞书的主要目标：

| 飞书文档 | 链接 | 用途 |
|---|---|---|
| 开题报告与开题汇报 | https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink | 承载开题报告正文，并同步开题汇报 PPT 的核心内容和最终口径 |
| 动机测试与可行性测试 | https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink | 承载动机实验、可行性实验、阶段画像和实验结论边界 |
| 开题汇报飞书幻灯片 | https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg | 由 `slides/opening_defense_20260712.pptx` 导入生成 |

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
