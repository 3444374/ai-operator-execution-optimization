# 开题材料导航

本文件回答一个问题：准备开题材料时，需要什么内容应该去哪里找。开题报告、PPT、飞书汇报和文献材料必须相互同步，不允许各自形成一套口径。

## 先读顺序

进入开题工作时，按这个顺序读：

1. `AGENTS.md`：本目录长期规则。
2. `README.md`：开题工作区总入口。
3. `navigation.md`：查找材料和同步关系。
4. `claim_matrix.md`：开题 framing、证据等级、P0 实验和停止规则。
5. 当前任务对应文件：报告、PPT、飞书、文献或素材。

如果需要项目整体背景，先回到根目录：

```text
README.md
PROJECT_INDEX.md
AGENTS.md
```

## 项目材料从哪里找

| 需要的内容 | 优先读取 |
|---|---|
| 项目总方向、边界、目录职责 | `AGENTS.md`、`README.md`、`PROJECT_INDEX.md` |
| 当前研究路线和阶段计划 | `overview/current_direction_and_plan.md` |
| 数据库 AI 算子场景和 workload | `motivation/plans/workloads.md`、`motivation/plans/ai_sql_surface.md` |
| 真实 GPU-backed 动机实验结果 | `motivation/results/gpu/README.md` |
| 阶段拆分和链路画像结果 | `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md` |
| Ray 多 endpoint 结果 | `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md` |
| 历史 fake / CPU 结果 | `motivation/results/fake_cpu/`、`motivation/results/pg18_4_fake/` |
| 可行性验证 | `feasibility/results/` |
| 术语、实验讲解、学习版解释 | `learning/experiment_walkthrough.md` |
| 图表文件 | `figures/` |
| 外部系统和文献证据 | `research/` |
| Snowflake / pgai / PostgresML 链路对比 | `research/existing_ai_operator_execution_chains.md` |

## 开题内部材料从哪里找

| 需要的内容 | 优先读取 |
|---|---|
| 开题统一口径和页序 | `report/opening_report.md` |
| Claim 边界和实验准入 | `claim_matrix.md` |
| 开题报告正文 | `report/opening_report.md` |
| 当前开题 PPT 与可复现构建 | `slides/opening_defense_20260812_v9.pptx`、`slides/build_opening_defense_v9_artifact_tool.mjs`、`slides/opening_defense_20260812_v9_qa.md` |
| 当前叙事内容上游 | `opening_defense_outline_20260808.md`、`claim_matrix.md`、`report/opening_report.md`；v9 页数不固定，按对外叙事重新组织 |
| PPT 制作规则 | `ppt_rules.md` |
| 开题工作组织规则 | `work_rules.md` |
| 飞书同步规则 | `feishu/README.md` |
| 开题飞书历史快照 | `feishu/opening_report_wiki.md`（已过期、禁止同步；当前正文见`report/opening_report.md`） |
| 飞书进度汇报 | `feishu/progress_update.md` |
| 文献清单和精读笔记 | `literature/reading_list.md` |
| 答辩问答 | 现场预演手册 `report/opening_defense_qa/opening_defense_qa.tex`（同目录 PDF）；历史攻击面清单 `qa_bank.md` |
| 图表和素材规则 | `assets/README.md` |
| 模板来源和外部文件记录 | `templates/README.md` |
| 非实验类修改日志 | `logs/project_log.md` |

## 报告、PPT、飞书之间的同步关系

当前工作顺序：

```text
冻结叙事与图集
  -> 本地 PPT 制作与逐页 QA
  -> 中文 Markdown 报告补全
  -> 飞书云文档发布与差异审计
```

当前用户只要求中文 Markdown 与飞书云文档，不生成 Word，也不同步 Wiki。

### 报告是完整论证

`report/opening_report.md` 承载完整文字论证，包括背景、现状、问题、目标、研究内容、技术路线、实验基础、计划和参考文献。

报告中的每个关键判断都要能追溯到：

```text
实验结果 / 文献 / 官方文档 / 项目方向文档
```

### PPT 是现场讲解

当前 `slides/opening_defense_20260812_v9.pptx` 为 31 页对外答辩版，不承担报告的全部细节。PPT 保留学校视觉识别，但内容区按“背景收敛—动机导出—方法对应—验证闭环”自由排版；旧 `slides/opening_ppt.md` 只保留历史版式经验，不再作为当前内容入口。

PPT 修改后要反查报告：

- 有没有改变研究问题？
- 有没有改变技术路线？
- 有没有新增实验结论？
- 有没有新增或删除文献口径？

如果有，必须同步报告和飞书版。

### 飞书是同步发布面

`feishu/` 里的文稿用于同步到飞书 wiki。飞书内容应来自本地 Markdown，不直接在飞书里临时形成新口径。

飞书同步前检查：

- 题目是否和报告、PPT 一致。
- 研究内容是否和 `report/opening_report.md` 一致。
- 实验结论是否来自 `motivation/results/` 或 `feasibility/results/`。
- 解释方式是否参考 `learning/experiment_walkthrough.md`。
- 是否写清楚“能说明什么”和“不能说明什么”。

## 常见任务导航

### 写开题报告

读取：

```text
opening/report/opening_report.md
overview/current_direction_and_plan.md
research/
motivation/results/gpu/
```

写完后检查：

```text
slides/opening_ppt.md
feishu/progress_update.md
literature/reading_list.md
logs/project_log.md
```

### 做开题 PPT

读取：

```text
opening/ppt_rules.md
opening/report/opening_report.md
figures/README.md
figures/data/selected_motivation_figures.md
```

做完后检查：

```text
report/opening_report.md
feishu/README.md
qa_bank.md
```

### 同步飞书

读取：

```text
opening/feishu/README.md
opening/report/opening_report.md
opening/feishu/opening_report_wiki.md  # 历史快照，只读对照，禁止作为覆盖源
opening/slides/opening_ppt.md
motivation/results/
learning/experiment_walkthrough.md
```

同步后记录：

```text
opening/logs/project_log.md
```

### 整理文献

读取：

```text
opening/literature/reading_list.md
research/literature_and_evidence_review.md
research/existing_ai_operator_execution_chains.md
```

必要时使用 `nature-academic-search` 检索和核验，再按需使用 `deep-research` 做研究版图和缺口判断。

### 画实验图

读取：

```text
opening/assets/README.md
figures/AGENTS.md
figures/audit/figure_plan.md
figures/audit/experiment_charts_audit.md
opening/ppt_rules.md
```

图表数据必须追溯到真实 CSV 或正式报告。

## 同步检查清单

每次完成开题相关修改后，至少检查：

- `opening/README.md` 是否需要更新入口或状态。
- `opening/report/opening_report.md` 是否需要同步口径。
- `opening/report/opening_report.md` 和 `opening/slides/opening_ppt.md` 是否互相一致。
- `opening/feishu/` 是否需要同步。
- `opening/literature/reading_list.md` 是否需要补文献或调整状态。
- `opening/logs/project_log.md` 是否记录了非实验类修改。
