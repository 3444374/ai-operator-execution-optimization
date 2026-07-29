# research/reading_notes/ — 项目级精读笔记权威库

本目录是**所有论文精读笔记的权威来源**（project-level，单一来源）。其他位置（开题精读快照、Obsidian wiki）都是本目录的衍生。

## 内容

- **49 篇精读笔记**（`<短名>_<会议年份>.md`，按 `tpl-文献精读-深度版.md` 四层结构：基本信息 → 论文结构分析 → 批判性评估 → 与课题连接）。
- **2 个模板**：`tpl-文献精读-深度版.md`（深度精读）、`tpl-文献泛读.md`（泛读）。
- **配图**：`figs/`（16 张论文原图，服务于笔记讲解；14 篇笔记引用）。

## 来源与选取链路（provenance）

1. **候选池**：`research/ai_operator_literature_inventory.md`——按 Top 15、核心补充、工程资料和题录勘误分级。
2. **精读**：本目录现有 **49 篇**权威笔记；2026-07-29 新增 VTC、Llumnix、LOTUS、Palimpzest、Abacus、SemBench、FairServe、DLPM、Autellix、Chiron。
3. **Top 15 排名**：`research/top15_ranked_papers.md` 当前 15/15 为严格 CCF-A 正式 research paper，均已有本目录笔记和本地 PDF。

> 链路：题录核验 → 文献分级 → 全文精读 → Top 15 排名 → 开题精读快照（`opening/literature/top15_reading_notes/`）。

## 与开题 top15 快照的关系

- `opening/literature/top15_reading_notes/` 是本目录中 **Top 15 这 15 篇笔记的拷贝快照**（自包含交付，含同级 `figs/`）。
- **权威版始终在本目录**；快照为开题答辩交付用，笔记更新后需重新 `cp` 到快照目录。
- 精读清单与状态见 `opening/literature/reading_list.md`。

## 配图说明（figs/）

- 论文原图，从 `research/reference/<x>.pdf` 抽取：嵌入栅格图直抽（像素级精确）；矢量图按"图题锚定 + 彩色∪矢量范围 + 列/页宽 + getbbox 收紧 + 28px 留白"裁剪。
- 命名 `<短名>_fig<N>.png`，与笔记相对路径 `figs/<x>.png` 一致。
- 笔记内以 `![图 N · 描述](figs/<x>.png)` 嵌入，置于"## ▎配图（辅助讲解）"或"## ▎图复审补充"区块。

## 编辑规则

- 新增精读：先入 `ai_operator_literature_inventory.md` 候选池 → 按模板写笔记 → 登记 `reading_list.md` → 若进 Top 15 则 cp 到快照目录。
- 笔记内容必须区分：论文事实 / 官方文档 / 本地实验事实 / 合理推断 / 待确认（遵循 `karpathy-guidelines`）。
- 不把 microbenchmark 结论写成系统性结论；不把未核验资料写成事实。
