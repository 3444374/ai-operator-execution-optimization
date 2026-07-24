# research/reading_notes/ — 项目级精读笔记权威库

本目录是**所有论文精读笔记的权威来源**（project-level，单一来源）。其他位置（开题精读快照、Obsidian wiki）都是本目录的衍生。

## 内容

- **33 篇精读笔记**（`<短名>_<会议年份>.md`，按 `tpl-文献精读-深度版.md` 四层结构：基本信息 → 论文结构分析 → 批判性评估 → 与课题连接）。
- **2 个模板**：`tpl-文献精读-深度版.md`（深度精读）、`tpl-文献泛读.md`（泛读）。
- **配图**：`figs/`（16 张论文原图，服务于笔记讲解；14 篇笔记引用）。

## 来源与选取链路（provenance）

1. **候选池**：`research/ai_operator_literature_inventory.md`——项目全量文献库（**66 篇**，CCF-A 优先 + 权威系统论文 + 工程系统资料，v5 / 2026-07-24）。
2. **精读**：从 66 篇中精读本目录 **33 篇**（覆盖开题所需的基础工作、核心技术、相关工作；部分论文仅入 inventory 未精读）。
3. **Top 15 排名**：`research/top15_ranked_papers.md` 按学术研究标准从 66 篇选前 15（均在本次 33 篇精读范围内）。

> 链路：`research` 全量文献库（66 篇）→ 精读（本目录 33 篇）→ Top 15 排名 → 开题精读快照（`opening/literature/top15_reading_notes/`）。

## 与开题 top15 快照的关系

- `opening/literature/top15_reading_notes/` 是本目录中 **Top 15 这 15 篇笔记的拷贝快照**（自包含交付，含同级 `figs/`）。
- **权威版始终在本目录**；快照为开题答辩交付用，笔记更新后需重新 `cp` 到快照目录。
- 精读清单与状态见 `opening/literature/reading_list.md`。

## 配图说明（figs/）

- 论文原图，从 `research/reference/<x>.pdf` 抽取：嵌入栅格图直抽（像素级精确）；矢量图按"图题锚定 + 彩色∪矢量范围 + 列/页宽 + getbbox 收紧 + 28px 留白"裁剪。
- 命名 `<短名>_fig<N>.png`，与笔记相对路径 `figs/<x>.png` 一致。
- 笔记内以 `![图 N · 描述](figs/<x>.png)` 嵌入，置于"## ▎配图（辅助讲解）"或"## ▎图复审补充"区块。

## 与 Obsidian wiki 的关系

- 本目录是**源**，wiki `raw/papers/` 是**编译面**。
- 笔记/配图变更后用 `../ai-operator-wiki/sync-wiki.sh --notes` 同步；反向编辑用 `--back`。

## 编辑规则

- 新增精读：先入 `ai_operator_literature_inventory.md` 候选池 → 按模板写笔记 → 登记 `reading_list.md` → 若进 Top 15 则 cp 到快照目录。
- 笔记内容必须区分：论文事实 / 官方文档 / 本地实验事实 / 合理推断 / 待确认（遵循 `karpathy-guidelines`）。
- 不把 microbenchmark 结论写成系统性结论；不把未核验资料写成事实。
