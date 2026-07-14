# CLAUDE.md

本文件等同于 codex 环境中的 `AGENTS.md`，是 Claude Code 环境的项目规则入口。所有规则内容通过 `@` 从已有 `AGENTS.md` 导入。

@AGENTS.md
@code/AGENTS.md
@experiments/AGENTS.md
@feasibility/AGENTS.md
@figures/AGENTS.md
@learning/AGENTS.md
@motivation/AGENTS.md
@notes/AGENTS.md
@opening/AGENTS.md
@overview/AGENTS.md
@research/AGENTS.md

---

## Claude Code ↔ codex 同步

**任何影响项目结构、方向、实验结论或关键入口的操作，必须记入 `PROJECT_LOG.md`。**

本项目在 codex 和 Claude Code 之间切换开发。切换前，按变更类型检查并回写：

| 变更类型 | 必须更新 |
|---|---|
| 目录结构变化 | `PROJECT_INDEX.md`、`README.md`、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md`、受影响目录的 `README.md` |
| 实验结论变化 | `motivation/results/` 或 `experiments/results/` 对应报告、`PROJECT_OUTLINE.md` §当前最重要证据、`PROJECT_LOG.md` |
| 方向/题目变化 | `AGENTS.md` §1、`opening/report/opening_report.md`、`opening/feishu/`、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md` |
| 规则变化 | 对应目录 `AGENTS.md`；如影响全局则同步更新根 `AGENTS.md`，记入 `PROJECT_LOG.md` |
| 新增/删除文件 | `PROJECT_INDEX.md`、所在目录 `README.md` |
| 新增/更新图表 | `figures/README.md`、`figures/audit/`；如影响主线论证则同步 `opening/report/` |

回写目标：`AGENTS.md`（规则）和 `README.md`（内容）都要更新，保持两个环境规则一致。

AGENTS.md §9 包含相同的清单——在 codex 中做变更时，按同样规则回写。
