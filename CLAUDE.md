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
@deploy/runtime/AGENTS.md

## 多机器运行与环境切换的强制路由

涉及新机器、新容器、单/多 GPU 切换、缺依赖、缺模型或缺数据集时，不得从历史聊天
猜安装命令，也不得直接运行全量 pip/正式实验。必须先读
`deploy/runtime/README.md`，让 preflight 自动选择 machine profile 并保存报告；只有
明确选定能力/资产后才执行独立的安装或下载命令。driver 与 vLLM 环境保持隔离，新机器
通过最小 correctness gate 后按机器/模型/服务/workload 签名重新校准，不能继承其他
签名的性能最优配置；签名未变化时才允许复用带证据 SHA 的冻结校准合同。

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

## Git 规则

**禁止在 commit message 中添加 Co-Authored-By 或任何形式的 AI 署名。** 所有 commit 的用户署名只能是项目开发者本人，不允许将 Claude、codex 或任何 AI 工具写入 contributor。

**禁止把隐私数据提交进 Git**（API key、token、外部服务器 IP/host、非 localhost 用户名/口令、私钥、`sshpass -p <pw>`）。新代码连接串用环境变量引用；evidence 经 `src/baselines/common/redact.py` 脱敏；commit 前跑 `python code/scripts/environment/scan_git_secrets.py`（建议 `git config core.hooksPath .githooks`）。完整规则与本地默认放行口径见根 `AGENTS.md` §10。
