# CLAUDE.md

本文件是 Claude Code 的项目规则入口。全项目规则只从根 `AGENTS.md` 导入，避免无条件加载所有
目录规则并产生重复或冲突。

@AGENTS.md

处理某个目录或文件前，继续按 `AGENTS.md` §1 的规则层级执行：从项目根到目标目录，逐级读取沿途
存在的 `AGENTS.md`，再读目标目录 `README.md`。子目录规则只在任务进入该路径时生效。

Codex 与 Claude Code 共用同一套规则、事实入口、Git/隐私要求和变更日志；不要在本文件复制这些
规则。若 Claude Code 需要新增专属行为，只记录工具差异，不重复项目规范。
