# experiments/plans/AGENTS.md

继承上级规则。计划入口见 [README.md](README.md)，实验设计要求按 `experiments/AGENTS.md` 执行。

- 顶层计划在开头写当前状态、已完成项与剩余项；主要工作完成后移入 `completed/`，保留原要求并指向结果。
- `reference/` 只放长期参考；`archive/` 保存已替代、暂停且无当前执行授权的方案。
- 部分完成的计划明确剩余任务；剩余任务已由另一计划承担时，原计划移入 `completed/` 并指向替代入口。
- 移动按根 §7.1 更新现行引用与索引并检查本地链接；历史运行命令可保留，通过当前索引能找到新位置。
