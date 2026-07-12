# overview/AGENTS.md

本目录维护项目总纲、当前路线和阶段计划。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 记录课题定位、用户目标、阶段计划和方向边界。
- 记录当前证据链的高层结论。
- 不放原始 CSV，不放长篇文献摘录，不放代码实现细节。

## 边界

- `current_direction_and_plan.md` 记录当前路线、阶段计划和近期任务。
- `project_outline.md` 保持稳定，只记录高层定位和目录结构。
- 实验数字、原始结果和长篇分析放到 `feasibility/results/` 或 `motivation/results/`。
- 文献依据放到 `research/`。

## 规则

当以下内容变化时，同步更新 `current_direction_and_plan.md`：

- 课题方向或候选技术路线变化；
- 业务场景变化；
- 重要实验结论变化；
- PG18.4 / PG18.3 链路状态变化；
- baseline 优先级变化。

- 过时路线、旧任务和已被新实验替代的判断要更新或删除，避免和根 README、PROJECT_INDEX 冲突。
- 每个判断要区分本地实验事实、模拟实验事实、合理推断和待确认问题。
- 不得把 PG18.4 本地结果写成 PostgreSQL 18.3 内部平台结果。
