# code/AGENTS.md

本目录用于可复用工程代码、实验驱动脚本和测试入口。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放可复用系统实现，而不是一次性 notebook 或临时 CSV。
- 承载 PostgreSQL connector、fake/real `AI_EMBED` pipeline、Ray/Daft/Lance 类外部执行链路、优化实现和测试入口。
- 为 `feasibility/` 和 `motivation/` 提供可复用脚本，但正式结果仍写入对应结果目录。

## 边界

- 不在 `code/` 堆临时 CSV、原始实验结果或一次性 notebook。
- 绘图、图表复现和素材筛选脚本归 `figures/scripts/`；`code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口。
- 连接验证结果归 `feasibility/results/`。
- 系统画像、瓶颈定位和动机结果归 `motivation/results/`。

## 规则

- 所有代码应有可运行入口、配置、结果输出和最小验证。
- 不提前引入复杂框架；只有当重复逻辑稳定出现时才抽象。
- 每条真实运行 CSV 必须记录实际 `server_version` 和 `pgvector_version`，不能靠目录名推断平台。
- Python baseline、Ray task、Ray actor、模型服务等 baseline 要尽量共享同一数据读取和写回路径，避免 baseline 不可比。
- 完成代码实现或功能测试后，按根规则同步更新 `learning/` 中的学习讲解。

详细脚本说明见 `scripts/README.md`。
