# feasibility/benchmarks/AGENTS.md

本目录只放组件级可行性脚本和公共工具。

## 可以放

- Ray task、Ray object、Arrow IPC、shuffle、fan-in 等 microbenchmark。
- 用于生成 `feasibility/results/` 报告的分析脚本。
- 可行性脚本共享的轻量公共函数。

## 不放

- 数据库 AI 算子端到端主动机脚本。
- GPU-backed E2E profile 脚本。
- 正式工程代码。稳定可复用代码应迁移到 `code/`。

## 规则

- 默认输出路径应指向 `feasibility/results/`。
- 脚本名称要说明被验证的组件，不使用抽象阶段名。
- 新脚本要同步更新 `README.md` 和 `feasibility/results/README.md`。

