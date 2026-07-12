# motivation/AGENTS.md

本目录维护数据库 AI 算子的动机场景、端到端主动机实验、系统画像、瓶颈定位和可优化点分析。进入本目录前先读根目录 `AGENTS.md`，再读本文件和 `README.md`。

## 作用

- 定义为什么“数据库 AI 算子的外部服务链路”值得优化。
- 优先用生产式 GPU-backed E2E profile 建立主动机：数据库触发 AI 算子，外部 Ray/Daft/Lance-like 链路调用 GPU-backed 模型服务，结果写回数据库。
- 保留三类 AI 算子 baseline：`AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE`。
- 记录 PG18.4 / PG18.3 数据库触发链路中的阶段计时、瓶颈定位和不能声称的结论。

## 目录结构

- `plans/`：场景、路线、集成计划和实验设计，不放原始结果。
- `benchmarks/`：动机实验脚本。当前脚本多为历史 fake/CPU 预研或机制验证，不能直接外推到 GPU-backed 链路。
- `results/gpu/`：后续生产式 GPU-backed E2E 主动机结果和阶段画像。
- `results/pg18_4_fake/`：PG18.4 本地同构 fake-model 历史结果。
- `results/fake_cpu/`：fake/CPU 历史预研 CSV 和综合分析。

## 边界

- GPU-backed E2E profile 是主动机主线；CPU/fake 只用于脚本调试、计时边界验证、历史对照或谨慎消融。
- 消融实验优先在同一条 GPU-backed E2E 链路上做大块对照，例如 no-Ray/Ray、single worker/actor pool、主控写回/worker 写回、bounded/unbounded in-flight。
- 不把 PG18.4 fake-model 结果写成 PostgreSQL 18.3、真实 GPU 模型服务或真实生产链路结论。
- 不把 microbenchmark 包装成完整论文结论。
- Ray / 非 Ray 对比是 baseline 和消融，不是论文主问题本身。

## 结果规则

- 端到端动机测试、系统画像、瓶颈定位和可优化点分析放 `motivation/results/`。
- 只证明环境可连接、脚本可 dry-run、数据库可读写的结果放 `feasibility/results/`。
- 每个正式结果必须包含运行命令、参数、CSV 路径、阶段计时、严谨性自检和不能声称的结论。
- 修改实验、结果或计划后，同步检查本目录 `README.md`、`results/README.md`、根 `README.md`、`PROJECT_INDEX.md` 和 `learning/experiment_walkthrough.md` 是否需要更新。

