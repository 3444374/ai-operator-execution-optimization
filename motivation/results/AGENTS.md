# motivation/results/AGENTS.md

本文件继承根 `AGENTS.md` 与 `motivation/AGENTS.md`，只增加端到端动机结果和系统画像规则。

## 子目录

- `gpu/`：生产式 GPU-backed E2E 主动机结果。
- `cpu/`：CPU baseline 对照。
- `pg18_4_fake/`：PG18.4 本地同构 fake-model 历史结果。
- `fake_cpu/`：fake/CPU 历史预研结果。

## 规则

- 能影响课题主线判断的端到端结果放这里。
- 每个正式报告必须包含运行命令、参数、CSV、阶段计时、结果解释和不能声称的结论。
- 不把 `pg18_4_fake/` 或 `fake_cpu/` 结果外推成 GPU-backed 结论。
